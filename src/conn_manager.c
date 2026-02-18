#include "conn_manager.h"
#include "log.h"
#include "postgres_backend.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
  const ConnProfile *profile; // borrowed from catalog
  DbBackend *backend;         // owned by ConnManager (lazy)
  uint64_t last_used_ms;      // updated after exec (or via connm_mark_used)
} ConnEntry;

struct ConnManager {
  ConnCatalog *cat;     // owned
  SecretStore *secrets; // owned
  uint64_t ttl_ms;      // the time after which a backend that has
                        // not been used ( and has no running queries)
                        // may be disconnected.
  DbBackend *(*factory)(
      DbKind kind); // backend factory (borrowed - used for tests)

  ConnEntry *entries; // owned
  size_t n_entries;
};

/* Returns the ConnEntry identified by connection_name or NULL. Since for now we
 * expect few connection (<50 surely) this is good enough even for O(n) search
 * time. */
static ConnEntry *find_entry(ConnManager *m, const char *connection_name) {
  if (!m || !connection_name)
    return NULL;
  for (size_t i = 0; i < m->n_entries; i++) {
    const ConnProfile *p = m->entries[i].profile;
    if (p && p->connection_name &&
        strcmp(p->connection_name, connection_name) == 0) {
      return &m->entries[i];
    }
  }
  return NULL;
}

/* Returns the right DbBackend based on 'kind'. */
static DbBackend *db_backend_create(DbKind kind) {
  switch (kind) {
  case DB_KIND_POSTGRES:
    return postgres_backend_create();
  default:
    return NULL;
  }
}

/* Makes sure 'e' refers to a connected DbBackend. Creates the backend if null.
 * Connect the backend if not already connected. Returns OK/ERR. */
static int ensure_connected(ConnManager *m, ConnEntry *e) {
  if (!m || !e || !e->profile)
    return ERR;

  // Lazy-create backend object
  if (!e->backend) {
    if (!m->factory)
      return ERR;
    e->backend = m->factory(e->profile->kind);
  }

  // If backend is connected, keep it
  if (db_is_connected(e->backend) == YES) {
    return OK;
  }

  // Fetch password if needed
  StrBuf pw;
  sb_init(&pw);
  if (secret_store_get(m->secrets, e->profile->connection_name, &pw) != OK) {
    TLOG("ERROR - secret_store_get failed for %s", e->profile->connection_name);
    return ERR;
  }

  // Connect
  int rc =
      db_connect(e->backend, e->profile, &e->profile->safe_policy, pw.data);
  if (rc != OK) {
    TLOG("ERROR - db_connect failed for %s", e->profile->connection_name);
  }
  sb_zero_clean(&pw);

  if (rc != OK)
    return ERR;

  return OK;
}

#define TTL_CONNECTIONS_MS (10L * 60L * 1000L) // 10 minutes
ConnManager *connm_create_with_factory(ConnCatalog *cat, SecretStore *secrets,
                                       DbBackend *(*factory)(DbKind kind)) {
  if (!cat || !secrets)
    return NULL;

  ConnManager *m = (ConnManager *)xmalloc(sizeof(*m));

  m->cat = cat;
  m->secrets = secrets;

  m->ttl_ms = TTL_CONNECTIONS_MS;
  m->factory = factory ? factory : db_backend_create;

  // Build entries from catalog list (borrowed pointers)
  size_t total = catalog_count(cat);
  if (total == 0) {
    // Still valid; empty catalog
    m->entries = NULL;
    m->n_entries = 0;
    return m;
  }
  ConnProfile **tmp = xmalloc(total * sizeof(*tmp));
  size_t n = catalog_list(cat, tmp, total);
  m->entries = xmalloc(n * sizeof(*m->entries));

  // Use ConnProfiles to build our ConnEntrys
  m->n_entries = n;
  uint64_t t = now_ms_monotonic();
  for (size_t i = 0; i < n; i++) {
    m->entries[i].profile = tmp[i];
    m->entries[i].backend = NULL;
    m->entries[i].last_used_ms = t; // treat as "recent" at startup
  }

  free(tmp);
  return m;
}

ConnManager *connm_create(ConnCatalog *cat, SecretStore *secrets) {
  return connm_create_with_factory(cat, secrets, NULL);
}

/* Finds connections that have been unused for more than 'm->ttl_ms' ms and
 * closes them. */
static void connm_disconnect_idle(ConnManager *m) {
  if (!m || !m->entries || m->ttl_ms == 0)
    return;

  uint64_t t = now_ms_monotonic();
  for (size_t i = 0; i < m->n_entries; i++) {
    ConnEntry *e = &m->entries[i];
    if (!e->backend || db_is_connected(e->backend) != YES)
      continue;

    uint64_t age = (t >= e->last_used_ms) ? (t - e->last_used_ms) : 0;
    if (age > m->ttl_ms) {
      // connection has been unused for too long
      db_disconnect(e->backend);
    }
  }
}

void connm_destroy(ConnManager *m) {
  if (!m)
    return;

  if (m->entries) {
    for (size_t i = 0; i < m->n_entries; i++) {
      // out ConnEntry may not have a backend yet
      if (m->entries[i].backend) {
        // destroy() closes the connection too
        db_destroy(m->entries[i].backend);
        m->entries[i].backend = NULL;
      }
    }
    free(m->entries);
    m->entries = NULL;
  }

  if (m->cat) {
    catalog_destroy(m->cat);
    m->cat = NULL;
  }
  if (m->secrets) {
    secret_store_destroy(m->secrets);
    m->secrets = NULL;
  }
  free(m);
}

int connm_get_connection(ConnManager *m, const char *connection_name,
                         ConnView *out) {
  if (!m || !connection_name || !out)
    return ERR;
  out->db = NULL;
  out->profile = NULL;

  // Reap idle first (v1 simple model)
  connm_disconnect_idle(m);

  ConnEntry *e = find_entry(m, connection_name);
  TLOG("INFO - requested use of connection %s", connection_name);
  if (!e)
    return NO;

  if (ensure_connected(m, e) != OK)
    return ERR;

  out->db = e->backend;
  out->profile = e->profile;
  return YES;
}

void connm_mark_used(ConnManager *m, const char *connection_name) {
  if (!m || !connection_name)
    return;
  ConnEntry *e = find_entry(m, connection_name);
  if (!e)
    return;

  e->last_used_ms = now_ms_monotonic();
}

void connm_set_ttl_ms(ConnManager *m, uint64_t ttl_ms) {
  if (!m)
    return;
  m->ttl_ms = ttl_ms;
}
