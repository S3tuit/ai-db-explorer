#include "conn_catalog.h"
#include "bufio.h"
#include "json_codec.h"
#include "stdio_byte_channel.h"
#include "string_op.h"
#include "utils.h"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CONFIG_MAX_BYTES (8u * 1024u * 1024u)
#define CONFIG_MAX_CONNECTIONS 50u


/* Loads the entire file at 'path' into 'sb'.
 * Returns OK/ERR and assigns a static error message to *err_out on failure. */
static int read_file_to_sb(const char *path, StrBuf *sb, char **err_out) {
  int fd = -1;
  char *err_msg = NULL;
  BufChannel bc;
  int bc_inited = 0;

  if (!path || !sb) {
    if (err_out) *err_out = "Can't read file, invalid input.";
    return ERR;
  }

  fd = open(path, O_RDONLY);
  if (fd < 0) {
    err_msg = "ConnCatalog: unable to open config file.";
    goto error;
  }

  ByteChannel *ch = stdio_bytechannel_open_fd(fd, -1);
  if (!ch) {
    err_msg = "ConnCatalog: unable to read config file.";
    goto error;
  }

  if (bufch_init(&bc, ch) != OK) {
    bytech_destroy(ch);
    err_msg = "ConnCatalog: unable to read config file.";
    goto error;
  }
  bc_inited = 1;

  // we have to make sure the file is not too long
  size_t total = 0;
  for (;;) {
    // if there's still one byte to read, we have to read it
    int rc = bufch_ensure(&bc, 1);
    if (rc == NO) break;
    if (rc == ERR) {
      err_msg = "ConnCatalog: read error.";
      goto error;
    }

    // how much the channel has buffered
    size_t avail = 0;
    const uint8_t *p = bufch_peek(&bc, &avail);
    if (!p || avail == 0) continue;
    if (total + avail > CONFIG_MAX_BYTES) {
      err_msg = "ConnCatalog: config file exceeds 8 MiB.";
      goto error;
    }

    // read
    char *dst = NULL;
    if (sb_prepare_for_write(sb, avail, &dst) != OK) {
      err_msg = "ConnCatalog: out of memory.";
      goto error;
    }
    if (bufch_read_n(&bc, dst, avail) != OK) {
      err_msg = "ConnCatalog: read error.";
      goto error;
    }
    total += avail;
  }

  bufch_clean(&bc);
  return OK;

error:
  if (err_out) *err_out = err_msg;

  if (fd >= 0 && !bc_inited) close(fd);
  if (bc_inited) bufch_clean(&bc);
  return ERR;
}

/* Parses the "safetyPolicy" object and stores the resulting SafetyPolicy into
 * '*out'. */
static int parse_policy(const JsonGetter *jg, SafetyPolicy *out) {
  if (!jg || !out) return ERR;

  // makes sure policy if correctly formatted
  const char *const keys[] = {
    "readOnly", "statementTimeoutMs", "maxRowReturned"
  };
  int vrc = jsget_top_level_validation(jg, "safetyPolicy", keys, ARRLEN(keys));
  if (vrc != YES) return ERR;

  // readOnly
  JsonStrSpan span = {0};
  if (jsget_string_span(jg, "safetyPolicy.readOnly", &span) != YES) return ERR;
  int read_only_flag = 0;
  if (STREQ(span.ptr, span.len, "yes")) read_only_flag = 1;
  else if (STREQ(span.ptr, span.len, "no")) read_only_flag = 0;
  else return ERR;

  // statement_timeout_ms
  uint32_t timeout_ms = 0;
  if (jsget_u32(jg, "safetyPolicy.statementTimeoutMs", &timeout_ms) != YES) return ERR;

  // max_row_per_query
  uint32_t max_rows = 0;
  if (jsget_u32(jg, "safetyPolicy.maxRowReturned", &max_rows) != YES) return ERR;

  out->read_only = read_only_flag;
  out->statement_timeout_ms = timeout_ms;
  out->max_rows = max_rows;
  out->max_cell_bytes = 0;
  return OK;
}

/* Parses one database entry object into 'out'. */
static int parse_db_entry(const JsonGetter *jg, ConnProfile *out) {
  if (!jg || !out) return ERR;

  const char *const keys[] = {
    "type", "connectionName", "host", "port", "username", "database", "options"
  };
  int vrc = jsget_top_level_validation(jg, NULL, keys, ARRLEN(keys));
  if (vrc != YES) return ERR;

  char *type = NULL;
  char *conn_name = NULL;
  char *host = NULL;
  char *user = NULL;
  char *db_name = NULL;
  char *options = NULL;

  if (jsget_string_decode_alloc(jg, "type", &type) != YES) goto error;
  if (strcmp(type, "postgres") != 0) goto error;

  if (jsget_string_decode_alloc(jg, "connectionName", &conn_name) != YES) goto error;
  if (jsget_string_decode_alloc(jg, "host", &host) != YES) goto error;

  uint32_t port = 0;
  if (jsget_u32(jg, "port", &port) != YES || port > UINT16_MAX) goto error;

  if (jsget_string_decode_alloc(jg, "username", &user) != YES) goto error;
  if (jsget_string_decode_alloc(jg, "database", &db_name) != YES) goto error;

  int orc = jsget_string_decode_alloc(jg, "options", &options);
  if (orc == ERR) goto error;
  if (orc == NO) options = NULL;

  free(type);

  out->connection_name = conn_name;
  out->kind = DB_KIND_POSTGRES;
  out->host = host;
  out->port = (uint16_t)port;
  out->db_name = db_name;
  out->user = user;
  out->options = options;
  return OK;

error:
  free(type);
  free(conn_name);
  free(host);
  free(user);
  free(db_name);
  free(options);
  return ERR;
}

static void profile_free(ConnProfile *p) {
  if (!p) return;
  free((char *)p->connection_name);
  free((char *)p->host);
  free((char *)p->db_name);
  free((char *)p->user);
  free((char *)p->options);
}

/* Parses the "databases" array and allocates ConnProfile entries. */
static int parse_databases(const JsonGetter *jg, ConnProfile **out_profiles,
                           size_t *out_count) {
  if (!jg || !out_profiles || !out_count) return ERR;
  *out_profiles = NULL;
  *out_count = 0;

  JsonArrIter it;
  int rc = jsget_array_objects_begin(jg, "databases", &it);
  if (rc != YES) return ERR;

  if (it.count < 0 || (size_t)it.count > CONFIG_MAX_CONNECTIONS) return ERR;

  size_t n = (size_t)it.count;
  ConnProfile *profiles = NULL;
  if (n > 0) profiles = (ConnProfile *)xmalloc(n * sizeof(*profiles));

  size_t idx = 0;
  for (;;) {
    JsonStrSpan obj = {0};
    rc = jsget_array_objects_next(jg, &it, &obj);
    if (rc == NO) break;
    if (rc != YES) goto error;

    JsonGetter entry = {0};
    if (jsget_init(&entry, obj.ptr, obj.len) != OK) goto error;

    if (parse_db_entry(&entry, &profiles[idx]) != OK) goto error;

    // connectionName must be unique
    for (size_t j = 0; j < idx; j++) {
      if (strcmp(profiles[idx].connection_name,
                 profiles[j].connection_name) == 0) {
        goto error;
      }
    }

    idx++;
  }

  *out_profiles = profiles;
  *out_count = idx;
  return OK;

error:
  if (profiles) {
    for (size_t i = 0; i < n; i++) {
      profile_free(&profiles[i]);
    }
    free(profiles);
  }
  return ERR;
}

ConnCatalog *catalog_load_from_file(const char *path, char **err_out) {
  char *err_msg = NULL;
  StrBuf sb = {0};
  ConnCatalog *cat = NULL;

  if (!path) {
    err_msg = "ConnCatalog: can't read from a NULL path.";
    goto error;
  }

  if (read_file_to_sb(path, &sb, &err_msg) != OK) {
    goto error;
  }

  JsonGetter jg;
  if (jsget_init(&jg, sb.data, sb.len) != OK) {
    err_msg = "ConnCatalog: invalid JSON.";
    goto error;
  }

  // make sure these 2 objects are present in the config file
  const char *const root_keys[] = {"safetyPolicy", "databases"};
  if (jsget_top_level_validation(&jg, NULL, root_keys, ARRLEN(root_keys)) != YES) {
    err_msg = "ConnCatalog: unknown key at top level.";
    goto error;
  }

  // TODO: make errors of parse_policy and parse_databases more user-friendly

  cat = xmalloc(sizeof(*cat));
  if (parse_policy(&jg, &cat->policy) != OK) {
    err_msg = "ConnCatalog: invalid safetyPolicy.";
    goto error;
  }

  if (parse_databases(&jg, &cat->profiles, &cat->n_profiles) != OK) {
    err_msg = "ConnCatalog: invalid \"databases\".";
    goto error;
  }

  sb_clean(&sb);
  return cat;

error:
  sb_clean(&sb);
  catalog_destroy(cat);
  if (err_out && err_msg) *err_out = err_msg;
  return NULL;
}

void catalog_destroy(ConnCatalog *cat) {
  if (!cat) return;
  if (cat->profiles) {
    for (size_t i = 0; i < cat->n_profiles; i++) {
      profile_free(&cat->profiles[i]);
    }
    free(cat->profiles);
    cat->profiles = NULL;
  }
  free(cat);
}

size_t catalog_count(const ConnCatalog *cat) {
  if (!cat) return 0;
  return cat->n_profiles;
}

size_t catalog_list(ConnCatalog *cat, ConnProfile **out, size_t cap_count) {
  if (!cat) return 0;
  if (!out || cap_count == 0) return cat->n_profiles;

  size_t n = (cat->n_profiles < cap_count) ? cat->n_profiles : cap_count;
  for (size_t i = 0; i < n; i++) {
    out[i] = &cat->profiles[i];
  }
  return n;
}

SafetyPolicy *catalog_get_policy(ConnCatalog *cat) {
  if (!cat) return NULL;
  return &cat->policy;
}

ConnProfile *catalog_get_by_name(ConnCatalog *cat, const char *connection_name) {
  if (!cat || !connection_name) return NULL;
  for (size_t i = 0; i < cat->n_profiles; i++) {
    ConnProfile *p = &cat->profiles[i];
    if (p->connection_name && strcmp(p->connection_name, connection_name) == 0) {
      return p;
    }
  }
  return NULL;
}
