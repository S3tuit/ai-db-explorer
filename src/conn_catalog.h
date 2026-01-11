#ifndef CONN_CATALOG_H
#define CONN_CATALOG_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils.h"
#include "safety_policy.h"

typedef enum {
  DB_KIND_POSTGRES = 1,
  DB_KIND_MONGO    = 2,
} DbKind;

/**
 * Non-secret connection parameters.
 * All strings are owned by the catalog and remain valid until catalog_destroy().
 */
typedef struct {
  const char *connection_name;   // stable string id (unique)
  DbKind      kind;

  const char *host;              // e.g., "127.0.0.1"
  uint16_t    port;              // e.g., 5432
  const char *db_name;           // postgres database name; optional for mongo depending on your design
  const char *user;              // postgres user; optional for mongo depending on auth mechanism

  // Reference used to lookup the password in SecretStore (not the password itself).
  const char *password_ref;      // may be NULL if connection requires no password

  // Optional: extra options, TLS mode, parameters, etc.
  const char *options;           // may be NULL
} ConnProfile;

typedef struct ConnCatalog {
  // Dummy in-memory catalog for now (borrowed pointers are fields in this struct)
  ConnProfile   profiles[1];
  size_t        n_profiles;

  SafetyPolicy  policy;
} ConnCatalog;

// Creates a catalog from a config file path (dummy for now).
// Catalog owns all memory referenced by returned ConnProfile pointers.
static inline int catalog_load_from_file(const char *path, ConnCatalog **out) {
  if (!path || !out) return ERR;

  ConnCatalog *cat = (ConnCatalog *)xmalloc(sizeof(*cat));
  if (!cat) return ERR;

  cat->n_profiles = 1;

  // Dummy profile: all strings are static literals => valid for program lifetime.
  cat->profiles[0].connection_name = "pg_dummy";
  cat->profiles[0].kind            = DB_KIND_POSTGRES;
  cat->profiles[0].host            = "localhost";
  cat->profiles[0].port            = 5432;
  cat->profiles[0].db_name         = "postgres";
  cat->profiles[0].user            = "postgres";
  cat->profiles[0].password_ref    = "dummy";
  cat->profiles[0].options         = NULL;

  // Dummy policy stored inside the catalog (borrowed by callers).
  safety_policy_init(&cat->policy, NULL, NULL, NULL, NULL);

  *out = cat;
  return OK;
}

// Free catalog and all owned memory.
static inline void catalog_destroy(ConnCatalog *cat) {
  if (!cat) return;

  // If later you allocate strings, free them here.
  // If SafetyPolicy ever needs cleanup, call it here as well.

  free(cat);
}

// Number of profiles in the catalog.
static inline size_t catalog_count(const ConnCatalog *cat) {
  if (!cat) return 0;
  return cat->n_profiles;
}

/**
 * Populates '*out' with an array of ConnProfile pointers owned by 'cat'.
 *
 * Ownership: returned pointers are BORROWED from the catalog and remain valid
 * until catalog_destroy().
 *
 * cap_count indicates the maximum number of POINTERS available at `out`.
 * If out == NULL or cap_count == 0, returns the total number of profiles.
 * Otherwise writes up to that many pointers into out and returns how many were written.
 */
static inline size_t catalog_list(ConnCatalog *cat, ConnProfile **out,
                                  size_t cap_count) {
  if (!cat) return 0;

  if (!out || cap_count == 0) return cat->n_profiles;

  size_t n = (cat->n_profiles < cap_count) ? cat->n_profiles : cap_count;

  for (size_t i = 0; i < n; i++) {
    out[i] = &cat->profiles[i];
  }
  return n;
}

/**
 * Get the global SafetyPolicy. Caller must NOT free the returned policy. May
 * return NULL.
 */
static inline SafetyPolicy *catalog_get_policy(ConnCatalog *cat) {
  if (!cat) return NULL;

  return &cat->policy;
}

/**
 * Lookup and return a profile by connection_name. Caller must NOT free the
 * returned value. May return NULL.
 */
static inline ConnProfile *catalog_get_by_name(ConnCatalog *cat,
                                                const char *connection_name) {
  if (!cat || !connection_name) return NULL;

  for (size_t i = 0; i < cat->n_profiles; i++) {
    ConnProfile *p = &cat->profiles[i];
    if (p->connection_name && strcmp(p->connection_name, connection_name) == 0) {
      return p;
    }
  }
  return NULL;
}

#endif
