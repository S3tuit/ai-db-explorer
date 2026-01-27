#ifndef CONN_CATALOG_H
#define CONN_CATALOG_H

#include <stddef.h>
#include <stdint.h>
#include "safety_policy.h"

#define CURR_CONN_CAT_VERSION 1.0

typedef enum {
  DB_KIND_POSTGRES = 1,
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
  const char *db_name;           
  const char *user;              

  // Optional: extra options, TLS mode, parameters, etc.
  const char *options;           // may be NULL
} ConnProfile;

// TODO: use hash map
typedef struct ConnCatalog {
  ConnProfile  *profiles;    // owned array
  size_t        n_profiles;

  SafetyPolicy  policy;
} ConnCatalog;

// Creates a catalog from a config file path.
// Catalog owns all memory referenced by returned ConnProfile pointers.
// On failure: returns NULL and sets *err_out. 'err_out' is not malloc'd.
ConnCatalog *catalog_load_from_file(const char *path, char **err_out);

// Free catalog and all owned memory.
void catalog_destroy(ConnCatalog *cat);

// Number of profiles in the catalog.
size_t catalog_count(const ConnCatalog *cat);

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
size_t catalog_list(ConnCatalog *cat, ConnProfile **out, size_t cap_count);

/**
 * Get the global SafetyPolicy. Caller must NOT free the returned policy. May
 * return NULL.
 */
SafetyPolicy *catalog_get_policy(ConnCatalog *cat);

/**
 * Lookup and return a profile by connection_name. Caller must NOT free the
 * returned value. May return NULL.
 */
ConnProfile *catalog_get_by_name(ConnCatalog *cat, const char *connection_name);

#endif
