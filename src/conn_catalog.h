#ifndef CONN_CATALOG_H
#define CONN_CATALOG_H

#include "pl_arena.h"
#include "safety_policy.h"
#include <stddef.h>
#include <stdint.h>

#define CURR_CONN_CAT_VERSION "1.0"

typedef enum {
  DB_KIND_POSTGRES = 1,
} DbKind;

/* Represent whether or not a column should be treated as sensitive */
typedef struct ColumnRule {
  const char *table;
  const char *col;
  const char **schemas; // sorted unique array; NULL if no schema list
  uint32_t n_schemas;
  int is_global; // 1 if rule applies regardless of schema
} ColumnRule;

/* Groups all the ColumnRule for a ConnProfile. */
typedef struct ColumnPolicy {
  ColumnRule *rules; // sorted by (table, col)
  size_t n_rules;
  PlArena arena; // owns all strings and arrays in ColumnPolicy
} ColumnPolicy;

/* Represent whether or not a function is safe to call. */
typedef struct SafeFunctionRule {
  const char *name;
  const char **schemas; // sorted unique array; NULL if no schema list
  uint32_t n_schemas;
  int is_global; // 1 if rule applies regardless of schema
} SafeFunctionRule;

/* Groups all the SafeFunctionRule for a ConnProfile. */
typedef struct SafeFunctionPolicy {
  SafeFunctionRule *rules; // sorted by function name
  size_t n_rules;
  PlArena arena; // owns all strings and arrays in SafeFunctionPolicy
} SafeFunctionPolicy;

/**
 * Non-secret connection parameters.
 * All strings are owned by the catalog and remain valid until
 * catalog_destroy().
 */
typedef struct {
  const char *connection_name; // stable string id (unique)
  DbKind kind;

  const char *host; // e.g., "127.0.0.1"
  uint16_t port;    // e.g., 5432
  const char *db_name;
  const char *user;

  // Optional: extra options, TLS mode, parameters, etc.
  const char *options; // may be NULL

  SafetyPolicy safe_policy;
  // Column sensitivity rules for this connection (may be empty).
  ColumnPolicy col_policy;

  // User-defined safe functions for this connection (may be empty).
  SafeFunctionPolicy safe_funcs;
} ConnProfile;

typedef struct ConnCatalog {
  ConnProfile *profiles; // owned array
  size_t n_profiles;

  // Global/default policy loaded from config. Copied into each profile's
  // safe_policy during parsing in v1.
  SafetyPolicy policy;
} ConnCatalog;

// Creates a catalog from a config file path.
// Catalog owns all memory referenced by returned ConnProfile pointers.
// On failure: returns NULL and sets *err_out to an allocated message that the
// caller must free(3). On success, *err_out is set to NULL.
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
 * Otherwise writes up to that many pointers into out and returns how many were
 * written.
 */
size_t catalog_list(ConnCatalog *cat, ConnProfile **out, size_t cap_count);

/**
 * Returns YES if (schema?, table, column) is marked sensitive by the profile.
 *
 * Business logic (v1, no search_path resolution):
 * - If a global rule table.column exists, it always matches (even if
 * schema-qualified).
 * - If no global rule exists and SQL is schema-qualified, it matches only if
 * the schema is listed for that table.column.
 * - If no global rule exists and SQL is unqualified, any schema-scoped rule for
 *   that table.column matches (since we do not resolve search_path in v1).
 *
 * Returns YES/NO/ERR.
 */
int connp_is_col_sensitive(const ConnProfile *cp, const char *schema,
                           const char *table, const char *column);

/**
 * Returns YES if the function name is marked safe by the profile.
 *
 * Business logic (v1, no search_path resolution):
 * - If a global rule "fn" exists, it always matches (even if schema-qualified).
 * - If no global rule exists and SQL is schema-qualified, it matches only if
 * the schema is listed for that function name.
 * - If no global rule exists and SQL is unqualified, any schema-scoped rule for
 *   that function name matches (since we do not resolve search_path in v1).
 *
 * Returns YES/NO/ERR.
 */
int connp_is_func_safe(const ConnProfile *cp, const char *schema,
                       const char *name);

#endif
