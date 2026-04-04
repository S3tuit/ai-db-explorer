#ifndef CONN_CATALOG_H
#define CONN_CATALOG_H

#include "adbx_err.h"
#include "arena.h"
#include "safety_policy.h"
#include "secret_store.h"
#include "utils.h"

#include <stddef.h>
#include <stdint.h>

#define CURR_CONN_CAT_VERSION "1.1"
/* Max bytes allowed for ConnProfile.connection_name (excluding NUL). */
#define CONN_NAME_MAX_LEN 31u
#define NAMESPACE_MAX_LEN 31u

typedef enum {
  DB_KIND_POSTGRES = 1,
} DbKind;

typedef enum {
  CONNCAT_ERR_NONE = 0,
  CONNCAT_ERR_INVALID_INPUT,
  CONNCAT_ERR_AMBIGUOUS_DOMAIN,
  CONNCAT_ERR_INTERNAL,
} ConnCatalogErrCode;

typedef struct {
  ConnCatalogErrCode code;
  char msg[ADBX_ERRMSG_MAX];
} ConnCatalogErr;

typedef struct {
  const char *domain; // borrowed from ConnProfile
  ConnCatalogErr err;
} SensDomainOut;

/* One parsed sensitive-domain rule. Strings are owned by the parent policy
 * arena. schema/table may be NULL based on the original qualifier depth.
 */
typedef struct SensitiveRule {
  const char *schema;
  const char *table;
  const char *column_pat;
  const char *domain;   // which sensitiveDomain it belongs to
  uint16_t star_count;  // how many globs; '*'
  uint16_t literal_len; // how many non globs char
} SensitiveRule;

/* One contiguous slice of rules with the same precedence bucket. The slice is
 * borrowed from SensitiveDomainPolicy.storage.
 */
typedef struct SensitiveRuleBucket {
  SensitiveRule *rules;
  size_t n_rules;
} SensitiveRuleBucket;

/* Groups all sensitive-domain rules for a ConnProfile.
 * storage is sorted in precedence order and bucket fields are slices into it.
 */
typedef struct SensitiveDomainPolicy {
  SensitiveRule *storage;
  size_t n_storage;

  SensitiveRuleBucket exact_stc; // SensitiveRule with not globs and schema,
                                 // table, column_pat not null
  SensitiveRuleBucket glob_stc;  // SensitiveRule with globs and schema, table,
                                 // column_pat not null
  SensitiveRuleBucket exact_tc;  // SensitiveRule with not globs and table,
                                 // column_pat not null
  SensitiveRuleBucket glob_tc;
  SensitiveRuleBucket exact_c;
  SensitiveRuleBucket glob_c;

  Arena arena; // owns 'storage' and the strings it references
} SensitiveDomainPolicy;

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
  Arena arena; // owns all strings and arrays in SafeFunctionPolicy
} SafeFunctionPolicy;

/**
 * Non-secret connection parameters.
 * All strings are owned by the catalog and remain valid until
 * catalog_destroy().
 */
typedef struct {
  const char *connection_name; // stable string id (unique)
  SecretRefInfo secret_ref;    // borrows catalog namespace + connection_name
  DbKind kind;

  const char *host; // e.g., "127.0.0.1"
  uint16_t port;    // e.g., 5432
  const char *db_name;
  const char *user;

  // Optional: extra options, TLS mode, parameters, etc.
  const char *options; // may be NULL

  SafetyPolicy safe_policy;
  // Sensitive-domain rules for this connection (may be empty).
  SensitiveDomainPolicy sens_policy;

  // User-defined safe functions for this connection (may be empty).
  SafeFunctionPolicy safe_funcs;
} ConnProfile;

typedef struct ConnCatalog {
  const char *credential_namespace; // owned
  ConnProfile *profiles;            // owned array
  size_t n_profiles;

  // Global/default policy loaded from config. Copied into each profile's
  // safe_policy during parsing in v1.
  SafetyPolicy policy;
} ConnCatalog;

// Creates a catalog from an opened config-file fd.
// Borrows 'fd' and rewinds it to the beginning before reading.
// Catalog owns all memory referenced by returned ConnProfile pointers.
// On failure: returns NULL and sets *err_out to an allocated message that the
// caller must free(). On success, *err_out is set to NULL.
ConnCatalog *catalog_load_from_fd(int fd, char **err_out);

/* Creates one empty in-memory state catalog for a namespace with no saved
 * entries. It allocates the catalog and one owned namespace copy.
 * Error semantics: returns a caller-owned catalog on success, NULL on invalid
 * input or allocation failure.
 */
ConnCatalog *catalog_create_empty(const char *cred_namespace);

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

/* Checks if the column in input, identified by 'schema', 'table', 'column',
 * belongs to a sensitive domain. 'cp', 'table', and 'column' must not be
 * NULL. Empty 'schema' is treated as absent.
 *
 * Accepted lookup shapes:
 * - (schema, table, column): fully qualified lookup.
 * - (NULL, table, column): underqualified lookup that falls back
 *   conservatively across all schema-qualified matches.
 *
 * Column-only lookups are rejected because they are too ambiguous to validate
 * safely.
 *
 * When 'out' is not NULL, the function clears it on entry.
 *
 * - YES means it found a match. 'out->domain' is a borrowed non-NULL domain
 *   string and 'out->err' is clear.
 * - NO means not sensitive. 'out->domain' is NULL and 'out->err' is clear.
 * - ERR means invalid input, ambiguous underqualified lookup, or internal
 *   inconsistency. 'out->domain' is NULL and 'out->err' is set when 'out' is
 *   provided.
 */
AdbxTriStatus connp_get_sensitive_domain(const ConnProfile *cp,
                                         const char *schema, const char *table,
                                         const char *column,
                                         SensDomainOut *out);
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
AdbxTriStatus connp_is_func_safe(const ConnProfile *cp, const char *schema,
                                 const char *name);

#endif
