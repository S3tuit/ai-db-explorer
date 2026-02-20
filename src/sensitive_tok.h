#ifndef SENSITIVE_TOK_H
#define SENSITIVE_TOK_H

#include <stddef.h>
#include <stdint.h>

#include "conn_catalog.h"
#include "pl_arena.h"
typedef struct DbTokenStore DbTokenStore;

/* Token prefix for sensitive-value handles. */
#define SENSITIVE_TOK_PREFIX "tok_"
// "tok_" + 31-byte connection name + "_" + uint32 + "_" + uint32 + '\0'
#define SENSITIVE_TOK_BUFSZ 64u

/* Broker-owned token entry for sensitive values.
 * v1 stores Postgres metadata only.
 * TODO: support multi-db metadata once additional backends are enabled.
 */
typedef struct SensitiveTok {
  const char *value;   // borrowed plaintext value; may be NULL for SQL NULL
  uint32_t value_len;  // bytes in 'value' (excluding trailing NUL)
  const char *col_ref; // borrowed canonicalized identifier: schema.table.column
  uint32_t col_ref_len;
  uint32_t pg_oid; // Postgres OID for typed bind
} SensitiveTok;

/* Borrowed parsed view over one token buffer.
 * connection_name points inside the caller-owned input string passed to
 * stok_parse_view_inplace().
 */
typedef struct ParsedTokView {
  char *connection_name;
  uint32_t generation;
  uint32_t index;
} ParsedTokView;

/* Creates one heap-owned DbTokenStore from a connection profile.
 * Ownership:
 * - borrows 'profile' and 'arena';
 * - caller owns returned store and must call stok_store_destroy().
 * Side effects: allocates token array, string pool, and optional deterministic
 * hash index.
 * Returns a valid store on success, NULL on invalid input/allocation failure.
 */
DbTokenStore *stok_store_create(const ConnProfile *profile, PlArena *arena);

/* Destroys one heap-owned DbTokenStore.
 * Ownership: releases store-owned internals and invalidates 'store'.
 * Side effects: destroys token array/string pool/hash index.
 * Error semantics: none (safe on NULL).
 */
void stok_store_destroy(DbTokenStore *store);

/* Compares two stores by connection_name.
 * It borrows both inputs and does not allocate memory.
 * Side effects: none.
 * Returns YES when both stores target the same connection, NO otherwise.
 */
int stok_store_same_connection(const DbTokenStore *a, const DbTokenStore *b);

/* Returns YES when store is bound to 'connection_name', NO when different.
 * Returns ERR on invalid input.
 */
int stok_store_matches_conn_name(const DbTokenStore *store,
                                 const char *connection_name);

/* Returns number of tokens currently stored in one store.
 * Returns 0 on NULL input.
 */
size_t stok_store_len(const DbTokenStore *store);

/* Returns borrowed read-only SensitiveTok at 'idx', or NULL when out-of-range.
 */
const SensitiveTok *stok_store_get(const DbTokenStore *store, uint32_t idx);

/* Borrowed input view used to create one token entry.
 * This is caller-owned; stok_store_create_token() never takes ownership.
 */
typedef struct SensitiveTokIn {
  const char *value;  // may be NULL only when value_len == 0 (SQL NULL payload)
  uint32_t value_len; // bytes in 'value'
  const char *col_ref;
  uint32_t col_ref_len;
  uint32_t pg_oid;
} SensitiveTokIn;

/* Creates (or reuses) one token string for a sensitive value.
 * Ownership:
 * - borrows all inputs;
 * - mutates 'store' and writes token text into caller-owned out_tok buffer.
 * Side effects: may append one SensitiveTok entry, intern bytes in StringPool,
 * and update deterministic hash index.
 * Returns token byte length (without NUL) on success, -1 on invalid input or
 * allocation failure.
 */
int stok_store_create_token(DbTokenStore *store, uint32_t generation,
                            const SensitiveTokIn *in,
                            char out_tok[SENSITIVE_TOK_BUFSZ]);

/* Parses one token in-place.
 * Expected format: tok_<connection_name>_<generation>_<index>
 *
 * On success, this function writes NUL bytes over the last two underscores so
 * that out->connection_name points to a stable C string inside 'token'.
 * The input buffer must be writable and NUL-terminated.
 *
 * Returns OK on success, ERR on invalid format/range/input (does not change
 * input token).
 */
int stok_parse_view_inplace(char *token, ParsedTokView *out);

#endif
