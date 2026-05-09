#ifndef SENSITIVE_TOK_H
#define SENSITIVE_TOK_H

#include <stddef.h>
#include <stdint.h>

#include "conn_catalog.h"
#include "utils.h"
typedef struct DbTokenStore DbTokenStore;
typedef struct SensitiveTokSession SensitiveTokSession;

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
  const char *domain;  // borrowed canonical sensitive-domain name
  uint32_t domain_len;
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

typedef enum StokResolveStatus {
  STOK_RESOLVE_OK = 0,
  STOK_RESOLVE_ERR_INPUT = -1,
  STOK_RESOLVE_ERR_FORMAT = -2,
  STOK_RESOLVE_ERR_CONNECTION = -3,
  STOK_RESOLVE_ERR_STALE = -4,
  STOK_RESOLVE_ERR_UNKNOWN = -5,
} StokResolveStatus;

/* Creates one heap-owned sensitive-token session state.
 * Ownership:
 * - caller owns returned session and must call stok_session_destroy().
 * Side effects: allocates the bounded token arena and store registry.
 * Returns a valid session on success, NULL on invalid input/allocation failure.
 */
SensitiveTokSession *stok_session_create(uint32_t arena_cap);

/* Destroys one heap-owned sensitive-token session and every store it owns.
 * Error semantics: none (safe on NULL).
 */
void stok_session_destroy(SensitiveTokSession *sess);

/* Checks whether a sensitive-token session is initialized and internally
 * usable.
 */
AdbxTriStatus stok_session_is_ok(const SensitiveTokSession *sess);

/* Returns the current session generation, or 0 for NULL input. */
uint32_t stok_session_generation(const SensitiveTokSession *sess);

/* Returns the number of bytes currently used in the session token arena.
 * This is intended for focused tests and diagnostics.
 */
uint32_t stok_session_arena_used(SensitiveTokSession *sess);

/* Resolves per-session token store for 'profile', creating it if needed.
 * Ownership: returned store is borrowed from 'sess' and remains valid until
 * the session is destroyed. If token creation resets the session after arena
 * cap exhaustion, the store that triggered the reset remains valid but is
 * reinitialized with empty token state; all other borrowed stores are
 * invalidated and must be reacquired.
 */
DbTokenStore *stok_session_get_or_create_store(SensitiveTokSession *sess,
                                               const ConnProfile *profile);

/* Destroys one heap-owned DbTokenStore.
 * Ownership: releases store-owned internals and invalidates 'store'.
 * Side effects: destroys token array/hash index.
 * Error semantics: none (safe on NULL).
 */
void stok_store_destroy(DbTokenStore *store);

/* Compares two stores by connection_name.
 * It borrows both inputs and does not allocate memory.
 * Returns YES when both stores target the same connection, NO otherwise.
 */
AdbxTriStatus stok_store_same_connection(const DbTokenStore *a,
                                         const DbTokenStore *b);

/* Returns YES when store is bound to 'connection_name', NO when different.
 * Returns ERR on invalid input.
 */
AdbxTriStatus stok_store_matches_conn_name(const DbTokenStore *store,
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
  const char *domain; // borrowed and must outlive any token created from it
  uint32_t domain_len;
  uint32_t pg_oid;
} SensitiveTokIn;

/* Creates (or reuses) one token string for a sensitive value.
 * Ownership:
 * - borrows all inputs;
 * - mutates 'store' and writes token text into caller-owned out_tok buffer.
 * Side effects: may append one SensitiveTok entry and update the
 * deterministic hash index.
 * Returns token byte length (without NUL) on success, -1 on invalid input or
 * allocation failure.
 */
int stok_store_create_token(DbTokenStore *store, const SensitiveTokIn *in,
                            char out_tok[SENSITIVE_TOK_BUFSZ]);

/* Parses, validates, and resolves one caller-owned token string against a
 * store. The token buffer is modified only when the raw token format is valid.
 * On STOK_RESOLVE_OK, '*out_tok' receives a borrowed SensitiveTok entry.
 */
StokResolveStatus stok_store_resolve_token(DbTokenStore *store, char *token,
                                           const SensitiveTok **out_tok);

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
AdbxStatus stok_parse_view_inplace(char *token, ParsedTokView *out);

#endif
