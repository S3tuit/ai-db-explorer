#ifndef SENSITIVE_TOK_H
#define SENSITIVE_TOK_H

#include <stdint.h>

#include "conn_catalog.h"
#include "packed_array.h"
#include "spool.h"

/* Token prefix for sensitive-value handles. */
#define SENSITIVE_TOK_PREFIX "tok_"

/* Broker-owned token entry for sensitive values.
 * v1 stores Postgres metadata only.
 * TODO: support multi-db metadata once additional backends are enabled.
 */
typedef struct SensitiveTok {
  const char *value;   // broker-owned plaintext value; may be NULL for SQL NULL
  uint32_t value_len;  // bytes in 'value' (excluding trailing NUL)
  const char *col_ref; // canonicalized identifier: schema.table.column
  uint32_t pg_oid;     // Postgres OID for typed bind
  uint8_t is_null;     // 1 when SQL NULL, 0 otherwise
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

/* Per-connection token store owned by one broker session.
 * Ownership:
 * - connection_name is borrowed from ConnProfile/catalog lifetime.
 * - tokens/index/pool are owned and released by packed-array cleanup.
 */
typedef struct DbTokenStore {
  const char *connection_name;
  SafetyColumnStrategy mode;
  PackedArray *tokens; // entries are SensitiveTok
  StringPool col_ref_pool;
  struct HashTable *det_index; // used only for deterministic mode
} DbTokenStore;

/* Creates a packed array of DbTokenStore entries configured with cleanup.
 * Ownership: caller owns the returned PackedArray and must destroy it via
 * parr_destroy(). Individual DbTokenStore entries are cleaned automatically by
 * the array callback.
 * Returns a valid packed array on success, NULL on allocation failure.
 */
PackedArray *stok_store_array_create(void);

/* Looks up or lazily initializes one DbTokenStore for 'profile'.
 * Ownership: borrows all inputs. On success, '*out_store' points to
 * packed-array owned memory valid until stores is mutated/destroyed. Side
 * effects: may append a new DbTokenStore and allocate its internals. Returns OK
 * on success, ERR on invalid input or allocation failure.
 */
int stok_store_get_or_init(PackedArray *stores, const ConnProfile *profile,
                           DbTokenStore **out_store);

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
