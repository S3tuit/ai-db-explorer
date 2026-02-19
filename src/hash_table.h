#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>
#include <stdint.h>

typedef struct HashTable HashTable;

/* Callback used by custom-key hash tables.
 * Contract for callers:
 * - Must be deterministic for the same logical key.
 * - Should hash exactly the fields that HtEqFn compares.
 * - Must return non-zero for valid keys.
 * - Returning 0 signals an invalid key; custom put/get reject it.
 */
typedef uint64_t (*HtHashFn)(const void *key, void *ctx);

/* Callback used by custom-key hash tables for key equality.
 * Contract for callers:
 * - Must return YES when keys are equal, NO otherwise.
 * - Must define an equivalence relation consistent with HtHashFn.
 * - Must not mutate keys or rely on transient external state.
 */
typedef int (*HtEqFn)(const void *a, const void *b, void *ctx);

/* Hashes arbitrary bytes using the table's default non-cryptographic hash.
 * It's up to the caller to validate input; data may be NULL only if len == 0.
 * Returns a deterministic hash for valid input. Returning 0 in custom hash
 * functions signals an error, however, we skip the 0 check in this func because
 * it's near impossible that it'll return exactly 0.
 */
uint64_t inline ht_hash_bytes(const void *data, size_t len);

/* Same as 'ht_hash_bytes' but uses a caller-defined 'seed'. Usefull to create
 * one hash from 2+ non contiguos values. */
uint64_t inline ht_hash_bytes_withSeed(const void *key, size_t len,
                                       uint64_t seed);

/* Allocates a caller-owned hash table with default capacity.
 * Ownership: caller owns the returned table and must call ht_destroy().
 * Side effects: allocates heap memory.
 * Returns a valid table on success, NULL on allocation failure.
 */
HashTable *ht_create(void);

/* Allocates a heap-owned hash table with capacity at least min_slots.
 * Ownership: caller owns the returned table and must call ht_destroy().
 * Side effects: allocates heap memory.
 * Returns a valid table on success, NULL on invalid input or allocation
 * failure.
 */
HashTable *ht_create_with_capacity(size_t min_slots);

/* Allocates a heap-owned hash table that uses caller-provided key semantics.
 * Ownership: caller owns the returned table and must call ht_destroy().
 * Side effects: allocates heap memory.
 * Returns a valid table on success, NULL on invalid input or allocation
 * failure.
 */
HashTable *ht_create_custom(HtHashFn hash_fn, HtEqFn eq_fn, void *ctx);

/* Allocates a heap-owned custom-key hash table with capacity at least
 * min_slots.
 * Ownership: caller owns the returned table and must call ht_destroy().
 * Side effects: allocates heap memory.
 * Returns a valid table on success, NULL on invalid input or allocation
 * failure.
 */
HashTable *ht_create_custom_with_capacity(size_t min_slots, HtHashFn hash_fn,
                                          HtEqFn eq_fn, void *ctx);

/* Initializes a hash table with default capacity.
 * Intended usage: re-initialize a heap-created table after ht_clean().
 * Side effects: allocates internal slot storage.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int ht_init(HashTable *ht);

/* Initializes a hash table with capacity at least min_slots.
 * Intended usage: re-initialize a heap-created table after ht_clean().
 * Side effects: allocates internal slot storage.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int ht_init_with_capacity(HashTable *ht, size_t min_slots);

/* Initializes a custom-key hash table with default capacity.
 * Intended usage: re-initialize a heap-created table after ht_clean().
 * Side effects: allocates internal slot storage.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int ht_init_custom(HashTable *ht, HtHashFn hash_fn, HtEqFn eq_fn, void *ctx);

/* Initializes a custom-key hash table with capacity at least min_slots.
 * Intended usage: re-initialize a heap-created table after ht_clean().
 * Side effects: allocates internal slot storage.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int ht_init_custom_with_capacity(HashTable *ht, size_t min_slots,
                                 HtHashFn hash_fn, HtEqFn eq_fn, void *ctx);

/* Releases internal allocations of a hash table but keeps the struct.
 * Ownership: caller retains the table struct and may call ht_init*() again.
 * Side effects: frees internal slot storage and resets metadata.
 * Error semantics: none (safe on NULL).
 */
void ht_clean(HashTable *ht);

/* Releases all resources owned by a heap-created hash table.
 * Ownership: invalidates ht pointer.
 * Side effects: frees internal storage and table object.
 * Error semantics: none (safe on NULL).
 */
void ht_destroy(HashTable *ht);

/* Returns number of live key/value pairs in the table. */
size_t ht_len(const HashTable *ht);

/* Inserts or updates one key/value pair.
 * Keys are borrowed (not copied) and must remain valid and immutable while they
 * are in the table. value must be non-NULL.
 * Side effects: may grow/rehash the table.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int ht_put(HashTable *ht, const char *key, uint32_t key_len, const void *value);

/* Inserts or updates one key/value pair in a custom-key hash table.
 * Keys are borrowed (not copied) and must remain valid/immutable while they
 * are in the table. value must be non-NULL.
 * Side effects: may grow/rehash the table.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int ht_put_custom(HashTable *ht, const void *key, const void *value);

/* Looks up a key and returns its mapped value pointer, or NULL if missing.
 * Keys are borrowed and compared by bytes.
 */
const void *ht_get(const HashTable *ht, const char *key, uint32_t key_len);

/* Looks up a key in a custom-key hash table and returns mapped value pointer,
 * or NULL if missing.
 * Keys are borrowed and compared using the table's custom eq_fn.
 */
const void *ht_get_custom(const HashTable *ht, const void *key);

#endif
