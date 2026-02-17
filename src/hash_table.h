#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>
#include <stdint.h>

typedef struct HashTable HashTable;

/* Allocates a heap-owned hash table with default capacity.
 * Ownership: caller owns the returned table and must call ht_destroy().
 * Side effects: allocates heap memory.
 * Returns a valid table on success, NULL on invalid input or allocation
 * failure.
 */
HashTable *ht_create(void);

/* Allocates a heap-owned hash table with capacity at least min_slots.
 * Ownership: caller owns the returned table and must call ht_destroy().
 * Side effects: allocates heap memory.
 * Returns a valid table on success, NULL on invalid input or allocation
 * failure.
 */
HashTable *ht_create_with_capacity(size_t min_slots);

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

/* Looks up a key and returns its mapped value pointer, or NULL if missing.
 * Keys are borrowed and compared by bytes.
 */
const void *ht_get(const HashTable *ht, const char *key, uint32_t key_len);

#endif
