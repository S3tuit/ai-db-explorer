#ifndef SPOOL_H
#define SPOOL_H

#include <stddef.h>

#include "pl_arena.h"

/* StringPool interns repeated strings so equal content shares one pointer.
 *
 * Use this when many objects reference a small set of repeated string values
 * (for example schema/table/column names) and you want:
 * - lower memory usage via deduplication,
 * - pointer-stable string storage for the pool lifetime,
 * - fast equality checks by pointer when values were interned through
 *   the same pool.
 *
 * Lifetime/ownership:
 * - All returned pointers from spool_add/spool_addn are owned by this pool.
 * - Pointers remain valid until spool_clean/spool_destroy.
 */
typedef struct StringPool {
  PlArena arena;           // owned storage for interned bytes
  struct HashTable *index; // owned key->interned pointer map
} StringPool;

/* Allocates and initializes a heap-owned string pool.
 * Ownership: caller owns returned pool and must call spool_destroy().
 * Side effects: allocates heap memory for pool internals.
 * Returns valid pool on success, NULL on allocation/init failure.
 */
StringPool *spool_create(void);

/* Initializes an in-place string pool.
 * Ownership: caller retains the struct and must call spool_clean().
 * Side effects: initializes arena and hash table internals.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
int spool_init(StringPool *sp);

/* Releases resources owned by an initialized pool, keeping the struct.
 * Ownership: caller retains the struct and may call spool_init() again.
 * Side effects: frees hash table and arena allocations.
 * Error semantics: none (safe on NULL).
 */
void spool_clean(StringPool *sp);

/* Releases a heap-owned pool and all interned strings.
 * Ownership: invalidates 'sp'.
 * Side effects: frees all pool allocations.
 * Error semantics: none (safe on NULL).
 */
void spool_destroy(StringPool *sp);

/* Interns a NUL-terminated string and returns its stable pool pointer.
 * If the same content was already interned, returns existing pointer.
 * Pointer validity: returned pointer remains valid until spool_clean/destroy.
 * Returns NULL on invalid input or allocation failure.
 */
const char *spool_add(StringPool *sp, const char *s);

/* Interns exactly 'len' bytes from 's' and returns stable pool pointer.
 * If the same byte sequence was already interned, returns existing pointer.
 * The stored payload is NUL-terminated for convenience.
 * Pointer validity: returned pointer remains valid until spool_clean/destroy.
 * Returns NULL on invalid input or allocation failure.
 */
const char *spool_addn(StringPool *sp, const char *s, size_t len);

#endif
