#ifndef PACKED_ARRAY_H
#define PACKED_ARRAY_H

#include <stddef.h>
#include <stdbool.h>


/* A swap-remove packed array. This module is responsible for the ownership of
 * the objects it contains.
 *
 * - The array owns the storage for its objects.
 * - Callers may *borrow* pointers to objects, but those pointers
 *   are not stable across growth or swap-remove.
 */

typedef struct PackedArray PackedArray;

/* Optional cleanup hook.
 * Called for each live object on parr_destroy(), and for the removed object
 * on parr_drop_swap(). The array passes the object's address and cleanup_ctx.
 *
 * If objects are plain old data, you can ignore cleanup and keep it NULL.
 */
typedef void (*parr_cleanup_fn)(void *obj, void *cleanup_ctx);

/* Creates a packed array for objects of fixed size obj_sz bytes.
 * New objects returned by parr_emplace() are uninitialized.
 *
 * Alignment: objects are stored at least max_align_t-aligned and each slot
 * is padded so every object begins at an aligned address.
 *
 * Returns: a new PackedArray* (owned by caller).
 */
PackedArray *parr_create(size_t obj_sz);

/* Creates a packed array with an explicit upper bound on storage in bytes.
 * The array will never grow its internal buffer beyond upper_bound bytes.
 */
PackedArray *parr_create_upper_bound(size_t obj_sz, size_t upper_bound);

/* Destroyes the array and free its storage.
 * If a cleanup callback was set, it is called for every live object first.
 */
void parr_destroy(PackedArray *a);

/* Sets or replaces the cleanup callback.
 * cleanup may be NULL to disable.
 *
 * Note: changing the cleanup function does not retroactively affect already
 * cleaned up objects; it only affects future destroy/drop operations.
 */
void parr_set_cleanup(PackedArray *a, parr_cleanup_fn cleanup, void *cleanup_ctx);

/* Number of live objects. */
size_t parr_len(const PackedArray *a);

/* Returns a pointer to the object at idx (mutable).
 * idx must be < parr_len(a).
 *
 * Pointer stability:
 * - The returned pointer becomes invalid if the array grows.
 * - The object at idx may change after parr_drop_swap().
 */
void *parr_at(PackedArray *a, size_t idx);

/* Const version of parr_at(). */
const void *parr_cat(const PackedArray *a, size_t idx);

/* Allocates a new object slot at the end and return:
 * - the new object's index, and
 * - (optionally) a pointer to its storage via out_ptr.
 *
 * The memory is uninitialized.
 * This may grow the internal buffer, invalidating previously borrowed pointers.
 * Returns SIZE_MAX on failure.
 */
size_t parr_emplace(PackedArray *a, void **out_ptr);

/* Remove the object at idx in O(1) by swapping the last element into idx.
 * Order is not preserved.
 *
 * If a cleanup callback was set, it is called on the removed object *before*
 * it is overwritten.
 *
 * idx must be < parr_len(a).
 */
void parr_drop_swap(PackedArray *a, size_t idx);

#endif
