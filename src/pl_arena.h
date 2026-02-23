#ifndef PL_ARENA_H
#define PL_ARENA_H

#include <stdint.h>

/* A bump allocator that stores data in a chain of non-moving blocks.
 * Allocations are pointer-sized aligned (sizeof(uintptr_t)) with zero
 * padding between entries. The structure grows by doubling blocks, but
 * never past its cap.
 *
 * Layout of how an object is stored:
 * +--------------+-----------+
 * | object bytes | 0-padding |
 * +--------------+-----------+
 *
 * Designed for short-lived entities where many objects share the same
 * lifetime and are freed together (e.g. a query IR tree, a result set).
 * Allocation is a pointer bump into contiguous memory, deallocation is
 * O(blocks) instead of O(objects). Individual objects cannot be freed;
 * the entire arena is released at once via pl_arena_clean/pl_arena_destroy.
 *
 * WARNING: alignment is sizeof(uintptr_t), not alignof(max_align_t).
 * Do not store types that require stricter alignment (double, long double).
 */
typedef struct {
  struct PlArenaBlock *head; // first block
  struct PlArenaBlock *tail; // last block (append target)
  uint32_t used;             // total bytes used across blocks
  uint32_t cap;              // hard bytes-cap for total used
  uint32_t block_sz;         // size of the next block to allocate
} PlArena;

/* A small pointer vector used to collect elements before flattening them
 * into a PlArena-owned array.
 *
 * Ownership: PtrVec owns its heap buffer; flattened arrays are arena-owned.
 * Side effects: ptrvec_push may allocate or reallocate the heap buffer.
 * Error semantics: functions return OK/ERR, or NULL on allocation failure. */
typedef struct {
  void **items;
  uint32_t len;
  uint32_t cap;
} PtrVec;

/* Allocates and returns a PlArena. The first block is 1 KiB when size_p
 * is NULL, otherwise it uses *size_p. Each subsequent block doubles in size.
 * If cap_p is provided, it is used as the total hard cap. Returns NULL if
 * there's an error. */
PlArena *pl_arena_create(uint32_t *size_p, uint32_t *cap_p);

/* Initializes an arena in-place. Returns OK on success, ERR on bad input. */
int pl_arena_init(PlArena *ar, uint32_t *size_p, uint32_t *cap_p);

/* Frees the memory used by 'ar' and its data. */
void pl_arena_destroy(PlArena *ar);

/* Frees the memory used by 'ar' but not the arena itself. */
void pl_arena_clean(PlArena *ar);

/* Checks whether 'ar' is in the canonical zeroed/uninitialized state.
 * Returns YES when zeroed, NO when any field is non-zero, ERR on invalid input.
 */
int pl_arena_is_zeroed(const PlArena *ar);

/* Validates basic arena structural consistency for initialized arenas.
 * Returns YES when valid, NO when inconsistent (or zeroed), ERR on bad input.
 * Should not be used for normal build since it's pretty slow.
 */
int pl_arena_is_ok(const PlArena *ar);

/* Ensure 'extra' bytes available, growing if needed.
 * Returns OK on success, ERR if cap reached or errors occurred. */
int pl_arena_ensure(PlArena *ar, uint32_t extra);

/* Allocates 'len' bytes inside the arena and returns the pointer.
 * Memory is uninitialized. Returns NULL on error. */
void *pl_arena_alloc(PlArena *ar, uint32_t len);

/* Allocates 'len' bytes inside the arena and returns the pointer.
 * Memory is zero-initialized. Returns NULL on error. */
void *pl_arena_calloc(PlArena *ar, uint32_t len);

/* Copies 'len' bytes from 'start_v' into the arena.
 * Returns a pointer to the stored copy on success, NULL on error. */
void *pl_arena_add(PlArena *ar, void *start_v, uint32_t len);

/* Copies 'len' bytes from 'start_v' into the arena and appends a NUL byte.
 * Use this for C strings. Returns a pointer to the stored copy, NULL on error.
 */
void *pl_arena_add_nul(PlArena *ar, void *start_v, uint32_t len);

/* Returns the number of bytes used by the data inside 'ar'. */
uint32_t pl_arena_get_used(PlArena *ar);

/* Appends a pointer to a temporary vector.
 * Ownership: vector owns the heap buffer for items.
 * Side effects: may realloc on heap.
 * Returns OK/ERR. */
int ptrvec_push(PtrVec *v, void *ptr);

/* Copies a temporary vector into the arena and returns the new array.
 * Ownership: returned array is owned by the arena.
 * Side effects: allocates arena memory.
 * Returns NULL on error or when v->len == 0. */
void **ptrvec_flatten(PtrVec *v, PlArena *a);

/* Frees the heap storage owned by the vector but keeps the struct.
 * Ownership: caller retains the vector struct for reuse.
 * Side effects: frees heap memory.
 * Returns void. */
void ptrvec_clean(PtrVec *v);

#endif
