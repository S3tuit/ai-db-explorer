#ifndef PL_ARENA_H
#define PL_ARENA_H

#include <stdint.h>


/* It stores data in a chain of non-moving blocks. Each object (of n bytes)
 * is prefixed with n in the in-memory layout. The last byte is always 0.
 * Offsets are aligned with 0 padding and refer to the logical concatenation
 * of all blocks, in insertion order. The structure grows by adding blocks,
 * but never past its cap.
 *
 * Layout of how an object is stored:
 * +---+--------------+---+-----------+
 * | n | object bytes | 0 | 0-padding |
 * +---+--------------+---+-----------+
 */
typedef struct {
    struct PlArenaBlock *head;   // first block
    struct PlArenaBlock *tail;   // last block (append target)
    uint32_t used;        // total bytes used across blocks
    uint32_t cap;         // hard bytes-cap for total used
    uint32_t block_sz;    // size of the next block to allocate
} PlArena;

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

/* Ensure 'extra' bytes available, growing if needed.
 * Returns OK on success, ERR if cap reached or errors occurred. */
int pl_arena_ensure(PlArena *ar, uint32_t extra);

/* Allocates 'len' bytes inside the arena and returns the payload pointer.
 * The payload is zero-initialized and NUL-terminated. */
void *pl_arena_alloc(PlArena *ar, uint32_t len);

/* Adds 'len' bytes starting from 'start_v' to the data of 'ar'.
 * Returns a pointer to the stored payload on success, NULL on error. */
void *pl_arena_add(PlArena *ar, void *start_v, uint32_t len);

/* Returns the number of bytes used by the data inside 'ar'. */
uint32_t pl_arena_get_used(PlArena *ar);

#endif
