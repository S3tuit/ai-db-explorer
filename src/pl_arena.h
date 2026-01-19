#ifndef PL_ARENA_H
#define PL_ARENA_H

#include <stdint.h>


/* It stores data in a contigous array. Each object (of n bytes) 
 * is prefixed with n in the in-memory layout. The last
 * byte is always 0. offsets are always memory alligned with 0 padding.
 * The structure automatically grows, but never past its cap. 
 * 
 * Layout of how an object is stored:
 * +---+--------------+---+-----------+
 * | n | object bytes | 0 | 0-padding |
 * +---+--------------+---+-----------+
 *
 * */
typedef struct {
    uint8_t *data;        // array where data is stored
    uint32_t used;        // bytes used
    uint32_t cap;         // hard bytes-cap to the size of 'data'
    uint32_t curr_sz;     // current allocated size of 'data'
} PlArena;

/* Allocates and returns a PlArena, of '*size_p' bytes and '*cap_p'
 * bytes cap. Default values are used if one or two arguments are
 * missing. Returns NULL if there's an error. */
PlArena *pl_arena_create(uint32_t *size_p, uint32_t *cap_p);

/* Frees the memory used by 'ar' and its data. */
void pl_arena_destroy(PlArena *ar);

/* Ensure 'extra' bytes available, growing if needed.
 * Returns OK on success, ERR if cap reached or errors occurred. */
int pl_arena_ensure(PlArena *ar, uint32_t extra);

typedef struct {
    uint32_t len;         // num of bytes (without the last 0)
    uint8_t *v;           // pointer to the payload inside the arena
} PlObject;

/* Adds 'len' bytes starting from 'start_v' to the data of 'ar'.
 * Returns the offset from the start of data where 'str' is stored.
 * Returns UINT32_MAX if error. */
uint32_t pl_arena_add(PlArena *ar, void *start_v, uint32_t len);

/* Returns a PlObject that point to the 'offset' of 'ar''s data.
 * On error returns {0, NULL}. */
PlObject pl_arena_get(PlArena *ar, uint32_t offset);

/* Returns a pointer to the 'offset' of 'ar's data. On error, NULL. */
void *pl_arena_getv(PlArena *ar, uint32_t offset);

/* Returns the number of bytes used by the data inside 'ar'. Return -1
 * if error. */
uint32_t pl_arena_get_used(PlArena *ar);

#endif
