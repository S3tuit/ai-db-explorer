#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdalign.h>
#include <stddef.h>

#include "pl_arena.h"
#include "utils.h"


/*------------------------------------ helpers ------------------------------*/
static inline uint32_t min_u32(uint32_t a, uint32_t b) { return a < b ? a : b; }

static inline uint32_t pl_align_up_u32(uint32_t n, uint32_t a) {
  // a must be power-of-two for this fast path
  return (n + (a - 1u)) & ~(a - 1u);
}

static inline int pl_is_power_of_two_u32(uint32_t x) {
  return x != 0u && (x & (x - 1u)) == 0u;
}

/*---------------------------------------------------------------------------*/

PlArena *pl_arena_create(uint32_t *size_p, uint32_t *cap_p) {
  // resolve defaults
  uint32_t size = (size_p == NULL || *size_p < 1024) ? 1024 : *size_p;
  uint32_t cap  = (cap_p  == NULL || *cap_p  < 1048000) ? 1048000 : *cap_p; // ~1MB

  // Validate alignment
  if (!pl_is_power_of_two_u32((uint32_t)alignof(max_align_t))) return NULL;

  // defensive
  if (size > cap) size = cap;

  PlArena *ar = xmalloc(sizeof(*ar));
  ar->data = xmalloc(size);

  ar->cap = cap;
  ar->curr_sz = size;
  ar->used = 0;

  return ar;
}

/* Frees the memory used by 'ar' and its data. */
void pl_arena_destroy(PlArena *ar) {
  if (!ar) return;
  free(ar->data);
  ar->data = NULL;
  ar->used = 0;
  ar->cap = 0;
  ar->curr_sz = 0;
  free(ar);
}

static int pl_arena_grow(PlArena *ar, uint32_t min_needed) {
  if (!ar) return ERR;
  if (min_needed > ar->cap) return ERR;

  uint32_t new_sz = ar->curr_sz;
  if (new_sz == 0) new_sz = 1024u;

  while (new_sz < min_needed) {
    // grow by doubling but never past cap
    if (new_sz > ar->cap / 2u) {
      new_sz = ar->cap;
    } else {
      new_sz *= 2u;
    }
  }

  uint8_t *new_data = realloc(ar->data, new_sz);
  if (!new_data) return ERR;

  ar->data = new_data;
  ar->curr_sz = new_sz;
  return OK;
}

/* Ensure 'extra' bytes available, growing if needed.
 * Returns OK on success, ERR if cap reached or errors occurred. */
int pl_arena_ensure(PlArena *ar, uint32_t extra) {
  if (!ar) return ERR;

  // overflow guard: used + extra
  if (extra > UINT32_MAX - ar->used) return ERR;

  uint32_t needed = ar->used + extra;
  if (needed <= ar->curr_sz) return OK;

  return pl_arena_grow(ar, needed);
}

/* Adds 'len' bytes starting from 'start_v' to the data of 'ar'.
 * Returns the offset from the start of data where 'str' is stored.
 * Returns UINT32_MAX if error. */
uint32_t pl_arena_add(PlArena *ar, void *start_v, uint32_t len) {
  if (!ar) return UINT32_MAX;

  // allow empty payloads
  static const uint8_t empty = 0;
  const uint8_t *start = (const uint8_t *)start_v;
  if (len == 0) start = &empty;
  if (!start) return UINT32_MAX;

  // entry layout: [u32 len][payload len bytes][0][padding...]
  // padding aligns *next* entry start
  const uint32_t data_sz = len + 1u;
  const uint32_t raw_entry_sz = (uint32_t)sizeof(uint32_t) + data_sz;

  // Align entry size to PL_ARENA_ALIGNMENT
  const uint32_t entry_sz = pl_align_up_u32(raw_entry_sz, (uint32_t)alignof(max_align_t));
  const uint32_t padding = entry_sz - raw_entry_sz;

  if (pl_arena_ensure(ar, entry_sz) != OK) return UINT32_MAX;

  const uint32_t offset = ar->used;
  uint8_t *p = ar->data + ar->used;

  // write len
  memcpy(p, &len, sizeof(uint32_t));
  p += sizeof(uint32_t);

  // write payload
  if (len != 0) memcpy(p, start, len);
  p[len] = 0; // terminator
  p += data_sz;

  // zero padding
  if (padding) memset(p, 0, padding);

  ar->used += entry_sz;
  return offset;
}

/* Returns a PlObject that point to the the 'offset' of 'ar''s data.
 * On error returns {0, NULL}. */
PlObject pl_arena_get(PlArena *ar, uint32_t offset) {
  PlObject obj = {0u, NULL};
  if (!ar) return obj;

  // Out of bounds
  if (offset > ar->used) return obj;
  if (ar->used - offset < (uint32_t)sizeof(uint32_t)) return obj;

  uint8_t *p = ar->data + offset;

  uint32_t len = 0;
  memcpy(&len, p, sizeof(uint32_t));
  p += sizeof(uint32_t);

  // Must contain payload + trailing 0
  if ((uint64_t)offset + sizeof(uint32_t) + (uint64_t)len + 1ull > (uint64_t)ar->used) {
    return obj;
  }

  obj.len = len;
  obj.v = p;
  return obj;
}

void *pl_arena_getv(PlArena *ar, uint32_t offset) {
  if (!ar) return NULL;
  if (offset > ar->used) return NULL;
  if (ar->used - offset < (uint32_t)sizeof(uint32_t)) return NULL;

  return ar->data + offset + sizeof(uint32_t);
}

/* Returns the number of bytes used by the data inside 'ar'. */
uint32_t pl_arena_get_used(PlArena *ar) {
  if (!ar) return 0u;
  return ar->used;
}
