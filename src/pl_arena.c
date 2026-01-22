#include <stdlib.h>
#include <string.h>
#include <stdalign.h>
#include <stddef.h>

#include "pl_arena.h"
#include "utils.h"

/*------------------------------------ helpers ------------------------------*/
static inline uint32_t pl_align_up_u32(uint32_t n, uint32_t a) {
  // a must be power-of-two for this fast path
  return (n + (a - 1u)) & ~(a - 1u);
}

static inline int pl_is_power_of_two_u32(uint32_t x) {
  return x != 0u && (x & (x - 1u)) == 0u;
}

typedef struct PlArenaBlock {
  struct PlArenaBlock *next;
  uint32_t used;
  uint32_t cap;
  alignas(max_align_t) uint8_t data[];
} PlArenaBlock;

/*---------------------------------------------------------------------------*/

/* Allocates a new block with capacity 'cap'. */
static PlArenaBlock *pl_arena_block_create(uint32_t cap) {
  PlArenaBlock *b = xmalloc(sizeof(*b) + cap);
  b->next = NULL;
  b->used = 0;
  b->cap = cap;
  return b;
}

int pl_arena_init(PlArena *ar, uint32_t *size_p, uint32_t *cap_p) {
  if (!ar) return ERR;

  // resolve defaults
  uint32_t size = (size_p == NULL || *size_p == 0) ? 1024u : *size_p; // ~1KB
  uint32_t cap  = (cap_p  == NULL || *cap_p  == 0) ? 1048000u : *cap_p; // ~1MB

  // Validate alignment
  if (!pl_is_power_of_two_u32((uint32_t)alignof(max_align_t))) return ERR;

  // defensive
  if (size > cap) size = cap;

  ar->head = pl_arena_block_create(size);
  ar->tail = ar->head;
  ar->used = 0;
  ar->cap = cap;
  ar->block_sz = size;

  return OK;
}

PlArena *pl_arena_create(uint32_t *size_p, uint32_t *cap_p) {
  PlArena *ar = xmalloc(sizeof(*ar));
  if (pl_arena_init(ar, size_p, cap_p) != OK) {
    free(ar);
    return NULL;
  }
  return ar;
}

/* Frees the memory used by 'ar' and its data. */
void pl_arena_destroy(PlArena *ar) {
  if (!ar) return;
  pl_arena_clean(ar);
  free(ar);
}

void pl_arena_clean(PlArena *ar) {
  if (!ar) return;
  PlArenaBlock *b = ar->head;
  while (b) {
    PlArenaBlock *next = b->next;
    free(b);
    b = next;
  }
  ar->head = NULL;
  ar->tail = NULL;
  ar->used = 0;
  ar->cap = 0;
  ar->block_sz = 0;
}

/* Ensure 'extra' bytes available, growing by adding blocks if needed.
 * Returns OK on success, ERR if cap reached or errors occurred. */
int pl_arena_ensure(PlArena *ar, uint32_t extra) {
  if (!ar) return ERR;

  // overflow guard: used + extra
  if (extra > UINT32_MAX - ar->used) return ERR;

  uint32_t needed = ar->used + extra;
  if (needed > ar->cap) return ERR;

  // If the current block has space, we're done.
  if (ar->tail && (ar->tail->cap - ar->tail->used) >= extra) return OK;

  uint32_t remaining = ar->cap - ar->used;
  if (remaining < extra) return ERR;
  
  // Blocks keep doubling in size each time one is added
  uint32_t new_sz = ar->block_sz;
  if (new_sz > UINT32_MAX / 2u) return ERR;
  new_sz *= 2u;
  while (new_sz < extra && new_sz < remaining) {
    if (new_sz > UINT32_MAX / 2u) break;
    new_sz *= 2u;
  }
  if (new_sz < extra) new_sz = extra;
  if (new_sz > remaining) new_sz = remaining;
  if (new_sz < extra) return ERR;

  PlArenaBlock *nb = pl_arena_block_create(new_sz);
  ar->tail->next = nb;
  ar->tail = nb;
  ar->block_sz = new_sz;
  return OK;
}

/* Allocates 'len' bytes inside the arena and returns the payload pointer.
 * The payload is zero-initialized and NUL-terminated. */
void *pl_arena_alloc(PlArena *ar, uint32_t len) {
  if (!ar) return NULL;

  // entry layout: [u32 len][align pad][payload len bytes][0][padding...]
  // padding aligns *next* entry start
  const uint32_t header_sz = pl_align_up_u32(
      (uint32_t)sizeof(uint32_t), (uint32_t)alignof(max_align_t));
  const uint32_t data_sz = len + 1u;
  const uint32_t raw_entry_sz = header_sz + data_sz;

  // Align entry size to PL_ARENA_ALIGNMENT
  const uint32_t entry_sz = pl_align_up_u32(raw_entry_sz, (uint32_t)alignof(max_align_t));
  const uint32_t padding = entry_sz - raw_entry_sz;

  if (pl_arena_ensure(ar, entry_sz) != OK) return NULL;

  uint8_t *p = ar->tail->data + ar->tail->used;

  // write len
  memcpy(p, &len, sizeof(uint32_t));
  p += sizeof(uint32_t);
  if (header_sz > sizeof(uint32_t)) {
    memset(p, 0, header_sz - sizeof(uint32_t));
    p += header_sz - sizeof(uint32_t);
  }

  uint8_t *payload = p;
  // write payload
  if (len != 0) memset(payload, 0, len);
  payload[len] = 0; // terminator
  p = payload + data_sz;

  // zero padding
  if (padding) memset(p, 0, padding);

  ar->tail->used += entry_sz;
  ar->used += entry_sz;
  return (void *)payload;
}

/* Adds 'len' bytes starting from 'start_v' to the data of 'ar'.
 * Returns a pointer to the stored payload on success, NULL on error. */
void *pl_arena_add(PlArena *ar, void *start_v, uint32_t len) {
  if (!ar) return NULL;
  if (!start_v && len != 0) return NULL;

  uint8_t *payload = (uint8_t *)pl_arena_alloc(ar, len);
  if (!payload) return NULL;
  if (len != 0) memcpy(payload, start_v, len);
  return payload;
}

/* Returns the number of bytes used by the data inside 'ar'. */
uint32_t pl_arena_get_used(PlArena *ar) {
  if (!ar) return 0u;
  return ar->used;
}
