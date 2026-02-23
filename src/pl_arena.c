#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "pl_arena.h"
#include "utils.h"

/*------------------------------------ helpers ------------------------------*/
#define PL_ARENA_ALIGN ((uint32_t)sizeof(uintptr_t))

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
  _Alignas(sizeof(uintptr_t)) uint8_t data[];
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

/* Initializes an arena in-place.
 * Ownership: caller retains the arena object and must call pl_arena_clean().
 * Side effects: allocates the first block.
 * Returns OK on success, ERR on bad input or allocation failure. */
int pl_arena_init(PlArena *ar, uint32_t *size_p, uint32_t *cap_p) {
  if (!ar)
    return ERR;

  // resolve defaults
  uint32_t size = (size_p == NULL || *size_p == 0) ? 2024u : *size_p; // ~2KB
  uint32_t cap = (cap_p == NULL || *cap_p == 0) ? 2048000u : *cap_p;  // ~2MB

  // Validate alignment
  if (!pl_is_power_of_two_u32(PL_ARENA_ALIGN))
    return ERR;

  // defensive
  if (size > cap)
    size = cap;

  ar->head = pl_arena_block_create(size);
  ar->tail = ar->head;
  ar->used = 0;
  ar->cap = cap;
  ar->block_sz = size;

  return OK;
}

/* Allocates and initializes a heap-owned arena.
 * Ownership: caller owns the returned arena and must call pl_arena_destroy().
 * Side effects: allocates heap blocks.
 * Returns NULL on error. */
PlArena *pl_arena_create(uint32_t *size_p, uint32_t *cap_p) {
  PlArena *ar = xmalloc(sizeof(*ar));
  if (pl_arena_init(ar, size_p, cap_p) != OK) {
    free(ar);
    return NULL;
  }
  return ar;
}

/* Frees the memory used by 'ar' and its data. */
/* Frees the arena and all blocks it owns.
 * Ownership: invalidates 'ar' for further use.
 * Side effects: frees memory.
 * Returns void. */
void pl_arena_destroy(PlArena *ar) {
  if (!ar)
    return;
  pl_arena_clean(ar);
  free(ar);
}

/* Frees all blocks owned by the arena but keeps the arena object.
 * Ownership: caller retains 'ar' for reuse.
 * Side effects: frees memory.
 * Returns void. */
void pl_arena_clean(PlArena *ar) {
  if (!ar)
    return;
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

int pl_arena_is_zeroed(const PlArena *ar) {
  if (!ar)
    return ERR;
  if (ar->head || ar->tail || ar->used != 0 || ar->cap != 0 ||
      ar->block_sz != 0) {
    return NO;
  }
  return YES;
}

int pl_arena_is_ok(const PlArena *ar) {
  if (!ar)
    return ERR;

  int zeroed = pl_arena_is_zeroed(ar);
  if (zeroed == YES)
    return NO;
  if (zeroed == ERR)
    return ERR;

  if (!ar->head || !ar->tail)
    return NO;
  if (ar->cap == 0 || ar->block_sz == 0)
    return NO;
  if (ar->used > ar->cap)
    return NO;

  const PlArenaBlock *b = ar->head;
  const PlArenaBlock *last = NULL;
  uint64_t used_sum = 0;
  while (b) {
    if (b->cap == 0)
      return NO;
    if (b->used > b->cap)
      return NO;

    used_sum += b->used;
    if (used_sum > ar->cap)
      return NO;

    last = b;
    b = b->next;
  }

  if (last != ar->tail)
    return NO;
  if (used_sum != ar->used)
    return NO;

  return YES;
}

/* Ensure 'extra' bytes available, growing by adding blocks if needed.
 * Returns OK on success, ERR if cap reached or errors occurred. */
/* Ensures 'extra' bytes are available, growing by adding a block.
 * Ownership: arena retains ownership of blocks.
 * Side effects: may allocate a new block.
 * Returns OK on success, ERR on cap/overflow. */
int pl_arena_ensure(PlArena *ar, uint32_t extra) {
  if (!ar)
    return ERR;

  // overflow guard: used + extra
  if (extra > UINT32_MAX - ar->used)
    return ERR;

  uint32_t needed = ar->used + extra;
  if (needed > ar->cap)
    return ERR;

  // If the current block has space, we're done.
  if (ar->tail && (ar->tail->cap - ar->tail->used) >= extra)
    return OK;

  uint32_t remaining = ar->cap - ar->used;
  if (remaining < extra)
    return ERR;

  // Blocks keep doubling in size each time one is added
  uint32_t new_sz = ar->block_sz;
  if (new_sz > UINT32_MAX / 2u)
    return ERR;
  new_sz *= 2u;
  while (new_sz < extra && new_sz < remaining) {
    if (new_sz > UINT32_MAX / 2u)
      break;
    new_sz *= 2u;
  }
  if (new_sz < extra)
    new_sz = extra;
  if (new_sz > remaining)
    new_sz = remaining;
  if (new_sz < extra)
    return ERR;

  PlArenaBlock *nb = pl_arena_block_create(new_sz);
  ar->tail->next = nb;
  ar->tail = nb;
  ar->block_sz = new_sz;
  return OK;
}

/* Bumps the arena cursor by 'len' bytes (aligned up to PL_ARENA_ALIGN)
 * and returns a pointer to the start of the reserved region.
 * Padding bytes between 'len' and the aligned size are zeroed.
 * Returns NULL on invalid input, overflow, or capacity failure.
 */
static inline uint8_t *pl_arena_reserve(PlArena *ar, uint32_t len) {
  if (!ar)
    return NULL;

  if (len > UINT32_MAX - PL_ARENA_ALIGN)
    return NULL;

  const uint32_t entry_sz = pl_align_up_u32(len, PL_ARENA_ALIGN);

  if (pl_arena_ensure(ar, entry_sz) != OK)
    return NULL;

  uint8_t *payload = ar->tail->data + ar->tail->used;

  uint32_t padding = entry_sz - len;
  if (padding)
    memset(payload + len, 0, padding);

  ar->tail->used += entry_sz;
  ar->used += entry_sz;
  return payload;
}

void *pl_arena_alloc(PlArena *ar, uint32_t len) {
  return (void *)pl_arena_reserve(ar, len);
}

void *pl_arena_calloc(PlArena *ar, uint32_t len) {
  uint8_t *payload = pl_arena_reserve(ar, len);
  if (!payload)
    return NULL;
  if (len != 0)
    memset(payload, 0, len);
  return payload;
}

void *pl_arena_add(PlArena *ar, void *start_v, uint32_t len) {
  if (!start_v && len != 0)
    return NULL;

  uint8_t *payload = pl_arena_reserve(ar, len);
  if (!payload)
    return NULL;
  if (len != 0)
    memcpy(payload, start_v, len);
  return payload;
}

void *pl_arena_add_nul(PlArena *ar, void *start_v, uint32_t len) {
  if (!start_v && len != 0)
    return NULL;

  if (len > UINT32_MAX - 1u)
    return NULL;

  uint8_t *payload = pl_arena_reserve(ar, len + 1u);
  if (!payload)
    return NULL;
  if (len != 0)
    memcpy(payload, start_v, len);
  payload[len] = '\0';
  return payload;
}

uint32_t pl_arena_get_used(PlArena *ar) {
  if (!ar)
    return 0u;
  return ar->used;
}

int ptrvec_push(PtrVec *v, void *ptr) {
  if (!v)
    return ERR;
  if (v->len + 1 > v->cap) {
    uint32_t new_cap = v->cap ? v->cap * 2u : 8u;
    void **new_items =
        (void **)xrealloc(v->items, new_cap * sizeof(*new_items));
    if (!new_items)
      return ERR;
    v->items = new_items;
    v->cap = new_cap;
  }
  v->items[v->len++] = ptr;
  return OK;
}

void **ptrvec_flatten(PtrVec *v, PlArena *a) {
  if (!v || !a || v->len == 0)
    return NULL;
  void **arr = (void **)pl_arena_alloc(a, (uint32_t)(v->len * sizeof(*arr)));
  if (!arr)
    return NULL;
  memcpy(arr, v->items, v->len * sizeof(*arr));
  return arr;
}

void ptrvec_clean(PtrVec *v) {
  if (!v)
    return;
  free(v->items);
  v->items = NULL;
  v->len = 0;
  v->cap = 0;
}
