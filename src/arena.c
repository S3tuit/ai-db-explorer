#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "arena.h"
#include "utils.h"

/*------------------------------------ helpers ------------------------------*/
#define ARENA_ALIGN ((uint32_t)sizeof(uintptr_t))

static inline uint32_t arena_align_up_u32(uint32_t n, uint32_t a) {
  // a must be power-of-two for this fast path
  return (n + (a - 1u)) & ~(a - 1u);
}

static inline int arena_is_power_of_two_u32(uint32_t x) {
  return x != 0u && (x & (x - 1u)) == 0u;
}

typedef struct ArenaBlock {
  struct ArenaBlock *next;
  uint32_t used;
  uint32_t cap;
  _Alignas(sizeof(uintptr_t)) uint8_t data[];
} ArenaBlock;

/*---------------------------------------------------------------------------*/

/* Allocates a new block with capacity 'cap'. */
static ArenaBlock *arena_block_create(uint32_t cap) {
  ArenaBlock *b = xmalloc(sizeof(*b) + cap);
  b->next = NULL;
  b->used = 0;
  b->cap = cap;
  return b;
}

/* Initializes an arena in-place.
 * Ownership: caller retains the arena object and must call arena_clean().
 * Side effects: allocates the first block.
 * Returns OK on success, ERR on bad input or allocation failure. */
AdbxStatus arena_init(Arena *ar, uint32_t *size_p, uint32_t *cap_p) {
  if (!ar)
    return ERR;

  // resolve defaults
  uint32_t size = (size_p == NULL || *size_p == 0) ? 2024u : *size_p; // ~2KB
  uint32_t cap = (cap_p == NULL || *cap_p == 0) ? 2048000u : *cap_p;  // ~2MB

  // Validate alignment
  if (!arena_is_power_of_two_u32(ARENA_ALIGN))
    return ERR;

  // defensive
  if (size > cap)
    size = cap;

  ar->head = arena_block_create(size);
  ar->tail = ar->head;
  ar->used = 0;
  ar->cap = cap;
  ar->block_sz = size;

  return OK;
}

/* Allocates and initializes a heap-owned arena.
 * Ownership: caller owns the returned arena and must call arena_destroy().
 * Side effects: allocates heap blocks.
 * Returns NULL on error. */
Arena *arena_create(uint32_t *size_p, uint32_t *cap_p) {
  Arena *ar = xmalloc(sizeof(*ar));
  if (arena_init(ar, size_p, cap_p) != OK) {
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
void arena_destroy(Arena *ar) {
  if (!ar)
    return;
  arena_clean(ar);
  free(ar);
}

/* Frees all blocks owned by the arena but keeps the arena object.
 * Ownership: caller retains 'ar' for reuse.
 * Side effects: frees memory.
 * Returns void. */
void arena_clean(Arena *ar) {
  if (!ar)
    return;
  ArenaBlock *b = ar->head;
  while (b) {
    ArenaBlock *next = b->next;
    free(b);
    b = next;
  }
  ar->head = NULL;
  ar->tail = NULL;
  ar->used = 0;
  ar->cap = 0;
  ar->block_sz = 0;
}

AdbxTriStatus arena_is_zeroed(const Arena *ar) {
  if (!ar)
    return ERR;
  if (ar->head || ar->tail || ar->used != 0 || ar->cap != 0 ||
      ar->block_sz != 0) {
    return NO;
  }
  return YES;
}

AdbxTriStatus arena_is_ok(const Arena *ar) {
  if (!ar)
    return ERR;

  AdbxTriStatus zeroed = arena_is_zeroed(ar);
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

  const ArenaBlock *b = ar->head;
  const ArenaBlock *last = NULL;
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
AdbxStatus arena_ensure(Arena *ar, uint32_t extra) {
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

  ArenaBlock *nb = arena_block_create(new_sz);
  ar->tail->next = nb;
  ar->tail = nb;
  ar->block_sz = new_sz;
  return OK;
}

/* Bumps the arena cursor by 'len' bytes (aligned up to ARENA_ALIGN)
 * and returns a pointer to the start of the reserved region.
 * Padding bytes between 'len' and the aligned size are zeroed.
 * Returns NULL on invalid input, overflow, or capacity failure.
 */
static inline uint8_t *arena_reserve(Arena *ar, uint32_t len) {
  if (!ar)
    return NULL;

  if (len > UINT32_MAX - ARENA_ALIGN)
    return NULL;

  const uint32_t entry_sz = arena_align_up_u32(len, ARENA_ALIGN);

  if (arena_ensure(ar, entry_sz) != OK)
    return NULL;

  uint8_t *payload = ar->tail->data + ar->tail->used;

  uint32_t padding = entry_sz - len;
  if (padding)
    memset(payload + len, 0, padding);

  ar->tail->used += entry_sz;
  ar->used += entry_sz;
  return payload;
}

void *arena_alloc(Arena *ar, uint32_t len) {
  return (void *)arena_reserve(ar, len);
}

void *arena_calloc(Arena *ar, uint32_t len) {
  uint8_t *payload = arena_reserve(ar, len);
  if (!payload)
    return NULL;
  if (len != 0)
    memset(payload, 0, len);
  return payload;
}

void *arena_add(Arena *ar, void *start_v, uint32_t len) {
  if (!start_v && len != 0)
    return NULL;

  uint8_t *payload = arena_reserve(ar, len);
  if (!payload)
    return NULL;
  if (len != 0)
    memcpy(payload, start_v, len);
  return payload;
}

void *arena_add_nul(Arena *ar, void *start_v, uint32_t len) {
  if (!start_v && len != 0)
    return NULL;

  if (len > UINT32_MAX - 1u)
    return NULL;

  uint8_t *payload = arena_reserve(ar, len + 1u);
  if (!payload)
    return NULL;
  if (len != 0)
    memcpy(payload, start_v, len);
  payload[len] = '\0';
  return payload;
}

uint32_t arena_get_used(Arena *ar) {
  if (!ar)
    return 0u;
  return ar->used;
}

AdbxStatus ptrvec_push(PtrVec *v, void *ptr) {
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

void **ptrvec_flatten(PtrVec *v, Arena *a) {
  if (!v || !a || v->len == 0)
    return NULL;
  void **arr = (void **)arena_alloc(a, (uint32_t)(v->len * sizeof(*arr)));
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
