#include "packed_array.h"
#include "string_op.h"
#include "utils.h"

#include <stdalign.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct PackedArray {
  unsigned char *buf; /* raw storage */
  size_t len;         /* live objects */
  size_t cap;         /* capacity in objects */
  size_t obj_sz;      /* requested object size */
  size_t stride;      /* padded size per slot */
  size_t alignment;   /* slot alignment */
  size_t max_bytes;   /* hard cap on buffer size in bytes */

  parr_cleanup_fn cleanup;
  void *cleanup_ctx;
};

/* Rounds 'n' up to the next multiple of 'multiple'.
 * It borrows all inputs and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns 0 on overflow or invalid multiple; otherwise
 * returns the rounded value.
 */
static size_t round_up(size_t n, size_t multiple) {
  if (multiple == 0)
    return 0;
  size_t rem = n % multiple;
  if (rem == 0)
    return n;
  size_t add = multiple - rem;
  if (n > SIZE_MAX - add)
    return 0;
  return n + add;
}

static inline void *slot_ptr(PackedArray *a, size_t idx) {
  return (void *)(a->buf + (idx * a->stride));
}

static inline const void *slot_cptr(const PackedArray *a, size_t idx) {
  return (const void *)(a->buf + (idx * a->stride));
}

/* Ensures capacity for at least 'min_cap' objects.
 * It borrows 'a' and may replace its owned buffer.
 * Side effects: may grow internal storage and update cap.
 * Error semantics: returns OK on success, ERR on invalid input, arithmetic
 * overflow, or when min_cap exceeds configured max_bytes bound.
 */
static int ensure_cap(PackedArray *a, size_t min_cap) {
  if (!a)
    return ERR;
  if (a->cap >= min_cap)
    return OK;

  if (a->stride == 0)
    return ERR;
  size_t max_cap = a->max_bytes / a->stride;
  if (min_cap > max_cap)
    return ERR;

  size_t new_cap = (a->cap == 0) ? 1 : a->cap;
  while (new_cap < min_cap && new_cap <= max_cap / 2) {
    /* Doubling keeps amortized O(1) append and is easy to reason about. */
    new_cap *= 2;
  }
  if (new_cap < min_cap)
    new_cap = min_cap;
  size_t new_bytes = new_cap * a->stride;

  unsigned char *p = (unsigned char *)xrealloc(a->buf, new_bytes);
  a->buf = p;
  a->cap = new_cap;
  return OK;
}

static inline int parr_is_usable(const PackedArray *a) {
  if (!a)
    return NO;
  if (!a->buf && a->cap > 0)
    return NO;
  if (a->stride == 0)
    return NO;
  if (a->len > a->cap)
    return NO;
  return YES;
}

/* Returns YES when idx is a valid live index inside 'a'.
 * It borrows 'a' and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns YES for in-range index, NO otherwise.
 */
static inline int parr_idx_in_range(const PackedArray *a, size_t idx) {
  if (!a)
    return NO;
  return (idx < a->len) ? YES : NO;
}

static int parr_init(PackedArray *a, size_t obj_sz, size_t upper_bound) {
  if (!a || obj_sz == 0 || upper_bound == 0)
    return ERR;

  a->len = 0;
  a->cap = 0;
  a->obj_sz = obj_sz;
  a->max_bytes = upper_bound;

  /* A pragmatic default: support storing any normal C object safely. */
  a->alignment = alignof(max_align_t);
  a->stride = round_up(obj_sz, a->alignment);
  if (a->stride == 0)
    return ERR;

  a->cleanup = NULL;
  a->cleanup_ctx = NULL;
  a->buf = NULL;

  size_t max_cap = a->max_bytes / a->stride;
  size_t init_cap = (max_cap < 16) ? max_cap : 16;
  if (init_cap > 0 && ensure_cap(a, init_cap) != OK)
    return ERR;
  return OK;
}

PackedArray *parr_create(size_t obj_sz) {
  return parr_create_upper_bound(obj_sz, STRBUF_MAX_BYTES);
}

PackedArray *parr_create_upper_bound(size_t obj_sz, size_t upper_bound) {
  if (obj_sz == 0)
    return NULL;

  PackedArray *a = xmalloc(sizeof(*a));
  if (parr_init(a, obj_sz, upper_bound) != OK) {
    free(a);
    return NULL;
  }
  return a;
}

void parr_set_cleanup(PackedArray *a, parr_cleanup_fn cleanup,
                      void *cleanup_ctx) {
  if (!a)
    return;
  a->cleanup = cleanup;
  a->cleanup_ctx = cleanup_ctx;
}

size_t parr_len(const PackedArray *a) {
  if (!a)
    return 0;
  return a->len;
}

void *parr_at(PackedArray *a, size_t idx) {
  if (!parr_is_usable(a))
    return NULL;
  if (!parr_idx_in_range(a, idx))
    return NULL;
  return slot_ptr(a, idx);
}

const void *parr_cat(const PackedArray *a, size_t idx) {
  if (!parr_is_usable(a))
    return NULL;
  if (!parr_idx_in_range(a, idx))
    return NULL;
  return slot_cptr(a, idx);
}

size_t parr_emplace(PackedArray *a, void **out_ptr) {
  if (out_ptr)
    *out_ptr = NULL;
  if (!parr_is_usable(a))
    return SIZE_MAX;
  if (ensure_cap(a, a->len + 1) != OK)
    return SIZE_MAX;

  size_t idx = a->len;
  void *ptr = slot_ptr(a, idx);

  /* Intentionally uninitialized: caller owns initialization. */
  a->len++;

  if (out_ptr)
    *out_ptr = ptr;
  return idx;
}

void parr_drop_swap(PackedArray *a, size_t idx) {
  if (!parr_is_usable(a))
    return;
  if (!parr_idx_in_range(a, idx))
    return;

  void *victim = slot_ptr(a, idx);

  /* Cleanup happens while the object is still intact. */
  if (a->cleanup) {
    a->cleanup(victim, a->cleanup_ctx);
  }

  size_t last_idx = a->len - 1;
  if (idx != last_idx) {
    void *last = slot_ptr(a, last_idx);
    /* Copy only obj_sz bytes; padding is irrelevant. */
    memcpy(victim, last, a->obj_sz);
  }

  a->len--;
}

void parr_destroy(PackedArray *a) {
  if (!a)
    return;

  if (a->cleanup) {
    for (size_t i = 0; i < a->len; ++i) {
      void *obj = slot_ptr(a, i);
      a->cleanup(obj, a->cleanup_ctx);
    }
  }

  free(a->buf);
  free(a);
}
