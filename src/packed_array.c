#include "packed_array.h"
#include "utils.h"
#include "string_op.h"

#include <string.h>
#include <stdlib.h>
#include <stdalign.h>
#include <stdint.h>

struct PackedArray {
    unsigned char *buf;     /* raw storage */
    size_t len;             /* live objects */
    size_t cap;             /* capacity in objects */
    size_t obj_sz;          /* requested object size */
    size_t stride;          /* padded size per slot */
    size_t alignment;       /* slot alignment */
    size_t max_bytes;       /* hard cap on buffer size in bytes */

    parr_cleanup_fn cleanup;
    void *cleanup_ctx;
};

static size_t round_up(size_t n, size_t multiple) {
    if (multiple == 0) return n;
    size_t rem = n % multiple;
    if (rem == 0) return n;
    return n + (multiple - rem);
}

static void *slot_ptr(PackedArray *a, size_t idx) {
    return (void *)(a->buf + (idx * a->stride));
}

static const void *slot_cptr(const PackedArray *a, size_t idx) {
    return (const void *)(a->buf + (idx * a->stride));
}

static int ensure_cap(PackedArray *a, size_t min_cap) {
    if (!a) return ERR;
    if (a->cap >= min_cap) return OK;

    size_t new_cap = (a->cap == 0) ? 1 : a->cap;
    while (new_cap < min_cap) {
        /* Doubling keeps amortized O(1) append and is easy to reason about. */
        if (new_cap > SIZE_MAX / 2) {
            new_cap = min_cap;
            break;
        }
        new_cap *= 2;
    }

    if (a->stride == 0 || new_cap > SIZE_MAX / a->stride) return ERR;
    size_t new_bytes = new_cap * a->stride;
    if (new_bytes > a->max_bytes) return ERR;

    unsigned char *p = (unsigned char *)xrealloc(a->buf, new_bytes);
    a->buf = p;
    a->cap = new_cap;
    return OK;
}

static int parr_validate_index(const PackedArray *a, size_t idx) {
    if (!a) return NO;
    return (idx < a->len) ? YES : NO;
}

static int parr_is_usable(const PackedArray *a) {
    if (!a) return NO;
    if (!a->buf && a->cap > 0) return NO;
    if (a->stride == 0) return NO;
    return YES;
}

static int parr_init(PackedArray *a, size_t obj_sz, size_t upper_bound) {
    if (!a || obj_sz == 0 || upper_bound == 0) return ERR;

    a->len = 0;
    a->cap = 0;
    a->obj_sz = obj_sz;
    a->max_bytes = upper_bound;

    /* A pragmatic default: support storing any normal C object safely. */
    a->alignment = alignof(max_align_t);
    a->stride = round_up(obj_sz, a->alignment);
    if (a->stride == 0) return ERR;

    a->cleanup = NULL;
    a->cleanup_ctx = NULL;
    a->buf = NULL;

    size_t max_cap = a->max_bytes / a->stride;
    size_t init_cap = (max_cap < 16) ? max_cap : 16;
    if (init_cap > 0 && ensure_cap(a, init_cap) != OK) return ERR;
    return OK;
}

PackedArray *parr_create(size_t obj_sz) {
    return parr_create_upper_bound(obj_sz, STRBUF_MAX_BYTES);
}

PackedArray *parr_create_upper_bound(size_t obj_sz, size_t upper_bound) {
    if (obj_sz == 0) return NULL;

    PackedArray *a = (PackedArray *)xmalloc(sizeof(PackedArray));
    if (parr_init(a, obj_sz, upper_bound) != OK) {
        free(a);
        return NULL;
    }
    return a;
}

void parr_set_cleanup(PackedArray *a, parr_cleanup_fn cleanup, void *cleanup_ctx) {
    if (!a) return;
    a->cleanup = cleanup;
    a->cleanup_ctx = cleanup_ctx;
}

size_t parr_len(const PackedArray *a) {
    if (!a) return 0;
    return a->len;
}

void *parr_at(PackedArray *a, size_t idx) {
    if (!parr_is_usable(a)) return NULL;
    if (parr_validate_index(a, idx) != YES) return NULL;
    return slot_ptr(a, idx);
}

const void *parr_cat(const PackedArray *a, size_t idx) {
    if (!parr_is_usable(a)) return NULL;
    if (parr_validate_index(a, idx) != YES) return NULL;
    return slot_cptr(a, idx);
}

size_t parr_emplace(PackedArray *a, void **out_ptr) {
    if (out_ptr) *out_ptr = NULL;
    if (!parr_is_usable(a)) return SIZE_MAX;
    if (ensure_cap(a, a->len + 1) != OK) return SIZE_MAX;

    size_t idx = a->len;
    void *ptr = slot_ptr(a, idx);

    /* Intentionally uninitialized: caller owns initialization. */
    a->len++;

    if (out_ptr) *out_ptr = ptr;
    return idx;
}

void parr_drop_swap(PackedArray *a, size_t idx) {
    if (!parr_is_usable(a)) return;
    if (parr_validate_index(a, idx) != YES) return;

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
    if (!a) return;

    if (a->cleanup) {
        for (size_t i = 0; i < a->len; ++i) {
            void *obj = slot_ptr(a, i);
            a->cleanup(obj, a->cleanup_ctx);
        }
    }

    free(a->buf);
    free(a);
}
