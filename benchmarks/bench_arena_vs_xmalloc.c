/* Benchmark: malloc+memcpy vs arena_alloc+memcpy for bulk short-lived
 * allocations — the pattern used throughout ai-db-explorer (query IR nodes,
 * result sets, etc.) where many small objects share a lifetime and are freed
 * together.
 *
 * Both paths allocate OBJ_SIZE bytes and memcpy a payload into them.
 * Bulk free is deferred to the per-round cleanup (untimed), mirroring real
 * usage: arena_destroy for the arena, individual free() calls for malloc.
 *
 * The malloc path stores each returned pointer into a pre-allocated array
 * (ptrs[idx++] = p) so it can free them in cleanup.  The arena path doesn't
 * need this bookkeeping.  This slightly favors the arena, but the extra cost
 * is a sequential write to a hot cacheline (~1 ns) and reflects the real
 * overhead: without an arena you must track pointers to free them later.
 *
 * Result: on my machine, arena is ~5x faster than malloc for this workload. */

#include "adbx_bench.h"

#include "arena.h"
#include "utils.h"

#include <string.h>

#define OBJ_SIZE 32u

static const uint8_t payload[OBJ_SIZE] = {
    0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x01, 0x02, 0x03,
    0x04, 0x05, 0x06, 0x07, 0x08, 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE,
    0xBA, 0xBE, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
};

/* ── xmalloc + memcpy (bulk free in cleanup) ─────────────────────────── */

typedef struct {
  void **ptrs;
  int idx;
  int ops;
} XmallocCtx;

static void xmalloc_setup(void *ctx) {
  XmallocCtx *c = (XmallocCtx *)ctx;
  c->ptrs = (void **)malloc((size_t)c->ops * sizeof(void *));
  c->idx = 0;
}

static void xmalloc_cleanup(void *ctx) {
  XmallocCtx *c = (XmallocCtx *)ctx;
  for (int i = 0; i < c->idx; i++)
    free(c->ptrs[i]);
  free(c->ptrs);
  c->ptrs = NULL;
  c->idx = 0;
}

static void bench_xmalloc_memcpy(void *ctx) {
  XmallocCtx *c = (XmallocCtx *)ctx;
  void *p = xmalloc(OBJ_SIZE);
  memcpy(p, payload, OBJ_SIZE);
  adbx_bench_use(p);
  c->ptrs[c->idx++] = p;
}

/* ── arena_alloc + memcpy ─────────────────────────────────────────────── */

typedef struct {
  Arena *ar;
} ArenaCtx;

static void arena_setup(void *ctx) {
  ArenaCtx *c = (ArenaCtx *)ctx;
  c->ar = arena_create(NULL, NULL);
}

static void arena_cleanup(void *ctx) {
  ArenaCtx *c = (ArenaCtx *)ctx;
  arena_destroy(c->ar);
  c->ar = NULL;
}

static void bench_arena_alloc(void *ctx) {
  ArenaCtx *c = (ArenaCtx *)ctx;
  void *p = arena_alloc(c->ar, OBJ_SIZE);
  memcpy(p, payload, OBJ_SIZE);
  adbx_bench_use(p);
}

/* ── main ────────────────────────────────────────────────────────────── */

#define WARMUP 3
#define ROUNDS 5000
#define OPS 500

int main(void) {
  printf("bench_arena_vs_xmalloc  (%d warmup, %d rounds x %d ops)\n\n", WARMUP,
         ROUNDS, OPS);

  XmallocCtx mctx = {.ops = OPS};
  ADBX_BENCH_F("xmalloc+memcpy 32B", WARMUP, ROUNDS, OPS, xmalloc_setup,
               xmalloc_cleanup, bench_xmalloc_memcpy, &mctx);

  ArenaCtx actx = {0};
  ADBX_BENCH_F("arena_alloc 32B", WARMUP, ROUNDS, OPS, arena_setup,
               arena_cleanup, bench_arena_alloc, &actx);

  return 0;
}
