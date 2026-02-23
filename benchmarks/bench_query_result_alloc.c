#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pl_arena.h"
#include "utils.h"

/* We use just one round for the benchmark because that's the closest scenario
 * to our arena use case. Our entities (QueryResult, QirQueryHandle) are
 * one-shot create-fill-destroy with different allocation patterns between them.
 */
#define BENCH_MEASURE_ROUNDS 1u
#define BENCH_OPS_PER_ROUND 250000u
#define BENCH_BLOCK_INIT_BYTES 4096u

typedef enum BenchStrategy {
  BENCH_XMALLOC = 0,
  BENCH_PL_ARENA_CALLOC = 1,
  BENCH_PL_ARENA_ALLOC = 2,
  BENCH_PL_ARENA_ADD = 3,
} BenchStrategy;

typedef struct BenchStats {
  uint64_t min_ns;
  uint64_t p50_ns;
} BenchStats;

/* Describes one benchmark case (strategy + display name). */
typedef struct BenchCase {
  BenchStrategy strat;
  const char *name;
} BenchCase;

/* Callback type for arena allocation strategies.
 * It borrows 'ar' and 'payload'; returns arena-owned pointer or NULL.
 */
typedef char *(*BenchArenaAllocFn)(PlArena *ar, const char *payload,
                                   uint32_t len);

static volatile uint64_t g_guard_sink = 0;

/* Returns current monotonic time in nanoseconds.
 * It borrows no pointers and allocates no memory.
 * Side effects: reads monotonic clock.
 * Error semantics: returns monotonic nanoseconds; exits on clock read failure.
 */
static uint64_t bench_now_ns(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    fprintf(stderr, "benchmark: clock_gettime failed\n");
    exit(1);
  }
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Compares two uint64 samples for qsort ordering.
 * It borrows 'a' and 'b' and allocates no memory.
 * Side effects: none.
 * Error semantics: returns <0/0/>0 following qsort comparator contract.
 */
static int bench_u64_cmp(const void *a, const void *b) {
  uint64_t va = *(const uint64_t *)a;
  uint64_t vb = *(const uint64_t *)b;
  if (va < vb)
    return -1;
  if (va > vb)
    return 1;
  return 0;
}

/* Returns the nearest-rank quantile index for [0..100] percentile.
 * It borrows no pointers and allocates no memory.
 * Side effects: none.
 * Error semantics: returns index in [0, n-1]; returns 0 when n == 0.
 */
static size_t bench_quantile_index(size_t n, unsigned pct) {
  if (n == 0)
    return 0;

  size_t rank = ((size_t)pct * n + 99u) / 100u; // ceil(pct*n/100)
  if (rank == 0)
    rank = 1;
  if (rank > n)
    rank = n;
  return rank - 1u;
}

/* Computes min/p50/p99 from round-latency samples.
 * It borrows 'samples' and writes into caller-owned 'out'.
 * Side effects: allocates and frees a temporary sortable copy.
 * Error semantics: returns OK on success, ERR on invalid input/allocation
 * failure.
 */
static int bench_compute_stats(const uint64_t *samples, size_t n,
                               BenchStats *out) {
  if (!samples || !out || n == 0)
    return ERR;

  uint64_t *sorted = (uint64_t *)malloc(n * sizeof(*sorted));
  if (!sorted)
    return ERR;
  memcpy(sorted, samples, n * sizeof(*sorted));
  qsort(sorted, n, sizeof(*sorted), bench_u64_cmp);

  out->min_ns = sorted[0];
  out->p50_ns = sorted[bench_quantile_index(n, 50u)];
  free(sorted);
  return OK;
}

/* Computes a safe arena cap for one benchmark round.
 * It borrows no pointers and allocates no memory.
 * Side effects: writes cap to caller-owned 'out_cap'.
 * Error semantics: returns OK on success, ERR on invalid input/overflow.
 */
static int bench_compute_arena_cap(uint32_t len, uint32_t ops,
                                   uint32_t *out_cap) {
  if (!out_cap || len == 0 || ops == 0)
    return ERR;

  // Include overhead slack (align padding + block metadata).
  uint64_t per_entry = (uint64_t)len + 64u;
  uint64_t total = (uint64_t)ops * per_entry;
  if (total > UINT32_MAX)
    return ERR;

  uint32_t cap = (uint32_t)total;
  if (cap < BENCH_BLOCK_INIT_BYTES)
    cap = BENCH_BLOCK_INIT_BYTES;
  *out_cap = cap;
  return OK;
}

/* Initializes a payload buffer with deterministic bytes.
 * It borrows caller-owned 'buf' and writes exactly 'len' bytes.
 * Side effects: mutates 'buf'.
 * Error semantics: returns OK on success, ERR on invalid input.
 */
static int bench_fill_payload(char *buf, uint32_t len) {
  if (!buf || len == 0)
    return ERR;
  for (uint32_t i = 0; i < len; i++) {
    buf[i] = (char)('a' + (i % 26u));
  }
  return OK;
}

/*---------------------------------------------------------------------------*/
/* Arena allocation callbacks                                                */
/*---------------------------------------------------------------------------*/

/* Arena callback: calloc + memcpy. */
static char *bench_fn_arena_calloc(PlArena *ar, const char *payload,
                                   uint32_t len) {
  char *p = (char *)pl_arena_calloc(ar, len);
  if (p)
    memcpy(p, payload, len);
  return p;
}

/* Arena callback: alloc + memcpy. */
static char *bench_fn_arena_alloc(PlArena *ar, const char *payload,
                                  uint32_t len) {
  char *p = (char *)pl_arena_alloc(ar, len);
  if (p)
    memcpy(p, payload, len);
  return p;
}

/* Arena callback: add (copy built-in). */
static char *bench_fn_arena_add(PlArena *ar, const char *payload,
                                uint32_t len) {
  return (char *)pl_arena_add(ar, (void *)payload, len);
}

/*---------------------------------------------------------------------------*/
/* Round runners                                                             */
/*---------------------------------------------------------------------------*/

/* Runs one xmalloc round: allocate/copy N strings, then free them.
 * When 'include_free' is non-zero the timing window covers the free loop too.
 * It borrows caller-owned 'ptrs' and writes elapsed ns to 'out_ns'.
 * Side effects: heap allocation/free for each element and guard accumulation.
 * Error semantics: returns OK on success, ERR on invalid input.
 */
static int bench_round_xmalloc(uint32_t len, uint32_t ops, const char *payload,
                               char **ptrs, int include_free,
                               uint64_t *out_ns) {
  if (!payload || !ptrs || !out_ns || len == 0 || ops == 0)
    return ERR;

  uint64_t checksum = 0;
  uint64_t t0 = bench_now_ns();
  for (uint32_t i = 0; i < ops; i++) {
    char *p = (char *)xmalloc((size_t)len + 1u);
    memcpy(p, payload, len);
    p[len] = '\0';
    checksum += (uint8_t)p[0];
    checksum += (uint8_t)p[len - 1u];
    ptrs[i] = p;
  }
  if (!include_free) {
    uint64_t t1 = bench_now_ns();
    for (uint32_t i = 0; i < ops; i++)
      free(ptrs[i]);
    g_guard_sink ^= checksum;
    *out_ns = t1 - t0;
  } else {
    for (uint32_t i = 0; i < ops; i++)
      free(ptrs[i]);
    uint64_t t1 = bench_now_ns();
    g_guard_sink ^= checksum;
    *out_ns = t1 - t0;
  }
  return OK;
}

/* Runs one arena round using the given allocation callback.
 * When 'include_free' is non-zero the timing window covers pl_arena_clean too.
 * It borrows 'payload' and writes elapsed ns to 'out_ns'.
 * Side effects: arena block allocations/frees and guard accumulation.
 * Error semantics: returns OK on success, ERR on invalid input/arena failure.
 */
static int bench_round_arena(uint32_t len, uint32_t ops, const char *payload,
                             BenchArenaAllocFn alloc_fn, int include_free,
                             uint64_t *out_ns) {
  if (!payload || !alloc_fn || !out_ns || len == 0 || ops == 0)
    return ERR;

  uint32_t cap = 0;
  if (bench_compute_arena_cap(len, ops, &cap) != OK)
    return ERR;
  uint32_t init_sz = BENCH_BLOCK_INIT_BYTES;
  if (init_sz > cap)
    init_sz = cap;

  PlArena ar = {0};
  if (pl_arena_init(&ar, &init_sz, &cap) != OK)
    return ERR;

  uint64_t checksum = 0;
  uint64_t t0 = bench_now_ns();
  for (uint32_t i = 0; i < ops; i++) {
    char *p = alloc_fn(&ar, payload, len);
    if (!p) {
      pl_arena_clean(&ar);
      return ERR;
    }
    checksum += (uint8_t)p[0];
    checksum += (uint8_t)p[len - 1u];
  }
  if (!include_free) {
    uint64_t t1 = bench_now_ns();
    pl_arena_clean(&ar);
    g_guard_sink ^= checksum;
    *out_ns = t1 - t0;
  } else {
    pl_arena_clean(&ar);
    uint64_t t1 = bench_now_ns();
    g_guard_sink ^= checksum;
    *out_ns = t1 - t0;
  }
  return OK;
}

/* Dispatches one round to the right strategy function. */
static int bench_dispatch_round(BenchStrategy strat, BenchArenaAllocFn arena_fn,
                                uint32_t len, uint32_t ops, const char *payload,
                                char **xmalloc_ptrs, int include_free,
                                uint64_t *out_ns) {
  if (strat == BENCH_XMALLOC)
    return bench_round_xmalloc(len, ops, payload, xmalloc_ptrs, include_free,
                               out_ns);
  return bench_round_arena(len, ops, payload, arena_fn, include_free, out_ns);
}

/*---------------------------------------------------------------------------*/
/* Strategy runner                                                           */
/*---------------------------------------------------------------------------*/

/* Returns the arena callback for a given strategy, or NULL for BENCH_XMALLOC.
 */
static BenchArenaAllocFn bench_get_arena_fn(BenchStrategy strat) {
  switch (strat) {
  case BENCH_PL_ARENA_CALLOC:
    return bench_fn_arena_calloc;
  case BENCH_PL_ARENA_ALLOC:
    return bench_fn_arena_alloc;
  case BENCH_PL_ARENA_ADD:
    return bench_fn_arena_add;
  default:
    return NULL;
  }
}

/* Runs warmup and measurement rounds for one strategy + payload size.
 * When 'include_free' is non-zero, each round's timing covers deallocation.
 * It borrows input strings and allocates temporary buffers for timing samples.
 * Side effects: executes the selected allocation strategy and prints summary.
 * Error semantics: returns OK on success, ERR on invalid input/round failure.
 */
static int bench_run_strategy(BenchStrategy strat, const char *name,
                              uint32_t len, uint32_t ops, const char *payload,
                              int include_free) {
  if (!name || !payload || len == 0 || ops == 0)
    return ERR;

  BenchArenaAllocFn arena_fn = bench_get_arena_fn(strat);

  uint64_t *samples =
      (uint64_t *)malloc(BENCH_MEASURE_ROUNDS * sizeof(*samples));
  if (!samples)
    return ERR;

  char **xmalloc_ptrs = NULL;
  if (strat == BENCH_XMALLOC) {
    xmalloc_ptrs = (char **)malloc((size_t)ops * sizeof(*xmalloc_ptrs));
    if (!xmalloc_ptrs) {
      free(samples);
      return ERR;
    }
  }

  for (uint32_t i = 0; i < BENCH_MEASURE_ROUNDS; i++) {
    uint64_t ns = 0;
    if (bench_dispatch_round(strat, arena_fn, len, ops, payload, xmalloc_ptrs,
                             include_free, &ns) != OK) {
      free(xmalloc_ptrs);
      free(samples);
      return ERR;
    }
    samples[i] = ns;
  }

  BenchStats st = {0};
  if (bench_compute_stats(samples, BENCH_MEASURE_ROUNDS, &st) != OK) {
    free(xmalloc_ptrs);
    free(samples);
    return ERR;
  }

  double min_ns_op = (double)st.min_ns / (double)ops;
  double p50_ns_op = (double)st.p50_ns / (double)ops;
  printf("  %-16s %3u bytes  min_ns/op=%.2f  p50_ns/op=%.2f\n", name, len,
         min_ns_op, p50_ns_op);

  free(xmalloc_ptrs);
  free(samples);
  return OK;
}

/*---------------------------------------------------------------------------*/
/* Main                                                                      */
/*---------------------------------------------------------------------------*/

/* Forks a child that runs exactly one strategy + size combination.
 * Each strategy gets a fresh process so no allocator state (mmap threshold,
 * brk region, bin warmth) carries over from a prior strategy's rounds.
 * g_guard_sink is volatile so the compiler cannot elide work in the child.
 * Returns OK if the child exited successfully, ERR otherwise.
 */
static int bench_run_strategy_isolated(BenchStrategy strat, const char *name,
                                       uint32_t len, uint32_t ops,
                                       const char *payload, int include_free) {
  fflush(stdout);
  pid_t pid = fork();
  if (pid < 0) {
    perror("benchmark: fork");
    return ERR;
  }
  if (pid == 0) {
    int rc = bench_run_strategy(strat, name, len, ops, payload, include_free);
    exit(rc == OK ? 0 : 1);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) < 0) {
    perror("benchmark: waitpid");
    return ERR;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
    return ERR;
  return OK;
}

/* Runs all cases × sizes for one group. Each strategy is forked in isolation.
 */
static int bench_run_group(const char *label, const BenchCase *cases,
                           size_t n_cases, const uint32_t *sizes,
                           size_t n_sizes, const char *payload,
                           int include_free) {
  printf("%s  (ops=%u, rounds=%u)\n", label, BENCH_OPS_PER_ROUND,
         BENCH_MEASURE_ROUNDS);
  for (size_t i = 0; i < n_sizes; i++) {
    for (size_t j = 0; j < n_cases; j++) {
      if (bench_run_strategy_isolated(cases[j].strat, cases[j].name, sizes[i],
                                      BENCH_OPS_PER_ROUND, payload,
                                      include_free) != OK) {
        fprintf(stderr, "benchmark: %s run failed for len=%u\n", cases[j].name,
                sizes[i]);
        return ERR;
      }
    }
  }
  return OK;
}

int main(void) {
  const uint32_t sizes[] = {8u, 16u, 32u, 64u, 128u};
  const BenchCase cases[] = {
      {BENCH_XMALLOC, "xmalloc"},
      {BENCH_PL_ARENA_CALLOC, "pl_arena_calloc"},
      {BENCH_PL_ARENA_ADD, "pl_arena_add"},
      {BENCH_PL_ARENA_ALLOC, "pl_arena_alloc"},
  };

  char payload[128];
  if (bench_fill_payload(payload, sizeof(payload)) != OK) {
    fprintf(stderr, "benchmark: failed to initialize payload\n");
    return 1;
  }

  if (bench_run_group("--- alloc only ---", cases, ARRLEN(cases), sizes,
                      ARRLEN(sizes), payload, 0) != OK) {
    fprintf(stderr, "benchmark: alloc-only group failed\n");
    return 1;
  }
  printf("\n");
  if (bench_run_group("--- alloc + free ---", cases, ARRLEN(cases), sizes,
                      ARRLEN(sizes), payload, 1) != OK) {
    fprintf(stderr, "benchmark: alloc+free group failed\n");
    return 1;
  }
  return 0;
}
