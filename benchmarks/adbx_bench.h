#ifndef ADBX_BENCH_H
#define ADBX_BENCH_H
/*
 * adbx_bench.h — lightweight fork-isolated micro-benchmark harness.
 *
 * Usage:
 *
 *   void my_fn(void *ctx) { ... one operation ... }
 *
 *   int main(void) {
 *     MyCtx ctx = setup();
 *     ADBX_BENCH("my_label", 3, 500, 5000, my_fn, &ctx);
 *     return 0;
 *   }
 *
 * Each ADBX_BENCH invocation forks a child process, runs warmup rounds,
 * then measures rounds of ops calls each and prints:
 *   label  p1_ns/op  median_ns/op  mean_99ci
 *
 * Args: label, warmup, rounds, ops, fn, ctx
 *
 * For benchmarks that need per-round setup/cleanup (e.g. create/destroy
 * an arena each round), use ADBX_BENCH_F:
 *
 *   void setup(void *ctx)
 *   void cleanup(void *ctx)
 *   void bench(void *ctx)
 *
 *   ADBX_BENCH_F("label", 3, 500, 5000, setup, cleanup, bench, &ctx);
 */

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ── compiler barrier ────────────────────────────────────────────────── */

/* Force the compiler to materialise `ptr` (prevent dead-store / dead-code
 * elimination).  Use this on pointers returned by allocators under test. */
static inline void adbx_bench_use(void *ptr) {
  __asm__ volatile("" : : "r"(ptr) : "memory");
}

/* ── internal helpers ────────────────────────────────────────────────── */

typedef void (*adbx_bench_fn)(void *);

static int adbx_bench__cmp_double(const void *a, const void *b) {
  double da = *(const double *)a;
  double db = *(const double *)b;
  return (da > db) - (da < db);
}

static double adbx_bench__elapsed_ns(struct timespec *t0, struct timespec *t1) {
  return (double)(t1->tv_sec - t0->tv_sec) * 1e9 +
         (double)(t1->tv_nsec - t0->tv_nsec);
}

/* t-distribution critical values for two-tailed 99% CI (α/2 = 0.005).
 * Index by (df - 1).  We only need df = ROUNDS - 1; for df >= 100 we
 * approximate with the normal z = 2.576.                               */
static double adbx_bench__t_crit(int df) {
  /* table for df 1..30 */
  static const double tbl[] = {
      63.657, 9.925, 5.841, 4.604, 4.032, 3.707, 3.499, 3.355, 3.250, 3.169,
      3.106,  3.055, 3.012, 2.977, 2.947, 2.921, 2.898, 2.878, 2.861, 2.845,
      2.831,  2.819, 2.807, 2.797, 2.787, 2.779, 2.771, 2.763, 2.756, 2.750,
  };
  if (df >= 1 && df <= 30)
    return tbl[df - 1];
  if (df <= 100) {
    /* linear interpolation between df=30 and df=100 */
    double t30 = 2.750, t120 = 2.617;
    return t30 + (t120 - t30) * ((double)(df - 30) / 90.0);
  }
  return 2.576; /* z for large df */
}

static void adbx_bench__print(const char *label, double *samples, int rounds) {
  /* sort for percentiles */
  qsort(samples, (size_t)rounds, sizeof(double), adbx_bench__cmp_double);

  /* median (p50) */
  double median;
  if (rounds % 2 == 0)
    median = (samples[rounds / 2 - 1] + samples[rounds / 2]) / 2.0;
  else
    median = samples[rounds / 2];

  /* p1 */
  int p1_idx = (int)((double)rounds * 0.01);
  if (p1_idx < 0)
    p1_idx = 0;
  double p1 = samples[p1_idx];

  /* mean and stddev */
  double sum = 0, sum2 = 0;
  for (int r = 0; r < rounds; r++) {
    sum += samples[r];
    sum2 += samples[r] * samples[r];
  }
  double mean = sum / (double)rounds;
  double var = (sum2 - sum * sum / (double)rounds) / (double)(rounds - 1);
  double stddev = sqrt(var);

  /* 99% CI of the mean */
  int df = rounds - 1;
  double t = adbx_bench__t_crit(df);
  double margin = t * stddev / sqrt((double)rounds);
  double ci_lo = mean - margin;
  double ci_hi = mean + margin;

  printf("  %-24s  p1=%.2f ns/op  median=%.2f ns/op  "
         "mean=[%.2f, %.2f] ns/op (99%% CI)\n",
         label, p1, median, ci_lo, ci_hi);
  fflush(stdout);
}

/* No per-round lifecycle — runs all ops in a growing loop. */
__attribute__((unused))
static void adbx_bench__run(const char *label, int warmup, int rounds, int ops,
                            adbx_bench_fn fn, void *ctx) {
  double *samples = (double *)malloc((size_t)rounds * sizeof(double));
  if (!samples) {
    fprintf(stderr, "adbx_bench: malloc failed\n");
    _exit(1);
  }

  /* warmup */
  for (int w = 0; w < warmup; w++)
    for (int i = 0; i < ops; i++)
      fn(ctx);

  /* measurement */
  for (int r = 0; r < rounds; r++) {
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < ops; i++)
      fn(ctx);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    samples[r] = adbx_bench__elapsed_ns(&t0, &t1) / (double)ops;
  }

  adbx_bench__print(label, samples, rounds);
  free(samples);
}

/* Per-round setup/cleanup — calls setup before and cleanup after each round
 * (including warmup).  Only the ops loop is timed. */
static void adbx_bench__run_f(const char *label, int warmup, int rounds,
                              int ops, adbx_bench_fn setup,
                              adbx_bench_fn cleanup, adbx_bench_fn fn,
                              void *ctx) {
  double *samples = (double *)malloc((size_t)rounds * sizeof(double));
  if (!samples) {
    fprintf(stderr, "adbx_bench: malloc failed\n");
    _exit(1);
  }

  /* warmup */
  for (int w = 0; w < warmup; w++) {
    setup(ctx);
    for (int i = 0; i < ops; i++)
      fn(ctx);
    cleanup(ctx);
  }

  /* measurement */
  for (int r = 0; r < rounds; r++) {
    setup(ctx);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < ops; i++)
      fn(ctx);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    cleanup(ctx);
    samples[r] = adbx_bench__elapsed_ns(&t0, &t1) / (double)ops;
  }

  adbx_bench__print(label, samples, rounds);
  free(samples);
}

/* ── public macro ────────────────────────────────────────────────────── */

#define ADBX_BENCH(label, warmup, rounds, ops, fn, ctx)                        \
  do {                                                                         \
    fflush(stdout);                                                            \
    pid_t pid_ = fork();                                                       \
    if (pid_ < 0) {                                                            \
      perror("adbx_bench: fork");                                              \
      exit(1);                                                                 \
    }                                                                          \
    if (pid_ == 0) {                                                           \
      adbx_bench__run((label), (warmup), (rounds), (ops), (fn), (ctx));        \
      _exit(0);                                                                \
    }                                                                          \
    int status_;                                                               \
    waitpid(pid_, &status_, 0);                                                \
    if (!WIFEXITED(status_) || WEXITSTATUS(status_) != 0) {                    \
      fprintf(stderr, "adbx_bench: child failed for '%s'\n", (label));         \
    }                                                                          \
  } while (0)

#define ADBX_BENCH_F(label, warmup, rounds, ops, setup_fn, cleanup_fn, fn,     \
                     ctx)                                                      \
  do {                                                                         \
    fflush(stdout);                                                            \
    pid_t pid_ = fork();                                                       \
    if (pid_ < 0) {                                                            \
      perror("adbx_bench: fork");                                              \
      exit(1);                                                                 \
    }                                                                          \
    if (pid_ == 0) {                                                           \
      adbx_bench__run_f((label), (warmup), (rounds), (ops), (setup_fn),        \
                        (cleanup_fn), (fn), (ctx));                            \
      _exit(0);                                                                \
    }                                                                          \
    int status_;                                                               \
    waitpid(pid_, &status_, 0);                                                \
    if (!WIFEXITED(status_) || WEXITSTATUS(status_) != 0) {                    \
      fprintf(stderr, "adbx_bench: child failed for '%s'\n", (label));         \
    }                                                                          \
  } while (0)

#endif /* ADBX_BENCH_H */
