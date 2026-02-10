#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "resume_token.h"
#include "string_op.h"
#include "test.h"

/* Creates and returns a unique temporary directory path.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates a directory under /tmp.
 * Error semantics: asserts on setup failures and returns non-NULL path. */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_restok_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  return dup_or_null(dir);
}

/* Best-effort cleanup of runtime directory used by resume token module.
 * Ownership: borrows 'tmpdir'; no allocations.
 * Side effects: unlinks token files and removes token cache/temp directory.
 * Error semantics: none (best-effort cleanup helper). */
static void cleanup_runtime_dir(const char *tmpdir) {
  if (!tmpdir)
    return;

  char p[512];
  snprintf(p, sizeof(p), "%s/ai-dbexplorer-mcp", tmpdir);
  (void)rmdir(p);
  (void)rmdir(tmpdir);
}

/* Sets runtime environment variable used by resume token module.
 * Ownership: borrows 'tmpdir'; no allocations.
 * Side effects: mutates process environment.
 * Error semantics: none (asserts on unsupported platform path). */
static void set_runtime_env(const char *tmpdir) {
#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_RUNTIME_DIR", tmpdir, 1) == 0);
#elif defined(__APPLE__)
  ASSERT_TRUE(setenv("TMPDIR", tmpdir, 1) == 0);
#else
  (void)tmpdir;
  ASSERT_TRUE(0 && "unsupported platform in test");
#endif
}

/* Verifies loading from empty state returns NO. */
static void test_load_missing_returns_no(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  uint8_t out[RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load("/tmp/sock_a", out) == NO);

  cleanup_runtime_dir(tmpdir);
  free(tmpdir);
}

/* Verifies storing then loading by socket path succeeds. */
static void test_store_then_load_ok(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  const uint8_t in[RESUME_TOKEN_LEN] = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
      17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
  ASSERT_TRUE(restok_store("/tmp/sock_a", in) == OK);

  uint8_t out[RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load("/tmp/sock_a", out) == YES);
  ASSERT_TRUE(memcmp(in, out, RESUME_TOKEN_LEN) == 0);

  ASSERT_TRUE(restok_delete("/tmp/sock_a") == OK);
  cleanup_runtime_dir(tmpdir);
  free(tmpdir);
}

/* Verifies tokens are isolated by socket path hash partitioning. */
static void test_sock_path_scoping(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  const uint8_t a[RESUME_TOKEN_LEN] = {
      10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
      26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
  const uint8_t b[RESUME_TOKEN_LEN] = {
      90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105,
      106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
      121};

  ASSERT_TRUE(restok_store("/tmp/sock_a", a) == OK);
  ASSERT_TRUE(restok_store("/tmp/sock_b", b) == OK);

  uint8_t out_a[RESUME_TOKEN_LEN] = {0};
  uint8_t out_b[RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load("/tmp/sock_a", out_a) == YES);
  ASSERT_TRUE(restok_load("/tmp/sock_b", out_b) == YES);
  ASSERT_TRUE(memcmp(a, out_a, RESUME_TOKEN_LEN) == 0);
  ASSERT_TRUE(memcmp(b, out_b, RESUME_TOKEN_LEN) == 0);

  ASSERT_TRUE(restok_delete("/tmp/sock_a") == OK);
  ASSERT_TRUE(restok_delete("/tmp/sock_b") == OK);
  cleanup_runtime_dir(tmpdir);
  free(tmpdir);
}

/* Verifies delete is idempotent and load returns NO after deletion. */
static void test_delete_clears_token(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  const uint8_t tok[RESUME_TOKEN_LEN] = {
      5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 15, 14, 13, 12, 11, 10,
      25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26};
  ASSERT_TRUE(restok_store("/tmp/sock_delete", tok) == OK);
  ASSERT_TRUE(restok_delete("/tmp/sock_delete") == OK);
  ASSERT_TRUE(restok_delete("/tmp/sock_delete") == OK);

  uint8_t out[RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load("/tmp/sock_delete", out) == NO);

  cleanup_runtime_dir(tmpdir);
  free(tmpdir);
}

int main(void) {
  test_load_missing_returns_no();
  test_store_then_load_ok();
  test_sock_path_scoping();
  test_delete_clears_token();

  fprintf(stderr, "OK: test_resume_token\n");
  return 0;
}
