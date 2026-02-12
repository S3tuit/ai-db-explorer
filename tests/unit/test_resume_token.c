#define _GNU_SOURCE

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "resume_token.h"
#include "string_op.h"
#include "test.h"

/* Creates and returns a unique temporary directory path.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates a directory under /tmp.
 * Error semantics: asserts on setup failures and returns non-NULL path.
 */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_restok_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  return dup_or_null(dir);
}

/* Sets runtime environment variable used by resume token storage directory.
 * Ownership: borrows 'tmpdir'; no allocations.
 * Side effects: mutates process environment.
 * Error semantics: asserts on unsupported platform or setenv failures.
 */
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

/* Best-effort cleanup of temporary runtime directories/files.
 * Ownership: borrows 'tmpdir' and optional 'store'; no allocations.
 * Side effects: unlinks token file and removes directories if empty.
 * Error semantics: none (best-effort cleanup helper).
 */
static void cleanup_runtime_dir(const char *tmpdir, ResumeTokenStore *store) {
  if (store && store->token_path)
    (void)unlink(store->token_path);

  if (!tmpdir)
    return;

  char p[512];
  snprintf(p, sizeof(p), "%s/ai-dbexplorer-mcp", tmpdir);
  (void)rmdir(p);
  (void)rmdir(tmpdir);
}

/* Verifies initialization succeeds and missing token returns NO. */
static void test_init_and_load_missing(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  ResumeTokenStore store = {0};
  ASSERT_TRUE(restok_init(&store) == YES);
  ASSERT_TRUE(store.enabled == YES);

  uint8_t out[ADBX_RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load(&store, out) == NO);

  cleanup_runtime_dir(tmpdir, &store);
  restok_clean(&store);
  free(tmpdir);
}

/* Verifies storing then loading raw 32-byte tokens succeeds. */
static void test_store_then_load_ok(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  ResumeTokenStore store = {0};
  ASSERT_TRUE(restok_init(&store) == YES);
  ASSERT_TRUE(store.enabled == YES);

  const uint8_t in[ADBX_RESUME_TOKEN_LEN] = {
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
      17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
  ASSERT_TRUE(restok_store(&store, in) == OK);

  uint8_t out[ADBX_RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load(&store, out) == YES);
  ASSERT_TRUE(memcmp(in, out, ADBX_RESUME_TOKEN_LEN) == 0);

  ASSERT_TRUE(restok_delete(&store) == OK);
  ASSERT_TRUE(restok_load(&store, out) == NO);

  cleanup_runtime_dir(tmpdir, &store);
  restok_clean(&store);
  free(tmpdir);
}

/* Verifies malformed token files are treated as stale and deleted. */
static void test_corrupted_file_is_deleted(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  ResumeTokenStore store = {0};
  ASSERT_TRUE(restok_init(&store) == YES);
  ASSERT_TRUE(store.enabled == YES);

  const uint8_t tok[ADBX_RESUME_TOKEN_LEN] = {
      50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65,
      66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81};
  ASSERT_TRUE(restok_store(&store, tok) == OK);
  ASSERT_TRUE(store.token_path != NULL);

  int fd = open(store.token_path, O_WRONLY | O_TRUNC, 0600);
  ASSERT_TRUE(fd >= 0);
  const uint8_t bad[7] = {1, 2, 3, 4, 5, 6, 7};
  ASSERT_TRUE(write(fd, bad, sizeof(bad)) == (ssize_t)sizeof(bad));
  ASSERT_TRUE(close(fd) == 0);

  uint8_t out[ADBX_RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load(&store, out) == NO);

  struct stat st;
  ASSERT_TRUE(stat(store.token_path, &st) != 0);

  cleanup_runtime_dir(tmpdir, &store);
  restok_clean(&store);
  free(tmpdir);
}

/* Verifies permissive directory mode disables resume for this process. */
static void test_dir_policy_disables_resume(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  ResumeTokenStore store = {0};
  ASSERT_TRUE(restok_init(&store) == YES);
  ASSERT_TRUE(store.enabled == YES);
  ASSERT_TRUE(store.dir_path != NULL);

  ASSERT_TRUE(chmod(store.dir_path, 0755) == 0);

  uint8_t out[ADBX_RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load(&store, out) == NO);
  ASSERT_TRUE(store.enabled == NO);

  // Disabled state is fail-safe no-op for persistence operations.
  const uint8_t tok[ADBX_RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_store(&store, tok) == OK);
  ASSERT_TRUE(restok_delete(&store) == OK);

  ASSERT_TRUE(chmod(store.dir_path, 0700) == 0);
  cleanup_runtime_dir(tmpdir, &store);
  restok_clean(&store);
  free(tmpdir);
}

int main(void) {
  test_init_and_load_missing();
  test_store_then_load_ok();
  test_corrupted_file_is_deleted();
  test_dir_policy_disables_resume();

  fprintf(stderr, "OK: test_resume_token\n");
  return 0;
}
