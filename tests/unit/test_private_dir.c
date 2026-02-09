#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "private_dir.h"
#include "string_op.h"
#include "test.h"

/* Helper: create a temporary directory via mkdtemp. Caller must free. */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_privdir_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  return dup_or_null(dir);
}

/* Helper: recursively remove a test directory (shallow, only our structure). */
static void rm_tmpdir(const char *path) {
  /* Best-effort removal of known structure. */
  char buf[512];

  snprintf(buf, sizeof(buf), "%s/%s/run/%s", path, PRIVDIR_APPNAME,
           PRIVDIR_SOCK_FILENAME);
  (void)unlink(buf);

  snprintf(buf, sizeof(buf), "%s/%s/secret/%s", path, PRIVDIR_APPNAME,
           PRIVDIR_TOKEN_FILENAME);
  (void)unlink(buf);

  snprintf(buf, sizeof(buf), "%s/%s/run", path, PRIVDIR_APPNAME);
  (void)rmdir(buf);

  snprintf(buf, sizeof(buf), "%s/%s/secret", path, PRIVDIR_APPNAME);
  (void)rmdir(buf);

  snprintf(buf, sizeof(buf), "%s/%s", path, PRIVDIR_APPNAME);
  (void)rmdir(buf);

  (void)rmdir(path);
}

static void test_resolve_with_env_var(void) {
  char *tmpdir = make_tmpdir();

#ifdef __linux__
  setenv("XDG_RUNTIME_DIR", tmpdir, 1);
#else
  setenv("TMPDIR", tmpdir, 1);
#endif

  PrivDir *pd = privdir_resolve();
  ASSERT_TRUE(pd != NULL);

  /* base should end with /ai-dbexplorer/ */
  size_t base_len = strlen(pd->base);
  const char *suffix = "/" PRIVDIR_APPNAME "/";
  size_t suffix_len = strlen(suffix);
  ASSERT_TRUE(base_len >= suffix_len);
  ASSERT_STREQ(pd->base + base_len - suffix_len, suffix);

  /* run_dir should end with /run/ */
  ASSERT_TRUE(strlen(pd->run_dir) > 4);
  ASSERT_STREQ(pd->run_dir + strlen(pd->run_dir) - 4, "run/");

  /* secret_dir should end with /secret/ */
  ASSERT_TRUE(strlen(pd->secret_dir) > 7);
  ASSERT_STREQ(pd->secret_dir + strlen(pd->secret_dir) - 7, "secret/");

  /* sock_path should end with broker.sock */
  ASSERT_TRUE(strlen(pd->sock_path) > strlen(PRIVDIR_SOCK_FILENAME));
  ASSERT_STREQ(
      pd->sock_path + strlen(pd->sock_path) - strlen(PRIVDIR_SOCK_FILENAME),
      PRIVDIR_SOCK_FILENAME);

  /* token_path should end with token */
  ASSERT_TRUE(strlen(pd->token_path) > strlen(PRIVDIR_TOKEN_FILENAME));
  ASSERT_STREQ(
      pd->token_path + strlen(pd->token_path) - strlen(PRIVDIR_TOKEN_FILENAME),
      PRIVDIR_TOKEN_FILENAME);

  privdir_free(pd);
  rm_tmpdir(tmpdir);
  free(tmpdir);
}

static void test_resolve_fallback(void) {
#ifdef __linux__
  unsetenv("XDG_RUNTIME_DIR");
#else
  unsetenv("TMPDIR");
#endif

  PrivDir *pd = privdir_resolve();
  ASSERT_TRUE(pd != NULL);

  /* base should match /tmp/ai-dbexplorer-<uid>/ */
  char expected[128];
  snprintf(expected, sizeof(expected), "/tmp/%s-%u/", PRIVDIR_APPNAME,
           (unsigned)getuid());
  ASSERT_TRUE(strcmp(pd->base, expected) == 0);

  privdir_free(pd);
}

static void test_resolve_rejects_relative_path(void) {
#ifdef __linux__
  setenv("XDG_RUNTIME_DIR", "relative/path", 1);
#else
  setenv("TMPDIR", "relative/path", 1);
#endif

  PrivDir *pd = privdir_resolve();
  /* Should fall back to /tmp/ path, not crash. The env is invalid so
   * it uses the fallback. */
  ASSERT_TRUE(pd != NULL);

  char expected[128];
  snprintf(expected, sizeof(expected), "/tmp/%s-%u/", PRIVDIR_APPNAME,
           (unsigned)getuid());
  ASSERT_TRUE(strcmp(pd->base, expected) == 0);

  privdir_free(pd);
}

static void test_resolve_rejects_overlong_path(void) {
  /* Create a path that's way too long for sun_path (108 bytes on Linux). */
  char longpath[256];
  longpath[0] = '/';
  memset(longpath + 1, 'a', sizeof(longpath) - 2);
  longpath[sizeof(longpath) - 1] = '\0';

#ifdef __linux__
  setenv("XDG_RUNTIME_DIR", longpath, 1);
#else
  setenv("TMPDIR", longpath, 1);
#endif

  /* The env path is too long, so resolve should fall back to /tmp/ form.
   * But /tmp/ form with uid should be short enough to work. */
  PrivDir *pd = privdir_resolve();
  if (pd) {
    /* If fallback worked, base should be /tmp/... form */
    ASSERT_TRUE(strncmp(pd->base, "/tmp/", 5) == 0);
    privdir_free(pd);
  }
  /* If pd is NULL, that's also acceptable — overlong path rejected. */
}

static void test_create_layout_permissions(void) {
  char *tmpdir = make_tmpdir();

#ifdef __linux__
  setenv("XDG_RUNTIME_DIR", tmpdir, 1);
#else
  setenv("TMPDIR", tmpdir, 1);
#endif

  PrivDir *pd = privdir_resolve();
  ASSERT_TRUE(pd != NULL);

  int rc = privdir_create_layout(pd);
  ASSERT_TRUE(rc == OK);

  /* Verify each directory exists and has 0700 perms. */
  struct stat st;

  ASSERT_TRUE(stat(pd->base, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(pd->run_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(pd->secret_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  /* Calling create_layout again should succeed (dirs already exist). */
  ASSERT_TRUE(privdir_create_layout(pd) == OK);

  privdir_cleanup(pd);
  privdir_free(pd);
  rm_tmpdir(tmpdir);
  free(tmpdir);
}

static void test_generate_and_read_token(void) {
  char *tmpdir = make_tmpdir();

#ifdef __linux__
  setenv("XDG_RUNTIME_DIR", tmpdir, 1);
#else
  setenv("TMPDIR", tmpdir, 1);
#endif

  PrivDir *pd = privdir_resolve();
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(privdir_create_layout(pd) == OK);

  int rc = privdir_generate_token(pd);
  ASSERT_TRUE(rc == OK);

  /* Verify file exists with 0600 perms. */
  struct stat st;
  ASSERT_TRUE(stat(pd->token_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);
  ASSERT_TRUE(st.st_size == PRIVDIR_TOKEN_LEN);

  /* Read back and verify it's 32 bytes. */
  uint8_t token[PRIVDIR_TOKEN_LEN];
  memset(token, 0, sizeof(token));
  ASSERT_TRUE(privdir_read_token(pd, token) == OK);

  /* Token should not be all zeros (extremely unlikely from CSPRNG). */
  int all_zero = 1;
  for (int i = 0; i < PRIVDIR_TOKEN_LEN; i++) {
    if (token[i] != 0) {
      all_zero = 0;
      break;
    }
  }
  ASSERT_TRUE(!all_zero);

  privdir_cleanup(pd);
  privdir_free(pd);
  rm_tmpdir(tmpdir);
  free(tmpdir);
}

static void test_cleanup(void) {
  char *tmpdir = make_tmpdir();

#ifdef __linux__
  setenv("XDG_RUNTIME_DIR", tmpdir, 1);
#else
  setenv("TMPDIR", tmpdir, 1);
#endif

  PrivDir *pd = privdir_resolve();
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(privdir_create_layout(pd) == OK);
  ASSERT_TRUE(privdir_generate_token(pd) == OK);

  /* Verify everything exists before cleanup. */
  struct stat st;
  ASSERT_TRUE(stat(pd->base, &st) == 0);
  ASSERT_TRUE(stat(pd->run_dir, &st) == 0);
  ASSERT_TRUE(stat(pd->secret_dir, &st) == 0);
  ASSERT_TRUE(stat(pd->token_path, &st) == 0);

  /* Cleanup should remove everything. */
  privdir_cleanup(pd);

  ASSERT_TRUE(stat(pd->token_path, &st) != 0);
  ASSERT_TRUE(stat(pd->secret_dir, &st) != 0);
  ASSERT_TRUE(stat(pd->run_dir, &st) != 0);
  ASSERT_TRUE(stat(pd->base, &st) != 0);

  privdir_free(pd);
  rm_tmpdir(tmpdir);
  free(tmpdir);
}

int main(void) {
  test_resolve_with_env_var();
  test_resolve_fallback();
  test_resolve_rejects_relative_path();
  test_resolve_rejects_overlong_path();
  test_create_layout_permissions();
  test_generate_and_read_token();
  test_cleanup();

  fprintf(stderr, "OK: test_private_dir\n");
  return 0;
}
