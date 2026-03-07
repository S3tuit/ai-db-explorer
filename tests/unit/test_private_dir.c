#define _GNU_SOURCE

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "file_io.h"
#include "private_dir.h"
#include "test.h"

/* Helper: create a temporary directory via mkdtemp.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates a directory under /tmp.
 * Error semantics: asserts on setup failure and returns non-NULL path.
 */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_privdir_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  return strdup(dir);
}

/* Helper: duplicates one directory path with exactly one trailing '/'.
 * Ownership: returns heap string owned by caller.
 * Side effects: allocates heap memory.
 * Error semantics: asserts on invalid input or allocation failure.
 */
static char *dup_dir_with_slash(const char *path) {
  ASSERT_TRUE(path != NULL);

  size_t len = strlen(path);
  while (len > 1 && path[len - 1] == '/')
    len--;

  int needs_slash = (len == 0 || path[len - 1] != '/');
  char *out = xmalloc(len + (needs_slash ? 2u : 1u));
  memcpy(out, path, len);
  if (needs_slash)
    out[len++] = '/';
  out[len] = '\0';
  return out;
}

/* Helper: joins one directory and child name, returning a directory path.
 * Ownership: returns heap string owned by caller.
 * Side effects: allocates heap memory.
 * Error semantics: asserts on allocation failure.
 */
static char *join_dir_path(const char *dir, const char *child) {
  char *joined = path_join(dir, child);
  ASSERT_TRUE(joined != NULL);
  char *out = dup_dir_with_slash(joined);
  free(joined);
  return out;
}

/* Helper: builds the expected /tmp fallback app dir for the current uid.
 * Ownership: returns heap string owned by caller.
 * Side effects: allocates heap memory.
 * Error semantics: asserts on formatting failure.
 */
static char *make_tmp_fallback_app_dir(void) {
  int n =
      snprintf(NULL, 0, "/tmp/%s-%u/", PRIVDIR_APP_DIRNAME, (unsigned)getuid());
  ASSERT_TRUE(n > 0);
  char *out = xmalloc((size_t)n + 1u);
  ASSERT_TRUE(snprintf(out, (size_t)n + 1u, "/tmp/%s-%u/", PRIVDIR_APP_DIRNAME,
                       (unsigned)getuid()) == n);
  return out;
}

/* Helper: points runtime env vars at one temp directory.
 * Ownership: borrows 'tmpdir'; no allocations.
 * Side effects: mutates process environment.
 * Error semantics: asserts on unsupported platform or setenv failure.
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

/* Helper: removes one path if it still exists.
 * Ownership: borrows 'path'; no allocations.
 * Side effects: best-effort unlink or rmdir based on current inode type.
 * Error semantics: none (cleanup helper is best-effort).
 */
static void remove_path_if_exists(const char *path) {
  if (!path)
    return;

  struct stat st = {0};
  if (lstat(path, &st) != 0)
    return;
  if (S_ISDIR(st.st_mode)) {
    (void)rmdir(path);
  } else {
    (void)unlink(path);
  }
}

/* Helper: asserts one directory contains no application artifacts.
 * Ownership: borrows 'path'; no allocations.
 * Side effects: reads directory entries.
 * Error semantics: asserts on any unexpected content.
 */
static void assert_dir_empty(const char *path) {
  DIR *d = opendir(path);
  ASSERT_TRUE(d != NULL);

  for (;;) {
    struct dirent *ent = readdir(d);
    if (!ent)
      break;
    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
      continue;
    ASSERT_TRUE(0 && "unexpected leftover private-dir artifact");
  }

  closedir(d);
}

/* Helper: asserts the resolved paths match one parent base and app dir.
 * Ownership: borrows inputs; allocates temporary expected paths internally.
 * Side effects: allocates and frees temporary heap strings.
 * Error semantics: asserts on any mismatch.
 */
static void assert_resolved_layout(const PrivDir *pd, const char *expected_base,
                                   const char *expected_app_dir) {
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(expected_base != NULL);
  ASSERT_TRUE(expected_app_dir != NULL);

  ASSERT_STREQ(pd->base, expected_base);
  ASSERT_STREQ(pd->app_dir, expected_app_dir);

  char *expected_run = join_dir_path(expected_app_dir, PRIVDIR_RUN_DIRNAME);
  char *expected_secret =
      join_dir_path(expected_app_dir, PRIVDIR_SECRET_DIRNAME);
  char *expected_sock = path_join(expected_run, PRIVDIR_SOCK_FILENAME);
  char *expected_token = path_join(expected_secret, PRIVDIR_TOKEN_FILENAME);
  ASSERT_TRUE(expected_sock != NULL);
  ASSERT_TRUE(expected_token != NULL);

  ASSERT_STREQ(pd->run_dir, expected_run);
  ASSERT_STREQ(pd->secret_dir, expected_secret);
  ASSERT_STREQ(pd->sock_path, expected_sock);
  ASSERT_STREQ(pd->token_path, expected_token);

  free(expected_run);
  free(expected_secret);
  free(expected_sock);
  free(expected_token);
}

/* Verifies env-based resolve keeps the env directory as the parent base and
 * places broker artifacts under the adbxplorer/ app dir inside it.
 */
static void test_resolve_with_env_var(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  PrivDir *pd = privdir_resolve(NULL, NULL);
  ASSERT_TRUE(pd != NULL);

  char *expected_base = dup_dir_with_slash(tmpdir);
  char *expected_app = join_dir_path(expected_base, PRIVDIR_APP_DIRNAME);
  assert_resolved_layout(pd, expected_base, expected_app);

  free(expected_app);
  free(expected_base);
  privdir_clean(pd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

/* Verifies explicit input is treated as the parent base directory and not as
 * the app directory itself.
 */
static void test_resolve_explicit_base_uses_parent_dir(void) {
  char *tmpdir = make_tmpdir();

  PrivDir *pd = privdir_resolve(tmpdir, NULL);
  ASSERT_TRUE(pd != NULL);

  char *expected_base = dup_dir_with_slash(tmpdir);
  char *expected_app = join_dir_path(expected_base, PRIVDIR_APP_DIRNAME);
  assert_resolved_layout(pd, expected_base, expected_app);

  free(expected_app);
  free(expected_base);
  privdir_clean(pd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

/* Verifies missing env falls back to /tmp/ as parent base and uses a
 * uid-scoped app dir there.
 */
static void test_resolve_fallback(void) {
#ifdef __linux__
  unsetenv("XDG_RUNTIME_DIR");
#else
  unsetenv("TMPDIR");
#endif

  PrivDir *pd = privdir_resolve(NULL, NULL);
  ASSERT_TRUE(pd != NULL);

  char *expected_app = make_tmp_fallback_app_dir();
  assert_resolved_layout(pd, "/tmp/", expected_app);

  free(expected_app);
  privdir_clean(pd);
}

/* Verifies invalid env paths are ignored and resolution falls back to the
 * uid-scoped /tmp private-dir.
 */
static void test_resolve_env_relative_path_uses_tmp_fallback(void) {
#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_RUNTIME_DIR", "relative/path", 1) == 0);
#else
  ASSERT_TRUE(setenv("TMPDIR", "relative/path", 1) == 0);
#endif

  PrivDir *pd = privdir_resolve(NULL, NULL);
  ASSERT_TRUE(pd != NULL);

  char *expected_app = make_tmp_fallback_app_dir();
  assert_resolved_layout(pd, "/tmp/", expected_app);

  free(expected_app);
  privdir_clean(pd);
}

/* Verifies overlong env paths fails.
 */
static void test_resolve_overlong_env_path_fails(void) {
  char longpath[256];
  longpath[0] = '/';
  memset(longpath + 1, 'a', sizeof(longpath) - 2);
  longpath[sizeof(longpath) - 1] = '\0';

#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_RUNTIME_DIR", longpath, 1) == 0);
#else
  ASSERT_TRUE(setenv("TMPDIR", longpath, 1) == 0);
#endif

  char *err = NULL;
  ASSERT_TRUE(privdir_resolve(longpath, &err) == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "too long") != NULL);
  free(err);
}

/* Verifies invalid explicit input returns an explanatory error string. */
static void test_resolve_rejects_relative_input_with_error(void) {
  char *err = NULL;
  ASSERT_TRUE(privdir_resolve("relative/path", &err) == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "absolute path") != NULL);
  free(err);
}

/* Verifies overlong explicit input returns an explanatory error instead of
 * silently changing the selected base directory.
 */
static void test_resolve_overlong_explicit_path_reports_error(void) {
  char longpath[256];
  longpath[0] = '/';
  memset(longpath + 1, 'b', sizeof(longpath) - 2);
  longpath[sizeof(longpath) - 1] = '\0';

  char *err = NULL;
  ASSERT_TRUE(privdir_resolve(longpath, &err) == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "too long") != NULL);
  free(err);
}

/* Verifies broker-runtime open creates trusted dirs, writes the token, keeps
 * the app dir in place, and runtime cleanup removes the transient artifacts.
 */
static void test_broker_runtime_open_and_cleanup(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  PrivDir *pd = privdir_resolve(NULL, NULL);
  ASSERT_TRUE(pd != NULL);

  uint8_t issued[PRIVDIR_TOKEN_LEN] = {0};
  PrivDirBrokerRuntime *rt = privdir_broker_runtime_open(pd, issued);
  ASSERT_TRUE(rt != NULL);

  struct stat st = {0};
  ASSERT_TRUE(stat(pd->app_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(pd->run_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(pd->secret_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(pd->token_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);
  ASSERT_TRUE(st.st_size == PRIVDIR_TOKEN_LEN);

  uint8_t disk_token[PRIVDIR_TOKEN_LEN] = {0};
  ASSERT_TRUE(
      fileio_read_exact(pd->token_path, PRIVDIR_TOKEN_LEN, disk_token) == OK);
  ASSERT_TRUE(memcmp(disk_token, issued, PRIVDIR_TOKEN_LEN) == 0);

  char *lock_path = path_join(pd->app_dir, PRIVDIR_LOCK_FILENAME);
  ASSERT_TRUE(lock_path != NULL);
  ASSERT_TRUE(stat(lock_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);

  privdir_broker_runtime_clean(rt);

  ASSERT_TRUE(stat(pd->token_path, &st) != 0);
  ASSERT_TRUE(stat(lock_path, &st) != 0);
  ASSERT_TRUE(stat(pd->secret_dir, &st) != 0);
  ASSERT_TRUE(stat(pd->run_dir, &st) != 0);
  ASSERT_TRUE(stat(pd->app_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  assert_dir_empty(pd->app_dir);

  ASSERT_TRUE(rmdir(pd->app_dir) == 0);
  free(lock_path);
  privdir_clean(pd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

/* Verifies broker runtime refuses a symlinked app dir, blocking final-component
 * path substitution before any broker artifact is created.
 */
static void test_broker_runtime_rejects_symlinked_app_dir(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  char *target = path_join(tmpdir, "real-app");
  ASSERT_TRUE(target != NULL);
  ASSERT_TRUE(mkdir(target, 0700) == 0);

  char *app_link = path_join(tmpdir, PRIVDIR_APP_DIRNAME);
  ASSERT_TRUE(app_link != NULL);
  ASSERT_TRUE(symlink(target, app_link) == 0);

  PrivDir *pd = privdir_resolve(tmpdir, NULL);
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(privdir_broker_runtime_open(pd, NULL) == NULL);

  privdir_clean(pd);
  remove_path_if_exists(app_link);
  ASSERT_TRUE(rmdir(target) == 0);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(app_link);
  free(target);
  free(tmpdir);
}

/* Verifies one process holding the broker runtime lock blocks a second process
 * from opening the same runtime. fcntl locks are process-scoped, so this must
 * be asserted cross-process rather than by reopening in the same process.
 */
static void test_broker_runtime_blocks_second_process(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  PrivDir *pd = privdir_resolve(NULL, NULL);
  ASSERT_TRUE(pd != NULL);

  PrivDirBrokerRuntime *rt = privdir_broker_runtime_open(pd, NULL);
  ASSERT_TRUE(rt != NULL);

  pid_t pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    PrivDir *child_pd = privdir_resolve(tmpdir, NULL);
    if (!child_pd)
      _exit(2);

    PrivDirBrokerRuntime *child_rt =
        privdir_broker_runtime_open(child_pd, NULL);
    if (child_rt) {
      privdir_broker_runtime_clean(child_rt);
      privdir_clean(child_pd);
      _exit(1);
    }

    privdir_clean(child_pd);
    _exit(0);
  }

  int status = 0;
  ASSERT_TRUE(waitpid(pid, &status, 0) == pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_TRUE(WEXITSTATUS(status) == 0);

  privdir_broker_runtime_clean(rt);
  ASSERT_TRUE(rmdir(pd->app_dir) == 0);
  privdir_clean(pd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

int main(void) {
  test_resolve_with_env_var();
  test_resolve_explicit_base_uses_parent_dir();
  test_resolve_fallback();
  test_resolve_env_relative_path_uses_tmp_fallback();
  test_resolve_overlong_env_path_fails();
  test_resolve_rejects_relative_input_with_error();
  test_resolve_overlong_explicit_path_reports_error();
  test_broker_runtime_open_and_cleanup();
  test_broker_runtime_rejects_symlinked_app_dir();
  test_broker_runtime_blocks_second_process();

  fprintf(stderr, "OK: test_private_dir\n");
  return 0;
}
