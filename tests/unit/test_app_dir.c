#define _GNU_SOURCE

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "app_dir.h"
#include "file_io.h"
#include "test.h"

/* Helper: create a temporary directory via mkdtemp.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates a directory under /tmp.
 * Error semantics: asserts on setup failure and returns non-NULL path.
 */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_appdir_XXXXXX";
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
      snprintf(NULL, 0, "/tmp/%s-%u/", APPDIR_APP_DIRNAME, (unsigned)getuid());
  ASSERT_TRUE(n > 0);
  char *out = xmalloc((size_t)n + 1u);
  ASSERT_TRUE(snprintf(out, (size_t)n + 1u, "/tmp/%s-%u/", APPDIR_APP_DIRNAME,
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
    ASSERT_TRUE(0 && "unexpected leftover app-dir artifact");
  }

  closedir(d);
}

/* Helper: asserts the resolved paths match one expected app dir.
 * Ownership: borrows inputs; allocates temporary expected paths internally.
 * Side effects: allocates and frees temporary heap strings.
 * Error semantics: asserts on any mismatch.
 */
static void assert_resolved_layout(const AppDir *appd,
                                   const char *expected_app_dir) {
  ASSERT_TRUE(appd != NULL);
  ASSERT_TRUE(expected_app_dir != NULL);

  ASSERT_STREQ(appd->app_dir, expected_app_dir);

  char *expected_run = join_dir_path(expected_app_dir, APPDIR_RUN_DIRNAME);
  char *expected_secret =
      join_dir_path(expected_app_dir, APPDIR_SECRET_DIRNAME);
  char *expected_sock = path_join(expected_run, APPDIR_SOCK_FILENAME);
  char *expected_token = path_join(expected_secret, APPDIR_TOKEN_FILENAME);
  ASSERT_TRUE(expected_sock != NULL);
  ASSERT_TRUE(expected_token != NULL);

  ASSERT_STREQ(appd->run_dir, expected_run);
  ASSERT_STREQ(appd->secret_dir, expected_secret);
  ASSERT_STREQ(appd->sock_path, expected_sock);
  ASSERT_STREQ(appd->token_path, expected_token);

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

  AppDir *appd = appdir_resolve(NULL, NULL);
  ASSERT_TRUE(appd != NULL);

  char *expected_base = dup_dir_with_slash(tmpdir);
  char *expected_app = join_dir_path(expected_base, APPDIR_APP_DIRNAME);
  assert_resolved_layout(appd, expected_app);

  free(expected_app);
  free(expected_base);
  appdir_clean(appd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

/* Verifies explicit input is treated as the exact app directory path.
 */
static void test_resolve_explicit_app_dir_uses_exact_path(void) {
  char *tmpdir = make_tmpdir();
  char *app_dir = join_dir_path(tmpdir, "mounted-runtime");

  AppDir *appd = appdir_resolve(app_dir, NULL);
  ASSERT_TRUE(appd != NULL);

  assert_resolved_layout(appd, app_dir);
  appdir_clean(appd);
  free(app_dir);
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

  AppDir *appd = appdir_resolve(NULL, NULL);
  ASSERT_TRUE(appd != NULL);

  char *expected_app = make_tmp_fallback_app_dir();
  assert_resolved_layout(appd, expected_app);

  free(expected_app);
  appdir_clean(appd);
}

/* Verifies invalid env paths are ignored and resolution falls back to the
 * uid-scoped /tmp app-dir.
 */
static void test_resolve_env_relative_path_uses_tmp_fallback(void) {
#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_RUNTIME_DIR", "relative/path", 1) == 0);
#else
  ASSERT_TRUE(setenv("TMPDIR", "relative/path", 1) == 0);
#endif

  AppDir *appd = appdir_resolve(NULL, NULL);
  ASSERT_TRUE(appd != NULL);

  char *expected_app = make_tmp_fallback_app_dir();
  assert_resolved_layout(appd, expected_app);

  free(expected_app);
  appdir_clean(appd);
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

  AppDirErr err;
  ASSERT_TRUE(appdir_resolve(longpath, &err) == NULL);
  ASSERT_TRUE(err.msg[0] != '\0');
  ASSERT_TRUE(strstr(err.msg, "too long") != NULL);
}

/* Verifies invalid explicit input returns an explanatory error string. */
static void test_resolve_rejects_relative_input_with_error(void) {
  AppDirErr err;
  ASSERT_TRUE(appdir_resolve("relative/path", &err) == NULL);
  ASSERT_TRUE(err.msg[0] != '\0');
  ASSERT_TRUE(strstr(err.msg, "absolute path") != NULL);
}

/* Verifies overlong explicit input returns an explanatory error instead of
 * silently changing the selected base directory.
 */
static void test_resolve_overlong_explicit_path_reports_error(void) {
  char longpath[256];
  longpath[0] = '/';
  memset(longpath + 1, 'b', sizeof(longpath) - 2);
  longpath[sizeof(longpath) - 1] = '\0';

  AppDirErr err;
  ASSERT_TRUE(appdir_resolve(longpath, &err) == NULL);
  ASSERT_TRUE(err.msg[0] != '\0');
  ASSERT_TRUE(strstr(err.msg, "too long") != NULL);
}

/* Verifies explicit input rejects '/' so callers cannot select the filesystem
 * root as the broker runtime root.
 */
static void test_resolve_rejects_root_app_dir(void) {
  AppDirErr err;
  ASSERT_TRUE(appdir_resolve("/", &err) == NULL);
  ASSERT_TRUE(err.code == APPDIR_ERR_PATH);
  ASSERT_TRUE(err.msg[0] != '\0');
  ASSERT_TRUE(strstr(err.msg, "non-root") != NULL);
}

/* Verifies broker-runtime open creates trusted dirs, writes the token, keeps
 * the app dir in place, and runtime cleanup removes the transient artifacts.
 */
static void test_broker_runtime_open_and_cleanup(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  AppDir *appd = appdir_resolve(NULL, NULL);
  ASSERT_TRUE(appd != NULL);

  uint8_t issued[APPDIR_TOKEN_LEN] = {0};
  AppDirBrokerRuntime *rt = appdir_broker_runtime_open(appd, issued, NULL);
  ASSERT_TRUE(rt != NULL);

  struct stat st = {0};
  ASSERT_TRUE(stat(appd->app_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(appd->run_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(appd->secret_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  ASSERT_TRUE(stat(appd->token_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);
  ASSERT_TRUE(st.st_size == APPDIR_TOKEN_LEN);

  uint8_t disk_token[APPDIR_TOKEN_LEN] = {0};
  ASSERT_TRUE(
      fileio_read_exact(appd->token_path, APPDIR_TOKEN_LEN, disk_token) == OK);
  ASSERT_TRUE(memcmp(disk_token, issued, APPDIR_TOKEN_LEN) == 0);

  char *lock_path = path_join(appd->app_dir, APPDIR_LOCK_FILENAME);
  ASSERT_TRUE(lock_path != NULL);
  ASSERT_TRUE(stat(lock_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);

  appdir_broker_runtime_clean(rt);

  ASSERT_TRUE(stat(appd->token_path, &st) != 0);
  ASSERT_TRUE(stat(lock_path, &st) != 0);
  ASSERT_TRUE(stat(appd->secret_dir, &st) != 0);
  ASSERT_TRUE(stat(appd->run_dir, &st) != 0);
  ASSERT_TRUE(stat(appd->app_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  assert_dir_empty(appd->app_dir);

  ASSERT_TRUE(rmdir(appd->app_dir) == 0);
  free(lock_path);
  appdir_clean(appd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

/* Verifies broker runtime rejects an explicit app dir that already exists as a
 * regular file instead of creating runtime artifacts on the wrong inode type.
 */
static void test_broker_runtime_rejects_regular_file_app_dir(void) {
  char *tmpdir = make_tmpdir();
  char *app_path = path_join(tmpdir, "file-app");
  ASSERT_TRUE(app_path != NULL);

  uint8_t one = 0xA5u;
  ASSERT_TRUE(fileio_write_exact(app_path, &one, 1u, 0600) == OK);

  AppDir *appd = appdir_resolve(app_path, NULL);
  ASSERT_TRUE(appd != NULL);

  AppDirErr err;
  ASSERT_TRUE(appdir_broker_runtime_open(appd, NULL, &err) == NULL);
  ASSERT_TRUE(err.code == APPDIR_ERR_PATH);
  ASSERT_TRUE(strstr(err.msg, "not a directory") != NULL);

  appdir_clean(appd);
  ASSERT_TRUE(unlink(app_path) == 0);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(app_path);
  free(tmpdir);
}

/* Verifies broker runtime repairs an existing app dir with too-open
 * permissions instead of failing closed on a recoverable local setup issue.
 */
static void test_broker_runtime_repairs_existing_app_dir_permissions(void) {
  char *tmpdir = make_tmpdir();
  char *app_path = path_join(tmpdir, "preexisting-app");
  ASSERT_TRUE(app_path != NULL);
  ASSERT_TRUE(mkdir(app_path, 0755) == 0);

  AppDir *appd = appdir_resolve(app_path, NULL);
  ASSERT_TRUE(appd != NULL);

  AppDirErr err;
  AppDirBrokerRuntime *rt = appdir_broker_runtime_open(appd, NULL, &err);
  ASSERT_TRUE(rt != NULL);
  ASSERT_TRUE(err.code == APPDIR_ERR_NONE);
  ASSERT_TRUE(err.msg[0] == '\0');

  struct stat st = {0};
  ASSERT_TRUE(stat(appd->app_dir, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0700);

  appdir_broker_runtime_clean(rt);
  ASSERT_TRUE(rmdir(appd->app_dir) == 0);
  appdir_clean(appd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(app_path);
  free(tmpdir);
}

/* Verifies broker runtime reports a missing parent directory when explicit
 * app-dir input points under a path tree the broker cannot create.
 */
static void test_broker_runtime_fails_when_app_dir_parent_missing(void) {
  char *tmpdir = make_tmpdir();
  char *missing_parent = path_join(tmpdir, "missing-parent");
  ASSERT_TRUE(missing_parent != NULL);
  char *app_path = path_join(missing_parent, "nested-app");
  ASSERT_TRUE(app_path != NULL);

  AppDir *appd = appdir_resolve(app_path, NULL);
  ASSERT_TRUE(appd != NULL);

  AppDirErr err;
  ASSERT_TRUE(appdir_broker_runtime_open(appd, NULL, &err) == NULL);
  ASSERT_TRUE(err.code == APPDIR_ERR_RUNTIME);
  ASSERT_TRUE(strstr(err.msg, "failed to create app-dir app path") != NULL);

  appdir_clean(appd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(app_path);
  free(missing_parent);
  free(tmpdir);
}

/* Verifies broker runtime refuses a symlinked explicit app dir, blocking
 * final-component path substitution before any broker artifact is created.
 */
static void test_broker_runtime_rejects_symlinked_app_dir(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  char *target = path_join(tmpdir, "real-app");
  ASSERT_TRUE(target != NULL);
  ASSERT_TRUE(mkdir(target, 0700) == 0);

  char *app_link = path_join(tmpdir, APPDIR_APP_DIRNAME);
  ASSERT_TRUE(app_link != NULL);
  ASSERT_TRUE(symlink(target, app_link) == 0);

  AppDir *appd = appdir_resolve(app_link, NULL);
  ASSERT_TRUE(appd != NULL);
  ASSERT_TRUE(appdir_broker_runtime_open(appd, NULL, NULL) == NULL);

  appdir_clean(appd);
  remove_path_if_exists(app_link);
  ASSERT_TRUE(rmdir(target) == 0);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(app_link);
  free(target);
  free(tmpdir);
}

/* Verifies broker runtime rejects child paths that already exist as the wrong
 * inode type instead of treating them as trusted runtime directories.
 */
static void test_broker_runtime_rejects_regular_file_run_child(void) {
  char *tmpdir = make_tmpdir();
  char *app_path = path_join(tmpdir, "child-file-app");
  ASSERT_TRUE(app_path != NULL);
  ASSERT_TRUE(mkdir(app_path, 0700) == 0);

  char *run_path = path_join(app_path, APPDIR_RUN_DIRNAME);
  ASSERT_TRUE(run_path != NULL);
  uint8_t byte = 0x11u;
  ASSERT_TRUE(fileio_write_exact(run_path, &byte, 1u, 0600) == OK);

  AppDir *appd = appdir_resolve(app_path, NULL);
  ASSERT_TRUE(appd != NULL);

  AppDirErr err;
  ASSERT_TRUE(appdir_broker_runtime_open(appd, NULL, &err) == NULL);
  ASSERT_TRUE(err.code == APPDIR_ERR_RUNTIME);
  ASSERT_TRUE(strstr(err.msg, APPDIR_RUN_DIRNAME) != NULL);

  appdir_clean(appd);
  ASSERT_TRUE(unlink(run_path) == 0);
  ASSERT_TRUE(rmdir(app_path) == 0);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(run_path);
  free(app_path);
  free(tmpdir);
}

/* Verifies broker runtime rejects a symlinked child runtime directory so the
 * broker never redirects secret-bearing artifacts outside the trusted app dir.
 */
static void test_broker_runtime_rejects_symlinked_secret_child(void) {
  char *tmpdir = make_tmpdir();
  char *app_path = path_join(tmpdir, "child-link-app");
  ASSERT_TRUE(app_path != NULL);
  ASSERT_TRUE(mkdir(app_path, 0700) == 0);

  char *target = path_join(tmpdir, "real-secret-dir");
  ASSERT_TRUE(target != NULL);
  ASSERT_TRUE(mkdir(target, 0700) == 0);

  char *secret_link = path_join(app_path, APPDIR_SECRET_DIRNAME);
  ASSERT_TRUE(secret_link != NULL);
  ASSERT_TRUE(symlink(target, secret_link) == 0);

  AppDir *appd = appdir_resolve(app_path, NULL);
  ASSERT_TRUE(appd != NULL);

  AppDirErr err;
  ASSERT_TRUE(appdir_broker_runtime_open(appd, NULL, &err) == NULL);
  // since we open(2) with O_DIRECTORY we don't get ELOOP in case of a symlink,
  // just a generic ENOTDIR
  ASSERT_TRUE(err.code == APPDIR_ERR_RUNTIME);
  ASSERT_TRUE(strstr(err.msg, secret_link) != NULL);

  appdir_clean(appd);
  remove_path_if_exists(secret_link);
  ASSERT_TRUE(rmdir(target) == 0);
  ASSERT_TRUE(rmdir(app_path) == 0);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(secret_link);
  free(target);
  free(app_path);
  free(tmpdir);
}

/* Verifies one process holding the broker runtime lock blocks a second process
 * from opening the same runtime. fcntl locks are process-scoped, so this must
 * be asserted cross-process rather than by reopening in the same process.
 */
static void test_broker_runtime_blocks_second_process(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  AppDir *appd = appdir_resolve(NULL, NULL);
  ASSERT_TRUE(appd != NULL);

  AppDirBrokerRuntime *rt = appdir_broker_runtime_open(appd, NULL, NULL);
  ASSERT_TRUE(rt != NULL);

  pid_t pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    AppDir *child_appd = appdir_resolve(appd->app_dir, NULL);
    if (!child_appd)
      _exit(2);

    AppDirBrokerRuntime *child_rt =
        appdir_broker_runtime_open(child_appd, NULL, NULL);
    if (child_rt) {
      appdir_broker_runtime_clean(child_rt);
      appdir_clean(child_appd);
      _exit(1);
    }

    appdir_clean(child_appd);
    _exit(0);
  }

  int status = 0;
  ASSERT_TRUE(waitpid(pid, &status, 0) == pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_TRUE(WEXITSTATUS(status) == 0);

  appdir_broker_runtime_clean(rt);
  ASSERT_TRUE(rmdir(appd->app_dir) == 0);
  appdir_clean(appd);
  ASSERT_TRUE(rmdir(tmpdir) == 0);
  free(tmpdir);
}

int main(void) {
  test_resolve_with_env_var();
  test_resolve_explicit_app_dir_uses_exact_path();
  test_resolve_fallback();
  test_resolve_env_relative_path_uses_tmp_fallback();
  test_resolve_overlong_env_path_fails();
  test_resolve_rejects_relative_input_with_error();
  test_resolve_overlong_explicit_path_reports_error();
  test_resolve_rejects_root_app_dir();
  test_broker_runtime_open_and_cleanup();
  test_broker_runtime_rejects_regular_file_app_dir();
  test_broker_runtime_repairs_existing_app_dir_permissions();
  test_broker_runtime_fails_when_app_dir_parent_missing();
  test_broker_runtime_rejects_symlinked_app_dir();
  test_broker_runtime_rejects_regular_file_run_child();
  test_broker_runtime_rejects_symlinked_secret_child();
  test_broker_runtime_blocks_second_process();

  fprintf(stderr, "OK: test_app_dir\n");
  return 0;
}
