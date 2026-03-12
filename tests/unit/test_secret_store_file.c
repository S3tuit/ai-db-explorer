#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "file_io.h"
#include "secret_store.h"
#include "string_op.h"
#include "test.h"

#define TEST_NAMESPACE "TestNamespace"
#define OTHER_NAMESPACE "OtherNamespace"
#define TEST_REF_NS(ns, name)                                                  \
  (&(SecretRefInfo){.cred_namespace = (ns), .connection_name = (name)})
#define TEST_REF(name) TEST_REF_NS(TEST_NAMESPACE, name)

typedef struct {
  EnvGuard env;
  char *tmp;
  char *cred_path;
  SecretStore *ss;
} FileStoreCtx;

/* Returns the expected app directory when XDG_CONFIG_HOME is used directly.
 * It borrows 'tmp' and returns one owned path.
 */
static char *xdg_app_dir_path(const char *tmp) {
  return path_join(tmp, "adbxplorer");
}

/* Returns the expected credentials file path when XDG_CONFIG_HOME is used.
 * It borrows 'tmp' and returns one owned path.
 */
static char *xdg_cred_path(const char *tmp) {
  char *dir = xdg_app_dir_path(tmp);
  char *cred = path_join(dir, "credentials.json");
  free(dir);
  return cred;
}

/* Returns the expected app directory when HOME fallback is used.
 * It borrows 'tmp' and returns one owned path.
 */
static char *home_app_dir_path(const char *tmp) {
#ifdef __APPLE__
  char *base = path_join(tmp, "Library/Application Support");
#else
  char *base = path_join(tmp, ".config");
#endif
  char *dir = path_join(base, "adbxplorer");
  free(base);
  return dir;
}

/* Returns the expected credentials path when HOME fallback is used.
 * It borrows 'tmp' and returns one owned path.
 */
static char *home_cred_path(const char *tmp) {
  char *dir = home_app_dir_path(tmp);
  char *cred = path_join(dir, "credentials.json");
  free(dir);
  return cred;
}

/* Returns the expected app directory for the current platform-default policy.
 * It borrows 'tmp' and returns one owned path.
 */
static char *default_app_dir_path(const char *tmp) {
#ifdef __linux__
  return xdg_app_dir_path(tmp);
#else
  return home_app_dir_path(tmp);
#endif
}

/* Returns the expected credentials path for the current platform-default
 * policy. It borrows 'tmp' and returns one owned path.
 */
static char *default_cred_path(const char *tmp) {
#ifdef __linux__
  return xdg_cred_path(tmp);
#else
  return home_cred_path(tmp);
#endif
}

/* Removes one file path if present (best effort).
 * It borrows 'path' and ignores ENOENT.
 * Side effects: filesystem mutation.
 */
static void unlink_if_exists(const char *path) {
  if (!path)
    return;
  if (unlink(path) == 0)
    return;
  if (errno == ENOENT)
    return;
}

/* Removes one directory path if present (best effort).
 * It borrows 'path' and ignores ENOENT/ENOTEMPTY.
 * Side effects: filesystem mutation.
 */
static void rmdir_if_exists(const char *path) {
  if (!path)
    return;
  if (rmdir(path) == 0)
    return;
  if (errno == ENOENT || errno == ENOTEMPTY)
    return;
}

/* Cleans test temporary tree paths for both XDG and HOME fallback layouts.
 * It borrows 'tmp' and performs best-effort cleanup.
 * Side effects: filesystem mutation.
 */
static void cleanup_tmp_tree(const char *tmp) {
  if (!tmp)
    return;

  char *xdg_app = xdg_app_dir_path(tmp);
  char *xdg_cred = xdg_cred_path(tmp);
  char *home_app = home_app_dir_path(tmp);
  char *home_cred = home_cred_path(tmp);
  char *home_base = NULL;
#ifdef __APPLE__
  home_base = path_join(tmp, "Library");
#else
  home_base = path_join(tmp, ".config");
#endif
  char *target = path_join(tmp, "symlink_target.json");

  unlink_if_exists(target);
  unlink_if_exists(xdg_cred);
  unlink_if_exists(home_cred);

  rmdir_if_exists(xdg_cred);
  rmdir_if_exists(home_cred);
  rmdir_if_exists(xdg_app);
  rmdir_if_exists(home_app);
  rmdir_if_exists(home_base);
  rmdir_if_exists(tmp);

  free(target);
  free(xdg_app);
  free(xdg_cred);
  free(home_app);
  free(home_cred);
  free(home_base);
}

/* Returns YES if 's' contains only decimal digits and is non-empty.
 * It borrows 's' and performs no allocations.
 * Error semantics: returns YES/NO.
 */
static AdbxTriStatus is_uint_str(const char *s) {
  if (!s || s[0] == '\0')
    return NO;
  for (size_t i = 0; s[i] != '\0'; i++) {
    if (s[i] < '0' || s[i] > '9')
      return NO;
  }
  return YES;
}

/* Closes one open fd whose resolved path matches 'path' exactly.
 * It borrows 'path' and performs best-effort scan on /proc/self/fd then
 * /dev/fd. Side effects: closes one live fd on success.
 * Error semantics: returns YES when one fd is found+closed, NO when not found,
 * ERR on invalid input.
 */
static AdbxTriStatus close_fd_for_path(const char *path) {
  if (!path || path[0] == '\0')
    return ERR;

  const char *scan_dirs[] = {"/proc/self/fd", "/dev/fd"};
  for (size_t d = 0; d < ARRLEN(scan_dirs); d++) {
    DIR *dir = opendir(scan_dirs[d]);
    if (!dir)
      continue;

    int skip_fd = dirfd(dir);
    for (;;) {
      struct dirent *ent = readdir(dir);
      if (!ent)
        break;
      if (is_uint_str(ent->d_name) != YES)
        continue;

      int fd = atoi(ent->d_name);
      if (fd < 0 || fd == skip_fd)
        continue;

      char link_path[128];
      int n = snprintf(link_path, sizeof(link_path), "%s/%s", scan_dirs[d],
                       ent->d_name);
      if (n <= 0 || (size_t)n >= sizeof(link_path))
        continue;

      char target[PATH_MAX];
      ssize_t got = readlink(link_path, target, sizeof(target) - 1);
      if (got < 0)
        continue;
      target[(size_t)got] = '\0';

      if (strcmp(target, path) != 0)
        continue;

      closedir(dir);
      ASSERT_TRUE(close(fd) == 0);
      return YES;
    }
    closedir(dir);
  }

  return NO;
}

/* Ensures one directory tree exists (mkdir -p semantics).
 * It borrows 'dir_path' and mutates a temporary copy.
 * Side effects: creates missing directories.
 */
static void ensure_dir_tree(const char *dir_path) {
  ASSERT_TRUE(dir_path != NULL);
  char *tmp = dup_or_null(dir_path);
  ASSERT_TRUE(tmp != NULL);

  size_t len = strlen(tmp);
  for (size_t i = 1; i < len; i++) {
    if (tmp[i] != '/')
      continue;
    tmp[i] = '\0';
    if (tmp[0] != '\0') {
      int rc = mkdir(tmp, 0700);
      ASSERT_TRUE(rc == 0 || errno == EEXIST);
    }
    tmp[i] = '/';
  }
  int rc = mkdir(tmp, 0700);
  ASSERT_TRUE(rc == 0 || errno == EEXIST);
  free(tmp);
}

/* Writes one UTF-8 text payload as mode 0600.
 * It borrows inputs and overwrites destination path.
 * Side effects: filesystem mutation.
 */
static void write_text_0600(const char *path, const char *text) {
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(text != NULL);
  ASSERT_TRUE(fileio_write_exact(path, (const uint8_t *)text, strlen(text),
                                 0600) == OK);
}

/* Asserts path is a regular file and non-empty.
 * It borrows 'path' and performs stat checks.
 * Side effects: filesystem reads.
 */
static void assert_nonempty_regular_file(const char *path) {
  struct stat st = {0};
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(lstat(path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE(st.st_size > 0);
}

/* Asserts backend reported parse-category error with a non-empty message.
 * It borrows 'ss' and reads backend error state.
 */
static void assert_parse_error(SecretStore *ss) {
  ASSERT_TRUE(secret_store_last_error_code(ss) == SSERR_PARSE);
  const char *msg = secret_store_last_error(ss);
  ASSERT_TRUE(msg != NULL);
  ASSERT_TRUE(msg[0] != '\0');
}

/* Initializes one isolated file-store context rooted at 'ctx->tmp' according
 * to the current platform default policy.
 * It writes owned resources in 'ctx'.
 * Side effects: environment changes and filesystem creation.
 */
static void ctx_open_xdg(FileStoreCtx *ctx) {
  ASSERT_TRUE(ctx != NULL);
  memset(ctx, 0, sizeof(*ctx));

  env_guard_begin(&ctx->env);
  ctx->tmp = make_tmp_dir();

#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", ctx->tmp, 1) == 0);
#else
  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(setenv("HOME", ctx->tmp, 1) == 0);
  char *home_base = path_join(ctx->tmp, "Library/Application Support");
  ASSERT_TRUE(home_base != NULL);
  ensure_dir_tree(home_base);
  free(home_base);
#endif

  ctx->cred_path = default_cred_path(ctx->tmp);
  ASSERT_TRUE(ctx->cred_path != NULL);

  ctx->ss = secret_store_file_backend_create();
  ASSERT_TRUE(ctx->ss != NULL);
}

/* Cleans one file-store context opened by ctx_open_xdg.
 * It consumes resources in 'ctx'.
 * Side effects: environment/filesystem cleanup.
 */
static void ctx_close(FileStoreCtx *ctx) {
  if (!ctx)
    return;
  if (ctx->ss)
    secret_store_destroy(ctx->ss);
  free(ctx->cred_path);
  env_guard_end(&ctx->env);
  cleanup_tmp_tree(ctx->tmp);
  free(ctx->tmp);
  memset(ctx, 0, sizeof(*ctx));
}

/* Verifies missing file + set creates credentials.json with non-zero size. */
static void test_missing_file_set_creates_nonempty(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  struct stat st = {0};
  ASSERT_TRUE(lstat(ctx.cred_path, &st) != 0);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == OK);
  assert_nonempty_regular_file(ctx.cred_path);

  // deletes the secret store while keeping the context
  secret_store_destroy(ctx.ss);
  ctx.ss = NULL;
  ctx.ss = secret_store_file_backend_create();
  ASSERT_TRUE(ctx.ss != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies set overwrite the secret if called with a ref that's already
 * present. */
static void test_set_overwrite_current_secret(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  struct stat st = {0};
  ASSERT_TRUE(lstat(ctx.cred_path, &st) != 0);

  StrBuf out;

  sb_init(&out);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == OK);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");
  sb_zero_clean(&out);

  sb_init(&out);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "second") == OK);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "second");
  sb_zero_clean(&out);

  sb_init(&out);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-3") == OK);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-3");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies missing file + get returns NO. */
static void test_missing_file_get_returns_no(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == NO);
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies missing file + delete returns OK. */
static void test_missing_file_delete_returns_ok(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_delete(ctx.ss, TEST_REF("MyPostgres")) == OK);
  ctx_close(&ctx);
}

/* Verifies missing file + wipe_all is a successful no-op and later set/get
 * work.
 */
static void test_missing_file_wipe_all_is_noop_and_later_set_get_works(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_wipe_all(ctx.ss) == OK);
  struct stat st = {0};
  ASSERT_TRUE(lstat(ctx.cred_path, &st) != 0);
  ASSERT_TRUE(errno == ENOENT);

  // deletes the secret store while keeping the context
  secret_store_destroy(ctx.ss);
  ctx.ss = NULL;
  ctx.ss = secret_store_file_backend_create();
  ASSERT_TRUE(ctx.ss != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == NO);
  ASSERT_TRUE(
      secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-after-wipe") == OK);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-after-wipe");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies missing file + wipe_namespace is a successful no-op and later
 * namespace-scoped set/get still work.
 */
static void test_missing_file_wipe_namespace_is_noop_and_later_set_get_works(
    void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE) == OK);
  struct stat st = {0};
  ASSERT_TRUE(lstat(ctx.cred_path, &st) != 0);
  ASSERT_TRUE(errno == ENOENT);

  secret_store_destroy(ctx.ss);
  ctx.ss = NULL;
  ctx.ss = secret_store_file_backend_create();
  ASSERT_TRUE(ctx.ss != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == NO);
  ASSERT_TRUE(
      secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-after-wipe") == OK);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               "pw-other") == OK);

  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-after-wipe");
  sb_zero_clean(&out);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               &out) == YES);
  ASSERT_STREQ(out.data, "pw-other");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies wipe_namespace deletes only entries in the selected namespace and
 * keeps unrelated namespaces intact across reopen.
 */
static void test_wipe_namespace_removes_only_target_namespace(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("KeepPg"), "pw-keep") == OK);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("GonePg"), "pw-gone") == OK);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               "pw-other") == OK);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE) == OK);

  secret_store_destroy(ctx.ss);
  ctx.ss = NULL;
  ctx.ss = secret_store_file_backend_create();
  ASSERT_TRUE(ctx.ss != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("KeepPg"), &out) == NO);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("GonePg"), &out) == NO);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               &out) == YES);
  ASSERT_STREQ(out.data, "pw-other");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies wiping a namespace that is not present is a no-op.
 * It borrows the current store and confirms existing refs remain readable.
 */
static void test_wipe_namespace_missing_namespace_is_noop(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-main") ==
              OK);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               "pw-other") == OK);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, "MissingNamespace") == OK);

  secret_store_destroy(ctx.ss);
  ctx.ss = NULL;
  ctx.ss = secret_store_file_backend_create();
  ASSERT_TRUE(ctx.ss != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-main");
  sb_zero_clean(&out);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               &out) == YES);
  ASSERT_STREQ(out.data, "pw-other");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies wipe_namespace rejects invalid namespace inputs with SSERR_INPUT.
 */
static void test_wipe_namespace_invalid_namespace_reports_input_error(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, NULL) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_INPUT);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, "") == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_INPUT);

  ctx_close(&ctx);
}

/* Verifies zero-byte credentials file fails closed with parse error. */
static void test_zero_size_credentials_file_fails_closed(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(fileio_write_exact(ctx.cred_path, NULL, 0, 0600) == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  assert_parse_error(ctx.ss);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == ERR);
  assert_parse_error(ctx.ss);
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies file truncated to empty after valid load fails closed and does not
 * silently reset.
 */
static void test_truncate_after_valid_load_fails_closed(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  ASSERT_TRUE(fileio_write_exact(ctx.cred_path, NULL, 0, 0600) == OK);

  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  assert_parse_error(ctx.ss);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("AnotherPostgres"), "pw-2") ==
              ERR);
  assert_parse_error(ctx.ss);
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Runs one schema-violation payload and expects parse-category error. */
static void run_schema_violation_case(const char *json, const char *name) {
  ASSERT_TRUE(json != NULL);
  ASSERT_TRUE(name != NULL);

  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  write_text_0600(ctx.cred_path, json);

  StrBuf out;
  sb_init(&out);
  AdbxTriStatus rc = secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out);
  if (rc != ERR) {
    fprintf(stderr, "schema case '%s' expected ERR, got %d\n", name, (int)rc);
    exit(1);
  }
  if (secret_store_last_error_code(ctx.ss) != SSERR_PARSE) {
    fprintf(stderr, "schema case '%s' expected SSERR_PARSE, got %d\n", name,
            (int)secret_store_last_error_code(ctx.ss));
    exit(1);
  }
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies parse-category errors for key schema violations. */
static void test_json_schema_violations_are_parse(void) {
  run_schema_violation_case("{\"entries\":[]}", "missing-version");
  run_schema_violation_case("{\"version\":\"2\",\"entries\":[]}",
                            "wrong-version");
  run_schema_violation_case(
      "{\"version\":\"1\",\"entries\":[{\"connectionName\":\"x\","
      "\"secret\":\"pw\"}]}",
      "missing-namespace");
  run_schema_violation_case(
      "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
      "\"TestNamespace\",\"secret\":\"pw\"}]}",
      "missing-connection-name");
  run_schema_violation_case(
      "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
      "\"TestNamespace\",\"connectionName\":\"x\"}]}",
      "missing-secret");
  run_schema_violation_case(
      "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":\"\","
      "\"connectionName\":\"x\",\"secret\":\"x\"}]}",
      "empty-namespace");
  run_schema_violation_case(
      "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
      "\"TestNamespace\",\"connectionName\":\"\",\"secret\":\"x\"}]}",
      "empty-connection-name");
  run_schema_violation_case("{\"version\":\"1\",\"entries\":[],\"extra\":1}",
                            "unknown-top-key");
  run_schema_violation_case(
      "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
      "\"TestNamespace\",\"connectionName\":\"x\",\"secret\":\"y\",\"k\":1}]}",
      "unknown-entry-key");
}

/* Verifies symlink at credentials path fails closed with O_NOFOLLOW path. */
static void test_symlink_credentials_path_fails(void) {
#ifdef O_NOFOLLOW
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  char *target = path_join(ctx.tmp, "symlink_target.json");
  write_text_0600(target,
                  "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
                  "\"TestNamespace\",\"connectionName\":\"MyPostgres\","
                  "\"secret\":\"pw-target\"}]}");
  ASSERT_TRUE(symlink(target, ctx.cred_path) == 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_CRED_FILE);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-new") ==
              ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_CRED_FILE);
  sb_zero_clean(&out);

  free(target);
  ctx_close(&ctx);
#endif
}

/* Verifies non-regular credentials path (directory) fails closed. */
static void test_directory_at_credentials_path_fails(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(mkdir(ctx.cred_path, 0700) == 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_CRED_FILE);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-new") ==
              ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_CRED_FILE);
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies Linux uses XDG_CONFIG_HOME directly, while macOS ignores it and
 * uses the HOME fallback tree instead.
 */
static void test_xdg_absolute_path_used_directly(void) {
  EnvGuard env;
  env_guard_begin(&env);

  char *tmp = make_tmp_dir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmp, 1) == 0);

#ifdef __APPLE__
  char *home_base = path_join(tmp, "Library/Application Support");
  ASSERT_TRUE(home_base != NULL);
  ensure_dir_tree(home_base);
  free(home_base);
#endif

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss != NULL);

#ifdef __linux__
  char *app_xdg = xdg_app_dir_path(tmp);
  char *cred_xdg = xdg_cred_path(tmp);
  char *app_wrong = home_app_dir_path(tmp);
#else
  char *app_xdg = home_app_dir_path(tmp);
  char *cred_xdg = home_cred_path(tmp);
  char *app_wrong = xdg_app_dir_path(tmp);
#endif

  struct stat st = {0};
  ASSERT_TRUE(lstat(app_xdg, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));
  ASSERT_TRUE(lstat(app_wrong, &st) != 0);

  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-1") == OK);
  assert_nonempty_regular_file(cred_xdg);

  secret_store_destroy(ss);
  free(app_xdg);
  free(cred_xdg);
  free(app_wrong);
  env_guard_end(&env);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies HOME fallback path is used when XDG_CONFIG_HOME is unset. */
static void test_home_fallback_path_used(void) {
  EnvGuard env;
  env_guard_begin(&env);

  char *tmp = make_tmp_dir();
  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(setenv("HOME", tmp, 1) == 0);

#ifdef __APPLE__
  char *home_base = path_join(tmp, "Library/Application Support");
#else
  char *home_base = path_join(tmp, ".config");
#endif
  ensure_dir_tree(home_base);
  free(home_base);

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss != NULL);
  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-home") == OK);

  char *cred_home = home_cred_path(tmp);
  char *cred_xdg = xdg_cred_path(tmp);
  struct stat st = {0};
  ASSERT_TRUE(lstat(cred_home, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE(lstat(cred_xdg, &st) != 0);

  secret_store_destroy(ss);
  free(cred_home);
  free(cred_xdg);
  env_guard_end(&env);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies invalid env causes backend create/probe failure. */
static void test_invalid_env_create_and_probe_fail(void) {
  EnvGuard env;
  env_guard_begin(&env);

  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", "relative/path", 1) == 0);
  ASSERT_TRUE(unsetenv("HOME") == 0);

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss == NULL);

  SecretStore *out = NULL;
  ASSERT_TRUE(secret_store_file_backend_probe(&out) == ERR);
  ASSERT_TRUE(out == NULL);

  env_guard_end(&env);
}

/* Verifies external credentials file deletion invalidates cache to missing. */
static void test_external_file_deletion_invalidation(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  ASSERT_TRUE(unlink(ctx.cred_path) == 0);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == NO);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-2") == OK);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-2");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies malformed external rewrite returns ERR and does not silently
 * replace cache.
 */
static void test_external_malformed_rewrite_fails_closed(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  write_text_0600(ctx.cred_path, "{\"version\":");

  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  assert_parse_error(ctx.ss);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("AnotherPostgres"), "pw-2") ==
              ERR);
  assert_parse_error(ctx.ss);

  write_text_0600(ctx.cred_path,
                  "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
                  "\"TestNamespace\",\"connectionName\":\"MyPostgres\","
                  "\"secret\":\"pw-new\"}]}");
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-new");
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

#define CHILD_OK 0
#define CHILD_ERR 1

/* Verifies lock contention during set reports SSERR_WRITE. */
static void test_set_lock_contention_reports_write_error(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == OK);

  char *cred_path = default_cred_path(ctx.tmp);
  ASSERT_TRUE(cred_path != NULL);
  int n = snprintf(NULL, 0, "%s.lock", cred_path);
  ASSERT_TRUE(n > 0);
  char *lock_path = xmalloc((size_t)n + 1u);
  ASSERT_TRUE(snprintf(lock_path, (size_t)n + 1u, "%s.lock", cred_path) == n);
  free(cred_path);
  ASSERT_TRUE(lock_path != NULL);
  int lfd = open(lock_path, O_CREAT | O_WRONLY, 0600);
  ASSERT_TRUE(lfd >= 0);

  struct flock fl = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
  ASSERT_TRUE(fcntl(lfd, F_SETLK, &fl) == 0);

  pid_t pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    AdbxStatus rc = secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-2");
    if (rc == ERR && secret_store_last_error_code(ctx.ss) == SSERR_WRITE)
      _exit(CHILD_OK);
    _exit(CHILD_ERR);
  }

  int status = 0;
  ASSERT_TRUE(waitpid(pid, &status, 0) == pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_TRUE(WEXITSTATUS(status) == CHILD_OK);

  ASSERT_TRUE(close(lfd) == 0);
  unlink_if_exists(lock_path);
  free(lock_path);
  ctx_close(&ctx);
}

/* Verifies a stale lock file (without active fcntl lock) does not block
 * writes.
 */
static void test_stale_lock_file_does_not_block_set(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  char *cred_path = default_cred_path(ctx.tmp);
  ASSERT_TRUE(cred_path != NULL);
  int n = snprintf(NULL, 0, "%s.lock", cred_path);
  ASSERT_TRUE(n > 0);
  char *lock_path = xmalloc((size_t)n + 1u);
  ASSERT_TRUE(snprintf(lock_path, (size_t)n + 1u, "%s.lock", cred_path) == n);
  free(cred_path);
  ASSERT_TRUE(lock_path != NULL);

  int lfd = open(lock_path, O_CREAT | O_WRONLY, 0600);
  ASSERT_TRUE(lfd >= 0);
  ASSERT_TRUE(close(lfd) == 0);

  struct stat st = {0};
  ASSERT_TRUE(lstat(lock_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-stale") ==
              OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-stale");
  sb_zero_clean(&out);

  ASSERT_TRUE(lstat(lock_path, &st) != 0);
  ASSERT_TRUE(errno == ENOENT);

  free(lock_path);
  ctx_close(&ctx);
}

/* Verifies missing HOME and XDG config env produces SSERR_ENV on calls.
 */
static void test_calls_report_env_error_when_env_missing(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  char *app_dir = default_app_dir_path(ctx.tmp);
  ASSERT_TRUE(close_fd_for_path(app_dir) == YES);
  free(app_dir);

  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(unsetenv("HOME") == 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_ENV);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_ENV);
  ASSERT_TRUE(secret_store_delete(ctx.ss, TEST_REF("MyPostgres")) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_ENV);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_ENV);
  ASSERT_TRUE(secret_store_wipe_all(ctx.ss) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_ENV);
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

/* Verifies removing config directory after init reports SSERR_DIR on calls.
 */
static void test_calls_report_dir_error_after_config_dir_deleted(void) {
  FileStoreCtx ctx;
  ctx_open_xdg(&ctx);

  char *app_dir = default_app_dir_path(ctx.tmp);
  ASSERT_TRUE(close_fd_for_path(app_dir) == YES);

  unlink_if_exists(ctx.cred_path);
  ASSERT_TRUE(rmdir(app_dir) == 0);
  ASSERT_TRUE(rmdir(ctx.tmp) == 0);
  free(app_dir);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_DIR);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1") == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_DIR);
  ASSERT_TRUE(secret_store_delete(ctx.ss, TEST_REF("MyPostgres")) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_DIR);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_DIR);
  ASSERT_TRUE(secret_store_wipe_all(ctx.ss) == ERR);
  ASSERT_TRUE(secret_store_last_error_code(ctx.ss) == SSERR_DIR);
  sb_zero_clean(&out);

  ctx_close(&ctx);
}

int main(void) {
  test_missing_file_set_creates_nonempty();
  test_set_overwrite_current_secret();
  test_missing_file_get_returns_no();
  test_missing_file_delete_returns_ok();
  test_missing_file_wipe_all_is_noop_and_later_set_get_works();
  test_missing_file_wipe_namespace_is_noop_and_later_set_get_works();
  test_wipe_namespace_removes_only_target_namespace();
  test_wipe_namespace_missing_namespace_is_noop();
  test_wipe_namespace_invalid_namespace_reports_input_error();
  test_zero_size_credentials_file_fails_closed();
  test_truncate_after_valid_load_fails_closed();
  test_json_schema_violations_are_parse();
  test_symlink_credentials_path_fails();
  test_directory_at_credentials_path_fails();
  test_xdg_absolute_path_used_directly();
  test_home_fallback_path_used();
  test_invalid_env_create_and_probe_fail();
  test_external_file_deletion_invalidation();
  test_external_malformed_rewrite_fails_closed();
  test_set_lock_contention_reports_write_error();
  test_stale_lock_file_does_not_block_set();
  test_calls_report_env_error_when_env_missing();
  test_calls_report_dir_error_after_config_dir_deleted();

  fprintf(stderr, "OK: test_secret_store_file\n");
  return 0;
}
