#define _GNU_SOURCE

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "config_dir.h"
#include "conn_catalog.h"
#include "file_io.h"
#include "test.h"

/* Creates one temporary directory path.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates filesystem directory under /tmp.
 * Error semantics: asserts on failure and returns non-NULL path.
 */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_cfg_store_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  char *copy = strdup(dir);
  ASSERT_TRUE(copy != NULL);
  return copy;
}

/* Removes one config file and its app-owned parent tree best-effort.
 * It borrows 'config_path' and removes known parent directories best-effort.
 * Side effects: unlinks one file and rmdir on up to four parent directories.
 * Error semantics: none (best-effort cleanup helper).
 */
static void cleanup_config_path_tree(const char *config_path) {
  if (!config_path)
    return;
  (void)unlink(config_path);

  char *tmp = strdup(config_path);
  if (!tmp)
    return;
  for (int i = 0; i < 4; i++) {
    char *slash = strrchr(tmp, '/');
    if (!slash)
      break;
    *slash = '\0';
    if (tmp[0] == '\0')
      break;
    (void)rmdir(tmp);
  }
  free(tmp);
}

/* Creates one directory if missing.
 * It borrows 'path' and performs one mkdir call.
 * Side effects: may create one filesystem directory.
 * Error semantics: asserts on unexpected mkdir failure.
 */
static void ensure_dir_exists(const char *path) {
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(mkdir(path, 0700) == 0 || errno == EEXIST);
}

#ifdef __APPLE__
/* Creates the current platform's default config base directory under HOME.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates the platform default parent directories.
 * Error semantics: asserts on failure and returns non-NULL path.
 */
static char *make_platform_base_dir(const char *home) {
  ASSERT_TRUE(home != NULL);

  char *lib = path_join(home, "Library");
  ASSERT_TRUE(lib != NULL);
  ensure_dir_exists(lib);

  char *base = path_join(lib, "Application Support");
  ASSERT_TRUE(base != NULL);
  ensure_dir_exists(base);
  free(lib);
  return base;
}
#endif

/* Verifies the shared default app-dir opener follows the platform policy.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_app_dir_open(void) {
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmpdir, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

  ConfDir dir = {.fd = -1, .path = NULL};
  ConfDirErrCode code = CONFDIR_ERR_NONE;
  char *err = NULL;
  ASSERT_TRUE(confdir_default_open(&dir, &code, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(code == CONFDIR_ERR_NONE);
  ASSERT_TRUE(dir.fd >= 0);
  ASSERT_TRUE(dir.path != NULL);

#ifdef __APPLE__
  char expected[512];
  snprintf(expected, sizeof(expected),
           "%s/Library/Application Support/adbxplorer", tmpdir);
#else
  char expected[512];
  snprintf(expected, sizeof(expected), "%s/adbxplorer", tmpdir);
#endif
  ASSERT_TRUE(strcmp(dir.path, expected) == 0);

  struct stat st = {0};
  ASSERT_TRUE(fstat(dir.fd, &st) == 0);
  ASSERT_TRUE(S_ISDIR(st.st_mode));

  (void)rmdir(dir.path);
  confdir_clean(&dir);
  free(tmpdir);
}

/* Verifies default config open creates the app-owned file and returns a usable
 * fd/path pair under one valid default base directory.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_config_open_from_platform_policy(void) {
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmpdir, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

#ifdef __APPLE__
  char *base = make_platform_base_dir(tmpdir);
#endif

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(NULL, &cfg, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(cfg.fd >= 0);
  ASSERT_TRUE(cfg.path != NULL);

#ifdef __APPLE__
  char expected[512];
  snprintf(expected, sizeof(expected),
           "%s/Library/Application Support/adbxplorer/config.json", tmpdir);
#else
  char expected[512];
  snprintf(expected, sizeof(expected), "%s/adbxplorer/config.json", tmpdir);
#endif
  ASSERT_TRUE(strcmp(cfg.path, expected) == 0);

  struct stat st = {0};
  ASSERT_TRUE(fstat(cfg.fd, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE(st.st_size > 0);

  char *cat_err = NULL;
  ConnCatalog *cat = catalog_load_from_fd(cfg.fd, &cat_err);
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(cat_err == NULL);
  catalog_destroy(cat);

  cleanup_config_path_tree(cfg.path);
  conffile_clean(&cfg);
#ifdef __APPLE__
  free(base);
#endif
  free(tmpdir);
}

/* Verifies Linux fallback creates HOME/.config when XDG is unset.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_linux_home_fallback_creates_dot_config(void) {
#ifndef __linux__
  return;
#else
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(NULL, &cfg, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(cfg.path != NULL);

  char expected[512];
  snprintf(expected, sizeof(expected), "%s/.config/adbxplorer/config.json",
           tmpdir);
  ASSERT_TRUE(strcmp(cfg.path, expected) == 0);

  struct stat st = {0};
  ASSERT_TRUE(fstat(cfg.fd, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));

  cleanup_config_path_tree(cfg.path);
  conffile_clean(&cfg);
  free(tmpdir);
#endif
}

/* Verifies Linux fails closed when XDG_CONFIG_HOME selects a missing base
 * directory.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_linux_missing_xdg_base_fails_closed(void) {
#ifndef __linux__
  return;
#else
  char *tmpdir = make_tmpdir();
  char *missing = path_join(tmpdir, "missing-xdg");
  ASSERT_TRUE(missing != NULL);
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", missing, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(NULL, &cfg, &err) == ERR);
  ASSERT_TRUE(cfg.fd < 0);
  ASSERT_TRUE(cfg.path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  conffile_clean(&cfg);
  free(missing);
  free(tmpdir);
#endif
}

/* Verifies default app-dir open reports ENV classification for invalid env.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_app_dir_open_reports_env_error(void) {
  char *tmpdir = make_tmpdir();
#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", "relative/path", 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);
#else
  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(unsetenv("HOME") == 0);
#endif

  ConfDir dir = {.fd = -1, .path = NULL};
  ConfDirErrCode code = CONFDIR_ERR_NONE;
  char *err = NULL;
  ASSERT_TRUE(confdir_default_open(&dir, &code, &err) == ERR);
  ASSERT_TRUE(dir.fd < 0);
  ASSERT_TRUE(dir.path == NULL);
  ASSERT_TRUE(code == CONFDIR_ERR_ENV);
  ASSERT_TRUE(err != NULL);

  free(err);
  confdir_clean(&dir);
  free(tmpdir);
}

/* Verifies default app-dir open reports DIR classification for missing bases.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_app_dir_open_reports_dir_error(void) {
  char *tmpdir = make_tmpdir();
#ifdef __linux__
  char *missing = path_join(tmpdir, "missing-xdg");
  ASSERT_TRUE(missing != NULL);
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", missing, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);
#else
  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);
#endif

  ConfDir dir = {.fd = -1, .path = NULL};
  ConfDirErrCode code = CONFDIR_ERR_NONE;
  char *err = NULL;
  ASSERT_TRUE(confdir_default_open(&dir, &code, &err) == ERR);
  ASSERT_TRUE(dir.fd < 0);
  ASSERT_TRUE(dir.path == NULL);
  ASSERT_TRUE(code == CONFDIR_ERR_DIR);
  ASSERT_TRUE(err != NULL);

  free(err);
  confdir_clean(&dir);
#ifdef __linux__
  free(missing);
#endif
  free(tmpdir);
}

/* Verifies macOS fails closed when the platform base directory is missing.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_macos_missing_platform_base_fails_closed(void) {
#ifndef __APPLE__
  return;
#else
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(unsetenv("XDG_CONFIG_HOME") == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(NULL, &cfg, &err) == ERR);
  ASSERT_TRUE(cfg.fd < 0);
  ASSERT_TRUE(cfg.path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  conffile_clean(&cfg);
  free(tmpdir);
#endif
}

/* Verifies explicit absolute config paths must already exist as regular files.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_explicit_existing_file_succeeds(void) {
  char *tmpdir = make_tmpdir();
  char *dir = path_join(tmpdir, "explicit");
  ASSERT_TRUE(dir != NULL);
  ensure_dir_exists(dir);

  char *cfg_path = path_join(dir, "config.json");
  ASSERT_TRUE(cfg_path != NULL);
  ASSERT_TRUE(fileio_write_exact(cfg_path, (const uint8_t *)"{}", 2, 0600) ==
              OK);

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(cfg_path, &cfg, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(cfg.fd >= 0);
  ASSERT_TRUE(cfg.path != NULL);
  ASSERT_TRUE(strcmp(cfg.path, cfg_path) == 0);

  struct stat st = {0};
  ASSERT_TRUE(fstat(cfg.fd, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));

  conffile_clean(&cfg);
  (void)unlink(cfg_path);
  (void)rmdir(dir);
  free(cfg_path);
  free(dir);
  free(tmpdir);
}

/* Verifies explicit final symlinks are allowed and resolve to the target file.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_explicit_symlink_target_succeeds(void) {
  char *tmpdir = make_tmpdir();
  char *target = path_join(tmpdir, "target.json");
  char *link_path = path_join(tmpdir, "config-link.json");
  ASSERT_TRUE(target != NULL);
  ASSERT_TRUE(link_path != NULL);
  ASSERT_TRUE(fileio_write_exact(target, (const uint8_t *)"{}", 2, 0600) == OK);
  ASSERT_TRUE(symlink(target, link_path) == 0);

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(link_path, &cfg, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(cfg.fd >= 0);
  ASSERT_TRUE(strcmp(cfg.path, link_path) == 0);

  struct stat st = {0};
  ASSERT_TRUE(fstat(cfg.fd, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));

  conffile_clean(&cfg);
  (void)unlink(link_path);
  (void)unlink(target);
  free(link_path);
  free(target);
  free(tmpdir);
}

/* Verifies explicit absolute config paths fail closed when the file is
 * missing.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_explicit_missing_file_fails_closed(void) {
  char *tmpdir = make_tmpdir();
  char *cfg_path = path_join(tmpdir, "missing.json");
  ASSERT_TRUE(cfg_path != NULL);

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(cfg_path, &cfg, &err) == ERR);
  ASSERT_TRUE(cfg.fd < 0);
  ASSERT_TRUE(cfg.path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  conffile_clean(&cfg);
  free(cfg_path);
  free(tmpdir);
}

/* Verifies explicit relative config paths are rejected.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_explicit_relative_path_fails_closed(void) {
  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open("config.json", &cfg, &err) == ERR);
  ASSERT_TRUE(cfg.fd < 0);
  ASSERT_TRUE(cfg.path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  conffile_clean(&cfg);
}

/* Verifies directory paths are rejected when used as config file path.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_reject_directory_as_config_file(void) {
  char *tmpdir = make_tmpdir();

  ConfFile cfg = {.fd = -1, .path = NULL};
  char *err = NULL;
  ASSERT_TRUE(confdir_open(tmpdir, &cfg, &err) == ERR);
  ASSERT_TRUE(cfg.fd < 0);
  ASSERT_TRUE(cfg.path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  conffile_clean(&cfg);
  (void)rmdir(tmpdir);
  free(tmpdir);
}

int main(void) {
  test_default_app_dir_open();
  test_default_config_open_from_platform_policy();
  test_linux_home_fallback_creates_dot_config();
  test_linux_missing_xdg_base_fails_closed();
  test_default_app_dir_open_reports_env_error();
  test_default_app_dir_open_reports_dir_error();
  test_macos_missing_platform_base_fails_closed();
  test_explicit_existing_file_succeeds();
  test_explicit_symlink_target_succeeds();
  test_explicit_missing_file_fails_closed();
  test_explicit_relative_path_fails_closed();
  test_reject_directory_as_config_file();

  fprintf(stderr, "OK: test_config_store\n");
  return 0;
}
