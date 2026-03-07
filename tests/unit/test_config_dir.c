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

/* Verifies the shared default base-dir resolver follows the platform policy.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_base_dir_resolution(void) {
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmpdir, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

  char *dir = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve_default_base_dir(&dir, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(dir != NULL);

#ifdef __APPLE__
  char expected[512];
  snprintf(expected, sizeof(expected), "%s/Library/Application Support",
           tmpdir);
#else
  char expected[512];
  snprintf(expected, sizeof(expected), "%s", tmpdir);
#endif
  ASSERT_TRUE(strcmp(dir, expected) == 0);

  free(dir);
  free(tmpdir);
}

/* Verifies default config resolution creates the app-owned file under one
 * valid default base directory.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_config_from_platform_policy(void) {
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmpdir, 1) == 0);
  ASSERT_TRUE(setenv("HOME", tmpdir, 1) == 0);

#ifdef __APPLE__
  char *base = make_platform_base_dir(tmpdir);
#endif

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(NULL, &path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(path != NULL);

#ifdef __APPLE__
  char expected[512];
  snprintf(expected, sizeof(expected),
           "%s/Library/Application Support/adbxplorer/config.json", tmpdir);
#else
  char expected[512];
  snprintf(expected, sizeof(expected), "%s/adbxplorer/config.json", tmpdir);
#endif
  ASSERT_TRUE(strcmp(path, expected) == 0);

  struct stat st = {0};
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE(st.st_size > 0);

  char *cat_err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &cat_err);
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(cat_err == NULL);
  catalog_destroy(cat);

  cleanup_config_path_tree(path);
  free(path);
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

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(NULL, &path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(path != NULL);

#ifdef __APPLE__
  char expected[512];
  snprintf(expected, sizeof(expected),
           "%s/Library/Application Support/adbxplorer/config.json", tmpdir);
#else
  char expected[512];
  snprintf(expected, sizeof(expected), "%s/.config/adbxplorer/config.json",
           tmpdir);
#endif
  ASSERT_TRUE(strcmp(path, expected) == 0);

  struct stat st = {0};
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));

  cleanup_config_path_tree(path);
  free(path);
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

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(NULL, &path, &err) == ERR);
  ASSERT_TRUE(path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  free(missing);
  free(tmpdir);
#endif
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

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(NULL, &path, &err) == ERR);
  ASSERT_TRUE(path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
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

  char *cfg = path_join(dir, "config.json");
  ASSERT_TRUE(cfg != NULL);
  ASSERT_TRUE(fileio_write_exact(cfg, (const uint8_t *)"{}", 2, 0600) == OK);

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(cfg, &path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(strcmp(path, cfg) == 0);

  free(path);
  (void)unlink(cfg);
  (void)rmdir(dir);
  free(cfg);
  free(dir);
  free(tmpdir);
}

/* Verifies explicit absolute config paths fail closed when the file is
 * missing.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_explicit_missing_file_fails_closed(void) {
  char *tmpdir = make_tmpdir();
  char *cfg = path_join(tmpdir, "missing.json");
  ASSERT_TRUE(cfg != NULL);

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(cfg, &path, &err) == ERR);
  ASSERT_TRUE(path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  free(cfg);
  free(tmpdir);
}

/* Verifies explicit relative config paths are rejected.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_explicit_relative_path_fails_closed(void) {
  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve("config.json", &path, &err) == ERR);
  ASSERT_TRUE(path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
}

/* Verifies directory paths are rejected when used as config file path.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_reject_directory_as_config_file(void) {
  char *tmpdir = make_tmpdir();

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(tmpdir, &path, &err) == ERR);
  ASSERT_TRUE(path == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  (void)rmdir(tmpdir);
  free(tmpdir);
}

int main(void) {
  test_default_base_dir_resolution();
  test_default_config_from_platform_policy();
  test_linux_home_fallback_creates_dot_config();
  test_linux_missing_xdg_base_fails_closed();
  test_macos_missing_platform_base_fails_closed();
  test_explicit_existing_file_succeeds();
  test_explicit_missing_file_fails_closed();
  test_explicit_relative_path_fails_closed();
  test_reject_directory_as_config_file();

  fprintf(stderr, "OK: test_config_store\n");
  return 0;
}
