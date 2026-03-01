#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "config_dir.h"
#include "conn_catalog.h"
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

/* Removes the default config file tree created by cfg_resolve_and_ensure.
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

/* Verifies XDG-based default resolution creates a valid config file.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_config_from_xdg(void) {
  char *tmpdir = make_tmpdir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmpdir, 1) == 0);

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(NULL, &path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(path != NULL);

  char expected[512];
  snprintf(expected, sizeof(expected), "%s/adbxplorer/config.json", tmpdir);
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
  free(tmpdir);
}

/* Verifies HOME fallback path on current platform when XDG is unset.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_default_config_from_home_fallback(void) {
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
}

/* Verifies explicit custom path creation for missing parent directories.
 * It borrows no heap input; all allocations are cleaned inside the test.
 */
static void test_custom_config_path_creation(void) {
  char *tmpdir = make_tmpdir();
  char custom[512];
  snprintf(custom, sizeof(custom), "%s/custom/deep/config.json", tmpdir);

  char *path = NULL;
  char *err = NULL;
  ASSERT_TRUE(confdir_resolve(custom, &path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(strcmp(path, custom) == 0);

  struct stat st = {0};
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));

  free(path);
  path = NULL;

  ASSERT_TRUE(confdir_resolve(custom, &path, &err) == OK);
  ASSERT_TRUE(err == NULL);

  cleanup_config_path_tree(path);
  free(path);
  free(tmpdir);
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
  test_default_config_from_xdg();
  test_default_config_from_home_fallback();
  test_custom_config_path_creation();
  test_reject_directory_as_config_file();

  fprintf(stderr, "OK: test_config_store\n");
  return 0;
}
