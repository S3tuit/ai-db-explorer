#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "file_io.h"
#include "secret_store.h"
#include "string_op.h"
#include "test.h"

static char *cred_path_for_tmp(const char *tmp);

/* Builds a credentials file JSON payload with one entry.
 * It borrows input strings and returns caller-owned JSON text.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *build_single_entry_json(const char *ref, const char *secret) {
  if (!ref || !secret)
    return NULL;
  size_t n = strlen(ref) + strlen(secret) + 128;
  char *json = xmalloc(n);
  snprintf(json, n,
           "{\"version\":\"1\",\"entries\":[{\"ref\":\"%s\",\"secret\":\"%s\"}]}",
           ref, secret);
  return json;
}

/* Builds an isolated temp directory for secret-store tests.
 * It returns one owned path string and creates the directory on disk.
 * Side effects: creates one directory in /tmp.
 * Error semantics: test helper (aborts on failure via ASSERT_TRUE).
 */
static char *make_tmp_dir(void) {
  char templ[] = "/tmp/adbxssXXXXXX";
  char *out = dup_or_null(templ);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(mkdtemp(out) != NULL);
  return out;
}

/* Restores one environment variable to previous value.
 * It borrows all inputs and does not allocate.
 * Side effects: updates process environment.
 * Error semantics: test helper (asserts on set/unset failures).
 */
static void restore_env(const char *name, const char *old_val, int had_old) {
  ASSERT_TRUE(name != NULL);
  if (!had_old) {
    ASSERT_TRUE(unsetenv(name) == 0);
    return;
  }
  ASSERT_TRUE(old_val != NULL);
  ASSERT_TRUE(setenv(name, old_val, 1) == 0);
}

/* Removes one test directory tree created by make_tmp_dir.
 * It borrows 'tmp' and performs best-effort cleanup.
 * Side effects: unlinks files and removes directories.
 * Error semantics: test helper (ignores cleanup errors).
 */
static void cleanup_tmp_tree(const char *tmp) {
  if (!tmp)
    return;
  size_t app_n = strlen(tmp) + strlen("/adbxplorer") + 1;
  char *app = xmalloc(app_n);
  snprintf(app, app_n, "%s/adbxplorer", tmp);
  char *cred = cred_path_for_tmp(tmp);
  if (cred)
    (void)unlink(cred);
  free(cred);
  (void)rmdir(app);
  free(app);
  (void)rmdir(tmp);
}

/* Returns one newly allocated credential file path under temp base.
 * It borrows 'tmp' and returns ownership to caller.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on allocation failure.
 */
static char *cred_path_for_tmp(const char *tmp) {
  if (!tmp)
    return NULL;
  size_t n = strlen(tmp) + strlen("/adbxplorer/credentials.json") + 1;
  char *p = xmalloc(n);
  snprintf(p, n, "%s/adbxplorer/credentials.json", tmp);
  return p;
}

/* Covers file backend CRUD and tri-state get behavior.
 * It validates set/get/update/delete/wipe_all and strict file mode creation.
 */
static void test_file_backend_roundtrip(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss != NULL);

  StrBuf out;
  sb_init(&out);

  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == NO);
  ASSERT_TRUE(secret_store_set(ss, "MyPostgres", "pw-1") == OK);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  ASSERT_TRUE(secret_store_set(ss, "MyPostgres", "pw-2") == OK);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == YES);
  ASSERT_STREQ(out.data, "pw-2");

  ASSERT_TRUE(secret_store_set(ss, "AnotherPostgres", "pw-x") == OK);
  ASSERT_TRUE(secret_store_delete(ss, "MyPostgres") == OK);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == NO);
  ASSERT_TRUE(secret_store_get(ss, "AnotherPostgres", &out) == YES);
  ASSERT_STREQ(out.data, "pw-x");

  ASSERT_TRUE(secret_store_wipe_all(ss) == OK);
  ASSERT_TRUE(secret_store_get(ss, "AnotherPostgres", &out) == NO);

  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  struct stat st = {0};
  ASSERT_TRUE(lstat(cred_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);

  free(cred_path);
  sb_zero_clean(&out);
  secret_store_destroy(ss);

  restore_env("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies strict file-permission policy rejects drifted mode.
 * It tampers credentials file mode and expects later get() to fail with ERR.
 */
static void test_file_backend_rejects_bad_mode(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss != NULL);

  ASSERT_TRUE(secret_store_set(ss, "MyPostgres", "pw-1") == OK);
  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  ASSERT_TRUE(chmod(cred_path, 0644) == 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == ERR);
  sb_zero_clean(&out);

  free(cred_path);
  secret_store_destroy(ss);
  restore_env("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies public factory still yields a usable backend in default build.
 * The test exercises one set/get pair via secret_store_create.
 */
static void test_secret_store_factory_usable(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *ss = secret_store_create();
  ASSERT_TRUE(ss != NULL);
  ASSERT_TRUE(secret_store_set(ss, "MyPostgres", "pw-xyz") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == YES);
  ASSERT_STREQ(out.data, "pw-xyz");

  sb_zero_clean(&out);
  secret_store_destroy(ss);
  restore_env("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies cache refresh when credentials file changes on disk.
 * It writes one new file payload directly and expects get() to reflect it.
 */
static void test_file_backend_refreshes_on_disk_change(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss != NULL);

  ASSERT_TRUE(secret_store_set(ss, "MyPostgres", "pw-1") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  char *json = build_single_entry_json("MyPostgres", "pw-2-long");
  ASSERT_TRUE(json != NULL);
  ASSERT_TRUE(fileio_write_exact(cred_path, (const uint8_t *)json, strlen(json),
                                 0600) == OK);
  free(json);

  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == YES);
  ASSERT_STREQ(out.data, "pw-2-long");

  free(cred_path);
  sb_zero_clean(&out);
  secret_store_destroy(ss);
  restore_env("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies duplicate refs in credentials file are rejected as hard errors.
 */
static void test_file_backend_duplicate_ref_is_err(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *ss = secret_store_file_backend_create();
  ASSERT_TRUE(ss != NULL);

  const char *dup_json =
      "{\"version\":\"1\",\"entries\":[{\"ref\":\"MyPostgres\",\"secret\":"
      "\"pw-a\"},{\"ref\":\"MyPostgres\",\"secret\":\"pw-b\"}]}";

  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  ASSERT_TRUE(fileio_write_exact(cred_path, (const uint8_t *)dup_json,
                                 strlen(dup_json), 0600) == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, "MyPostgres", &out) == ERR);

  sb_zero_clean(&out);
  free(cred_path);
  secret_store_destroy(ss);
  restore_env("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

int main(void) {
  test_file_backend_roundtrip();
  test_file_backend_rejects_bad_mode();
  test_secret_store_factory_usable();
  test_file_backend_refreshes_on_disk_change();
  test_file_backend_duplicate_ref_is_err();
  fprintf(stderr, "OK: test_secret_store\n");
  return 0;
}
