#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "file_io.h"
#include "secret_store.h"
#include "string_op.h"
#include "test.h"

#define TEST_NAMESPACE "TestNamespace"
#define TEST_REF(name)                                                         \
  (&(SecretRefInfo){.cred_namespace = TEST_NAMESPACE,                          \
                    .connection_name = (name)})

/* Returns one newly allocated credential file path under temp base. It borrows
 * 'tmp' and returns ownership to caller. Returns NULL on invalid input.
 */
static char *cred_path_for_tmp(const char *tmp) {
  if (!tmp)
    return NULL;
  size_t n = strlen(tmp) + strlen("/adbxplorer/credentials.json") + 1;
  char *p = xmalloc(n);
  snprintf(p, n, "%s/adbxplorer/credentials.json", tmp);
  return p;
}

/* Returns one newly allocated backend-choice file path under temp base. It
 * borrows 'tmp' and returns ownership to caller. Return NULL on invalid input.
 */
static char *backend_cfg_path_for_tmp(const char *tmp) {
  if (!tmp)
    return NULL;
  size_t n = strlen(tmp) + strlen("/adbxplorer/secret_store_backend") + 1;
  char *p = xmalloc(n);
  snprintf(p, n, "%s/adbxplorer/secret_store_backend", tmp);
  return p;
}

/* Builds a credentials file JSON payload with one entry.
 * It borrows input strings and returns caller-owned JSON text.
 * Returns NULL on invalid input.
 */
static char *build_single_entry_json(const char *cred_namespace,
                                     const char *connection_name,
                                     const char *secret) {
  if (!cred_namespace || !connection_name || !secret)
    return NULL;
  size_t n =
      strlen(cred_namespace) + strlen(connection_name) + strlen(secret) + 192;
  char *json = xmalloc(n);
  snprintf(json, n,
           "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":\"%s\","
           "\"connectionName\":\"%s\",\"secret\":\"%s\"}]}",
           cred_namespace, connection_name, secret);
  return json;
}

/* Restores one environment variable to previous value.
 * It borrows all inputs and does not allocate.
 */
static void restore_one_env_value(const char *name, const char *old_val,
                                  int had_old) {
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
 */
static void cleanup_tmp_tree(const char *tmp) {
  if (!tmp)
    return;
  size_t app_n = strlen(tmp) + strlen("/adbxplorer") + 1;
  char *app = xmalloc(app_n);
  snprintf(app, app_n, "%s/adbxplorer", tmp);
  char *cred = cred_path_for_tmp(tmp);
  char *backend_cfg = backend_cfg_path_for_tmp(tmp);
  if (cred)
    (void)unlink(cred);
  if (backend_cfg)
    (void)unlink(backend_cfg);
  free(cred);
  free(backend_cfg);
  (void)rmdir(app);
  free(app);
  (void)rmdir(tmp);
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

  SecretStore *ss;
  secret_store_file_backend_probe(&ss);
  ASSERT_TRUE(ss != NULL);

  StrBuf out;
  sb_init(&out);

  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == NO);
  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-1") == OK);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-2") == OK);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-2");

  ASSERT_TRUE(secret_store_set(ss, TEST_REF("AnotherPostgres"), "pw-x") == OK);
  ASSERT_TRUE(secret_store_delete(ss, TEST_REF("MyPostgres")) == OK);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == NO);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("AnotherPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-x");

  ASSERT_TRUE(secret_store_wipe_all(ss) == OK);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("AnotherPostgres"), &out) == NO);

  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  struct stat st = {0};
  ASSERT_TRUE(lstat(cred_path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);

  free(cred_path);
  sb_zero_clean(&out);
  secret_store_destroy(ss);

  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
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

  SecretStore *ss;
  secret_store_file_backend_probe(&ss);
  ASSERT_TRUE(ss != NULL);

  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-1") == OK);
  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  ASSERT_TRUE(chmod(cred_path, 0644) == 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == ERR);
  sb_zero_clean(&out);

  free(cred_path);
  secret_store_destroy(ss);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies public factory selects one backend, persists the choice, and can
 * reopen the persisted choice on later calls.
 */
static void test_secret_store_factory_usable(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  char *err = NULL;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss != NULL);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-xyz") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-xyz");

  char *backend_cfg = backend_cfg_path_for_tmp(tmp);
  ASSERT_TRUE(backend_cfg != NULL);
  StrBuf cfg_out;
  sb_init(&cfg_out);
  ASSERT_TRUE(fileio_sb_read_limit(backend_cfg, 32, &cfg_out) == OK);
  char *cfg_txt = sb_to_cstr(&cfg_out);
  ASSERT_TRUE(cfg_txt != NULL);
  ASSERT_TRUE(strcmp(cfg_txt, "file\n") == 0 ||
              strcmp(cfg_txt, "keychain\n") == 0 ||
              strcmp(cfg_txt, "libsecret\n") == 0);

  sb_zero_clean(&out);
  secret_store_destroy(ss);

  SecretStore *ss2 = secret_store_create(&err);
  ASSERT_TRUE(ss2 != NULL);
  ASSERT_TRUE(err == NULL);
  ASSERT_TRUE(secret_store_get(ss2, TEST_REF("MyPostgres"), &out) == YES);

  // secret store file should not change once persisted
  StrBuf cfg_out2;
  sb_init(&cfg_out2);
  ASSERT_TRUE(fileio_sb_read_limit(backend_cfg, 32, &cfg_out2) == OK);
  char *cfg_txt2 = sb_to_cstr(&cfg_out2);
  ASSERT_TRUE(strcmp(cfg_txt, cfg_txt2) == 0);

  sb_reset(&cfg_out);
  ASSERT_TRUE(fileio_sb_read_limit(backend_cfg, 32, &cfg_out) == OK);
  ASSERT_STREQ(out.data, "pw-xyz");
  sb_zero_clean(&out);
  secret_store_destroy(ss2);

  sb_zero_clean(&cfg_out);
  sb_zero_clean(&cfg_out2);
  free(backend_cfg);
  free(err);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies malformed persisted backend choice fails closed instead of silently
 * reprobeing a different backend.
 */
static void test_secret_store_factory_rejects_malformed_backend_choice(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  size_t app_n = strlen(tmp) + strlen("/adbxplorer") + 1;
  char *app = xmalloc(app_n);
  snprintf(app, app_n, "%s/adbxplorer", tmp);
  ASSERT_TRUE(mkdir(app, 0700) == 0);

  char *backend_cfg = backend_cfg_path_for_tmp(tmp);
  ASSERT_TRUE(backend_cfg != NULL);
  const char bad_cfg[] = "nonsense\n";
  ASSERT_TRUE(fileio_write_exact(backend_cfg, (const uint8_t *)bad_cfg,
                                 sizeof(bad_cfg) - 1, 0600) == OK);

  char *err = NULL;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "malformed") != NULL);

  free(err);
  free(backend_cfg);
  free(app);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
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

  SecretStore *ss;
  secret_store_file_backend_probe(&ss);
  ASSERT_TRUE(ss != NULL);

  ASSERT_TRUE(secret_store_set(ss, TEST_REF("MyPostgres"), "pw-1") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-1");

  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  char *json =
      build_single_entry_json(TEST_NAMESPACE, "MyPostgres", "pw-2-long");
  ASSERT_TRUE(json != NULL);
  ASSERT_TRUE(fileio_write_exact(cred_path, (const uint8_t *)json, strlen(json),
                                 0600) == OK);
  free(json);

  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == YES);
  ASSERT_STREQ(out.data, "pw-2-long");

  free(cred_path);
  sb_zero_clean(&out);
  secret_store_destroy(ss);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies duplicate namespace+connection refs are rejected as hard errors.
 */
static void test_file_backend_duplicate_ref_is_err(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *ss;
  secret_store_file_backend_probe(&ss);
  ASSERT_TRUE(ss != NULL);

  const char *dup_json =
      "{\"version\":\"1\",\"entries\":[{\"credentialNamespace\":"
      "\"TestNamespace\",\"connectionName\":\"MyPostgres\",\"secret\":"
      "\"pw-a\"},{\"credentialNamespace\":\"TestNamespace\","
      "\"connectionName\":\"MyPostgres\",\"secret\":\"pw-b\"}]}";

  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(cred_path != NULL);
  ASSERT_TRUE(fileio_write_exact(cred_path, (const uint8_t *)dup_json,
                                 strlen(dup_json), 0600) == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ss, TEST_REF("MyPostgres"), &out) == ERR);

  sb_zero_clean(&out);
  free(cred_path);
  secret_store_destroy(ss);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies backend probe tri-state contract.
 * File backend must return YES and allocate a store.
 * Platform-specific backends return NO when unavailable in this build and ERR
 * when compiled in but initialization fails.
 */
static void test_backend_probe_contract(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *store = NULL;

  ASSERT_TRUE(secret_store_file_backend_probe(&store) == YES);
  ASSERT_TRUE(store != NULL);
  secret_store_destroy(store);
  store = NULL;

  ASSERT_TRUE(secret_store_keychain_backend_probe(&store) == NO);
  ASSERT_TRUE(store == NULL);

  AdbxTriStatus ls_rc = secret_store_libsecret_backend_probe(&store);
  // Valid outcomes: YES (service reachable), NO (library missing), ERR (broken)
  if (ls_rc == YES) {
    ASSERT_TRUE(store != NULL);
    secret_store_destroy(store);
    store = NULL;
  } else {
    ASSERT_TRUE(store == NULL);
  }

  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

int main(void) {
  test_file_backend_roundtrip();
  test_file_backend_rejects_bad_mode();
  test_secret_store_factory_usable();
  test_secret_store_factory_rejects_malformed_backend_choice();
  test_file_backend_refreshes_on_disk_change();
  test_file_backend_duplicate_ref_is_err();

  test_backend_probe_contract();
  fprintf(stderr, "OK: test_secret_store\n");
  return 0;
}
