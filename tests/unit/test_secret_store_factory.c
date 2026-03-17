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
#define SS_GET(ss, ref, out) secret_store_get((ss), (ref), (out), NULL)
#define SS_SET(ss, ref, secret) secret_store_set((ss), (ref), (secret), NULL)
#define SS_DELETE(ss, ref) secret_store_delete((ss), (ref), NULL)
#define SS_WIPE_ALL(ss) secret_store_wipe_all((ss), NULL)
#define SS_FILE_PROBE(out_store)                                               \
  secret_store_file_backend_probe((out_store), NULL)
#define SS_KEYCHAIN_PROBE(out_store)                                           \
  secret_store_keychain_backend_probe((out_store), NULL)
#define SS_LIBSECRET_PROBE(out_store)                                          \
  secret_store_libsecret_backend_probe((out_store), NULL)

/* Returns one newly allocated app directory path under temp base. It borrows
 * 'tmp' and returns ownership to caller. Returns NULL on invalid input.
 */
static char *app_path_for_tmp(const char *tmp) {
  if (!tmp)
    return NULL;
  size_t n = strlen(tmp) + strlen("/adbxplorer") + 1;
  char *p = xmalloc(n);
  snprintf(p, n, "%s/adbxplorer", tmp);
  return p;
}

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

/* Creates one isolated app directory for a factory test.
 * It borrows 'tmp' and returns one owned path to the created directory.
 * Side effects: creates the app directory on disk.
 * Return semantics: returns the owned path on success; this is a test helper
 * and aborts on failure.
 */
static char *ensure_app_dir_for_tmp(const char *tmp) {
  char *app = app_path_for_tmp(tmp);
  ASSERT_TRUE(app != NULL);
  ASSERT_TRUE(mkdir(app, 0700) == 0);
  return app;
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

/* Verifies public factory selects one backend, persists the choice, and can
 * reopen the persisted choice on later calls.
 */
static void test_secret_store_factory_usable(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStoreErr err;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss != NULL);
  ASSERT_TRUE(err.code == SSERR_NONE);
  ASSERT_TRUE(err.msg[0] == '\0');
  ASSERT_TRUE(SS_SET(ss, TEST_REF("MyPostgres"), "pw-xyz") == OK);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(SS_GET(ss, TEST_REF("MyPostgres"), &out) == YES);
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
  ASSERT_TRUE(err.code == SSERR_NONE);
  ASSERT_TRUE(err.msg[0] == '\0');
  ASSERT_TRUE(SS_GET(ss2, TEST_REF("MyPostgres"), &out) == YES);

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
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies an empty persisted backend selector fails closed.
 */
static void test_secret_store_factory_rejects_empty_backend_choice(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  char *app = ensure_app_dir_for_tmp(tmp);
  char *backend_cfg = backend_cfg_path_for_tmp(tmp);
  ASSERT_TRUE(backend_cfg != NULL);
  ASSERT_TRUE(fileio_write_exact(backend_cfg, NULL, 0, 0600) == OK);

  SecretStoreErr err;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss == NULL);
  ASSERT_TRUE(err.code == SSERR_PARSE);
  ASSERT_TRUE(strstr(err.msg, "malformed") != NULL);

  free(backend_cfg);
  free(app);
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

  SecretStoreErr err;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss == NULL);
  ASSERT_TRUE(err.code == SSERR_PARSE);
  ASSERT_TRUE(strstr(err.msg, "malformed") != NULL);
  free(backend_cfg);
  free(app);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies a selector file with drifted permissions fails closed instead of
 * being silently repaired.
 */
static void
test_secret_store_factory_rejects_backend_choice_permission_drift(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  char *app = ensure_app_dir_for_tmp(tmp);
  char *backend_cfg = backend_cfg_path_for_tmp(tmp);
  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(backend_cfg != NULL);
  ASSERT_TRUE(cred_path != NULL);
  ASSERT_TRUE(fileio_write_exact(backend_cfg, (const uint8_t *)"file\n", 5,
                                 0644) == OK);

  SecretStoreErr err;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss == NULL);
  ASSERT_TRUE(err.code == SSERR_DIR);
  ASSERT_TRUE(strstr(err.msg, "Fix with: chmod 600") != NULL);
  ASSERT_TRUE(access(cred_path, F_OK) != 0);

  free(cred_path);
  free(backend_cfg);
  free(app);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

/* Verifies a persisted backend unsupported on the current platform fails closed
 * without falling back to the file backend.
 */
static void
test_secret_store_factory_rejects_unsupported_pinned_backend_for_platform(
    void) {
#if !defined(__linux__) && !defined(__APPLE__)
  return;
#else
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  char *app = ensure_app_dir_for_tmp(tmp);
  char *backend_cfg = backend_cfg_path_for_tmp(tmp);
  char *cred_path = cred_path_for_tmp(tmp);
  ASSERT_TRUE(backend_cfg != NULL);
  ASSERT_TRUE(cred_path != NULL);
#if defined(__linux__)
  const char *unsupported_name = "keychain\n";
  const char *unsupported_label = "keychain";
#else
  const char *unsupported_name = "libsecret\n";
  const char *unsupported_label = "libsecret";
#endif
  ASSERT_TRUE(fileio_write_exact(backend_cfg, (const uint8_t *)unsupported_name,
                                 strlen(unsupported_name), 0600) == OK);

  SecretStoreErr err;
  SecretStore *ss = secret_store_create(&err);
  ASSERT_TRUE(ss == NULL);
  ASSERT_TRUE(err.code == SSERR_ENV);
  ASSERT_TRUE(strstr(err.msg, "configured secret-store backend") != NULL);
  ASSERT_TRUE(strstr(err.msg, unsupported_label) != NULL);
  ASSERT_TRUE(access(cred_path, F_OK) != 0);

  free(cred_path);
  free(backend_cfg);
  free(app);
  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
#endif
}

/* Verifies backend probe behavior stays platform-consistent while the file
 * backend is always available; file variant should always be available,
 * libsecret cannot be available on Mac, keychain cannot be available on Linux.
 */
static void test_backend_probe_contract(void) {
  char *tmp = make_tmp_dir();
  char *old_xdg = getenv("XDG_CONFIG_HOME");
  int had_xdg = (old_xdg != NULL);
  old_xdg = old_xdg ? dup_or_null(old_xdg) : NULL;
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", tmp, 1) == 0);

  SecretStore *store = NULL;
  AdbxTriStatus keychain_rc = NO;
  AdbxTriStatus libsecret_rc = NO;

  ASSERT_TRUE(SS_FILE_PROBE(&store) == YES);
  ASSERT_TRUE(store != NULL);
  secret_store_destroy(store);
  store = NULL;

  keychain_rc = SS_KEYCHAIN_PROBE(&store);
  if (keychain_rc == YES) {
    ASSERT_TRUE(store != NULL);
    secret_store_destroy(store);
    store = NULL;
  } else {
    ASSERT_TRUE(store == NULL);
  }

  libsecret_rc = SS_LIBSECRET_PROBE(&store);
  if (libsecret_rc == YES) {
    ASSERT_TRUE(store != NULL);
    secret_store_destroy(store);
    store = NULL;
  } else {
    ASSERT_TRUE(store == NULL);
  }

#if defined(__linux__)
  ASSERT_TRUE(keychain_rc == NO);
#elif defined(__APPLE__)
  ASSERT_TRUE(libsecret_rc == NO);
#endif
  ASSERT_TRUE(!(keychain_rc == YES && libsecret_rc == YES));

  restore_one_env_value("XDG_CONFIG_HOME", old_xdg, had_xdg);
  free(old_xdg);
  cleanup_tmp_tree(tmp);
  free(tmp);
}

int main(void) {
  test_secret_store_factory_usable();
  test_secret_store_factory_rejects_empty_backend_choice();
  test_secret_store_factory_rejects_malformed_backend_choice();
  test_secret_store_factory_rejects_backend_choice_permission_drift();
  test_secret_store_factory_rejects_unsupported_pinned_backend_for_platform();
  test_backend_probe_contract();
  fprintf(stderr, "OK: test_secret_store_factory\n");
  return 0;
}
