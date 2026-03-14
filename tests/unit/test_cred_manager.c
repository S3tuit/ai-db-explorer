#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "config_dir.h"
#include "cred_manager.h"
#include "file_io.h"
#include "rapidhash.h"
#include "secret_store.h"
#include "string_op.h"
#include "test.h"

typedef struct {
  EnvGuard env;
  char *tmp;
  char *config_path;
  char *state_name;
  char *state_path;
} SyncTestCtx;

typedef struct {
  AdbxStatus rc;
  char *err;
  char *stdout_text;
} CredTestResult;

#define CHILD_OK 0
#define CHILD_ERR 1

/* ---------------------------------------------------------------------------
 * Shared JSON fixtures
 *
 * Tuples:
 *   KeepPg     →  keep-host  / 5432 / keep-user  / keep-db
 *   OtherPg    →  other-host / 5432 / other-user / other-db
 *   RenamedPg  →  other-host / 5432 / other-user / other-db  (same as OtherPg)
 *   AlphaPg    →  keep-host  / 5432 / keep-user  / keep-db   (same as KeepPg)
 *   BetaPg     →  keep-host  / 5432 / keep-user  / keep-db   (same as KeepPg)
 * -------------------------------------------------------------------------*/
static const char *NS_A = "TestNsA";

/* Single connection: KeepPg. */
static const char *JSON_ONE_DB = "{"
                                 "\"version\":\"1.0\","
                                 "\"credentialNamespace\":\"TestNsA\","
                                 "\"safetyPolicy\":{},"
                                 "\"databases\":["
                                 "{"
                                 "\"type\":\"postgres\","
                                 "\"connectionName\":\"KeepPg\","
                                 "\"host\":\"keep-host\","
                                 "\"port\":5432,"
                                 "\"username\":\"keep-user\","
                                 "\"database\":\"keep-db\""
                                 "}"
                                 "]"
                                 "}";

/* Two connections: KeepPg + OtherPg. */
static const char *JSON_TWO_DB = "{"
                                 "\"version\":\"1.0\","
                                 "\"credentialNamespace\":\"TestNsA\","
                                 "\"safetyPolicy\":{},"
                                 "\"databases\":["
                                 "{"
                                 "\"type\":\"postgres\","
                                 "\"connectionName\":\"KeepPg\","
                                 "\"host\":\"keep-host\","
                                 "\"port\":5432,"
                                 "\"username\":\"keep-user\","
                                 "\"database\":\"keep-db\""
                                 "},"
                                 "{"
                                 "\"type\":\"postgres\","
                                 "\"connectionName\":\"OtherPg\","
                                 "\"host\":\"other-host\","
                                 "\"port\":5432,"
                                 "\"username\":\"other-user\","
                                 "\"database\":\"other-db\""
                                 "}"
                                 "]"
                                 "}";

/* Two connections: KeepPg + OtherPg with altered host (for "changed field"
 * tests where OtherPg has a different host between config and state). */
static const char *JSON_TWO_DB_ALT = "{"
                                     "\"version\":\"1.0\","
                                     "\"credentialNamespace\":\"TestNsA\","
                                     "\"safetyPolicy\":{},"
                                     "\"databases\":["
                                     "{"
                                     "\"type\":\"postgres\","
                                     "\"connectionName\":\"KeepPg\","
                                     "\"host\":\"keep-host\","
                                     "\"port\":5432,"
                                     "\"username\":\"keep-user\","
                                     "\"database\":\"keep-db\""
                                     "},"
                                     "{"
                                     "\"type\":\"postgres\","
                                     "\"connectionName\":\"OtherPg\","
                                     "\"host\":\"alt-host\","
                                     "\"port\":5432,"
                                     "\"username\":\"other-user\","
                                     "\"database\":\"other-db\""
                                     "}"
                                     "]"
                                     "}";

/* Two connections: KeepPg + RenamedPg (same tuple as OtherPg). */
static const char *JSON_RENAME_CONF = "{"
                                      "\"version\":\"1.0\","
                                      "\"credentialNamespace\":\"TestNsA\","
                                      "\"safetyPolicy\":{},"
                                      "\"databases\":["
                                      "{"
                                      "\"type\":\"postgres\","
                                      "\"connectionName\":\"KeepPg\","
                                      "\"host\":\"keep-host\","
                                      "\"port\":5432,"
                                      "\"username\":\"keep-user\","
                                      "\"database\":\"keep-db\""
                                      "},"
                                      "{"
                                      "\"type\":\"postgres\","
                                      "\"connectionName\":\"RenamedPg\","
                                      "\"host\":\"other-host\","
                                      "\"port\":5432,"
                                      "\"username\":\"other-user\","
                                      "\"database\":\"other-db\""
                                      "}"
                                      "]"
                                      "}";

/* Three connections: KeepPg + WrongPg + MissingPg. */
static const char *JSON_THREE_DB = "{"
                                   "\"version\":\"1.0\","
                                   "\"credentialNamespace\":\"TestNsA\","
                                   "\"safetyPolicy\":{},"
                                   "\"databases\":["
                                   "{"
                                   "\"type\":\"postgres\","
                                   "\"connectionName\":\"KeepPg\","
                                   "\"host\":\"keep-host\","
                                   "\"port\":5432,"
                                   "\"username\":\"keep-user\","
                                   "\"database\":\"keep-db\""
                                   "},"
                                   "{"
                                   "\"type\":\"postgres\","
                                   "\"connectionName\":\"WrongPg\","
                                   "\"host\":\"wrong-host\","
                                   "\"port\":5432,"
                                   "\"username\":\"wrong-user\","
                                   "\"database\":\"wrong-db\""
                                   "},"
                                   "{"
                                   "\"type\":\"postgres\","
                                   "\"connectionName\":\"MissingPg\","
                                   "\"host\":\"missing-host\","
                                   "\"port\":5432,"
                                   "\"username\":\"missing-user\","
                                   "\"database\":\"missing-db\""
                                   "}"
                                   "]"
                                   "}";

/* Two connections with identical tuples (same as KeepPg): for ambiguous rename
 * detection tests. */
static const char *JSON_AMBIG_CONF = "{"
                                     "\"version\":\"1.0\","
                                     "\"credentialNamespace\":\"TestNsA\","
                                     "\"safetyPolicy\":{},"
                                     "\"databases\":["
                                     "{"
                                     "\"type\":\"postgres\","
                                     "\"connectionName\":\"AlphaPg\","
                                     "\"host\":\"keep-host\","
                                     "\"port\":5432,"
                                     "\"username\":\"keep-user\","
                                     "\"database\":\"keep-db\""
                                     "},"
                                     "{"
                                     "\"type\":\"postgres\","
                                     "\"connectionName\":\"BetaPg\","
                                     "\"host\":\"keep-host\","
                                     "\"port\":5432,"
                                     "\"username\":\"keep-user\","
                                     "\"database\":\"keep-db\""
                                     "}"
                                     "]"
                                     "}";

/* Single connection with a different namespace (for mismatch tests). */
static const char *JSON_ONE_DB_NS_B = "{"
                                      "\"version\":\"1.0\","
                                      "\"credentialNamespace\":\"TestNsB\","
                                      "\"safetyPolicy\":{},"
                                      "\"databases\":["
                                      "{"
                                      "\"type\":\"postgres\","
                                      "\"connectionName\":\"KeepPg\","
                                      "\"host\":\"keep-host\","
                                      "\"port\":5432,"
                                      "\"username\":\"keep-user\","
                                      "\"database\":\"keep-db\""
                                      "}"
                                      "]"
                                      "}";

/* Single connection in a different namespace for reset tests. */
static const char *JSON_ONE_DB_OTHER_NS = "{"
                                          "\"version\":\"1.0\","
                                          "\"credentialNamespace\":\"OtherNs\","
                                          "\"safetyPolicy\":{},"
                                          "\"databases\":["
                                          "{"
                                          "\"type\":\"postgres\","
                                          "\"connectionName\":\"OtherNsPg\","
                                          "\"host\":\"otherns-host\","
                                          "\"port\":5432,"
                                          "\"username\":\"otherns-user\","
                                          "\"database\":\"otherns-db\""
                                          "}"
                                          "]"
                                          "}";

/* ---------------------------------------------------------------------------
 * Test helpers
 * -------------------------------------------------------------------------*/

static void unlink_if_exists(const char *path) {
  if (!path)
    return;
  if (unlink(path) == 0 || errno == ENOENT)
    return;
}

static void rmdir_if_exists(const char *path) {
  if (!path)
    return;
  if (rmdir(path) == 0 || errno == ENOENT || errno == ENOTEMPTY)
    return;
}

static char *state_file_name_for_namespace(const char *cred_namespace) {
  ASSERT_TRUE(cred_namespace != NULL);
  uint64_t ns_hash = rapidhash(cred_namespace, strlen(cred_namespace));
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "state.%016" PRIx64 ".json", ns_hash);
  ASSERT_TRUE(n > 0 && (size_t)n < sizeof(buf));
  return dup_or_null(buf);
}

static void write_json_file(const char *path, const char *json) {
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(json != NULL);
  ASSERT_TRUE(fileio_write_exact(path, (const uint8_t *)json, strlen(json),
                                 0600) == OK);
}

static void sync_test_ctx_init(SyncTestCtx *ctx, const char *cred_namespace,
                               const char *config_json,
                               const char *state_json_or_null) {
  ASSERT_TRUE(ctx != NULL);
  ASSERT_TRUE(cred_namespace != NULL);
  ASSERT_TRUE(config_json != NULL);
  memset(ctx, 0, sizeof(*ctx));

  env_guard_begin(&ctx->env);
  ctx->tmp = make_tmp_dir();
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", ctx->tmp, 1) == 0);
  ASSERT_TRUE(setenv("HOME", ctx->tmp, 1) == 0);

  ctx->config_path = path_join(ctx->tmp, "config.json");
  ASSERT_TRUE(ctx->config_path != NULL);
  write_json_file(ctx->config_path, config_json);

  ConfDir app = {.fd = -1, .path = NULL};
  ConfDirErrCode app_code = CONFDIR_ERR_NONE;
  char *app_err = NULL;
  ASSERT_TRUE(confdir_default_open(&app, &app_code, &app_err) == OK);
  free(app_err);

  ctx->state_name = state_file_name_for_namespace(cred_namespace);
  ctx->state_path = path_join(app.path, ctx->state_name);
  ASSERT_TRUE(ctx->state_path != NULL);
  if (state_json_or_null)
    write_json_file(ctx->state_path, state_json_or_null);
  confdir_clean(&app);
}

static void sync_test_ctx_clean(SyncTestCtx *ctx) {
  if (!ctx)
    return;

  unlink_if_exists(ctx->config_path);
  unlink_if_exists(ctx->state_path);

  char *cred_path = NULL;
  char *app_dir = NULL;
  if (ctx->tmp) {
    app_dir = path_join(ctx->tmp, "adbxplorer");
    cred_path = path_join(app_dir, "credentials.json");
  }
  unlink_if_exists(cred_path);
  rmdir_if_exists(app_dir);
  rmdir_if_exists(ctx->tmp);

  free(cred_path);
  free(app_dir);
  free(ctx->config_path);
  free(ctx->state_name);
  free(ctx->state_path);
  free(ctx->tmp);
  env_guard_end(&ctx->env);
  memset(ctx, 0, sizeof(*ctx));
}

static void seed_secret(const char *cred_namespace, const char *connection_name,
                        const char *secret) {
  ASSERT_TRUE(cred_namespace != NULL);
  ASSERT_TRUE(connection_name != NULL);
  ASSERT_TRUE(secret != NULL);

  SecretStore *store = secret_store_create(NULL);
  ASSERT_TRUE(store != NULL);
  ASSERT_TRUE(
      secret_store_set(store,
                       &(SecretRefInfo){.cred_namespace = cred_namespace,
                                        .connection_name = connection_name},
                       secret) == OK);
  secret_store_destroy(store);
}

static void assert_secret_value(const char *cred_namespace,
                                const char *connection_name,
                                const char *expected_secret) {
  ASSERT_TRUE(cred_namespace != NULL);
  ASSERT_TRUE(connection_name != NULL);
  ASSERT_TRUE(expected_secret != NULL);

  SecretStore *store = secret_store_create(NULL);
  ASSERT_TRUE(store != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(
      secret_store_get(store,
                       &(SecretRefInfo){.cred_namespace = cred_namespace,
                                        .connection_name = connection_name},
                       &out) == YES);
  ASSERT_STREQ(out.data, expected_secret);
  sb_zero_clean(&out);
  secret_store_destroy(store);
}

static void assert_secret_missing(const char *cred_namespace,
                                  const char *connection_name) {
  ASSERT_TRUE(cred_namespace != NULL);
  ASSERT_TRUE(connection_name != NULL);

  SecretStore *store = secret_store_create(NULL);
  ASSERT_TRUE(store != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(
      secret_store_get(store,
                       &(SecretRefInfo){.cred_namespace = cred_namespace,
                                        .connection_name = connection_name},
                       &out) == NO);
  sb_zero_clean(&out);
  secret_store_destroy(store);
}

static void assert_state_names(const char *state_path,
                               const char *cred_namespace,
                               const char *const *names, size_t n_names) {
  ASSERT_TRUE(state_path != NULL);
  ASSERT_TRUE(cred_namespace != NULL);

  char *state_err = NULL;
  ConnCatalog *saved = catalog_load_from_file(state_path, &state_err);
  ASSERT_TRUE(saved != NULL);
  ASSERT_TRUE(state_err == NULL);
  ASSERT_STREQ(saved->credential_namespace, cred_namespace);
  ASSERT_TRUE(saved->n_profiles == n_names);
  for (size_t i = 0; i < n_names; i++)
    ASSERT_STREQ(saved->profiles[i].connection_name, names[i]);
  catalog_destroy(saved);
  free(state_err);
}

static void assert_state_json_eq(const char *state_path,
                                 const char *expected_json) {
  ASSERT_TRUE(state_path != NULL);
  ASSERT_TRUE(expected_json != NULL);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(fileio_sb_read_limit(state_path, 16 * 1024, &out) == OK);
  ASSERT_STREQ(sb_to_cstr(&out), expected_json);
  sb_clean(&out);
}

static void run_sync_ok(const char *config_path) {
  CredManagerReq req = {.cmd = CRED_MAN_SYNC, .connection_name = NULL};
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  free(err);
}

static char *run_sync_err(const char *config_path) {
  CredManagerReq req = {.cmd = CRED_MAN_SYNC, .connection_name = NULL};
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_path, &err) == ERR);
  ASSERT_TRUE(err != NULL);
  return err;
}

static void run_sync_one_ok(const char *config_path,
                            const char *connection_name) {
  CredManagerReq req = {
      .cmd = CRED_MAN_SYNC,
      .connection_name = connection_name,
  };
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_path, &err) == OK);
  ASSERT_TRUE(err == NULL);
  free(err);
}

static char *run_sync_one_err(const char *config_path,
                              const char *connection_name) {
  CredManagerReq req = {
      .cmd = CRED_MAN_SYNC,
      .connection_name = connection_name,
  };
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_path, &err) == ERR);
  ASSERT_TRUE(err != NULL);
  return err;
}

static void run_reset_namespace_ok(const char *cred_namespace,
                                   const char *config_input) {
  CredManagerReq req = {
      .cmd = CRED_MAN_RESET,
      .cred_namespace = cred_namespace,
      .reset_scope = CRED_MAN_RESET_SCOPE_NAMESPACE,
  };
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_input, &err) == OK);
  ASSERT_TRUE(err == NULL);
  free(err);
}

static char *run_reset_namespace_err(const char *cred_namespace,
                                     const char *config_input) {
  CredManagerReq req = {
      .cmd = CRED_MAN_RESET,
      .cred_namespace = cred_namespace,
      .reset_scope = CRED_MAN_RESET_SCOPE_NAMESPACE,
  };
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_input, &err) == ERR);
  ASSERT_TRUE(err != NULL);
  return err;
}

static char *run_reset_all_err(const char *config_input) {
  CredManagerReq req = {
      .cmd = CRED_MAN_RESET,
      .cred_namespace = NULL,
      .reset_scope = CRED_MAN_RESET_SCOPE_ALL,
  };
  char *err = NULL;
  ASSERT_TRUE(cred_manager_execute(&req, config_input, &err) == ERR);
  ASSERT_TRUE(err != NULL);
  return err;
}

/* Releases one captured credential-test result.
 * It consumes owned strings inside 'res'.
 * Side effects: frees heap memory.
 * Error semantics: test helper.
 */
static void cred_test_result_clean(CredTestResult *res) {
  if (!res)
    return;
  free(res->err);
  free(res->stdout_text);
  memset(res, 0, sizeof(*res));
}

/* Runs one credential-manager test command while capturing stdout.
 *
 * We intentionally redirect STDOUT_FILENO to a temporary file so the
 * production code sees a non-tty stdout and emits stable plain-text output
 * without ANSI colors. When 'factory' is non-NULL we install it through the
 * db_backend_set_test_factory() seam and always clear it before returning.
 *
 * Ownership: writes owned strings into caller-owned 'out'.
 * Side effects: temporarily redirects stdout and may install a test-only
 * backend factory.
 * Error semantics: test helper (asserts on setup or teardown failure).
 */
static void run_test_capture(CredTestResult *out, const char *config_path,
                             const char *connection_name,
                             DbBackendFactory factory) {
  ASSERT_TRUE(out != NULL);
  memset(out, 0, sizeof(*out));

  FILE *capture = tmpfile();
  ASSERT_TRUE(capture != NULL);

  fflush(stdout);
  int saved_stdout = dup(STDOUT_FILENO);
  ASSERT_TRUE(saved_stdout >= 0);
  ASSERT_TRUE(dup2(fileno(capture), STDOUT_FILENO) >= 0);

  db_backend_set_test_factory(factory);
  CredManagerReq req = {
      .cmd = CRED_MAN_TEST,
      .connection_name = connection_name,
  };
  out->rc = cred_manager_execute(&req, config_path, &out->err);
  db_backend_set_test_factory(NULL);

  fflush(stdout);
  ASSERT_TRUE(dup2(saved_stdout, STDOUT_FILENO) >= 0);
  ASSERT_TRUE(close(saved_stdout) == 0);

  out->stdout_text = read_all(capture);
  ASSERT_TRUE(out->stdout_text != NULL);
  ASSERT_TRUE(fclose(capture) == 0);
}

/* Runs one sync-all command inside a forked child and exits with CHILD_OK only
 * when the observed return code and error string match the expected outcome.
 * It borrows all inputs and does not return to the caller.
 * Side effects: updates one test-only env var in the child process and invokes
 * credential-manager sync logic before exiting the process.
 * Error semantics: test helper; exits CHILD_OK on the expected outcome,
 * CHILD_ERR otherwise.
 */
static void run_sync_child_and_exit(const char *config_path,
                                    const char *hold_lock_ms,
                                    AdbxStatus expect_rc,
                                    const char *expect_err_substr) {
  if (hold_lock_ms) {
    if (setenv("ADBX_CREDM_HOLD_LOCK_MS", hold_lock_ms, 1) != 0)
      _exit(CHILD_ERR);
  } else {
    if (unsetenv("ADBX_CREDM_HOLD_LOCK_MS") != 0)
      _exit(CHILD_ERR);
  }

  CredManagerReq req = {.cmd = CRED_MAN_SYNC, .connection_name = NULL};
  char *err = NULL;
  AdbxStatus rc = cred_manager_execute(&req, config_path, &err);

  int ok = (rc == expect_rc);
  if (expect_rc == OK) {
    ok = ok && (err == NULL);
  } else {
    ok = ok && err != NULL;
    if (ok && expect_err_substr)
      ok = (strstr(err, expect_err_substr) != NULL);
  }

  free(err);
  _exit(ok ? CHILD_OK : CHILD_ERR);
}

/* ---------------------------------------------------------------------------
 * sync-all tests
 * -------------------------------------------------------------------------*/

/* Verifies sync fails closed when state is missing, even if matching secrets
 * already exist in the store.
 */
static void test_sync_missing_state_all_secrets_present(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "other-secret");

  char *err = run_sync_err(ctx.config_path);
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "other-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  sync_test_ctx_clean(&ctx);
}

/* Verifies sync succeeds without mutation when state and config already match.
 */
static void test_sync_unchanged_state_and_config(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "other-secret");

  run_sync_ok(ctx.config_path);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "other-secret");
  const char *names[] = {"KeepPg", "OtherPg"};
  assert_state_names(ctx.state_path, NS_A, names, ARRLEN(names));

  sync_test_ctx_clean(&ctx);
}

/* Verifies a one-to-one tuple rename reuses the stored secret reference and
 * does not disturb an unchanged sibling connection.
 */
static void test_sync_rename_reuse(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_RENAME_CONF, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "rename-secret");

  run_sync_ok(ctx.config_path);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "RenamedPg", "rename-secret");
  assert_secret_missing(NS_A, "OtherPg");
  const char *names[] = {"KeepPg", "RenamedPg"};
  assert_state_names(ctx.state_path, NS_A, names, ARRLEN(names));

  sync_test_ctx_clean(&ctx);
}

/* Verifies sync deletes secrets that only exist in the previous saved state.
 */
static void test_sync_removed_connection_deletion(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "gone-secret");

  run_sync_ok(ctx.config_path);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_missing(NS_A, "OtherPg");
  const char *names[] = {"KeepPg"};
  assert_state_names(ctx.state_path, NS_A, names, ARRLEN(names));

  sync_test_ctx_clean(&ctx);
}

/* Verifies sync fails closed when the hashed state file contains a different
 * credential namespace than the current config.
 */
static void test_sync_namespace_mismatch_fails_closed(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB_NS_B);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_err(ctx.config_path);
  ASSERT_TRUE(strstr(err, "namespace mismatch") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);

  sync_test_ctx_clean(&ctx);
}

/* Verifies sync does not auto-rename when one removed entry ambiguously matches
 * multiple current config entries by tuple and therefore would require a
 * prompt.
 */
static void test_sync_ambiguous_tuple_match_requires_prompt(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_AMBIG_CONF, JSON_ONE_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_err(ctx.config_path);
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_missing(NS_A, "AlphaPg");
  assert_secret_missing(NS_A, "BetaPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);

  sync_test_ctx_clean(&ctx);
}

/* Verifies overlapping sync-all processes are rejected instead of both
 * succeeding. The holder child uses the test-only hold hook to stay inside
 * cred_manager long enough for the contender child to overlap.
 */
static void test_sync_concurrent_process_rejected(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "other-secret");

  pid_t holder = fork();
  ASSERT_TRUE(holder >= 0);
  if (holder == 0)
    run_sync_child_and_exit(ctx.config_path, "300", OK, NULL);

  struct timespec ts = {.tv_sec = 0, .tv_nsec = 100 * 1000 * 1000L};
  while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
  }

  pid_t contender = fork();
  ASSERT_TRUE(contender >= 0);
  if (contender == 0)
    run_sync_child_and_exit(ctx.config_path, NULL, ERR,
                            "another instance of adbxplorer in 'cred' mode");

  int holder_status = 0;
  int contender_status = 0;
  ASSERT_TRUE(waitpid(holder, &holder_status, 0) == holder);
  ASSERT_TRUE(waitpid(contender, &contender_status, 0) == contender);
  ASSERT_TRUE(WIFEXITED(holder_status));
  ASSERT_TRUE(WIFEXITED(contender_status));
  ASSERT_TRUE(WEXITSTATUS(holder_status) == CHILD_OK);
  ASSERT_TRUE(WEXITSTATUS(contender_status) == CHILD_OK);

  sync_test_ctx_clean(&ctx);
}

/* ---------------------------------------------------------------------------
 * sync-one tests
 * -------------------------------------------------------------------------*/

/* Verifies targeted sync leaves unrelated changed entries untouched when the
 * selected connection is already synchronized.
 */
static void test_sync_one_target_unchanged_unrelated_changed(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB_ALT, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "other-secret");

  run_sync_one_ok(ctx.config_path, "KeepPg");

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "other-secret");
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync fails clearly when the requested connection name does
 * not exist in the current config.
 */
static void test_sync_one_target_missing_from_config(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_one_err(ctx.config_path, "MissingPg");
  ASSERT_TRUE(strstr(err, "was not found in the current config") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync is a no-op when the selected connection already has
 * matching state and a stored secret.
 */
static void test_sync_one_target_unchanged_secret_present(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  run_sync_one_ok(ctx.config_path, "KeepPg");

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync fails closed when the selected connection is present
 * in state/config but its stored secret is missing.
 */
static void
test_sync_one_target_unchanged_secret_missing_requires_prompt(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  char *err = run_sync_one_err(ctx.config_path, "KeepPg");
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_missing(NS_A, "KeepPg");
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync prompts when the selected connection keeps the same
 * name but changes one of the sync tuple fields.
 */
static void test_sync_one_target_changed_same_name_requires_prompt(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB_ALT, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "old-secret");

  char *err = run_sync_one_err(ctx.config_path, "OtherPg");
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "old-secret");
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync reuses one unique tuple match and patches only the
 * renamed entry inside saved state.
 */
static void test_sync_one_target_unique_rename_reuses_secret(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_RENAME_CONF, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "rename-secret");

  run_sync_one_ok(ctx.config_path, "RenamedPg");

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "RenamedPg", "rename-secret");
  assert_secret_missing(NS_A, "OtherPg");
  const char *names[] = {"KeepPg", "RenamedPg"};
  assert_state_names(ctx.state_path, NS_A, names, ARRLEN(names));

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync falls back to prompting when a unique tuple rename is
 * detected but the old stored secret is missing.
 */
static void
test_sync_one_target_unique_rename_missing_secret_requires_prompt(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_RENAME_CONF, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_one_err(ctx.config_path, "RenamedPg");
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_missing(NS_A, "OtherPg");
  assert_secret_missing(NS_A, "RenamedPg");
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync does not auto-rename when tuple matching is
 * ambiguous for the selected connection.
 */
static void test_sync_one_target_ambiguous_tuple_requires_prompt(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_AMBIG_CONF, JSON_ONE_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_one_err(ctx.config_path, "AlphaPg");
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_missing(NS_A, "AlphaPg");
  assert_secret_missing(NS_A, "BetaPg");
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync fails closed when saved state is missing, even if the
 * target secret already exists in the store.
 */
static void test_sync_one_target_missing_state_requires_prompt(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_one_err(ctx.config_path, "KeepPg");
  ASSERT_TRUE(strstr(err, "interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync fails closed when the saved state file under the
 * namespace hash does not belong to the current config namespace.
 */
static void test_sync_one_target_namespace_mismatch_fails_closed(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB_NS_B);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_sync_one_err(ctx.config_path, "KeepPg");
  ASSERT_TRUE(strstr(err, "namespace mismatch") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB_NS_B);

  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync does not prune unrelated stale entries while syncing
 * one unchanged connection.
 */
static void
test_sync_one_target_leaves_unrelated_stale_entries_untouched(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "other-secret");

  run_sync_one_ok(ctx.config_path, "KeepPg");

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "other-secret");
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB);

  sync_test_ctx_clean(&ctx);
}

/* ---------------------------------------------------------------------------
 * test-action tests
 * -------------------------------------------------------------------------*/

/* Verifies '--test <connection>' fails clearly when the requested connection
 * is not present in the config file.
 */
static void test_test_one_target_missing_from_config(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, "MissingPg", NULL);

  ASSERT_TRUE(res.rc == ERR);
  ASSERT_TRUE(res.err != NULL);
  ASSERT_TRUE(strstr(res.err, "was not found in the config") != NULL);
  ASSERT_STREQ(res.stdout_text, "");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* Verifies '--test <connection>' reports a missing stored credential through
 * stdout without creating or modifying credential state files.
 */
static void test_test_one_missing_secret_reports_fail(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, "KeepPg", NULL);

  ASSERT_TRUE(res.rc == ERR);
  ASSERT_TRUE(res.err == NULL);
  ASSERT_TRUE(strstr(res.stdout_text,
                     "FAIL KeepPg: missing stored credential\n") != NULL);
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* Verifies '--test' keeps going across all profiles and reports both success
 * and missing-credential results.
 */
static void test_test_all_with_one_missing_secret(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  seed_secret(NS_A, "KeepPg", "KeepPg");

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, NULL, fake_backend_create);

  ASSERT_TRUE(res.rc == ERR);
  ASSERT_TRUE(res.err == NULL);
  ASSERT_TRUE(strstr(res.stdout_text, " OK  KeepPg\n") != NULL);
  ASSERT_TRUE(strstr(res.stdout_text,
                     "FAIL OtherPg: missing stored credential\n") != NULL);
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* Verifies '--test <connection>' succeeds when the fake backend receives the
 * expected password for the selected connection.
 */
static void test_test_one_success(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  seed_secret(NS_A, "KeepPg", "KeepPg");
  assert_secret_missing(NS_A, "OtherPg");

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, "KeepPg", fake_backend_create);

  ASSERT_TRUE(res.rc == OK);
  ASSERT_TRUE(res.err == NULL);
  ASSERT_TRUE(strstr(res.stdout_text, " OK  KeepPg\n") != NULL);
  ASSERT_TRUE(strstr(res.stdout_text, "OtherPg") == NULL);
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* Verifies '--test <connection>' reports backend auth failure when the stored
 * password does not match the fake backend auth rule.
 */
static void test_test_one_backend_failure(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  seed_secret(NS_A, "KeepPg", "wrong-secret");

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, "KeepPg", fake_backend_create);

  ASSERT_TRUE(res.rc == ERR);
  ASSERT_TRUE(res.err == NULL);
  ASSERT_TRUE(strstr(res.stdout_text, "FAIL KeepPg: fake auth failed\n") !=
              NULL);
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* Verifies '--test' keeps testing every configured profile and reports mixed
 * success, backend failure, and missing-secret results in config order.
 */
static void test_test_all_mixed_results(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_THREE_DB, NULL);

  seed_secret(NS_A, "KeepPg", "KeepPg");
  seed_secret(NS_A, "WrongPg", "wrong-secret");

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, NULL, fake_backend_create);

  ASSERT_TRUE(res.rc == ERR);
  ASSERT_TRUE(res.err == NULL);

  char *keep = strstr(res.stdout_text, " OK  KeepPg\n");
  char *wrong = strstr(res.stdout_text, "FAIL WrongPg: fake auth failed\n");
  char *missing =
      strstr(res.stdout_text, "FAIL MissingPg: missing stored credential\n");
  ASSERT_TRUE(keep != NULL);
  ASSERT_TRUE(wrong != NULL);
  ASSERT_TRUE(missing != NULL);
  ASSERT_TRUE(keep < wrong);
  ASSERT_TRUE(wrong < missing);
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* Verifies '--test' returns OK when every configured profile succeeds against
 * the fake backend.
 */
static void test_test_all_success(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  seed_secret(NS_A, "KeepPg", "KeepPg");
  seed_secret(NS_A, "OtherPg", "OtherPg");

  CredTestResult res;
  run_test_capture(&res, ctx.config_path, NULL, fake_backend_create);

  ASSERT_TRUE(res.rc == OK);
  ASSERT_TRUE(res.err == NULL);
  ASSERT_TRUE(strstr(res.stdout_text, " OK  KeepPg\n") != NULL);
  ASSERT_TRUE(strstr(res.stdout_text, " OK  OtherPg\n") != NULL);
  ASSERT_TRUE(strstr(res.stdout_text, "FAIL") == NULL);
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  cred_test_result_clean(&res);
  sync_test_ctx_clean(&ctx);
}

/* ---------------------------------------------------------------------------
 * reset-namespace tests
 * -------------------------------------------------------------------------*/

/* Verifies namespace reset deletes only the selected namespace and preserves
 * unrelated secrets and state.
 */
static void test_reset_namespace_deletes_only_selected_namespace(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  ConfDir app = {.fd = -1, .path = NULL};
  char *app_err = NULL;
  ASSERT_TRUE(confdir_default_open(&app, NULL, &app_err) == OK);
  free(app_err);
  char *other_state_name = state_file_name_for_namespace("OtherNs");
  char *other_state_path = path_join(app.path, other_state_name);
  ASSERT_TRUE(other_state_path != NULL);
  write_json_file(other_state_path, JSON_ONE_DB_OTHER_NS);
  confdir_clean(&app);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "other-secret");
  seed_secret("OtherNs", "OtherNsPg", "other-ns-secret");

  run_reset_namespace_ok(NS_A, NULL);

  assert_secret_missing(NS_A, "KeepPg");
  assert_secret_missing(NS_A, "OtherPg");
  assert_secret_value("OtherNs", "OtherNsPg", "other-ns-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);
  ASSERT_TRUE(access(other_state_path, F_OK) == 0);

  unlink_if_exists(other_state_path);
  free(other_state_name);
  free(other_state_path);
  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset succeeds when the state file is already missing.
 */
static void test_reset_namespace_missing_state_succeeds(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  run_reset_namespace_ok(NS_A, NULL);

  assert_secret_missing(NS_A, "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset succeeds when there are no secrets in that
 * namespace and only the matching state file is removed.
 */
static void test_reset_namespace_missing_secrets_succeeds(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  ConfDir app = {.fd = -1, .path = NULL};
  char *app_err = NULL;
  ASSERT_TRUE(confdir_default_open(&app, NULL, &app_err) == OK);
  free(app_err);
  char *other_state_name = state_file_name_for_namespace("OtherNs");
  char *other_state_path = path_join(app.path, other_state_name);
  ASSERT_TRUE(other_state_path != NULL);
  write_json_file(other_state_path, JSON_ONE_DB_OTHER_NS);
  confdir_clean(&app);

  seed_secret("OtherNs", "OtherNsPg", "other-ns-secret");

  run_reset_namespace_ok(NS_A, NULL);

  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);
  ASSERT_TRUE(access(other_state_path, F_OK) == 0);
  assert_secret_value("OtherNs", "OtherNsPg", "other-ns-secret");

  unlink_if_exists(other_state_path);
  free(other_state_name);
  free(other_state_path);
  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset is a successful no-op when both state and secrets
 * are already absent.
 */
static void test_reset_namespace_missing_state_and_secrets_is_noop(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_reset_namespace_ok(NS_A, NULL);

  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);
  assert_secret_missing(NS_A, "KeepPg");

  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset rejects a NULL namespace with a clear error.
 */
static void test_reset_namespace_null_namespace_fails(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  char *err = run_reset_namespace_err(NULL, NULL);
  ASSERT_TRUE(strstr(err, "non-empty namespace string") != NULL);
  free(err);

  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset rejects an empty namespace with a clear error.
 */
static void test_reset_namespace_empty_namespace_fails(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  char *err = run_reset_namespace_err("", NULL);
  ASSERT_TRUE(strstr(err, "non-empty namespace string") != NULL);
  free(err);

  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset ignores config_input and does not try to open it.
 */
static void test_reset_namespace_ignores_config_input(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");

  run_reset_namespace_ok(NS_A, "/definitely/not/a/real/config.json");

  assert_secret_missing(NS_A, "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);

  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset propagates secret-store failures and leaves the
 * state file untouched when credentials.json is malformed.
 */
static void test_reset_namespace_secret_store_failure_propagates(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  ConfDir app = {.fd = -1, .path = NULL};
  char *app_err = NULL;
  ASSERT_TRUE(confdir_default_open(&app, NULL, &app_err) == OK);
  free(app_err);
  char *cred_path = path_join(app.path, "credentials.json");
  ASSERT_TRUE(cred_path != NULL);
  confdir_clean(&app);

  write_json_file(cred_path, "{\"version\":");

  char *err = run_reset_namespace_err(NS_A, NULL);
  ASSERT_TRUE(strstr(err, "failed to wipe secrets for namespace") != NULL);
  free(err);

  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);

  free(cred_path);
  sync_test_ctx_clean(&ctx);
}

/* Verifies namespace reset reports a state-file unlink failure after the
 * secrets were already removed.
 */
static void test_reset_namespace_state_unlink_failure_reports_err(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  ASSERT_TRUE(mkdir(ctx.state_path, 0700) == 0);
  seed_secret(NS_A, "KeepPg", "keep-secret");

  char *err = run_reset_namespace_err(NS_A, NULL);
  ASSERT_TRUE(strstr(err, "state file") != NULL);
  free(err);

  assert_secret_missing(NS_A, "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);

  ASSERT_TRUE(rmdir(ctx.state_path) == 0);
  sync_test_ctx_clean(&ctx);
}

/* Verifies reset --all requires an interactive terminal and does not mutate
 * any secrets or state files on that failure path.
 */
static void test_reset_all_requires_tty_and_is_noop(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  ConfDir app = {.fd = -1, .path = NULL};
  char *app_err = NULL;
  ASSERT_TRUE(confdir_default_open(&app, NULL, &app_err) == OK);
  free(app_err);
  char *other_state_name = state_file_name_for_namespace("OtherNs");
  char *other_state_path = path_join(app.path, other_state_name);
  ASSERT_TRUE(other_state_path != NULL);
  write_json_file(other_state_path, JSON_ONE_DB_OTHER_NS);
  confdir_clean(&app);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret("OtherNs", "OtherNsPg", "other-ns-secret");

  char *err = run_reset_all_err("/definitely/not/a/real/config.json");
  ASSERT_TRUE(strstr(err, "requires an interactive terminal") != NULL);
  free(err);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value("OtherNs", "OtherNsPg", "other-ns-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  ASSERT_TRUE(access(other_state_path, F_OK) == 0);

  unlink_if_exists(other_state_path);
  free(other_state_name);
  free(other_state_path);
  sync_test_ctx_clean(&ctx);
}

/* ------------------------------ TTY TEST --------------------------------- */

typedef int (*TtyChildFn)(void);
typedef void (*TtyParentFn)(int master_fd);
static const char *g_reset_all_tty_config_input;
static const char *g_sync_all_tty_config_input;
static DbBackendFactory g_sync_all_tty_factory;
static int g_sync_all_tty_min_connect_calls = -1;
static const char *g_sync_one_tty_config_input;
static const char *g_sync_one_tty_connection_name;
static DbBackendFactory g_sync_one_tty_factory;
static int g_sync_one_tty_min_connect_calls = -1;

/* Runs 'child_fn' inside a fresh controlling terminal backed by a PTY and lets
 * 'parent_fn' drive the PTY master from the parent process. It borrows both
 * callbacks. Side effects: forks, creates a PTY pair, wires the child stdio to
 * the PTY slave, and may kill the child on timeout. It asserts on setup or
 * child-process failures and returns no value.
 */
static void test_tty(TtyChildFn child_fn, TtyParentFn parent_fn) {
  ASSERT_TRUE(child_fn != NULL);
  ASSERT_TRUE(parent_fn != NULL);

  int master_fd = posix_openpt(O_RDWR | O_NOCTTY);
  ASSERT_TRUE(master_fd >= 0);
  if (grantpt(master_fd) < 0) {
    fprintf(stderr, "failed grantpt: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }
  if (unlockpt(master_fd) < 0) {
    fprintf(stderr, "failed unlockpt: %s", strerror(errno));
    exit(EXIT_FAILURE);
  }
  char *slave_path = ptsname(master_fd);
  ASSERT_TRUE(slave_path);

  pid_t pid = fork();
  ASSERT_TRUE(pid >= 0);

  if (pid == 0) {
    close(master_fd);
    if (setsid() < 0)
      _exit(CHILD_ERR);

    /* Opening the PTY slave after setsid() gives the child a controlling
     * terminal so cred_manager can reopen it through /dev/tty.
     */
    int slave_fd = open(slave_path, O_RDWR);
    if (slave_fd < 0)
      _exit(CHILD_ERR);

    if (dup2(slave_fd, STDIN_FILENO) < 0 || dup2(slave_fd, STDOUT_FILENO) < 0 ||
        dup2(slave_fd, STDERR_FILENO) < 0) {
      close(slave_fd);
      _exit(CHILD_ERR);
    }
    close(slave_fd);

    int rc = child_fn();
    _exit(rc);
  } else {
    parent_fn(master_fd);

    int status = 0;
    for (;;) {
      pid_t w_rc = waitpid(pid, &status, 0);
      if (w_rc < 0) {
        if (errno == EINTR)
          continue;
        ASSERT_TRUE(0);
        break;
      }
      ASSERT_TRUE(w_rc == pid);
      break;
    }
    ASSERT_TRUE(WIFEXITED(status));

    int child_rc = WEXITSTATUS(status);
    ASSERT_TRUE(close(master_fd) == 0);
    ASSERT_TRUE(child_rc == CHILD_OK);
  }
}

/* Writes the NUL-terminated string 'str' to 'tty_fd'. */
static AdbxStatus tty_write_str(int tty_fd, const char *str) {
  if (tty_fd < 0 || !str)
    return ERR;

  size_t len = strlen(str);
  size_t off = 0;
  while (off < len) {
    ssize_t wr = write(tty_fd, str + off, len - off);
    if (wr < 0) {
      if (errno == EINTR)
        continue;
      return ERR;
    }
    off += (size_t)wr;
  }

  return OK;
}

/* Reads bytes from 'tty_fd' until 'str' is found or there are no more bytes to
 * read.
 */
static AdbxStatus tty_read_until(int tty_fd, const char *str) {
  if (tty_fd < 0 || !str || *str == '\0')
    return ERR;

  size_t len = strlen(str);
  char buf[512];
  size_t b_idx = 0;
  if (len >= sizeof(buf) - 2)
    return ERR;

  for (;;) {
    char ch = '\0';
    ssize_t nread = read(tty_fd, &ch, 1);
    if (nread == 0) {
      return ERR;
    }
    if (nread < 0) {
      if (errno == EINTR)
        continue;
      return ERR;
    }

    buf[b_idx++] = ch;
    if (b_idx >= len) {
      buf[b_idx] = '\0';
      if (strcmp(str, buf + b_idx - len) == 0) {
        return OK;
      }
    }

    if (b_idx > sizeof(buf) - 2) {
      return ERR;
    }
  }
}

/* Reads the remaining PTY output into 'buf' until EOF/EIO or the buffer fills.
 * It borrows 'buf' and NUL-terminates it on success.
 */
static AdbxStatus tty_read_rest(int tty_fd, char *buf, size_t cap) {
  if (tty_fd < 0 || !buf || cap < 2)
    return ERR;

  size_t off = 0;
  for (;;) {
    ssize_t nread = read(tty_fd, buf + off, cap - off - 1);
    if (nread == 0)
      break;
    if (nread < 0) {
      if (errno == EINTR)
        continue;
      if (errno == EIO)
        break;
      return ERR;
    }

    off += (size_t)nread;
    if (off >= cap - 1)
      break;
  }

  buf[off] = '\0';
  return OK;
}

/* Waits for the password prompt of one specific connection. It borrows
 * 'connection_name' and performs no allocations.
 * Side effects: consumes prompt bytes from the PTY stream.
 * Error semantics: test helper (asserts on mismatch).
 */
static void tty_expect_password_prompt(int tty_fd,
                                       const char *connection_name) {
  ASSERT_TRUE(connection_name != NULL);

  char prompt[128];
  int n =
      snprintf(prompt, sizeof(prompt), "Password for %s: ", connection_name);
  ASSERT_TRUE(n > 0);
  ASSERT_TRUE((size_t)n < sizeof(prompt));
  ASSERT_TRUE(tty_read_until(tty_fd, prompt) == OK);
}

/* Waits until the current password flow reaches the action prompt.
 * It performs no allocations.
 * Side effects: consumes bytes from the PTY stream.
 * Error semantics: test helper (asserts on mismatch).
 */
static void tty_expect_action_prompt(int tty_fd) {
  ASSERT_TRUE(tty_read_until(tty_fd, "re-enter password:") == OK);
}

/* Enters one password for 'connection_name' and waits for the follow-up action
 * prompt. It borrows all inputs and performs no allocations.
 * Side effects: writes one password line to the PTY and consumes the resulting
 * action prompt.
 * Error semantics: test helper (asserts on protocol failure).
 */
static void tty_enter_password(int tty_fd, const char *connection_name,
                               const char *password) {
  ASSERT_TRUE(password != NULL);
  tty_expect_password_prompt(tty_fd, connection_name);
  ASSERT_TRUE(tty_write_str(tty_fd, password) == OK);
  ASSERT_TRUE(tty_write_str(tty_fd, "\n") == OK);
  tty_expect_action_prompt(tty_fd);
}

/* ------------------------------ TTY RESET -------------------------------- */

/* Runs reset --all in the PTY child and succeeds only when the command returns
 * OK with no heap-allocated error string. It borrows the current test-global
 * config input.
 */
static int tty_child_reset_all_ok(void) {
  CredManagerReq req = {
      .cmd = CRED_MAN_RESET,
      .reset_scope = CRED_MAN_RESET_SCOPE_ALL,
  };
  char *err = NULL;
  AdbxStatus rc =
      cred_manager_execute(&req, g_reset_all_tty_config_input, &err);
  int ok = (rc == OK && err == NULL);
  free(err);
  return ok ? CHILD_OK : CHILD_ERR;
}

/* Drives the reset --all confirmation prompt and confirms the destructive
 * action with mixed-case input. It borrows the PTY master fd.
 */
static void tty_parent_confirm_reset_all(int master_fd) {
  char tail[128];
  ASSERT_TRUE(tty_read_until(master_fd, "RESET ALL") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "to continue: ") == OK);
  ASSERT_TRUE(tty_write_str(master_fd, "Reset aLl\n") == OK);
  ASSERT_TRUE(tty_read_rest(master_fd, tail, sizeof(tail)) == OK);
  ASSERT_TRUE(strstr(tail, "Aborted.") == NULL);
}

/* Drives the reset --all confirmation prompt and sends an invalid
 * confirmation. It borrows the PTY master fd.
 */
static void tty_parent_abort_reset_all(int master_fd) {
  ASSERT_TRUE(tty_read_until(master_fd, "RESET ALL") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "to continue: ") == OK);
  ASSERT_TRUE(tty_write_str(master_fd, "reset  all\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "Aborted.") == OK);
}

/* Verifies reset --all accepts a mixed-case confirmation, wipes every stored
 * secret, deletes only managed state.<hash>.json files, and leaves unrelated
 * files in the app dir untouched.
 */
static void test_reset_all_tty_confirm_wipes_secrets_and_state(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  ConfDir app = {.fd = -1, .path = NULL};
  ASSERT_TRUE(confdir_default_open(&app, NULL, NULL) == OK);
  char *other_state_name = state_file_name_for_namespace("OtherNs");
  char *other_state_path = path_join(app.path, other_state_name);
  char *ignore_path = path_join(app.path, "state-ignore.json");
  ASSERT_TRUE(other_state_path != NULL);
  ASSERT_TRUE(ignore_path != NULL);
  write_json_file(other_state_path, JSON_ONE_DB_OTHER_NS);
  write_json_file(ignore_path, JSON_ONE_DB);
  confdir_clean(&app);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret("OtherNs", "OtherNsPg", "other-ns-secret");
  g_reset_all_tty_config_input = "/definitely/not/a/real/config.json";
  test_tty(tty_child_reset_all_ok, tty_parent_confirm_reset_all);
  g_reset_all_tty_config_input = NULL;

  assert_secret_missing(NS_A, "KeepPg");
  assert_secret_missing("OtherNs", "OtherNsPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) != 0);
  ASSERT_TRUE(access(other_state_path, F_OK) != 0);
  ASSERT_TRUE(access(ignore_path, F_OK) == 0);

  unlink_if_exists(ignore_path);
  free(other_state_name);
  free(other_state_path);
  free(ignore_path);
  sync_test_ctx_clean(&ctx);
}

/* Verifies reset --all treats malformed confirmation input as an abort and
 * leaves secrets plus managed and unmanaged state files untouched.
 */
static void test_reset_all_tty_invalid_confirmation_is_noop(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, JSON_ONE_DB);

  ConfDir app = {.fd = -1, .path = NULL};
  ASSERT_TRUE(confdir_default_open(&app, NULL, NULL) == OK);
  char *other_state_name = state_file_name_for_namespace("OtherNs");
  char *other_state_path = path_join(app.path, other_state_name);
  char *ignore_path = path_join(app.path, "state-ignore.json");
  ASSERT_TRUE(other_state_path != NULL);
  ASSERT_TRUE(ignore_path != NULL);
  write_json_file(other_state_path, JSON_ONE_DB_OTHER_NS);
  write_json_file(ignore_path, JSON_ONE_DB);
  confdir_clean(&app);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret("OtherNs", "OtherNsPg", "other-ns-secret");
  g_reset_all_tty_config_input = "/definitely/not/a/real/config.json";
  test_tty(tty_child_reset_all_ok, tty_parent_abort_reset_all);
  g_reset_all_tty_config_input = NULL;

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value("OtherNs", "OtherNsPg", "other-ns-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  ASSERT_TRUE(access(other_state_path, F_OK) == 0);
  ASSERT_TRUE(access(ignore_path, F_OK) == 0);

  unlink_if_exists(other_state_path);
  unlink_if_exists(ignore_path);
  free(other_state_name);
  free(other_state_path);
  free(ignore_path);
  sync_test_ctx_clean(&ctx);
}

/* ----------------------------- TTY SYNC ALL ------------------------------ */

/* Runs sync-all in the PTY child and succeeds only when the command returns OK
 * with no heap-allocated error string. When configured, it also installs the
 * shared fake backend and requires at least 'g_sync_all_tty_min_connect_calls'
 * connect attempts inside the child process.
 * Returns CHILD_OK on the expected outcome, CHILD_ERR otherwise.
 */
static int tty_child_sync_all_ok(void) {
  CredManagerReq req = {.cmd = CRED_MAN_SYNC, .connection_name = NULL};
  char *err = NULL;
  if (g_sync_all_tty_factory) {
    fake_backend_reset_counters();
    db_backend_set_test_factory(g_sync_all_tty_factory);
  }

  AdbxStatus rc = cred_manager_execute(&req, g_sync_all_tty_config_input, &err);
  int ok = (rc == OK && err == NULL);
  if (ok && g_sync_all_tty_min_connect_calls >= 0) {
    ok = (fake_backend_connect_calls() >= g_sync_all_tty_min_connect_calls);
  }

  db_backend_set_test_factory(NULL);
  free(err);
  return ok ? CHILD_OK : CHILD_ERR;
}

/* Executes one PTY-backed sync-all run using 'parent_fn' as the scripted user
 * interaction. It borrows 'config_input' and 'parent_fn'.
 * Side effects: updates PTY-test globals, forks, and runs one sync-all
 * command.
 */
static void run_sync_all_tty(const char *config_input, DbBackendFactory factory,
                             int min_connect_calls, TtyParentFn parent_fn) {
  g_sync_all_tty_config_input = config_input;
  g_sync_all_tty_factory = factory;
  g_sync_all_tty_min_connect_calls = min_connect_calls;
  test_tty(tty_child_sync_all_ok, parent_fn);
  g_sync_all_tty_config_input = NULL;
  g_sync_all_tty_factory = NULL;
  g_sync_all_tty_min_connect_calls = -1;
}

static void tty_parent_sync_all_simple_save(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
  tty_enter_password(master_fd, "OtherPg", "OtherPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_all_retry_password(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "wrong");
  ASSERT_TRUE(tty_write_str(master_fd, "r\n") == OK);
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_all_test_success(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "t\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "KeepPg") == OK);
  tty_expect_action_prompt(master_fd);
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_all_test_fail_then_retry(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "wrong");
  ASSERT_TRUE(tty_write_str(master_fd, "t\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "fake auth failed") == OK);
  tty_expect_action_prompt(master_fd);
  ASSERT_TRUE(tty_write_str(master_fd, "r\n") == OK);
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_all_test_fail_then_save(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "wrong");
  ASSERT_TRUE(tty_write_str(master_fd, "t\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "fake auth failed") == OK);
  tty_expect_action_prompt(master_fd);
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_all_only_changed_prompts(int master_fd) {
  tty_enter_password(master_fd, "OtherPg", "new-other");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

/* Verifies sync-all can bootstrap an empty namespace by prompting for both
 * configured connections and persisting both secrets plus the current state
 * snapshot.
 */
static void test_sync_all_tty_missing_state_simple_save(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  run_sync_all_tty(ctx.config_path, NULL, -1, tty_parent_sync_all_simple_save);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  assert_secret_value(NS_A, "OtherPg", "OtherPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies the retry path discards the earlier typed password and stores only
 * the final retried secret.
 */
static void test_sync_all_tty_retry_replaces_typed_secret(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_all_tty(ctx.config_path, NULL, -1,
                   tty_parent_sync_all_retry_password);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies advisory connectivity testing can succeed before saving a password,
 * and that the fake backend was actually exercised.
 */
static void test_sync_all_tty_connectivity_success(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_all_tty(ctx.config_path, fake_backend_create, 1,
                   tty_parent_sync_all_test_success);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies a failed advisory connectivity test can be followed by password
 * retry, after which only the retried secret is stored.
 */
static void test_sync_all_tty_connectivity_fail_then_retry(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_all_tty(ctx.config_path, fake_backend_create, 1,
                   tty_parent_sync_all_test_fail_then_retry);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies a failed advisory connectivity test does not block saving when the
 * user explicitly continues with the typed password.
 */
static void test_sync_all_tty_connectivity_fail_then_save_anyway(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_all_tty(ctx.config_path, fake_backend_create, 1,
                   tty_parent_sync_all_test_fail_then_save);

  assert_secret_value(NS_A, "KeepPg", "wrong");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies sync-all prompts only for the changed-by-name profile when another
 * profile is already unchanged and synchronized.
 */
static void test_sync_all_tty_only_changed_profile_prompts(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB_ALT, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "old-other");
  run_sync_all_tty(ctx.config_path, NULL, -1,
                   tty_parent_sync_all_only_changed_prompts);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "new-other");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB_ALT);
  sync_test_ctx_clean(&ctx);
}

/* ----------------------------- TTY SYNC ONE ------------------------------ */

/* Runs targeted sync in the PTY child and succeeds only when the command
 * returns OK with no heap-allocated error string. When configured, it also
 * installs the shared fake backend and requires at least
 * 'g_sync_one_tty_min_connect_calls' connectivity attempts.
 * Returns CHILD_OK on the expected outcome, CHILD_ERR otherwise.
 */
static int tty_child_sync_one_ok(void) {
  CredManagerReq req = {
      .cmd = CRED_MAN_SYNC,
      .connection_name = g_sync_one_tty_connection_name,
  };
  char *err = NULL;
  if (g_sync_one_tty_factory) {
    fake_backend_reset_counters();
    db_backend_set_test_factory(g_sync_one_tty_factory);
  }

  AdbxStatus rc = cred_manager_execute(&req, g_sync_one_tty_config_input, &err);
  int ok = (rc == OK && err == NULL);
  if (ok && g_sync_one_tty_min_connect_calls >= 0) {
    ok = (fake_backend_connect_calls() >= g_sync_one_tty_min_connect_calls);
  }

  db_backend_set_test_factory(NULL);
  free(err);
  return ok ? CHILD_OK : CHILD_ERR;
}

/* Executes one PTY-backed sync-one run using 'parent_fn' as the scripted user
 * interaction. It borrows all inputs.
 */
static void run_sync_one_tty(const char *config_input,
                             const char *connection_name,
                             DbBackendFactory factory, int min_connect_calls,
                             TtyParentFn parent_fn) {
  g_sync_one_tty_config_input = config_input;
  g_sync_one_tty_connection_name = connection_name;
  g_sync_one_tty_factory = factory;
  g_sync_one_tty_min_connect_calls = min_connect_calls;
  test_tty(tty_child_sync_one_ok, parent_fn);
  g_sync_one_tty_config_input = NULL;
  g_sync_one_tty_connection_name = NULL;
  g_sync_one_tty_factory = NULL;
  g_sync_one_tty_min_connect_calls = -1;
}

static void tty_parent_sync_one_simple_save(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_one_retry_password(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "wrong");
  ASSERT_TRUE(tty_write_str(master_fd, "r\n") == OK);
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_one_test_success(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "t\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "KeepPg") == OK);
  tty_expect_action_prompt(master_fd);
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_one_test_fail_then_retry(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "wrong");
  ASSERT_TRUE(tty_write_str(master_fd, "t\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "fake auth failed") == OK);
  tty_expect_action_prompt(master_fd);
  ASSERT_TRUE(tty_write_str(master_fd, "r\n") == OK);
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_one_test_fail_then_save(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "wrong");
  ASSERT_TRUE(tty_write_str(master_fd, "t\n") == OK);
  ASSERT_TRUE(tty_read_until(master_fd, "fake auth failed") == OK);
  tty_expect_action_prompt(master_fd);
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_one_only_target_prompts(int master_fd) {
  tty_enter_password(master_fd, "OtherPg", "new-other");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

static void tty_parent_sync_one_missing_secret_prompt(int master_fd) {
  tty_enter_password(master_fd, "KeepPg", "KeepPg");
  ASSERT_TRUE(tty_write_str(master_fd, "\n") == OK);
}

/* Verifies targeted sync can bootstrap one missing-state connection without
 * touching unrelated config entries.
 */
static void test_sync_one_tty_missing_state_simple_save(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, NULL);

  run_sync_one_tty(ctx.config_path, "KeepPg", NULL, -1,
                   tty_parent_sync_one_simple_save);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  assert_secret_missing(NS_A, "OtherPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  const char *names[] = {"KeepPg"};
  assert_state_names(ctx.state_path, NS_A, names, ARRLEN(names));
  sync_test_ctx_clean(&ctx);
}

/* Verifies the retry path in targeted sync discards the earlier typed secret
 * and stores only the retried value.
 */
static void test_sync_one_tty_retry_replaces_typed_secret(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_one_tty(ctx.config_path, "KeepPg", NULL, -1,
                   tty_parent_sync_one_retry_password);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies advisory connectivity testing can succeed before targeted sync
 * persists the password, and that the fake backend is exercised.
 */
static void test_sync_one_tty_connectivity_success(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_one_tty(ctx.config_path, "KeepPg", fake_backend_create, 1,
                   tty_parent_sync_one_test_success);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies a failed advisory connectivity test can be followed by password
 * retry during targeted sync, after which only the retried secret is stored.
 */
static void test_sync_one_tty_connectivity_fail_then_retry(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_one_tty(ctx.config_path, "KeepPg", fake_backend_create, 1,
                   tty_parent_sync_one_test_fail_then_retry);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies a failed advisory connectivity test does not block targeted sync
 * when the user explicitly continues with the typed password.
 */
static void test_sync_one_tty_connectivity_fail_then_save_anyway(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_ONE_DB, NULL);

  run_sync_one_tty(ctx.config_path, "KeepPg", fake_backend_create, 1,
                   tty_parent_sync_one_test_fail_then_save);

  assert_secret_value(NS_A, "KeepPg", "wrong");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_ONE_DB);
  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync prompts only for the selected changed profile and
 * leaves unrelated synchronized secrets untouched.
 */
static void test_sync_one_tty_only_target_profile_prompts(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB_ALT, JSON_TWO_DB);

  seed_secret(NS_A, "KeepPg", "keep-secret");
  seed_secret(NS_A, "OtherPg", "old-other");
  run_sync_one_tty(ctx.config_path, "OtherPg", NULL, -1,
                   tty_parent_sync_one_only_target_prompts);

  assert_secret_value(NS_A, "KeepPg", "keep-secret");
  assert_secret_value(NS_A, "OtherPg", "new-other");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB_ALT);
  sync_test_ctx_clean(&ctx);
}

/* Verifies targeted sync re-prompts for an unchanged profile when the secret is
 * missing, without altering unrelated synchronized entries.
 */
static void test_sync_one_tty_unchanged_but_secret_missing_prompts(void) {
  SyncTestCtx ctx;
  sync_test_ctx_init(&ctx, NS_A, JSON_TWO_DB, JSON_TWO_DB);

  seed_secret(NS_A, "OtherPg", "other-secret");
  run_sync_one_tty(ctx.config_path, "KeepPg", NULL, -1,
                   tty_parent_sync_one_missing_secret_prompt);

  assert_secret_value(NS_A, "KeepPg", "KeepPg");
  assert_secret_value(NS_A, "OtherPg", "other-secret");
  ASSERT_TRUE(access(ctx.state_path, F_OK) == 0);
  assert_state_json_eq(ctx.state_path, JSON_TWO_DB);
  sync_test_ctx_clean(&ctx);
}

int main(void) {
  test_sync_missing_state_all_secrets_present();
  test_sync_unchanged_state_and_config();
  test_sync_rename_reuse();
  test_sync_removed_connection_deletion();
  test_sync_namespace_mismatch_fails_closed();
  test_sync_ambiguous_tuple_match_requires_prompt();
  test_sync_concurrent_process_rejected();

  test_sync_one_target_unchanged_unrelated_changed();
  test_sync_one_target_missing_from_config();
  test_sync_one_target_unchanged_secret_present();
  test_sync_one_target_unchanged_secret_missing_requires_prompt();
  test_sync_one_target_changed_same_name_requires_prompt();
  test_sync_one_target_unique_rename_reuses_secret();
  test_sync_one_target_unique_rename_missing_secret_requires_prompt();
  test_sync_one_target_ambiguous_tuple_requires_prompt();
  test_sync_one_target_missing_state_requires_prompt();
  test_sync_one_target_namespace_mismatch_fails_closed();
  test_sync_one_target_leaves_unrelated_stale_entries_untouched();
  test_test_one_target_missing_from_config();
  test_test_one_missing_secret_reports_fail();
  test_test_all_with_one_missing_secret();
  test_test_one_success();
  test_test_one_backend_failure();
  test_test_all_mixed_results();
  test_test_all_success();

  test_reset_namespace_deletes_only_selected_namespace();
  test_reset_namespace_missing_state_succeeds();
  test_reset_namespace_missing_secrets_succeeds();
  test_reset_namespace_missing_state_and_secrets_is_noop();
  test_reset_namespace_null_namespace_fails();
  test_reset_namespace_empty_namespace_fails();
  test_reset_namespace_ignores_config_input();
  test_reset_namespace_secret_store_failure_propagates();
  test_reset_namespace_state_unlink_failure_reports_err();
  test_reset_all_requires_tty_and_is_noop();

  // TTY tests
  test_reset_all_tty_confirm_wipes_secrets_and_state();
  test_reset_all_tty_invalid_confirmation_is_noop();
  test_sync_all_tty_missing_state_simple_save();
  test_sync_all_tty_retry_replaces_typed_secret();
  test_sync_all_tty_connectivity_success();
  test_sync_all_tty_connectivity_fail_then_retry();
  test_sync_all_tty_connectivity_fail_then_save_anyway();
  test_sync_all_tty_only_changed_profile_prompts();
  test_sync_one_tty_missing_state_simple_save();
  test_sync_one_tty_retry_replaces_typed_secret();
  test_sync_one_tty_connectivity_success();
  test_sync_one_tty_connectivity_fail_then_retry();
  test_sync_one_tty_connectivity_fail_then_save_anyway();
  test_sync_one_tty_only_target_profile_prompts();
  test_sync_one_tty_unchanged_but_secret_missing_prompts();
  fprintf(stderr, "test_cred_manager: OK\n");
  return 0;
}
