#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
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

  SecretStore *store = secret_store_create();
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

  SecretStore *store = secret_store_create();
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

  SecretStore *store = secret_store_create();
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
  fprintf(stderr, "test_cred_manager: OK\n");
  return 0;
}
