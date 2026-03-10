#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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
static const char *JSON_ONE_DB =
    "{"
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
static const char *JSON_TWO_DB =
    "{"
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
static const char *JSON_TWO_DB_ALT =
    "{"
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
static const char *JSON_RENAME_CONF =
    "{"
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

/* Two connections with identical tuples (same as KeepPg): for ambiguous rename
 * detection tests. */
static const char *JSON_AMBIG_CONF =
    "{"
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
static const char *JSON_ONE_DB_NS_B =
    "{"
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

int main(void) {
  test_sync_missing_state_all_secrets_present();
  test_sync_unchanged_state_and_config();
  test_sync_rename_reuse();
  test_sync_removed_connection_deletion();
  test_sync_namespace_mismatch_fails_closed();
  test_sync_ambiguous_tuple_match_requires_prompt();

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
  fprintf(stderr, "test_cred_manager: OK\n");
  return 0;
}
