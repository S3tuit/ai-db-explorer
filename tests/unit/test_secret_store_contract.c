#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "file_io.h"
#include "secret_store.h"
#include "string_op.h"
#include "test.h"

#define TEST_NAMESPACE "SecretStoreContractNamespace"
#define OTHER_NAMESPACE "SecretStoreContractOtherNamespace"
#define TEST_REF_NS(ns, name)                                                  \
  (&(SecretRefInfo){.cred_namespace = (ns), .connection_name = (name)})
#define TEST_REF(name) TEST_REF_NS(TEST_NAMESPACE, name)

typedef struct SecretStoreHarness SecretStoreHarness;

struct SecretStoreHarness {
  const char *name;
  AdbxTriStatus (*probe)(SecretStore **out_store, SecretStoreErr *out_err);
  void (*setup)(EnvGuard *env, char **tmp_out);
  void (*teardown)(EnvGuard *env, char *tmp);
};

typedef struct {
  EnvGuard env;
  char *tmp;
  SecretStore *ss;
  const SecretStoreHarness *harness;
} ContractCtx;

/* Removes one temporary config tree created for the file-backed contract
 * harness. It borrows 'tmp' and performs best-effort filesystem cleanup.
 * Side effects: unlinks credentials.json under XDG_CONFIG_HOME and removes the
 * temporary directories. This is security-relevant because it prevents test
 * secrets from persisting across runs.
 */
static void cleanup_file_tmp_tree(const char *tmp) {
  if (!tmp)
    return;

  char *app_dir = path_join(tmp, "adbxplorer");
  char *cred_path = path_join(app_dir, "credentials.json");
  char *lock_path = path_join(app_dir, "credentials.json.lock");

  if (cred_path)
    (void)unlink(cred_path);
  if (lock_path)
    (void)unlink(lock_path);
  if (app_dir)
    (void)rmdir(app_dir);
  (void)rmdir(tmp);

  free(app_dir);
  free(cred_path);
  free(lock_path);
}

/* Prepares one isolated XDG_CONFIG_HOME for the file-backed contract harness.
 * It writes one owned temporary directory into 'tmp_out' and captures the
 * previous environment into 'env'.
 */
static void contract_setup_file(EnvGuard *env, char **tmp_out) {
  ASSERT_TRUE(env != NULL);
  ASSERT_TRUE(tmp_out != NULL);

  env_guard_begin(env);
  *tmp_out = make_tmp_dir();
  ASSERT_TRUE(*tmp_out != NULL);
  ASSERT_TRUE(setenv("XDG_CONFIG_HOME", *tmp_out, 1) == 0);
}

/* Restores one file-backed contract harness environment.
 * It consumes the owned temporary directory in 'tmp' and restores the
 * environment snapshot in 'env'.
 */
static void contract_teardown_file(EnvGuard *env, char *tmp) {
  ASSERT_TRUE(env != NULL);
  cleanup_file_tmp_tree(tmp);
  free(tmp);
  env_guard_end(env);
}

/* Prepares one libsecret-backed contract harness environment.
 * It borrows 'env' and 'tmp_out'; no temporary filesystem state is needed for
 * this backend today.
 */
static void contract_setup_libsecret(EnvGuard *env, char **tmp_out) {
  ASSERT_TRUE(env != NULL);
  ASSERT_TRUE(tmp_out != NULL);
  env_guard_begin(env);
  *tmp_out = NULL;
}

/* Restores one libsecret-backed contract harness environment.
 * It borrows 'env'; 'tmp' is ignored because the libsecret harness currently
 * allocates no temporary filesystem state.
 * Side effects: restores the environment snapshot captured at setup time.
 * Return semantics: none.
 */
static void contract_teardown_libsecret(EnvGuard *env, char *tmp) {
  (void)tmp;
  ASSERT_TRUE(env != NULL);
  env_guard_end(env);
}

static const SecretStoreHarness FILE_HARNESS = {
    .name = "file",
    .probe = secret_store_file_backend_probe,
    .setup = contract_setup_file,
    .teardown = contract_teardown_file,
};

static const SecretStoreHarness LIBSECRET_HARNESS = {
    .name = "libsecret",
    .probe = secret_store_libsecret_backend_probe,
    .setup = contract_setup_libsecret,
    .teardown = contract_teardown_libsecret,
};

/* Resolves which backend contract harness this test binary should use.
 * It reads the optional ADBX_SECRET_STORE_CONTRACT_BACKEND environment
 * variable and returns one static harness descriptor.
 * Returns a borrowed harness descriptor; aborts the test on
 * unsupported backend names so misconfigured runs fail clearly.
 */
static const SecretStoreHarness *contract_select_harness(void) {
  const char *name = getenv("ADBX_SECRET_STORE_CONTRACT_BACKEND");
  if (!name || strcmp(name, "") == 0 || strcmp(name, "file") == 0)
    return &FILE_HARNESS;
  if (strcmp(name, "libsecret") == 0)
    return &LIBSECRET_HARNESS;

  fprintf(stderr,
          "Unsupported ADBX_SECRET_STORE_CONTRACT_BACKEND=%s. "
          "Expected 'file' or 'libsecret'.\n",
          name);
  exit(1);
}

/* Opens one SecretStore instance using the selected contract harness.
 * It writes owned resources into 'ctx', including the opened store.
 * Side effects: mutates environment according to the harness setup and opens
 * the selected secret-store backend.
 */
static void contract_ctx_open(ContractCtx *ctx) {
  SecretStoreErr err;

  ASSERT_TRUE(ctx != NULL);
  memset(ctx, 0, sizeof(*ctx));
  ctx->harness = contract_select_harness();
  ctx->harness->setup(&ctx->env, &ctx->tmp);

  ASSERT_TRUE(ctx->harness->probe(&ctx->ss, &err) == YES);
  ASSERT_TRUE(ctx->ss != NULL);
  ASSERT_TRUE(err.code == SSERR_NONE);
  ASSERT_TRUE(err.msg[0] == '\0');
}

/* Closes one contract test context opened by contract_ctx_open().
 */
static void contract_ctx_close(ContractCtx *ctx) {
  if (!ctx)
    return;
  if (ctx->ss)
    secret_store_destroy(ctx->ss);
  if (ctx->harness)
    ctx->harness->teardown(&ctx->env, ctx->tmp);
  memset(ctx, 0, sizeof(*ctx));
}

/* Asserts one SecretStoreErr has been cleared by a successful public call.
 * It borrows 'err' and performs no allocations.
 * Side effects: none.
 * Return semantics: none; this is a test helper and aborts on mismatch.
 */
static void assert_err_cleared(const SecretStoreErr *err) {
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(err->code == SSERR_NONE);
  ASSERT_TRUE(err->msg[0] == '\0');
}

/* Overwrites one SecretStoreErr with non-empty failure-like state.
 * It borrows 'err' and performs no allocations.
 * Side effects: mutates caller-owned error storage so later success/no-op paths
 * can prove they clear stale diagnostics.
 * Return semantics: none.
 */
static void dirty_err(SecretStoreErr *err) {
  ASSERT_TRUE(err != NULL);
  err->code = SSERR_ENV;
  (void)snprintf(err->msg, sizeof(err->msg), "dirty");
}

/* Wipes the two namespaces owned by this contract suite before one test body
 * runs. It borrows 'ctx' and performs no allocations.
 * Side effects: mutates backend state through the public API so each test
 * starts from deterministic namespace-local state. Return semantics: none; this
 * is a test helper and aborts on failure.
 */
static void contract_prepare_namespaces(ContractCtx *ctx) {
  SecretStoreErr err;

  ASSERT_TRUE(ctx != NULL);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx->ss, TEST_NAMESPACE, &err) == OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx->ss, OTHER_NAMESPACE, &err) ==
              OK);
  assert_err_cleared(&err);
}

/* Verifies get() returns NO in a clean environment for a missing reference. */
static void test_clean_env_get_returns_no(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);
  sb_init(&out);
  dirty_err(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MissingPg"), &out, &err) ==
              NO);
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies delete() returns OK when the reference is absent in a clean env. */
static void test_clean_env_delete_returns_ok(void) {
  ContractCtx ctx;
  SecretStoreErr err;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_delete(ctx.ss, TEST_REF("MissingPg"), &err) == OK);
  assert_err_cleared(&err);

  contract_ctx_close(&ctx);
}

/* Verifies wipe_all() is a clean-env no-op and the store remains writable. */
static void test_clean_env_wipe_all_is_noop_and_later_set_get_works(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_wipe_all(ctx.ss, &err) == OK);
  assert_err_cleared(&err);

  sb_init(&out);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-after-wipe",
                               &err) == OK);

  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              YES);
  ASSERT_STREQ(out.data, "pw-after-wipe");
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies wipe_namespace() is a clean-env no-op and later set/get work. */
static void
test_clean_env_wipe_namespace_is_noop_and_later_set_get_works(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE, &err) == OK);
  assert_err_cleared(&err);

  sb_init(&out);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"),
                               "pw-after-ns-wipe", &err) == OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              YES);
  ASSERT_STREQ(out.data, "pw-after-ns-wipe");
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies the basic set/get/delete roundtrip on one reference. */
static void test_set_get_delete_roundtrip(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              NO);
  assert_err_cleared(&err);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1", &err) ==
              OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              YES);
  ASSERT_STREQ(out.data, "pw-1");
  assert_err_cleared(&err);

  ASSERT_TRUE(secret_store_delete(ctx.ss, TEST_REF("MyPostgres"), &err) == OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              NO);
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies set() overwrites the existing secret for the same typed reference.
 */
static void test_set_overwrite_current_secret(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  sb_init(&out);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1", &err) ==
              OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-2", &err) ==
              OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              YES);
  ASSERT_STREQ(out.data, "pw-2");
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies wipe_all() removes secrets from every namespace. */
static void test_wipe_all_removes_all_namespaces(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("KeepPg"), "pw-main", &err) ==
              OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               "pw-other", &err) == OK);
  assert_err_cleared(&err);

  ASSERT_TRUE(secret_store_wipe_all(ctx.ss, &err) == OK);
  assert_err_cleared(&err);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("KeepPg"), &out, &err) == NO);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               &out, &err) == NO);
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies wipe_namespace() removes only the selected namespace. */
static void test_wipe_namespace_removes_only_target_namespace(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("KeepPg"), "pw-main", &err) ==
              OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               "pw-other", &err) == OK);
  assert_err_cleared(&err);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE, &err) == OK);
  assert_err_cleared(&err);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("KeepPg"), &out, &err) == NO);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               &out, &err) == YES);
  ASSERT_STREQ(out.data, "pw-other");
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies wiping a missing namespace is a no-op for existing secrets. */
static void test_wipe_namespace_missing_namespace_is_noop(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("KeepPg"), "pw-main", &err) ==
              OK);
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               "pw-other", &err) == OK);
  assert_err_cleared(&err);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, "MissingNamespace", &err) ==
              OK);
  assert_err_cleared(&err);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("KeepPg"), &out, &err) == YES);
  ASSERT_STREQ(out.data, "pw-main");
  assert_err_cleared(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF_NS(OTHER_NAMESPACE, "OtherPg"),
                               &out, &err) == YES);
  ASSERT_STREQ(out.data, "pw-other");
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies wipe_namespace() rejects invalid namespace inputs. */
static void test_wipe_namespace_invalid_namespace_reports_input_error(void) {
  ContractCtx ctx;
  SecretStoreErr err;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, NULL, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, "", &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);

  contract_ctx_close(&ctx);
}

/* Verifies invalid typed references are rejected with SSERR_INPUT. */
static void test_invalid_ref_inputs_report_input_error(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  SecretRefInfo empty_ns = {.cred_namespace = "", .connection_name = "Pg"};
  SecretRefInfo empty_name = {.cred_namespace = TEST_NAMESPACE,
                              .connection_name = ""};
  SecretRefInfo null_ns = {.cred_namespace = NULL, .connection_name = "Pg"};
  SecretRefInfo null_name = {.cred_namespace = TEST_NAMESPACE,
                             .connection_name = NULL};
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  sb_init(&out);
  ASSERT_TRUE(secret_store_get(ctx.ss, NULL, &out, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_get(ctx.ss, &empty_ns, &out, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_get(ctx.ss, &empty_name, &out, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);

  ASSERT_TRUE(secret_store_set(ctx.ss, NULL, "pw", &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_set(ctx.ss, &null_ns, "pw", &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_set(ctx.ss, &null_name, "pw", &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);

  ASSERT_TRUE(secret_store_delete(ctx.ss, NULL, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_delete(ctx.ss, &empty_ns, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);
  ASSERT_TRUE(secret_store_delete(ctx.ss, &empty_name, &err) == ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

/* Verifies set() rejects a NULL secret payload with SSERR_INPUT. */
static void test_set_null_secret_reports_input_error(void) {
  ContractCtx ctx;
  SecretStoreErr err;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), NULL, &err) ==
              ERR);
  ASSERT_TRUE(err.code == SSERR_INPUT);

  contract_ctx_close(&ctx);
}

/* Verifies successful public operations clear stale caller-owned errors. */
static void test_success_clears_output_error(void) {
  ContractCtx ctx;
  SecretStoreErr err;
  StrBuf out;

  contract_ctx_open(&ctx);
  contract_prepare_namespaces(&ctx);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_set(ctx.ss, TEST_REF("MyPostgres"), "pw-1", &err) ==
              OK);
  assert_err_cleared(&err);

  sb_init(&out);
  dirty_err(&err);
  ASSERT_TRUE(secret_store_get(ctx.ss, TEST_REF("MyPostgres"), &out, &err) ==
              YES);
  ASSERT_STREQ(out.data, "pw-1");
  assert_err_cleared(&err);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_delete(ctx.ss, TEST_REF("MyPostgres"), &err) == OK);
  assert_err_cleared(&err);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_wipe_namespace(ctx.ss, TEST_NAMESPACE, &err) == OK);
  assert_err_cleared(&err);

  dirty_err(&err);
  ASSERT_TRUE(secret_store_wipe_all(ctx.ss, &err) == OK);
  assert_err_cleared(&err);

  sb_zero_clean(&out);
  contract_ctx_close(&ctx);
}

int main(void) {
  test_clean_env_get_returns_no();
  test_clean_env_delete_returns_ok();
  test_clean_env_wipe_all_is_noop_and_later_set_get_works();
  test_clean_env_wipe_namespace_is_noop_and_later_set_get_works();
  test_set_get_delete_roundtrip();
  test_set_overwrite_current_secret();
  test_wipe_all_removes_all_namespaces();
  test_wipe_namespace_removes_only_target_namespace();
  test_wipe_namespace_missing_namespace_is_noop();
  test_wipe_namespace_invalid_namespace_reports_input_error();
  test_invalid_ref_inputs_report_input_error();
  test_set_null_secret_reports_input_error();
  test_success_clears_output_error();
  fprintf(stderr, "OK: test_secret_store_contract\n");
  return 0;
}
