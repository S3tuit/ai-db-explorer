#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cli_args.h"
#include "test.h"

/* Parses one argv vector and asserts success.
 * It borrows 'argv' and writes the parsed result into caller-owned 'out'.
 * Side effects: initializes a local error buffer for cli_parse_args().
 * Error semantics: assertions abort on parse failure.
 */
static void parse_ok_impl(int argc, char **argv, CliArgs *out,
                          const char *file, int line) {
  char err[512];
  memset(err, 0, sizeof(err));
  memset(out, 0, sizeof(*out));

  ASSERT_TRUE_AT(cli_parse_args(argc, argv, out, err, sizeof(err)) == OK, file,
                 line);
  ASSERT_TRUE_AT(err[0] == '\0', file, line);
}

#define PARSE_OK(argc, argv, out)                                             \
  parse_ok_impl((argc), (argv), (out), __FILE__, __LINE__)

/* Parses one argv vector and asserts failure with an error substring.
 * It borrows 'argv' and 'expected_substr' and allocates no memory.
 * Side effects: initializes a local error buffer for cli_parse_args().
 * Error semantics: assertions abort when parsing unexpectedly succeeds or the
 * error text does not contain the expected stable substring.
 */
static void parse_err_contains_impl(int argc, char **argv,
                                    const char *expected_substr,
                                    const char *file, int line) {
  CliArgs out = {0};
  char err[512];
  memset(err, 0, sizeof(err));

  ASSERT_TRUE_AT(cli_parse_args(argc, argv, &out, err, sizeof(err)) == ERR,
                 file, line);
  ASSERT_TRUE_AT(expected_substr != NULL, file, line);
  ASSERT_TRUE_AT(strstr(err, expected_substr) != NULL, file, line);
}

#define PARSE_ERR_CONTAINS(argc, argv, expected_substr)                       \
  parse_err_contains_impl((argc), (argv), (expected_substr), __FILE__,        \
                          __LINE__)

/* Verifies the default CLI shape selects client mode with no optional paths.
 * It allocates no memory and borrows the stack argv vector.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_defaults_to_client(void) {
  char *argv[] = {"adbxplorer"};
  CliArgs out = {0};

  PARSE_OK(1, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CLIENT);
  ASSERT_TRUE(out.app_dir_input == NULL);
  ASSERT_TRUE(out.config_input == NULL);
}

/* Verifies explicit client mode accepts both app and config paths.
 * It allocates no memory and borrows the stack argv vector.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_client_paths(void) {
  char *argv[] = {"adbxplorer", "-client", "-appdir", "/tmp/app",
                  "-config",    "/tmp/conf.json"};
  CliArgs out = {0};

  PARSE_OK(6, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CLIENT);
  ASSERT_STREQ(out.app_dir_input, "/tmp/app");
  ASSERT_STREQ(out.config_input, "/tmp/conf.json");
}

/* Verifies explicit broker mode accepts both app and config paths.
 * It allocates no memory and borrows the stack argv vector.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_broker_paths(void) {
  char *argv[] = {"adbxplorer", "-broker", "-appdir", "/tmp/app",
                  "-config",    "/tmp/conf.json"};
  CliArgs out = {0};

  PARSE_OK(6, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_BROKER);
  ASSERT_STREQ(out.app_dir_input, "/tmp/app");
  ASSERT_STREQ(out.config_input, "/tmp/conf.json");
}

/* Verifies cred sync mode accepts an optional config path without a
 * connection operand.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_cred_sync_without_connection(void) {
  char *argv[] = {"adbxplorer", "-cred", "-s", "-config", "/tmp/conf.json"};
  CliArgs out = {0};

  PARSE_OK(5, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CRED);
  ASSERT_TRUE(out.app_dir_input == NULL);
  ASSERT_STREQ(out.config_input, "/tmp/conf.json");
  ASSERT_TRUE(out.cred_req.cmd == CRED_MAN_SYNC);
  ASSERT_TRUE(out.cred_req.connection_name == NULL);
}

/* Verifies cred sync mode accepts an optional connection operand.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_cred_sync_with_connection(void) {
  char *argv[] = {"adbxplorer", "-cred", "--sync", "MyPostgres"};
  CliArgs out = {0};

  PARSE_OK(4, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CRED);
  ASSERT_TRUE(out.cred_req.cmd == CRED_MAN_SYNC);
  ASSERT_STREQ(out.cred_req.connection_name, "MyPostgres");
}

/* Verifies cred test mode accepts no connection operand.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_cred_test_without_connection(void) {
  char *argv[] = {"adbxplorer", "-cred", "-t"};
  CliArgs out = {0};

  PARSE_OK(3, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CRED);
  ASSERT_TRUE(out.cred_req.cmd == CRED_MAN_TEST);
  ASSERT_TRUE(out.cred_req.connection_name == NULL);
}

/* Verifies cred test mode accepts an optional connection operand.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_cred_test_with_connection(void) {
  char *argv[] = {"adbxplorer", "-cred", "--test", "AnotherPostgres"};
  CliArgs out = {0};

  PARSE_OK(4, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CRED);
  ASSERT_TRUE(out.cred_req.cmd == CRED_MAN_TEST);
  ASSERT_STREQ(out.cred_req.connection_name, "AnotherPostgres");
}

/* Verifies cred reset mode accepts one namespace operand.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_cred_reset_namespace(void) {
  char *argv[] = {"adbxplorer", "-cred", "-r", "my_namespace"};
  CliArgs out = {0};

  PARSE_OK(4, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CRED);
  ASSERT_TRUE(out.cred_req.cmd == CRED_MAN_RESET);
  ASSERT_TRUE(out.cred_req.reset_scope == CRED_MAN_RESET_SCOPE_NAMESPACE);
  ASSERT_STREQ(out.cred_req.cred_namespace, "my_namespace");
}

/* Verifies cred reset mode accepts the --everything scope.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_cred_reset_everything(void) {
  char *argv[] = {"adbxplorer", "-cred", "-r", "--everything"};
  CliArgs out = {0};

  PARSE_OK(4, argv, &out);
  ASSERT_TRUE(out.mode == APP_MODE_CRED);
  ASSERT_TRUE(out.cred_req.cmd == CRED_MAN_RESET);
  ASSERT_TRUE(out.cred_req.reset_scope == CRED_MAN_RESET_SCOPE_ALL);
  ASSERT_TRUE(out.cred_req.cred_namespace == NULL);
}

/* Verifies conflicting top-level modes are rejected.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_conflicting_modes(void) {
  char *argv[] = {"adbxplorer", "-client", "-broker"};
  PARSE_ERR_CONTAINS(3, argv, "conflicting top-level modes");
}

/* Verifies conflicting credential commands are rejected.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_conflicting_cred_commands(void) {
  char *argv[] = {"adbxplorer", "-cred", "-s", "-t"};
  PARSE_ERR_CONTAINS(4, argv, "conflicting credential commands");
}

/* Verifies malformed cred invocations are rejected uniformly.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_invalid_cred_shapes(void) {
  char *argv_no_cmd[] = {"adbxplorer", "-cred"};
  char *argv_reset_missing[] = {"adbxplorer", "-cred", "-r"};
  char *argv_reset_mixed[] = {"adbxplorer", "-cred", "-r", "ns",
                              "--everything"};

  PARSE_ERR_CONTAINS(2, argv_no_cmd, "invalid '-cred' invocation");
  PARSE_ERR_CONTAINS(3, argv_reset_missing, "invalid '-cred' invocation");
  PARSE_ERR_CONTAINS(5, argv_reset_mixed, "invalid '-cred' invocation");
}

/* Verifies appdir is rejected in cred mode.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_appdir_in_cred_mode(void) {
  char *argv[] = {"adbxplorer", "-cred", "-appdir", "/tmp/app", "-s"};
  PARSE_ERR_CONTAINS(5, argv, "'-appdir' is not supported");
}

/* Verifies cred-only flags and operands are rejected outside cred mode.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_cred_only_inputs_outside_cred_mode(void) {
  char *argv_sync[] = {"adbxplorer", "-s"};
  char *argv_test[] = {"adbxplorer", "-t"};
  char *argv_reset[] = {"adbxplorer", "-r", "ns"};
  char *argv_everything[] = {"adbxplorer", "--everything"};
  char *argv_operand[] = {"adbxplorer", "MyPostgres"};

  PARSE_ERR_CONTAINS(2, argv_sync, "credential-only flags require '-cred'");
  PARSE_ERR_CONTAINS(2, argv_test, "credential-only flags require '-cred'");
  PARSE_ERR_CONTAINS(3, argv_reset, "credential-only flags require '-cred'");
  PARSE_ERR_CONTAINS(2, argv_everything,
                     "credential-only flags require '-cred'");
  PARSE_ERR_CONTAINS(2, argv_operand, "credential-only flags require '-cred'");
}

/* Verifies an extra positional operand in cred mode is rejected.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_extra_positional_operand(void) {
  char *argv[] = {"adbxplorer", "-cred", "-s", "one", "two"};
  PARSE_ERR_CONTAINS(5, argv, "unexpected extra positional argument");
}

/* Verifies unknown flags and missing option values are rejected.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_rejects_unknown_or_incomplete_flags(void) {
  char *argv_unknown[] = {"adbxplorer", "--bogus"};
  char *argv_config_missing[] = {"adbxplorer", "-config"};
  char *argv_appdir_missing[] = {"adbxplorer", "-appdir"};

  PARSE_ERR_CONTAINS(2, argv_unknown, "unknown flag");
  PARSE_ERR_CONTAINS(2, argv_config_missing, "missing path after '-config'");
  PARSE_ERR_CONTAINS(2, argv_appdir_missing, "missing path after '-appdir'");
}

/* Verifies cli_parse_args fails closed on invalid API inputs.
 * Error semantics: assertions abort on failure.
 */
static void test_cli_parse_invalid_api_inputs(void) {
  char *argv[] = {"adbxplorer"};
  CliArgs out = {0};
  char err[64];
  memset(err, 0, sizeof(err));

  ASSERT_TRUE(cli_parse_args(1, NULL, &out, err, sizeof(err)) == ERR);
  ASSERT_TRUE(cli_parse_args(1, argv, NULL, err, sizeof(err)) == ERR);
  ASSERT_TRUE(cli_parse_args(1, argv, &out, NULL, sizeof(err)) == ERR);
  ASSERT_TRUE(cli_parse_args(1, argv, &out, err, 0) == ERR);
}

int main(void) {
  test_cli_parse_defaults_to_client();
  test_cli_parse_client_paths();
  test_cli_parse_broker_paths();
  test_cli_parse_cred_sync_without_connection();
  test_cli_parse_cred_sync_with_connection();
  test_cli_parse_cred_test_without_connection();
  test_cli_parse_cred_test_with_connection();
  test_cli_parse_cred_reset_namespace();
  test_cli_parse_cred_reset_everything();
  test_cli_parse_rejects_conflicting_modes();
  test_cli_parse_rejects_conflicting_cred_commands();
  test_cli_parse_rejects_invalid_cred_shapes();
  test_cli_parse_rejects_appdir_in_cred_mode();
  test_cli_parse_rejects_cred_only_inputs_outside_cred_mode();
  test_cli_parse_rejects_extra_positional_operand();
  test_cli_parse_rejects_unknown_or_incomplete_flags();
  test_cli_parse_invalid_api_inputs();
  printf("OK: test_cli_args\n");
  return 0;
}
