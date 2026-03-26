#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "json_codec.h"
#include "test.h"
#include "test_broker_run_utils.h"

/* 'got' must have the same 'got_len' bytes as 'expected'. */
static void assert_bytes_eq(const char *got, size_t got_len,
                            const char *expected, const char *file, int line) {
  size_t exp_len = strlen(expected);

  ASSERT_TRUE_AT(got != NULL, file, line);
  ASSERT_TRUE_AT(got_len == exp_len, file, line);
  ASSERT_TRUE_AT(memcmp(got, expected, exp_len) == 0, file, line);
}

/* Sends one list_database_connections request and compares the exact framed
 * JSON response body.
 * It borrows 'ctx', 'req_json', and 'expected_json'.
 * Side effects: opens one transient client connection and fills one StrBuf.
 * Error semantics: assertions abort on failure.
 */
static void assert_request_response_eq(const BrokerRunTestCtx *ctx,
                                       const char *req_json,
                                       const char *expected_json) {
  StrBuf resp;
  sb_init(&resp);
  ASSERT_TRUE(broker_test_request_json(ctx, req_json, &resp) == OK);
  assert_bytes_eq(resp.data, resp.len, expected_json, __FILE__, __LINE__);
  sb_clean(&resp);
}

/* Sends one tools/list request and asserts the broker returns a normal tools
 * inventory payload instead of rejecting host-supplied params metadata.
 * It borrows 'ctx' and 'req_json' and frees all temporary JSON state before
 * returning.
 * Side effects: opens one transient client connection, fills one StrBuf, and
 * parses one JSON response.
 * Error semantics: assertions abort on request, parse, or shape mismatch.
 */
static void assert_tools_list_success(const BrokerRunTestCtx *ctx,
                                      const char *req_json) {
  StrBuf resp;
  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  JsonArrIter it = {0};
  JsonGetter tool_obj = {0};
  JsonStrSpan unknown = {0};
  char *tool_name = NULL;
  uint32_t got_id = 0;
  const char *root_allowed[] = {"jsonrpc", "id", "result"};

  sb_init(&resp);
  ASSERT_TRUE(broker_test_request_json(ctx, req_json, &resp) == OK);
  ASSERT_TRUE(jsget_init(&jg, resp.data, resp.len, &tok_buf) == OK);
  ASSERT_TRUE(
      jsget_top_level_validation(&jg, NULL, root_allowed, 3, &unknown) == YES);
  ASSERT_TRUE(jsget_u32(&jg, "id", &got_id) == YES);
  ASSERT_TRUE(got_id != 0);
  ASSERT_TRUE(jsget_array_objects_begin(&jg, "result.tools", &it) == YES);
  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &tool_obj) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(&tool_obj, "name", &tool_name) == YES);
  ASSERT_TRUE(tool_name != NULL && tool_name[0] != '\0');

  free(tool_name);
  sb_clean(&resp);
}

/* Verifies list_database_connections succeeds end-to-end through the broker
 * request path when the ConnManager is sane and the request is well-formed.
 * It uses the shared test catalog and asserts the exact JSON-RPC response.
 */
static void test_list_database_connections_happy_path(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);
  ConnManager *cm = broker_test_make_cm_from_catalog(cat);
  ASSERT_TRUE(cm != NULL);

  BrokerRunTestCtx ctx = {0};
  ASSERT_TRUE(broker_test_start(&ctx, cm) == OK);

  assert_request_response_eq(
      &ctx,
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{"
      "\"name\":\"list_database_connections\",\"arguments\":{}}}",
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{"
      "\"content\":[{\"type\":\"text\",\"text\":\"Database connections listed "
      "successfully.\"}],"
      "\"structuredContent\":{\"connections\":[{\"connectionName\":\"TestDb\","
      "\"type\":\"postgres\",\"readOnly\":true}]}}}");

  broker_test_stop(&ctx);
}

/* Verifies list_database_connections returns one JSON-RPC error when the
 * underlying ConnManager catalog state becomes inconsistent.
 * It temporarily corrupts the catalog shape behind the running manager, then
 * restores it before broker teardown.
 */
static void test_list_database_connections_inconsistent_conn_manager(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);
  ConnProfile *saved_profiles = cat->profiles;
  size_t saved_n_profiles = cat->n_profiles;
  ConnManager *cm = broker_test_make_cm_from_catalog(cat);
  ASSERT_TRUE(cm != NULL);

  BrokerRunTestCtx ctx = {0};
  ASSERT_TRUE(broker_test_start(&ctx, cm) == OK);

  cat->profiles = NULL;
  cat->n_profiles = saved_n_profiles;
  assert_request_response_eq(
      &ctx,
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{"
      "\"name\":\"list_database_connections\",\"arguments\":{}}}",
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{"
      "\"code\":-32602,"
      "\"message\":\"Inconsistent internal state while executing "
      "list_database_connections, please, retry. If the issue persist, report "
      "it.\"}}");

  cat->profiles = saved_profiles;
  cat->n_profiles = saved_n_profiles;
  broker_test_stop(&ctx);
}

/* Verifies broker returns one JSON-RPC request error, rather than dropping the
 * session, when the client omits the request id.
 */
static void test_list_database_connections_missing_id_returns_error(void) {
  ConnManager *cm = broker_test_make_empty_cm();
  ASSERT_TRUE(cm != NULL);

  BrokerRunTestCtx ctx = {0};
  ASSERT_TRUE(broker_test_start(&ctx, cm) == OK);

  assert_request_response_eq(
      &ctx,
      "{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\",\"params\":{"
      "\"name\":\"list_database_connections\",\"arguments\":{}}}",
      "{\"jsonrpc\":\"2.0\",\"id\":0,\"error\":{"
      "\"code\":-32600,"
      "\"message\":\"Invalid JSON-RPC request: expected "
      "'jsonrpc':'2.0' with valid 'id', 'method', and 'params'.\"}}");

  broker_test_stop(&ctx);
}

/* Verifies list_database_connections rejects non-empty arguments because its
 * documented input schema is the empty object.
 */
static void test_list_database_connections_rejects_extra_arguments(void) {
  ConnManager *cm = broker_test_make_empty_cm();
  ASSERT_TRUE(cm != NULL);

  BrokerRunTestCtx ctx = {0};
  ASSERT_TRUE(broker_test_start(&ctx, cm) == OK);

  assert_request_response_eq(
      &ctx,
      "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{"
      "\"name\":\"list_database_connections\",\"arguments\":{\"unexpected\":1}"
      "}}",
      "{\"jsonrpc\":\"2.0\",\"id\":4,\"error\":{"
      "\"code\":-32602,"
      "\"message\":\"Invalid list_database_connections arguments: expected "
      "empty 'params.arguments' object.\"}}");

  broker_test_stop(&ctx);
}

/* Verifies tools/list ignores host-provided params metadata because the
 * response is a single static inventory page and does not depend on caller
 * input.
 */
static void test_tools_list_ignores_params_payload(void) {
  ConnManager *cm = broker_test_make_empty_cm();
  ASSERT_TRUE(cm != NULL);

  BrokerRunTestCtx ctx = {0};
  ASSERT_TRUE(broker_test_start(&ctx, cm) == OK);

  assert_tools_list_success(
      &ctx,
      "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/list\",\"params\":{"
      "\"_meta\":{\"caller\":\"codex\"},\"cursor\":\"ignored\","
      "\"unexpected\":1}}");

  broker_test_stop(&ctx);
}

int main(void) {
  test_list_database_connections_happy_path();
  test_list_database_connections_inconsistent_conn_manager();
  test_list_database_connections_missing_id_returns_error();
  test_list_database_connections_rejects_extra_arguments();
  test_tools_list_ignores_params_payload();

  fprintf(stderr, "OK: test_broker_tool_calls\n");
  return 0;
}
