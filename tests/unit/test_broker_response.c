#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "broker_response.h"
#include "json_codec.h"
#include "query_result.h"
#include "test.h"

/* 'got' must have the same 'got_len' bytes as 'expected'. */
static void assert_bytes_eq(const char *got, size_t got_len,
                            const char *expected, const char *file, int line) {

  size_t exp_len = strlen(expected);

  ASSERT_TRUE_AT(got != NULL, file, line);
  ASSERT_TRUE_AT(got_len == exp_len, file, line);
  ASSERT_TRUE_AT(memcmp(got, expected, exp_len) == 0, file, line);
}

/* Creates one integer-backed MCP id on the stack.
 * It allocates no heap memory and returns the value by copy.
 * Side effects: none.
 * Error semantics: none.
 */
static McpId id_u32(uint32_t v) {
  McpId id = {0};
  mcp_id_init_u32(&id, v);
  return id;
}

/* Stores one plaintext column definition in the borrowed builder.
 * It borrows 'qb', 'name', and 'type'.
 * Side effects: mutates the owned QueryResult behind 'qb'.
 * Returns OK on success, ERR on invalid input or out-of-bounds.
 */
static int set_col_plain(QueryResultBuilder *qb, uint32_t col, const char *name,
                         const char *type) {
  return qb_set_col(qb, col, name, type, 0);
}

/* Stores one plaintext cell value in the borrowed builder.
 * It borrows 'qb' and 'value'.
 * Side effects: mutates the owned QueryResult behind 'qb'.
 * Returns YES on success, NO on payload truncation, ERR on invalid input.
 */
static int set_cell_plain(QueryResultBuilder *qb, uint32_t row, uint32_t col,
                          const char *value) {
  return qb_set_cell(qb, row, col, value, value ? strlen(value) : 0u);
}

/* Builds one successful QueryResult response, serializes it, and compares the
 * full JSON-RPC payload against 'expected_json'.
 * It borrows all input arrays and writes no caller-owned outputs.
 * Side effects: allocates/destroys a QueryResult, BrokerResponse, and StrBuf.
 * Error semantics: assertions abort on failure.
 */
static void
encode_jsonrpc_impl(const McpId *id, uint32_t ncols, uint32_t nrows,
                    uint64_t exec_ms, uint8_t result_truncated,
                    uint64_t max_query_bytes, const char *const *col_names,
                    const char *const *col_types, const char *const *cells,
                    const char *expected_json, const char *file, int line) {

  QueryResult *qr = qr_create(ncols, nrows, result_truncated, max_query_bytes);
  ASSERT_TRUE_AT(qr != NULL, file, line);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE_AT(qb_init(&qb, qr, NULL) == OK, file, line);

  qr->exec_ms = exec_ms;

  for (uint32_t c = 0; c < ncols; ++c) {
    const char *nm = col_names ? col_names[c] : NULL;
    const char *tp = col_types ? col_types[c] : NULL;

    if (!nm) {
      ASSERT_TRUE_AT(tp == NULL, file, line);
      continue;
    }
    ASSERT_TRUE_AT(set_col_plain(&qb, c, nm, tp) == OK, file, line);
  }

  for (uint32_t r = 0; r < nrows; ++r) {
    for (uint32_t c = 0; c < ncols; ++c) {
      size_t idx = (size_t)r * (size_t)ncols + (size_t)c;
      const char *val = cells ? cells[idx] : NULL;
      ASSERT_TRUE_AT(set_cell_plain(&qb, r, c, val) == YES, file, line);
    }
  }

  BrokerResponse *bresp = bresp_create_query_result(id, qr);
  ASSERT_TRUE_AT(bresp != NULL, file, line);
  qr = NULL;

  StrBuf json;
  sb_init(&json);
  ASSERT_TRUE_AT(bresp_to_jsonrpc(bresp, &json) == OK, file, line);
  assert_bytes_eq(json.data, json.len, expected_json, file, line);

  sb_clean(&json);
  bresp_destroy(bresp);
}

#define ENCODE_JSONRPC(...) encode_jsonrpc_impl(__VA_ARGS__, __FILE__, __LINE__)

/* Validates one connection entry emitted by list_database_connections.
 * It borrows 'obj' and expected strings and allocates decoded JSON strings
 * that are freed before returning.
 * Side effects: allocates temporary decoded strings.
 * Error semantics: assertions abort on failure.
 */
static void assert_conn_profile_json_obj(const JsonGetter *obj,
                                         const char *expected_name,
                                         const char *expected_type,
                                         int expected_read_only) {
  const char *allowed[] = {"connectionName", "type", "readOnly"};
  JsonStrSpan unknown = {0};
  char *name = NULL;
  char *type = NULL;
  int read_only = -1;

  ASSERT_TRUE(jsget_top_level_validation(obj, NULL, allowed, 3, &unknown) ==
              YES);
  ASSERT_TRUE(jsget_string_decode_alloc(obj, "connectionName", &name) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(obj, "type", &type) == YES);
  ASSERT_TRUE(jsget_bool01(obj, "readOnly", &read_only) == YES);
  ASSERT_STREQ(name, expected_name);
  ASSERT_STREQ(type, expected_type);
  ASSERT_TRUE(read_only == expected_read_only);

  free(name);
  free(type);
}

/* Returns the first borrowed ConnProfile from the shared test catalog.
 * It borrows 'cat' and allocates nothing.
 * Error semantics: assertions abort on malformed catalog fixtures.
 */
static const ConnProfile *test_catalog_first_profile(ConnCatalog *cat) {
  ConnProfile *profile = NULL;

  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(catalog_list(cat, &profile, 1) == 1);
  ASSERT_TRUE(profile != NULL);
  return profile;
}

/* Creates one DbRelationInfo with copied identity metadata.
 * It borrows string inputs and returns a caller-owned relation info.
 * Side effects: allocates one DbRelationInfo and its embedded arena storage.
 * Error semantics: assertions abort on allocation or initialization failure.
 */
static DbRelationInfo *make_relation_info_impl(const char *schema_name,
                                               const char *relation_name,
                                               DbRelationKind kind,
                                               uint32_t ncols,
                                               const char *file, int line) {
  DbRelationInfo *info = db_relation_info_create(ncols);
  ASSERT_TRUE_AT(info != NULL, file, line);
  ASSERT_TRUE_AT(
      db_relation_info_set_identity(info, schema_name, relation_name, kind) ==
          OK,
      file, line);
  return info;
}

#define MAKE_RELATION_INFO(schema_name, relation_name, kind, ncols)            \
  make_relation_info_impl((schema_name), (relation_name), (kind), (ncols),    \
                          __FILE__, __LINE__)

/* Validates one column entry emitted by describe_relation.
 * It borrows 'obj' and expected strings and allocates temporary decoded
 * strings that are freed before returning.
 * Error semantics: assertions abort on failure.
 */
static void assert_relation_col_json_obj(const JsonGetter *obj,
                                         const char *expected_name,
                                         const char *expected_type,
                                         int expected_sensitive,
                                         int expected_is_primary_key,
                                         int expected_is_foreign_key,
                                         const char *expected_ref_schema,
                                         const char *expected_ref_relation,
                                         const char *expected_ref_column) {
  const char *allowed[] = {"name", "type",       "sensitive",
                           "isPrimaryKey", "isForeignKey", "references"};
  JsonStrSpan unknown = {0};
  char *name = NULL;
  char *type = NULL;
  JsonGetter ref_obj = {0};
  int sensitive = -1;
  int is_primary_key = -1;
  int is_foreign_key = -1;

  ASSERT_TRUE(jsget_top_level_validation(obj, NULL, allowed, 6, &unknown) ==
              YES);
  ASSERT_TRUE(jsget_string_decode_alloc(obj, "name", &name) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(obj, "type", &type) == YES);
  ASSERT_TRUE(jsget_bool01(obj, "sensitive", &sensitive) == YES);
  ASSERT_TRUE(jsget_bool01(obj, "isPrimaryKey", &is_primary_key) == YES);
  ASSERT_TRUE(jsget_bool01(obj, "isForeignKey", &is_foreign_key) == YES);
  ASSERT_STREQ(name, expected_name);
  ASSERT_STREQ(type, expected_type);
  ASSERT_TRUE(sensitive == expected_sensitive);
  ASSERT_TRUE(is_primary_key == expected_is_primary_key);
  ASSERT_TRUE(is_foreign_key == expected_is_foreign_key);

  if (!expected_ref_schema && !expected_ref_relation && !expected_ref_column) {
    ASSERT_TRUE(jsget_exists_nonnull(obj, "references") == NO);
  } else {
    char *ref_schema = NULL;
    char *ref_relation = NULL;
    char *ref_column = NULL;
    ASSERT_TRUE(expected_ref_schema != NULL);
    ASSERT_TRUE(expected_ref_relation != NULL);
    ASSERT_TRUE(expected_ref_column != NULL);
    ASSERT_TRUE(jsget_object(obj, "references", &ref_obj) == YES);
    ASSERT_TRUE(
        jsget_string_decode_alloc(&ref_obj, "schemaName", &ref_schema) == YES);
    ASSERT_TRUE(jsget_string_decode_alloc(&ref_obj, "relationName",
                                          &ref_relation) == YES);
    ASSERT_TRUE(
        jsget_string_decode_alloc(&ref_obj, "columnName", &ref_column) == YES);
    ASSERT_STREQ(ref_schema, expected_ref_schema);
    ASSERT_STREQ(ref_relation, expected_ref_relation);
    ASSERT_STREQ(ref_column, expected_ref_column);
    free(ref_schema);
    free(ref_relation);
    free(ref_column);
  }

  free(name);
  free(type);
}

static void test_query_result_json_basic_rows_and_nulls(void) {
  const char *col_names[] = {"id", "name", "amount"};
  const char *col_types[] = {"int4", "text", NULL};
  const char *cells[] = {"1", "alice", "10.50", "2", NULL, "99"};

  const char *expected = "{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{"
                         "\"content\":[{\"type\":\"text\",\"text\":\"Query "
                         "executed successfully.\"}],"
                         "\"structuredContent\":{"
                         "\"exec_ms\":12,"
                         "\"columns\":["
                         "{\"name\":\"id\",\"type\":\"int4\"},"
                         "{\"name\":\"name\",\"type\":\"text\"},"
                         "{\"name\":\"amount\",\"type\":\"unknown\"}"
                         "],"
                         "\"rows\":["
                         "[\"1\",\"alice\",\"10.50\"],"
                         "[\"2\",null,\"99\"]"
                         "],"
                         "\"rowcount\":2,"
                         "\"resultTruncated\":true"
                         "}}}";

  McpId id = id_u32(7);
  ENCODE_JSONRPC(&id, 3, 2, 12, 1, 0, col_names, col_types, cells, expected);
}

static void test_query_result_json_null_qrcolumn_safe_defaults(void) {
  McpId id = id_u32(100);
  QueryResult *qr = qr_create(2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);

  qr->exec_ms = 42;
  ASSERT_TRUE(set_col_plain(&qb, 0, "id", "int4") == OK);
  ASSERT_TRUE(qr_get_col(qr, 1) == NULL);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "5") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "abc") == YES);

  const char *expected = "{\"jsonrpc\":\"2.0\",\"id\":100,\"result\":{"
                         "\"content\":[{\"type\":\"text\",\"text\":\"Query "
                         "executed successfully.\"}],"
                         "\"structuredContent\":{"
                         "\"exec_ms\":42,"
                         "\"columns\":["
                         "{\"name\":\"id\",\"type\":\"int4\"},"
                         "{\"name\":\"\",\"type\":\"\"}"
                         "],"
                         "\"rows\":[[\"5\",\"abc\"]],"
                         "\"rowcount\":1,"
                         "\"resultTruncated\":false"
                         "}}}";

  BrokerResponse *bresp = bresp_create_query_result(&id, qr);
  ASSERT_TRUE(bresp != NULL);
  qr = NULL;

  StrBuf json;
  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  assert_bytes_eq(json.data, json.len, expected, __FILE__, __LINE__);

  sb_clean(&json);
  bresp_destroy(bresp);
}

static void test_query_result_json_escapes_strings(void) {
  const char *col_names[] = {"txt"};
  const char *col_types[] = {"text"};
  const char raw_with_ctrl[] = "a\"b\\c\n\td\r\x01Z";
  const char *cells[] = {raw_with_ctrl};

  const char *expected = "{\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{"
                         "\"content\":[{\"type\":\"text\",\"text\":\"Query "
                         "executed successfully.\"}],"
                         "\"structuredContent\":{"
                         "\"exec_ms\":5,"
                         "\"columns\":[{\"name\":\"txt\",\"type\":\"text\"}],"
                         "\"rows\":[[\"a\\\"b\\\\c\\n\\td\\r\\u0001Z\"]],"
                         "\"rowcount\":1,"
                         "\"resultTruncated\":false"
                         "}}}";

  McpId id = id_u32(9);
  ENCODE_JSONRPC(&id, 1, 1, 5, 0, 0, col_names, col_types, cells, expected);
}

static void test_query_result_json_empty_result(void) {
  const char *expected = "{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{"
                         "\"content\":[{\"type\":\"text\",\"text\":\"Query "
                         "executed successfully.\"}],"
                         "\"structuredContent\":{"
                         "\"exec_ms\":1,"
                         "\"columns\":[],"
                         "\"rows\":[],"
                         "\"rowcount\":0,"
                         "\"resultTruncated\":false"
                         "}}}";

  McpId id = id_u32(42);
  ENCODE_JSONRPC(&id, 0, 0, 1, 0, 0, NULL, NULL, NULL, expected);
}

static void test_rpc_error_json(void) {
  McpId id = id_u32(7);
  BrokerResponse *bresp = bresp_create_err(&id, BRESPERR_INPARAM, "bad \"x\"");
  ASSERT_TRUE(bresp != NULL);

  const char *expected = "{\"jsonrpc\":\"2.0\",\"id\":7,\"error\":{"
                         "\"code\":-32602,"
                         "\"message\":\"bad \\\"x\\\"\""
                         "}}";

  StrBuf json;
  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  assert_bytes_eq(json.data, json.len, expected, __FILE__, __LINE__);

  sb_clean(&json);
  bresp_destroy(bresp);
}

static void test_tool_error_json(void) {
  McpId id = id_u32(4);
  BrokerResponse *bresp = bresp_create_tool_err(&id, "Query failed.");
  ASSERT_TRUE(bresp != NULL);

  const char *expected =
      "{\"jsonrpc\":\"2.0\",\"id\":4,\"result\":{"
      "\"content\":[{\"type\":\"text\",\"text\":\"Query failed.\"}],"
      "\"isError\":true"
      "}}";

  StrBuf json;
  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  assert_bytes_eq(json.data, json.len, expected, __FILE__, __LINE__);

  sb_clean(&json);
  bresp_destroy(bresp);
}

static void test_query_result_json_string_id(void) {
  McpId id = {0};
  ASSERT_TRUE(mcp_id_init_str_copy(&id, "req-xyz") == OK);

  QueryResult *qr = qr_create(1, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);
  ASSERT_TRUE(set_col_plain(&qb, 0, "message", "text") == OK);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "ok") == YES);

  const char *expected =
      "{\"jsonrpc\":\"2.0\",\"id\":\"req-xyz\",\"result\":{"
      "\"content\":[{\"type\":\"text\",\"text\":\"Query executed "
      "successfully.\"}],"
      "\"structuredContent\":{"
      "\"exec_ms\":0,"
      "\"columns\":[{\"name\":\"message\",\"type\":\"text\"}],"
      "\"rows\":[[\"ok\"]],"
      "\"rowcount\":1,"
      "\"resultTruncated\":false"
      "}}}";

  BrokerResponse *bresp = bresp_create_query_result(&id, qr);
  ASSERT_TRUE(bresp != NULL);
  qr = NULL;

  StrBuf json;
  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  assert_bytes_eq(json.data, json.len, expected, __FILE__, __LINE__);

  sb_clean(&json);
  bresp_destroy(bresp);
  mcp_id_clean(&id);
}

/* Verifies that list_database_connections emits structuredContent compatible
 * with meta/tools.json and preserves the typed connection metadata.
 */
static void test_conn_profiles_broker_response_schema_shape(void) {
  ConnProfile profiles[2] = {0};
  McpId id = id_u32(88);
  StrBuf json;
  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  JsonGetter structured = {0};
  JsonGetter obj = {0};
  JsonArrIter it = {0};
  JsonStrSpan unknown = {0};
  const char *root_allowed[] = {"jsonrpc", "id", "result"};
  const char *structured_allowed[] = {"connections"};
  uint32_t got_id = 0;

  profiles[0].connection_name = "analytics";
  profiles[0].kind = DB_KIND_POSTGRES;
  profiles[0].safe_policy.read_only = 1;

  profiles[1].connection_name = "warehouse";
  profiles[1].kind = DB_KIND_POSTGRES;
  profiles[1].safe_policy.read_only = 0;

  sb_init(&json);
  BrokerResponse *bresp = bresp_create_conn_profiles(&id, profiles, 2);
  ASSERT_TRUE(bresp != NULL);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  ASSERT_TRUE(jsget_init(&jg, json.data, json.len, &tok_buf) == OK);
  ASSERT_TRUE(
      jsget_top_level_validation(&jg, NULL, root_allowed, 3, &unknown) == YES);
  ASSERT_TRUE(jsget_u32(&jg, "id", &got_id) == YES);
  ASSERT_TRUE(got_id == 88);
  ASSERT_TRUE(jsget_object(&jg, "result.structuredContent", &structured) ==
              YES);
  ASSERT_TRUE(jsget_top_level_validation(&structured, NULL, structured_allowed,
                                         1, &unknown) == YES);
  ASSERT_TRUE(jsget_array_objects_begin(
                  &jg, "result.structuredContent.connections", &it) == YES);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  assert_conn_profile_json_obj(&obj, "analytics", "postgres", 1);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  assert_conn_profile_json_obj(&obj, "warehouse", "postgres", 0);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == NO);
  sb_clean(&json);
  bresp_destroy(bresp);
}

/* Verifies the list_database_connections BrokerResponse rejects invalid
 * pointers and malformed profile data rather than emitting partial or
 * misleading JSON.
 */
static void test_conn_profiles_broker_response_bad_input(void) {
  ConnProfile profile = {0};
  McpId id = id_u32(9);
  StrBuf json;
  BrokerResponse *bresp = NULL;

  profile.connection_name = "broken";
  profile.kind = DB_KIND_POSTGRES;

  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(NULL, &json) == ERR);
  ASSERT_TRUE(bresp_create_conn_profiles(NULL, &profile, 1) == NULL);

  profile.kind = 0;
  ASSERT_TRUE(bresp_create_conn_profiles(&id, &profile, 1) == NULL);

  profile.kind = DB_KIND_POSTGRES;
  profile.connection_name = NULL;
  ASSERT_TRUE(bresp_create_conn_profiles(&id, &profile, 1) == NULL);

  profile.connection_name = "broken";
  bresp = bresp_create_conn_profiles(&id, &profile, 1);
  ASSERT_TRUE(bresp != NULL);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, NULL) == ERR);
  bresp_destroy(bresp);
  sb_clean(&json);
}

/* Verifies BrokerResponse deep-copies both string-backed ids and copied
 * connection names, so later caller mutations do not affect serialization.
 * It borrows mutable caller-owned buffers, then mutates/frees the originals
 * after BrokerResponse creation.
 */
static void test_conn_profiles_broker_response_deep_copies_inputs(void) {
  McpId id = {0};
  ConnProfile profile = {0};
  char *name = dup_or_null("analytics");
  StrBuf json;

  ASSERT_TRUE(name != NULL);
  ASSERT_TRUE(mcp_id_init_str_copy(&id, "req-copy") == OK);

  profile.connection_name = name;
  profile.kind = DB_KIND_POSTGRES;
  profile.safe_policy.read_only = 1;

  BrokerResponse *bresp = bresp_create_conn_profiles(&id, &profile, 1);
  ASSERT_TRUE(bresp != NULL);

  memcpy(name, "corrupted", strlen("corrupted") + 1u);
  mcp_id_clean(&id);

  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  assert_bytes_eq(
      json.data, json.len,
      "{\"jsonrpc\":\"2.0\",\"id\":\"req-copy\",\"result\":{"
      "\"content\":[{\"type\":\"text\",\"text\":\"Database connections "
      "listed successfully.\"}],"
      "\"structuredContent\":{\"connections\":[{\"connectionName\":"
      "\"analytics\",\"type\":\"postgres\",\"readOnly\":true}]}}}",
      __FILE__, __LINE__);

  free(name);
  sb_clean(&json);
  bresp_destroy(bresp);
}

/* Verifies list_database_connections serializes an empty connection catalog as
 * an empty JSON array rather than omitting the field or failing.
 * It borrows only stack inputs and allocates one BrokerResponse and StrBuf.
 */
static void test_conn_profiles_broker_response_empty_list(void) {
  McpId id = id_u32(5);
  BrokerResponse *bresp = bresp_create_conn_profiles(&id, NULL, 0);
  StrBuf json;

  ASSERT_TRUE(bresp != NULL);

  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  assert_bytes_eq(
      json.data, json.len,
      "{\"jsonrpc\":\"2.0\",\"id\":5,\"result\":{"
      "\"content\":[{\"type\":\"text\",\"text\":\"Database connections "
      "listed successfully.\"}],"
      "\"structuredContent\":{\"connections\":[]}}}",
      __FILE__, __LINE__);

  sb_clean(&json);
  bresp_destroy(bresp);
}

/* Verifies the public constructors reject malformed ids and other invalid
 * inputs without taking ownership of caller state on failure.
 * It borrows stack-built ids and destroys QueryResult inputs that remain owned
 * by the caller after constructor failure.
 */
static void test_broker_response_constructor_bad_input(void) {
  McpId bad_id = {.kind = MCP_ID_STR, .str = NULL};
  McpId ok_id = id_u32(11);
  ConnProfile profile = {0};

  profile.connection_name = "analytics";
  profile.kind = DB_KIND_POSTGRES;

  QueryResult *qr1 = qr_create(1, 0, 0, 0);
  ASSERT_TRUE(qr1 != NULL);
  ASSERT_TRUE(bresp_create_query_result(NULL, qr1) == NULL);
  qr_destroy(qr1);

  QueryResult *qr2 = qr_create(1, 0, 0, 0);
  ASSERT_TRUE(qr2 != NULL);
  ASSERT_TRUE(bresp_create_query_result(&bad_id, qr2) == NULL);
  qr_destroy(qr2);

  ASSERT_TRUE(bresp_create_query_result(&ok_id, NULL) == NULL);
  ASSERT_TRUE(bresp_create_conn_profiles(&bad_id, &profile, 1) == NULL);
  ASSERT_TRUE(bresp_create_err(NULL, BRESPERR_INPARAM, "x") == NULL);
  ASSERT_TRUE(bresp_create_err(&bad_id, BRESPERR_INPARAM, "x") == NULL);
  ASSERT_TRUE(bresp_create_tool_err(NULL, "x") == NULL);
  ASSERT_TRUE(bresp_create_tool_err(&bad_id, "x") == NULL);
}

/* Verifies bresp_to_jsonrpc clears caller-owned output buffers when
 * serialization fails after the buffer already contains stale data.
 * It temporarily corrupts a borrowed QueryResult after ownership transfer to
 * force the serializer down its error path, then restores the payload before
 * destruction to avoid leaking test allocations.
 */
static void test_broker_response_to_jsonrpc_resets_out_json_on_error(void) {
  McpId id = id_u32(12);
  QueryResult *qr = qr_create(1, 1, 0, 0);
  QueryResultBuilder qb = {0};
  QRColumn *saved_cols = NULL;
  StrBuf json;

  ASSERT_TRUE(qr != NULL);
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);
  ASSERT_TRUE(set_col_plain(&qb, 0, "name", "text") == OK);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "goku") == YES);

  BrokerResponse *bresp = bresp_create_query_result(&id, qr);
  ASSERT_TRUE(bresp != NULL);

  saved_cols = qr->cols;
  qr->cols = NULL;

  sb_init(&json);
  ASSERT_TRUE(sb_append_bytes(&json, "stale", 5) == OK);
  ASSERT_TRUE(json.len == 5);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == ERR);
  ASSERT_TRUE(json.len == 0);

  qr->cols = saved_cols;
  sb_clean(&json);
  bresp_destroy(bresp);
}

/* Verifies describe_relation allows zero-column relation metadata and
 * serializes an empty columns array rather than failing.
 * It borrows the shared test catalog profile and transfers relation ownership
 * into BrokerResponse on success.
 */
static void test_relation_info_broker_response_empty_columns(void) {
  ConnCatalog *cat = load_test_catalog();
  const ConnProfile *profile = test_catalog_first_profile(cat);
  DbRelationInfo *info =
      MAKE_RELATION_INFO("public", "empty_relation", DBREL_KIND_VIEW, 0);
  McpId id = id_u32(21);
  StrBuf json;
  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  JsonArrIter it = {0};
  JsonGetter obj = {0};
  char *schema_name = NULL;
  char *relation_name = NULL;
  char *relation_kind = NULL;
  uint32_t got_id = 0;

  BrokerResponse *bresp = bresp_create_relation_info(&id, profile, info);
  ASSERT_TRUE(bresp != NULL);
  info = NULL;

  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  ASSERT_TRUE(jsget_init(&jg, json.data, json.len, &tok_buf) == OK);
  ASSERT_TRUE(jsget_u32(&jg, "id", &got_id) == YES);
  ASSERT_TRUE(got_id == 21);
  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "result.structuredContent.schemaName",
                                        &schema_name) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(&jg,
                                        "result.structuredContent.relationName",
                                        &relation_name) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(&jg,
                                        "result.structuredContent.relationKind",
                                        &relation_kind) == YES);
  ASSERT_STREQ(schema_name, "public");
  ASSERT_STREQ(relation_name, "empty_relation");
  ASSERT_STREQ(relation_kind, "view");
  ASSERT_TRUE(jsget_array_objects_begin(&jg, "result.structuredContent.columns",
                                        &it) == YES);
  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == NO);

  free(schema_name);
  free(relation_name);
  free(relation_kind);
  sb_clean(&json);
  bresp_destroy(bresp);
  catalog_destroy(cat);
}

/* Verifies describe_relation rejects invalid pointers and malformed relation
 * metadata rather than accepting payloads that would fail later during
 * serialization.
 * It borrows the shared test catalog profile and destroys any relation infos
 * that remain caller-owned after constructor failure.
 */
static void test_relation_info_broker_response_bad_input(void) {
  ConnCatalog *cat = load_test_catalog();
  const ConnProfile *profile = test_catalog_first_profile(cat);
  McpId id = id_u32(22);
  DbRelationInfo *info = NULL;

  info = MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 0);
  ASSERT_TRUE(bresp_create_relation_info(NULL, profile, info) == NULL);
  db_relation_info_destroy(info);

  info = MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 0);
  ASSERT_TRUE(bresp_create_relation_info(&id, NULL, info) == NULL);
  db_relation_info_destroy(info);

  ASSERT_TRUE(bresp_create_relation_info(&id, profile, NULL) == NULL);

  info = MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 0);
  info->kind = DBREL_KIND_NONE;
  ASSERT_TRUE(bresp_create_relation_info(&id, profile, info) == NULL);
  db_relation_info_destroy(info);

  info = MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 1);
  ASSERT_TRUE(db_relation_info_set_col(info, 0, "account_id", "int4", 0, 0,
                                       NULL, NULL, NULL) == OK);
  info->cols[0].is_foreign_key = 1;
  ASSERT_TRUE(bresp_create_relation_info(&id, profile, info) == NULL);
  db_relation_info_destroy(info);

  info = MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 1);
  ASSERT_TRUE(
      db_relation_info_set_col(info, 0, "age", "int4", 0, 0, NULL, NULL, NULL) ==
      OK);
  info->cols[0].name = NULL;
  ASSERT_TRUE(bresp_create_relation_info(&id, profile, info) == NULL);
  db_relation_info_destroy(info);

  catalog_destroy(cat);
}

/* Verifies describe_relation serializes one valid relation payload with
 * broker-computed sensitivity flags and nullable/non-null references as
 * expected.
 * It borrows the shared test catalog profile and transfers relation ownership
 * into BrokerResponse on success.
 */
static void test_relation_info_broker_response_happy_path(void) {
  ConnCatalog *cat = load_test_catalog();
  const ConnProfile *profile = test_catalog_first_profile(cat);
  DbRelationInfo *info =
      MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 3);
  McpId id = id_u32(23);
  StrBuf json;
  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  JsonArrIter it = {0};
  JsonGetter obj = {0};
  uint32_t got_id = 0;

  ASSERT_TRUE(db_relation_info_set_col(info, 0, "fiscal_code", "text", 0, 0,
                                       NULL, NULL, NULL) == OK);
  ASSERT_TRUE(db_relation_info_set_col(info, 1, "age", "int4", 0, 0, NULL,
                                       NULL, NULL) == OK);
  ASSERT_TRUE(db_relation_info_set_col(info, 2, "account_id", "int4", 0, 1,
                                       "public", "accounts", "id") == OK);

  BrokerResponse *bresp = bresp_create_relation_info(&id, profile, info);
  ASSERT_TRUE(bresp != NULL);
  info = NULL;

  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  ASSERT_TRUE(jsget_init(&jg, json.data, json.len, &tok_buf) == OK);
  ASSERT_TRUE(jsget_u32(&jg, "id", &got_id) == YES);
  ASSERT_TRUE(got_id == 23);
  ASSERT_TRUE(jsget_array_objects_begin(&jg, "result.structuredContent.columns",
                                        &it) == YES);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  assert_relation_col_json_obj(&obj, "fiscal_code", "text", 1, 0, 0, NULL,
                               NULL, NULL);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  assert_relation_col_json_obj(&obj, "age", "int4", 0, 0, 0, NULL, NULL, NULL);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  assert_relation_col_json_obj(&obj, "account_id", "int4", 0, 0, 1, "public",
                               "accounts", "id");

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == NO);
  sb_clean(&json);
  bresp_destroy(bresp);
  catalog_destroy(cat);
}

/* Verifies describe_relation computes sensitivity eagerly and does not depend
 * on borrowed ConnProfile/catalog state after BrokerResponse creation.
 * It destroys the catalog and the original string-backed id before
 * serialization.
 */
static void test_relation_info_broker_response_does_not_borrow_profile(void) {
  ConnCatalog *cat = load_test_catalog();
  const ConnProfile *profile = test_catalog_first_profile(cat);
  DbRelationInfo *info =
      MAKE_RELATION_INFO("public", "users", DBREL_KIND_TABLE, 1);
  McpId id = {0};
  StrBuf json;
  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  JsonArrIter it = {0};
  JsonGetter obj = {0};
  char *got_id = NULL;
  int sensitive = -1;

  ASSERT_TRUE(mcp_id_init_str_copy(&id, "rel-owned") == OK);
  ASSERT_TRUE(db_relation_info_set_col(info, 0, "fiscal_code", "text", 0, 0,
                                       NULL, NULL, NULL) == OK);

  BrokerResponse *bresp = bresp_create_relation_info(&id, profile, info);
  ASSERT_TRUE(bresp != NULL);
  info = NULL;

  catalog_destroy(cat);
  cat = NULL;
  mcp_id_clean(&id);

  sb_init(&json);
  ASSERT_TRUE(bresp_to_jsonrpc(bresp, &json) == OK);
  ASSERT_TRUE(jsget_init(&jg, json.data, json.len, &tok_buf) == OK);
  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "id", &got_id) == YES);
  ASSERT_TRUE(jsget_array_objects_begin(&jg, "result.structuredContent.columns",
                                        &it) == YES);
  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  ASSERT_TRUE(jsget_bool01(&obj, "sensitive", &sensitive) == YES);
  ASSERT_STREQ(got_id, "rel-owned");
  ASSERT_TRUE(sensitive == 1);

  free(got_id);
  sb_clean(&json);
  bresp_destroy(bresp);
}

int main(void) {
  test_query_result_json_basic_rows_and_nulls();
  test_query_result_json_null_qrcolumn_safe_defaults();
  test_query_result_json_escapes_strings();
  test_query_result_json_empty_result();
  test_rpc_error_json();
  test_tool_error_json();
  test_query_result_json_string_id();
  test_conn_profiles_broker_response_schema_shape();
  test_conn_profiles_broker_response_bad_input();
  test_conn_profiles_broker_response_deep_copies_inputs();
  test_conn_profiles_broker_response_empty_list();
  test_broker_response_constructor_bad_input();
  test_broker_response_to_jsonrpc_resets_out_json_on_error();
  test_relation_info_broker_response_empty_columns();
  test_relation_info_broker_response_bad_input();
  test_relation_info_broker_response_happy_path();
  test_relation_info_broker_response_does_not_borrow_profile();

  fprintf(stderr, "OK: test_broker_response\n");
  return 0;
}
