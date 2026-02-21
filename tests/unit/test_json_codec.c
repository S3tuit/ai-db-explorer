#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "json_codec.h"
#include "query_result.h"
#include "test.h"

/* -------------------------------- helpers -------------------------------- */

/* 'got' must have the same 'got_len' bytes as 'expected'. */
static void assert_bytes_eq(const char *got, size_t got_len,
                            const char *expected, const char *file, int line) {

  size_t exp_len = strlen(expected);

  ASSERT_TRUE_AT(got != NULL, file, line);
  ASSERT_TRUE_AT(got_len == exp_len, file, line);
  ASSERT_TRUE_AT(memcmp(got, expected, exp_len) == 0, file, line);
}

static McpId id_u32(uint32_t v) {
  McpId id = {0};
  mcp_id_init_u32(&id, v);
  return id;
}

static int set_col_plain(QueryResultBuilder *qb, uint32_t col,
                         const char *name, const char *type) {
  return qb_set_col(qb, col, name, type, 0);
}

static int set_cell_plain(QueryResultBuilder *qb, uint32_t row, uint32_t col,
                          const char *value) {
  return qb_set_cell(qb, row, col, value, value ? strlen(value) : 0u);
}

/* builds a QueryResult, fills cols/cells, encode into JSON, compares payload.
 */
static void
encode_jsonrpc_impl(const McpId *id, uint32_t ncols, uint32_t nrows,
                    uint64_t exec_ms, uint8_t result_truncated,
                    uint64_t max_query_bytes, const char *const *col_names,
                    const char *const *col_types, const char *const *cells,
                    const char *expected_json, const char *file, int line) {

  QueryResult *qr =
      qr_create_ok(id, ncols, nrows, result_truncated, max_query_bytes);
  ASSERT_TRUE_AT(qr != NULL, file, line);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE_AT(qb_init(&qb, qr, NULL) == OK, file, line);

  qr->exec_ms = exec_ms;

  /* set columns (if provided) */
  for (uint32_t c = 0; c < ncols; ++c) {
    const char *nm = col_names ? col_names[c] : NULL;
    const char *tp = col_types ? col_types[c] : NULL;

    if (!nm) {
      ASSERT_TRUE_AT(tp == NULL, file, line);
      continue;
    }
    int rc = set_col_plain(&qb, c, nm, tp);
    ASSERT_TRUE_AT(rc == OK, file, line);
  }

  /* set cells */
  for (uint32_t r = 0; r < nrows; ++r) {
    for (uint32_t c = 0; c < ncols; ++c) {
      size_t idx = (size_t)r * (size_t)ncols + (size_t)c;
      const char *val = cells ? cells[idx] : NULL;
      int rc = set_cell_plain(&qb, r, c, val);
      ASSERT_TRUE_AT(rc == YES, file, line);
    }
  }

  char *json = NULL;
  size_t json_len = 0;
  int rc = qr_to_jsonrpc(qr, &json, &json_len);

  ASSERT_TRUE_AT(rc == OK, file, line);
  assert_bytes_eq(json, json_len, expected_json, file, line);

  free(json);
  qr_destroy(qr);
}

#define ENCODE_JSONRPC(...) encode_jsonrpc_impl(__VA_ARGS__, __FILE__, __LINE__)

/* ------------------------------ tests ------------------------------ */

static void test_json_basic_rows_and_nulls(void) {
  const char *col_names[] = {"id", "name", "amount"};
  const char *col_types[] = {"int4", "text", NULL}; /* amount -> "unknown" */

  /* 2 rows x 3 cols */
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
  ENCODE_JSONRPC(
      /* id */ &id,
      /* ncols */ 3,
      /* nrows */ 2,
      /* exec_ms */ 12,
      /* result_truncated */ 1,
      /* max_query_bytes */ 0, col_names, col_types, cells, expected);
}

static void test_json_null_qrcolumn_safe_defaults(void) {
  /* 2 columns, but we only set column 0 */
  McpId id = id_u32(100);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);

  qr->exec_ms = 42;

  /* Set only column 0 */
  ASSERT_TRUE(set_col_plain(&qb, 0, "id", "int4") == OK);

  /* Column 1 is completely unset */
  ASSERT_TRUE(qr_get_col(qr, 1) == NULL);

  /* Set cells anyway (json must not rely on column metadata existing) */
  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "5") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "abc") == YES);

  /* If qr_get_col returns NULL, json uses empty strings "" in output */
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

  char *json = NULL;
  size_t json_len = 0;
  int rc = qr_to_jsonrpc(qr, &json, &json_len);

  ASSERT_TRUE(rc == OK);
  assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

  free(json);
  qr_destroy(qr);
}

static void test_json_escapes_strings(void) {
  const char *col_names[] = {"txt"};
  const char *col_types[] = {"text"};

  /* Include quotes, backslash, newline, tab, carriage return, and a control
   * char 0x01 */
  const char raw_with_ctrl[] = "a\"b\\c\n\td\r\x01Z";

  const char *cells[] = {raw_with_ctrl};

  /* Expected JSON escaping:
     "  -> \"
     \  -> \\
     \n -> \\n
     \t -> \\t
     \r -> \\r
     0x01 -> \\u0001
  */
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

static void test_json_empty_result(void) {
  /* 0 cols, 0 rows */
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

static void test_json_error_result(void) {
  McpId id = id_u32(7);
  QueryResult *qr = qr_create_err(&id, QRERR_INPARAM, "bad \"x\"");
  ASSERT_TRUE(qr != NULL);

  const char *expected = "{\"jsonrpc\":\"2.0\",\"id\":7,\"error\":{"
                         "\"code\":-32602,"
                         "\"message\":\"bad \\\"x\\\"\""
                         "}}";

  char *json = NULL;
  size_t json_len = 0;
  int rc = qr_to_jsonrpc(qr, &json, &json_len);

  ASSERT_TRUE(rc == OK);
  assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

  free(json);
  qr_destroy(qr);
}

static void test_json_tool_error_result(void) {
  McpId id = id_u32(4);
  QueryResult *qr = qr_create_tool_err(&id, "Query failed.");
  ASSERT_TRUE(qr != NULL);

  const char *expected =
      "{\"jsonrpc\":\"2.0\",\"id\":4,\"result\":{"
      "\"content\":[{\"type\":\"text\",\"text\":\"Query failed.\"}],"
      "\"isError\":true"
      "}}";

  char *json = NULL;
  size_t json_len = 0;
  int rc = qr_to_jsonrpc(qr, &json, &json_len);

  ASSERT_TRUE(rc == OK);
  assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

  free(json);
  qr_destroy(qr);
}

static void test_json_string_id(void) {
  McpId id = {0};
  ASSERT_TRUE(mcp_id_init_str_copy(&id, "req-xyz") == OK);

  QueryResult *qr = qr_create_msg(&id, "ok");
  ASSERT_TRUE(qr != NULL);

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

  char *json = NULL;
  size_t json_len = 0;
  int rc = qr_to_jsonrpc(qr, &json, &json_len);
  ASSERT_TRUE(rc == OK);
  assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

  free(json);
  qr_destroy(qr);
  mcp_id_clean(&id);
}

static void test_json_builder_object(void) {
  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_obj_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_str(&sb, "a", "x") == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "b", 2) == OK);
  ASSERT_TRUE(json_kv_l(&sb, "c", -3) == OK);
  ASSERT_TRUE(json_kv_bool(&sb, "d", 1) == OK);
  ASSERT_TRUE(json_kv_bool(&sb, "e", 0) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);

  const char *expected =
      "{\"a\":\"x\",\"b\":2,\"c\":-3,\"d\":true,\"e\":false}";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);

  ASSERT_TRUE(json_rpc_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "id", 1) == OK);
  const char *exp2 = "{\"jsonrpc\":\"2.0\",\"id\":1";
  assert_bytes_eq(sb.data, sb.len, exp2, __FILE__, __LINE__);
  sb_clean(&sb);
}

static void test_json_builder_array(void) {
  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_arr_begin(&sb) == OK);
  ASSERT_TRUE(json_arr_elem_str(&sb, "x") == OK);
  ASSERT_TRUE(json_arr_elem_u64(&sb, 2) == OK);
  ASSERT_TRUE(json_arr_elem_l(&sb, -3) == OK);
  ASSERT_TRUE(json_arr_elem_bool(&sb, 1) == OK);
  ASSERT_TRUE(json_arr_elem_bool(&sb, 0) == OK);
  ASSERT_TRUE(json_arr_end(&sb) == OK);

  const char *expected = "[\"x\",2,-3,true,false]";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);
}

static void test_json_builder_nested(void) {
  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_obj_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_obj_begin(&sb, "a") == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "b", 1) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);
  ASSERT_TRUE(json_kv_arr_begin(&sb, "c") == OK);
  ASSERT_TRUE(json_arr_elem_bool(&sb, 1) == OK);
  ASSERT_TRUE(json_arr_end(&sb) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);

  const char *expected = "{\"a\":{\"b\":1},\"c\":[true]}";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);
}

static void test_jsget_simple_rpc_validation(void) {
  const char *ok =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\",\"params\":{}}";
  const char *ok_str =
      "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"exec\"}";
  const char *no_id = "{\"jsonrpc\":\"2.0\",\"method\":\"exec\"}";
  const char *bad_ver = "{\"jsonrpc\":\"2.1\",\"id\":1,\"method\":\"exec\"}";
  const char *bad_json = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\"";

  JsonGetter jg;
  ASSERT_TRUE(jsget_init(&jg, ok, strlen(ok)) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == YES);

  ASSERT_TRUE(jsget_init(&jg, ok_str, strlen(ok_str)) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == YES);

  ASSERT_TRUE(jsget_init(&jg, no_id, strlen(no_id)) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == NO);

  ASSERT_TRUE(jsget_init(&jg, bad_ver, strlen(bad_ver)) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == NO);

  ASSERT_TRUE(jsget_init(&jg, bad_json, strlen(bad_json)) == ERR);
}

static void test_jsget_paths(void) {
  const char *json =
      "{\"a\":\"x\",\"b\":{\"c\":\"hello\",\"d\":{\"e\":\"z\"}}}";
  JsonGetter jg;
  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);

  char *s1 = NULL;
  char *s2 = NULL;
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_string_span(&jg, "a", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'x');

  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "b.c", &s1) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "b.d.e", &s2) == YES);
  ASSERT_STREQ(s1, "hello");
  ASSERT_STREQ(s2, "z");

  free(s1);
  free(s2);
}

static void test_jsget_object_view(void) {
  const char *json = "{\"a\":{\"b\":{\"c\":\"z\",\"num\":77},\"n\":1},\"x\":2}";
  JsonGetter jg;
  JsonGetter sub;
  JsonStrSpan sp = {0};
  uint32_t num = 0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_object(&jg, "a.b", &sub) == YES);
  ASSERT_TRUE(jsget_string_span(&sub, "c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');
  ASSERT_TRUE(jsget_u32(&sub, "num", &num) == YES);
  ASSERT_TRUE(num == 77);

  ASSERT_TRUE(jsget_object(&jg, "a.n", &sub) == ERR);
  ASSERT_TRUE(jsget_object(&jg, "a.missing", &sub) == NO);
}

static void test_jsget_object_inplace(void) {
  const char *json = "{\"a\":{\"b\":{\"c\":\"z\"}},\"x\":2}";
  JsonGetter jg;
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_object(&jg, "a.b", &jg) == YES);
  ASSERT_TRUE(jsget_string_span(&jg, "c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');
}

static void test_jsget_null_and_overflow(void) {
  const char *json = "{\"a\":null,\"b\":4294967296}";
  JsonGetter jg;
  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);

  uint32_t v = 0;
  ASSERT_TRUE(jsget_u32(&jg, "a", &v) == NO);
  ASSERT_TRUE(jsget_u32(&jg, "b", &v) == ERR);
}

static void test_jsget_u32_and_bool(void) {
  const char *json = "{\"id\":7,\"ok\":true,\"err\":false}";
  JsonGetter jg;
  uint32_t id = 0;
  int ok = 0;
  int err = 10;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_u32(&jg, "id", &id) == YES);
  ASSERT_TRUE(id == 7);
  ASSERT_TRUE(jsget_bool01(&jg, "ok", &ok) == YES);
  ASSERT_TRUE(ok == 1);
  ASSERT_TRUE(jsget_bool01(&jg, "err", &err) == YES);
  ASSERT_TRUE(err == 0);
}

static void test_jsget_f64(void) {
  const char *json = "{\"pi\":3.1415,\"i\":2,\"bad\":\"x\"}";
  JsonGetter jg;
  double v = 0.0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_f64(&jg, "pi", &v) == YES);
  ASSERT_TRUE(v > 3.14 && v < 3.15);
  ASSERT_TRUE(jsget_f64(&jg, "i", &v) == YES);
  ASSERT_TRUE(v == 2.0);
  ASSERT_TRUE(jsget_f64(&jg, "bad", &v) == ERR);
  ASSERT_TRUE(jsget_f64(&jg, "missing", &v) == NO);
}

static void test_jsget_i64(void) {
  const char *json = "{\"n\":-12,\"z\":0,\"bad\":3.1}";
  JsonGetter jg;
  int64_t v = 0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_i64(&jg, "n", &v) == YES);
  ASSERT_TRUE(v == -12);
  ASSERT_TRUE(jsget_i64(&jg, "z", &v) == YES);
  ASSERT_TRUE(v == 0);
  ASSERT_TRUE(jsget_i64(&jg, "bad", &v) == ERR);
  ASSERT_TRUE(jsget_i64(&jg, "missing", &v) == NO);
}

static void test_jsget_string_span_and_decode(void) {
  const char *json = "{\"raw\":\"a\\\\n\\\"b\\\"\"}";
  JsonGetter jg;
  JsonStrSpan sp = {0};
  char *decoded = NULL;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_string_span(&jg, "raw", &sp) == YES);
  ASSERT_TRUE(sp.len == strlen("a\\\\n\\\"b\\\""));
  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "raw", &decoded) == YES);
  ASSERT_STREQ(decoded, "a\\n\"b\"");
  free(decoded);
}

static void test_jsget_array_strings(void) {
  const char *json = "{\"arr\":[\"x\",\"y\"]}";
  JsonGetter jg;
  JsonArrIter it;
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_array_strings_begin(&jg, "arr", &it) == YES);

  ASSERT_TRUE(jsget_array_strings_next(&jg, &it, &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'x');

  ASSERT_TRUE(jsget_array_strings_next(&jg, &it, &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'y');

  ASSERT_TRUE(jsget_array_strings_next(&jg, &it, &sp) == NO);
}

static void test_jsget_array_objects(void) {
  const char *json = "{\"arr\":[{\"a\":1},{\"b\":2}]}";
  JsonGetter jg;
  JsonArrIter it;
  JsonGetter obj = {0};
  uint32_t v = 0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_array_objects_begin(&jg, "arr", &it) == YES);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  ASSERT_TRUE(jsget_u32(&obj, "a", &v) == YES);
  ASSERT_TRUE(v == 1);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  ASSERT_TRUE(jsget_u32(&obj, "b", &v) == YES);
  ASSERT_TRUE(v == 2);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == NO);
}

static void test_jsget_top_level_validation(void) {
  const char *json = "{\"a\":1,\"b\":2}";
  const char *json_extra = "{\"a\":1,\"b\":2,\"c\":3}";
  JsonGetter jg;
  const char *allowed[] = {"a", "b"};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_top_level_validation(&jg, NULL, allowed, 2) == YES);

  ASSERT_TRUE(jsget_init(&jg, json_extra, strlen(json_extra)) == OK);
  ASSERT_TRUE(jsget_top_level_validation(&jg, NULL, allowed, 2) == NO);

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_top_level_validation(&jg, "missing", allowed, 2) == NO);
}

static void test_jsget_exists_nonnull(void) {
  const char *json = "{\"a\":null,\"b\":1,\"c\":\"x\"}";
  const char *json_str = "{\"a\":\"null\"}";
  JsonGetter jg;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "a") == NO);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "b") == YES);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "c") == YES);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "missing") == NO);

  ASSERT_TRUE(jsget_init(&jg, json_str, strlen(json_str)) == OK);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "a") == YES);
}

int main(void) {
  test_json_basic_rows_and_nulls();
  test_json_null_qrcolumn_safe_defaults();
  test_json_escapes_strings();
  test_json_empty_result();
  test_json_error_result();
  test_json_tool_error_result();
  test_json_string_id();
  test_json_builder_object();
  test_json_builder_array();
  test_json_builder_nested();
  test_jsget_simple_rpc_validation();
  test_jsget_paths();
  test_jsget_object_view();
  test_jsget_object_inplace();
  test_jsget_null_and_overflow();
  test_jsget_u32_and_bool();
  test_jsget_f64();
  test_jsget_i64();
  test_jsget_string_span_and_decode();
  test_jsget_array_strings();
  test_jsget_array_objects();
  test_jsget_top_level_validation();
  test_jsget_exists_nonnull();

  fprintf(stderr, "OK: test_json\n");
  return (0);
}
