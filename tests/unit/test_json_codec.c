#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "query_result.h"
#include "json_codec.h"
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

/* builds a QueryResult, fills cols/cells, encode into JSON, compares payload. */
static void encode_jsonrpc_impl(
        uint32_t id,
        uint32_t ncols,
        uint32_t nrows,
        uint64_t exec_ms,
        uint8_t truncated,
        const char *const *col_names,
        const char *const *col_types,   
        const char *const *cells,
        const char *expected_json,
        const char *file, int line) {

    QueryResult *qr = qr_create_ok(id, ncols, nrows, truncated);
    ASSERT_TRUE_AT(qr != NULL, file, line);

    qr->exec_ms = exec_ms;

    /* set columns (if provided) */
    for (uint32_t c = 0; c < ncols; ++c) {
        const char *nm = col_names ? col_names[c] : NULL;
        const char *tp = col_types ? col_types[c] : NULL;

        if (!nm) {
            ASSERT_TRUE_AT(tp == NULL, file, line);
            continue;
        }
        int rc = qr_set_col(qr, c, nm, tp);
        ASSERT_TRUE_AT(rc == OK, file, line);
    }

    /* set cells */
    for (uint32_t r = 0; r < nrows; ++r) {
        for (uint32_t c = 0; c < ncols; ++c) {
            size_t idx = (size_t)r * (size_t)ncols + (size_t)c;
            const char *val = cells ? cells[idx] : NULL;
            int rc = qr_set_cell(qr, r, c, val);
            ASSERT_TRUE_AT(rc == OK, file, line);
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
    const char *cells[] = {
        "1", "alice", "10.50",
        "2", NULL,    "99"
    };

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{"
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
          "\"truncated\":true"
        "}}";

    ENCODE_JSONRPC(
        /* id */ 7,
        /* ncols */ 3,
        /* nrows */ 2,
        /* exec_ms */ 12,
        /* truncated */ 1,
        col_names,
        col_types,
        cells,
        expected
    );
}

static void test_json_null_qrcolumn_safe_defaults(void) {
    /* 2 columns, but we only set column 0 */
    QueryResult *qr = qr_create_ok(100, 2, 1, 0);
    ASSERT_TRUE(qr != NULL);

    qr->exec_ms = 42;

    /* Set only column 0 */
    ASSERT_TRUE(qr_set_col(qr, 0, "id", "int4") == OK);

    /* Column 1 is completely unset */
    ASSERT_TRUE(qr_get_col(qr, 1) == NULL);

    /* Set cells anyway (json must not rely on column metadata existing) */
    ASSERT_TRUE(qr_set_cell(qr, 0, 0, "5") == OK);
    ASSERT_TRUE(qr_set_cell(qr, 0, 1, "abc") == OK);

    /* If qr_get_col returns NULL, json uses empty strings "" in output */
    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":100,\"result\":{"
          "\"exec_ms\":42,"
          "\"columns\":["
            "{\"name\":\"id\",\"type\":\"int4\"},"
            "{\"name\":\"\",\"type\":\"\"}"
          "],"
          "\"rows\":[[\"5\",\"abc\"]],"
          "\"rowcount\":1,"
          "\"truncated\":false"
        "}}";

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

    const char *cells[] = {
        raw_with_ctrl
    };

    /* Expected JSON escaping:
       "  -> \"
       \  -> \\
       \n -> \\n
       \t -> \\t
       \r -> \\r
       0x01 -> \\u0001
    */
    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{"
          "\"exec_ms\":5,"
          "\"columns\":[{\"name\":\"txt\",\"type\":\"text\"}],"
          "\"rows\":[[\"a\\\"b\\\\c\\n\\td\\r\\u0001Z\"]],"
          "\"rowcount\":1,"
          "\"truncated\":false"
        "}}";

    ENCODE_JSONRPC(
        9, 1, 1, 5, 0,
        col_names, col_types, cells,
        expected
    );
}

static void test_json_empty_result(void) {
    /* 0 cols, 0 rows */
    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{"
          "\"exec_ms\":1,"
          "\"columns\":[],"
          "\"rows\":[],"
          "\"rowcount\":0,"
          "\"truncated\":false"
        "}}";

    ENCODE_JSONRPC(
        42, 0, 0, 1, 0,
        NULL, NULL, NULL,
        expected
    );
}

static void test_json_error_result(void) {
    QueryResult *qr = qr_create_err(7, "bad \"x\"");
    ASSERT_TRUE(qr != NULL);

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":7,\"error\":{"
          "\"exec_ms\":0,"
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

static void test_json_builder_object(void) {
    StrBuf sb = {0};
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
    StrBuf sb = {0};
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
    StrBuf sb = {0};
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

static void test_json_simple_validation(void) {
    const char *ok =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\",\"params\":{}}";
    const char *no_id =
        "{\"jsonrpc\":\"2.0\",\"method\":\"exec\"}";
    const char *bad_ver =
        "{\"jsonrpc\":\"2.1\",\"id\":1,\"method\":\"exec\"}";
    const char *bad_json =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\"";

    ASSERT_TRUE(json_simple_validation(ok) == YES);
    ASSERT_TRUE(json_simple_validation(no_id) == NO);
    ASSERT_TRUE(json_simple_validation(bad_ver) == NO);
    ASSERT_TRUE(json_simple_validation(bad_json) == ERR);
}

static void test_json_get_value_strings(void) {
    const char *json = "{\"a\":\"x\",\"b\":\"hello\"}";
    char c = '\0';
    char *s = NULL;

    int rc = json_get_value(json, strlen(json), "%c%s", "a", &c, "b", &s);
    ASSERT_TRUE(rc == YES);
    ASSERT_TRUE(c == 'x');
    ASSERT_STREQ(s, "hello");

    free(s);
}

static void test_json_get_value_null(void) {
    const char *json = "{\"a\":null}";
    uint32_t v = 0;
    int rc = json_get_value(json, strlen(json), "%u", "a", &v);
    ASSERT_TRUE(rc == NO);
}

static void test_json_get_value_u32_overflow(void) {
    const char *json = "{\"a\":4294967296}";
    uint32_t v = 0;
    int rc = json_get_value(json, strlen(json), "%u", "a", &v);
    ASSERT_TRUE(rc == ERR);
}

int main (void) {
    test_json_basic_rows_and_nulls();
    test_json_null_qrcolumn_safe_defaults();
    test_json_escapes_strings();
    test_json_empty_result();
    test_json_error_result();
    test_json_builder_object();
    test_json_builder_array();
    test_json_builder_nested();
    test_json_simple_validation();
    test_json_get_value_strings();
    test_json_get_value_null();
    test_json_get_value_u32_overflow();

    fprintf(stderr, "OK: test_json\n");
    return(0);
}
