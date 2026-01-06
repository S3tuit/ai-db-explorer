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

static void test_command_to_jsonrpc_sql(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_SQL,
        .raw_sql = "SELECT 1;"
    };
    int rc = command_to_jsonrpc(&cmd, 1, &json, &json_len);

    ASSERT_TRUE(rc == OK);

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\",\"params\":{"
          "\"sql\":\"SELECT 1;\""
        "}}";

    assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

    free(json);
}

static void test_command_to_jsonrpc_meta(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_META,
        .cmd = "connect",
        .args = "name=\"main\""
    };
    int rc = command_to_jsonrpc(&cmd, 1, &json, &json_len);

    ASSERT_TRUE(rc == OK);

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"connect\",\"params\":{"
          "\"name\":\"main\""
        "}}";

    assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

    free(json);
}

static void test_command_to_jsonrpc_meta_no_params(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_META,
        .cmd = "status",
        .args = NULL
    };
    int rc = command_to_jsonrpc(&cmd, 2, &json, &json_len);

    ASSERT_TRUE(rc == OK);

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"status\"}";

    assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

    free(json);
}

static void test_command_to_jsonrpc_meta_parse_args(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_META,
        .cmd = "connect",
        .args = "timeout=123 timeout=7"
    };
    int rc = command_to_jsonrpc(&cmd, 3, &json, &json_len);

    ASSERT_TRUE(rc == OK);

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"connect\",\"params\":{"
          "\"timeout\":7"
        "}}";

    assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

    free(json);
}

static void test_command_to_jsonrpc_meta_no_equals(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_META,
        .cmd = "connect",
        .args = "dbname postgres"
    };
    int rc = command_to_jsonrpc(&cmd, 4, &json, &json_len);

    ASSERT_TRUE(rc == OK);

    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"connect\",\"params\":{"
          "\"dbname\":\"\",\"postgres\":\"\""
        "}}";

    assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

    free(json);
}

static void test_command_to_jsonrpc_meta_arg_cases(void) {
    struct {
        const char *args;
        const char *expected_params;
    } cases[] = {
        { "key=\"value a b", "\"key\":\"value a b\"" },
        { "a=1=b", "\"a\":\"1=b\"" },
        { "=1", "\"\":1" },
        { "overflow=123456789012345678901234567890",
          "\"overflow\":\"123456789012345678901234567890\"" },
        { "a=a a=b", "\"a\":\"b\"" },
        { "string=123a", "\"string\":\"123a\"" },
        { "timeout=123 55", "\"timeout\":123,\"55\":\"\"" },
        { "peppa=\"pig\" pig=\"peppa\"", "\"peppa\":\"pig\",\"pig\":\"peppa\"" }
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char *json = NULL;
        size_t json_len = 0;

        Command cmd = {
            .type = CMD_META,
            .cmd = "connect",
            .args = (char *)cases[i].args
        };
        int rc = command_to_jsonrpc(&cmd, 5, &json, &json_len);
        ASSERT_TRUE(rc == OK);

        char expected[512];
        int n = snprintf(expected, sizeof(expected),
            "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"connect\",\"params\":{%s}}",
            cases[i].expected_params);
        ASSERT_TRUE(n > 0);
        ASSERT_TRUE((size_t)n < sizeof(expected));

        assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);
        free(json);
    }
}

static void test_json_encode_decode_present(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_SQL,
        .raw_sql = "SELECT 1;"
    };
    int rc = command_to_jsonrpc(&cmd, 7, &json, &json_len);
    ASSERT_TRUE(rc == OK);

    char *sql = NULL;
    rc = json_get_value(json, json_len, "%s", "params.sql", &sql);
    ASSERT_TRUE(rc == YES);
    ASSERT_STREQ(sql, "SELECT 1;");

    free(sql);
    free(json);
}

static void test_json_encode_decode_missing(void) {
    char *json = NULL;
    size_t json_len = 0;

    Command cmd = {
        .type = CMD_META,
        .cmd = "status",
        .args = NULL
    };
    int rc = command_to_jsonrpc(&cmd, 1, &json, &json_len);
    ASSERT_TRUE(rc == OK);
    
    uint32_t id = 0;
    char *raw = NULL;
    rc = json_get_value(json, json_len, "%u%s", "id", &id, "params.raw", &raw);
    ASSERT_TRUE(rc == NO);

    free(json);
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
    test_command_to_jsonrpc_sql();
    test_command_to_jsonrpc_meta();
    test_command_to_jsonrpc_meta_no_params();
    test_command_to_jsonrpc_meta_parse_args();
    test_command_to_jsonrpc_meta_no_equals();
    test_command_to_jsonrpc_meta_arg_cases();
    test_json_encode_decode_present();
    test_json_encode_decode_missing();
    test_json_get_value_strings();
    test_json_get_value_null();
    test_json_get_value_u32_overflow();

    fprintf(stderr, "OK: test_json\n");
    return(0);
}
