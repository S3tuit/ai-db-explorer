#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../src/query_result.h"
#include "../src/serializer.h"
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

/* builds a QueryResult, fills cols/cells, serializes, compares payload. */
static void serialize_jsonrpc_impl(
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

    QueryResult *qr = qr_create(id, ncols, nrows, truncated);
    ASSERT_TRUE_AT(qr != NULL, file, line);

    qr->exec_ms = exec_ms;

    /* set columns (if provided) */
    for (uint32_t c = 0; c < ncols; ++c) {
        const char *nm = col_names ? col_names[c] : NULL;
        const char *tp = col_types ? col_types[c] : NULL;

        /* assumes that qr_set_col returns:
           1 set, 0 if name AND type are NULL, -1 on error */
        int rc = qr_set_col(qr, c, nm, tp);
        if (nm == NULL && tp == NULL) {
            ASSERT_TRUE_AT(rc == 0, file, line);
        } else {
            ASSERT_TRUE_AT(rc == 1, file, line);
        }
    }

    /* set cells */
    for (uint32_t r = 0; r < nrows; ++r) {
        for (uint32_t c = 0; c < ncols; ++c) {
            size_t idx = (size_t)r * (size_t)ncols + (size_t)c;
            const char *val = cells ? cells[idx] : NULL;
            int rc = qr_set_cell(qr, r, c, val);
            ASSERT_TRUE_AT(rc == 1, file, line);
        }
    }

    char *json = NULL;
    size_t json_len = 0;
    int rc = serializer_qr_to_jsonrpc(qr, &json, &json_len);

    ASSERT_TRUE_AT(rc == 1, file, line);
    assert_bytes_eq(json, json_len, expected_json, file, line);

    free(json);
    qr_destroy(qr);
}

#define SERIALIZE_JSONRPC(...) serialize_jsonrpc_impl(__VA_ARGS__, __FILE__, __LINE__)


/* ------------------------------ tests ------------------------------ */

static void test_serializer_basic_rows_and_nulls(void) {
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

    SERIALIZE_JSONRPC(
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

static void test_serializer_null_qrcolumn_safe_defaults(void) {
    /* 2 columns, but we only set column 0 */
    QueryResult *qr = qr_create(100, 2, 1, 0);
    ASSERT_TRUE(qr != NULL);

    qr->exec_ms = 42;

    /* Set only column 0 */
    ASSERT_TRUE(qr_set_col(qr, 0, "id", "int4") == 1);

    /* Column 1 is completely unset */
    ASSERT_TRUE(qr_get_col(qr, 1) == NULL);

    /* Set cells anyway (serializer must not rely on column metadata existing) */
    ASSERT_TRUE(qr_set_cell(qr, 0, 0, "5") == 1);
    ASSERT_TRUE(qr_set_cell(qr, 0, 1, "abc") == 1);

    /* If qr_get_col returns NULL, serializer uses empty strings "" in output */
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
    int rc = serializer_qr_to_jsonrpc(qr, &json, &json_len);

    ASSERT_TRUE(rc == 1);
    assert_bytes_eq(json, json_len, expected, __FILE__, __LINE__);

    free(json);
    qr_destroy(qr);
}

static void test_serializer_escapes_strings(void) {
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

    SERIALIZE_JSONRPC(
        9, 1, 1, 5, 0,
        col_names, col_types, cells,
        expected
    );
}

static void test_serializer_empty_result(void) {
    /* 0 cols, 0 rows */
    const char *expected =
        "{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{"
          "\"exec_ms\":1,"
          "\"columns\":[],"
          "\"rows\":[],"
          "\"rowcount\":0,"
          "\"truncated\":false"
        "}}";

    SERIALIZE_JSONRPC(
        42, 0, 0, 1, 0,
        NULL, NULL, NULL,
        expected
    );
}

int main (void) {
    test_serializer_basic_rows_and_nulls();
    test_serializer_null_qrcolumn_safe_defaults();
    test_serializer_escapes_strings();
    test_serializer_empty_result();

    fprintf(stderr, "OK: test_serializer\n");
    return(0);
}
