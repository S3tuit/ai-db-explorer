#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "test.h"
#include "query_result.h"

static void test_create_and_basic_set_get(void) {
    QueryResult *qr = qr_create_ok(7, 3, 2, 1);
    ASSERT_TRUE(qr != NULL);
    ASSERT_TRUE(qr->ncols == 3);
    ASSERT_TRUE(qr->nrows == 2);
    ASSERT_TRUE(qr->id == 7);
    ASSERT_TRUE(qr->truncated == 1);

    ASSERT_TRUE(qr_set_col(qr, 0, "id", "int4") == 1);
    ASSERT_TRUE(qr_set_col(qr, 1, "name", "text") == 1);
    const QRColumn *c = qr_get_col(qr, 1);
    ASSERT_STREQ(c->name, "name");
    ASSERT_STREQ(c->type, "text");

    // unset column should be NULL
    ASSERT_TRUE(qr_get_col(qr, 2) == NULL);

    ASSERT_TRUE(qr_set_col(qr, 2, "amount", NULL) == 1); // should become "unknown"
    const QRColumn *c2 = qr_get_col(qr, 2);
    ASSERT_STREQ(c2->type, "unknown");

    // set some cells
    ASSERT_TRUE(qr_set_cell(qr, 0, 0, "1") == 1);
    ASSERT_TRUE(qr_set_cell(qr, 0, 1, "alice") == 1);
    ASSERT_TRUE(qr_set_cell(qr, 0, 2, "10.50") == 1);

    ASSERT_STREQ(qr_get_cell(qr, 0, 0), "1");
    ASSERT_STREQ(qr_get_cell(qr, 0, 1), "alice");
    
    // overwrite a cell
    ASSERT_TRUE(qr_set_cell(qr, 0, 2, "99") == 1);
    ASSERT_STREQ(qr_get_cell(qr, 0, 2), "99");

    // default cells should be NULL (SQL NULL)
    ASSERT_TRUE(qr_is_null(qr, 1, 0) == 1);
    ASSERT_TRUE(qr_get_cell(qr, 1, 0) == NULL);

    qr_destroy(qr);
}

static void test_set_cell_capped_respects_cap(void) {
    QueryResult *qr = qr_create_ok(1, 2, 1, 0);
    ASSERT_TRUE(qr != NULL);

    const char *str = "this should overflow";

    ASSERT_TRUE(qr_set_cell_capped(qr, 0, 0, str, 10) == 1);
    ASSERT_STREQ(qr_get_cell(qr, 0, 0), "this s...");

    // cap lower than 4 -> dupn_or_null returns NULL, qr stores NULL
    ASSERT_TRUE(qr_set_cell_capped(qr, 0, 0, str, 3) == 1);
    ASSERT_TRUE(qr_get_cell(qr, 0, 0) == NULL);

    qr_destroy(qr);
}

static void test_deep_copy_outlives_input_buffers(void) {
    QueryResult *qr = qr_create_ok(1, 2, 1, 0);
    ASSERT_TRUE(qr != NULL);
    ASSERT_TRUE(qr->truncated == 0);

    char name_buf[32];
    char type_buf[32];
    char cell_buf[32];

    strcpy(name_buf, "codice");
    strcpy(type_buf, "text");
    strcpy(cell_buf, "ABC123");

    ASSERT_TRUE(qr_set_col(qr, 0, name_buf, type_buf) == 1);
    ASSERT_TRUE(qr_set_col(qr, 1, "descrizione", "text") == 1);
    ASSERT_TRUE(qr_set_cell(qr, 0, 0, cell_buf) == 1);

    // mutate the original buffers after setting
    strcpy(name_buf, "XXXXXX");
    strcpy(type_buf, "YYYYYY");
    strcpy(cell_buf, "ZZZZZZ");

    // stored copies must remain intact
    ASSERT_STREQ(qr->cols[0].name, "codice");
    ASSERT_STREQ(qr->cols[0].type, "text");
    ASSERT_STREQ(qr_get_cell(qr, 0, 0), "ABC123");

    qr_destroy(qr);
}

static void test_bounds_and_bad_inputs(void) {
    QueryResult *qr = qr_create_ok(1, 2, 2, 0);
    ASSERT_TRUE(qr != NULL);
    ASSERT_TRUE(qr->status == QR_OK);

    // qr_set_col name cannot be NULL
    ASSERT_TRUE(qr_set_col(qr, 0, NULL, "text") == -1);
    
    // out of bounds col
    ASSERT_TRUE(qr_set_col(qr, 99, "x", "text") == -1);

    // out of bounds cell access
    ASSERT_TRUE(qr_set_cell(qr, 5, 0, "x") == -1);
    ASSERT_TRUE(qr_set_cell(qr, 0, 5, "x") == -1);

    // qr_get_cell out of bounds returns NULL
    ASSERT_TRUE(qr_get_cell(qr, 5, 0) == NULL);

    // qr_is_null out of bounds returns -1
    ASSERT_TRUE(qr_is_null(qr, 5, 0) == -1);

    // null qr returns 0
    ASSERT_TRUE(qr_set_cell(NULL, 0, 0, "x") == 0);
    ASSERT_TRUE(qr_set_col(NULL, 0, "x", "y") == 0);

    qr_destroy(qr);
}

static void test_create_error(void) {
    QueryResult *qr = qr_create_err(3, "An error.");

    ASSERT_TRUE(qr != NULL);
    ASSERT_TRUE(qr->id == 3);
    ASSERT_TRUE(qr->status == QR_ERROR);
    ASSERT_STREQ(qr->err_msg, "An error.");

    qr_destroy(qr);
}

int main(void) {
    test_create_and_basic_set_get();
    test_set_cell_capped_respects_cap();
    test_deep_copy_outlives_input_buffers();
    test_bounds_and_bad_inputs();
    test_create_error();

    fprintf(stderr, "OK: test_query_result\n");
    return 0;
}

