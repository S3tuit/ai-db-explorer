#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "query_result.h"
#include "test.h"

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

static void test_create_and_basic_set_get(void) {
  McpId id = id_u32(7);
  QueryResult *qr = qr_create_ok(&id, 3, 2, 1, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL, NULL, 0) == OK);
  ASSERT_TRUE(qr->ncols == 3);
  ASSERT_TRUE(qr->nrows == 2);
  ASSERT_TRUE(qr->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr->id.u32 == 7);
  ASSERT_TRUE(qr->result_truncated == 1);

  ASSERT_TRUE(set_col_plain(&qb, 0, "id", "int4") == OK);
  ASSERT_TRUE(set_col_plain(&qb, 1, "name", "text") == OK);
  const QRColumn *c = qr_get_col(qr, 1);
  ASSERT_STREQ(c->name, "name");
  ASSERT_STREQ(c->type, "text");

  // unset column should be NULL
  ASSERT_TRUE(qr_get_col(qr, 2) == NULL);

  ASSERT_TRUE(set_col_plain(&qb, 2, "amount", NULL) ==
              OK); // should become "unknown"
  const QRColumn *c2 = qr_get_col(qr, 2);
  ASSERT_STREQ(c2->type, "unknown");

  // set some cells
  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "1") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "alice") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 2, "10.50") == YES);

  ASSERT_STREQ(qr_get_cell(qr, 0, 0), "1");
  ASSERT_STREQ(qr_get_cell(qr, 0, 1), "alice");

  // overwrite a cell
  ASSERT_TRUE(set_cell_plain(&qb, 0, 2, "99") == YES);
  ASSERT_STREQ(qr_get_cell(qr, 0, 2), "99");

  // default cells should be NULL (SQL NULL)
  ASSERT_TRUE(qr_is_null(qr, 1, 0) == YES);
  ASSERT_TRUE(qr_get_cell(qr, 1, 0) == NULL);

  qr_destroy(qr);
}

static void test_max_query_bytes_cap(void) {
  McpId id = id_u32(1);
  QueryResult *qr = qr_create_ok(&id, 2, 2, 0, 5);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL, NULL, 0) == OK);

  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "12345") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, NULL) == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 1, 0, "67890") == NO);

  qr_destroy(qr);
}

static void test_deep_copy_outlives_input_buffers(void) {
  McpId id = id_u32(1);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL, NULL, 0) == OK);
  ASSERT_TRUE(qr->result_truncated == 0);

  char name_buf[32];
  char type_buf[32];
  char cell_buf[32];

  strcpy(name_buf, "codice");
  strcpy(type_buf, "text");
  strcpy(cell_buf, "ABC123");

  ASSERT_TRUE(set_col_plain(&qb, 0, name_buf, type_buf) == OK);
  ASSERT_TRUE(set_col_plain(&qb, 1, "descrizione", "text") == OK);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, cell_buf) == YES);

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
  McpId id = id_u32(1);
  QueryResult *qr = qr_create_ok(&id, 2, 2, 0, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL, NULL, 0) == OK);
  ASSERT_TRUE(qr->status == QR_OK);

  // qr_set_col name cannot be NULL
  ASSERT_TRUE(set_col_plain(&qb, 0, NULL, "text") == ERR);

  // out of bounds col
  ASSERT_TRUE(set_col_plain(&qb, 99, "x", "text") == ERR);

  // out of bounds cell access
  ASSERT_TRUE(set_cell_plain(&qb, 5, 0, "x") == ERR);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 5, "x") == ERR);

  // qr_get_cell out of bounds returns NULL
  ASSERT_TRUE(qr_get_cell(qr, 5, 0) == NULL);

  // qr_is_null out of bounds returns -1
  ASSERT_TRUE(qr_is_null(qr, 5, 0) == ERR);

  // null qr returns 0
  ASSERT_TRUE(qb_set_cell(NULL, 0, 0, "x", 1) == ERR);
  ASSERT_TRUE(qb_set_col(NULL, 0, "x", "y", 0) == ERR);

  qr_destroy(qr);
}

static void test_create_error(void) {
  McpId id = id_u32(3);
  QueryResult *qr = qr_create_err(&id, QRERR_INREQ, "An error.");

  ASSERT_TRUE(qr != NULL);
  ASSERT_TRUE(qr->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr->id.u32 == 3);
  ASSERT_TRUE(qr->status == QR_ERROR);
  ASSERT_TRUE(qr->err_code == QRERR_INREQ);
  ASSERT_STREQ(qr->err_msg, "An error.");

  qr_destroy(qr);
}

static void test_create_tool_error(void) {
  McpId id = id_u32(4);
  QueryResult *qr = qr_create_tool_err(&id, "Query failed.");

  ASSERT_TRUE(qr != NULL);
  ASSERT_TRUE(qr->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr->id.u32 == 4);
  ASSERT_TRUE(qr->status == QR_TOOL_ERROR);
  ASSERT_STREQ(qr->err_msg, "Query failed.");

  qr_destroy(qr);
}

static void test_create_msg(void) {
  McpId id = id_u32(9);
  QueryResult *qr = qr_create_msg(&id, "Hello");
  ASSERT_TRUE(qr != NULL);
  ASSERT_TRUE(qr->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr->id.u32 == 9);
  ASSERT_TRUE(qr->status == QR_OK);
  ASSERT_TRUE(qr->ncols == 1);
  ASSERT_TRUE(qr->nrows == 1);
  ASSERT_STREQ(qr_get_col(qr, 0)->name, "message");
  ASSERT_STREQ(qr_get_col(qr, 0)->type, "text");
  ASSERT_STREQ(qr_get_cell(qr, 0, 0), "Hello");
  qr_destroy(qr);

  McpId id2 = id_u32(10);
  QueryResult *qr_null = qr_create_msg(&id2, NULL);
  ASSERT_TRUE(qr_null != NULL);
  ASSERT_TRUE(qr_null->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr_null->id.u32 == 10);
  ASSERT_TRUE(qr_null->status == QR_OK);
  ASSERT_TRUE(qr_null->ncols == 1);
  ASSERT_TRUE(qr_null->nrows == 1);
  ASSERT_STREQ(qr_get_cell(qr_null, 0, 0), "");
  qr_destroy(qr_null);
}

int main(void) {
  test_create_and_basic_set_get();
  test_max_query_bytes_cap();
  test_deep_copy_outlives_input_buffers();
  test_bounds_and_bad_inputs();
  test_create_error();
  test_create_tool_error();
  test_create_msg();

  fprintf(stderr, "OK: test_query_result\n");
  return 0;
}
