#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "query_result.h"
#include "sensitive_tok.h"
#include "test.h"

static McpId id_u32(uint32_t v) {
  McpId id = {0};
  mcp_id_init_u32(&id, v);
  return id;
}

static int set_col_plain(QueryResultBuilder *qb, uint32_t col, const char *name,
                         const char *type) {
  return qb_set_col(qb, col, name, type, 0);
}

static int set_cell_plain(QueryResultBuilder *qb, uint32_t row, uint32_t col,
                          const char *value) {
  return qb_set_cell(qb, row, col, value, value ? strlen(value) : 0u);
}

/* Creates one deterministic token store backed by the given arena.
 * It borrows 'arena' and returns a heap-owned store; caller must destroy it.
 */
static DbTokenStore *create_det_store(PlArena *arena, const char *conn_name) {
  ConnProfile cp = make_profile(conn_name, SAFETY_COLSTRAT_DETERMINISTIC);
  return stok_store_create(&cp, arena);
}

static void test_create_and_basic_set_get(void) {
  McpId id = id_u32(7);
  QueryResult *qr = qr_create_ok(&id, 3, 2, 1, 0);
  ASSERT_TRUE(qr != NULL);
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);
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
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);

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
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);
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
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);
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

static void test_qb_init_input_validation(void) {
  McpId id = id_u32(11);
  QueryResult *qr = qr_create_ok(&id, 1, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(NULL, qr, NULL) == ERR);
  ASSERT_TRUE(qb_init(&qb, NULL, NULL) == ERR);

  qr_destroy(qr);
}

static void test_qb_init_policy_and_reset(void) {
  McpId id = id_u32(12);
  QueryResult *qr = qr_create_ok(&id, 1, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuilder qb = {0};
  QueryResultBuildPolicy policy = {
      .plan = (const ValidatorPlan *)(uintptr_t)0x1,
      .store = (DbTokenStore *)(uintptr_t)0x2,
      .generation = 42,
  };
  ASSERT_TRUE(qb_init(&qb, qr, &policy) == OK);
  ASSERT_TRUE(qb.qr == qr);
  ASSERT_TRUE(qb.plan == policy.plan);
  ASSERT_TRUE(qb.store == policy.store);
  ASSERT_TRUE(qb.generation == policy.generation);

  // Re-init with NULL policy must clear any previous policy state.
  ASSERT_TRUE(qb_init(&qb, qr, NULL) == OK);
  ASSERT_TRUE(qb.qr == qr);
  ASSERT_TRUE(qb.plan == NULL);
  ASSERT_TRUE(qb.store == NULL);
  ASSERT_TRUE(qb.generation == 0);

  qr_destroy(qr);
}

static void test_qr_set_id_replaces_previous(void) {
  QueryResult *qr = qr_create_ok(NULL, 1, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  McpId int_id = id_u32(99);
  ASSERT_TRUE(qr_set_id(qr, &int_id) == OK);
  ASSERT_TRUE(qr->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr->id.u32 == 99);

  McpId str_id = {0};
  ASSERT_TRUE(mcp_id_init_str_copy(&str_id, "req-abc") == OK);
  ASSERT_TRUE(qr_set_id(qr, &str_id) == OK);
  ASSERT_TRUE(qr->id.kind == MCP_ID_STR);
  ASSERT_STREQ(qr->id.str, "req-abc");

  // Set back to int; previous string id must be released safely.
  McpId int_id2 = id_u32(7);
  ASSERT_TRUE(qr_set_id(qr, &int_id2) == OK);
  ASSERT_TRUE(qr->id.kind == MCP_ID_INT);
  ASSERT_TRUE(qr->id.u32 == 7);

  ASSERT_TRUE(qr_set_id(NULL, &int_id2) == ERR);
  ASSERT_TRUE(qr_set_id(qr, NULL) == ERR);

  mcp_id_clean(&str_id);
  qr_destroy(qr);
}

/* Verifies that when the validator plan marks all output columns as plaintext,
 * QueryResultBuilder stores plaintext values and does not require a token store.
 */
static void test_qb_plan_plaintext_only_no_tokenization(void) {
  char sql[] = "SELECT u.name, u.age FROM users u WHERE u.id = 1;";
  ValidateQueryOut out = {0};
  ASSERT_TRUE(get_validate_query_out(&out, sql) == OK);
  ASSERT_TRUE(out.plan.cols != NULL);
  ASSERT_TRUE(parr_len(out.plan.cols) == 2);

  McpId id = id_u32(20);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuildPolicy policy = {
      .plan = &out.plan,
      .store = NULL, // plaintext-only plan must not depend on token store
      .generation = 1,
  };
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, &policy) == OK);

  ASSERT_TRUE(qb_set_col(&qb, 0, "name", "text", 25u) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 1, "age", "int4", 23u) == OK);
  ASSERT_TRUE(qr_get_col(qr, 0)->value_type == QRCOL_V_PLAINTEXT);
  ASSERT_TRUE(qr_get_col(qr, 1)->value_type == QRCOL_V_PLAINTEXT);

  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "alice") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "31") == YES);
  ASSERT_STREQ(qr_get_cell(qr, 0, 0), "alice");
  ASSERT_STREQ(qr_get_cell(qr, 0, 1), "31");

  qr_destroy(qr);
  vq_out_clean(&out);
}

/* Verifies that sensitive output columns are tokenized, while plaintext output
 * columns stay untouched, and the stored token maps back to the source value.
 */
static void test_qb_tokenizes_sensitive_column_and_store_roundtrip(void) {
  char sql[] =
      "SELECT u.name, u.fiscal_code FROM users u WHERE u.id = 1 LIMIT 10;";
  ValidateQueryOut out = {0};
  ASSERT_TRUE(get_validate_query_out(&out, sql) == OK);
  ASSERT_TRUE(out.plan.cols != NULL);
  ASSERT_TRUE(parr_len(out.plan.cols) == 2);

  const ValidatorColPlan *vcol1 =
      (const ValidatorColPlan *)parr_cat(out.plan.cols, 1);
  ASSERT_TRUE(vcol1 != NULL);
  ASSERT_TRUE(vcol1->kind == VCOL_OUT_TOKEN);
  ASSERT_TRUE(vcol1->col_id != NULL);
  ASSERT_TRUE(vcol1->col_id_len > 0);

  PlArena arena = {0};
  ASSERT_TRUE(pl_arena_init(&arena, NULL, NULL) == OK);
  DbTokenStore *store = create_det_store(&arena, "pgmain");
  ASSERT_TRUE(store != NULL);

  McpId id = id_u32(21);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuildPolicy policy = {
      .plan = &out.plan,
      .store = store,
      .generation = 42u,
  };
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, &policy) == OK);

  ASSERT_TRUE(qb_set_col(&qb, 0, "name", "text", 25u) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 1, "fiscal_code", "text", 25u) == OK);
  ASSERT_TRUE(qr_get_col(qr, 0)->value_type == QRCOL_V_PLAINTEXT);
  ASSERT_TRUE(qr_get_col(qr, 1)->value_type == QRCOL_V_TOKEN);

  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "alice") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "RSSMRA80A01H501U") == YES);
  ASSERT_STREQ(qr_get_cell(qr, 0, 0), "alice");
  ASSERT_TRUE(strcmp(qr_get_cell(qr, 0, 1), "RSSMRA80A01H501U") != 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  const char *tok = qr_get_cell(qr, 0, 1);
  ASSERT_TRUE(tok != NULL);
  ASSERT_TRUE(strncmp(tok, SENSITIVE_TOK_PREFIX, strlen(SENSITIVE_TOK_PREFIX)) ==
              0);

  char tok_copy[SENSITIVE_TOK_BUFSZ] = {0};
  size_t tok_len = strlen(tok);
  ASSERT_TRUE(tok_len < sizeof(tok_copy));
  memcpy(tok_copy, tok, tok_len + 1u);

  ParsedTokView v = {0};
  ASSERT_TRUE(stok_parse_view_inplace(tok_copy, &v) == OK);
  ASSERT_STREQ(v.connection_name, "pgmain");
  ASSERT_TRUE(v.generation == 42u);

  const SensitiveTok *st = stok_store_get(store, v.index);
  ASSERT_TRUE(st != NULL);
  ASSERT_TRUE(st->value != NULL);
  ASSERT_TRUE(st->value_len == strlen("RSSMRA80A01H501U"));
  ASSERT_TRUE(memcmp(st->value, "RSSMRA80A01H501U", st->value_len) == 0);
  ASSERT_TRUE(st->col_ref != NULL);
  ASSERT_TRUE(st->col_ref_len == vcol1->col_id_len);
  ASSERT_TRUE(memcmp(st->col_ref, vcol1->col_id, st->col_ref_len) == 0);
  ASSERT_TRUE(st->pg_oid == 25u);

  qr_destroy(qr);
  stok_store_destroy(store);
  pl_arena_clean(&arena);
  vq_out_clean(&out);
}

/* Verifies that SQL NULL on sensitive columns remains NULL and does not create
 * any token-store entry.
 */
static void test_qb_sensitive_null_remains_null(void) {
  char sql[] =
      "SELECT u.name, u.fiscal_code FROM users u WHERE u.id = 1 LIMIT 10;";
  ValidateQueryOut out = {0};
  ASSERT_TRUE(get_validate_query_out(&out, sql) == OK);

  PlArena arena = {0};
  ASSERT_TRUE(pl_arena_init(&arena, NULL, NULL) == OK);
  DbTokenStore *store = create_det_store(&arena, "pgmain");
  ASSERT_TRUE(store != NULL);

  McpId id = id_u32(22);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuildPolicy policy = {
      .plan = &out.plan,
      .store = store,
      .generation = 1u,
  };
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, &policy) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 0, "name", "text", 25u) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 1, "fiscal_code", "text", 25u) == OK);

  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "alice") == YES);
  ASSERT_TRUE(qb_set_cell(&qb, 0, 1, NULL, 0u) == YES);
  ASSERT_TRUE(qr_is_null(qr, 0, 1) == YES);
  ASSERT_TRUE(qr_get_cell(qr, 0, 1) == NULL);
  ASSERT_TRUE(stok_store_len(store) == 0);

  qr_destroy(qr);
  stok_store_destroy(store);
  pl_arena_clean(&arena);
  vq_out_clean(&out);
}

/* Verifies sensitive-token columns fail closed when token store is missing. */
static void test_qb_sensitive_col_missing_store_returns_err(void) {
  char sql[] =
      "SELECT u.name, u.fiscal_code FROM users u WHERE u.id = 1 LIMIT 10;";
  ValidateQueryOut out = {0};
  ASSERT_TRUE(get_validate_query_out(&out, sql) == OK);

  McpId id = id_u32(23);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuildPolicy policy = {
      .plan = &out.plan,
      .store = NULL,
      .generation = 1u,
  };
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, &policy) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 0, "name", "text", 25u) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 1, "fiscal_code", "text", 25u) == OK);

  ASSERT_TRUE(set_cell_plain(&qb, 0, 0, "alice") == YES);
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "RSSMRA80A01H501U") == ERR);
  ASSERT_TRUE(qr_get_cell(qr, 0, 1) == NULL);

  qr_destroy(qr);
  vq_out_clean(&out);
}

/* Verifies sensitive-token columns fail closed when plan metadata is missing. */
static void test_qb_sensitive_col_missing_col_id_returns_err(void) {
  char sql[] =
      "SELECT u.name, u.fiscal_code FROM users u WHERE u.id = 1 LIMIT 10;";
  ValidateQueryOut out = {0};
  ASSERT_TRUE(get_validate_query_out(&out, sql) == OK);
  ASSERT_TRUE(out.plan.cols != NULL);

  ValidatorColPlan *vcol1 = (ValidatorColPlan *)parr_cat(out.plan.cols, 1);
  ASSERT_TRUE(vcol1 != NULL);
  ASSERT_TRUE(vcol1->kind == VCOL_OUT_TOKEN);

  PlArena arena = {0};
  ASSERT_TRUE(pl_arena_init(&arena, NULL, NULL) == OK);
  DbTokenStore *store = create_det_store(&arena, "pgmain");
  ASSERT_TRUE(store != NULL);

  McpId id = id_u32(24);
  QueryResult *qr = qr_create_ok(&id, 2, 1, 0, 0);
  ASSERT_TRUE(qr != NULL);

  QueryResultBuildPolicy policy = {
      .plan = &out.plan,
      .store = store,
      .generation = 1u,
  };
  QueryResultBuilder qb = {0};
  ASSERT_TRUE(qb_init(&qb, qr, &policy) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 0, "name", "text", 25u) == OK);
  ASSERT_TRUE(qb_set_col(&qb, 1, "fiscal_code", "text", 25u) == OK);

  vcol1->col_id = NULL;
  vcol1->col_id_len = 0;
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "RSSMRA80A01H501U") == ERR);

  vcol1->col_id = "users.fiscal_code";
  vcol1->col_id_len = 0;
  ASSERT_TRUE(set_cell_plain(&qb, 0, 1, "RSSMRA80A01H501U") == ERR);

  qr_destroy(qr);
  stok_store_destroy(store);
  pl_arena_clean(&arena);
  vq_out_clean(&out);
}

int main(void) {
  test_create_and_basic_set_get();
  test_max_query_bytes_cap();
  test_deep_copy_outlives_input_buffers();
  test_bounds_and_bad_inputs();
  test_create_error();
  test_create_tool_error();
  test_create_msg();
  test_qb_init_input_validation();
  test_qb_init_policy_and_reset();
  test_qr_set_id_replaces_previous();
  test_qb_plan_plaintext_only_no_tokenization();
  test_qb_tokenizes_sensitive_column_and_store_roundtrip();
  test_qb_sensitive_null_remains_null();
  test_qb_sensitive_col_missing_store_returns_err();
  test_qb_sensitive_col_missing_col_id_returns_err();

  fprintf(stderr, "OK: test_query_result\n");
  return 0;
}
