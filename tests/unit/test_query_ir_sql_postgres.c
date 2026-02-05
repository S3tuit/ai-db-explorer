#include <stdio.h>
#include <string.h>

#include "db_backend.h"
#include "postgres_backend.h"
#include "query_ir.h"
#include "test.h"

/* Builds a QueryIR using the Postgres backend.
 * Ownership: caller must call qir_handle_destroy() on out_h.
 * Side effects: allocates backend + arena memory.
 * Returns void; assertions abort on failure. */
static void parse_sql_postgres(const char *sql, QirQueryHandle *out_h) {
  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);
  ASSERT_TRUE(db->vt && db->vt->make_query_ir);

  ASSERT_TRUE(db->vt->make_query_ir(db, sql, out_h) == OK);
  db_destroy(db);
}

/* Asserts that identifier matches a string. */
static void assert_ident_eq(const QirIdent *id, const char *expected,
                            const char *file, int line) {
  ASSERT_TRUE_AT(id != NULL, file, line);
  ASSERT_TRUE_AT(id->name != NULL, file, line);
  if (strcmp(id->name, expected) != 0) {
    fprintf(stderr, "IDENT MISMATCH: got='%s' expected='%s'\n", id->name,
            expected);
    ASSERT_TRUE_AT(false, file, line);
  }
}
#define ASSERT_IDENT_EQ(id, expected)                                          \
  assert_ident_eq((id), (expected), __FILE__, __LINE__)

/* Extracts a touch report for a parsed query.
 * Ownership: caller must destroy the report with qir_touch_report_destroy().
 * Side effects: allocates memory for the report.
 * Returns pointer; assertions abort on failure. */
static QirTouchReport *extract_touches(const QirQueryHandle *h) {
  ASSERT_TRUE(h != NULL);
  ASSERT_TRUE(h->q != NULL);
  QirTouchReport *tr = qir_extract_touches(h->q);
  ASSERT_TRUE(tr != NULL);
  return tr;
}

/* Asserts that a touch matching the given fields exists. */
static void assert_touch_has(const QirTouchReport *tr, QirScope scope,
                             QirTouchKind kind, const char *qual,
                             const char *col, const char *file, int line) {
  ASSERT_TRUE_AT(tr != NULL, file, line);
  ASSERT_TRUE_AT(qual != NULL, file, line);
  ASSERT_TRUE_AT(col != NULL, file, line);
  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches ? tr->touches[i] : NULL;
    if (!t)
      continue;
    if (t->scope != scope)
      continue;
    if (t->kind != kind)
      continue;
    if (!t->col.qualifier.name || !t->col.column.name)
      continue;
    if (strcmp(t->col.qualifier.name, qual) != 0)
      continue;
    if (strcmp(t->col.column.name, col) != 0)
      continue;
    return;
  }
  ASSERT_TRUE_AT(false, file, line);
}
#define ASSERT_TOUCH(tr, scope, kind, qual, col)                               \
  assert_touch_has((tr), (scope), (kind), (qual), (col), __FILE__, __LINE__)

/* 1. AND + comparisons + params. */
static void test_pg_params_predicates(void) {
  const char *sql = "SELECT p.id AS pid "
                    "FROM private.people AS p "
                    "WHERE p.age >= $1 AND p.region = $2 "
                    "LIMIT 200;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->limit_value == 200);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_AND);

  const QirExpr *lhs = h.q->where->u.bin.l;
  const QirExpr *rhs = h.q->where->u.bin.r;
  ASSERT_TRUE(lhs && rhs);
  ASSERT_TRUE(lhs->kind == QIR_EXPR_GE);
  ASSERT_TRUE(lhs->u.bin.r->kind == QIR_EXPR_PARAM);
  ASSERT_TRUE(lhs->u.bin.r->u.param_index == 1);
  ASSERT_TRUE(rhs->kind == QIR_EXPR_EQ);
  ASSERT_TRUE(rhs->u.bin.r->kind == QIR_EXPR_PARAM);
  ASSERT_TRUE(rhs->u.bin.r->u.param_index == 2);

  qir_handle_destroy(&h);
}

/* 2. IN list with params. */
static void test_pg_in_list_params(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.region IN ($1, $2, $3) "
                    "LIMIT 50;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_IN);
  ASSERT_TRUE(h.q->where->u.in_.nitems == 3);
  ASSERT_TRUE(h.q->where->u.in_.items[0]->kind == QIR_EXPR_PARAM);
  ASSERT_TRUE(h.q->where->u.in_.items[0]->u.param_index == 1);
  ASSERT_TRUE(h.q->where->u.in_.items[1]->u.param_index == 2);
  ASSERT_TRUE(h.q->where->u.in_.items[2]->u.param_index == 3);

  qir_handle_destroy(&h);
}

/* 3. DISTINCT ON. */
static void test_pg_distinct_on(void) {
  const char *sql =
      "SELECT DISTINCT ON (p.region) p.region AS region, p.name AS name "
      "FROM private.people AS p "
      "ORDER BY p.region, p.name "
      "LIMIT 20;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->has_distinct == true);
  ASSERT_TRUE(h.q->n_order_by == 2);

  qir_handle_destroy(&h);
}

/* 4. Casts. */
static void test_pg_casts(void) {
  const char *sql = "SELECT p.age::text AS age_txt "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_CAST);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.cast.type.name, "text");

  qir_handle_destroy(&h);
}

/* 5. COPY should be rejected. */
static void test_pg_copy_rejected(void) {
  const char *sql = "COPY (SELECT p.name FROM private.people AS p) TO PROGRAM "
                    "'cat /etc/passwd';";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 6. DO block should be rejected. */
static void test_pg_do_rejected(void) {
  const char *sql = "DO $$ BEGIN PERFORM pg_sleep(1); END $$;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 7. SET should be rejected. */
static void test_pg_set_rejected(void) {
  const char *sql = "SET statement_timeout = 0;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 8. Recursive CTE should be rejected. */
static void test_pg_recursive_cte_rejected(void) {
  const char *sql = "WITH RECURSIVE t(n) AS ("
                    "  SELECT 1 "
                    "  UNION ALL "
                    "  SELECT n+1 FROM t WHERE n < 5"
                    ") "
                    "SELECT t.n AS n FROM t LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 9. Quoted identifiers should be normalized. */
static void test_pg_quoted_identifiers(void) {
  const char *sql = "SELECT p.\"NaMe\" AS \"outName\" "
                    "FROM \"Private\".\"People\" AS p "
                    "WHERE p.\"AgE\" > 25 "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* 10. ANY/ALL array comparisons should map to IN. */
static void test_pg_any_all_as_in(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.region = ANY($1) "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_IN);
  ASSERT_TRUE(h.q->where->u.in_.nitems == 1);
  ASSERT_TRUE(h.q->where->u.in_.items[0]->kind == QIR_EXPR_PARAM);
  ASSERT_TRUE(h.q->where->u.in_.items[0]->u.param_index == 1);

  qir_handle_destroy(&h);
}

/* 11. Row comparison should be rejected. */
static void test_pg_row_comparison_rejected(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE (p.region, p.age) = ($1, $2) "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 12. LATERAL should be rejected. */
static void test_pg_lateral_rejected(void) {
  const char *sql = "SELECT p.id AS pid, x.v AS v "
                    "FROM private.people AS p "
                    "JOIN LATERAL (SELECT p.age AS v) AS x ON true "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 13. Set-returning function in FROM should be rejected. */
static void test_pg_set_returning_rejected(void) {
  const char *sql = "SELECT x.val AS val "
                    "FROM unnest(ARRAY[1,2,3]) AS x(val) "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

/* 14. JSON operators should preserve base column touch. */
static void test_pg_json_operator_touch(void) {
  const char *sql = "SELECT p.profile->>'ssn' AS ssn "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "profile");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* 15. Cast chains. */
static void test_pg_cast_chains(void) {
  const char *sql = "SELECT (p.age::text)::varchar AS age_txt "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_CAST);
  ASSERT_TRUE(h.q->select_items[0]->value->u.cast.expr != NULL);

  qir_handle_destroy(&h);
}

/*------------ CURRENTLY NOT SUPPORTED BUT WE MAY IN THE FUTURE --------------*/

static void test_pg_interval_literal_rejected(void) {
  const char *sql = "SELECT p.id AS pid "
                    "FROM private.people AS p "
                    "WHERE p.updated_at > NOW() - INTERVAL '1 DAY';";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

static void test_pg_array_literal_rejected(void) {
  const char *sql = "SELECT p.id AS pid "
                    "FROM private.people AS p "
                    "WHERE p.id = ANY(ARRAY[1,2,3]);";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

static void test_pg_bitwise_op_rejected(void) {
  const char *sql = "SELECT p.flags AS flags "
                    "FROM private.people AS p "
                    "WHERE (p.flags & 4) = 4;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);
  qir_handle_destroy(&h);
}

int main(void) {
  test_pg_params_predicates();
  test_pg_in_list_params();
  test_pg_distinct_on();
  test_pg_casts();
  test_pg_copy_rejected();
  test_pg_do_rejected();
  test_pg_set_rejected();
  test_pg_recursive_cte_rejected();
  test_pg_quoted_identifiers();
  test_pg_any_all_as_in();
  test_pg_row_comparison_rejected();
  test_pg_lateral_rejected();
  test_pg_set_returning_rejected();
  test_pg_json_operator_touch();
  test_pg_cast_chains();
  test_pg_interval_literal_rejected();
  test_pg_array_literal_rejected();
  test_pg_bitwise_op_rejected();
  fprintf(stderr, "OK: test_query_ir_sql_postgres\n");
  return 0;
}
