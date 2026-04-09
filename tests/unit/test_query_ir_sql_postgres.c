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
  ASSERT_TRUE(db_make_query_ir(db, sql, out_h, NULL) == OK);
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

/* Asserts that expression is a qualified column reference. */
static void assert_colref_expr(const QirExpr *e, const char *qual,
                               const char *col, const char *file, int line) {
  ASSERT_TRUE_AT(e != NULL, file, line);
  ASSERT_TRUE_AT(e->kind == QIR_EXPR_COLREF, file, line);
  ASSERT_TRUE_AT(e->u.colref.qualifier.name != NULL, file, line);
  ASSERT_TRUE_AT(e->u.colref.column.name != NULL, file, line);
  ASSERT_TRUE_AT(strcmp(e->u.colref.qualifier.name, qual) == 0, file, line);
  ASSERT_TRUE_AT(strcmp(e->u.colref.column.name, col) == 0, file, line);
}
#define ASSERT_COLREF(e, qual, col)                                            \
  assert_colref_expr((e), (qual), (col), __FILE__, __LINE__)

/* Asserts that the parsed statement flags match one EXPLAIN expectation. */
static void assert_stmt_flags(const QirQuery *q, bool explain, bool analyze,
                              const char *file, int line) {
  ASSERT_TRUE_AT(q != NULL, file, line);
  ASSERT_TRUE_AT(qir_query_is_explain(q) == explain, file, line);
  ASSERT_TRUE_AT(qir_query_is_explain_analyze(q) == analyze, file, line);
  if (analyze) {
    ASSERT_TRUE_AT(qir_query_is_explain(q), file, line);
  }
}
#define ASSERT_STMT_FLAGS(q, explain, analyze)                                 \
  assert_stmt_flags((q), (explain), (analyze), __FILE__, __LINE__)

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

/* 2b. Param $0 is emitted by Postgres AST without ParamRef.number, so the
 * parser must omit the predicate from QIR; later validation rejects the query.
 */
static void test_pg_param_zero_omitted_number(void) {
  const char *sql = "SELECT p.id AS pid "
                    "FROM private.people AS p "
                    "WHERE p.region = $0 "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where == NULL);

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

/* 8. WITH RECURSIVE should parse like a regular CTE. */
static void test_pg_recursive_cte(void) {
  const char *sql = "WITH RECURSIVE t AS ("
                    "  SELECT p.id AS id "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT t.id AS id "
                    "FROM t AS t "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->name, "t");

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "id");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "t", "id");
  qir_touch_report_destroy(tr);

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

/* 11. EXPLAIN SELECT should preserve the wrapped SELECT in IR. */
static void test_pg_filter_normalized_to_case(void) {
  const char *sql =
      "SELECT SUM(p.amount) FILTER (WHERE p.kind = 'invoice') AS total, "
      "COUNT(*) FILTER (WHERE p.kind = 'invoice') AS cnt "
      "FROM payments AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nselect == 2);

  ASSERT_TRUE(h.q->select_items[0]->value != NULL);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
  const QirFuncCall *sum = &h.q->select_items[0]->value->u.funcall;
  ASSERT_IDENT_EQ(&sum->schema, "");
  ASSERT_IDENT_EQ(&sum->name, "sum");
  ASSERT_TRUE(sum->is_star == false);
  ASSERT_TRUE(sum->nargs == 1);
  ASSERT_TRUE(sum->args != NULL);
  ASSERT_TRUE(sum->args[0] != NULL);
  ASSERT_TRUE(sum->args[0]->kind == QIR_EXPR_CASE);
  ASSERT_TRUE(sum->args[0]->u.case_.arg == NULL);
  ASSERT_TRUE(sum->args[0]->u.case_.nwhens == 1);
  ASSERT_TRUE(sum->args[0]->u.case_.else_expr == NULL);
  ASSERT_TRUE(sum->args[0]->u.case_.whens != NULL);
  ASSERT_TRUE(sum->args[0]->u.case_.whens[0] != NULL);
  ASSERT_TRUE(sum->args[0]->u.case_.whens[0]->when_expr != NULL);
  ASSERT_TRUE(sum->args[0]->u.case_.whens[0]->when_expr->kind == QIR_EXPR_EQ);
  ASSERT_COLREF(sum->args[0]->u.case_.whens[0]->when_expr->u.bin.l, "p",
                "kind");
  ASSERT_TRUE(sum->args[0]->u.case_.whens[0]->when_expr->u.bin.r != NULL);
  ASSERT_TRUE(sum->args[0]->u.case_.whens[0]->when_expr->u.bin.r->kind ==
              QIR_EXPR_LITERAL);
  ASSERT_TRUE(sum->args[0]->u.case_.whens[0]->when_expr->u.bin.r->u.lit.kind ==
              QIR_LIT_STRING);
  ASSERT_TRUE(strcmp(sum->args[0]->u.case_.whens[0]->when_expr->u.bin.r->u.lit
                         .v.s,
                     "invoice") == 0);
  ASSERT_COLREF(sum->args[0]->u.case_.whens[0]->then_expr, "p", "amount");

  ASSERT_TRUE(h.q->select_items[1]->value != NULL);
  ASSERT_TRUE(h.q->select_items[1]->value->kind == QIR_EXPR_FUNCALL);
  const QirFuncCall *count = &h.q->select_items[1]->value->u.funcall;
  ASSERT_IDENT_EQ(&count->schema, "");
  ASSERT_IDENT_EQ(&count->name, "count");
  ASSERT_TRUE(count->is_star == false);
  ASSERT_TRUE(count->nargs == 1);
  ASSERT_TRUE(count->args != NULL);
  ASSERT_TRUE(count->args[0] != NULL);
  ASSERT_TRUE(count->args[0]->kind == QIR_EXPR_CASE);
  ASSERT_TRUE(count->args[0]->u.case_.arg == NULL);
  ASSERT_TRUE(count->args[0]->u.case_.nwhens == 1);
  ASSERT_TRUE(count->args[0]->u.case_.else_expr == NULL);
  ASSERT_TRUE(count->args[0]->u.case_.whens != NULL);
  ASSERT_TRUE(count->args[0]->u.case_.whens[0] != NULL);
  ASSERT_TRUE(count->args[0]->u.case_.whens[0]->then_expr != NULL);
  ASSERT_TRUE(count->args[0]->u.case_.whens[0]->then_expr->kind ==
              QIR_EXPR_LITERAL);
  ASSERT_TRUE(count->args[0]->u.case_.whens[0]->then_expr->u.lit.kind ==
              QIR_LIT_INT64);
  ASSERT_TRUE(count->args[0]->u.case_.whens[0]->then_expr->u.lit.v.i64 == 1);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "amount");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "kind");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* 11. EXPLAIN SELECT should preserve the wrapped SELECT in IR. */
static void test_pg_explain_select(void) {
  const char *sql = "EXPLAIN SELECT p.id AS id "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->kind == QIR_STMT_SELECT);
  ASSERT_STMT_FLAGS(h.q, true, false);
  ASSERT_TRUE(h.q->limit_value == 10);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* 12. EXPLAIN ANALYZE should set both EXPLAIN and ANALYZE flags. */
static void test_pg_explain_analyze_select(void) {
  const char *sql = "EXPLAIN ANALYZE SELECT p.id AS id "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->kind == QIR_STMT_SELECT);
  ASSERT_STMT_FLAGS(h.q, true, true);

  qir_handle_destroy(&h);
}

/* 13. EXPLAIN ANALYSE should map to the same ANALYZE flag. */
static void test_pg_explain_analyse_select(void) {
  const char *sql = "EXPLAIN ANALYSE SELECT p.id AS id "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->kind == QIR_STMT_SELECT);
  ASSERT_STMT_FLAGS(h.q, true, true);

  qir_handle_destroy(&h);
}

/* 14. EXPLAIN options we do not model should still keep EXPLAIN semantics. */
static void test_pg_explain_generic_plan_select(void) {
  const char *sql = "EXPLAIN (GENERIC_PLAN) SELECT p.id AS id "
                    "FROM private.people AS p "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->kind == QIR_STMT_SELECT);
  ASSERT_STMT_FLAGS(h.q, true, false);

  qir_handle_destroy(&h);
}

/* 15. EXPLAIN only supports wrapped SELECT statements. */
static void test_pg_explain_delete_rejected(void) {
  const char *sql = "EXPLAIN DELETE FROM private.people WHERE id = 1;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);

  qir_handle_destroy(&h);
}

/* 16. Row comparison should be rejected. */
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

/* 17. LATERAL should be rejected. */
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

/* 18. Set-returning function in FROM should be rejected. */
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

/* 19. JSON operators should preserve base column touch. */
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

/* 20. Cast chains. */
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

/* 21. ILIKE should normalize to LIKE-style predicates in IR. */
static void test_pg_ilike_operators(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.name ILIKE $1 OR p.region NOT ILIKE $2 "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_OR);

  const QirExpr *lhs = h.q->where->u.bin.l;
  const QirExpr *rhs = h.q->where->u.bin.r;
  ASSERT_TRUE(lhs != NULL);
  ASSERT_TRUE(rhs != NULL);
  ASSERT_TRUE(lhs->kind == QIR_EXPR_LIKE);
  ASSERT_TRUE(lhs->u.bin.r != NULL);
  ASSERT_TRUE(lhs->u.bin.r->kind == QIR_EXPR_PARAM);
  ASSERT_TRUE(lhs->u.bin.r->u.param_index == 1);
  ASSERT_TRUE(rhs->kind == QIR_EXPR_NOT_LIKE);
  ASSERT_TRUE(rhs->u.bin.r != NULL);
  ASSERT_TRUE(rhs->u.bin.r->kind == QIR_EXPR_PARAM);
  ASSERT_TRUE(rhs->u.bin.r->u.param_index == 2);

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
  test_pg_param_zero_omitted_number();
  test_pg_distinct_on();
  test_pg_casts();
  test_pg_copy_rejected();
  test_pg_do_rejected();
  test_pg_set_rejected();
  test_pg_recursive_cte();
  test_pg_quoted_identifiers();
  test_pg_any_all_as_in();
  test_pg_filter_normalized_to_case();
  test_pg_explain_select();
  test_pg_explain_analyze_select();
  test_pg_explain_analyse_select();
  test_pg_explain_generic_plan_select();
  test_pg_explain_delete_rejected();
  test_pg_row_comparison_rejected();
  test_pg_lateral_rejected();
  test_pg_set_returning_rejected();
  test_pg_json_operator_touch();
  test_pg_cast_chains();
  test_pg_ilike_operators();
  test_pg_interval_literal_rejected();
  test_pg_array_literal_rejected();
  test_pg_bitwise_op_rejected();
  fprintf(stderr, "OK: test_query_ir_sql_postgres\n");
  return 0;
}
