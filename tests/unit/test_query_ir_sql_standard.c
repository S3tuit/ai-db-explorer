#include <stdio.h>
#include <string.h>

#include "db_backend.h"
#include "postgres_backend.h"
#include "query_ir.h"
#include "test.h"

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

/* Prints all touches to stderr for debugging failed expectations. */
static void dump_touches(const QirTouchReport *tr) {
  if (!tr)
    return;
  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches ? tr->touches[i] : NULL;
    if (!t)
      continue;
    fprintf(stderr, "TOUCH[%u] scope=%d kind=%d qual=%s col=%s\n", i,
            (int)t->scope, (int)t->kind,
            t->col.qualifier.name ? t->col.qualifier.name : "(null)",
            t->col.column.name ? t->col.column.name : "(null)");
  }
}

/* Asserts no unknown touches; dumps all touches on failure. */
static void assert_no_unknown_touches(const QirTouchReport *tr,
                                      const char *file, int line) {
  ASSERT_TRUE_AT(tr != NULL, file, line);
  if (tr->has_unknown_touches) {
    dump_touches(tr);
    ASSERT_TRUE_AT(false, file, line);
  }
}
#define ASSERT_NO_UNKNOWN_TOUCHES(tr)                                          \
  assert_no_unknown_touches((tr), __FILE__, __LINE__)

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

/* Asserts that expression is a column reference and matches 'qual' and 'col'.
 */
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

/* A1. Multiple predicates AND + comparisons + literals. */
static void test_sql_standard_predicates_and_limit(void) {
  const char *sql = "SELECT p.id AS pid "
                    "FROM private.people AS p "
                    "WHERE p.age >= 25 AND p.region = 'c' "
                    "LIMIT 200;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->kind == QIR_STMT_SELECT);
  ASSERT_TRUE(h.q->has_star == false);
  ASSERT_TRUE(h.q->limit_value == 200);

  // SELECT list
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_TRUE(h.q->select_items != NULL);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "pid");
  ASSERT_TRUE(h.q->select_items[0]->value != NULL);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_COLREF);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.colref.qualifier, "p");
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.colref.column, "id");

  // FROM
  ASSERT_TRUE(h.q->nfrom == 1);
  ASSERT_TRUE(h.q->from_items != NULL);
  ASSERT_TRUE(h.q->from_items[0]->kind == QIR_FROM_BASE_REL);
  ASSERT_IDENT_EQ(&h.q->from_items[0]->alias, "p");
  ASSERT_IDENT_EQ(&h.q->from_items[0]->u.rel.schema, "private");
  ASSERT_IDENT_EQ(&h.q->from_items[0]->u.rel.name, "people");

  // WHERE: p.age >= 25 AND p.region = 'c'
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_AND);

  const QirExpr *lhs = h.q->where->u.bin.l;
  const QirExpr *rhs = h.q->where->u.bin.r;
  ASSERT_TRUE(lhs && rhs);

  ASSERT_TRUE(lhs->kind == QIR_EXPR_GE);
  ASSERT_COLREF(lhs->u.bin.l, "p", "age");
  ASSERT_TRUE(lhs->u.bin.r && lhs->u.bin.r->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(lhs->u.bin.r->u.lit.kind == QIR_LIT_INT64);
  ASSERT_TRUE(lhs->u.bin.r->u.lit.v.i64 == 25);

  ASSERT_TRUE(rhs->kind == QIR_EXPR_EQ);
  ASSERT_COLREF(rhs->u.bin.l, "p", "region");
  ASSERT_TRUE(rhs->u.bin.r && rhs->u.bin.r->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(rhs->u.bin.r->u.lit.kind == QIR_LIT_STRING);
  ASSERT_TRUE(strcmp(rhs->u.bin.r->u.lit.v.s, "c") == 0);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_NO_UNKNOWN_TOUCHES(tr);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A2. IN list with literals. */
static void test_sql_standard_in_list(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.region IN ('a', 'b', 'c');";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_IN);
  ASSERT_COLREF(h.q->where->u.in_.lhs, "p", "region");
  ASSERT_TRUE(h.q->where->u.in_.nitems == 3);

  const QirExpr *i0 = h.q->where->u.in_.items[0];
  const QirExpr *i1 = h.q->where->u.in_.items[1];
  const QirExpr *i2 = h.q->where->u.in_.items[2];
  ASSERT_TRUE(i0 && i1 && i2);
  ASSERT_TRUE(i0->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(i1->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(i2->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(i0->u.lit.kind == QIR_LIT_STRING);
  ASSERT_TRUE(i1->u.lit.kind == QIR_LIT_STRING);
  ASSERT_TRUE(i2->u.lit.kind == QIR_LIT_STRING);
  ASSERT_TRUE(strcmp(i0->u.lit.v.s, "a") == 0);
  ASSERT_TRUE(strcmp(i1->u.lit.v.s, "b") == 0);
  ASSERT_TRUE(strcmp(i2->u.lit.v.s, "c") == 0);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_NO_UNKNOWN_TOUCHES(tr);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A3. OR at top level. */
static void test_sql_standard_or(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.region = 'a' OR p.status = false;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_OR);
  ASSERT_TRUE(h.q->where->u.bin.l->u.bin.r->u.lit.kind = QIR_LIT_BOOL);

  const QirExpr *lhs = h.q->where->u.bin.l;
  const QirExpr *rhs = h.q->where->u.bin.r;
  ASSERT_TRUE(lhs && rhs);
  ASSERT_TRUE(lhs->kind == QIR_EXPR_EQ);
  ASSERT_TRUE(rhs->kind == QIR_EXPR_EQ);

  qir_handle_destroy(&h);
}

/* A4. NOT predicate. */
static void test_sql_standard_not(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE NOT (p.disabled = true);";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_NOT);
  ASSERT_TRUE(h.q->where->u.bin.l != NULL);
  ASSERT_TRUE(h.q->where->u.bin.r == NULL);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "disabled");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A5. ORDER BY with qualified column. */
static void test_sql_standard_order_by(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "ORDER BY p.name;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->n_order_by == 1);
  ASSERT_COLREF(h.q->order_by[0], "p", "name");

  qir_handle_destroy(&h);
}

/* A6. ORDER BY resolves SELECT alias. */
static void test_sql_standard_order_by_alias(void) {
  const char *sql = "WITH cte_people AS ("
                    "  SELECT p.name AS nm "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT p.nm AS a_name "
                    "FROM cte_people AS p "
                    "ORDER BY a_name;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->n_order_by == 1);
  ASSERT_COLREF(h.q->order_by[0], "p", "nm");

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "p", "nm");
  ASSERT_TOUCH(tr, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "name");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A7. DISTINCT. */
static void test_sql_standard_distinct(void) {
  const char *sql = "SELECT DISTINCT p.region AS region "
                    "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->has_distinct == true);

  qir_handle_destroy(&h);
}

/* A8. Function calls in SELECT and WHERE. */
static void test_sql_standard_func_call(void) {
  const char *sql = "SELECT lower(p.email) AS email_lc "
                    "FROM private.people AS p "
                    "WHERE upper(p.region) = upper($1);";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.funcall.schema, "");
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.funcall.name, "lower");

  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_EQ);
  ASSERT_TRUE(h.q->where->u.bin.l->kind == QIR_EXPR_FUNCALL);
  ASSERT_TRUE(h.q->where->u.bin.r->kind == QIR_EXPR_FUNCALL);

  qir_handle_destroy(&h);
}

/* A9. INNER JOIN + ON predicate. */
static void test_sql_standard_join_inner(void) {
  const char *sql =
      "SELECT p.name AS person_name, f.friend_name AS friend_name "
      "FROM private.people AS p "
      "JOIN friends AS f ON f.person_id = p.id "
      "LIMIT 50;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->njoins == 1);
  ASSERT_TRUE(h.q->joins[0]->kind == QIR_JOIN_INNER);
  ASSERT_TRUE(h.q->joins[0]->on != NULL);
  ASSERT_TRUE(h.q->joins[0]->rhs != NULL);
  ASSERT_IDENT_EQ(&h.q->joins[0]->rhs->alias, "f");

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_NO_UNKNOWN_TOUCHES(tr);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "f", "friend_name");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "f", "person_id");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A10. CROSS JOIN. */
static void test_sql_standard_join_cross(void) {
  const char *sql = "SELECT p.name AS n, r.code AS c "
                    "FROM private.people AS p "
                    "CROSS JOIN audit.regions AS r;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->njoins == 1);
  ASSERT_TRUE(h.q->joins[0]->kind == QIR_JOIN_CROSS);
  ASSERT_TRUE(h.q->joins[0]->on == NULL);

  qir_handle_destroy(&h);
}

/* A11. OFFSET. */
static void test_sql_standard_offset(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "ORDER BY p.id "
                    "LIMIT 10 OFFSET 20;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->has_offset == true);

  qir_handle_destroy(&h);
}

/* A12. Semicolon inside string literal. */
static void test_sql_standard_semicolon_literal(void) {
  const char *sql = "SELECT 'a; b; c' AS txt "
                    "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->ntouches == 0);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A13. IN with empty list (parse error). */
static void test_sql_standard_in_empty_list(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.region IN ();";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

  qir_handle_destroy(&h);
}

/* A14. COUNT(*) */
static void test_sql_standard_count_star(void) {
  const char *sql = "SELECT COUNT(*) "
                    "FROM people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
  ASSERT_TRUE(h.q->select_items[0]->value->u.funcall.is_star == true);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->ntouches == 0);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* A15. Literal-only SELECT list. */
static void test_sql_standard_literal_select(void) {
  const char *sql = "SELECT 'a' AS a_string, 3 AS a_num;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "a_string");
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(h.q->select_items[0]->value->u.lit.kind == QIR_LIT_STRING);
  ASSERT_TRUE(strcmp(h.q->select_items[0]->value->u.lit.v.s, "a") == 0);
  ASSERT_IDENT_EQ(&h.q->select_items[1]->out_alias, "a_num");
  ASSERT_TRUE(h.q->select_items[1]->value->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(h.q->select_items[1]->value->u.lit.kind == QIR_LIT_INT64);
  ASSERT_TRUE(h.q->select_items[1]->value->u.lit.v.i64 == 3);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->ntouches == 0);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* B1. Multiple CTEs. */
static void test_sql_standard_ctes(void) {
  const char *sql = "WITH people AS ("
                    "  SELECT h.name AS name, "
                    "         h.gender AS gender "
                    "  FROM private.humans h"
                    "), "
                    "male AS ("
                    "  SELECT pm.name AS name "
                    "  FROM people AS pm "
                    "  WHERE pm.gender = 'M'"
                    "), "
                    "female AS ("
                    "  SELECT pf.name AS name "
                    "  FROM people AS pf "
                    "  WHERE pf.gender = 'F'"
                    ") "
                    "SELECT m.name AS name "
                    "FROM male AS m;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 3);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->name, "people");
  ASSERT_IDENT_EQ(&h.q->ctes[1]->name, "male");
  ASSERT_IDENT_EQ(&h.q->ctes[2]->name, "female");

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_NO_UNKNOWN_TOUCHES(tr);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_NESTED, QIR_TOUCH_DERIVED, "pm", "gender");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "m", "name");
  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches[i];
    if (t->scope == QIR_SCOPE_NESTED) {
      ASSERT_TRUE(t->source_query != h.q);
      break;
    }
  }
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* B2. CTE with sensitive column should still parse and be touchable. */
static void test_sql_standard_cte_sensitive_col(void) {
  const char *sql = "WITH tab1 AS ("
                    "  SELECT u.fiscal_code AS fiscal_code "
                    "  FROM users u "
                    "  WHERE u.id = 1"
                    ") "
                    "SELECT t.fiscal_code AS fiscal_code "
                    "FROM tab1 AS t "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "u", "fiscal_code");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "t", "fiscal_code");
  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches[i];
    if (t->scope == QIR_SCOPE_MAIN && t->col.qualifier.name &&
        strcmp(t->col.qualifier.name, "t") == 0) {
      ASSERT_TRUE(t->source_query == h.q);
      break;
    }
  }
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* B3. Subquery in FROM. */
static void test_sql_standard_subquery_from(void) {
  const char *sql = "SELECT x.name AS name "
                    "FROM ("
                    "  SELECT p.name AS name "
                    "  FROM private.people AS p "
                    "  WHERE p.age > 25"
                    ") AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nfrom == 1);
  ASSERT_TRUE(h.q->from_items[0]->kind == QIR_FROM_SUBQUERY);
  ASSERT_IDENT_EQ(&h.q->from_items[0]->alias, "x");

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "x", "name");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* B4. Scalar subquery in WHERE. */
static void test_sql_standard_subquery_where(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.id = ("
                    "  SELECT f.person_id "
                    "  FROM friends AS f "
                    "  WHERE f.friend_name = $1 "
                    "  LIMIT 1"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_EQ);
  ASSERT_TRUE(h.q->where->u.bin.r != NULL);
  ASSERT_TRUE(h.q->where->u.bin.r->kind == QIR_EXPR_SUBQUERY);

  qir_handle_destroy(&h);
}

/* B5. EXISTS subquery. */
static void test_sql_standard_exists(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE EXISTS ("
                    "  SELECT 1 "
                    "  FROM orders AS o "
                    "  WHERE o.user_id = p.id AND o.total > 100"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_SUBQUERY);

  qir_handle_destroy(&h);
}

/* B6. IN (SELECT ...) */
static void test_sql_standard_in_subquery(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.id IN ("
                    "  SELECT o.user_id "
                    "  FROM orders AS o "
                    "  WHERE o.total > 10"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_IN);
  ASSERT_TRUE(h.q->where->u.in_.nitems == 1);
  ASSERT_TRUE(h.q->where->u.in_.items[0] != NULL);
  ASSERT_TRUE(h.q->where->u.in_.items[0]->kind == QIR_EXPR_SUBQUERY);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  ASSERT_TOUCH(tr, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "o", "user_id");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* C1. Star should be detected inside CTE body. */
static void test_sql_standard_star_in_cte(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT p.* "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT x.name AS name "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_TRUE(h.q->ctes[0]->query != NULL);
  ASSERT_TRUE(h.q->ctes[0]->query->has_star == true);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == true);
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* D1. Multiple statements. */
static void test_sql_standard_multi_stmt_rejected(void) {
  const char *sql = "SELECT p.name AS name FROM private.people AS p; "
                    "SELECT pg_sleep(5);";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unsupported == true);
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* D2. Comment-trick multiple statements. */
static void test_sql_standard_comment_multi_stmt_rejected(void) {
  const char *sql = "SELECT p.name AS name FROM private.people AS p;-- \n"
                    "SELECT pg_sleep(5);";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

  qir_handle_destroy(&h);
}

/* D3. Transaction control. */
static void test_sql_standard_txn_rejected(void) {
  const char *sql = "BEGIN; SELECT 1; COMMIT;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

  qir_handle_destroy(&h);
}

/* D4. Prepared statements. */
static void test_sql_standard_prepare_rejected(void) {
  const char *sql =
      "PREPARE s AS SELECT p.name AS name FROM private.people AS p; "
      "EXECUTE s;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

  qir_handle_destroy(&h);
}

/* D5. Data-changing statements. */
static void test_sql_standard_update_rejected(void) {
  const char *sql = "UPDATE private.people SET name = 'x' WHERE id = 1;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unsupported == true);
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* D6. ORDER BY with conflicting aliases. */
static void test_sql_standard_order_by_alias_conflict(void) {
  const char *sql = "SELECT p.name AS nm, "
                    "       p.surname AS nm "
                    "FROM private.people AS p "
                    "ORDER BY nm;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);

  qir_handle_destroy(&h);
}

/* E1. Unqualified column reference. */
static void test_sql_standard_unqualified_col(void) {
  const char *sql = "SELECT name AS name "
                    "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_COLREF);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.colref.qualifier, "");
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.colref.column, "name");

  qir_handle_destroy(&h);
}

/* E2. Schema-qualified function call. */
static void test_sql_standard_schema_func(void) {
  const char *sql = "SELECT pg_cataLog.\"loWer\"(p.email) AS email_lc "
                    "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.funcall.schema, "pg_catalog");
  ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.funcall.name, "lower");

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "email");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E3. Mixed-case identifiers should be normalized. */
static void test_sql_standard_mixed_case_touches(void) {
  const char *sql =
      "SELECT P.\"Name\" AS Nm, P.coLumn AS C1, P.\"coLumn\" AS C2 "
      "FROM private.people AS P "
      "WHERE P.\"Age\" > 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "column");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E4. LIKE operator. */
static void test_sql_standard_like(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.name LIKE 'A%';";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_LIKE);

  qir_handle_destroy(&h);
}

/* E9. NOT LIKE operator. */
static void test_sql_standard_not_like(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.name NOT LIKE 'A%';";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_NOT_LIKE);

  qir_handle_destroy(&h);
}

/* E5. BETWEEN and NOT BETWEEN. */
static void test_sql_standard_between(void) {
  const char *sql_between = "SELECT p.name AS name "
                            "FROM private.people AS p "
                            "WHERE p.age BETWEEN 18 AND 30;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql_between, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_AND);
  qir_handle_destroy(&h);

  const char *sql_not_between = "SELECT p.name AS name "
                                "FROM private.people AS p "
                                "WHERE p.age NOT BETWEEN 18 AND 30;";

  QirQueryHandle h2 = {0};
  parse_sql_postgres(sql_not_between, &h2);

  ASSERT_TRUE(h2.q != NULL);
  ASSERT_TRUE(h2.q->status == QIR_OK);
  ASSERT_TRUE(h2.q->where != NULL);
  ASSERT_TRUE(h2.q->where->kind == QIR_EXPR_OR);
  qir_handle_destroy(&h2);
}

/* E6. CASE expression. */
static void test_sql_standard_case(void) {
  const char *sql =
      "SELECT CASE WHEN p.age > 18 THEN p.name ELSE 'minor' END AS label "
      "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_CASE);
  ASSERT_TRUE(h.q->select_items[0]->value->u.case_.nwhens >= 1);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E7. Window function. */
static void test_sql_standard_window(void) {
  const char *sql =
      "SELECT row_number() OVER (PARTITION BY p.region ORDER BY p.id) AS rn "
      "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_WINDOWFUNC);
  ASSERT_TRUE(h.q->select_items[0]->value->u.window.n_partition_by == 1);
  ASSERT_TRUE(h.q->select_items[0]->value->u.window.n_order_by == 1);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E8. GROUP BY / HAVING. */
static void test_sql_standard_group_by_having(void) {
  const char *sql = "SELECT p.region AS region, count(*) AS c "
                    "FROM private.people AS p "
                    "GROUP BY p.region "
                    "HAVING count(*) > 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->n_group_by == 1);
  ASSERT_COLREF(h.q->group_by[0], "p", "region");
  ASSERT_TRUE(h.q->having != NULL);
  ASSERT_TRUE(h.q->having->kind == QIR_EXPR_GT);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E9. Window function with only PARTITION BY. */
static void test_sql_standard_window_partition_only(void) {
  const char *sql = "SELECT row_number() OVER (PARTITION BY p.region) AS rn "
                    "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_WINDOWFUNC);
  ASSERT_TRUE(h.q->select_items[0]->value->u.window.n_partition_by == 1);
  ASSERT_TRUE(h.q->select_items[0]->value->u.window.n_order_by == 0);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E10. Unqualified/unknown qualifier should be marked as UNKNOWN touch. */
static void test_sql_standard_unknown_touch(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE name = 'x' OR z.id = 1;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == true);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_UNKNOWN, "", "name");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_UNKNOWN, "z", "id");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

/* E11. VALUES in FROM. */
static void test_sql_standard_values_from_rejected(void) {
  const char *sql = "SELECT v.x AS x "
                    "FROM (VALUES (1), (2)) AS v(x) "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nfrom == 1);
  ASSERT_TRUE(h.q->from_items[0]->kind == QIR_FROM_VALUES);
  ASSERT_IDENT_EQ(&h.q->from_items[0]->alias, "v");
  ASSERT_TRUE(h.q->from_items[0]->u.values.ncolnames == 1);
  ASSERT_IDENT_EQ(&h.q->from_items[0]->u.values.colnames[0], "x");
  qir_handle_destroy(&h);
}

/* E12. IS NULL and IS NOT NULL*/
static void test_sql_standard_null_comparison(void) {
  const char *sql = "SELECT v.x AS x "
                    "FROM values v "
                    "WHERE v.y IS NULL AND v.z IS NOT NULL;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  QirExpr *lhs = h.q->where->u.bin.l;
  QirExpr *rhs = h.q->where->u.bin.r;
  ASSERT_TRUE(lhs->kind == QIR_EXPR_EQ);
  ASSERT_TRUE(rhs->kind == QIR_EXPR_NE);

  ASSERT_TRUE(lhs->u.bin.l->kind == QIR_EXPR_COLREF);
  ASSERT_TRUE(lhs->u.bin.r->kind == QIR_EXPR_LITERAL);
  ASSERT_IDENT_EQ(&lhs->u.bin.l->u.colref.qualifier, "v");
  ASSERT_IDENT_EQ(&lhs->u.bin.l->u.colref.column, "y");
  ASSERT_TRUE(lhs->u.bin.r->u.lit.kind == QIR_LIT_NULL);

  ASSERT_TRUE(rhs->u.bin.l->kind == QIR_EXPR_COLREF);
  ASSERT_TRUE(rhs->u.bin.r->kind == QIR_EXPR_LITERAL);
  ASSERT_IDENT_EQ(&rhs->u.bin.l->u.colref.qualifier, "v");
  ASSERT_IDENT_EQ(&rhs->u.bin.l->u.colref.column, "z");
  ASSERT_TRUE(rhs->u.bin.r->u.lit.kind == QIR_LIT_NULL);

  qir_handle_destroy(&h);
}

/* E13. LEFT JOIN yields base touches for both tables. */
static void test_left_join_base_touches(void) {
  const char *sql = "SELECT u.id AS id, e.amount AS amount "
                    "FROM users u "
                    "LEFT JOIN expenses e ON e.user_id = u.id "
                    "WHERE u.balance = 10 "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_TRUE(h.q->njoins == 1);
  ASSERT_TRUE(h.q->joins != NULL);
  ASSERT_TRUE(h.q->joins[0] != NULL);
  ASSERT_TRUE(h.q->joins[0]->kind == QIR_JOIN_LEFT);

  QirTouchReport *tr = extract_touches(&h);
  ASSERT_TRUE(tr->has_unknown_touches == false);
  ASSERT_TRUE(tr->has_unsupported == false);
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "u", "id");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "u", "balance");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "e", "amount");
  ASSERT_TOUCH(tr, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "e", "user_id");
  qir_touch_report_destroy(tr);

  qir_handle_destroy(&h);
}

int main(void) {
  test_sql_standard_predicates_and_limit();
  test_sql_standard_in_list();
  test_sql_standard_or();
  test_sql_standard_not();
  test_sql_standard_order_by();
  test_sql_standard_order_by_alias();
  test_sql_standard_distinct();
  test_sql_standard_func_call();
  test_sql_standard_join_inner();
  test_sql_standard_join_cross();
  test_sql_standard_offset();
  test_sql_standard_semicolon_literal();
  test_sql_standard_in_empty_list();
  test_sql_standard_count_star();
  test_sql_standard_literal_select();
  test_sql_standard_ctes();
  test_sql_standard_cte_sensitive_col();
  test_sql_standard_subquery_from();
  test_sql_standard_subquery_where();
  test_sql_standard_exists();
  test_sql_standard_in_subquery();
  test_sql_standard_star_in_cte();
  test_sql_standard_multi_stmt_rejected();
  test_sql_standard_comment_multi_stmt_rejected();
  test_sql_standard_txn_rejected();
  test_sql_standard_prepare_rejected();
  test_sql_standard_update_rejected();
  test_sql_standard_order_by_alias_conflict();
  test_sql_standard_unqualified_col();
  test_sql_standard_schema_func();
  test_sql_standard_mixed_case_touches();
  test_sql_standard_like();
  test_sql_standard_not_like();
  test_sql_standard_between();
  test_sql_standard_case();
  test_sql_standard_window();
  test_sql_standard_window_partition_only();
  test_sql_standard_group_by_having();
  test_sql_standard_unknown_touch();
  test_sql_standard_values_from_rejected();
  test_sql_standard_null_comparison();
  test_left_join_base_touches();
  fprintf(stderr, "OK: test_query_ir_sql_standard\n");
  return 0;
}
