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
static void assert_ident_eq(
        const QirIdent *id, const char *expected, const char *file, int line) {
    ASSERT_TRUE_AT(id != NULL, file, line);
    ASSERT_TRUE_AT(id->name != NULL, file, line);
    ASSERT_TRUE_AT(strcmp(id->name, expected) == 0, file, line);
}
#define ASSERT_IDENT_EQ(id, expected) assert_ident_eq((id), (expected), __FILE__, __LINE__)

/* Asserts that expression is a column reference and matches 'qual' and 'col'. */
static void assert_colref_expr(
        const QirExpr *e, const char *qual, const char *col,
        const char *file, int line) {
    ASSERT_TRUE_AT(e != NULL, file, line);
    ASSERT_TRUE_AT(e->kind == QIR_EXPR_COLREF, file, line);
    ASSERT_TRUE_AT(e->u.colref.qualifier.name != NULL, file, line);
    ASSERT_TRUE_AT(e->u.colref.column.name != NULL, file, line);
    ASSERT_TRUE_AT(strcmp(e->u.colref.qualifier.name, qual) == 0, file, line);
    ASSERT_TRUE_AT(strcmp(e->u.colref.column.name, col) == 0, file, line);
}
#define ASSERT_COLREF(e, qual, col) \
    assert_colref_expr((e), (qual), (col), __FILE__, __LINE__)

/* A1. Multiple predicates AND + comparisons + literals. */
static void test_sql_standard_predicates_and_limit(void) {
    const char *sql =
        "SELECT p.id AS pid "
        "FROM private.people AS p "
        "WHERE p.age >= 25 AND p.region = 'c' "
        "LIMIT 200;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->kind == QIR_STMT_SELECT);
    ASSERT_TRUE(h.q->has_star == false);
    ASSERT_TRUE(h.q->has_unsupported == false);
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

    qir_handle_destroy(&h);
}

/* A2. IN list with literals. */
static void test_sql_standard_in_list(void) {
    const char *sql =
        "SELECT p.name AS name "
        "FROM private.people AS p "
        "WHERE p.region IN ('a', 'b', 'c');";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->has_unsupported == false);

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

    qir_handle_destroy(&h);
}

/* A3. OR at top level. */
static void test_sql_standard_or(void) {
    const char *sql =
        "SELECT p.name AS name "
        "FROM private.people AS p "
        "WHERE p.region = 'a' OR p.region = 'b';";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->where != NULL);
    ASSERT_TRUE(h.q->where->kind == QIR_EXPR_OR);

    const QirExpr *lhs = h.q->where->u.bin.l;
    const QirExpr *rhs = h.q->where->u.bin.r;
    ASSERT_TRUE(lhs && rhs);
    ASSERT_TRUE(lhs->kind == QIR_EXPR_EQ);
    ASSERT_TRUE(rhs->kind == QIR_EXPR_EQ);

    qir_handle_destroy(&h);
}

/* A4. NOT predicate. */
static void test_sql_standard_not(void) {
    const char *sql =
        "SELECT p.name AS name "
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

    qir_handle_destroy(&h);
}

/* A5. ORDER BY with qualified column. */
static void test_sql_standard_order_by(void) {
    const char *sql =
        "SELECT p.name AS name "
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

/* A5b. ORDER BY resolves SELECT alias. */
static void test_sql_standard_order_by_alias(void) {
    const char *sql =
        "WITH cte_people AS ("
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

    qir_handle_destroy(&h);
}

/* A6. DISTINCT. */
static void test_sql_standard_distinct(void) {
    const char *sql =
        "SELECT DISTINCT p.region AS region "
        "FROM private.people AS p;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->has_distinct == true);

    qir_handle_destroy(&h);
}

/* A7. Function calls in SELECT and WHERE. */
static void test_sql_standard_func_call(void) {
    const char *sql =
        "SELECT lower(p.email) AS email_lc "
        "FROM private.people AS p "
        "WHERE upper(p.region) = upper($1);";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->nselect == 1);
    ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
    ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.funcall.name, "lower");

    ASSERT_TRUE(h.q->where != NULL);
    ASSERT_TRUE(h.q->where->kind == QIR_EXPR_EQ);
    ASSERT_TRUE(h.q->where->u.bin.l->kind == QIR_EXPR_FUNCALL);
    ASSERT_TRUE(h.q->where->u.bin.r->kind == QIR_EXPR_FUNCALL);

    qir_handle_destroy(&h);
}

/* A8. INNER JOIN + ON predicate. */
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

    qir_handle_destroy(&h);
}

/* A9. CROSS JOIN. */
static void test_sql_standard_join_cross(void) {
    const char *sql =
        "SELECT p.name AS n, r.code AS c "
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

/* A10. OFFSET. */
static void test_sql_standard_offset(void) {
    const char *sql =
        "SELECT p.name AS name "
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

/* A11. Semicolon inside string literal. */
static void test_sql_standard_semicolon_literal(void) {
    const char *sql =
        "SELECT 'a; b; c' AS txt "
        "FROM private.people AS p;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->has_unsupported == false);

    qir_handle_destroy(&h);
}

/* A12. IN with empty list (parse error). */
static void test_sql_standard_in_empty_list(void) {
    const char *sql =
        "SELECT p.name AS name "
        "FROM private.people AS p "
        "WHERE p.region IN ();";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

    qir_handle_destroy(&h);
}

/* A13. COUNT(*) */
static void test_sql_standard_count_star(void) {
    const char *sql =
        "SELECT COUNT(*) "
        "FROM people AS p;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->nselect == 1);
    ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
    ASSERT_TRUE(h.q->select_items[0]->value->u.funcall.is_star == true);

    qir_handle_destroy(&h);
}

/* B1. Multiple CTEs. */
static void test_sql_standard_ctes(void) {
    const char *sql =
        "WITH male AS ("
        "  SELECT p.name AS name "
        "  FROM private.people AS p "
        "  WHERE p.gender = 'M'"
        "), "
        "female AS ("
        "  SELECT p.name AS name "
        "  FROM private.people AS p "
        "  WHERE p.gender = 'F'"
        ") "
        "SELECT m.name AS name "
        "FROM male AS m;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->nctes == 2);
    ASSERT_IDENT_EQ(&h.q->ctes[0]->name, "male");
    ASSERT_IDENT_EQ(&h.q->ctes[1]->name, "female");

    qir_handle_destroy(&h);
}

/* B2. Subquery in FROM. */
static void test_sql_standard_subquery_from(void) {
    const char *sql =
        "SELECT x.name AS name "
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

    qir_handle_destroy(&h);
}

/* B3. Scalar subquery in WHERE. */
static void test_sql_standard_subquery_where(void) {
    const char *sql =
        "SELECT p.name AS name "
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

/* B4. EXISTS subquery. */
static void test_sql_standard_exists(void) {
    const char *sql =
        "SELECT p.name AS name "
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

/* B5. IN (SELECT ...) */
static void test_sql_standard_in_subquery(void) {
    const char *sql =
        "SELECT p.name AS name "
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

    qir_handle_destroy(&h);
}

/* C1. Star should be detected inside CTE body. */
static void test_sql_standard_star_in_cte(void) {
    const char *sql =
        "WITH x AS ("
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

    qir_handle_destroy(&h);
}

/* D1. Multiple statements. */
static void test_sql_standard_multi_stmt_rejected(void) {
    const char *sql =
        "SELECT p.name AS name FROM private.people AS p; "
        "SELECT pg_sleep(5);";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_PARSE_ERROR);

    qir_handle_destroy(&h);
}

/* D2. Comment-trick multiple statements. */
static void test_sql_standard_comment_multi_stmt_rejected(void) {
    const char *sql =
        "SELECT p.name AS name FROM private.people AS p;-- \n"
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
    const char *sql =
        "UPDATE private.people SET name = 'x' WHERE id = 1;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);

    qir_handle_destroy(&h);
}

/* D6. ORDER BY with conflicting aliases. */
static void test_sql_standard_order_by_alias_conflict(void) {
    const char *sql =
        "SELECT p.name AS nm, "
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
    const char *sql =
        "SELECT name AS name "
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
    const char *sql =
        "SELECT pg_catalog.lower(p.email) AS email_lc "
        "FROM private.people AS p;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);
    ASSERT_IDENT_EQ(&h.q->select_items[0]->value->u.funcall.name, "pg_catalog.lower");

    qir_handle_destroy(&h);
}

/* E3. LIKE operator. */
static void test_sql_standard_like(void) {
    const char *sql =
        "SELECT p.name AS name "
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

/* E8. NOT LIKE operator. */
static void test_sql_standard_not_like(void) {
    const char *sql =
        "SELECT p.name AS name "
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

/* E4. BETWEEN and NOT BETWEEN. */
static void test_sql_standard_between(void) {
    const char *sql_between =
        "SELECT p.name AS name "
        "FROM private.people AS p "
        "WHERE p.age BETWEEN 18 AND 30;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql_between, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->where != NULL);
    ASSERT_TRUE(h.q->where->kind == QIR_EXPR_AND);
    qir_handle_destroy(&h);

    const char *sql_not_between =
        "SELECT p.name AS name "
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

/* E5. CASE expression. */
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

    qir_handle_destroy(&h);
}

/* E6. Window function. */
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

    qir_handle_destroy(&h);
}

/* E7. GROUP BY / HAVING. */
static void test_sql_standard_group_by_having(void) {
    const char *sql =
        "SELECT p.region AS region, count(*) AS c "
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

    qir_handle_destroy(&h);
}

/* E9. Window function with only PARTITION BY. */
static void test_sql_standard_window_partition_only(void) {
    const char *sql =
        "SELECT row_number() OVER (PARTITION BY p.region) AS rn "
        "FROM private.people AS p;";

    QirQueryHandle h = {0};
    parse_sql_postgres(sql, &h);

    ASSERT_TRUE(h.q != NULL);
    ASSERT_TRUE(h.q->status == QIR_OK);
    ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_WINDOWFUNC);
    ASSERT_TRUE(h.q->select_items[0]->value->u.window.n_partition_by == 1);
    ASSERT_TRUE(h.q->select_items[0]->value->u.window.n_order_by == 0);

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
    test_sql_standard_ctes();
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
    test_sql_standard_like();
    test_sql_standard_not_like();
    test_sql_standard_between();
    test_sql_standard_case();
    test_sql_standard_window();
    test_sql_standard_window_partition_only();
    test_sql_standard_group_by_having();
    fprintf(stderr, "OK: test_query_ir_sql_standard\n");
    return 0;
}
