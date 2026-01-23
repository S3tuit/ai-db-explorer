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
    ASSERT_TRUE(h.q->has_subquery == false);
    ASSERT_TRUE(h.q->has_unsupported == false);
    ASSERT_TRUE(h.q->limit_value == 200);

    // SELECT list
    ASSERT_TRUE(h.q->nselect == 1);
    ASSERT_TRUE(h.q->select_items != NULL);
    ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "pid");
    ASSERT_IDENT_EQ(&h.q->select_items[0]->value.qualifier, "p");
    ASSERT_IDENT_EQ(&h.q->select_items[0]->value.column, "id");

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

int main(void) {
    test_sql_standard_predicates_and_limit();
    fprintf(stderr, "OK: test_query_ir_sql_standard\n");
    return 0;
}
