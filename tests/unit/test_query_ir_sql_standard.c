#include <stdio.h>
#include <string.h>

#include "db_backend.h"
#include "postgres_backend.h"
#include "query_ir.h"
#include "test.h"

/* Binds one parsed query in place.
 * Ownership: borrows 'h' and mutates the bound metadata inside its QueryIR.
 * Side effects: prints the binder error on failure before asserting.
 * Returns void; assertions abort on failure. */
static void assert_bind_ok(const QirQueryHandle *h, const char *file, int line) {
  ASSERT_TRUE(h != NULL);
  ASSERT_TRUE(h->q != NULL);
  QirBindErr err = {0};
  AdbxTriStatus rc = bind_query_ir(h->q, &err);
  if (rc != YES && err.msg[0] != '\0')
    fprintf(stderr, "BIND FAILED: %s\n", err.msg);
  ASSERT_TRUE_AT(rc == YES, file, line);
}
#define ASSERT_BIND_OK(h) assert_bind_ok((h), __FILE__, __LINE__)

/* Asserts that binding fails with one specific binder error. */
static void assert_bind_fails(const QirQueryHandle *h, QirBindErrCode expected,
                              const char *file, int line) {
  ASSERT_TRUE_AT(h != NULL, file, line);
  ASSERT_TRUE_AT(h->q != NULL, file, line);
  QirBindErr err = {0};
  AdbxTriStatus rc = bind_query_ir(h->q, &err);
  ASSERT_TRUE_AT(rc != YES, file, line);
  ASSERT_TRUE_AT(err.code == expected, file, line);
}
#define ASSERT_BIND_FAILS(h, expected)                                          \
  assert_bind_fails((h), (expected), __FILE__, __LINE__)

/* Asserts that binding fails with one specific binder error and message
 * substring. */
static void assert_bind_fails_msg(const QirQueryHandle *h,
                                  QirBindErrCode expected,
                                  const char *substr, const char *file,
                                  int line) {
  ASSERT_TRUE_AT(h != NULL, file, line);
  ASSERT_TRUE_AT(h->q != NULL, file, line);
  ASSERT_TRUE_AT(substr != NULL, file, line);
  QirBindErr err = {0};
  AdbxTriStatus rc = bind_query_ir(h->q, &err);
  ASSERT_TRUE_AT(rc != YES, file, line);
  ASSERT_TRUE_AT(err.code == expected, file, line);
  ASSERT_TRUE_AT(strstr(err.msg, substr) != NULL, file, line);
}
#define ASSERT_BIND_FAILS_MSG(h, expected, substr)                              \
  assert_bind_fails_msg((h), (expected), (substr), __FILE__, __LINE__)

typedef struct TouchMatchCtx {
  QirScope scope;
  QirTouchKind kind;
  const char *qual;
  const char *col;
  bool found;
} TouchMatchCtx;

typedef struct TouchOwnerCtx {
  QirScope scope;
  QirTouchKind kind;
  const char *qual;
  const char *col;
  const QirQuery *owner;
  bool expect_equal;
  bool found;
} TouchOwnerCtx;

static AdbxStatus touch_match_cb(QirScope scope, const QirQuery *owner_query,
                                 const QirColRef *colref, QirTouchKind kind,
                                 void *vctx) {
  TouchMatchCtx *ctx = (TouchMatchCtx *)vctx;
  (void)owner_query;

  if (!ctx || !colref)
    return ERR;
  if (scope != ctx->scope || kind != ctx->kind)
    return OK;
  if (!colref->qualifier.name || !colref->column.name)
    return OK;
  if (strcmp(colref->qualifier.name, ctx->qual) != 0)
    return OK;
  if (strcmp(colref->column.name, ctx->col) != 0)
    return OK;
  ctx->found = true;
  return OK;
}

static AdbxStatus touch_owner_cb(QirScope scope, const QirQuery *owner_query,
                                 const QirColRef *colref, QirTouchKind kind,
                                 void *vctx) {
  TouchOwnerCtx *ctx = (TouchOwnerCtx *)vctx;

  if (!ctx || !colref)
    return ERR;
  if (scope != ctx->scope || kind != ctx->kind)
    return OK;
  if (!colref->qualifier.name || !colref->column.name)
    return OK;
  if (strcmp(colref->qualifier.name, ctx->qual) != 0)
    return OK;
  if (strcmp(colref->column.name, ctx->col) != 0)
    return OK;
  if (ctx->expect_equal && owner_query != ctx->owner)
    return OK;
  if (!ctx->expect_equal && owner_query == ctx->owner)
    return OK;
  ctx->found = true;
  return OK;
}

/* Asserts that a touch matching the given fields exists. */
static void assert_touch_has(const QirQuery *q, QirScope scope,
                             QirTouchKind kind, const char *qual,
                             const char *col, const char *file, int line) {
  ASSERT_TRUE_AT(q != NULL, file, line);
  ASSERT_TRUE_AT(qual != NULL, file, line);
  ASSERT_TRUE_AT(col != NULL, file, line);
  TouchMatchCtx ctx = {
      .scope = scope,
      .kind = kind,
      .qual = qual,
      .col = col,
      .found = false,
  };
  ASSERT_TRUE_AT(qir_walk_touches(q, touch_match_cb, &ctx) == OK, file, line);
  ASSERT_TRUE_AT(ctx.found, file, line);
}
#define ASSERT_TOUCH(q, scope, kind, qual, col)                                \
  assert_touch_has((q), (scope), (kind), (qual), (col), __FILE__, __LINE__)

static void assert_touch_owner_query(const QirQuery *q, QirScope scope,
                                     QirTouchKind kind, const char *qual,
                                     const char *col,
                                     const QirQuery *owner_query,
                                     bool expect_equal, const char *file,
                                     int line) {
  ASSERT_TRUE_AT(q != NULL, file, line);
  ASSERT_TRUE_AT(qual != NULL, file, line);
  ASSERT_TRUE_AT(col != NULL, file, line);
  ASSERT_TRUE_AT(owner_query != NULL, file, line);
  TouchOwnerCtx ctx = {
      .scope = scope,
      .kind = kind,
      .qual = qual,
      .col = col,
      .owner = owner_query,
      .expect_equal = expect_equal,
      .found = false,
  };
  ASSERT_TRUE_AT(qir_walk_touches(q, touch_owner_cb, &ctx) == OK, file, line);
  ASSERT_TRUE_AT(ctx.found, file, line);
}
#define ASSERT_TOUCH_OWNER_EQ(q, scope, kind, qual, col, owner_query)          \
  assert_touch_owner_query((q), (scope), (kind), (qual), (col),               \
                           (owner_query), true, __FILE__, __LINE__)
#define ASSERT_TOUCH_OWNER_NE(q, scope, kind, qual, col, owner_query)          \
  assert_touch_owner_query((q), (scope), (kind), (qual), (col),               \
                           (owner_query), false, __FILE__, __LINE__)

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

/* Asserts that one bound colref resolves to the expected range item and
 * correlation depth. */
static void assert_colref_binding(const QirColRef *cr,
                                  const QirFromItem *binding_from,
                                  uint32_t correlation_depth,
                                  const char *file, int line) {
  ASSERT_TRUE_AT(cr != NULL, file, line);
  ASSERT_TRUE_AT(binding_from != NULL, file, line);
  ASSERT_TRUE_AT(cr->binding_from == binding_from, file, line);
  ASSERT_TRUE_AT(cr->correlation_depth == correlation_depth, file, line);
}
#define ASSERT_COLREF_BINDING(cr, binding_from, depth)                          \
  assert_colref_binding((cr), (binding_from), (depth), __FILE__, __LINE__)

/* Asserts that expression is one generalized operator node. */
static void assert_op_expr(const QirExpr *e, QirOpClass cls,
                           const char *op_name, const char *file, int line) {
  ASSERT_TRUE_AT(e != NULL, file, line);
  ASSERT_TRUE_AT(e->kind == QIR_EXPR_OP, file, line);
  ASSERT_TRUE_AT(e->u.op.cls == cls, file, line);
  ASSERT_TRUE_AT(e->u.op.op_name != NULL, file, line);
  ASSERT_TRUE_AT(strcmp(e->u.op.op_name, op_name) == 0, file, line);
}
#define ASSERT_OP(e, cls, op_name)                                             \
  assert_op_expr((e), (cls), (op_name), __FILE__, __LINE__)

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
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->kind == QIR_FROM_BASE_REL);
  ASSERT_IDENT_EQ(&h.q->from_root->alias, "p");
  ASSERT_IDENT_EQ(&h.q->from_root->u.rel.schema, "private");
  ASSERT_IDENT_EQ(&h.q->from_root->u.rel.name, "people");

  // WHERE: p.age >= 25 AND p.region = 'c'
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_AND);

  const QirExpr *lhs = h.q->where->u.bin.l;
  const QirExpr *rhs = h.q->where->u.bin.r;
  ASSERT_TRUE(lhs && rhs);

  ASSERT_OP(lhs, QIR_OP_OTHER, ">=");
  ASSERT_COLREF(lhs->u.op.lhs, "p", "age");
  ASSERT_TRUE(lhs->u.op.args[0] && lhs->u.op.args[0]->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(lhs->u.op.args[0]->u.lit.kind == QIR_LIT_INT64);
  ASSERT_TRUE(lhs->u.op.args[0]->u.lit.v.i64 == 25);

  ASSERT_OP(rhs, QIR_OP_EQ, "=");
  ASSERT_COLREF(rhs->u.op.lhs, "p", "region");
  ASSERT_TRUE(rhs->u.op.args[0] && rhs->u.op.args[0]->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(rhs->u.op.args[0]->u.lit.kind == QIR_LIT_STRING);
  ASSERT_TRUE(strcmp(rhs->u.op.args[0]->u.lit.v.s, "c") == 0);

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");

  qir_handle_destroy(&h);
}

/* A1b. Multiple FROM items are unsupported (normalize to CROSS JOIN not allowed). */
static void test_sql_standard_multi_from_unsupported(void) {
  const char *sql = "SELECT a.id AS id "
                    "FROM users a, users b "
                    "WHERE a.id = b.id;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_UNSUPPORTED);

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
  ASSERT_OP(h.q->where, QIR_OP_IN, "=");
  ASSERT_COLREF(h.q->where->u.op.lhs, "p", "region");
  ASSERT_TRUE(h.q->where->u.op.nargs == 3);

  const QirExpr *i0 = h.q->where->u.op.args[0];
  const QirExpr *i1 = h.q->where->u.op.args[1];
  const QirExpr *i2 = h.q->where->u.op.args[2];
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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");

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
  ASSERT_OP(lhs, QIR_OP_EQ, "=");
  ASSERT_OP(rhs, QIR_OP_EQ, "=");

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "disabled");

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

/* A6. ORDER BY alias stays syntactic until binder substitutes SELECT expr. */
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
  ASSERT_COLREF(h.q->order_by[0], "", "a_name");

  ASSERT_BIND_OK(&h);
  ASSERT_COLREF(h.q->order_by[0], "p", "nm");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "p", "nm");
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "name");

  qir_handle_destroy(&h);
}

/* A6b. Nested ORDER BY alias resolution must stay local to the subquery. */
static void test_sql_standard_nested_order_by_alias_is_local(void) {
  const char *sql = "SELECT p.name AS nm "
                    "FROM private.people AS p "
                    "WHERE p.id = ("
                    "  SELECT o.user_id AS nm "
                    "  FROM orders AS o "
                    "  WHERE o.user_id = p.id "
                    "  ORDER BY nm "
                    "  LIMIT 1"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "nm");
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_OP(h.q->where, QIR_OP_EQ, "=");
  ASSERT_TRUE(h.q->where->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->kind == QIR_EXPR_OP);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.nargs == 1);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.args[0]->kind ==
              QIR_EXPR_SUBQUERY);

  QirQuery *sq = h.q->where->u.op.args[0]->u.op.args[0]->u.subquery;
  ASSERT_TRUE(sq != NULL);
  ASSERT_TRUE(sq->nselect == 1);
  ASSERT_IDENT_EQ(&sq->select_items[0]->out_alias, "nm");
  ASSERT_TRUE(sq->n_order_by == 1);
  ASSERT_COLREF(sq->order_by[0], "", "nm");

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(sq->order_by[0] == sq->select_items[0]->value);
  ASSERT_TRUE(sq->order_by[0] != h.q->select_items[0]->value);
  ASSERT_COLREF(sq->order_by[0], "o", "user_id");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "o", "user_id");

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
  ASSERT_OP(h.q->where, QIR_OP_EQ, "=");
  ASSERT_TRUE(h.q->where->u.op.lhs->kind == QIR_EXPR_FUNCALL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->kind == QIR_EXPR_FUNCALL);

  qir_handle_destroy(&h);
}

/* A8b. COALESCE/GREATEST/LEAST normalize to regular function calls. */
static void test_sql_standard_minmax_and_coalesce(void) {
  const char *sql =
      "SELECT GREATEST(COALESCE(p.score, 0), LEAST(p.cap, 100)) AS amount "
      "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_TRUE(h.q->select_items[0]->value != NULL);
  ASSERT_TRUE(h.q->select_items[0]->value->kind == QIR_EXPR_FUNCALL);

  const QirFuncCall *greatest = &h.q->select_items[0]->value->u.funcall;
  ASSERT_IDENT_EQ(&greatest->schema, "");
  ASSERT_IDENT_EQ(&greatest->name, "greatest");
  ASSERT_TRUE(greatest->nargs == 2);
  ASSERT_TRUE(greatest->args != NULL);

  ASSERT_TRUE(greatest->args[0] != NULL);
  ASSERT_TRUE(greatest->args[0]->kind == QIR_EXPR_FUNCALL);
  const QirFuncCall *coalesce = &greatest->args[0]->u.funcall;
  ASSERT_IDENT_EQ(&coalesce->schema, "");
  ASSERT_IDENT_EQ(&coalesce->name, "coalesce");
  ASSERT_TRUE(coalesce->nargs == 2);
  ASSERT_COLREF(coalesce->args[0], "p", "score");
  ASSERT_TRUE(coalesce->args[1] != NULL);
  ASSERT_TRUE(coalesce->args[1]->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(coalesce->args[1]->u.lit.kind == QIR_LIT_INT64);
  ASSERT_TRUE(coalesce->args[1]->u.lit.v.i64 == 0);

  ASSERT_TRUE(greatest->args[1] != NULL);
  ASSERT_TRUE(greatest->args[1]->kind == QIR_EXPR_FUNCALL);
  const QirFuncCall *least = &greatest->args[1]->u.funcall;
  ASSERT_IDENT_EQ(&least->schema, "");
  ASSERT_IDENT_EQ(&least->name, "least");
  ASSERT_TRUE(least->nargs == 2);
  ASSERT_COLREF(least->args[0], "p", "cap");
  ASSERT_TRUE(least->args[1] != NULL);
  ASSERT_TRUE(least->args[1]->kind == QIR_EXPR_LITERAL);
  ASSERT_TRUE(least->args[1]->u.lit.kind == QIR_LIT_INT64);
  ASSERT_TRUE(least->args[1]->u.lit.v.i64 == 100);

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "score");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "cap");

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "f", "friend_name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "f", "person_id");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");

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

/* A11. JOIN ON cannot reference a later join alias. */
static void test_sql_standard_join_on_cannot_see_successive_join(void) {
  const char *sql = "SELECT a.id AS id "
                    "FROM a "
                    "JOIN b ON c.id = a.id "
                    "JOIN c ON c.id = b.id;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->njoins == 2);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_UNRESOLVED_COLREF);

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_DERIVED, "pm", "gender");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "m", "name");
  ASSERT_TOUCH_OWNER_NE(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_DERIVED, "pm",
                        "gender", h.q);

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "u", "fiscal_code");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "t", "fiscal_code");
  ASSERT_TOUCH_OWNER_EQ(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "t",
                        "fiscal_code", h.q);

  qir_handle_destroy(&h);
}

/* B3. A later CTE in the same WITH clause is not visible to an earlier one. */
static void test_sql_standard_cte_cannot_see_later_sibling(void) {
  const char *sql = "WITH a AS ("
                    "  SELECT d.x AS ax "
                    "  FROM d"
                    "), "
                    "b AS (SELECT 2 AS x), "
                    "c AS (SELECT 3 AS x), "
                    "d AS (SELECT 4 AS x) "
                    "SELECT a.ax AS ax "
                    "FROM a;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 4);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->name, "a");
  ASSERT_IDENT_EQ(&h.q->ctes[3]->name, "d");

  ASSERT_BIND_OK(&h);

  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->binding_cte == h.q->ctes[0]);

  QirQuery *cte_a = h.q->ctes[0]->query;
  ASSERT_TRUE(cte_a != NULL);
  ASSERT_TRUE(cte_a->from_root != NULL);
  ASSERT_TRUE(cte_a->from_root->kind == QIR_FROM_BASE_REL);
  ASSERT_TRUE(cte_a->from_root->binding_cte == NULL);

  qir_handle_destroy(&h);
}

/* B4. A non-recursive CTE body must not see itself as a visible CTE. */
static void test_sql_standard_cte_cannot_see_itself(void) {
  const char *sql = "WITH a AS ("
                    "  SELECT a.x AS ax "
                    "  FROM a"
                    ") "
                    "SELECT a.ax AS ax "
                    "FROM a;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);

  ASSERT_BIND_OK(&h);

  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->binding_cte == h.q->ctes[0]);

  QirQuery *cte_a = h.q->ctes[0]->query;
  ASSERT_TRUE(cte_a != NULL);
  ASSERT_TRUE(cte_a->from_root != NULL);
  ASSERT_TRUE(cte_a->from_root->binding_cte == NULL);
  ASSERT_TRUE(cte_a->select_items != NULL);
  ASSERT_TRUE(cte_a->select_items[0] != NULL);
  ASSERT_TRUE(cte_a->select_items[0]->value != NULL);
  ASSERT_COLREF(cte_a->select_items[0]->value, "a", "x");
  ASSERT_COLREF_BINDING(&cte_a->select_items[0]->value->u.colref,
                        cte_a->from_root, 0);

  qir_handle_destroy(&h);
}

/* B5. Duplicate visible CTE names must fail with an ambiguous CTE bind error. */
static void test_sql_standard_cte_ambiguous_lookup(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT 1 AS a"
                    "), "
                    "x AS ("
                    "  SELECT 2 AS a"
                    ") "
                    "SELECT z.a AS a "
                    "FROM x AS z;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 2);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->name, "x");
  ASSERT_IDENT_EQ(&h.q->ctes[1]->name, "x");

  ASSERT_BIND_FAILS_MSG(&h, QIR_BINDERR_AMBIGUOUS_CTE, "Ambiguous CTE");

  qir_handle_destroy(&h);
}

/* B6. Subquery in FROM. */
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
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->kind == QIR_FROM_SUBQUERY);
  ASSERT_IDENT_EQ(&h.q->from_root->alias, "x");

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "x", "name");

  qir_handle_destroy(&h);
}

/* B6. FROM subqueries stay isolated from outer range aliases. */
static void test_sql_standard_from_subquery_cannot_see_outer_alias(void) {
  const char *sql = "SELECT p.id AS id "
                    "FROM people AS p "
                    "JOIN ("
                    "  SELECT p.id AS pid"
                    ") AS x ON x.pid = p.id;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->njoins == 1);
  ASSERT_TRUE(h.q->joins[0] != NULL);
  ASSERT_TRUE(h.q->joins[0]->rhs != NULL);
  ASSERT_TRUE(h.q->joins[0]->rhs->kind == QIR_FROM_SUBQUERY);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_UNRESOLVED_COLREF);

  qir_handle_destroy(&h);
}

/* B7. Scalar subquery in WHERE. */
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
  ASSERT_OP(h.q->where, QIR_OP_EQ, "=");
  ASSERT_TRUE(h.q->where->u.op.args[0] != NULL);
  ASSERT_OP(h.q->where->u.op.args[0], QIR_OP_OTHER, "EXPR_SUBLINK");
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.lhs == NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.nargs == 1);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.args != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->u.op.args[0]->kind ==
              QIR_EXPR_SUBQUERY);

  qir_handle_destroy(&h);
}

/* B8. EXISTS subquery. */
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
  ASSERT_OP(h.q->where, QIR_OP_OTHER, "EXISTS_SUBLINK");
  ASSERT_TRUE(h.q->where->u.op.lhs == NULL);
  ASSERT_TRUE(h.q->where->u.op.nargs == 1);
  ASSERT_TRUE(h.q->where->u.op.args != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->kind == QIR_EXPR_SUBQUERY);

  qir_handle_destroy(&h);
}

/* B9. Correlated expression subquery binds outer aliases with depth 1. */
static void test_sql_standard_correlated_subquery_binding(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE EXISTS ("
                    "  SELECT 1 "
                    "  FROM orders AS o "
                    "  WHERE o.user_id = p.id"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_OP(h.q->where, QIR_OP_OTHER, "EXISTS_SUBLINK");
  ASSERT_TRUE(h.q->where->u.op.nargs == 1);
  ASSERT_TRUE(h.q->where->u.op.args != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->kind == QIR_EXPR_SUBQUERY);

  QirQuery *sq = h.q->where->u.op.args[0]->u.subquery;
  ASSERT_TRUE(sq != NULL);
  ASSERT_TRUE(sq->from_root != NULL);
  ASSERT_TRUE(sq->where != NULL);
  ASSERT_OP(sq->where, QIR_OP_EQ, "=");
  ASSERT_COLREF(sq->where->u.op.lhs, "o", "user_id");
  ASSERT_TRUE(sq->where->u.op.nargs == 1);
  ASSERT_COLREF(sq->where->u.op.args[0], "p", "id");

  ASSERT_BIND_OK(&h);

  ASSERT_COLREF_BINDING(&sq->where->u.op.lhs->u.colref, sq->from_root, 0);
  ASSERT_COLREF_BINDING(&sq->where->u.op.args[0]->u.colref, h.q->from_root, 1);

  qir_handle_destroy(&h);
}

/* B10. IN (SELECT ...) */
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
  ASSERT_OP(h.q->where, QIR_OP_IN, "ANY_SUBLINK");
  ASSERT_TRUE(h.q->where->u.op.lhs != NULL);
  ASSERT_TRUE(h.q->where->u.op.nargs == 1);
  ASSERT_TRUE(h.q->where->u.op.args != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.op.args[0]->kind == QIR_EXPR_SUBQUERY);

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "o", "user_id");

  qir_handle_destroy(&h);
}

/* B11. NOT IN (SELECT ...) keeps the membership predicate inside NOT. */
static void test_sql_standard_not_in_subquery(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE p.id NOT IN ("
                    "  SELECT o.user_id "
                    "  FROM orders AS o "
                    "  WHERE o.total > 10"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_TRUE(h.q->where->kind == QIR_EXPR_NOT);
  ASSERT_OP(h.q->where->u.bin.l, QIR_OP_IN, "ANY_SUBLINK");
  ASSERT_TRUE(h.q->where->u.bin.l->u.op.lhs != NULL);
  ASSERT_TRUE(h.q->where->u.bin.l->u.op.nargs == 1);
  ASSERT_TRUE(h.q->where->u.bin.l->u.op.args != NULL);
  ASSERT_TRUE(h.q->where->u.bin.l->u.op.args[0] != NULL);
  ASSERT_TRUE(h.q->where->u.bin.l->u.op.args[0]->kind == QIR_EXPR_SUBQUERY);

  qir_handle_destroy(&h);
}

/* B12. Nested WITH clauses inherit visible CTEs from outer query blocks. */
static void test_sql_standard_nested_with_sees_outer_cte(void) {
  const char *sql = "WITH outer_cte AS ("
                    "  SELECT s.x AS x "
                    "  FROM src s"
                    ") "
                    "SELECT y.x AS x "
                    "FROM ("
                    "  WITH inner_cte AS ("
                    "    SELECT o.x AS x "
                    "    FROM outer_cte o"
                    "  ) "
                    "  SELECT i.x AS x "
                    "  FROM inner_cte i"
                    ") AS y;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->kind == QIR_FROM_SUBQUERY);

  ASSERT_BIND_OK(&h);

  QirQuery *nested = h.q->from_root->u.subquery;
  ASSERT_TRUE(nested != NULL);
  ASSERT_TRUE(nested->nctes == 1);
  ASSERT_TRUE(nested->from_root != NULL);
  ASSERT_TRUE(nested->from_root->binding_cte == nested->ctes[0]);

  QirQuery *inner_cte = nested->ctes[0]->query;
  ASSERT_TRUE(inner_cte != NULL);
  ASSERT_TRUE(inner_cte->from_root != NULL);
  ASSERT_TRUE(inner_cte->from_root->binding_cte == h.q->ctes[0]);

  qir_handle_destroy(&h);
}

/* B13. An inner WITH shadows an outer CTE with the same name. */
static void test_sql_standard_nested_with_shadows_outer_cte(void) {
  const char *sql = "WITH vals AS ("
                    "  SELECT s.x AS x "
                    "  FROM src s"
                    ") "
                    "SELECT y.x AS x "
                    "FROM ("
                    "  WITH vals AS ("
                    "    SELECT t.y AS x "
                    "    FROM tbl t"
                    "  ) "
                    "  SELECT v.x AS x "
                    "  FROM vals v"
                    ") AS y;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->kind == QIR_FROM_SUBQUERY);

  ASSERT_BIND_OK(&h);

  QirQuery *nested = h.q->from_root->u.subquery;
  ASSERT_TRUE(nested != NULL);
  ASSERT_TRUE(nested->nctes == 1);
  ASSERT_TRUE(nested->from_root != NULL);
  ASSERT_TRUE(nested->from_root->binding_cte == nested->ctes[0]);
  ASSERT_TRUE(nested->from_root->binding_cte != h.q->ctes[0]);

  qir_handle_destroy(&h);
}

/* C1. Parser preserves explicit CTE output column names. */
static void test_sql_standard_cte_column_list_parsed(void) {
  const char *sql = "WITH x(a, b) AS ("
                    "  SELECT p.name, p.region "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT x.a AS a "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_TRUE(h.q->ctes[0] != NULL);
  ASSERT_TRUE(h.q->ctes[0]->query != NULL);
  ASSERT_TRUE(h.q->ctes[0]->colnames != NULL);
  ASSERT_TRUE(h.q->ctes[0]->ncolnames == 2);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->colnames[0], "a");
  ASSERT_IDENT_EQ(&h.q->ctes[0]->colnames[1], "b");

  qir_handle_destroy(&h);
}

/* C2. Parser keeps explicit CTE column names even when the inner arity
 * disagrees; binder/validator decide later whether the query is valid.
 */
static void test_sql_standard_cte_column_list_count_mismatch_preserved(void) {
  const char *sql = "WITH x(a, b) AS ("
                    "  SELECT p.name "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT x.a AS a "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_TRUE(h.q->ctes[0] != NULL);
  ASSERT_TRUE(h.q->ctes[0]->query != NULL);
  ASSERT_TRUE(h.q->ctes[0]->colnames != NULL);
  ASSERT_TRUE(h.q->ctes[0]->ncolnames == 2);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->colnames[0], "a");
  ASSERT_IDENT_EQ(&h.q->ctes[0]->colnames[1], "b");

  qir_handle_destroy(&h);
}

/* C3. Explicit CTE column lists override inner projection names during star
 * expansion.
 */
static void test_sql_standard_star_in_cte_column_list_overrides_names(void) {
  const char *sql = "WITH x(a, b) AS ("
                    "  SELECT p.name, p.region "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT * "
                    "FROM x AS x "
                    "ORDER BY a;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "a");
  ASSERT_IDENT_EQ(&h.q->select_items[1]->out_alias, "b");
  ASSERT_COLREF(h.q->select_items[0]->value, "x", "a");
  ASSERT_COLREF(h.q->select_items[1]->value, "x", "b");
  ASSERT_TRUE(h.q->n_order_by == 1);
  ASSERT_TRUE(h.q->order_by[0] == h.q->select_items[0]->value);
  ASSERT_COLREF(h.q->order_by[0], "x", "a");

  qir_handle_destroy(&h);
}

/* C4. Non-star references should also fail closed when a CTE column list does
 * not match the CTE projection width.
 */
static void test_sql_standard_cte_column_list_non_star_mismatch_rejected(void) {
  const char *sql = "WITH x(a, b) AS ("
                    "  SELECT p.name "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT x.a AS a "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS_MSG(&h, QIR_BINDERR_INVALID_CTE,
                        "column list does not match its projection");

  qir_handle_destroy(&h);
}

/* C5. SELECT * over a bound CTE expands into explicit derived column refs. */
static void test_sql_standard_star_in_cte(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT p.name AS name, p.region AS region "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT * "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->nctes == 1);

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "name");
  ASSERT_IDENT_EQ(&h.q->select_items[1]->out_alias, "region");
  ASSERT_COLREF(h.q->select_items[0]->value, "x", "name");
  ASSERT_COLREF(h.q->select_items[1]->value, "x", "region");
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "p", "region");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "x", "name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "x", "region");

  qir_handle_destroy(&h);
}

/* C6. Qualified star resolves against the current visible derived source. */
static void test_sql_standard_qualified_star_in_cte(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT p.name AS name, p.region AS region "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT x.* "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_COLREF(h.q->select_items[0]->value, "x", "name");
  ASSERT_COLREF(h.q->select_items[1]->value, "x", "region");

  qir_handle_destroy(&h);
}

/* C7. CTE star expansion fails closed when the derived source exposes unnamed
 * expressions.
 */
static void test_sql_standard_star_in_cte_rejected_for_unnamed_expr(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT upper(p.name) "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT * "
                    "FROM x AS x;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_STAR);

  qir_handle_destroy(&h);
}

/* C8. Subquery star expansion uses the subquery's exposed output names. */
static void test_sql_standard_star_in_subquery(void) {
  const char *sql = "SELECT * "
                    "FROM ("
                    "  SELECT p.name AS name, p.region AS region "
                    "  FROM private.people AS p"
                    ") AS s;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_COLREF(h.q->select_items[0]->value, "s", "name");
  ASSERT_COLREF(h.q->select_items[1]->value, "s", "region");

  qir_handle_destroy(&h);
}

/* C9. Subquery star expansion rejects unnamed output expressions. */
static void test_sql_standard_star_in_subquery_rejected_for_unnamed_expr(void) {
  const char *sql = "SELECT * "
                    "FROM ("
                    "  SELECT upper(p.name) "
                    "  FROM private.people AS p"
                    ") AS s;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_STAR);

  qir_handle_destroy(&h);
}

/* C10. VALUES with explicit column names can expand SELECT *. */
static void test_sql_standard_star_in_values(void) {
  const char *sql = "SELECT * "
                    "FROM (VALUES (1, 2)) AS v(a, b) "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_IDENT_EQ(&h.q->select_items[0]->out_alias, "a");
  ASSERT_IDENT_EQ(&h.q->select_items[1]->out_alias, "b");
  ASSERT_COLREF(h.q->select_items[0]->value, "v", "a");
  ASSERT_COLREF(h.q->select_items[1]->value, "v", "b");

  qir_handle_destroy(&h);
}

/* C11. VALUES without exposed column names keep SELECT * rejected. */
static void test_sql_standard_star_in_values_rejected_without_colnames(void) {
  const char *sql = "SELECT * "
                    "FROM (VALUES (1, 2)) AS v "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_STAR);

  qir_handle_destroy(&h);
}

/* C12. Base relations still do not allow SELECT *. */
static void test_sql_standard_star_in_base_rel_rejected(void) {
  const char *sql = "SELECT * "
                    "FROM private.people AS p;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_STAR);

  qir_handle_destroy(&h);
}

/* C13. Mixed derived/base sources keep plain star rejected. */
static void test_sql_standard_star_join_mixed_base_rejected(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT p.name AS name "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT * "
                    "FROM x AS x "
                    "INNER JOIN private.people AS p ON 1 = 1;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_STAR);

  qir_handle_destroy(&h);
}

/* C14. Plain star over multiple derived sources expands left-to-right in SQL
 * FROM/JOIN order.
 */
static void test_sql_standard_star_join_all_derived_accepted(void) {
  const char *sql = "WITH x AS ("
                    "  SELECT p.name AS name "
                    "  FROM private.people AS p"
                    "), "
                    "y AS ("
                    "  SELECT p.region AS region "
                    "  FROM private.people AS p"
                    ") "
                    "SELECT * "
                    "FROM x AS x "
                    "INNER JOIN ("
                    "  SELECT y.region AS region "
                    "  FROM y AS y"
                    ") AS s ON 1 = 1;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->nselect == 2);
  ASSERT_COLREF(h.q->select_items[0]->value, "x", "name");
  ASSERT_COLREF(h.q->select_items[1]->value, "s", "region");

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

  qir_handle_destroy(&h);
}

/* D6. ORDER BY with conflicting aliases fails during binding. */
static void test_sql_standard_order_by_alias_conflict(void) {
  const char *sql = "SELECT p.name AS nm, "
                    "       p.surname AS nm "
                    "FROM private.people AS p "
                    "ORDER BY nm;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->n_order_by == 1);
  ASSERT_COLREF(h.q->order_by[0], "", "nm");

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_AMBIGUOUS_COLREF);

  qir_handle_destroy(&h);
}

/* E1. Bare column binds when exactly one local range item is visible. */
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

  ASSERT_BIND_OK(&h);
  ASSERT_COLREF_BINDING(&h.q->select_items[0]->value->u.colref, h.q->from_root,
                        0);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "", "name");

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "email");

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "column");

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
  ASSERT_OP(h.q->where, QIR_OP_OTHER, "~~");

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
  ASSERT_OP(h.q->where, QIR_OP_OTHER, "!~~");

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
  ASSERT_OP(h.q->where, QIR_OP_OTHER, "BETWEEN");
  ASSERT_TRUE(h.q->where->u.op.nargs == 2);
  qir_handle_destroy(&h);

  const char *sql_not_between = "SELECT p.name AS name "
                                "FROM private.people AS p "
                                "WHERE p.age NOT BETWEEN 18 AND 30;";

  QirQueryHandle h2 = {0};
  parse_sql_postgres(sql_not_between, &h2);

  ASSERT_TRUE(h2.q != NULL);
  ASSERT_TRUE(h2.q->status == QIR_OK);
  ASSERT_TRUE(h2.q->where != NULL);
  ASSERT_OP(h2.q->where, QIR_OP_OTHER, "NOT BETWEEN");
  ASSERT_TRUE(h2.q->where->u.op.nargs == 2);
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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "age");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "name");

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "id");

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
  ASSERT_OP(h.q->having, QIR_OP_OTHER, ">");

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");

  qir_handle_destroy(&h);
}

/* E9. GROUP BY can resolve SELECT aliases inside larger expressions. */
static void test_sql_standard_group_by_alias_in_func(void) {
  const char *sql = "SELECT lower(u.fiscal_code) AS fc "
                    "FROM users u "
                    "GROUP BY upper(fc);";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->n_group_by == 1);
  ASSERT_TRUE(h.q->group_by[0] != NULL);
  ASSERT_TRUE(h.q->group_by[0]->kind == QIR_EXPR_FUNCALL);
  ASSERT_IDENT_EQ(&h.q->group_by[0]->u.funcall.name, "upper");
  ASSERT_TRUE(h.q->group_by[0]->u.funcall.nargs == 1);
  ASSERT_COLREF(h.q->group_by[0]->u.funcall.args[0], "", "fc");

  ASSERT_BIND_OK(&h);
  ASSERT_TRUE(h.q->group_by[0]->u.funcall.args[0] == h.q->select_items[0]->value);
  ASSERT_TRUE(h.q->group_by[0]->u.funcall.args[0]->kind == QIR_EXPR_FUNCALL);
  ASSERT_IDENT_EQ(&h.q->group_by[0]->u.funcall.args[0]->u.funcall.name, "lower");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "u", "fiscal_code");

  qir_handle_destroy(&h);
}

/* E10. Window function with only PARTITION BY. */
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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "p", "region");

  qir_handle_destroy(&h);
}

/* E11. Unresolved column references should fail during binding. */
static void test_sql_standard_unknown_touch(void) {
  const char *sql = "SELECT p.name AS name "
                    "FROM private.people AS p "
                    "WHERE name = 'x' OR z.id = 1;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_UNRESOLVED_COLREF);

  qir_handle_destroy(&h);
}

/* E12. Bare columns remain rejected in correlated nested scopes. */
static void test_sql_standard_correlated_bare_col_rejected(void) {
  const char *sql = "SELECT u.id "
                    "FROM users u "
                    "WHERE EXISTS ("
                    "  SELECT 1 "
                    "  FROM orders o "
                    "  WHERE name = 'Goku'"
                    ");";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  ASSERT_BIND_FAILS(&h, QIR_BINDERR_UNRESOLVED_COLREF);

  qir_handle_destroy(&h);
}

/* E13. VALUES in FROM. */
static void test_sql_standard_values_from_rejected(void) {
  const char *sql = "SELECT v.x AS x "
                    "FROM (VALUES (1), (2)) AS v(x) "
                    "LIMIT 10;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->kind == QIR_FROM_VALUES);
  ASSERT_IDENT_EQ(&h.q->from_root->alias, "v");
  ASSERT_TRUE(h.q->from_root->u.values.ncolnames == 1);
  ASSERT_IDENT_EQ(&h.q->from_root->u.values.colnames[0], "x");
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
  ASSERT_OP(lhs, QIR_OP_OTHER, "IS_NULL");
  ASSERT_OP(rhs, QIR_OP_OTHER, "IS_NOT_NULL");

  ASSERT_TRUE(lhs->u.op.nargs == 0);
  ASSERT_TRUE(lhs->u.op.args == NULL);
  ASSERT_TRUE(lhs->u.op.lhs->kind == QIR_EXPR_COLREF);
  ASSERT_IDENT_EQ(&lhs->u.op.lhs->u.colref.qualifier, "v");
  ASSERT_IDENT_EQ(&lhs->u.op.lhs->u.colref.column, "y");

  ASSERT_TRUE(rhs->u.op.nargs == 0);
  ASSERT_TRUE(rhs->u.op.args == NULL);
  ASSERT_TRUE(rhs->u.op.lhs->kind == QIR_EXPR_COLREF);
  ASSERT_IDENT_EQ(&rhs->u.op.lhs->u.colref.qualifier, "v");
  ASSERT_IDENT_EQ(&rhs->u.op.lhs->u.colref.column, "z");

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

  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "u", "id");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "u", "balance");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "e", "amount");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "e", "user_id");

  qir_handle_destroy(&h);
}

/* E14. Binary arithmetic operators (+, -, *, /) in SELECT and WHERE. */
static void test_sql_standard_binary_arithmetic(void) {
  const char *sql = "SELECT (t.a + t.b) AS sum_ab, "
                    "       (t.a * t.b) AS mul_ab "
                    "FROM tbl t "
                    "WHERE (t.a - t.b) > 0;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  // SELECT list: first item is (t.a + t.b)
  ASSERT_TRUE(h.q->nselect == 2);
  QirExpr *s0 = h.q->select_items[0]->value;
  ASSERT_OP(s0, QIR_OP_OTHER, "+");
  ASSERT_TRUE(s0->u.op.lhs != NULL);
  ASSERT_COLREF(s0->u.op.lhs, "t", "a");
  ASSERT_TRUE(s0->u.op.nargs == 1);
  ASSERT_COLREF(s0->u.op.args[0], "t", "b");

  // SELECT list: second item is (t.a * t.b)
  QirExpr *s1 = h.q->select_items[1]->value;
  ASSERT_OP(s1, QIR_OP_OTHER, "*");
  ASSERT_TRUE(s1->u.op.lhs != NULL);
  ASSERT_COLREF(s1->u.op.lhs, "t", "a");
  ASSERT_TRUE(s1->u.op.nargs == 1);
  ASSERT_COLREF(s1->u.op.args[0], "t", "b");

  // WHERE: (t.a - t.b) > 0  ->  outer op ">" with lhs = op "-"
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_OP(h.q->where, QIR_OP_OTHER, ">");
  QirExpr *sub = h.q->where->u.op.lhs;
  ASSERT_OP(sub, QIR_OP_OTHER, "-");
  ASSERT_TRUE(sub->u.op.lhs != NULL);
  ASSERT_COLREF(sub->u.op.lhs, "t", "a");
  ASSERT_TRUE(sub->u.op.nargs == 1);
  ASSERT_COLREF(sub->u.op.args[0], "t", "b");

  // Touches: all columns should be base touches on "t".
  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "t", "a");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "t", "b");

  qir_handle_destroy(&h);
}

/* E15. Unary minus in SELECT, WHERE, and inside a CTE. */
static void test_sql_standard_unary_minus(void) {
  const char *sql = "WITH neg AS ("
                    "  SELECT -v.x AS nx FROM vals v"
                    ") "
                    "SELECT -t.a AS neg_a "
                    "FROM tbl t "
                    "WHERE -t.b < 0;";

  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);

  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  // -- Main query SELECT: -t.a --
  ASSERT_TRUE(h.q->nselect == 1);
  QirExpr *s0 = h.q->select_items[0]->value;
  ASSERT_OP(s0, QIR_OP_OTHER, "-");
  ASSERT_TRUE(s0->u.op.lhs == NULL);
  ASSERT_TRUE(s0->u.op.nargs == 1);
  ASSERT_COLREF(s0->u.op.args[0], "t", "a");

  // -- Main query WHERE: -t.b < 0  ->  outer "<" with lhs = unary "-" --
  ASSERT_TRUE(h.q->where != NULL);
  ASSERT_OP(h.q->where, QIR_OP_OTHER, "<");
  QirExpr *wlhs = h.q->where->u.op.lhs;
  ASSERT_OP(wlhs, QIR_OP_OTHER, "-");
  ASSERT_TRUE(wlhs->u.op.lhs == NULL);
  ASSERT_TRUE(wlhs->u.op.nargs == 1);
  ASSERT_COLREF(wlhs->u.op.args[0], "t", "b");

  // -- CTE body: -v.x --
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_TRUE(h.q->ctes != NULL);
  QirQuery *cte = h.q->ctes[0]->query;
  ASSERT_TRUE(cte != NULL);
  ASSERT_TRUE(cte->status == QIR_OK);
  ASSERT_TRUE(cte->nselect == 1);
  QirExpr *cs0 = cte->select_items[0]->value;
  ASSERT_OP(cs0, QIR_OP_OTHER, "-");
  ASSERT_TRUE(cs0->u.op.lhs == NULL);
  ASSERT_TRUE(cs0->u.op.nargs == 1);
  ASSERT_COLREF(cs0->u.op.args[0], "v", "x");

  // -- Touch extraction: columns inside unary expressions must be found --
  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "t", "a");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "t", "b");
  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "v", "x");

  qir_handle_destroy(&h);
}

/* E16. Two-branch UNION ALL with touch extraction. */
static void test_sql_standard_union_all_two_branches(void) {
  const char *sql = "SELECT t.a AS col_a "
                    "FROM tbl t "
                    "WHERE t.a > 0 "
                    "UNION ALL "
                    "SELECT s.b AS col_b "
                    "FROM tbl2 s "
                    "WHERE s.b < 10;";
  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  // Father body comes from the first branch (SELECT t.a ...).
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_COLREF(h.q->select_items[0]->value, "t", "a");
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_IDENT_EQ(&h.q->from_root->alias, "t");
  ASSERT_TRUE(h.q->where != NULL);

  // Second branch linked via union_next.
  QirQuery *b2 = h.q->union_next;
  ASSERT_TRUE(b2 != NULL);
  ASSERT_TRUE(b2->status == QIR_OK);
  ASSERT_TRUE(b2->nselect == 1);
  ASSERT_COLREF(b2->select_items[0]->value, "s", "b");
  ASSERT_TRUE(b2->from_root != NULL);
  ASSERT_IDENT_EQ(&b2->from_root->alias, "s");
  ASSERT_TRUE(b2->where != NULL);
  ASSERT_TRUE(b2->union_next == NULL);

  // Father-only fields: children have defaults.
  ASSERT_TRUE(b2->nctes == 0);
  ASSERT_TRUE(b2->limit_value == -1);
  ASSERT_TRUE(b2->n_order_by == 0);

  // Touch extraction: both branches' columns are found.
  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "t", "a");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "s", "b");

  qir_handle_destroy(&h);
}

/* E17. Three-branch UNION ALL (verifies left-deep tree flattening). */
static void test_sql_standard_union_all_three_branches(void) {
  const char *sql = "SELECT t.x AS c1 FROM t1 t "
                    "UNION ALL "
                    "SELECT s.y AS c1 FROM t2 s "
                    "UNION ALL "
                    "SELECT r.z AS c1 FROM t3 r;";
  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  // Father = first branch.
  ASSERT_TRUE(h.q->nselect == 1);
  ASSERT_COLREF(h.q->select_items[0]->value, "t", "x");

  // Second branch.
  QirQuery *b2 = h.q->union_next;
  ASSERT_TRUE(b2 != NULL);
  ASSERT_TRUE(b2->nselect == 1);
  ASSERT_COLREF(b2->select_items[0]->value, "s", "y");

  // Third branch.
  QirQuery *b3 = b2->union_next;
  ASSERT_TRUE(b3 != NULL);
  ASSERT_TRUE(b3->nselect == 1);
  ASSERT_COLREF(b3->select_items[0]->value, "r", "z");
  ASSERT_TRUE(b3->union_next == NULL);

  // Touch extraction: all 3 branches.
  ASSERT_BIND_OK(&h);
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "t", "x");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "s", "y");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "r", "z");

  qir_handle_destroy(&h);
}

/* E18. UNION ALL with CTE: CTE refs resolved across union chain. */
static void test_sql_standard_union_all_with_cte(void) {
  const char *sql = "WITH vals AS ("
                    "  SELECT v.x AS vx FROM src v"
                    ") "
                    "SELECT a.vx AS col "
                    "FROM vals a "
                    "UNION ALL "
                    "SELECT b.y AS col "
                    "FROM tbl b;";
  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  // Father holds the CTE.
  ASSERT_TRUE(h.q->nctes == 1);
  ASSERT_IDENT_EQ(&h.q->ctes[0]->name, "vals");

  ASSERT_BIND_OK(&h);

  // First branch: FROM vals a stays BASE_REL syntactically and binds to the CTE.
  ASSERT_TRUE(h.q->from_root != NULL);
  ASSERT_TRUE(h.q->from_root->kind == QIR_FROM_BASE_REL);
  ASSERT_TRUE(h.q->from_root->binding_cte != NULL);
  ASSERT_IDENT_EQ(&h.q->from_root->binding_cte->name, "vals");

  // Second branch: FROM tbl b → BASE_REL.
  QirQuery *b2 = h.q->union_next;
  ASSERT_TRUE(b2 != NULL);
  ASSERT_TRUE(b2->from_root != NULL);
  ASSERT_TRUE(b2->from_root->kind == QIR_FROM_BASE_REL);
  ASSERT_IDENT_EQ(&b2->from_root->alias, "b");

  // Child has default metadata.
  ASSERT_TRUE(b2->nctes == 0);

  ASSERT_TOUCH(h.q, QIR_SCOPE_NESTED, QIR_TOUCH_BASE, "v", "x");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_DERIVED, "a", "vx");
  ASSERT_TOUCH(h.q, QIR_SCOPE_MAIN, QIR_TOUCH_BASE, "b", "y");

  qir_handle_destroy(&h);
}

/* E19. UNION ALL with ORDER BY and LIMIT on the outer query. */
static void test_sql_standard_union_all_order_limit(void) {
  const char *sql = "SELECT t.a AS col "
                    "FROM tbl t "
                    "UNION ALL "
                    "SELECT s.b AS col "
                    "FROM tbl2 s "
                    "ORDER BY col "
                    "LIMIT 100;";
  QirQueryHandle h = {0};
  parse_sql_postgres(sql, &h);
  ASSERT_TRUE(h.q != NULL);
  ASSERT_TRUE(h.q->status == QIR_OK);

  // ORDER BY and LIMIT on father.
  ASSERT_TRUE(h.q->n_order_by == 1);
  ASSERT_TRUE(h.q->limit_value == 100);

  // union_next exists.
  ASSERT_TRUE(h.q->union_next != NULL);
  // Child has no ORDER BY / LIMIT.
  ASSERT_TRUE(h.q->union_next->n_order_by == 0);
  ASSERT_TRUE(h.q->union_next->limit_value == -1);

  qir_handle_destroy(&h);
}

int main(void) {
  test_sql_standard_predicates_and_limit();
  test_sql_standard_multi_from_unsupported();
  test_sql_standard_in_list();
  test_sql_standard_or();
  test_sql_standard_not();
  test_sql_standard_order_by();
  test_sql_standard_order_by_alias();
  test_sql_standard_nested_order_by_alias_is_local();
  test_sql_standard_distinct();
  test_sql_standard_func_call();
  test_sql_standard_minmax_and_coalesce();
  test_sql_standard_join_inner();
  test_sql_standard_join_cross();
  test_sql_standard_join_on_cannot_see_successive_join();
  test_sql_standard_offset();
  test_sql_standard_semicolon_literal();
  test_sql_standard_in_empty_list();
  test_sql_standard_count_star();
  test_sql_standard_literal_select();
  test_sql_standard_ctes();
  test_sql_standard_cte_sensitive_col();
  test_sql_standard_cte_cannot_see_later_sibling();
  test_sql_standard_cte_cannot_see_itself();
  test_sql_standard_cte_ambiguous_lookup();
  test_sql_standard_subquery_from();
  test_sql_standard_from_subquery_cannot_see_outer_alias();
  test_sql_standard_subquery_where();
  test_sql_standard_exists();
  test_sql_standard_correlated_subquery_binding();
  test_sql_standard_in_subquery();
  test_sql_standard_not_in_subquery();
  test_sql_standard_nested_with_sees_outer_cte();
  test_sql_standard_nested_with_shadows_outer_cte();
  test_sql_standard_cte_column_list_parsed();
  test_sql_standard_cte_column_list_count_mismatch_preserved();
  test_sql_standard_star_in_cte_column_list_overrides_names();
  test_sql_standard_cte_column_list_non_star_mismatch_rejected();
  test_sql_standard_star_in_cte();
  test_sql_standard_qualified_star_in_cte();
  test_sql_standard_star_in_cte_rejected_for_unnamed_expr();
  test_sql_standard_star_in_subquery();
  test_sql_standard_star_in_subquery_rejected_for_unnamed_expr();
  test_sql_standard_star_in_values();
  test_sql_standard_star_in_values_rejected_without_colnames();
  test_sql_standard_star_in_base_rel_rejected();
  test_sql_standard_star_join_mixed_base_rejected();
  test_sql_standard_star_join_all_derived_accepted();
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
  test_sql_standard_group_by_alias_in_func();
  test_sql_standard_unknown_touch();
  test_sql_standard_correlated_bare_col_rejected();
  test_sql_standard_values_from_rejected();
  test_sql_standard_null_comparison();
  test_left_join_base_touches();
  test_sql_standard_binary_arithmetic();
  test_sql_standard_unary_minus();
  test_sql_standard_union_all_two_branches();
  test_sql_standard_union_all_three_branches();
  test_sql_standard_union_all_with_cte();
  test_sql_standard_union_all_order_limit();
  fprintf(stderr, "OK: test_query_ir_sql_standard\n");
  return 0;
}
