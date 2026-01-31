#include "validator.h"
#include "query_ir.h"
#include "vault.h"
#include "utils.h"

#include <string.h>

#define MAX_ROWS_SENS_ON 200

/* Resets 'old_msg' and writes 'new_msg' into 'old_msg'. */
static int set_err(StrBuf *old_msg, const char *new_msg) {
  if (!old_msg || !new_msg) return ERR;
  sb_reset(old_msg);
  return sb_append_bytes(old_msg, new_msg, strlen(new_msg));
}

/*---------------------------- QUERY VALIDATION -----------------------------*/
/* To validate a query we combine the QirTouchReport and a two-pass approach:
 *  - QirTouchReport used for:
 *      + All column references resolve to an alias (via QirTouchReport).
 *      + Sensitive columns can be referenced only inside the main query.
 *      + If at least one sensitive column is referenced, switches sensitive
 *        mode on. Vault should be opened, else, it fails.
 *
 *  - Pass A. It contains all the validations that are independent of the mode
 *    (sensitive mode on/off). Pass A checks that:
 *      + All tables have aliases (FROM/JOIN).
 *      + All functions are safe to call.
 *      + No SELECT * / alias.* (rejects q->has_star).
 *
 *  - Pass B. This runs only if sensitive mode is on and only if the Vault is
 *    open. It applies the same rules recursively to all nested queries (CTEs
 *    and subqueries) and checks that:
 *      + Sensitive columns are not referenced by casts or functions.
 *      + Sensitive columns appear only in SELECT (as simple colref) or WHERE.
 *      + Sensitive columns can only be compared using = or IN() and only to
 *        parameters.
 *      + WHERE must be a conjunction of predicates: `pred (AND pred)`. No `NOT`,
 *        no `OR`.
 *      + All JOINs must be INNER and use only = and AND; JOIN ON cannot
 *        reference sensitive columns. Only column references or literals can
 *        be used inside JOIN.
 *      + GROUP BY / HAVING / ORDER BY cannot reference sensitive columns.
 *      + DISTINCT and OFFSET are rejected in sensitive mode.
 *
 * Note: UNION/INTERSECT/EXCEPT are not modeled by the IR, so they are rejected
 * by the parser/IR layer before validation runs.
 */

typedef enum SensitiveLoc {
  SENS_LOC_SELECT = 1,
  SENS_LOC_WHERE,
  SENS_LOC_JOIN_ON,
  SENS_LOC_GROUP_BY,
  SENS_LOC_HAVING,
  SENS_LOC_ORDER_BY
} SensitiveLoc;

typedef struct SensitiveCtx {
  DbBackend *db;
  const ConnProfile *cp;
  const QirQuery *q;
  StrBuf *err;
} SensitiveCtx;

// PASS A START
static int validate_query_pass_a(DbBackend *db, const ConnProfile *cp,
                                 const QirQuery *q, StrBuf *err);

/* Finds and return a QirFromItem using 'alias' in the given query. Returns
 * NULL on no match. */
static inline const QirFromItem *find_from_alias(const QirQuery *q, const char *alias) {
  if (!q || !alias || alias[0] == '\0') return NULL;

  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items[i];
    if (!fi) continue;
    if (fi->alias.name && strcmp(fi->alias.name, alias) == 0) return fi;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins[i];
    if (!j || !j->rhs) continue;
    if (j->rhs->alias.name && strcmp(j->rhs->alias.name, alias) == 0) return j->rhs;
  }
  return NULL;
}

/* Returns YES if the colref resolves to a sensitive base table column, else,
 * NO or ERR. This is not suitable to understand if a colref contains data of a
 * sensitive column because we don't resolve the original table. However, since
 * we enforced that sensitive columns can only appear in the main SELECT, this
 * can be used to understand if a colref, of the main SELECT, contains sensitive
 * data. */
static inline int colref_is_sensitive(const QirQuery *q, const ConnProfile *cp,
                               const QirColRef *c) {
  if (!q || !cp || !c) return ERR;

  const QirFromItem *fi = find_from_alias(q, c->qualifier.name);
  if (!fi) return ERR;
  // We don't trace back the original relationship
  if (fi->kind != QIR_FROM_BASE_REL) return NO;

  const char *schema = fi->u.rel.schema.name;
  const char *table = fi->u.rel.name.name;
  const char *col = c->column.name;

  return connp_is_col_sensitive(cp, schema, table, col);
}

/* Validates that every QirFromItem and QirJoin has an alias. Returns YES, or 
 * NO/ERR and sets '*err'. */
static int validate_range_aliases(const QirQuery *q, StrBuf *err) {
  if (!q) return ERR;

  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items[i];
    if (!fi || !fi->alias.name || fi->alias.name[0] == '\0') {
      // TODO: log the table name too
      set_err(err, "missing alias in FROM item");
      return NO;
    }
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins[i];
    if (!j || !j->rhs || !j->rhs->alias.name || j->rhs->alias.name[0] == '\0') {
      // TODO: log the table name too
      set_err(err, "missing alias in JOIN item");
      return NO;
    }
  }
  return YES;
}

/* Validates all the subqueries embedded inside 'e'. Return YES/NO/ERR. Sets
 * '*err' if it doesn't return YES. */
typedef int (*ValidateQueryFn)(DbBackend *, const ConnProfile *, const QirQuery *, StrBuf *);

/* Walks an expression tree and validates all nested subqueries via the
 * callback provided by the caller. The callback controls the policy (Pass A
 * vs Pass B). */
static int validate_expr_subqueries(DbBackend *db, const ConnProfile *cp,
                                    const QirExpr *e, StrBuf *err,
                                    ValidateQueryFn validate_query_fn) {
  if (!e) return YES;

  // Any QirExpr may be a subquery so we recursively call this function on
  // all the QirExpr inside 'e'
  switch (e->kind) {
    case QIR_EXPR_SUBQUERY: {
      return validate_query_fn(db, cp, e->u.subquery, err);
    }
    case QIR_EXPR_FUNCALL: {
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.funcall.args[i], err,
                                          validate_query_fn);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CAST:
      return validate_expr_subqueries(db, cp, e->u.cast.expr, err,
                                      validate_query_fn);
    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_LIKE:
    case QIR_EXPR_NOT_LIKE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR: {
      int rc = validate_expr_subqueries(db, cp, e->u.bin.l, err, validate_query_fn);
      if (rc != YES) return rc;
      return validate_expr_subqueries(db, cp, e->u.bin.r, err, validate_query_fn);
    }
    case QIR_EXPR_NOT:
      return validate_expr_subqueries(db, cp, e->u.bin.l, err, validate_query_fn);
    case QIR_EXPR_IN: {
      int rc = validate_expr_subqueries(db, cp, e->u.in_.lhs, err,
                                        validate_query_fn);
      if (rc != YES) return rc;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        rc = validate_expr_subqueries(db, cp, e->u.in_.items[i], err,
                                      validate_query_fn);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CASE: {
      if (e->u.case_.arg) {
        int rc = validate_expr_subqueries(db, cp, e->u.case_.arg, err,
                                          validate_query_fn);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        int rc = validate_expr_subqueries(db, cp, w->when_expr, err,
                                          validate_query_fn);
        if (rc != YES) return rc;
        rc = validate_expr_subqueries(db, cp, w->then_expr, err,
                                      validate_query_fn);
        if (rc != YES) return rc;
      }
      if (e->u.case_.else_expr) {
        return validate_expr_subqueries(db, cp, e->u.case_.else_expr, err,
                                        validate_query_fn);
      }
      return YES;
    }
    case QIR_EXPR_WINDOWFUNC: {
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.window.func.args[i],
                                          err, validate_query_fn);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.window.partition_by[i],
                                          err, validate_query_fn);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.window.order_by[i],
                                          err, validate_query_fn);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_COLREF:
    case QIR_EXPR_PARAM:
    case QIR_EXPR_LITERAL:
      return YES;
    case QIR_EXPR_UNSUPPORTED:
      set_err(err, "unsupported expression");
      return NO;
  }
  return ERR;
}

static int validate_expr_subqueries_pass_a(DbBackend *db, const ConnProfile *cp,
                                           const QirExpr *e, StrBuf *err) {
  return validate_expr_subqueries(db, cp, e, err, validate_query_pass_a);
}

/* Returns YES if sensitive mode should be enabled based on the touch report.
 * We do not attempt schema resolution here; we only rely on the alias/col
 * mapping already produced by the touch extractor. */
static int should_enable_sensitive_mode(const QirQuery *q, const ConnProfile *cp,
                                        const QirTouchReport *tr) {
  if (!q || !cp || !tr) return ERR;
  if (tr->has_unknown_touches) return YES;

  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches[i];
    if (!t) continue;
    if (t->kind != QIR_TOUCH_BASE) continue;
    int rc = colref_is_sensitive(q, cp, &t->col);
    if (rc != NO) return rc;
  }
  return NO;
}

/* Returns YES if sensitive touches are only in allowed scopes.
 * Side effects: writes a human-readable reason into err on failure.
 * We reject any QIR_TOUCH_UNKNOWN because we cannot trust alias resolution. */
static int validate_sensitive_touches_scope(const QirTouchReport *tr,
                                            const ConnProfile *cp,
                                            const QirQuery *q,
                                            StrBuf *err) {
  if (!tr || !cp || !q) return ERR;

  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches[i];
    if (!t) continue;

    if (t->kind == QIR_TOUCH_UNKNOWN) {
      // Unknown touches mean alias resolution failed, so we cannot safely
      // reason about sensitive columns. Reject with a clear message.
      set_err(err, "unknown column reference");
      return NO;
    }

    if (t->kind != QIR_TOUCH_BASE) continue;
    int rc = colref_is_sensitive(q, cp, &t->col);
    if (rc == ERR) return ERR;
    if (rc == YES && t->scope != QIR_SCOPE_MAIN) {
      set_err(err, "sensitive columns only allowed in main query");
      return NO;
    }
  }
  return YES;
}

/* Validates that all function calls in an expression tree are safe to call.
 * Returns YES/NO/ERR and sets err when returning NO/ERR. */
static int validate_expr_functions(DbBackend *db, const ConnProfile *cp,
                                  const QirExpr *e, StrBuf *err) {
  if (!e) return YES;

  switch (e->kind) {
    case QIR_EXPR_FUNCALL: {
      StrBuf sb = {0};
      const char *name = NULL;
      if (e->u.funcall.schema.name && e->u.funcall.schema.name[0] != '\0') {
        if (sb_append_bytes(&sb, e->u.funcall.schema.name,
                            strlen(e->u.funcall.schema.name)) != OK ||
            sb_append_bytes(&sb, ".", 1) != OK ||
            sb_append_bytes(&sb, e->u.funcall.name.name,
                            strlen(e->u.funcall.name.name)) != OK ||
            sb_append_bytes(&sb, "", 1) != OK) {
          sb_clean(&sb);
          set_err(err, "unable to validate function call");
          return ERR;
        }
        name = sb.data;
      } else {
        name = e->u.funcall.name.name;
      }
      if (!name || name[0] == '\0') {
        sb_clean(&sb);
        set_err(err, "invalid function name");
        return NO;
      }
      int rc = db_is_function_safe(db, name);
      if (rc == ERR) {
        sb_clean(&sb);
        set_err(err, "unable to validate function call");
        return ERR;
      }
      if (rc == NO) {
        // did user explicitly said this function is safe to call?
        int urc = connp_is_func_safe(cp, e->u.funcall.schema.name, e->u.funcall.name.name);
        if (urc == ERR) {
          sb_clean(&sb);
          set_err(err, "unable to validate function call");
          return ERR;
        }
        if (urc != YES) {
          sb_clean(&sb);
          // TODO: log the function name
          set_err(err, "unsafe function call");
          return NO;
        }
      }
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        rc = validate_expr_functions(db, cp, e->u.funcall.args[i], err);
        if (rc != YES) return rc;
      }
      sb_clean(&sb);
      return YES;
    }
    case QIR_EXPR_WINDOWFUNC: {
      StrBuf sb = {0};
      const char *name = NULL;
      if (e->u.window.func.schema.name &&
          e->u.window.func.schema.name[0] != '\0') {
        if (sb_append_bytes(&sb, e->u.window.func.schema.name,
                            strlen(e->u.window.func.schema.name)) != OK ||
            sb_append_bytes(&sb, ".", 1) != OK ||
            sb_append_bytes(&sb, e->u.window.func.name.name,
                            strlen(e->u.window.func.name.name)) != OK ||
            sb_append_bytes(&sb, "", 1) != OK) {
          sb_clean(&sb);
          set_err(err, "unable to validate function call");
          return ERR;
        }
        name = sb.data;
      } else {
        name = e->u.window.func.name.name;
      }
      if (!name || name[0] == '\0') {
        sb_clean(&sb);
        set_err(err, "invalid window function name");
        return NO;
      }
      int rc = db_is_function_safe(db, name);
      if (rc == ERR) {
        sb_clean(&sb);
        set_err(err, "unable to validate function call");
        return ERR;
      }
      if (rc == NO) {
        // did user explicitly said this function is safe to call?
        int urc = connp_is_func_safe(cp, e->u.window.func.schema.name,
                                     e->u.window.func.name.name);
        if (urc == ERR) {
          sb_clean(&sb);
          set_err(err, "unable to validate function call");
          return ERR;
        }
        if (urc != YES) {
          sb_clean(&sb);
          // TODO: log the function call too
          set_err(err, "unsafe function call");
          return NO;
        }
      }
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        rc = validate_expr_functions(db, cp, e->u.window.func.args[i], err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        rc = validate_expr_functions(db, cp, e->u.window.partition_by[i], err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        rc = validate_expr_functions(db, cp, e->u.window.order_by[i], err);
        if (rc != YES) return rc;
      }
      sb_clean(&sb);
      return YES;
    }
    case QIR_EXPR_CAST:
      return validate_expr_functions(db, cp, e->u.cast.expr, err);
    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_LIKE:
    case QIR_EXPR_NOT_LIKE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR: {
      int rc = validate_expr_functions(db, cp, e->u.bin.l, err);
      if (rc != YES) return rc;
      return validate_expr_functions(db, cp, e->u.bin.r, err);
    }
    case QIR_EXPR_NOT:
      return validate_expr_functions(db, cp, e->u.bin.l, err);
    case QIR_EXPR_IN: {
      int rc = validate_expr_functions(db, cp, e->u.in_.lhs, err);
      if (rc != YES) return rc;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        rc = validate_expr_functions(db, cp, e->u.in_.items[i], err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CASE: {
      if (e->u.case_.arg) {
        int rc = validate_expr_functions(db, cp, e->u.case_.arg, err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        int rc = validate_expr_functions(db, cp, w->when_expr, err);
        if (rc != YES) return rc;
        rc = validate_expr_functions(db, cp, w->then_expr, err);
        if (rc != YES) return rc;
      }
      if (e->u.case_.else_expr) {
        return validate_expr_functions(db, cp, e->u.case_.else_expr, err);
      }
      return YES;
    }
    case QIR_EXPR_SUBQUERY:
    case QIR_EXPR_COLREF:
    case QIR_EXPR_PARAM:
    case QIR_EXPR_LITERAL:
      return YES;
    case QIR_EXPR_UNSUPPORTED:
      set_err(err, "unsupported expression");
      return NO;
  }
  return ERR;
}

/* Returns YES if the expression tree contains a sensitive column reference.
 * Subqueries are treated as separate scopes and do not contribute to this check. */
static int expr_has_sensitive(const QirQuery *q, const ConnProfile *cp,
                              const QirExpr *e) {
  if (!q || !cp || !e) return ERR;

  switch (e->kind) {
    case QIR_EXPR_COLREF:
      return colref_is_sensitive(q, cp, &e->u.colref);
    case QIR_EXPR_PARAM:
    case QIR_EXPR_LITERAL:
      return NO;
    case QIR_EXPR_SUBQUERY:
      // we ignore subqueries since this func is used in Pass B and we know that
      // if a query reaches Pass B it doesn't have sensitive columns inside
      // subqueries or CTEs
      return NO;
    case QIR_EXPR_FUNCALL: {
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        int rc = expr_has_sensitive(q, cp, e->u.funcall.args[i]);
        if (rc != NO) return rc;
      }
      return NO;
    }
    case QIR_EXPR_CAST:
      return expr_has_sensitive(q, cp, e->u.cast.expr);
    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_LIKE:
    case QIR_EXPR_NOT_LIKE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR: {
      int rc = expr_has_sensitive(q, cp, e->u.bin.l);
      if (rc != NO) return rc;
      return expr_has_sensitive(q, cp, e->u.bin.r);
    }
    case QIR_EXPR_NOT:
      return expr_has_sensitive(q, cp, e->u.bin.l);
    case QIR_EXPR_IN: {
      int rc = expr_has_sensitive(q, cp, e->u.in_.lhs);
      if (rc != NO) return rc;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        rc = expr_has_sensitive(q, cp, e->u.in_.items[i]);
        if (rc != NO) return rc;
      }
      return NO;
    }
    case QIR_EXPR_CASE: {
      if (e->u.case_.arg) {
        int rc = expr_has_sensitive(q, cp, e->u.case_.arg);
        if (rc != NO) return rc;
      }
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        int rc = expr_has_sensitive(q, cp, w->when_expr);
        if (rc != NO) return rc;
        rc = expr_has_sensitive(q, cp, w->then_expr);
        if (rc != NO) return rc;
      }
      if (e->u.case_.else_expr) {
        return expr_has_sensitive(q, cp, e->u.case_.else_expr);
      }
      return NO;
    }
    case QIR_EXPR_WINDOWFUNC: {
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        int rc = expr_has_sensitive(q, cp, e->u.window.func.args[i]);
        if (rc != NO) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        int rc = expr_has_sensitive(q, cp, e->u.window.partition_by[i]);
        if (rc != NO) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        int rc = expr_has_sensitive(q, cp, e->u.window.order_by[i]);
        if (rc != NO) return rc;
      }
      return NO;
    }
    case QIR_EXPR_UNSUPPORTED:
      return ERR;
  }
  return ERR;
}

/* Returns YES if the expression tree contains a parameter reference. */
static int expr_has_param(const QirExpr *e) {
  if (!e) return NO;

  switch (e->kind) {
    case QIR_EXPR_PARAM:
      return YES;
    case QIR_EXPR_COLREF:
    case QIR_EXPR_LITERAL:
      return NO;
    case QIR_EXPR_SUBQUERY:
      return NO;
    case QIR_EXPR_FUNCALL: {
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        int rc = expr_has_param(e->u.funcall.args[i]);
        if (rc != NO) return rc;
      }
      return NO;
    }
    case QIR_EXPR_CAST:
      return expr_has_param(e->u.cast.expr);
    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_LIKE:
    case QIR_EXPR_NOT_LIKE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR: {
      int rc = expr_has_param(e->u.bin.l);
      if (rc != NO) return rc;
      return expr_has_param(e->u.bin.r);
    }
    case QIR_EXPR_NOT:
      return expr_has_param(e->u.bin.l);
    case QIR_EXPR_IN: {
      int rc = expr_has_param(e->u.in_.lhs);
      if (rc != NO) return rc;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        rc = expr_has_param(e->u.in_.items[i]);
        if (rc != NO) return rc;
      }
      return NO;
    }
    case QIR_EXPR_CASE: {
      if (e->u.case_.arg) {
        int rc = expr_has_param(e->u.case_.arg);
        if (rc != NO) return rc;
      }
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        int rc = expr_has_param(w->when_expr);
        if (rc != NO) return rc;
        rc = expr_has_param(w->then_expr);
        if (rc != NO) return rc;
      }
      if (e->u.case_.else_expr) {
        return expr_has_param(e->u.case_.else_expr);
      }
      return NO;
    }
    case QIR_EXPR_WINDOWFUNC: {
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        int rc = expr_has_param(e->u.window.func.args[i]);
        if (rc != NO) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        int rc = expr_has_param(e->u.window.partition_by[i]);
        if (rc != NO) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        int rc = expr_has_param(e->u.window.order_by[i]);
        if (rc != NO) return rc;
      }
      return NO;
    }
    case QIR_EXPR_UNSUPPORTED:
      return ERR;
  }
  return ERR;
}

/* Validates that parameters are only used inside WHERE and only compared to
 * sensitive columns. This is enforced in Pass A to avoid data exfiltration. */
static int validate_params_where(const QirQuery *q, const ConnProfile *cp,
                                 const QirExpr *e, StrBuf *err) {
  if (!q || !cp || !e) return ERR;

  switch (e->kind) {
    case QIR_EXPR_AND: {
      int rc = validate_params_where(q, cp, e->u.bin.l, err);
      if (rc != YES) return rc;
      return validate_params_where(q, cp, e->u.bin.r, err);
    }
    case QIR_EXPR_EQ: {
      int left_param = (e->u.bin.l && e->u.bin.l->kind == QIR_EXPR_PARAM);
      if (left_param) {
        int sens_r = expr_has_sensitive(q, cp, e->u.bin.r);
        if (sens_r == ERR) return ERR;
        if (sens_r != YES){
          set_err(err, "parameters can only compare to sensitive columns");
          return NO;
        }
      }
      int right_param = (e->u.bin.r && e->u.bin.r->kind == QIR_EXPR_PARAM);
      if (right_param) {
        int sens_l = expr_has_sensitive(q, cp, e->u.bin.l);
        if (sens_l == ERR) return ERR;
        if (sens_l != YES){
          set_err(err, "parameters can only compare to sensitive columns");
          return NO;
        }
      }
      return YES;
    }
    case QIR_EXPR_IN: {
      if (!e->u.in_.lhs) return ERR;
      int sens_l = expr_has_sensitive(q, cp, e->u.in_.lhs);
      if (sens_l == ERR) return ERR;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        const QirExpr *it = e->u.in_.items[i];
        if (!it) continue;
        if (it->kind == QIR_EXPR_PARAM) {
          if (sens_l != YES) {
            set_err(err, "parameters can only compare to sensitive columns");
            return NO;
          }
        }
      }
      return YES;
    }
    case QIR_EXPR_OR:
    case QIR_EXPR_NOT:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_LIKE:
    case QIR_EXPR_NOT_LIKE: {
      if (expr_has_param(e) == YES) {
        set_err(err, "parameters are only allowed inside WHERE comparisons");
        return NO;
      }
      return YES;
    }
    case QIR_EXPR_PARAM:
      // we should not see a dangling param inside the WHERE
      set_err(err, "parameters are only allowed inside WHERE comparisons");
      return NO;
    case QIR_EXPR_SUBQUERY:
    case QIR_EXPR_LITERAL:
    case QIR_EXPR_COLREF:
    case QIR_EXPR_FUNCALL:
    case QIR_EXPR_CAST:
    case QIR_EXPR_CASE:
    case QIR_EXPR_WINDOWFUNC:
      if (expr_has_param(e) == YES) {
        set_err(err, "parameters are only allowed inside WHERE comparisons");
        return NO;
      }
      return YES;
    case QIR_EXPR_UNSUPPORTED:
      return ERR;
  }
  return ERR;
}

/* Pass A: validates alias requirements, function safety, and validates
 * all nested queries. This pass is mode-independent and is always required. */
static int validate_query_pass_a(DbBackend *db, const ConnProfile *cp,
                                 const QirQuery *q, StrBuf *err) {
  if (!db || !cp || !q) return ERR;

  // SELECT * is forbidden regardless of Sensitive Mode.
  if (q->has_star) {
    set_err(err, "SELECT * is not allowed");
    return NO;
  }

  // Even in nested queries, we require aliases for all range items. Without
  // this, a nested query could hide ambiguous references and we would miss it
  // because touch extraction only sees column references.
  int rc = validate_range_aliases(q, err);
  if (rc != YES) return rc;

  // Function allowlist enforcement across the query.
  // SELECT
  for (uint32_t i = 0; i < q->nselect; i++) {
    int rc = validate_expr_functions(db, cp, q->select_items[i]->value, err);
    if (rc != YES) return rc;
    if (expr_has_param(q->select_items[i]->value) == YES) {
      set_err(err, "parameters are only allowed inside WHERE");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(db, cp, q->select_items[i]->value, err);
    if (rc != YES) return rc;
  }
  // WHERE
  if (q->where) {
    int rc = validate_expr_functions(db, cp, q->where, err);
    if (rc != YES) return rc;
    rc = validate_params_where(q, cp, q->where, err);
    if (rc != YES) return rc;
    rc = validate_expr_subqueries_pass_a(db, cp, q->where, err);
    if (rc != YES) return rc;
  }
  // GROUP BY
  for (uint32_t i = 0; i < q->n_group_by; i++) {
    int rc = validate_expr_functions(db, cp, q->group_by[i], err);
    if (rc != YES) return rc;
    if (expr_has_param(q->group_by[i]) == YES) {
      set_err(err, "parameters are only allowed inside WHERE");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(db, cp, q->group_by[i], err);
    if (rc != YES) return rc;
  }
  // HAVING
  if (q->having) {
    int rc = validate_expr_functions(db, cp, q->having, err);
    if (rc != YES) return rc;
    if (expr_has_param(q->having) == YES) {
      set_err(err, "parameters are only allowed inside WHERE");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(db, cp, q->having, err);
    if (rc != YES) return rc;
  }
  // ORDER BY
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    int rc = validate_expr_functions(db, cp, q->order_by[i], err);
    if (rc != YES) return rc;
    if (expr_has_param(q->order_by[i]) == YES) {
      set_err(err, "parameters are only allowed inside WHERE");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(db, cp, q->order_by[i], err);
    if (rc != YES) return rc;
  }
  // JOIN
  for (uint32_t i = 0; i < q->njoins; i++) {
    int rc = validate_expr_functions(db, cp, q->joins[i]->on, err);
    if (rc != YES) return rc;
    if (expr_has_param(q->joins[i]->on) == YES) {
      set_err(err, "parameters are only allowed inside WHERE");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(db, cp, q->joins[i]->on, err);
    if (rc != YES) return rc;
  }

  // Recurse into nested queries
  for (uint32_t i = 0; i < q->nctes; i++) {
    rc = validate_query_pass_a(db, cp, q->ctes[i]->query, err);
    if (rc != YES) return rc;
  }
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items[i];
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      rc = validate_query_pass_a(db, cp, fi->u.subquery, err);
      if (rc != YES) return rc;
    }
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins[i]->rhs;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      rc = validate_query_pass_a(db, cp, fi->u.subquery, err);
      if (rc != YES) return rc;
    }
  }
  return YES;
}

// PASS B START

static int validate_query_pass_b(DbBackend *db, const ConnProfile *cp,
                                 const QirQuery *q, StrBuf *err);

static int validate_expr_subqueries_pass_b(DbBackend *db, const ConnProfile *cp,
                                           const QirExpr *e, StrBuf *err) {
  return validate_expr_subqueries(db, cp, e, err, validate_query_pass_b);
}

static inline bool expr_is_simple_operand(const QirExpr *e) {
  if (!e) return false;
  return (e->kind == QIR_EXPR_COLREF ||
          e->kind == QIR_EXPR_PARAM ||
          e->kind == QIR_EXPR_LITERAL);
}

/* Validates Sensitive Mode expression rules based on location.
 * Returns YES/NO/ERR and writes a human-friendly reason to ctx->err on NO.
 * Read the start of validator.c for doc. */
static int validate_sensitive_expr(const SensitiveCtx *ctx, const QirExpr *e,
                                   SensitiveLoc loc) {
  if (!ctx || !ctx->cp || !ctx->q || !e) return ERR;

  switch (loc) {
    case SENS_LOC_SELECT: {
      int sens = expr_has_sensitive(ctx->q, ctx->cp, e);
      if (sens == ERR) return ERR;
      if (sens == YES && e->kind != QIR_EXPR_COLREF) {
        set_err(ctx->err, "sensitive columns must be selected directly");
        return NO;
      }
      return YES;
    }

    case SENS_LOC_JOIN_ON: {
      switch (e->kind) {
        case QIR_EXPR_AND: {
          int rc = validate_sensitive_expr(ctx, e->u.bin.l, loc);
          if (rc != YES) return rc;
          return validate_sensitive_expr(ctx, e->u.bin.r, loc);
        }
        case QIR_EXPR_EQ: {
          if (!expr_is_simple_operand(e->u.bin.l) ||
              !expr_is_simple_operand(e->u.bin.r)) {
            set_err(ctx->err, "JOIN predicates must compare simple operands");
            return NO;
          }
          int sens_l = expr_has_sensitive(ctx->q, ctx->cp, e->u.bin.l);
          if (sens_l == ERR) return ERR;
          int sens_r = expr_has_sensitive(ctx->q, ctx->cp, e->u.bin.r);
          if (sens_r == ERR) return ERR;
          if (sens_l == YES || sens_r == YES) {
            set_err(ctx->err, "sensitive columns cannot be referenced in JOIN");
            return NO;
          }
          return YES;
        }
        default:
          set_err(ctx->err, "JOIN ON must be AND of '=' predicates");
          return NO;
      }
    }

    case SENS_LOC_WHERE: {
      switch (e->kind) {
        case QIR_EXPR_SUBQUERY:
          return validate_query_pass_b(ctx->db, ctx->cp, e->u.subquery, ctx->err);
        case QIR_EXPR_AND: {
          int rc = validate_sensitive_expr(ctx, e->u.bin.l, loc);
          if (rc != YES) return rc;
          return validate_sensitive_expr(ctx, e->u.bin.r, loc);
        }
        case QIR_EXPR_EQ: {
          int sens_l = expr_has_sensitive(ctx->q, ctx->cp, e->u.bin.l);
          if (sens_l == ERR) return ERR;
          int sens_r = expr_has_sensitive(ctx->q, ctx->cp, e->u.bin.r);
          if (sens_r == ERR) return ERR;

          if (sens_l == YES) {
            if (e->u.bin.l->kind != QIR_EXPR_COLREF) {
              set_err(ctx->err, "sensitive columns must be referenced directly in WHERE");
              return NO;
            }
            if (e->u.bin.r->kind != QIR_EXPR_PARAM) {
              set_err(ctx->err, "sensitive columns must compare only to parameters");
              return NO;
            }
          }
          if (sens_r == YES) {
            if (e->u.bin.r->kind != QIR_EXPR_COLREF) {
              set_err(ctx->err, "sensitive columns must be referenced directly in WHERE");
              return NO;
            }
            if (e->u.bin.l->kind != QIR_EXPR_PARAM) {
              set_err(ctx->err, "sensitive columns must compare only to parameters");
              return NO;
            }
          }
          return YES;
        }
        case QIR_EXPR_IN: {
          int sens_l = expr_has_sensitive(ctx->q, ctx->cp, e->u.in_.lhs);
          if (sens_l == ERR) return ERR;
          if ((sens_l == YES && e->u.in_.lhs->kind != QIR_EXPR_COLREF)) {
            set_err(ctx->err, "sensitive columns must be referenced directly in IN()");
            return NO;
          }

          // validate each item inside IN()
          for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
            const QirExpr *it = e->u.in_.items[i];
            if (!it) continue;
            if (sens_l == YES) {
              if (it->kind != QIR_EXPR_PARAM) {
                set_err(ctx->err, "sensitive columns must compare only to parameters");
                return NO;
              }
            }

            int sens_i = expr_has_sensitive(ctx->q, ctx->cp, it);
            if (sens_i == ERR) return ERR;
            if (sens_i == YES) {
              set_err(ctx->err, "sensitive columns cannot appear in IN list");
              return NO;
            }
          }
          return YES;
        }
        case QIR_EXPR_OR:
        case QIR_EXPR_NOT:
          set_err(ctx->err, "WHERE must be a conjunction of AND predicates");
          return NO;
        default:
          set_err(ctx->err, "unsupported WHERE predicate in sensitive mode");
          return NO;
      }
    }
    case SENS_LOC_GROUP_BY:
    case SENS_LOC_HAVING:
    case SENS_LOC_ORDER_BY: {
      int sens = expr_has_sensitive(ctx->q, ctx->cp, e);
      if (sens == ERR) return ERR;
      if (sens == YES) {
        if (loc == SENS_LOC_GROUP_BY) set_err(ctx->err, "GROUP BY cannot reference sensitive columns");
        if (loc == SENS_LOC_HAVING) set_err(ctx->err, "HAVING cannot reference sensitive columns");
        if (loc == SENS_LOC_ORDER_BY) set_err(ctx->err, "ORDER BY cannot reference sensitive columns");
        return NO;
      }
      return YES;
    }
  }
  return ERR;
}

/* Pass B: enforces Sensitive Mode rules on this query and all nested queries.
 * This pass should only be executed when Sensitive Mode is enabled. */
static int validate_query_pass_b(DbBackend *db, const ConnProfile *cp,
                                 const QirQuery *q, StrBuf *err) {
  if (!db || !cp || !q) return ERR;
  SensitiveCtx ctx = {.db = db, .cp = cp, .q = q, .err = err};

  if (q->has_star) {
    set_err(err, "SELECT * is not allowed");
    return NO;
  }
  if (q->has_distinct) {
    set_err(err, "DISTINCT is not allowed in sensitive mode");
    return NO;
  }
  if (q->has_offset) {
    set_err(err, "OFFSET is not allowed in sensitive mode");
    return NO;
  }
  if (q->limit_value < 0) {
    set_err(err, "LIMIT is required in sensitive mode");
    return NO;
  }
  if (q->limit_value > MAX_ROWS_SENS_ON) {
    set_err(err, "LIMIT exceeds sensitive mode maximum");
    return NO;
  }

  // JOIN
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j) continue;
    if (j->kind != QIR_JOIN_INNER) {
      set_err(err, "only INNER JOIN is allowed in sensitive mode");
      return NO;
    }
    if (j->on) {
      int rc = validate_sensitive_expr(&ctx, j->on, SENS_LOC_JOIN_ON);
      if (rc != YES) return rc;
    }
  }

  // WHERE
  if (q->where) {
    int rc = validate_sensitive_expr(&ctx, q->where, SENS_LOC_WHERE);
    if (rc != YES) return rc;
  }

  // SELECT
  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value) continue; // TODO: maybe this should return error because the QirQuery is malformatted.
    int rc = validate_sensitive_expr(&ctx, si->value, SENS_LOC_SELECT);
    if (rc != YES) return rc;
    rc = validate_expr_subqueries_pass_b(db, cp, si->value, err);
    if (rc != YES) return rc;
  }

  // GROUP BY
  for (uint32_t i = 0; i < q->n_group_by; i++) {
    QirExpr *e = q->group_by ? q->group_by[i] : NULL;
    if (!e) continue;
    int rc = validate_sensitive_expr(&ctx, e, SENS_LOC_GROUP_BY);
    if (rc != YES) return rc;
    rc = validate_expr_subqueries_pass_b(db, cp, e, err);
    if (rc != YES) return rc;
  }

  // HAVING
  if (q->having) {
    int rc = validate_sensitive_expr(&ctx, q->having, SENS_LOC_HAVING);
    if (rc != YES) return rc;
    rc = validate_expr_subqueries_pass_b(db, cp, q->having, err);
    if (rc != YES) return rc;
  }

  // ORDER BY
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    QirExpr *e = q->order_by ? q->order_by[i] : NULL;
    if (!e) continue;
    int rc = validate_sensitive_expr(&ctx, e, SENS_LOC_ORDER_BY);
    if (rc != YES) return rc;
    rc = validate_expr_subqueries_pass_b(db, cp, e, err);
    if (rc != YES) return rc;
  }

  // Recurse into nested queries to enforce the same Sensitive Mode rules.
  for (uint32_t i = 0; i < q->nctes; i++) {
    int rc = validate_query_pass_b(db, cp, q->ctes[i]->query, err);
    if (rc != YES) return rc;
  }
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items ? q->from_items[i] : NULL;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      int rc = validate_query_pass_b(db, cp, fi->u.subquery, err);
      if (rc != YES) return rc;
    }
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins ? q->joins[i]->rhs : NULL;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      int rc = validate_query_pass_b(db, cp, fi->u.subquery, err);
      if (rc != YES) return rc;
    }
  }

  return YES;
}

int validate_query(DbBackend *db, const ConnProfile *cp,
                   const SafetyPolicy *policy, const char *sql,
                   StrBuf *err_msg) {
  (void)policy; // read-only enforced at session level for now

  if (!db || !cp || !sql || !err_msg) return ERR;

  QirQueryHandle h = {0};
  if (db_make_query_ir(db, sql, &h) != OK) {
    set_err(err_msg, "failed to parse query");
    return ERR;
  }

  QirQuery *q = h.q;
  if (!q || q->status != QIR_OK) {
    const char *reason = (q && q->status_reason) ? q->status_reason : "invalid query";
    set_err(err_msg, reason);
    qir_handle_destroy(&h);
    return ERR;
  }

  QirTouchReport *tr = qir_extract_touches(q);
  if (!tr) {
    set_err(err_msg, "unable to analyze query");
    qir_handle_destroy(&h);
    return ERR;
  }
  if (tr->has_unsupported) {
    set_err(err_msg, "unsupported query structure");
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    return ERR;
  }

  // Touch report to decide if Sensitive Mode is needed
  int sensitive_mode = should_enable_sensitive_mode(q, cp, tr);
  if (sensitive_mode == ERR) {
    set_err(err_msg, "unable to analyze query");
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    return ERR;
  }
  if (sensitive_mode == YES && vault_is_opened() != YES) {
    set_err(err_msg, "vault is closed");
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    return ERR;
  }

  // Touch report to ensure sensitive touches never appear outside the main
  // query
  int rc = validate_sensitive_touches_scope(tr, cp, q, err_msg);
  if (rc != YES) {
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    return ERR;
  }

  // Pass A/Pass B design. Read the start of validator.c for doc.
  rc = validate_query_pass_a(db, cp, q, err_msg);
  if (rc != YES) {
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    return ERR;
  }

  if (sensitive_mode == YES) {
    rc = validate_query_pass_b(db, cp, q, err_msg);
    if (rc != YES) {
      qir_touch_report_destroy(tr);
      qir_handle_destroy(&h);
      return ERR;
    }
  }

  qir_touch_report_destroy(tr);
  qir_handle_destroy(&h);
  return OK;
}
