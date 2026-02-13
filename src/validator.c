#include "validator.h"
#include "log.h"
#include "query_ir.h"
#include "utils.h"

#include <stdarg.h>
#include <string.h>

#define MAX_ROWS_SENS_ON 200

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
 *      + WHERE must be a conjunction of predicates: `pred (AND pred)`. No
 * `NOT`, no `OR`.
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

typedef struct ValidatorCtx {
  DbBackend *db;
  const ConnProfile *cp;
  ValidatorErr *err;
  // Scratch buffer for building short diagnostic strings. The contents are
  // valid until the next scratch use.
  StrBuf scratch;
} ValidatorCtx;

#define MAX_ERR_MSG_LEN 512
/* Resets 'ctx->err->msg' and writes a string into it using 'fmt' like printf().
 */
static int set_err(ValidatorCtx *ctx, ValidatorErrCode code, const char *fmt,
                   ...) {
  va_list args;
  char buffer[MAX_ERR_MSG_LEN];
  int len;

  if (!ctx || !ctx->err || !fmt)
    return ERR;
  if (!ctx->err->msg)
    return ERR;

  ctx->err->code = code;
  sb_reset(ctx->err->msg);

  va_start(args, fmt);
  len = vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);

  if (len < 0)
    return ERR;

  /* Handle truncation warning */
  if (len >= (int)sizeof(buffer)) {
    len = sizeof(buffer) - 1;
  }

  return sb_append_bytes(ctx->err->msg, buffer, len);
}

// PASS A START
static int validate_query_pass_a(ValidatorCtx *ctx, const QirQuery *q);

/* Finds and return a QirFromItem using 'alias' in the given query. Returns
 * NULL on no match. */
static inline const QirFromItem *find_from_alias(const QirQuery *q,
                                                 const char *alias) {
  if (!q || !alias || alias[0] == '\0')
    return NULL;

  if (q->from_root && q->from_root->alias.name &&
      strcmp(q->from_root->alias.name, alias) == 0)
    return q->from_root;
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins[i];
    if (!j || !j->rhs)
      continue;
    if (j->rhs->alias.name && strcmp(j->rhs->alias.name, alias) == 0)
      return j->rhs;
  }
  return NULL;
}

/* Returns YES if the colref resolves to a sensitive base table column, else,
 * NO or ERR. This is not suitable to understand if a colref contains data of a
 * sensitive column because we don't resolve the original table. However, since
 * we enforced that sensitive columns can only appear in the main SELECT, this
 * can be used to understand if a colref, of the main SELECT, contains sensitive
 * data. This should be called only on columns of the main SELECT. */
static inline int colref_is_sensitive(const QirQuery *q, const ConnProfile *cp,
                                      const QirColRef *c) {
  if (!q || !cp || !c)
    return ERR;

  const QirFromItem *fi = find_from_alias(q, c->qualifier.name);
  if (!fi) {
    return ERR;
  }
  // We don't trace back the original relationship
  if (fi->kind != QIR_FROM_BASE_REL)
    return NO;

  const char *schema = fi->u.rel.schema.name;
  const char *table = fi->u.rel.name.name;
  const char *col = c->column.name;

  return connp_is_col_sensitive(cp, schema, table, col);
}

/* Validates that every QirFromItem and QirJoin has an alias. Returns YES, or
 * NO/ERR and sets '*err'. */
static int validate_range_aliases(ValidatorCtx *ctx, const QirQuery *q) {
  if (!q)
    return ERR;

  if (q->from_root) {
    if (!q->from_root->alias.name || q->from_root->alias.name[0] == '\0') {
      const char *desc = qir_from_to_str(q->from_root, &ctx->scratch);
      set_err(ctx, VERR_NO_TABLE_ALIAS, "Missing alias in FROM item: %s.",
              desc);
      return NO;
    }
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins[i];
    if (!j || !j->rhs || !j->rhs->alias.name || j->rhs->alias.name[0] == '\0') {
      const char *desc = qir_from_to_str(j ? j->rhs : NULL, &ctx->scratch);
      set_err(ctx, VERR_NO_TABLE_ALIAS, "Missing alias in JOIN item: %s.",
              desc);
      return NO;
    }
  }
  return YES;
}

/* Validates all the subqueries embedded inside 'e'. Return YES/NO/ERR. Sets
 * '*err' if it doesn't return YES. */
typedef int (*ValidateQueryFn)(ValidatorCtx *, const QirQuery *);

/* Walks an expression tree and validates all nested subqueries via the
 * callback provided by the caller. The callback controls the policy (Pass A
 * vs Pass B). */
static int validate_expr_subqueries(ValidatorCtx *ctx, const QirExpr *e,
                                    ValidateQueryFn validate_query_fn) {
  if (!e)
    return YES;

  // Any QirExpr may be a subquery so we recursively call this function on
  // all the QirExpr inside 'e'
  switch (e->kind) {
  case QIR_EXPR_SUBQUERY: {
    return validate_query_fn(ctx, e->u.subquery);
  }
  case QIR_EXPR_FUNCALL: {
    for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
      int rc = validate_expr_subqueries(ctx, e->u.funcall.args[i],
                                        validate_query_fn);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_CAST:
    return validate_expr_subqueries(ctx, e->u.cast.expr, validate_query_fn);
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
    int rc = validate_expr_subqueries(ctx, e->u.bin.l, validate_query_fn);
    if (rc != YES)
      return rc;
    return validate_expr_subqueries(ctx, e->u.bin.r, validate_query_fn);
  }
  case QIR_EXPR_NOT:
    return validate_expr_subqueries(ctx, e->u.bin.l, validate_query_fn);
  case QIR_EXPR_IN: {
    int rc = validate_expr_subqueries(ctx, e->u.in_.lhs, validate_query_fn);
    if (rc != YES)
      return rc;
    for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
      rc = validate_expr_subqueries(ctx, e->u.in_.items[i], validate_query_fn);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_CASE: {
    if (e->u.case_.arg) {
      int rc = validate_expr_subqueries(ctx, e->u.case_.arg, validate_query_fn);
      if (rc != YES)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens[i];
      if (!w) {
        set_err(ctx, VERR_ANALYZE_FAIL,
                "Invalid query structure (NULL CASE WHEN).");
        return ERR;
      }
      int rc = validate_expr_subqueries(ctx, w->when_expr, validate_query_fn);
      if (rc != YES)
        return rc;
      rc = validate_expr_subqueries(ctx, w->then_expr, validate_query_fn);
      if (rc != YES)
        return rc;
    }
    if (e->u.case_.else_expr) {
      return validate_expr_subqueries(ctx, e->u.case_.else_expr,
                                      validate_query_fn);
    }
    return YES;
  }
  case QIR_EXPR_WINDOWFUNC: {
    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      int rc = validate_expr_subqueries(ctx, e->u.window.func.args[i],
                                        validate_query_fn);
      if (rc != YES)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      int rc = validate_expr_subqueries(ctx, e->u.window.partition_by[i],
                                        validate_query_fn);
      if (rc != YES)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      int rc = validate_expr_subqueries(ctx, e->u.window.order_by[i],
                                        validate_query_fn);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_COLREF:
  case QIR_EXPR_PARAM:
  case QIR_EXPR_LITERAL:
    return YES;
  case QIR_EXPR_UNSUPPORTED:
    // TODO: we could store the char location of the unsupported expr and log it
    set_err(ctx, VERR_UNSUPPORTED_QUERY, "Unsupported expression.");
    return NO;
  }
  return ERR;
}

static int validate_expr_subqueries_pass_a(ValidatorCtx *ctx,
                                           const QirExpr *e) {
  return validate_expr_subqueries(ctx, e, validate_query_pass_a);
}

/* Returns YES if sensitive touches are only in allowed scopes and sets
 * 'found_sensitive' to true if it saw at least one sensitive column.
 *
 * This function is the single source of truth for enabling sensitive mode:
 * it scans all touches, rejects unknown aliases, and flips *found_sensitive
 * when a sensitive base column is referenced. Touches carry their source
 * query so alias resolution is scoped correctly.
 *
 * Side effects: writes a human-readable reason into err on failure. */
static int validate_sensitive_touches_scope(ValidatorCtx *ctx,
                                            const QirTouchReport *tr,
                                            bool *found_sensitive) {
  if (!tr || !ctx)
    return ERR;

  for (uint32_t i = 0; i < tr->ntouches; i++) {
    const QirTouch *t = tr->touches[i];
    if (!t)
      continue;

    if (t->kind == QIR_TOUCH_UNKNOWN) {
      // Unknown touches mean alias resolution failed, so we cannot safely
      // reason about sensitive columns. Reject with a clear message.
      const char *desc = qir_colref_to_str(&t->col, &ctx->scratch);
      set_err(ctx, VERR_NO_COLUMN_ALIAS,
              "Unknown column reference '%s'. Every table must have an alias, "
              "and every column must be qualified as alias.column.",
              desc);
      return NO;
    }

    // We skip touches of kind different from _BASE because its simpler to
    // resolte to the real, original table. It's safe because all the columns
    // inside the whole query have at least 1 _BASE touch. So, even if a
    // sensitive column is referenced in a subquery, it'll have one _BASE touch
    // with t->scope != QIR_SCOPE_MAIN and we'll reject it.
    if (t->kind != QIR_TOUCH_BASE)
      continue;

    int rc = colref_is_sensitive(t->source_query, ctx->cp, &t->col);
    if (rc == ERR)
      return ERR;
    if (rc == YES) {
      if (*found_sensitive != true)
        *found_sensitive = true;
      if (t->scope != QIR_SCOPE_MAIN) {
        const char *desc = qir_colref_to_str(&t->col, &ctx->scratch);
        set_err(ctx, VERR_SENSITIVE_OUTSIDE_MAIN,
                "Column '%s' is sensitive, so it's only allowed in main query.",
                desc);
        return NO;
      }
    }
  }
  return YES;
}

static int name_cpm(const void *s1, const void *s2) {
  const char *key = s1;
  const char *const *elem = s2;
  return strcmp(key, *elem);
}

/* Returns YES if a function is safe to call for 'db' or is present in the
 * user-defined list of 'cp'. Returns YES/NO/ERR. */
static int validator_is_function_safe(ValidatorCtx *ctx, const char *schema,
                                      const char *name) {
  if (!ctx || !ctx->db || !name || name[0] == '\0')
    return ERR;
  const DbSafeFuncList *list = db_safe_functions(ctx->db);

  if (list && list->names && list->count > 0) {
    // backend list is always global
    char *safe_name =
        bsearch(name, list->names, list->count, sizeof(char *), name_cpm);
    if (safe_name)
      return YES;
  }

  return connp_is_func_safe(ctx->cp, schema, name);
}

/* Validates that all function calls in an expression tree are safe to call.
 * Returns YES/NO/ERR and sets err when returning NO/ERR. */
static int validate_expr_functions(ValidatorCtx *ctx, const QirExpr *e) {
  if (!e)
    return YES;

  switch (e->kind) {
  case QIR_EXPR_FUNCALL: {
    const char *name = e->u.funcall.name.name;
    if (!name || name[0] == '\0') {
      const char *desc = qir_func_to_str(&e->u.funcall, &ctx->scratch);
      set_err(ctx, VERR_FUNC_UNSAFE, "Invalid function name: %s.", desc);
      return NO;
    }
    int rc = validator_is_function_safe(ctx, e->u.funcall.schema.name, name);
    if (rc == ERR) {
      const char *desc = qir_func_to_str(&e->u.funcall, &ctx->scratch);
      set_err(ctx, VERR_ANALYZE_FAIL, "Unable to validate function call: %s.",
              desc);
      return ERR;
    }
    if (rc == NO) {
      const char *desc = qir_func_to_str(&e->u.funcall, &ctx->scratch);
      set_err(ctx, VERR_FUNC_UNSAFE, "Unsafe function call: %s.", desc);
      return NO;
    }

    for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
      rc = validate_expr_functions(ctx, e->u.funcall.args[i]);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_WINDOWFUNC: {
    const char *name = e->u.window.func.name.name;
    if (!name || name[0] == '\0') {
      const char *desc = qir_func_to_str(&e->u.window.func, &ctx->scratch);
      set_err(ctx, VERR_FUNC_UNSAFE, "Invalid window function name: %s.", desc);
      return NO;
    }
    int rc =
        validator_is_function_safe(ctx, e->u.window.func.schema.name, name);
    if (rc == ERR) {
      const char *desc = qir_func_to_str(&e->u.window.func, &ctx->scratch);
      set_err(ctx, VERR_ANALYZE_FAIL, "Unable to validate function call: %s.",
              desc);
      return ERR;
    }
    if (rc == NO) {
      const char *desc = qir_func_to_str(&e->u.window.func, &ctx->scratch);
      set_err(ctx, VERR_FUNC_UNSAFE, "Unsafe function call: %s.", desc);
      return NO;
    }

    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      rc = validate_expr_functions(ctx, e->u.window.func.args[i]);
      if (rc != YES)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      rc = validate_expr_functions(ctx, e->u.window.partition_by[i]);
      if (rc != YES)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      rc = validate_expr_functions(ctx, e->u.window.order_by[i]);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_CAST:
    return validate_expr_functions(ctx, e->u.cast.expr);
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
    int rc = validate_expr_functions(ctx, e->u.bin.l);
    if (rc != YES)
      return rc;
    return validate_expr_functions(ctx, e->u.bin.r);
  }
  case QIR_EXPR_NOT:
    return validate_expr_functions(ctx, e->u.bin.l);
  case QIR_EXPR_IN: {
    int rc = validate_expr_functions(ctx, e->u.in_.lhs);
    if (rc != YES)
      return rc;
    for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
      rc = validate_expr_functions(ctx, e->u.in_.items[i]);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_CASE: {
    if (e->u.case_.arg) {
      int rc = validate_expr_functions(ctx, e->u.case_.arg);
      if (rc != YES)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens[i];
      if (!w) {
        set_err(ctx, VERR_ANALYZE_FAIL,
                "Invalid query structure (NULL CASE WHEN).");
        return ERR;
      }
      int rc = validate_expr_functions(ctx, w->when_expr);
      if (rc != YES)
        return rc;
      rc = validate_expr_functions(ctx, w->then_expr);
      if (rc != YES)
        return rc;
    }
    if (e->u.case_.else_expr) {
      return validate_expr_functions(ctx, e->u.case_.else_expr);
    }
    return YES;
  }
  case QIR_EXPR_SUBQUERY:
  case QIR_EXPR_COLREF:
  case QIR_EXPR_PARAM:
  case QIR_EXPR_LITERAL:
    return YES;
  case QIR_EXPR_UNSUPPORTED:
    set_err(ctx, VERR_UNSUPPORTED_QUERY, "Unsupported expression.");
    return NO;
  }
  return ERR;
}

/* Returns YES if the expression tree contains a sensitive column reference.
 * Subqueries are treated as separate scopes and do not contribute to this
 * check. */
static int expr_has_sensitive(const QirQuery *q, const ConnProfile *cp,
                              const QirExpr *e) {
  if (!q || !cp || !e)
    return ERR;

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
      if (rc != NO)
        return rc;
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
    if (rc != NO)
      return rc;
    return expr_has_sensitive(q, cp, e->u.bin.r);
  }
  case QIR_EXPR_NOT:
    return expr_has_sensitive(q, cp, e->u.bin.l);
  case QIR_EXPR_IN: {
    int rc = expr_has_sensitive(q, cp, e->u.in_.lhs);
    if (rc != NO)
      return rc;
    for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
      rc = expr_has_sensitive(q, cp, e->u.in_.items[i]);
      if (rc != NO)
        return rc;
    }
    return NO;
  }
  case QIR_EXPR_CASE: {
    if (e->u.case_.arg) {
      int rc = expr_has_sensitive(q, cp, e->u.case_.arg);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens[i];
      if (!w)
        return ERR;
      int rc = expr_has_sensitive(q, cp, w->when_expr);
      if (rc != NO)
        return rc;
      rc = expr_has_sensitive(q, cp, w->then_expr);
      if (rc != NO)
        return rc;
    }
    if (e->u.case_.else_expr) {
      return expr_has_sensitive(q, cp, e->u.case_.else_expr);
    }
    return NO;
  }
  case QIR_EXPR_WINDOWFUNC: {
    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      int rc = expr_has_sensitive(q, cp, e->u.window.func.args[i]);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      int rc = expr_has_sensitive(q, cp, e->u.window.partition_by[i]);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      int rc = expr_has_sensitive(q, cp, e->u.window.order_by[i]);
      if (rc != NO)
        return rc;
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
  if (!e)
    return NO;

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
      if (rc != NO)
        return rc;
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
    if (rc != NO)
      return rc;
    return expr_has_param(e->u.bin.r);
  }
  case QIR_EXPR_NOT:
    return expr_has_param(e->u.bin.l);
  case QIR_EXPR_IN: {
    int rc = expr_has_param(e->u.in_.lhs);
    if (rc != NO)
      return rc;
    for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
      rc = expr_has_param(e->u.in_.items[i]);
      if (rc != NO)
        return rc;
    }
    return NO;
  }
  case QIR_EXPR_CASE: {
    if (e->u.case_.arg) {
      int rc = expr_has_param(e->u.case_.arg);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens[i];
      if (!w)
        return ERR;
      int rc = expr_has_param(w->when_expr);
      if (rc != NO)
        return rc;
      rc = expr_has_param(w->then_expr);
      if (rc != NO)
        return rc;
    }
    if (e->u.case_.else_expr) {
      return expr_has_param(e->u.case_.else_expr);
    }
    return NO;
  }
  case QIR_EXPR_WINDOWFUNC: {
    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      int rc = expr_has_param(e->u.window.func.args[i]);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      int rc = expr_has_param(e->u.window.partition_by[i]);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      int rc = expr_has_param(e->u.window.order_by[i]);
      if (rc != NO)
        return rc;
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
static int validate_params_where(ValidatorCtx *ctx, const QirQuery *q,
                                 const QirExpr *e) {
  if (!q || !ctx || !e)
    return ERR;

  switch (e->kind) {
  case QIR_EXPR_AND: {
    int rc = validate_params_where(ctx, q, e->u.bin.l);
    if (rc != YES)
      return rc;
    return validate_params_where(ctx, q, e->u.bin.r);
  }
  case QIR_EXPR_EQ: {
    int left_param = (e->u.bin.l && e->u.bin.l->kind == QIR_EXPR_PARAM);
    if (left_param) {
      // We don't check whether the sensitive column is a base reference
      // because it's not the responsibility of this function
      int sens_r = expr_has_sensitive(q, ctx->cp, e->u.bin.r);
      if (sens_r == ERR)
        return ERR;
      if (sens_r != YES) {
        set_err(ctx, VERR_PARAM_NON_SENSITIVE,
                "Parameters can only compare to sensitive columns.");
        return NO;
      }
    }
    int right_param = (e->u.bin.r && e->u.bin.r->kind == QIR_EXPR_PARAM);
    if (right_param) {
      int sens_l = expr_has_sensitive(q, ctx->cp, e->u.bin.l);
      if (sens_l == ERR)
        return ERR;
      if (sens_l != YES) {
        set_err(ctx, VERR_PARAM_NON_SENSITIVE,
                "Parameters can only compare to sensitive columns.");
        return NO;
      }
    }
    return YES;
  }
  case QIR_EXPR_IN: {
    if (!e->u.in_.lhs)
      return ERR;
    int sens_l = expr_has_sensitive(q, ctx->cp, e->u.in_.lhs);
    if (sens_l == ERR)
      return ERR;
    for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
      const QirExpr *it = e->u.in_.items[i];
      if (!it) {
        set_err(ctx, VERR_ANALYZE_FAIL,
                "Invalid query structure (NULL IN item).");
        return ERR;
      }
      if (it->kind == QIR_EXPR_PARAM) {
        if (sens_l != YES) {
          set_err(
              ctx, VERR_PARAM_NON_SENSITIVE,
              "Parameters inside IN() can only compare to sensitive columns.");
          return NO;
        }
      }
    }
    return YES;
  }
  case QIR_EXPR_OR:
  case QIR_EXPR_NOT: {
    // Allow parameters inside WHERE boolean structure; Pass B will enforce
    // sensitive-mode restrictions (AND-only, etc).
    if (e->u.bin.r) {
      int rc = validate_params_where(ctx, q, e->u.bin.r);
      if (rc != YES)
        return rc;
    }
    return validate_params_where(ctx, q, e->u.bin.l);
  }
  case QIR_EXPR_NE:
  case QIR_EXPR_GT:
  case QIR_EXPR_GE:
  case QIR_EXPR_LT:
  case QIR_EXPR_LE:
  case QIR_EXPR_LIKE:
  case QIR_EXPR_NOT_LIKE: {
    // Comparisons are valid WHERE contexts for parameters in Pass A.
    // Sensitive-mode constraints are enforced later in Pass B.
    return YES;
  }
  case QIR_EXPR_PARAM:
    // we should not see a dangling param inside the WHERE
    set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
            "Parameters are only allowed inside WHERE comparisons.");
    return NO;
  case QIR_EXPR_SUBQUERY:
  case QIR_EXPR_LITERAL:
  case QIR_EXPR_COLREF:
  case QIR_EXPR_FUNCALL:
  case QIR_EXPR_CAST:
  case QIR_EXPR_CASE:
  case QIR_EXPR_WINDOWFUNC:
    if (expr_has_param(e) == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed inside WHERE comparisons,");
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
static int validate_query_pass_a(ValidatorCtx *ctx, const QirQuery *q) {
  if (!ctx || !ctx->db || !ctx->cp || !q)
    return ERR;

  // SELECT * is forbidden regardless of Sensitive Mode.
  if (q->has_star) {
    set_err(ctx, VERR_STAR, "SELECT * is not allowed.");
    return NO;
  }

  // Even in nested queries, we require aliases for all range items. Without
  // this, a nested query could hide ambiguous references and we would miss it
  // because touch extraction only sees column references.
  int rc = validate_range_aliases(ctx, q);
  if (rc != YES)
    return rc;

  // Function allowlist enforcement across the query.
  // SELECT
  for (uint32_t i = 0; i < q->nselect; i++) {
    int rc = validate_expr_functions(ctx, q->select_items[i]->value);
    if (rc != YES)
      return rc;
    if (expr_has_param(q->select_items[i]->value) == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed inside WHERE.");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(ctx, q->select_items[i]->value);
    if (rc != YES)
      return rc;
  }
  // WHERE
  if (q->where) {
    int rc = validate_expr_functions(ctx, q->where);
    if (rc != YES)
      return rc;
    rc = validate_params_where(ctx, q, q->where);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_a(ctx, q->where);
    if (rc != YES)
      return rc;
  }
  // GROUP BY
  for (uint32_t i = 0; i < q->n_group_by; i++) {
    int rc = validate_expr_functions(ctx, q->group_by[i]);
    if (rc != YES)
      return rc;
    if (expr_has_param(q->group_by[i]) == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed inside WHERE.");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(ctx, q->group_by[i]);
    if (rc != YES)
      return rc;
  }
  // HAVING
  if (q->having) {
    int rc = validate_expr_functions(ctx, q->having);
    if (rc != YES)
      return rc;
    if (expr_has_param(q->having) == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed inside WHERE.");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(ctx, q->having);
    if (rc != YES)
      return rc;
  }
  // ORDER BY
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    int rc = validate_expr_functions(ctx, q->order_by[i]);
    if (rc != YES)
      return rc;
    if (expr_has_param(q->order_by[i]) == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed inside WHERE.");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(ctx, q->order_by[i]);
    if (rc != YES)
      return rc;
  }
  // JOIN
  for (uint32_t i = 0; i < q->njoins; i++) {
    int rc = validate_expr_functions(ctx, q->joins[i]->on);
    if (rc != YES)
      return rc;
    if (expr_has_param(q->joins[i]->on) == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed inside WHERE.");
      return NO;
    }
    rc = validate_expr_subqueries_pass_a(ctx, q->joins[i]->on);
    if (rc != YES)
      return rc;
  }

  // Recurse into nested queries
  for (uint32_t i = 0; i < q->nctes; i++) {
    rc = validate_query_pass_a(ctx, q->ctes[i]->query);
    if (rc != YES)
      return rc;
  }
  if (q->from_root && q->from_root->kind == QIR_FROM_SUBQUERY) {
    rc = validate_query_pass_a(ctx, q->from_root->u.subquery);
    if (rc != YES)
      return rc;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins[i]->rhs;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      rc = validate_query_pass_a(ctx, fi->u.subquery);
      if (rc != YES)
        return rc;
    }
  }
  return YES;
}

// PASS B START

static int validate_query_pass_b(ValidatorCtx *ctx, const QirQuery *q);

static int validate_expr_subqueries_pass_b(ValidatorCtx *ctx,
                                           const QirExpr *e) {
  return validate_expr_subqueries(ctx, e, validate_query_pass_b);
}

static inline bool expr_is_simple_operand(const QirExpr *e) {
  if (!e)
    return false;
  return (e->kind == QIR_EXPR_COLREF || e->kind == QIR_EXPR_LITERAL);
}

/* Validates Sensitive Mode expression rules based on location.
 * Returns YES/NO/ERR and writes a human-friendly reason to ctx->err on NO.
 * Read the start of validator.c for doc. */
static int validate_sensitive_expr(ValidatorCtx *ctx, const QirQuery *main_q,
                                   const QirExpr *e, SensitiveLoc loc) {
  if (!ctx || !main_q || !e)
    return ERR;

  switch (loc) {
  case SENS_LOC_SELECT: {
    int sens = expr_has_sensitive(main_q, ctx->cp, e);
    if (sens == ERR)
      return ERR;
    if (sens == YES && e->kind != QIR_EXPR_COLREF) {
      set_err(ctx, VERR_SENSITIVE_SELECT_EXPR,
              "Sensitive columns must be selected directly.");
      return NO;
    }
    return YES;
  }

  case SENS_LOC_JOIN_ON: {
    switch (e->kind) {
    case QIR_EXPR_AND: {
      int rc = validate_sensitive_expr(ctx, main_q, e->u.bin.l, loc);
      if (rc != YES)
        return rc;
      return validate_sensitive_expr(ctx, main_q, e->u.bin.r, loc);
    }
    case QIR_EXPR_EQ: {
      if (!expr_is_simple_operand(e->u.bin.l) ||
          !expr_is_simple_operand(e->u.bin.r)) {
        set_err(
            ctx, VERR_JOIN_ON_INVALID,
            "JOIN predicates must compare simple operands in sensitive mode.");
        return NO;
      }
      int sens_l = expr_has_sensitive(main_q, ctx->cp, e->u.bin.l);
      if (sens_l == ERR)
        return ERR;
      int sens_r = expr_has_sensitive(main_q, ctx->cp, e->u.bin.r);
      if (sens_r == ERR)
        return ERR;
      if (sens_l == YES || sens_r == YES) {
        const char *desc_l =
            qir_colref_to_str(&e->u.bin.l->u.colref, &ctx->scratch);
        const char *desc_r =
            qir_colref_to_str(&e->u.bin.r->u.colref, &ctx->scratch);
        set_err(ctx, VERR_JOIN_ON_SENSITIVE,
                "JOIN predicate references sensitive column ('%s' or '%s'), "
                "which is not allowed.",
                desc_l, desc_r);
        return NO;
      }
      return YES;
    }
    default:
      set_err(ctx, VERR_JOIN_ON_INVALID,
              "JOIN ON must be AND of '=' predicates");
      return NO;
    }
  }

  case SENS_LOC_WHERE: {
    switch (e->kind) {
    case QIR_EXPR_SUBQUERY: {
      return validate_query_pass_b(ctx, e->u.subquery);
    }
    case QIR_EXPR_AND: {
      int rc = validate_sensitive_expr(ctx, main_q, e->u.bin.l, loc);
      if (rc != YES)
        return rc;
      return validate_sensitive_expr(ctx, main_q, e->u.bin.r, loc);
    }
    case QIR_EXPR_EQ: {
      int sens_l = expr_has_sensitive(main_q, ctx->cp, e->u.bin.l);
      if (sens_l == ERR)
        return ERR;
      int sens_r = expr_has_sensitive(main_q, ctx->cp, e->u.bin.r);
      if (sens_r == ERR)
        return ERR;

      if (sens_l == YES) {
        if (e->u.bin.l->kind != QIR_EXPR_COLREF) {
          const char *desc =
              qir_colref_to_str(&e->u.bin.l->u.colref, &ctx->scratch);
          set_err(ctx, VERR_SENSITIVE_LOC,
                  "Sensitive column '%s' must be referenced directly in WHERE.",
                  desc);
          return NO;
        }
        if (e->u.bin.r->kind != QIR_EXPR_PARAM) {
          const char *desc =
              qir_colref_to_str(&e->u.bin.l->u.colref, &ctx->scratch);
          set_err(ctx, VERR_SENSITIVE_CMP,
                  "Sensitive column '%s' must compare only to parameters.",
                  desc);
          return NO;
        }
      }
      if (sens_r == YES) {
        if (e->u.bin.r->kind != QIR_EXPR_COLREF) {
          const char *desc =
              qir_colref_to_str(&e->u.bin.r->u.colref, &ctx->scratch);
          set_err(ctx, VERR_SENSITIVE_LOC,
                  "Sensitive column '%s' must be referenced directly in WHERE.",
                  desc);
          return NO;
        }
        if (e->u.bin.l->kind != QIR_EXPR_PARAM) {
          const char *desc =
              qir_colref_to_str(&e->u.bin.r->u.colref, &ctx->scratch);
          set_err(ctx, VERR_SENSITIVE_CMP,
                  "Sensitive column '%s' must compare only to parameters.",
                  desc);
          return NO;
        }
      }
      return YES;
    }
    case QIR_EXPR_IN: {
      int sens_l = expr_has_sensitive(main_q, ctx->cp, e->u.in_.lhs);
      if (sens_l == ERR)
        return ERR;
      if ((sens_l == YES && e->u.in_.lhs->kind != QIR_EXPR_COLREF)) {
        const char *desc =
            qir_colref_to_str(&e->u.in_.lhs->u.colref, &ctx->scratch);
        set_err(ctx, VERR_SENSITIVE_LOC,
                "Sensitive column '%s' must be referenced directly in IN().",
                desc);
        return NO;
      }

      // validate each item inside IN()
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        const QirExpr *it = e->u.in_.items[i];
        if (!it) {
          set_err(ctx, VERR_ANALYZE_FAIL,
                  "Invalid query structure (NULL IN item).");
          return ERR;
        }
        if (sens_l == YES) {
          if (it->kind != QIR_EXPR_PARAM) {
            const char *desc =
                qir_colref_to_str(&e->u.in_.lhs->u.colref, &ctx->scratch);
            set_err(ctx, VERR_SENSITIVE_CMP,
                    "Sensitive column '%s' must compare only to parameters.",
                    desc);
            return NO;
          }
        }

        int sens_i = expr_has_sensitive(main_q, ctx->cp, it);
        if (sens_i == ERR)
          return ERR;
        if (sens_i == YES) {
          const char *desc =
              qir_colref_to_str(&e->u.in_.lhs->u.colref, &ctx->scratch);
          set_err(ctx, VERR_SENSITIVE_CMP,
                  "Sensitive column '%s' cannot appear in IN list.", desc);
          return NO;
        }
      }
      return YES;
    }
    case QIR_EXPR_OR:
    case QIR_EXPR_NOT:
      set_err(ctx, VERR_WHERE_NOT_CONJ,
              "WHERE must be a conjunction of AND predicates if a sensitive "
              "column is referenced.");
      return NO;
    default:
      set_err(ctx, VERR_SENSITIVE_CMP,
              "Unsupported WHERE predicate in sensitive mode.");
      return NO;
    }
  }
  case SENS_LOC_GROUP_BY:
  case SENS_LOC_HAVING:
  case SENS_LOC_ORDER_BY: {
    int sens = expr_has_sensitive(main_q, ctx->cp, e);
    if (sens == ERR)
      return ERR;
    if (sens == YES) {
      const char *desc = qir_colref_to_str(&e->u.colref, &ctx->scratch);
      if (loc == SENS_LOC_GROUP_BY) {
        set_err(ctx, VERR_SENSITIVE_LOC,
                "GROUP BY cannot reference sensitive column '%s'.", desc);
      }
      if (loc == SENS_LOC_HAVING) {
        set_err(ctx, VERR_SENSITIVE_LOC,
                "HAVING cannot reference sensitive column '%s'.", desc);
      }
      if (loc == SENS_LOC_ORDER_BY) {
        set_err(ctx, VERR_SENSITIVE_LOC,
                "ORDER BY cannot reference sensitive column '%s'.", desc);
      }
      return NO;
    }
    return YES;
  }
  }
  return ERR;
}

/* Pass B: enforces Sensitive Mode rules on this query and all nested queries.
 * This pass should only be executed when Sensitive Mode is enabled. */
static int validate_query_pass_b(ValidatorCtx *ctx, const QirQuery *q) {
  if (!ctx || !ctx->db || !ctx->cp || !q)
    return ERR;

  if (q->has_star) {
    set_err(ctx, VERR_STAR, "SELECT * is not allowed.");
    return NO;
  }
  if (q->has_distinct) {
    set_err(ctx, VERR_DISTINCT_SENSITIVE,
            "DISTINCT is not allowed in sensitive mode.");
    return NO;
  }
  if (q->has_offset) {
    set_err(ctx, VERR_OFFSET_SENSITIVE,
            "OFFSET is not allowed in sensitive mode.");
    return NO;
  }
  if (q->limit_value < 0) {
    set_err(ctx, VERR_LIMIT_REQUIRED, "LIMIT is required in sensitive mode.");
    return NO;
  }
  if (q->limit_value > MAX_ROWS_SENS_ON) {
    set_err(ctx, VERR_LIMIT_EXCEEDS, "LIMIT exceeds sensitive mode maximum.");
    return NO;
  }

  // JOIN
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j) {
      set_err(ctx, VERR_ANALYZE_FAIL, "Invalid query structure (NULL JOIN).");
      return ERR;
    }
    if (j->kind != QIR_JOIN_INNER) {
      set_err(ctx, VERR_JOIN_NOT_INNER,
              "Only INNER JOIN is allowed in sensitive mode.");
      return NO;
    }
    if (j->on) {
      int rc = validate_sensitive_expr(ctx, q, j->on, SENS_LOC_JOIN_ON);
      if (rc != YES)
        return rc;
    }
  }

  // WHERE
  if (q->where) {
    int rc = validate_sensitive_expr(ctx, q, q->where, SENS_LOC_WHERE);
    if (rc != YES)
      return rc;
  }

  // SELECT
  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL SELECT item).");
      return ERR;
    }
    int rc = validate_sensitive_expr(ctx, q, si->value, SENS_LOC_SELECT);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b(ctx, si->value);
    if (rc != YES)
      return rc;
  }

  // GROUP BY
  for (uint32_t i = 0; i < q->n_group_by; i++) {
    QirExpr *e = q->group_by ? q->group_by[i] : NULL;
    if (!e) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL GROUP BY item).");
      return ERR;
    }
    int rc = validate_sensitive_expr(ctx, q, e, SENS_LOC_GROUP_BY);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b(ctx, e);
    if (rc != YES)
      return rc;
  }

  // HAVING
  if (q->having) {
    int rc = validate_sensitive_expr(ctx, q, q->having, SENS_LOC_HAVING);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b(ctx, q->having);
    if (rc != YES)
      return rc;
  }

  // ORDER BY
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    QirExpr *e = q->order_by ? q->order_by[i] : NULL;
    if (!e) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL ORDER BY item).");
      return ERR;
    }
    int rc = validate_sensitive_expr(ctx, q, e, SENS_LOC_ORDER_BY);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b(ctx, e);
    if (rc != YES)
      return rc;
  }

  // Recurse into nested queries to enforce the same Sensitive Mode rules.
  for (uint32_t i = 0; i < q->nctes; i++) {
    int rc = validate_query_pass_b(ctx, q->ctes[i]->query);
    if (rc != YES)
      return rc;
  }
  if (q->from_root && q->from_root->kind == QIR_FROM_SUBQUERY) {
    int rc = validate_query_pass_b(ctx, q->from_root->u.subquery);
    if (rc != YES)
      return rc;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins ? q->joins[i]->rhs : NULL;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      int rc = validate_query_pass_b(ctx, fi->u.subquery);
      if (rc != YES)
        return rc;
    }
  }

  return YES;
}

int validate_query(const ValidatorRequest *req, ValidatorErr *err) {
  if (!req || !err)
    return ERR;
  if (!req->db || !req->profile || !req->sql)
    return ERR;

  ValidatorCtx ctx = {
      .db = req->db, .cp = req->profile, .err = err, .scratch = {0}};

  QirQueryHandle h = {0};
  if (db_make_query_ir(req->db, req->sql, &h) != OK) {
    set_err(&ctx, VERR_PARSE_FAIL, "Failed to parse query.");
    sb_clean(&ctx.scratch);
    return ERR;
  }

  QirQuery *q = h.q;
  if (!q || q->status != QIR_OK) {
    const char *reason =
        (q && q->status_reason) ? q->status_reason : "Invalid query.";
    set_err(&ctx, VERR_UNSUPPORTED_QUERY, reason);
    qir_handle_destroy(&h);
    sb_clean(&ctx.scratch);
    return ERR;
  }
  if (q->has_star) {
    set_err(&ctx, VERR_STAR, "SELECT * is not allowed.");
    qir_handle_destroy(&h);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  QirTouchReport *tr = qir_extract_touches(q);
  if (!tr) {
    set_err(&ctx, VERR_ANALYZE_FAIL, "Unable to analyze query.");
    qir_handle_destroy(&h);
    sb_clean(&ctx.scratch);
    return ERR;
  }
  if (tr->has_unsupported) {
    set_err(&ctx, VERR_UNSUPPORTED_QUERY, "Unsupported query structure.");
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  // Touch report to decide if Sensitive Mode is needed and to ensure sensitive
  // touches never appear outside the main query
  bool sensitive_mode = false;
  int rc = validate_sensitive_touches_scope(&ctx, tr, &sensitive_mode);
  if (rc != YES) {
    if (rc == ERR && err->code == VERR_NONE) {
      set_err(&ctx, VERR_ANALYZE_FAIL,
              "Unable to analyze columns. Every table must have an alias, and "
              "every column must be qualified as alias.column.");
    }
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  // Pass A/Pass B design. Read the start of validator.c for doc.
  rc = validate_query_pass_a(&ctx, q);
  if (rc != YES) {
    if (rc == ERR && err->code == VERR_NONE) {
      set_err(&ctx, VERR_ANALYZE_FAIL, "Unable to start query analysis.");
    }
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  if (sensitive_mode == YES) {
    rc = validate_query_pass_b(&ctx, q);
    if (rc != YES) {
      if (rc == ERR && err->code == VERR_NONE) {
        set_err(&ctx, VERR_ANALYZE_FAIL, "Unable to analyze sensitive query.");
      }
      qir_touch_report_destroy(tr);
      qir_handle_destroy(&h);
      sb_clean(&ctx.scratch);
      return ERR;
    }
  }

  qir_touch_report_destroy(tr);
  qir_handle_destroy(&h);
  sb_clean(&ctx.scratch);
  return OK;
}
