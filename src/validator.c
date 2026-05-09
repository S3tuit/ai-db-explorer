#include "validator.h"
#include "log.h"
#include "query_ir.h"
#include "utils.h"

#include <assert.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#define MAX_ROWS_SENS_ON 200

/*---------------------------- QUERY VALIDATION -----------------------------*/
/* To validate a query we combine one binder-backed touch walk and a three-pass
 * approach:
 *  - qir_walk_touches() used for:
 *      + All bound column references are scanned once.
 *      + Sensitive columns can be referenced only inside the main query.
 *      + If at least one sensitive column is referenced, switches sensitive
 *        mode on. Vault should be opened, else, it fails.
 *
 *  - bind_query_ir() used for:
 *      + All column and CTE name binding.
 *      + Safe SELECT * / alias.* expansion over derived sources only.
 *      + Rejecting unsafe star expansion before validator policy runs.
 *
 *  - Pass A. It contains all the validations that are independent of the mode
 *    (sensitive mode on/off) and independent of token parameters. Pass A
 *    checks that:
 *      + All functions are safe to call.
 *      + Nested subqueries satisfy the same Pass A rules.
 *
 *  - Pass B. It validates token parameter usage regardless of sensitive mode.
 *    Pass B checks that:
 *      + Parameters are used only inside WHERE.
 *      + Parameters only appear as direct operands of '=' or items in IN().
 *      + Parameters only compare to direct sensitive columns.
 *      + Parameter metadata matches the sensitive domain and every provided
 *        token is used.
 *
 *  - Pass C. This runs only if sensitive mode is on.
 *    It applies the extra sensitive-column restrictions recursively to all
 *    nested queries (CTEs and subqueries) and checks that:
 *      + Sensitive columns are not referenced by casts or functions.
 *      + Sensitive columns appear only in SELECT (as simple colref) or WHERE.
 *      + Sensitive columns can only be compared using = or IN() and only to
 *        parameters.
 *      + WHERE must be a conjunction of predicates: `pred (AND pred)`. No
 *        `NOT`, no `OR`.
 *      + All JOINs must be INNER and use only = and AND; JOIN ON cannot
 *        reference sensitive columns. Only column references or literals can
 *        be used inside JOIN.
 *      + GROUP BY / HAVING / ORDER BY cannot reference sensitive columns.
 *      + DISTINCT and OFFSET are rejected in sensitive mode.
 *
 * Note: UNION/INTERSECT/EXCEPT are not modeled by the IR, so they are rejected
 * by the parser/IR layer before validation runs.
 */

typedef enum QueryLoc {
  QLOC_SELECT = 1,
  QLOC_WHERE,
  QLOC_JOIN_ON,
  QLOC_GROUP_BY,
  QLOC_HAVING,
  QLOC_ORDER_BY
} QueryLoc;

typedef struct ValidatorCtx {
  DbBackend *db;
  const ConnProfile *cp;
  ValidatorErr *err;
  const QirQuery *root_query;
  const SensitiveTok *params;
  uint32_t nparams;
  uint8_t *param_used;
  bool sensitive_mode;
  // Scratch buffer for building short diagnostic strings. The contents are
  // valid until the next scratch use.
  StrBuf scratch;
} ValidatorCtx;

static AdbxTriStatus validate_query_pass_a(ValidatorCtx *ctx,
                                           const QirQuery *q);
static AdbxTriStatus validate_query_pass_b_params(ValidatorCtx *ctx,
                                                  const QirQuery *q);
static AdbxTriStatus validate_query_pass_c_sensitive(ValidatorCtx *ctx,
                                                     const QirQuery *q);

#define MAX_ERR_MSG_LEN 512
/* Resets 'ctx->err->msg' and writes a string into it using 'fmt' like printf().
 */
static AdbxStatus set_err(ValidatorCtx *ctx, ValidatorErrCode code,
                          const char *fmt, ...) {
  va_list args;
  char buffer[MAX_ERR_MSG_LEN];
  int len;

  assert(ctx != NULL);
  assert(fmt != NULL);
  assert(ctx->err != NULL);

  ctx->err->code = code;
  sb_reset(&ctx->err->msg);

  va_start(args, fmt);
  len = vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);

  if (len < 0)
    return ERR;

  /* Handle truncation warning */
  if (len >= (int)sizeof(buffer)) {
    len = sizeof(buffer) - 1;
  }

  return sb_append_bytes(&ctx->err->msg, buffer, len);
}

/* Resets one request-scoped validator output object.
 * It borrows 'out' and reallocates per-request plan storage.
 * Side effects: frees/reinitializes plan buffers and clears error message/code.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus vq_out_reset(ValidateQueryOut *out) {
  assert(out != NULL);
  assert(out->plan.cols != NULL);

  parr_destroy(out->plan.cols);
  out->plan.cols = parr_create(sizeof(ValidatorColPlan));
  if (!out->plan.cols)
    return ERR;

  out->plan.mode = VPLAN_MODE_SELECT;
  out->err.code = VERR_NONE;
  sb_reset(&out->err.msg);
  return OK;
}

/* Resolves one query column reference to its borrowed sensitive-domain name.
 * Returns:
 *  - YES when 'cr' is sensitive and writes one borrowed domain
 *    pointer/length pair (if 'out_domain' and 'out_domain_len' are not NULL).
 *  - NO when the column is not sensitive.
 *  - ERR on invalid input, unresolved aliases, or internal inconsistency and
 *    modifies 'ctx'.
 *
 * This is not suitable to understand if a colref contains data of a sensitive
 * column because we don't resolve the original table. However, since we
 * enforced that sensitive columns can only appear in the main SELECT, this can
 * be used to understand if a colref, of the main SELECT, contains sensitive
 * data.
 */
static AdbxTriStatus
validator_resolve_sensitive_domain(ValidatorCtx *ctx, const QirColRef *cr,
                                   const char **out_domain,
                                   uint32_t *out_domain_len) {
  assert(ctx != NULL);
  assert(cr != NULL);

  if (out_domain)
    *out_domain = NULL;
  if (out_domain_len)
    *out_domain_len = 0;

  const QirFromItem *fi = cr->binding_from;
  if (!fi) {
    const char *desc = qir_colref_to_str(cr, &ctx->scratch);
    set_err(ctx, VERR_ANALYZE_FAIL,
            "Unable to resolve bound column source for '%s'.", desc);
    return ERR;
  }

  // We only resolve direct base-relation columns. Derived tables and bound CTE
  // names are handled as non-sensitive here because we do not trace them back
  // to an original relation.
  if (fi->kind != QIR_FROM_BASE_REL || fi->binding_cte)
    return NO;

  const char *schema = fi->u.rel.schema.name;
  const char *table = fi->u.rel.name.name;
  const char *col = cr->column.name;
  if (!table || table[0] == '\0' || !col || col[0] == '\0') {
    set_err(ctx, VERR_ANALYZE_FAIL,
            "Unable to resolve sensitive domain from query metadata.");
    return ERR;
  }

  SensDomainOut sdout;
  AdbxTriStatus rc =
      connp_get_sensitive_domain(ctx->cp, schema, table, col, &sdout);
  if (rc == NO)
    return NO;
  if (rc != YES || !sdout.domain || sdout.domain[0] == '\0') {
    const char *desc = qir_colref_to_str(cr, &ctx->scratch);
    if (sdout.err.msg[0] != '\0') {
      set_err(ctx, VERR_ANALYZE_FAIL, "%s", sdout.err.msg);
    } else {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Unable to resolve sensitive domain for '%s'.", desc);
    }
    return ERR;
  }

  if (out_domain_len) {
    size_t domain_len = strlen(sdout.domain);
    if (domain_len == 0 || domain_len > UINT32_MAX) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Resolved sensitive domain has invalid length.");
      return ERR;
    }
    *out_domain_len = (uint32_t)domain_len;
  }

  if (out_domain)
    *out_domain = sdout.domain;
  return YES;
}

/* Builds the output-column policy for one validated query.
 * It borrows query and policy metadata and writes into 'out_plan'.
 * Side effects: appends entries to out_plan->cols or switches the plan into
 * passthrough-plaintext mode for backend-shaped results like EXPLAIN.
 * Error semantics: returns OK on success, ERR on invalid input/allocation
 * failures; may set validator errors.
 */
static AdbxStatus validator_build_plan(ValidatorCtx *ctx, const QirQuery *q,
                                       ValidatorPlan *out_plan) {
  assert(ctx != NULL);
  assert(q != NULL);
  assert(out_plan != NULL);
  assert(out_plan->cols != NULL);

  if (qir_query_is_explain(q)) {
    // EXPLAIN returns backend-generated plan rows rather than the wrapped
    // SELECT output columns, so the validator must not assume column alignment.
    out_plan->mode = VPLAN_MODE_PASSTHROUGH_PLAINTEXT;
    return OK;
  }

  // We only inspect the father's SELECT list and ignore union_next branches:
  // (1) sensitive columns are already forbidden in union chains, so only the
  //     father can carry TOKEN slots;
  // (2) the database engine enforces that every branch has matching column
  //     count, compatible types, and uses the first branch's column names.
  for (uint32_t i = 0; i < q->nselect; i++) {
    assert(q->select_items != NULL);
    const QirSelectItem *si = q->select_items[i];
    if (!si || !si->value) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL SELECT item).");
      return ERR;
    }

    ValidatorColPlan *slot = NULL;
    uint32_t idx = parr_emplace(out_plan->cols, (void **)&slot);
    if (idx == UINT32_MAX || !slot)
      return ERR;
    slot->kind = VCOL_OUT_PLAINTEXT;
    slot->domain = NULL;
    slot->domain_len = 0;

    if (si->value->kind != QIR_EXPR_COLREF)
      continue;

    const char *domain = NULL;
    uint32_t domain_len = 0;
    int sens = validator_resolve_sensitive_domain(ctx, &si->value->u.colref,
                                                  &domain, &domain_len);
    if (sens == ERR)
      return ERR;
    if (sens != YES)
      continue;

    slot->kind = VCOL_OUT_TOKEN;
    slot->domain = domain;
    slot->domain_len = domain_len;
  }

  return OK;
}

// PASS A START
/* Returns a short stable description for one expression kind in diagnostics.
 * It borrows all inputs and does not allocate memory.
 * Error semantics: always returns a non-NULL borrowed string.
 */
static const char *validator_expr_diag(ValidatorCtx *ctx, const QirExpr *e) {
  if (!ctx || !e)
    return "<expression>";
  if (e->kind == QIR_EXPR_COLREF)
    return qir_colref_to_str(&e->u.colref, &ctx->scratch);
  if (e->kind == QIR_EXPR_LITERAL)
    return "<literal>";
  if (e->kind == QIR_EXPR_PARAM)
    return "<parameter>";
  if (e->kind == QIR_EXPR_SUBQUERY)
    return "<subquery>";
  return "<expression>";
}

/* Validates that parameter '$param_idx' exists and matches one sensitive
 * domain. It borrows all inputs and marks ctx->param_used on success.
 * This should be used only in pass B since we assume QirColRef's table is a
 * base relation of 'q's scope.
 * Returns YES on success, NO on policy mismatch, ERR on bad input or internal
 * inconsistency.
 */
static AdbxTriStatus validator_validate_param_domain_for_col(
    ValidatorCtx *ctx, const QirColRef *sensitive_cr, int param_idx) {
  assert(ctx);
  assert(sensitive_cr);

  if (param_idx < 1) {
    set_err(ctx, VERR_PARAM_IDX_RANGE,
            "Token parameters referenced inside the query should start with "
            "index 1: query references $%d.",
            param_idx);
    return NO;
  }

  if (!ctx->params || ctx->nparams == 0) {
    set_err(ctx, VERR_PARAM_IDX_RANGE,
            "Missing token parameters: query references $%d.", param_idx);
    return NO;
  }

  if ((uint32_t)param_idx > ctx->nparams) {
    set_err(ctx, VERR_PARAM_IDX_RANGE,
            "Token parameter index $%d is out of range (received %u).",
            param_idx, ctx->nparams);
    return NO;
  }

  const SensitiveTok *tok = &ctx->params[(uint32_t)param_idx - 1u];
  if (!tok->domain || tok->domain_len == 0) {
    set_err(ctx, VERR_ANALYZE_FAIL, "Invalid token parameter metadata for $%d.",
            param_idx);
    return ERR;
  }

  const char *domain = NULL;
  uint32_t domain_len = 0;
  int mrc = validator_resolve_sensitive_domain(ctx, sensitive_cr, &domain,
                                               &domain_len);
  if (mrc == ERR)
    return ERR;
  if (mrc == NO) {
    set_err(ctx, VERR_PARAM_NON_SENSITIVE,
            "Parameters can only be compared to direct columns of the same "
            "sensitive domain.");
    return ERR;
  }
  if (tok->domain_len != domain_len ||
      memcmp(tok->domain, domain, domain_len) != 0) {
    const char *desc = qir_colref_to_str(sensitive_cr, &ctx->scratch);
    set_err(ctx, VERR_PARAM_DOMAIN_MISMATCH,
            "Token parameter $%d domain '%.*s' does not match sensitive "
            "column domain for '%s'.",
            param_idx, (int)tok->domain_len, tok->domain, desc);
    return NO;
  }

  if (ctx->param_used)
    ctx->param_used[(uint32_t)param_idx - 1u] = 1u;
  return YES;
}

/* Validates all the subqueries embedded inside 'e'. Return YES/NO/ERR. Sets
 * '*err' if it doesn't return YES. */
typedef AdbxTriStatus (*ValidateQueryFn)(ValidatorCtx *, const QirQuery *);

/* Walks an expression tree and validates all nested subqueries via the
 * callback provided by the caller. The callback controls the policy (Pass A
 * vs Pass B vs Pass C). */
static AdbxTriStatus
validate_expr_subqueries(ValidatorCtx *ctx, const QirExpr *e,
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
  case QIR_EXPR_OP: {
    int rc = validate_expr_subqueries(ctx, e->u.op.lhs, validate_query_fn);
    if (rc != YES)
      return rc;
    for (uint32_t i = 0; i < e->u.op.nargs; i++) {
      rc = validate_expr_subqueries(ctx, e->u.op.args[i], validate_query_fn);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_AND:
  case QIR_EXPR_OR: {
    int rc = validate_expr_subqueries(ctx, e->u.bin.l, validate_query_fn);
    if (rc != YES)
      return rc;
    return validate_expr_subqueries(ctx, e->u.bin.r, validate_query_fn);
  }
  case QIR_EXPR_NOT:
    return validate_expr_subqueries(ctx, e->u.bin.l, validate_query_fn);
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

static AdbxTriStatus validate_expr_subqueries_pass_a(ValidatorCtx *ctx,
                                                     const QirExpr *e) {
  return validate_expr_subqueries(ctx, e, validate_query_pass_a);
}

/* Returns YES if sensitive touches are only in allowed scopes and sets
 * 'found_sensitive' to true if it saw at least one sensitive column.
 *
 * This function is the single source of truth for enabling sensitive mode:
 * it scans all touches, rejects unknown aliases, and flips
 * vctx->found_sensitive when a sensitive base column is referenced.
 *
 * Side effects: writes a human-readable reason into err on failure. */
static AdbxStatus validator_visit_touch(QirScope scope,
                                        const QirQuery *owner_query,
                                        const QirColRef *colref,
                                        QirTouchKind kind, void *vctx) {
  ValidatorCtx *ctx = (ValidatorCtx *)vctx;
  (void)owner_query;

  if (!ctx || !colref)
    return ERR;

  if (kind == QIR_TOUCH_UNKNOWN) {
    const char *desc = qir_colref_to_str(colref, &ctx->scratch);
    set_err(ctx, VERR_NO_COLUMN_ALIAS,
            "Unknown or unresolved column reference '%s'.",
            desc);
    return ERR;
  }

  // We validate just the TOUCH_BASE since each TOUCH_DERIVED has one
  // TOUCH_BASE with the same column referenced. So, we need to validate only
  // the TOUCH_BASE and make sure they have SCOPE_MAIN.
  if (kind != QIR_TOUCH_BASE)
    return OK;

  int rc = validator_resolve_sensitive_domain(ctx, colref, NULL, NULL);
  if (rc != YES)
    return rc == NO ? OK : ERR;

  ctx->sensitive_mode = true;
  if (scope != QIR_SCOPE_MAIN) {
    const char *desc = qir_colref_to_str(colref, &ctx->scratch);
    set_err(ctx, VERR_SENSITIVE_OUTSIDE_MAIN,
            "Column '%s' is sensitive, so it's only allowed in main query.",
            desc);
    return ERR;
  }
  if (ctx->root_query && ctx->root_query->union_next) {
    const char *desc = qir_colref_to_str(colref, &ctx->scratch);
    set_err(ctx, VERR_SENSITIVE_IN_UNION,
            "Column '%s' is sensitive and cannot be referenced inside a "
            "UNION/INTERSECT/EXCEPT branch.",
            desc);
    return ERR;
  }
  return OK;
}

static int name_cpm(const void *s1, const void *s2) {
  const char *key = s1;
  const char *const *elem = s2;
  return strcmp(key, *elem);
}

/* Returns YES if a function is safe to call for 'db' or is present in the
 * user-defined list of 'cp'. Returns YES/NO/ERR. */
static AdbxTriStatus validator_is_function_safe(ValidatorCtx *ctx,
                                                const char *schema,
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
static AdbxTriStatus validate_expr_functions(ValidatorCtx *ctx,
                                             const QirExpr *e) {
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
  case QIR_EXPR_OP: {
    int rc = validate_expr_functions(ctx, e->u.op.lhs);
    if (rc != YES)
      return rc;
    for (uint32_t i = 0; i < e->u.op.nargs; i++) {
      rc = validate_expr_functions(ctx, e->u.op.args[i]);
      if (rc != YES)
        return rc;
    }
    return YES;
  }
  case QIR_EXPR_AND:
  case QIR_EXPR_OR: {
    int rc = validate_expr_functions(ctx, e->u.bin.l);
    if (rc != YES)
      return rc;
    return validate_expr_functions(ctx, e->u.bin.r);
  }
  case QIR_EXPR_NOT:
    return validate_expr_functions(ctx, e->u.bin.l);
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
 * It borrows all inputs and does not allocate.
 * Side effects: on ERR it may propagate detailed validator errors from nested
 * sensitive-domain resolution into ctx->err.
 * Returns YES when a sensitive column is present, NO when not present, ERR on
 * invalid input or failed sensitivity resolution. Subqueries are treated as
 * separate scopes and do not contribute to this check.
 */
static AdbxTriStatus expr_has_sensitive(ValidatorCtx *ctx, const QirQuery *q,
                                        const QirExpr *e) {
  (void)q;
  if (!ctx)
    return ERR;
  if (!e)
    return NO;

  switch (e->kind) {
  case QIR_EXPR_COLREF:
    return validator_resolve_sensitive_domain(ctx, &e->u.colref, NULL, NULL);
  case QIR_EXPR_PARAM:
  case QIR_EXPR_LITERAL:
    return NO;
  case QIR_EXPR_SUBQUERY:
    // Subqueries are validated independently and do not contribute sensitivity
    // to the enclosing expression tree.
    return NO;
  case QIR_EXPR_FUNCALL: {
    for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
      int rc = expr_has_sensitive(ctx, q, e->u.funcall.args[i]);
      if (rc != NO)
        return rc;
    }
    return NO;
  }
  case QIR_EXPR_CAST:
    return expr_has_sensitive(ctx, q, e->u.cast.expr);
  case QIR_EXPR_OP: {
    int rc = NO;
    if (e->u.op.lhs) {
      rc = expr_has_sensitive(ctx, q, e->u.op.lhs);
      if (rc != NO)
        return rc;
    }

    for (uint32_t i = 0; i < e->u.op.nargs; i++) {
      rc = expr_has_sensitive(ctx, q, e->u.op.args[i]);
      if (rc != NO)
        return rc;
    }
    return NO;
  }
  case QIR_EXPR_AND:
  case QIR_EXPR_OR: {
    int rc = expr_has_sensitive(ctx, q, e->u.bin.l);
    if (rc != NO)
      return rc;
    return expr_has_sensitive(ctx, q, e->u.bin.r);
  }
  case QIR_EXPR_NOT:
    return expr_has_sensitive(ctx, q, e->u.bin.l);
  case QIR_EXPR_CASE: {
    if (e->u.case_.arg) {
      int rc = expr_has_sensitive(ctx, q, e->u.case_.arg);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens[i];
      if (!w)
        return ERR;
      int rc = expr_has_sensitive(ctx, q, w->when_expr);
      if (rc != NO)
        return rc;
      rc = expr_has_sensitive(ctx, q, w->then_expr);
      if (rc != NO)
        return rc;
    }
    if (e->u.case_.else_expr) {
      return expr_has_sensitive(ctx, q, e->u.case_.else_expr);
    }
    return NO;
  }
  case QIR_EXPR_WINDOWFUNC: {
    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      int rc = expr_has_sensitive(ctx, q, e->u.window.func.args[i]);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      int rc = expr_has_sensitive(ctx, q, e->u.window.partition_by[i]);
      if (rc != NO)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      int rc = expr_has_sensitive(ctx, q, e->u.window.order_by[i]);
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
static AdbxTriStatus expr_has_param(const QirExpr *e) {
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
  case QIR_EXPR_OP: {
    int rc = expr_has_param(e->u.op.lhs);
    if (rc != NO)
      return rc;
    for (uint32_t i = 0; i < e->u.op.nargs; i++) {
      rc = expr_has_param(e->u.op.args[i]);
      if (rc != NO)
        return rc;
    }
    return NO;
  }
  case QIR_EXPR_AND:
  case QIR_EXPR_OR: {
    int rc = expr_has_param(e->u.bin.l);
    if (rc != NO)
      return rc;
    return expr_has_param(e->u.bin.r);
  }
  case QIR_EXPR_NOT:
    return expr_has_param(e->u.bin.l);
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

/* Returns YES when 'other' is a direct sensitive column with the same domain
 * as parameter 'param_idx', NO on policy mismatch, ERR on malformed input or
 * resolution failure.
 * It borrows all inputs and performs no allocations. On success it also marks
 * the matched parameter as used.
 */
static AdbxTriStatus
validate_param_direct_sensitive_target(ValidatorCtx *ctx, const QirQuery *q,
                                       const QirExpr *other, int param_idx) {
  if (!ctx || !q || !other)
    return ERR;

  if (other->kind != QIR_EXPR_COLREF) {
    set_err(ctx, VERR_PARAM_NON_SENSITIVE,
            "Parameters can only compare to direct sensitive columns.");
    return NO;
  }

  return validator_validate_param_domain_for_col(ctx, &other->u.colref,
                                                 param_idx);
}

/* Validates parameter usage for one expression tree in one query clause.
 * It borrows 'ctx', 'q', and 'e'. Nested subqueries are intentionally treated
 * as separate query scopes and are NOT recursively validate.
 * Side effects: marks parameters as used, and  writes one validator error on
 * rejection. Returns YES on success, NO on policy mismatch, ERR on malformed
 * input or internal failure.
 */
static AdbxTriStatus validate_param_expr(ValidatorCtx *ctx, const QirQuery *q,
                                         const QirExpr *e, QueryLoc loc) {
  if (!ctx || !q || !e)
    return ERR;

  if (loc != QLOC_WHERE) {
    AdbxTriStatus has_param = expr_has_param(e);
    if (has_param == ERR)
      return ERR;
    if (has_param == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed as direct operands of WHERE '=' or "
              "IN predicates.");
      return NO;
    }
    return YES;
  }

  switch (e->kind) {
  case QIR_EXPR_SUBQUERY:
  case QIR_EXPR_LITERAL:
  case QIR_EXPR_COLREF:
    return YES;
  case QIR_EXPR_AND:
  case QIR_EXPR_OR: {
    AdbxTriStatus rc = validate_param_expr(ctx, q, e->u.bin.l, loc);
    if (rc != YES)
      return rc;
    return validate_param_expr(ctx, q, e->u.bin.r, loc);
  }
  case QIR_EXPR_NOT:
    return validate_param_expr(ctx, q, e->u.bin.l, loc);
  case QIR_EXPR_OP: {
    // EQ and IN must not have the left hand side NULL
    if (e->u.op.cls != QIR_OP_OTHER && !e->u.op.lhs) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL operator lhs).");
      return ERR;
    }

    if (e->u.op.cls == QIR_OP_EQ) {
      if (!e->u.op.args || e->u.op.nargs != 1 || !e->u.op.args[0]) {
        set_err(ctx, VERR_ANALYZE_FAIL,
                "Invalid query structure (malformed '=' predicate).");
        return ERR;
      }

      const QirExpr *rhs = e->u.op.args[0];
      if (e->u.op.lhs->kind == QIR_EXPR_PARAM) {
        return validate_param_direct_sensitive_target(
            ctx, q, rhs, e->u.op.lhs->u.param_index);
      }
      if (rhs->kind == QIR_EXPR_PARAM) {
        return validate_param_direct_sensitive_target(ctx, q, e->u.op.lhs,
                                                      rhs->u.param_index);
      }

      AdbxTriStatus lhs_has_param = expr_has_param(e->u.op.lhs);
      if (lhs_has_param == ERR)
        return ERR;
      AdbxTriStatus rhs_has_param = expr_has_param(rhs);
      if (rhs_has_param == ERR)
        return ERR;
      if (lhs_has_param == YES || rhs_has_param == YES) {
        set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
                "Parameters are only allowed as direct operands of WHERE '=' "
                "or IN predicates.");
        return NO;
      }
      return YES;
    }

    if (e->u.op.cls == QIR_OP_IN) {
      if (!e->u.op.args || e->u.op.nargs == 0) {
        set_err(ctx, VERR_ANALYZE_FAIL,
                "Invalid query structure (malformed IN predicate).");
        return ERR;
      }

      AdbxTriStatus lhs_has_param = expr_has_param(e->u.op.lhs);
      if (lhs_has_param == ERR)
        return ERR;
      if (lhs_has_param == YES) {
        set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
                "Parameters are only allowed as direct operands of WHERE '=' "
                "or IN predicates.");
        return NO;
      }

      int saw_param = 0;
      for (uint32_t i = 0; i < e->u.op.nargs; i++) {
        const QirExpr *it = e->u.op.args[i];
        if (!it) {
          set_err(ctx, VERR_ANALYZE_FAIL,
                  "Invalid query structure (NULL IN item).");
          return ERR;
        }
        if (it->kind == QIR_EXPR_PARAM) {
          saw_param = 1;
          continue;
        }
        AdbxTriStatus item_has_param = expr_has_param(it);
        if (item_has_param == ERR)
          return ERR;
        if (item_has_param == YES) {
          set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
                  "Parameters are only allowed as direct operands of WHERE '=' "
                  "or IN predicates.");
          return NO;
        }
      }

      if (!saw_param)
        return YES;

      for (uint32_t i = 0; i < e->u.op.nargs; i++) {
        const QirExpr *it = e->u.op.args[i];
        if (!it || it->kind != QIR_EXPR_PARAM)
          continue;
        AdbxTriStatus rc = validate_param_direct_sensitive_target(
            ctx, q, e->u.op.lhs, it->u.param_index);
        if (rc != YES)
          return rc;
      }
      return YES;
    }

    AdbxTriStatus has_param = expr_has_param(e);
    if (has_param == ERR)
      return ERR;
    if (has_param == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed as direct operands of WHERE '=' or "
              "IN predicates.");
      return NO;
    }
    return YES;
  }
  case QIR_EXPR_PARAM:
    set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
            "Parameters are only allowed as direct operands of WHERE '=' or "
            "IN predicates.");
    return NO;
  case QIR_EXPR_FUNCALL:
  case QIR_EXPR_CAST:
  case QIR_EXPR_CASE:
  case QIR_EXPR_WINDOWFUNC: {
    AdbxTriStatus has_param = expr_has_param(e);
    if (has_param == ERR)
      return ERR;
    if (has_param == YES) {
      set_err(ctx, VERR_PARAM_OUTSIDE_WHERE,
              "Parameters are only allowed as direct operands of WHERE '=' or "
              "IN predicates.");
      return NO;
    }
    return YES;
  }
  case QIR_EXPR_UNSUPPORTED:
    return ERR;
  }
  return ERR;
}

static AdbxTriStatus validate_expr_subqueries_pass_b_params(ValidatorCtx *ctx,
                                                            const QirExpr *e) {
  return validate_expr_subqueries(ctx, e, validate_query_pass_b_params);
}

/* Pass A: validates alias requirements, function safety, and validates
 * all nested queries. This pass is mode-independent and is always required. */
static AdbxTriStatus validate_query_pass_a(ValidatorCtx *ctx,
                                           const QirQuery *q) {
  if (!ctx || !ctx->db || !ctx->cp || !q)
    return ERR;

  // Function allowlist enforcement across the query.
  // SELECT
  for (uint32_t i = 0; i < q->nselect; i++) {
    int rc = validate_expr_functions(ctx, q->select_items[i]->value);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_a(ctx, q->select_items[i]->value);
    if (rc != YES)
      return rc;
  }
  // WHERE
  if (q->where) {
    int rc = validate_expr_functions(ctx, q->where);
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
    rc = validate_expr_subqueries_pass_a(ctx, q->group_by[i]);
    if (rc != YES)
      return rc;
  }
  // HAVING
  if (q->having) {
    int rc = validate_expr_functions(ctx, q->having);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_a(ctx, q->having);
    if (rc != YES)
      return rc;
  }
  // ORDER BY
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    int rc = validate_expr_functions(ctx, q->order_by[i]);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_a(ctx, q->order_by[i]);
    if (rc != YES)
      return rc;
  }
  // JOIN
  for (uint32_t i = 0; i < q->njoins; i++) {
    int rc = validate_expr_functions(ctx, q->joins[i]->on);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_a(ctx, q->joins[i]->on);
    if (rc != YES)
      return rc;
  }

  // Recurse into nested queries
  for (uint32_t i = 0; i < q->nctes; i++) {
    int rc = validate_query_pass_a(ctx, q->ctes[i]->query);
    if (rc != YES)
      return rc;
  }
  if (q->from_root && q->from_root->kind == QIR_FROM_SUBQUERY) {
    int rc = validate_query_pass_a(ctx, q->from_root->u.subquery);
    if (rc != YES)
      return rc;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins[i]->rhs;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      int rc = validate_query_pass_a(ctx, fi->u.subquery);
      if (rc != YES)
        return rc;
    }
  }
  for (const QirQuery *branch = q->union_next; branch;
       branch = branch->union_next) {
    int rc = validate_query_pass_a(ctx, branch);
    if (rc != YES)
      return rc;
  }
  return YES;
}

/* Pass B: validates token parameter placement and binding rules for this query
 * and all nested queries.
 */
static AdbxTriStatus validate_query_pass_b_params(ValidatorCtx *ctx,
                                                  const QirQuery *q) {
  if (!ctx || !ctx->db || !ctx->cp || !q)
    return ERR;

  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL SELECT item).");
      return ERR;
    }
    AdbxTriStatus rc = validate_param_expr(ctx, q, si->value, QLOC_SELECT);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b_params(ctx, si->value);
    if (rc != YES)
      return rc;
  }

  if (q->where) {
    AdbxTriStatus rc = validate_param_expr(ctx, q, q->where, QLOC_WHERE);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b_params(ctx, q->where);
    if (rc != YES)
      return rc;
  }

  for (uint32_t i = 0; i < q->n_group_by; i++) {
    QirExpr *e = q->group_by ? q->group_by[i] : NULL;
    if (!e) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL GROUP BY item).");
      return ERR;
    }
    AdbxTriStatus rc = validate_param_expr(ctx, q, e, QLOC_GROUP_BY);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b_params(ctx, e);
    if (rc != YES)
      return rc;
  }

  if (q->having) {
    AdbxTriStatus rc = validate_param_expr(ctx, q, q->having, QLOC_HAVING);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b_params(ctx, q->having);
    if (rc != YES)
      return rc;
  }

  for (uint32_t i = 0; i < q->n_order_by; i++) {
    QirExpr *e = q->order_by ? q->order_by[i] : NULL;
    if (!e) {
      set_err(ctx, VERR_ANALYZE_FAIL,
              "Invalid query structure (NULL ORDER BY item).");
      return ERR;
    }
    AdbxTriStatus rc = validate_param_expr(ctx, q, e, QLOC_ORDER_BY);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b_params(ctx, e);
    if (rc != YES)
      return rc;
  }

  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j) {
      set_err(ctx, VERR_ANALYZE_FAIL, "Invalid query structure (NULL JOIN).");
      return ERR;
    }
    if (!j->on)
      continue;
    AdbxTriStatus rc = validate_param_expr(ctx, q, j->on, QLOC_JOIN_ON);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_b_params(ctx, j->on);
    if (rc != YES)
      return rc;
  }

  for (uint32_t i = 0; i < q->nctes; i++) {
    AdbxTriStatus rc = validate_query_pass_b_params(ctx, q->ctes[i]->query);
    if (rc != YES)
      return rc;
  }
  if (q->from_root && q->from_root->kind == QIR_FROM_SUBQUERY) {
    AdbxTriStatus rc =
        validate_query_pass_b_params(ctx, q->from_root->u.subquery);
    if (rc != YES)
      return rc;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins ? q->joins[i]->rhs : NULL;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      AdbxTriStatus rc = validate_query_pass_b_params(ctx, fi->u.subquery);
      if (rc != YES)
        return rc;
    }
  }
  for (const QirQuery *branch = q->union_next; branch;
       branch = branch->union_next) {
    AdbxTriStatus rc = validate_query_pass_b_params(ctx, branch);
    if (rc != YES)
      return rc;
  }

  return YES;
}

// PASS C START

static AdbxTriStatus validate_query_pass_c_sensitive(ValidatorCtx *ctx,
                                                     const QirQuery *q);

static AdbxTriStatus
validate_expr_subqueries_pass_c_sensitive(ValidatorCtx *ctx, const QirExpr *e) {
  return validate_expr_subqueries(ctx, e, validate_query_pass_c_sensitive);
}

static inline bool expr_is_simple_operand(const QirExpr *e) {
  if (!e)
    return false;
  return (e->kind == QIR_EXPR_COLREF || e->kind == QIR_EXPR_LITERAL);
}

/* Validates Sensitive Mode expression rules based on location.
 * Returns YES/NO/ERR and writes a human-friendly reason to ctx->err on NO.
 * Read the start of validator.c for doc. */
static AdbxTriStatus validate_sensitive_expr(ValidatorCtx *ctx,
                                             const QirQuery *main_q,
                                             const QirExpr *e, QueryLoc loc) {
  if (!ctx || !main_q || !e)
    return ERR;

  switch (loc) {
  case QLOC_SELECT: {
    int sens = expr_has_sensitive(ctx, main_q, e);
    if (sens == ERR)
      return ERR;
    if (sens == YES && e->kind != QIR_EXPR_COLREF) {
      set_err(ctx, VERR_SENSITIVE_SELECT_EXPR,
              "Sensitive columns must be selected directly.");
      return NO;
    }
    return YES;
  }

  case QLOC_JOIN_ON: {
    switch (e->kind) {
    case QIR_EXPR_AND: {
      int rc = validate_sensitive_expr(ctx, main_q, e->u.bin.l, loc);
      if (rc != YES)
        return rc;
      return validate_sensitive_expr(ctx, main_q, e->u.bin.r, loc);
    }
    case QIR_EXPR_OP: {
      if (e->u.op.cls != QIR_OP_EQ || !e->u.op.args || e->u.op.nargs != 1 ||
          !e->u.op.args[0]) {
        set_err(ctx, VERR_JOIN_ON_INVALID,
                "JOIN ON must be AND of '=' predicates");
        return NO;
      }
      if (!expr_is_simple_operand(e->u.op.lhs) ||
          !expr_is_simple_operand(e->u.op.args[0])) {
        set_err(
            ctx, VERR_JOIN_ON_INVALID,
            "JOIN predicates must compare simple operands in sensitive mode.");
        return NO;
      }
      int sens_l = expr_has_sensitive(ctx, main_q, e->u.op.lhs);
      if (sens_l == ERR)
        return ERR;
      int sens_r = expr_has_sensitive(ctx, main_q, e->u.op.args[0]);
      if (sens_r == ERR)
        return ERR;
      if (sens_l == YES || sens_r == YES) {
        const char *desc_l = validator_expr_diag(ctx, e->u.op.lhs);
        const char *desc_r = validator_expr_diag(ctx, e->u.op.args[0]);
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

  case QLOC_WHERE: {
    switch (e->kind) {
    case QIR_EXPR_SUBQUERY: {
      return validate_query_pass_c_sensitive(ctx, e->u.subquery);
    }
    case QIR_EXPR_AND: {
      int rc = validate_sensitive_expr(ctx, main_q, e->u.bin.l, loc);
      if (rc != YES)
        return rc;
      return validate_sensitive_expr(ctx, main_q, e->u.bin.r, loc);
    }
    case QIR_EXPR_OP: {
      if (e->u.op.cls == QIR_OP_EQ) {
        if (!e->u.op.args || e->u.op.nargs != 1 || !e->u.op.args[0]) {
          set_err(ctx, VERR_ANALYZE_FAIL,
                  "Invalid query structure (malformed '=' predicate).");
          return ERR;
        }

        const QirExpr *rhs = e->u.op.args[0];
        int sens_l = expr_has_sensitive(ctx, main_q, e->u.op.lhs);
        if (sens_l == ERR)
          return ERR;
        int sens_r = expr_has_sensitive(ctx, main_q, rhs);
        if (sens_r == ERR)
          return ERR;

        if (sens_l == YES) {
          if (e->u.op.lhs->kind != QIR_EXPR_COLREF) {
            const char *desc = validator_expr_diag(ctx, e->u.op.lhs);
            set_err(
                ctx, VERR_SENSITIVE_LOC,
                "Sensitive column '%s' must be referenced directly in WHERE.",
                desc);
            return NO;
          }
          if (rhs->kind != QIR_EXPR_PARAM) {
            const char *desc =
                qir_colref_to_str(&e->u.op.lhs->u.colref, &ctx->scratch);
            set_err(ctx, VERR_SENSITIVE_CMP,
                    "Sensitive column '%s' must compare only to parameters.",
                    desc);
            return NO;
          }
        }
        if (sens_r == YES) {
          if (rhs->kind != QIR_EXPR_COLREF) {
            const char *desc = validator_expr_diag(ctx, rhs);
            set_err(
                ctx, VERR_SENSITIVE_LOC,
                "Sensitive column '%s' must be referenced directly in WHERE.",
                desc);
            return NO;
          }
          if (e->u.op.lhs->kind != QIR_EXPR_PARAM) {
            const char *desc = qir_colref_to_str(&rhs->u.colref, &ctx->scratch);
            set_err(ctx, VERR_SENSITIVE_CMP,
                    "Sensitive column '%s' must compare only to parameters.",
                    desc);
            return NO;
          }
        }
        return YES;
      }

      if (e->u.op.cls == QIR_OP_IN) {
        if (!e->u.op.args || e->u.op.nargs == 0) {
          set_err(ctx, VERR_ANALYZE_FAIL,
                  "Invalid query structure (malformed IN predicate).");
          return ERR;
        }

        int sens_l = expr_has_sensitive(ctx, main_q, e->u.op.lhs);
        if (sens_l == ERR)
          return ERR;
        if (sens_l == YES && e->u.op.lhs->kind != QIR_EXPR_COLREF) {
          const char *desc = validator_expr_diag(ctx, e->u.op.lhs);
          set_err(ctx, VERR_SENSITIVE_LOC,
                  "Sensitive column '%s' must be referenced directly in IN().",
                  desc);
          return NO;
        }

        for (uint32_t i = 0; i < e->u.op.nargs; i++) {
          const QirExpr *it = e->u.op.args[i];
          if (!it) {
            set_err(ctx, VERR_ANALYZE_FAIL,
                    "Invalid query structure (NULL IN item).");
            return ERR;
          }
          if (sens_l == YES && it->kind != QIR_EXPR_PARAM) {
            const char *desc =
                qir_colref_to_str(&e->u.op.lhs->u.colref, &ctx->scratch);
            set_err(ctx, VERR_SENSITIVE_CMP,
                    "Sensitive column '%s' must compare only to parameters.",
                    desc);
            return NO;
          }

          int sens_i = expr_has_sensitive(ctx, main_q, it);
          if (sens_i == ERR)
            return ERR;
          if (sens_i == YES) {
            const char *desc =
                qir_colref_to_str(&e->u.op.lhs->u.colref, &ctx->scratch);
            set_err(ctx, VERR_SENSITIVE_CMP,
                    "Sensitive column '%s' cannot appear in IN list.", desc);
            return NO;
          }
        }
        return YES;
      }

      set_err(ctx, VERR_SENSITIVE_CMP,
              "Unsupported WHERE predicate in sensitive mode.");
      return NO;
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
  case QLOC_GROUP_BY:
  case QLOC_HAVING:
  case QLOC_ORDER_BY: {
    int sens = expr_has_sensitive(ctx, main_q, e);
    if (sens == ERR)
      return ERR;
    if (sens == YES) {
      const char *desc = validator_expr_diag(ctx, e);
      if (loc == QLOC_GROUP_BY) {
        set_err(ctx, VERR_SENSITIVE_LOC,
                "GROUP BY cannot reference sensitive column '%s'.", desc);
      }
      if (loc == QLOC_HAVING) {
        set_err(ctx, VERR_SENSITIVE_LOC,
                "HAVING cannot reference sensitive column '%s'.", desc);
      }
      if (loc == QLOC_ORDER_BY) {
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
static AdbxTriStatus validate_query_pass_c_sensitive(ValidatorCtx *ctx,
                                                     const QirQuery *q) {
  if (!ctx || !ctx->db || !ctx->cp || !q)
    return ERR;

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
      int rc = validate_sensitive_expr(ctx, q, j->on, QLOC_JOIN_ON);
      if (rc != YES)
        return rc;
    }
  }

  // WHERE
  if (q->where) {
    int rc = validate_sensitive_expr(ctx, q, q->where, QLOC_WHERE);
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
    int rc = validate_sensitive_expr(ctx, q, si->value, QLOC_SELECT);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_c_sensitive(ctx, si->value);
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
    int rc = validate_sensitive_expr(ctx, q, e, QLOC_GROUP_BY);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_c_sensitive(ctx, e);
    if (rc != YES)
      return rc;
  }

  // HAVING
  if (q->having) {
    int rc = validate_sensitive_expr(ctx, q, q->having, QLOC_HAVING);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_c_sensitive(ctx, q->having);
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
    int rc = validate_sensitive_expr(ctx, q, e, QLOC_ORDER_BY);
    if (rc != YES)
      return rc;
    rc = validate_expr_subqueries_pass_c_sensitive(ctx, e);
    if (rc != YES)
      return rc;
  }

  // Recurse into nested queries to enforce the same Sensitive Mode rules.
  for (uint32_t i = 0; i < q->nctes; i++) {
    int rc = validate_query_pass_c_sensitive(ctx, q->ctes[i]->query);
    if (rc != YES)
      return rc;
  }
  if (q->from_root && q->from_root->kind == QIR_FROM_SUBQUERY) {
    int rc = validate_query_pass_c_sensitive(ctx, q->from_root->u.subquery);
    if (rc != YES)
      return rc;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins ? q->joins[i]->rhs : NULL;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      int rc = validate_query_pass_c_sensitive(ctx, fi->u.subquery);
      if (rc != YES)
        return rc;
    }
  }
  // Note: we intentionally do NOT recurse into union_next branches here.
  // Union branches cannot touch sensitive columns (enforced by
  // VERR_SENSITIVE_IN_UNION in the touch scope check). The father holds
  // LIMIT/DISTINCT/OFFSET metadata for the whole chain, so checking children
  // (which have default limit_value=-1) would produce false negatives.

  return YES;
}

AdbxStatus vq_out_init(ValidateQueryOut *out) {
  if (!out)
    return ERR;

  memset(out, 0, sizeof(*out));
  sb_init(&out->err.msg);
  out->plan.mode = VPLAN_MODE_SELECT;
  out->plan.cols = parr_create(sizeof(ValidatorColPlan));
  if (!out->plan.cols)
    return ERR;

  out->err.code = VERR_NONE;
  return OK;
}

void vq_out_clean(ValidateQueryOut *out) {
  if (!out)
    return;
  parr_destroy(out->plan.cols);
  out->plan.cols = NULL;
  sb_clean(&out->err.msg);
  out->err.code = VERR_NONE;
}

AdbxStatus validate_query(const ValidatorRequest *req, ValidateQueryOut *out) {
  if (!req || !out)
    return ERR;
  if (!req->db || !req->profile || !req->sql)
    return ERR;
  if (req->nparams > 0 && !req->params)
    return ERR;
  if (vq_out_reset(out) != OK)
    return ERR;

  uint8_t *param_used = NULL;
  if (req->nparams > 0) {
    param_used = (uint8_t *)xcalloc(req->nparams, sizeof(*param_used));
  }

  ValidatorCtx ctx = {
      .db = req->db,
      .cp = req->profile,
      .err = &out->err,
      .root_query = NULL,
      .params = req->params,
      .nparams = req->nparams,
      .param_used = param_used,
      .sensitive_mode = false,
  };
  sb_init(&ctx.scratch);

  QirQueryHandle h = {0};
  DbErr db_err;
  if (db_make_query_ir(req->db, req->sql, &h, &db_err) != OK) {
    set_err(&ctx, VERR_PARSE_FAIL, "%s",
            db_err.msg[0] != '\0' ? db_err.msg : "Failed to parse query.");
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  QirQuery *q = h.q;
  if (!q || q->status != QIR_OK) {
    const char *reason =
        (q && q->status_reason) ? q->status_reason : "Invalid query.";
    set_err(&ctx, VERR_UNSUPPORTED_QUERY, reason);
    qir_handle_destroy(&h);
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }
  if (qir_query_is_explain(q) && req->nparams != 0) {
    // EXPLAIN output can echo bound parameter values, so we fail closed and
    // forbid input parameters for EXPLAIN and EXPLAIN ANALYZE.
    set_err(&ctx, VERR_EXPLAIN_PARAMS_FORBIDDEN,
            "EXPLAIN and EXPLAIN ANALYZE do not allow bound input "
            "parameters.");
    qir_handle_destroy(&h);
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }
  // Binding pass
  {
    QirBindErr bind_err = {0};
    AdbxTriStatus brc = bind_query_ir(q, &bind_err);
    if (brc != YES) {
      if (bind_err.code == QIR_BINDERR_UNRESOLVED_COLREF ||
          bind_err.code == QIR_BINDERR_AMBIGUOUS_COLREF) {
        set_err(&ctx, VERR_NO_COLUMN_ALIAS, "%s",
                bind_err.msg[0] != '\0'
                    ? bind_err.msg
                    : "Unable to resolve one or more column references.");
      } else if (bind_err.code == QIR_BINDERR_STAR) {
        set_err(&ctx, VERR_STAR, "%s",
                bind_err.msg[0] != '\0' ? bind_err.msg
                                        : "SELECT * is not allowed.");
      } else if (bind_err.code == QIR_BINDERR_INVALID_CTE) {
        set_err(&ctx, VERR_ANALYZE_FAIL, "%s",
                bind_err.msg[0] != '\0' ? bind_err.msg
                                        : "Invalid CTE shape.");
      } else if (bind_err.code == QIR_BINDERR_UNSUPPORTED) {
        set_err(&ctx, VERR_UNSUPPORTED_QUERY, "%s",
                bind_err.msg[0] != '\0' ? bind_err.msg
                                        : "Unsupported query structure.");
      } else {
        set_err(&ctx, VERR_ANALYZE_FAIL, "%s",
                bind_err.msg[0] != '\0' ? bind_err.msg
                                        : "Unable to bind query references.");
      }
      qir_handle_destroy(&h);
      free(param_used);
      sb_clean(&ctx.scratch);
      return ERR;
    }
  }

  ctx.root_query = q;
  if (qir_walk_touches(q, validator_visit_touch, &ctx) != OK) {
    if (out->err.code == VERR_NONE) {
      set_err(&ctx, VERR_ANALYZE_FAIL,
              "Unable to analyze bound column references.");
    }
    qir_handle_destroy(&h);
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  // Pass A / Pass B / Pass C design. Read the start of validator.c for doc.
  int rc = validate_query_pass_a(&ctx, q);
  if (rc != YES) {
    if (rc == ERR && out->err.code == VERR_NONE) {
      set_err(&ctx, VERR_ANALYZE_FAIL, "Unable to start query analysis.");
    }
    qir_handle_destroy(&h);
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  rc = validate_query_pass_b_params(&ctx, q);
  if (rc != YES) {
    if (rc == ERR && out->err.code == VERR_NONE) {
      set_err(&ctx, VERR_ANALYZE_FAIL,
              "Unable to analyze query parameter usage.");
    }
    qir_handle_destroy(&h);
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  if (ctx.sensitive_mode) {
    rc = validate_query_pass_c_sensitive(&ctx, q);
    if (rc != YES) {
      if (rc == ERR && out->err.code == VERR_NONE) {
        set_err(&ctx, VERR_ANALYZE_FAIL, "Unable to analyze sensitive query.");
      }
      qir_handle_destroy(&h);
      free(param_used);
      sb_clean(&ctx.scratch);
      return ERR;
    }
  }

  if (ctx.nparams > 0) {
    for (uint32_t i = 0; i < ctx.nparams; i++) {
      if (ctx.param_used[i] == 0) {
        set_err(&ctx, VERR_PARAM_UNUSED,
                "Token parameter $%u is not referenced by query.", i + 1u);
        qir_handle_destroy(&h);
        free(param_used);
        sb_clean(&ctx.scratch);
        return ERR;
      }
    }
  }

  rc = validator_build_plan(&ctx, q, &out->plan);
  if (rc != OK) {
    if (out->err.code == VERR_NONE) {
      set_err(&ctx, VERR_ANALYZE_FAIL,
              "Unable to build output plan for validated query.");
    }
    qir_handle_destroy(&h);
    free(param_used);
    sb_clean(&ctx.scratch);
    return ERR;
  }

  qir_handle_destroy(&h);
  free(param_used);
  sb_clean(&ctx.scratch);
  return OK;
}
