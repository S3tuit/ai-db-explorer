#include "validator.h"
#include "query_ir.h"
#include "vault.h"
#include "utils.h"

#include <string.h>

/* Resets 'old_msg' and writes 'new_msg' into 'old_msg'. */
static int set_err(StrBuf *old_msg, const char *new_msg) {
  if (!old_msg || !new_msg) return ERR;
  sb_reset(old_msg);
  return sb_append_bytes(old_msg, new_msg, strlen(new_msg));
}

static int validate_query_rec(DbBackend *db, const ConnProfile *cp, const QirQuery *q,
                              int sensitive_mode, int is_main, StrBuf *err);

/* Finds and return a QirFromItem using 'alias' in the given query. Returns
 * NULL on no match. */
static const QirFromItem *find_from_alias(const QirQuery *q, const char *alias) {
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
 * NO or ERR. This should be called on each QirColRef and ONLY to asses wheter we're
 * in sensitive mode or not. This is not suitable to understand if a colref
 * contains data of a sensitive column. */
static int colref_is_sensitive(const QirQuery *q, const ConnProfile *cp,
                               const QirColRef *c) {
  if (!q || !cp || !c) return ERR;

  const QirFromItem *fi = find_from_alias(q, c->qualifier.name);
  if (!fi) return ERR;
  // We don't trace back the original relationship because this function will
  // be called on each colref so this returns YES at least one time if there's
  // one (or more) sensitive columns.
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
      set_err(err, "missing alias in FROM item");
      return NO;
    }
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins[i];
    if (!j || !j->rhs || !j->rhs->alias.name || j->rhs->alias.name[0] == '\0') {
      set_err(err, "missing alias in JOIN item");
      return NO;
    }
  }
  return YES;
}

/* Validates all the subqueries embedded inside 'e'. Return YES/NO/ERR. Sets
 * '*err' if it doesn't return YES. */
static int validate_expr_subqueries(DbBackend *db, const ConnProfile *cp,
                                    const QirExpr *e, int sensitive_mode,
                                    StrBuf *err) {
  if (!e) return YES;

  // Any QirExpr may be a subquery so we recursively call this function on
  // all the QirExpr inside 'e'
  switch (e->kind) {
    case QIR_EXPR_SUBQUERY: {
      int rc = validate_query_rec(db, cp, e->u.subquery, sensitive_mode, 0, err);
      return (rc == OK) ? YES : NO;
    }
    case QIR_EXPR_FUNCALL: {
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.funcall.args[i],
                                          sensitive_mode, err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CAST:
      return validate_expr_subqueries(db, cp, e->u.cast.expr, sensitive_mode, err);
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
      int rc = validate_expr_subqueries(db, cp, e->u.bin.l, sensitive_mode, err);
      if (rc != YES) return rc;
      return validate_expr_subqueries(db, cp, e->u.bin.r, sensitive_mode, err);
    }
    case QIR_EXPR_NOT:
      return validate_expr_subqueries(db, cp, e->u.bin.l, sensitive_mode, err);
    case QIR_EXPR_IN: {
      int rc = validate_expr_subqueries(db, cp, e->u.in_.lhs, sensitive_mode, err);
      if (rc != YES) return rc;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        rc = validate_expr_subqueries(db, cp, e->u.in_.items[i], sensitive_mode, err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CASE: {
      if (e->u.case_.arg) {
        int rc = validate_expr_subqueries(db, cp, e->u.case_.arg, sensitive_mode, err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        int rc = validate_expr_subqueries(db, cp, w->when_expr, sensitive_mode, err);
        if (rc != YES) return rc;
        rc = validate_expr_subqueries(db, cp, w->then_expr, sensitive_mode, err);
        if (rc != YES) return rc;
      }
      if (e->u.case_.else_expr) {
        return validate_expr_subqueries(db, cp, e->u.case_.else_expr,
                                        sensitive_mode, err);
      }
      return YES;
    }
    case QIR_EXPR_WINDOWFUNC: {
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.window.func.args[i],
                                          sensitive_mode, err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.window.partition_by[i],
                                          sensitive_mode, err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        int rc = validate_expr_subqueries(db, cp, e->u.window.order_by[i],
                                          sensitive_mode, err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_COLREF:
    case QIR_EXPR_PARAM:
    case QIR_EXPR_LITERAL:
    case QIR_EXPR_UNSUPPORTED:
      // TODO: is this safe?
      return YES;
  }
  return ERR;
}

/* Validates that all function calls in an expression tree are safe to call.
 * Returns YES/NO/ERR and sets err when returning NO/ERR. */
static int validate_expr_functions(DbBackend *db, const QirExpr *e, StrBuf *err) {
  if (!e) return YES;

  switch (e->kind) {
    case QIR_EXPR_FUNCALL: {
      const char *name = e->u.funcall.name.name;
      if (!name || name[0] == '\0') {
        set_err(err, "invalid function name");
        return NO;
      }
      int rc = db_is_function_safe(db, name);
      if (rc != YES) {
        set_err(err, "unsafe function call");
        return NO;
      }
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        rc = validate_expr_functions(db, e->u.funcall.args[i], err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_WINDOWFUNC: {
      const char *name = e->u.window.func.name.name;
      if (!name || name[0] == '\0') {
        set_err(err, "invalid window function name");
        return NO;
      }
      int rc = db_is_function_safe(db, name);
      if (rc != YES) {
        set_err(err, "unsafe function call");
        return NO;
      }
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        rc = validate_expr_functions(db, e->u.window.func.args[i], err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        rc = validate_expr_functions(db, e->u.window.partition_by[i], err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        rc = validate_expr_functions(db, e->u.window.order_by[i], err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CAST:
      return validate_expr_functions(db, e->u.cast.expr, err);
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
      int rc = validate_expr_functions(db, e->u.bin.l, err);
      if (rc != YES) return rc;
      return validate_expr_functions(db, e->u.bin.r, err);
    }
    case QIR_EXPR_NOT:
      return validate_expr_functions(db, e->u.bin.l, err);
    case QIR_EXPR_IN: {
      int rc = validate_expr_functions(db, e->u.in_.lhs, err);
      if (rc != YES) return rc;
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        rc = validate_expr_functions(db, e->u.in_.items[i], err);
        if (rc != YES) return rc;
      }
      return YES;
    }
    case QIR_EXPR_CASE: {
      if (e->u.case_.arg) {
        int rc = validate_expr_functions(db, e->u.case_.arg, err);
        if (rc != YES) return rc;
      }
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        int rc = validate_expr_functions(db, w->when_expr, err);
        if (rc != YES) return rc;
        rc = validate_expr_functions(db, w->then_expr, err);
        if (rc != YES) return rc;
      }
      if (e->u.case_.else_expr) {
        return validate_expr_functions(db, e->u.case_.else_expr, err);
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

/* Validates a query (and nested queries) for required aliases and safe calls. */
static int validate_query_rec(DbBackend *db, const ConnProfile *cp, const QirQuery *q,
                              int sensitive_mode, int is_main, StrBuf *err) {
  if (!db || !cp || !q) return ERR;
  (void)is_main;

  // Even in nested queries, we require aliases for all range items. Without
  // this, a nested query could hide ambiguous references and we would miss it
  // because touch extraction only sees column references.
  if (validate_range_aliases(q, err) != YES) return ERR;

  // Function allowlist enforcement across the query.
  for (uint32_t i = 0; i < q->nselect; i++) {
    if (validate_expr_functions(db, q->select_items[i]->value, err) != YES) return ERR;
    if (validate_expr_subqueries(db, cp, q->select_items[i]->value,
                                 sensitive_mode, err) != YES) return ERR;
  }
  if (q->where && validate_expr_functions(db, q->where, err) != YES) return ERR;
  if (q->where && validate_expr_subqueries(db, cp, q->where,
                                           sensitive_mode, err) != YES) return ERR;
  for (uint32_t i = 0; i < q->n_group_by; i++) {
    if (validate_expr_functions(db, q->group_by[i], err) != YES) return ERR;
    if (validate_expr_subqueries(db, cp, q->group_by[i],
                                 sensitive_mode, err) != YES) return ERR;
  }
  if (q->having && validate_expr_functions(db, q->having, err) != YES) return ERR;
  if (q->having && validate_expr_subqueries(db, cp, q->having,
                                            sensitive_mode, err) != YES) return ERR;
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    if (validate_expr_functions(db, q->order_by[i], err) != YES) return ERR;
    if (validate_expr_subqueries(db, cp, q->order_by[i],
                                 sensitive_mode, err) != YES) return ERR;
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    if (validate_expr_functions(db, q->joins[i]->on, err) != YES) return ERR;
    if (validate_expr_subqueries(db, cp, q->joins[i]->on,
                                 sensitive_mode, err) != YES) return ERR;
  }

  // Recurse into nested queries with is_main=false.
  for (uint32_t i = 0; i < q->nctes; i++) {
    if (validate_query_rec(db, cp, q->ctes[i]->query, sensitive_mode, 0, err) != OK) {
      return ERR;
    }
  }
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items[i];
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      if (validate_query_rec(db, cp, fi->u.subquery, sensitive_mode, 0, err) != OK) {
        return ERR;
      }
    }
  }
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirFromItem *fi = q->joins[i]->rhs;
    if (fi && fi->kind == QIR_FROM_SUBQUERY) {
      if (validate_query_rec(db, cp, fi->u.subquery, sensitive_mode, 0, err) != OK) {
        return ERR;
      }
    }
  }
  return OK;
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

  if (q->has_star) {
    set_err(err_msg, "SELECT * is not allowed");
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

  int rc = validate_sensitive_touches_scope(tr, cp, q, err_msg);
  if (rc != YES) {
    qir_touch_report_destroy(tr);
    qir_handle_destroy(&h);
    return ERR;
  }

  rc = validate_query_rec(db, cp, q, sensitive_mode, 1, err_msg);
  qir_touch_report_destroy(tr);
  qir_handle_destroy(&h);
  return rc;
}
