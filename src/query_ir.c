#include "query_ir.h"

#include <stdlib.h>
#include <string.h>

#include "utils.h"

// ----------------------------
// Handle helpers
// ----------------------------

/* Initializes a handle and allocates a blank QirQuery inside its arena.
 * Ownership: caller owns the handle and must call qir_handle_destroy().
 * Side effects: allocates arena blocks.
 * Returns OK on success, ERR on bad input or allocation failure. */
int qir_handle_init(QirQueryHandle *h) {
  if (!h) return ERR;

  uint32_t size = 8192u;
  uint32_t cap = 1048000u; // ~1MB
  if (pl_arena_init(&h->arena, &size, &cap) != OK) return ERR;

  QirQuery *q = (QirQuery *)pl_arena_alloc(&h->arena, (uint32_t)sizeof(*q));
  if (!q) {
    pl_arena_clean(&h->arena);
    return ERR;
  }

  q->status = QIR_OK;
  q->status_reason = NULL;
  q->kind = QIR_STMT_SELECT;
  q->limit_value = -1;
  h->q = q;
  return OK;
}

/* Frees all arena allocations owned by the handle and resets it.
 * Ownership: caller retains the handle object itself.
 * Side effects: frees memory.
 * Returns void. */
void qir_handle_destroy(QirQueryHandle *h) {
  if (!h) return;
  pl_arena_clean(&h->arena);
  h->q = NULL;
}

// ----------------------------
// Touch report freeing
// ----------------------------

/* Frees a touch report and its arena allocations.
 * Ownership: caller must not use 'tr' afterwards.
 * Side effects: frees memory.
 * Returns void. */
void qir_touch_report_destroy(QirTouchReport *tr) {
  if (!tr) return;
  pl_arena_clean(&tr->arena);
  free(tr);
}

/* Sets query status and (optional) reason once; first status wins.
 * Ownership: copies reason into arena when provided.
 * Side effects: mutates q->status and q->status_reason.
 * Error semantics: no return value; on invalid input it is a no-op. */
void qir_set_status(QirQuery *q, PlArena *arena, QirStatus status, const char *reason) {
  if (!q) return;
  if (q->status == QIR_OK) q->status = status;
  if (q->status != status) return;
  if (!q->status_reason && reason) {
    if (arena) {
      q->status_reason = (const char *)pl_arena_add(
          arena, (void *)reason, (uint32_t)strlen(reason));
    } else {
      q->status_reason = reason;
    }
  }
}

// ----------------------------
// Touch extraction
// ----------------------------

/* Compares two identifiers for exact equality. */
static bool qir_ident_eq(const QirIdent *a, const QirIdent *b) {
  if (!a || !b) return false;
  if (!a->name || !b->name) return false;
  return strcmp(a->name, b->name) == 0;
}

/* Resolves ORDER BY alias references to SELECT item expressions.
 * Ownership: returned pointer is owned by the QueryIR arena.
 * Side effects: may mark QIR_UNSUPPORTED on ambiguous aliases.
 * Returns the resolved expression or the original expression if no match. */
QirExpr *qir_resolve_order_alias(QirQuery *q, PlArena *arena, QirExpr *expr) {
  if (!q || !expr || expr->kind != QIR_EXPR_COLREF) return expr;
  if (!expr->u.colref.qualifier.name || expr->u.colref.qualifier.name[0] != '\0') {
    return expr;
  }

  const char *name = expr->u.colref.column.name;
  if (!name || name[0] == '\0') return expr;

  QirExpr *resolved = NULL;
  for (uint32_t i = 0; i < q->nselect; i++) {
    QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->out_alias.name) continue;
    if (strcmp(si->out_alias.name, name) == 0) {
      if (resolved) {
        qir_set_status(q, arena, QIR_UNSUPPORTED, "ambiguous ORDER BY alias");
        return expr;
      }
      resolved = si->value;
    }
  }
  return resolved ? resolved : expr;
}

/* Given a column qualifier (the "alias" in alias.column), decide whether it
 * refers to:
 * - a BASE relation (table/view) alias, or
 * - a DERIVED relation alias (subquery or CTE reference), or
 * - UNKNOWN (cannot be resolved safely). */
static QirTouchKind qir_resolve_qualifier_kind(
    const QirQuery *q,
    const QirIdent *qualifier
) {
  if (!q || !qualifier || !qualifier->name || qualifier->name[0] == '\0') {
    return QIR_TOUCH_UNKNOWN;
  }

  // Search FROM base list
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items ? q->from_items[i] : NULL;
    if (!fi) continue;
    if (!fi->alias.name || fi->alias.name[0] == '\0') continue;

    if (qir_ident_eq(&fi->alias, qualifier)) {
      if (fi->kind == QIR_FROM_BASE_REL) return QIR_TOUCH_BASE;
      if (fi->kind == QIR_FROM_SUBQUERY || fi->kind == QIR_FROM_CTE_REF) return QIR_TOUCH_DERIVED;
      return QIR_TOUCH_UNKNOWN;
    }
  }

  // Search join RHS aliases too
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j || !j->rhs) continue;
    const QirFromItem *fi = j->rhs;

    if (!fi->alias.name || fi->alias.name[0] == '\0') continue;
    if (qir_ident_eq(&fi->alias, qualifier)) {
      if (fi->kind == QIR_FROM_BASE_REL) return QIR_TOUCH_BASE;
      if (fi->kind == QIR_FROM_SUBQUERY || fi->kind == QIR_FROM_CTE_REF) return QIR_TOUCH_DERIVED;
      return QIR_TOUCH_UNKNOWN;
    }
  }

  return QIR_TOUCH_UNKNOWN;
}


/* Appends a new touch record to the report. */
static int qir_touch_report_add(
    QirTouchReport *tr,
    QirScope scope,
    QirTouchKind kind,
    const QirColRef *col,
    PtrVec *touches
) {
  if (!tr || !col) return -1;

  QirTouch *t = (QirTouch *)pl_arena_alloc(&tr->arena, (uint32_t)sizeof(*t));
  if (!t) return -1;

  t->scope = scope;
  t->kind = kind;
  t->col = *col; // shallow copy; identifiers are owned by QueryIR
  // Keep a temporary pointer vector and flatten it once at the end.
  if (ptrvec_push(touches, t) != OK) return -1;
  tr->ntouches++;

  if (kind == QIR_TOUCH_UNKNOWN) tr->has_unknown_touches = true;
  return 0;
}

// Forward declaration for mutual recursion (expr can contain subquery; query contains expr).
static void qir_extract_from_query_rec(
    const QirQuery *q,
    QirScope scope,
    QirTouchReport *tr,
    PtrVec *touches
);

/* Walk an expression tree and record every column reference encountered. Adds
 * the columns to 'tr'.
 *
 * The "owner_query" is the query block whose FROM/JOIN aliases define the
 * namespace for resolving qualifiers in this expression. */
static void qir_extract_from_expr_rec(
    const QirQuery *owner_query,
    const QirExpr *e,
    QirScope scope,
    QirTouchReport *tr,
    PtrVec *touches
) {
  if (!e || !tr) return;

  switch (e->kind) {
    case QIR_EXPR_COLREF: {
      QirTouchKind kind = qir_resolve_qualifier_kind(owner_query, &e->u.colref.qualifier);
      if (qir_touch_report_add(tr, scope, kind, &e->u.colref, touches) != 0) {
        tr->has_unsupported = true; // allocation failure treated as "cannot safely proceed"
      }
      break;
    }

    case QIR_EXPR_PARAM:
    case QIR_EXPR_LITERAL:
      // Params and literals do not introduce column touches.
      break;

    case QIR_EXPR_FUNCALL:
      // Even "safe" function calls can reference columns through their arguments.
      // Touch extraction must walk arguments regardless of allowlist decisions.
      for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.funcall.args[i], scope, tr, touches);
      }
      break;

    case QIR_EXPR_CAST:
      // Cast touches come from the underlying expression.
      qir_extract_from_expr_rec(owner_query, e->u.cast.expr, scope, tr, touches);
      break;

    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_LIKE:
    case QIR_EXPR_NOT_LIKE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR:
    case QIR_EXPR_NOT:
      qir_extract_from_expr_rec(owner_query, e->u.bin.l, scope, tr, touches);
      qir_extract_from_expr_rec(owner_query, e->u.bin.r, scope, tr, touches);
      break;

    case QIR_EXPR_IN:
      // IN(lhs, items...): touches can appear in lhs and in each item.
      qir_extract_from_expr_rec(owner_query, e->u.in_.lhs, scope, tr, touches);
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.in_.items[i], scope, tr, touches);
      }
      break;

    case QIR_EXPR_CASE:
      // CASE may reference columns in arg, WHEN, THEN, and ELSE branches.
      qir_extract_from_expr_rec(owner_query, e->u.case_.arg, scope, tr, touches);
      for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
        QirCaseWhen *w = e->u.case_.whens[i];
        if (!w) continue;
        qir_extract_from_expr_rec(owner_query, w->when_expr, scope, tr, touches);
        qir_extract_from_expr_rec(owner_query, w->then_expr, scope, tr, touches);
      }
      qir_extract_from_expr_rec(owner_query, e->u.case_.else_expr, scope, tr, touches);
      break;

    case QIR_EXPR_WINDOWFUNC:
      // Window functions can reference columns in args and window clauses.
      for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.window.func.args[i], scope, tr, touches);
      }
      for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.window.partition_by[i], scope, tr, touches);
      }
      for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.window.order_by[i], scope, tr, touches);
      }
      break;

    case QIR_EXPR_SUBQUERY:
      // Subquery introduces a new scope for alias resolution and also should
      // be treated as "nested" for Sensitive Mode rules.
      if (e->u.subquery) {
        qir_extract_from_query_rec(e->u.subquery, QIR_SCOPE_NESTED, tr, touches);
      }
      break;

    case QIR_EXPR_UNSUPPORTED:
    default:
      tr->has_unsupported = true;
      break;
  }
}

/* Walk a query block, recording touches from:
 * - CTE bodies (always treated as nested scope)
 * - FROM subqueries (nested)
 * - JOIN rhs subqueries (nested)
 * - JOIN ON predicates (current query scope)
 * - SELECT list (current query scope)
 * - WHERE (current query scope) */
static void qir_extract_from_query_rec(
    const QirQuery *q,
    QirScope scope,
    QirTouchReport *tr,
    PtrVec *touches
) {
  if (!q || !tr) return;

  // If backend has already flagged unsupported constructs, carry it through.
  if (q->status == QIR_UNSUPPORTED) tr->has_unsupported = true;

  // Recurse into CTE bodies (always nested relative to the parent query).
  for (uint32_t i = 0; i < q->nctes; i++) {
    const QirCte *cte = q->ctes ? q->ctes[i] : NULL;
    if (!cte || !cte->query) continue;
    qir_extract_from_query_rec(cte->query, QIR_SCOPE_NESTED, tr, touches);
  }

  // Recurse into FROM subqueries (nested)
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items ? q->from_items[i] : NULL;
    if (!fi) continue;
    if (fi->kind == QIR_FROM_SUBQUERY && fi->u.subquery) {
      qir_extract_from_query_rec(fi->u.subquery, QIR_SCOPE_NESTED, tr, touches);
    }
  }

  // Recurse into JOIN RHS subqueries and JOIN ON expressions
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j) continue;

    if (j->rhs && j->rhs->kind == QIR_FROM_SUBQUERY && j->rhs->u.subquery) {
      qir_extract_from_query_rec(j->rhs->u.subquery, QIR_SCOPE_NESTED, tr, touches);
    }

    if (j->on) {
      qir_extract_from_expr_rec(q, j->on, scope, tr, touches);
    }
  }

  // SELECT list touches (expressions can reference columns).
  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si) continue;

    if (!si->value) {
      tr->has_unsupported = true;
      continue;
    }
    qir_extract_from_expr_rec(q, si->value, scope, tr, touches);
  }

  // WHERE touches
  if (q->where) {
    qir_extract_from_expr_rec(q, q->where, scope, tr, touches);
  }

  // GROUP BY touches
  for (uint32_t i = 0; i < q->n_group_by; i++) {
    QirExpr *e = q->group_by ? q->group_by[i] : NULL;
    if (!e) {
      tr->has_unsupported = true;
      continue;
    }
    qir_extract_from_expr_rec(q, e, scope, tr, touches);
  }

  // HAVING touches
  if (q->having) {
    qir_extract_from_expr_rec(q, q->having, scope, tr, touches);
  }

  // ORDER BY touches
  for (uint32_t i = 0; i < q->n_order_by; i++) {
    QirExpr *e = q->order_by ? q->order_by[i] : NULL;
    if (!e) {
      tr->has_unsupported = true;
      continue;
    }
    qir_extract_from_expr_rec(q, e, scope, tr, touches);
  }
}

/* Entry point to produce a QirTouchReport from a QueryIR.
 *
 * The function:
 * - Collects all column references reachable in the IR (main + nested queries).
 * - Classifies each touch as BASE/DERIVED/UNKNOWN, based on qualifier resolution
 *   against the owning query block's aliases.
 * - Sets has_unknown_touches when a qualifier cannot be resolved.
 * - Sets has_unsupported when IR contains UNSUPPORTED nodes or allocation fails. */
QirTouchReport *qir_extract_touches(const QirQuery *q) {
  if (!q) return NULL;

  QirTouchReport *tr = (QirTouchReport *)xcalloc(1, sizeof(*tr));
  if (!tr) return NULL;
  if (pl_arena_init(&tr->arena, NULL, NULL) != OK) {
    free(tr);
    return NULL;
  }

  PtrVec touches = {0};

  // Start at top-level query in MAIN scope.
  qir_extract_from_query_rec(q, QIR_SCOPE_MAIN, tr, &touches);

  // Flatten the pointer list into a contiguous array for cache-friendly
  // traversal by validators. The array is arena-owned.
  if (tr->ntouches > 0) {
    tr->touches = (QirTouch **)ptrvec_flatten(&touches, &tr->arena);
    if (!tr->touches) tr->has_unsupported = true;
  }
  ptrvec_clean(&touches);

  // If we saw unknown touches or unsupported constructs, caller may reject.
  // Also propagate backend's unsupported flag at top-level.
  if (q->status == QIR_UNSUPPORTED) tr->has_unsupported = true;

  return tr;
}
