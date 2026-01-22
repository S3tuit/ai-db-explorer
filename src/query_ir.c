#include "query_ir.h"

#include <stdlib.h>
#include <string.h>

#include "utils.h"

// ----------------------------
// Handle helpers
// ----------------------------

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
  q->kind = QIR_STMT_SELECT;
  q->limit_value = -1;
  h->q = q;
  return OK;
}

void qir_handle_clean(QirQueryHandle *h) {
  if (!h) return;
  pl_arena_clean(&h->arena);
  h->q = NULL;
}

// ----------------------------
// Touch report freeing
// ----------------------------

void qir_touch_report_destroy(QirTouchReport *tr) {
  if (!tr) return;
  pl_arena_clean(&tr->arena);
  free(tr);
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


/* Appends a new touch recor to the report. */
static int qir_touch_report_add(
    QirTouchReport *tr,
    QirScope scope,
    QirTouchKind kind,
    const QirColRef *col,
    QirTouch **head
) {
  if (!tr || !col) return -1;

  QirTouch *t = (QirTouch *)pl_arena_alloc(&tr->arena, (uint32_t)sizeof(*t));
  if (!t) return -1;

  t->scope = scope;
  t->kind = kind;
  t->col = *col; // shallow copy; identifiers are owned by QueryIR
  t->next = NULL;

  // We build a linked list to avoid repeated array allocations. We flatten
  // into a contiguous array once at the end of extraction.
  if (!*head) {
    *head = t;
  }
  tr->ntouches++;

  if (kind == QIR_TOUCH_UNKNOWN) tr->has_unknown_touches = true;
  return 0;
}

// Forward declaration for mutual recursion (expr can contain subquery; query contains expr).
static void qir_extract_from_query_rec(
    const QirQuery *q,
    QirScope scope,
    QirTouchReport *tr,
    QirTouch **head
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
    QirTouch **head
) {
  if (!e || !tr) return;

  switch (e->kind) {
    case QIR_EXPR_COLREF: {
      QirTouchKind kind = qir_resolve_qualifier_kind(owner_query, &e->u.colref.qualifier);
      if (qir_touch_report_add(tr, scope, kind, &e->u.colref, head) != 0) {
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
        qir_extract_from_expr_rec(owner_query, e->u.funcall.args[i], scope, tr, head);
      }
      break;

    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR:
    case QIR_EXPR_NOT:
      qir_extract_from_expr_rec(owner_query, e->u.bin.l, scope, tr, head);
      qir_extract_from_expr_rec(owner_query, e->u.bin.r, scope, tr, head);
      break;

    case QIR_EXPR_IN:
      // IN(lhs, items...): touches can appear in lhs and in each item.
      qir_extract_from_expr_rec(owner_query, e->u.in_.lhs, scope, tr, head);
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.in_.items[i], scope, tr, head);
      }
      break;

    case QIR_EXPR_SUBQUERY:
      // Subquery introduces a new scope for alias resolution and also should
      // be treated as "nested" for Sensitive Mode rules.
      if (e->u.subquery) {
        qir_extract_from_query_rec(e->u.subquery, QIR_SCOPE_NESTED, tr, head);
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
    QirTouch **head
) {
  if (!q || !tr) return;

  // If backend has already flagged unsupported constructs, carry it through.
  if (q->has_unsupported) tr->has_unsupported = true;

  // Recurse into CTE bodies (always nested relative to the parent query).
  for (uint32_t i = 0; i < q->nctes; i++) {
    const QirCte *cte = q->ctes ? q->ctes[i] : NULL;
    if (!cte || !cte->query) continue;
    qir_extract_from_query_rec(cte->query, QIR_SCOPE_NESTED, tr, head);
  }

  // Recurse into FROM subqueries (nested)
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items ? q->from_items[i] : NULL;
    if (!fi) continue;
    if (fi->kind == QIR_FROM_SUBQUERY && fi->u.subquery) {
      qir_extract_from_query_rec(fi->u.subquery, QIR_SCOPE_NESTED, tr, head);
    }
  }

  // Recurse into JOIN RHS subqueries and JOIN ON expressions
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j) continue;

    if (j->rhs && j->rhs->kind == QIR_FROM_SUBQUERY && j->rhs->u.subquery) {
      qir_extract_from_query_rec(j->rhs->u.subquery, QIR_SCOPE_NESTED, tr, head);
    }

    if (j->on) {
      qir_extract_from_expr_rec(q, j->on, scope, tr, head);
    }
  }

  // SELECT list touches (v1: each item is a colref)
  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si) continue;

    // In this IR, select item value is always a colref.
    QirTouchKind kind = qir_resolve_qualifier_kind(q, &si->value.qualifier);
    if (qir_touch_report_add(tr, scope, kind, &si->value, head) != 0) {
      tr->has_unsupported = true;
    }
  }

  // WHERE touches
  if (q->where) {
    qir_extract_from_expr_rec(q, q->where, scope, tr, head);
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

  QirTouch *head = NULL;

  // Start at top-level query in MAIN scope.
  qir_extract_from_query_rec(q, QIR_SCOPE_MAIN, tr, &head);

  // Flatten the linked list into a contiguous array for cache-friendly
  // traversal by validators. This duplicates the memory for the QirTouch
  // but makes it simpler and faster for validators.
  if (tr->ntouches > 0) {
    QirTouch **arr = (QirTouch **)pl_arena_alloc(
        &tr->arena, (uint32_t)(tr->ntouches * sizeof(*arr)));
    if (!arr) {
      tr->has_unsupported = true;
    } else {
      uint32_t i = 0;
      for (QirTouch *t = head; t && i < tr->ntouches; t = t->next) {
        arr[i++] = t;
      }
      tr->touches = arr;
    }
  }

  // If we saw unknown touches or unsupported constructs, caller may reject.
  // Also propagate backend's unsupported flag at top-level.
  if (q->has_unsupported) tr->has_unsupported = true;

  return tr;
}
