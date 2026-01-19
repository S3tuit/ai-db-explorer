#include "query_ir.h"

#include <stdlib.h>
#include <string.h>

// ----------------------------
// Small helpers
// ----------------------------

static void qir_free_ident(QirIdent *id) {
  if (id && id->name) {
    if (id->name[0] != '\0') {
      free((void *)id->name);
    }
    id->name = NULL;
  }
}

static void qir_free_colref(QirColRef *cr) {
  if (!cr) return;
  qir_free_ident(&cr->qualifier);
  qir_free_ident(&cr->column);
}

static void qir_free_relref(QirRelRef *rr) {
  if (!rr) return;
  qir_free_ident(&rr->schema);
  qir_free_ident(&rr->name);
}

// Forward decls for recursive frees.
static void qir_free_expr(QirExpr *e);
static void qir_free_from_item(QirFromItem *fi);
static void qir_free_join(QirJoin *j);

// ----------------------------
// Freeing expressions
// ----------------------------

static void qir_free_expr(QirExpr *e) {
  if (!e) return;

  switch (e->kind) {
    case QIR_EXPR_COLREF:
      qir_free_colref(&e->u.colref);
      break;

    case QIR_EXPR_PARAM:
      // no heap members
      break;

    case QIR_EXPR_LITERAL:
      if (e->u.lit.kind == QIR_LIT_STRING && e->u.lit.v.s) {
        if (e->u.lit.v.s[0] != '\0') {
          free((void *)e->u.lit.v.s);
        }
        e->u.lit.v.s = NULL;
      }
      break;

    case QIR_EXPR_FUNCALL: {
      qir_free_ident(&e->u.funcall.name);
      if (e->u.funcall.args) {
        for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
          qir_free_expr(e->u.funcall.args[i]);
        }
        free(e->u.funcall.args);
      }
      e->u.funcall.args = NULL;
      e->u.funcall.nargs = 0;
      break;
    }

    case QIR_EXPR_EQ:
    case QIR_EXPR_NE:
    case QIR_EXPR_GT:
    case QIR_EXPR_GE:
    case QIR_EXPR_LT:
    case QIR_EXPR_LE:
    case QIR_EXPR_AND:
    case QIR_EXPR_OR:
    case QIR_EXPR_NOT:
      qir_free_expr(e->u.bin.l);
      qir_free_expr(e->u.bin.r);
      e->u.bin.l = NULL;
      e->u.bin.r = NULL;
      break;

    case QIR_EXPR_IN:
      qir_free_expr(e->u.in_.lhs);
      e->u.in_.lhs = NULL;
      if (e->u.in_.items) {
        for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
          qir_free_expr(e->u.in_.items[i]);
        }
        free(e->u.in_.items);
      }
      e->u.in_.items = NULL;
      e->u.in_.nitems = 0;
      break;

    case QIR_EXPR_SUBQUERY:
      // Owns nested query
      if (e->u.subquery) {
        qir_free_query(e->u.subquery);
        e->u.subquery = NULL;
      }
      break;

    case QIR_EXPR_UNSUPPORTED:
    default:
      // nothing to free
      break;
  }

  free(e);
}

// ----------------------------
// Freeing FROM / JOIN / CTE / Query
// ----------------------------

static void qir_free_from_item(QirFromItem *fi) {
  if (!fi) return;

  qir_free_ident(&fi->alias);

  switch (fi->kind) {
    case QIR_FROM_BASE_REL:
      qir_free_relref(&fi->u.rel);
      break;

    case QIR_FROM_SUBQUERY:
      qir_free_query(fi->u.subquery);
      fi->u.subquery = NULL;
      break;

    case QIR_FROM_CTE_REF:
      qir_free_ident(&fi->u.cte_name);
      break;

    case QIR_FROM_UNSUPPORTED:
    default:
      break;
  }

  free(fi);
}

static void qir_free_join(QirJoin *j) {
  if (!j) return;
  if (j->rhs) {
    qir_free_from_item(j->rhs);
    j->rhs = NULL;
  }
  if (j->on) {
    qir_free_expr(j->on);
    j->on = NULL;
  }
  free(j);
}

void qir_free_query(QirQuery *q) {
  if (!q) return;

  // CTEs
  if (q->ctes) {
    for (uint32_t i = 0; i < q->nctes; i++) {
      QirCte *cte = q->ctes[i];
      if (!cte) continue;
      qir_free_ident(&cte->name);
      if (cte->query) {
        qir_free_query(cte->query);
        cte->query = NULL;
      }
      free(cte);
    }
    free(q->ctes);
  }
  q->ctes = NULL;
  q->nctes = 0;

  // SELECT items
  if (q->select_items) {
    for (uint32_t i = 0; i < q->nselect; i++) {
      QirSelectItem *si = q->select_items[i];
      if (!si) continue;
      qir_free_colref(&si->value);
      qir_free_ident(&si->out_alias);
      free(si);
    }
    free(q->select_items);
  }
  q->select_items = NULL;
  q->nselect = 0;

  // FROM items
  if (q->from_items) {
    for (uint32_t i = 0; i < q->nfrom; i++) {
      qir_free_from_item(q->from_items[i]);
    }
    free(q->from_items);
  }
  q->from_items = NULL;
  q->nfrom = 0;

  // Joins
  if (q->joins) {
    for (uint32_t i = 0; i < q->njoins; i++) {
      qir_free_join(q->joins[i]);
    }
    free(q->joins);
  }
  q->joins = NULL;
  q->njoins = 0;

  // WHERE
  if (q->where) {
    qir_free_expr(q->where);
    q->where = NULL;
  }

  free(q);
}

// ----------------------------
// Touch report freeing
// ----------------------------

void qir_free_touch_report(QirTouchReport *tr) {
  if (!tr) return;
  if (tr->touches) {
    for (uint32_t i = 0; i < tr->ntouches; i++) {
      free(tr->touches[i]);
    }
    free(tr->touches);
  }
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
    const QirColRef *col
) {
  if (!tr || !col) return -1;

  QirTouch *t = (QirTouch *)calloc(1, sizeof(*t));
  if (!t) return -1;

  t->scope = scope;
  t->kind = kind;
  t->col = *col; // shallow copy; identifiers are owned by QueryIR

  uint32_t new_n = tr->ntouches + 1;
  QirTouch **new_arr = (QirTouch **)realloc(tr->touches, new_n * sizeof(*new_arr));
  if (!new_arr) {
    free(t);
    return -1;
  }

  tr->touches = new_arr;
  tr->touches[tr->ntouches] = t;
  tr->ntouches = new_n;

  if (kind == QIR_TOUCH_UNKNOWN) tr->has_unknown_touches = true;
  return 0;
}

// Forward declaration for mutual recursion (expr can contain subquery; query contains expr).
static void qir_extract_from_query_rec(
    const QirQuery *q,
    QirScope scope,
    QirTouchReport *tr
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
    QirTouchReport *tr
) {
  if (!e || !tr) return;

  switch (e->kind) {
    case QIR_EXPR_COLREF: {
      QirTouchKind kind = qir_resolve_qualifier_kind(owner_query, &e->u.colref.qualifier);
      if (qir_touch_report_add(tr, scope, kind, &e->u.colref) != 0) {
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
        qir_extract_from_expr_rec(owner_query, e->u.funcall.args[i], scope, tr);
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
      qir_extract_from_expr_rec(owner_query, e->u.bin.l, scope, tr);
      qir_extract_from_expr_rec(owner_query, e->u.bin.r, scope, tr);
      break;

    case QIR_EXPR_IN:
      // IN(lhs, items...): touches can appear in lhs and in each item.
      qir_extract_from_expr_rec(owner_query, e->u.in_.lhs, scope, tr);
      for (uint32_t i = 0; i < e->u.in_.nitems; i++) {
        qir_extract_from_expr_rec(owner_query, e->u.in_.items[i], scope, tr);
      }
      break;

    case QIR_EXPR_SUBQUERY:
      // Subquery introduces a new scope for alias resolution and also should
      // be treated as "nested" for Sensitive Mode rules.
      if (e->u.subquery) {
        qir_extract_from_query_rec(e->u.subquery, QIR_SCOPE_NESTED, tr);
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
    QirTouchReport *tr
) {
  if (!q || !tr) return;

  // If backend has already flagged unsupported constructs, carry it through.
  if (q->has_unsupported) tr->has_unsupported = true;

  // Recurse into CTE bodies (always nested relative to the parent query).
  for (uint32_t i = 0; i < q->nctes; i++) {
    const QirCte *cte = q->ctes ? q->ctes[i] : NULL;
    if (!cte || !cte->query) continue;
    qir_extract_from_query_rec(cte->query, QIR_SCOPE_NESTED, tr);
  }

  // Recurse into FROM subqueries (nested)
  for (uint32_t i = 0; i < q->nfrom; i++) {
    const QirFromItem *fi = q->from_items ? q->from_items[i] : NULL;
    if (!fi) continue;
    if (fi->kind == QIR_FROM_SUBQUERY && fi->u.subquery) {
      qir_extract_from_query_rec(fi->u.subquery, QIR_SCOPE_NESTED, tr);
    }
  }

  // Recurse into JOIN RHS subqueries and JOIN ON expressions
  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j) continue;

    if (j->rhs && j->rhs->kind == QIR_FROM_SUBQUERY && j->rhs->u.subquery) {
      qir_extract_from_query_rec(j->rhs->u.subquery, QIR_SCOPE_NESTED, tr);
    }

    if (j->on) {
      qir_extract_from_expr_rec(q, j->on, scope, tr);
    }
  }

  // SELECT list touches (v1: each item is a colref)
  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si) continue;

    // In this IR, select item value is always a colref.
    QirTouchKind kind = qir_resolve_qualifier_kind(q, &si->value.qualifier);
    if (qir_touch_report_add(tr, scope, kind, &si->value) != 0) {
      tr->has_unsupported = true;
    }
  }

  // WHERE touches
  if (q->where) {
    qir_extract_from_expr_rec(q, q->where, scope, tr);
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

  QirTouchReport *tr = (QirTouchReport *)calloc(1, sizeof(*tr));
  if (!tr) return NULL;

  // Start at top-level query in MAIN scope.
  qir_extract_from_query_rec(q, QIR_SCOPE_MAIN, tr);

  // If we saw unknown touches or unsupported constructs, caller may reject.
  // Also propagate backend's unsupported flag at top-level.
  if (q->has_unsupported) tr->has_unsupported = true;

  return tr;
}
