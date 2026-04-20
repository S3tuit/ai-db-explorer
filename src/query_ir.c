#include "query_ir.h"

#include <string.h>

#include "utils.h"

// ----------------------------
// Handle helpers
// ----------------------------

/* Initializes a handle and allocates a blank QirQuery inside its arena.
 * Ownership: caller owns the handle and must call qir_handle_destroy().
 * Side effects: allocates arena blocks.
 * Returns OK on success, ERR on bad input or allocation failure. */
AdbxStatus qir_handle_init(QirQueryHandle *h) {
  if (!h)
    return ERR;

  uint32_t size = 8192u;
  uint32_t cap = 1048000u; // ~1MB
  if (arena_init(&h->arena, &size, &cap) != OK)
    return ERR;

  QirQuery *q = (QirQuery *)arena_calloc(&h->arena, (uint32_t)sizeof(*q));
  if (!q) {
    arena_clean(&h->arena);
    return ERR;
  }
  memset(q, 0, sizeof(*q));

  q->arena = &h->arena;
  q->status = QIR_OK;
  q->status_reason = NULL;
  q->kind = QIR_STMT_SELECT;
  q->stmt_flags = QIR_STMTF_NONE;
  q->limit_value = -1;
  h->q = q;
  return OK;
}

/* Frees all arena allocations owned by the handle and resets it.
 * Ownership: caller retains the handle object itself.
 * Side effects: frees memory.
 * Returns void. */
void qir_handle_destroy(QirQueryHandle *h) {
  if (!h)
    return;
  arena_clean(&h->arena);
  h->q = NULL;
}

/* Sets query status and (optional) reason once; first status wins.
 * Ownership: copies reason into arena when provided.
 * Side effects: mutates q->status and q->status_reason.
 * Error semantics: no return value; on invalid input it is a no-op. */
void qir_set_status(QirQuery *q, Arena *arena, QirStatus status,
                    const char *reason) {
  if (!q)
    return;
  if (q->status == QIR_OK)
    q->status = status;
  if (q->status != status)
    return;
  if (!q->status_reason && reason) {
    if (arena) {
      q->status_reason = (const char *)arena_add_nul(arena, (void *)reason,
                                                     (uint32_t)strlen(reason));
    } else {
      q->status_reason = reason;
    }
  }
}

// ----------------------------
// Touch walking
// ----------------------------

/* Compares two identifiers for exact equality. */
static AdbxTriStatus qir_ident_eq(const QirIdent *a, const QirIdent *b) {
  if (!a || !b)
    return NO;
  if (!a->name || !b->name)
    return NO;
  return strcmp(a->name, b->name) == 0 ? YES : NO;
}

/* Stack of visible range aliases. The local '*query' exposes 'from_root' plus
 * the join RHS items in [0, njoins_visible). We need the prefix because each
 * JOIN ... ON may reference only the left chain plus the current RHS; later
 * joins are not visible there. */
typedef struct QirRangeFrame {
  const QirQuery *query;
  uint32_t njoins_visible;
  const struct QirRangeFrame *outer;
} QirRangeFrame;

/* Simple stack that tacks the visible CTEs. The binder consider only up to
 * 'nvisible' CTEs from 'query'. */
typedef struct QirCteFrame {
  const QirQuery *query;
  uint32_t nvisible;
  const struct QirCteFrame *outer; // pointer to outer scope
} QirCteFrame;

typedef enum QirBindExprMode {
  QIR_BIND_EXPR_NORMAL = 0,
  QIR_BIND_EXPR_ALLOW_OUTPUT_ALIAS,
} QirBindExprMode;

static AdbxStatus qir_bind_query_rec(QirQuery *q,
                                     const QirRangeFrame *outer_ranges,
                                     const QirCteFrame *outer_ctes,
                                     QirBindErr *out_err);

/* Returns a non NULL, NUL-term string even if 'id' is malformed/NULL. */
static const char *qir_ident_cstr(const QirIdent *id) {
  return (id && id->name && id->name[0] != '\0') ? id->name : "<unknown>";
}

/* Returns YES if 'cr' is the syntactic * / alias.* placeholder. */
static AdbxTriStatus qir_colref_is_star(const QirColRef *cr) {
  if (!cr || !cr->column.name)
    return NO;
  return strcmp(cr->column.name, "*") == 0 ? YES : NO;
}

/* Returns YES if 'e' is a direct star select placeholder. */
static AdbxTriStatus qir_expr_is_star_colref(const QirExpr *e) {
  if (!e || e->kind != QIR_EXPR_COLREF)
    return NO;
  return qir_colref_is_star(&e->u.colref);
}

/* Returns the visible SQL name of one range item in the current query block. */
static const char *qir_from_item_visible_name(const QirFromItem *fi) {
  if (!fi)
    return NULL;
  if (fi->alias.name && fi->alias.name[0] != '\0')
    return fi->alias.name;
  if (fi->kind == QIR_FROM_BASE_REL && fi->u.rel.name.name &&
      fi->u.rel.name.name[0] != '\0') {
    return fi->u.rel.name.name;
  }
  return NULL;
}

/* Returns the exposed output name of one SELECT item, or NULL when the binder
 * cannot recover it safely. */
static const char *qir_select_item_exposed_name(const QirSelectItem *si) {
  if (!si || !si->value)
    return NULL;
  if (si->out_alias.name && si->out_alias.name[0] != '\0')
    return si->out_alias.name;
  if (si->value->kind != QIR_EXPR_COLREF ||
      qir_colref_is_star(&si->value->u.colref) == YES) {
    return NULL;
  }
  if (!si->value->u.colref.column.name ||
      si->value->u.colref.column.name[0] == '\0') {
    return NULL;
  }
  return si->value->u.colref.column.name;
}

/* Checks that all the 'ncols' identifiers in 'cols' are known. */
static AdbxStatus qir_validate_named_columns(const QirIdent *cols,
                                             uint32_t ncols,
                                             const char *ctx_name,
                                             QirBindErr *out_err) {
  if (ncols == 0)
    return OK;
  if (!cols) {
    ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                  "Unable to expand SELECT * because %s has no output names.",
                  ctx_name ? ctx_name : "the source");
    return ERR;
  }
  for (uint32_t i = 0; i < ncols; i++) {
    if (!cols[i].name || cols[i].name[0] == '\0') {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "Unable to expand SELECT * because %s output column %u has "
                    "no name.",
                    ctx_name ? ctx_name : "the source", i + 1);
      return ERR;
    }
  }
  return OK;
}

/* Checks that every projected column name of 'q' is known, else, returns ERR
 * and modifies 'out_err'. */
static AdbxStatus qir_validate_query_projection_names(const QirQuery *q,
                                                      const char *ctx_name,
                                                      QirBindErr *out_err) {
  if (!q)
    return ERR;
  if (q->nselect == 0) {
    ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                  "Unable to expand SELECT * because %s has no projected "
                  "columns.",
                  ctx_name ? ctx_name : "the source");
    return ERR;
  }
  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!qir_select_item_exposed_name(si)) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "Unable to expand SELECT * because %s output column %u has "
                    "no name.",
                    ctx_name ? ctx_name : "the source", i + 1);
      return ERR;
    }
  }
  return OK;
}

/* Validates the declared exposed shape of one CTE after its body has been
 * fully bound. This is general query semantics, not star-specific validation:
 * every later reference to the CTE should see a coherent exposed arity. */
static AdbxStatus qir_validate_cte_shape(const QirCte *cte,
                                         QirBindErr *out_err) {
  if (!cte || !cte->query)
    return ERR;
  if (cte->ncolnames == 0)
    return OK;
  if (!cte->colnames) {
    ADBX_ERR_SETF(out_err, QIR_BINDERR_INVALID_CTE,
                  "CTE '%s' declares output column names but none were parsed.",
                  qir_ident_cstr(&cte->name));
    return ERR;
  }
  if (cte->ncolnames != cte->query->nselect) {
    ADBX_ERR_SETF(out_err, QIR_BINDERR_INVALID_CTE,
                  "CTE '%s' column list does not match its projection.",
                  qir_ident_cstr(&cte->name));
    return ERR;
  }
  for (uint32_t i = 0; i < cte->ncolnames; i++) {
    if (!cte->colnames[i].name || cte->colnames[i].name[0] == '\0') {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_INVALID_CTE,
                    "CTE '%s' output column %u has no name.",
                    qir_ident_cstr(&cte->name), i + 1);
      return ERR;
    }
  }
  return OK;
}

/* Returns the exposed output count of 'fi' if it's an expandable range item,
 * meaning we know the columns exposed by 'fi'. The numer of columns is
 * returned via 'out_ncols' when this returns OK. */
static AdbxStatus qir_expandable_from_item_ncols(const QirFromItem *fi,
                                                 uint32_t *out_ncols,
                                                 QirBindErr *out_err) {
  if (out_ncols)
    *out_ncols = 0;
  if (!fi)
    return ERR;

  switch (fi->kind) {
  case QIR_FROM_BASE_REL:
    if (!fi->binding_cte || !fi->binding_cte->query) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "Unable to expand SELECT * from base relation '%s'.",
                    qir_ident_cstr(&fi->u.rel.name));
      return ERR;
    }
    if (fi->binding_cte->ncolnames > 0) {
      if (qir_validate_named_columns(
              fi->binding_cte->colnames, fi->binding_cte->ncolnames,
              qir_ident_cstr(&fi->binding_cte->name), out_err) != OK) {
        return ERR;
      }
      if (out_ncols)
        *out_ncols = fi->binding_cte->ncolnames;
      return OK;
    }
    if (qir_validate_query_projection_names(
            fi->binding_cte->query, qir_ident_cstr(&fi->binding_cte->name),
            out_err) != OK) {
      return ERR;
    }
    if (out_ncols)
      *out_ncols = fi->binding_cte->query->nselect;
    return OK;
  case QIR_FROM_SUBQUERY:
    if (!fi->u.subquery)
      return ERR;
    if (qir_validate_query_projection_names(fi->u.subquery, "the subquery",
                                            out_err) != OK) {
      return ERR;
    }
    if (out_ncols)
      *out_ncols = fi->u.subquery->nselect;
    return OK;
  case QIR_FROM_VALUES:
    if (fi->u.values.ncolnames == 0) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "Unable to expand SELECT * because the VALUES item has no "
                    "column names.");
      return ERR;
    }
    if (qir_validate_named_columns(fi->u.values.colnames,
                                   fi->u.values.ncolnames, "the VALUES item",
                                   out_err) != OK) {
      return ERR;
    }
    if (out_ncols)
      *out_ncols = fi->u.values.ncolnames;
    return OK;
  case QIR_FROM_UNSUPPORTED:
  default:
    ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                  "Unable to expand SELECT * from an unsupported range item.");
    return ERR;
  }
}

/* Returns the exposed output name at 'idx' from 'fi'. 'fi' should be an
 * already-validated source, else this returns NULL.
 */
static const char *qir_expandable_from_item_colname_at(const QirFromItem *fi,
                                                       uint32_t idx) {
  if (!fi)
    return NULL;
  switch (fi->kind) {
  case QIR_FROM_BASE_REL:
    if (!fi->binding_cte)
      return NULL;
    if (fi->binding_cte->ncolnames > 0) {
      return (fi->binding_cte->colnames && idx < fi->binding_cte->ncolnames)
                 ? fi->binding_cte->colnames[idx].name
                 : NULL;
    }
    if (!fi->binding_cte->query || !fi->binding_cte->query->select_items ||
        idx >= fi->binding_cte->query->nselect) {
      return NULL;
    }
    return qir_select_item_exposed_name(
        fi->binding_cte->query->select_items[idx]);
  case QIR_FROM_SUBQUERY:
    if (!fi->u.subquery || !fi->u.subquery->select_items ||
        idx >= fi->u.subquery->nselect) {
      return NULL;
    }
    return qir_select_item_exposed_name(fi->u.subquery->select_items[idx]);
  case QIR_FROM_VALUES:
    return (fi->u.values.colnames && idx < fi->u.values.ncolnames)
               ? fi->u.values.colnames[idx].name
               : NULL;
  case QIR_FROM_UNSUPPORTED:
  default:
    return NULL;
  }
}

/* Builds one synthetic "qual.col AS col" select item during star expansion. */
static QirSelectItem *
qir_new_expanded_select_item(Arena *arena, const char *qual, const char *col) {
  if (!arena || !qual || qual[0] == '\0' || !col || col[0] == '\0')
    return NULL;
  QirSelectItem *si =
      (QirSelectItem *)arena_calloc(arena, (uint32_t)sizeof(*si));
  QirExpr *e = (QirExpr *)arena_calloc(arena, (uint32_t)sizeof(*e));
  if (!si || !e)
    return NULL;
  e->kind = QIR_EXPR_COLREF;
  e->u.colref.qualifier.name = qual;
  e->u.colref.column.name = col;
  si->value = e;
  si->out_alias.name = col;
  return si;
}

/* Returns YES/NO if 'name' references the table at 'fi'. */
static AdbxTriStatus qir_from_item_matches_visible_name(const QirFromItem *fi,
                                                        const QirIdent *name) {
  if (!fi || !name || !name->name || name->name[0] == '\0')
    return NO;

  // If a table has an alias, SQL requires the use of the alias to reference
  // the table, not the table name itself.
  if (fi->alias.name && fi->alias.name[0] != '\0')
    return qir_ident_eq(&fi->alias, name);

  if (fi->kind == QIR_FROM_BASE_REL && fi->u.rel.name.name &&
      fi->u.rel.name.name[0] != '\0') {
    return qir_ident_eq(&fi->u.rel.name, name);
  }
  return NO;
}

/* Finds a match for the table referenced by 'name' inside one local range
 * frame. This does not walk outer scopes. The frame carries a join prefix so
 * JOIN ... ON binding cannot see aliases introduced by successive joins. */
static AdbxTriStatus qir_find_from_in_frame(const QirRangeFrame *fr,
                                            const QirIdent *name,
                                            const QirFromItem **out_fi,
                                            QirBindErr *out_err) {
  const QirFromItem *match = NULL;
  const QirQuery *q;
  uint32_t njoins_visible;

  if (out_fi)
    *out_fi = NULL;
  if (!fr || !fr->query || !name)
    return ERR;
  q = fr->query;
  njoins_visible = fr->njoins_visible;
  if (njoins_visible > q->njoins)
    njoins_visible = q->njoins;

  // Range-item visibility is local to one query block. Correlated lookup is
  // handled separately by qir_bind_colref() walking the outer frame chain.
  if (qir_from_item_matches_visible_name(q->from_root, name) == YES)
    match = q->from_root;

  for (uint32_t i = 0; i < njoins_visible; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    const QirFromItem *fi = j ? j->rhs : NULL;
    if (qir_from_item_matches_visible_name(fi, name) != YES)
      continue;
    if (match) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_AMBIGUOUS_COLREF,
                    "Ambiguous column qualifier '%s'.", qir_ident_cstr(name));
      return ERR;
    }
    match = fi;
  }

  if (!match)
    return NO;
  if (out_fi)
    *out_fi = match;
  return YES;
}

/* Searches a matching CTE referenced by 'name' inside the linked cte_env
 * chain. Each frame exposes only the prefix [0, nvisible) of its query's CTE
 * list. This is why we use a dedicated QirCteFrame rather than reusing the
 * range frame: inside one WITH clause, earlier CTEs are visible to later ones,
 * but a CTE must not see itself or later siblings. */
static AdbxTriStatus qir_find_cte_in_frame(const QirCteFrame *cte_env,
                                           const QirIdent *name,
                                           const QirCte **out_cte,
                                           QirBindErr *out_err) {
  if (out_cte)
    *out_cte = NULL;
  if (!name || !name->name || name->name[0] == '\0')
    return NO;

  // CTE lookup is a separate namespace from range aliases. We search outward
  // until the first frame that defines the requested CTE name.
  for (const QirCteFrame *fr = cte_env; fr; fr = fr->outer) {
    const QirCte *match = NULL;
    const QirQuery *q = fr->query;
    uint32_t nvisible = fr->nvisible;
    if (!q)
      continue;
    if (nvisible > q->nctes)
      nvisible = q->nctes;
    for (uint32_t i = 0; i < nvisible; i++) {
      const QirCte *cte = q->ctes ? q->ctes[i] : NULL;
      if (!cte || qir_ident_eq(&cte->name, name) != YES)
        continue;
      if (match) {
        ADBX_ERR_SETF(out_err, QIR_BINDERR_AMBIGUOUS_CTE,
                      "Ambiguous CTE reference '%s'.", qir_ident_cstr(name));
        return ERR;
      }
      match = cte;
    }
    if (match) {
      if (out_cte)
        *out_cte = match;
      return YES;
    }
  }

  return NO;
}

/* Modifies the binding values of 'cr' by searching for matches inside
 * 'range_env'. Returns YES if it finds a match, else NO and modifies 'out_err'.
 * Returns ERR if bad input. */
static AdbxTriStatus qir_bind_colref(const QirRangeFrame *range_env,
                                     QirColRef *cr, QirBindErr *out_err) {
  if (!range_env || !cr)
    return ERR;

  cr->binding_from = NULL;
  cr->correlation_depth = 0;

  if (!cr->qualifier.name || cr->qualifier.name[0] == '\0') {
    // Conservative support for bare columns: they are allowed only when the
    // current visible range set contains exactly one local item (from_root)
    // and no correlated outer range scopes are visible. This avoids guessing
    // across joins or nested scopes without schema-aware column lookup.
    if (range_env->outer == NULL && range_env->njoins_visible == 0 &&
        range_env->query && range_env->query->from_root) {
      cr->binding_from = range_env->query->from_root;
      cr->correlation_depth = 0;
      return YES;
    }

    ADBX_ERR_SETF(out_err, QIR_BINDERR_UNRESOLVED_COLREF,
                  "Unable to bind bare column reference '%s' in this scope.",
                  qir_ident_cstr(&cr->column));
    return NO;
  }

  // Nearest visible range scope wins. When we bind against an outer frame, the
  // recorded depth marks the reference as correlated.
  uint32_t depth = 0;
  for (const QirRangeFrame *fr = range_env; fr; fr = fr->outer, depth++) {
    const QirFromItem *fi = NULL;
    AdbxTriStatus rc = qir_find_from_in_frame(fr, &cr->qualifier, &fi, out_err);
    if (rc != NO) {
      if (rc == YES) {
        cr->binding_from = fi;
        cr->correlation_depth = depth;
      } else if (out_err && out_err->code == QIR_BINDERR_NONE) {
        ADBX_ERR_SETF(out_err, QIR_BINDERR_INPUT,
                      "Invalid binder input while resolving '%s.%s'.",
                      qir_ident_cstr(&cr->qualifier),
                      qir_ident_cstr(&cr->column));
      }
      return rc;
    }
  }

  ADBX_ERR_SETF(out_err, QIR_BINDERR_UNRESOLVED_COLREF,
                "Unable to bind column reference '%s.%s'.",
                qir_ident_cstr(&cr->qualifier), qir_ident_cstr(&cr->column));
  return NO;
}

/* Modifies the binding values of 'fi' by searching for matches inside
 * 'cte_env'. */
static AdbxStatus qir_bind_from_item(QirFromItem *fi,
                                     const QirCteFrame *cte_env,
                                     QirBindErr *out_err) {
  if (!fi)
    return OK;

  fi->binding_cte = NULL;

  switch (fi->kind) {
  case QIR_FROM_BASE_REL: {
    // Only unqualified RangeVar names can resolve to a visible CTE. Qualified
    // schema.table syntax always stays a database relation.
    if (!fi->u.rel.schema.name || fi->u.rel.schema.name[0] != '\0' ||
        !fi->u.rel.name.name || fi->u.rel.name.name[0] == '\0') {
      return OK;
    }

    const QirCte *cte = NULL;
    AdbxTriStatus rc =
        qir_find_cte_in_frame(cte_env, &fi->u.rel.name, &cte, out_err);
    if (rc == YES)
      fi->binding_cte = cte;
    else if (rc == ERR) {
      if (out_err && out_err->code == QIR_BINDERR_NONE) {
        ADBX_ERR_SETF(out_err, QIR_BINDERR_INPUT,
                      "Invalid binder input while resolving FROM item '%s'.",
                      qir_ident_cstr(&fi->u.rel.name));
      }
      return ERR;
    }
    return OK;
  }

  case QIR_FROM_SUBQUERY:
  case QIR_FROM_VALUES:
  case QIR_FROM_UNSUPPORTED:
  default:
    return OK;
  }
}

/* Appends the explicit columns exposed by 'fi' to 'dst_items' starting at
 * 'dst_idx' during star expansion. Modifies 'dst_idx' and allocates new memory
 * inside 'q's Arena. */
static AdbxStatus qir_append_expanded_from_item(QirQuery *q,
                                                QirSelectItem **dst_items,
                                                uint32_t *dst_idx,
                                                const QirFromItem *fi,
                                                QirBindErr *out_err) {
  uint32_t ncols = 0;
  const char *qual;

  if (!q || !q->arena || !dst_items || !dst_idx || !fi)
    return ERR;
  qual = qir_from_item_visible_name(fi);
  if (!qual || qual[0] == '\0') {
    ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                  "Unable to expand SELECT * because one source has no visible "
                  "name.");
    return ERR;
  }
  if (qir_expandable_from_item_ncols(fi, &ncols, out_err) != OK)
    return ERR;
  for (uint32_t i = 0; i < ncols; i++) {
    const char *col = qir_expandable_from_item_colname_at(fi, i);
    QirSelectItem *si = NULL;
    if (!col || col[0] == '\0') {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "Unable to expand SELECT * because source '%s' exposes an "
                    "unnamed column.",
                    qual);
      return ERR;
    }
    si = qir_new_expanded_select_item(q->arena, qual, col);
    if (!si)
      return ERR;
    dst_items[*dst_idx] = si;
    (*dst_idx)++;
  }
  return OK;
}

/* Expands SELECT * / alias.* into explicit derived colrefs before expression
 * binding. Only local query-block sources participate in star expansion. */
static AdbxStatus qir_resolve_select(QirQuery *q,
                                     const QirRangeFrame *range_env,
                                     QirBindErr *out_err) {
  bool saw_star = false;
  uint32_t new_nselect = 0;
  QirSelectItem **new_items = NULL;
  uint32_t dst = 0;

  if (!q || !q->arena || !range_env || range_env->query != q)
    return ERR;

  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (si && qir_expr_is_star_colref(si->value) == YES) {
      saw_star = true;
      break;
    }
  }
  if (!saw_star)
    return OK;

  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value || qir_expr_is_star_colref(si->value) != YES) {
      new_nselect++;
      continue;
    }
    const QirColRef *cr = &si->value->u.colref;

    if (cr->qualifier.name && cr->qualifier.name[0] != '\0') {
      const QirFromItem *fi = NULL;
      uint32_t ncols = 0;
      AdbxTriStatus rc =
          qir_find_from_in_frame(range_env, &cr->qualifier, &fi, out_err);
      if (rc == YES) {
        if (qir_expandable_from_item_ncols(fi, &ncols, out_err) != OK)
          return ERR;
        new_nselect += ncols;
        continue;
      }
      if (rc == ERR)
        return ERR;
      ADBX_ERR_SETF(out_err, QIR_BINDERR_UNRESOLVED_COLREF,
                    "Unable to bind column reference '%s.*'.",
                    qir_ident_cstr(&cr->qualifier));
      return ERR;
    }

    // If we see a bare SELECT * with no alias, we have to make sure that all
    // the table (FROM + JOIN) are expandable
    uint32_t star_cols = 0;
    if (q->from_root) {
      uint32_t ncols = 0;
      if (qir_expandable_from_item_ncols(q->from_root, &ncols, out_err) != OK)
        return ERR;
      star_cols += ncols;
    }
    for (uint32_t j = 0; j < range_env->njoins_visible; j++) {
      const QirJoin *join = q->joins ? q->joins[j] : NULL;
      const QirFromItem *fi = join ? join->rhs : NULL;
      uint32_t ncols = 0;
      if (!fi)
        continue;
      if (qir_expandable_from_item_ncols(fi, &ncols, out_err) != OK)
        return ERR;
      star_cols += ncols;
    }
    if (star_cols == 0) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "Unable to expand SELECT * without any FROM source.");
      return ERR;
    }
    new_nselect += star_cols;
  }

  if (new_nselect == 0)
    return OK;
  new_items = (QirSelectItem **)arena_calloc(
      q->arena, (uint32_t)(new_nselect * sizeof(*new_items)));
  if (!new_items)
    return ERR;

  // Copy, as-is, the non stat columns and expand the star columns
  for (uint32_t i = 0; i < q->nselect; i++) {
    QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value || qir_expr_is_star_colref(si->value) != YES) {
      new_items[dst++] = si;
      continue;
    }
    const QirColRef *cr = &si->value->u.colref;

    if (cr->qualifier.name && cr->qualifier.name[0] != '\0') {
      const QirFromItem *fi = NULL;
      AdbxTriStatus rc =
          qir_find_from_in_frame(range_env, &cr->qualifier, &fi, out_err);
      if (rc != YES || qir_append_expanded_from_item(q, new_items, &dst, fi,
                                                     out_err) != OK) {
        return ERR;
      }
      continue;
    }

    if (q->from_root && qir_append_expanded_from_item(
                            q, new_items, &dst, q->from_root, out_err) != OK) {
      return ERR;
    }
    for (uint32_t j = 0; j < range_env->njoins_visible; j++) {
      const QirJoin *join = q->joins ? q->joins[j] : NULL;
      const QirFromItem *fi = join ? join->rhs : NULL;
      if (!fi)
        continue;
      if (qir_append_expanded_from_item(q, new_items, &dst, fi, out_err) != OK)
        return ERR;
    }
  }

  q->select_items = new_items;
  q->nselect = dst;
  return OK;
}

/* Resolves one bare column reference against the current query block's SELECT
 * aliases. Output aliases are local to one query block, so this doesn't search
 * outer range frames. Returns YES and writes one borrowed expression pointer on
 * unique match, NO when there is no alias match, and ERR on ambiguous alias or
 * invalid input. */
static AdbxTriStatus qir_resolve_output_alias(const QirRangeFrame *range_env,
                                              const QirColRef *cr,
                                              QirExpr **resolved_expr,
                                              QirBindErr *out_err) {
  if (resolved_expr)
    *resolved_expr = NULL;
  if (!range_env || !range_env->query || !cr)
    return ERR;
  if (cr->qualifier.name && cr->qualifier.name[0] != '\0')
    return NO;

  const char *name = cr->column.name;
  if (!name || name[0] == '\0')
    return NO;

  QirExpr *match = NULL;
  for (uint32_t i = 0; i < range_env->query->nselect; i++) {
    QirSelectItem *si = range_env->query->select_items
                            ? range_env->query->select_items[i]
                            : NULL;
    if (!si || !si->out_alias.name || !si->value)
      continue;
    if (strcmp(si->out_alias.name, name) != 0)
      continue;
    if (match) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_AMBIGUOUS_COLREF,
                    "Ambiguous output alias '%s'.", name);
      return ERR;
    }
    match = si->value;
  }

  if (!match)
    return NO;
  if (resolved_expr)
    *resolved_expr = match;
  return YES;
}

/* Traverses the expression tree rooted at '*slot' and populates binder
 * metadata. In GROUP BY/HAVING/ORDER BY mode, bare column references are first
 * resolved against local SELECT aliases; if an alias matches, the caller-owned
 * slot is rewritten to point at that SELECT expression and then rebound in
 * normal mode. Returns OK on success, else ERR and writes 'out_err'. */
static AdbxStatus qir_bind_expr_rec(QirExpr **slot,
                                    const QirRangeFrame *range_env,
                                    const QirCteFrame *cte_env,
                                    QirBindExprMode mode, QirBindErr *out_err) {
  QirExpr *e = slot ? *slot : NULL;
  if (!range_env)
    return ERR;
  if (!e | !slot)
    return OK;

  switch (e->kind) {
  case QIR_EXPR_COLREF:
    if (qir_colref_is_star(&e->u.colref) == YES) {
      ADBX_ERR_SETF(out_err, QIR_BINDERR_STAR,
                    "SELECT * must be expanded before expression binding.");
      return ERR;
    }
    if (mode == QIR_BIND_EXPR_ALLOW_OUTPUT_ALIAS) {
      QirExpr *resolved = NULL;
      AdbxTriStatus rc =
          qir_resolve_output_alias(range_env, &e->u.colref, &resolved, out_err);
      if (rc == YES) {
        *slot = resolved;
        return qir_bind_expr_rec(slot, range_env, cte_env, QIR_BIND_EXPR_NORMAL,
                                 out_err);
      }
      if (rc == ERR)
        return ERR;
    }
    return qir_bind_colref(range_env, &e->u.colref, out_err) == YES ? OK : ERR;
  case QIR_EXPR_PARAM:
  case QIR_EXPR_LITERAL:
    return OK;
  case QIR_EXPR_FUNCALL:
    for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
      AdbxStatus rc = qir_bind_expr_rec(&e->u.funcall.args[i], range_env,
                                        cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    return OK;
  case QIR_EXPR_CAST:
    return qir_bind_expr_rec(&e->u.cast.expr, range_env, cte_env, mode,
                             out_err);
  case QIR_EXPR_OP:
    if (e->u.op.lhs) {
      AdbxStatus rc =
          qir_bind_expr_rec(&e->u.op.lhs, range_env, cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.op.nargs; i++) {
      AdbxStatus rc = qir_bind_expr_rec(&e->u.op.args[i], range_env, cte_env,
                                        mode, out_err);
      if (rc != OK)
        return rc;
    }
    return OK;
  case QIR_EXPR_AND:
  case QIR_EXPR_OR: {
    AdbxStatus rc =
        qir_bind_expr_rec(&e->u.bin.l, range_env, cte_env, mode, out_err);
    if (rc != OK)
      return rc;
    return qir_bind_expr_rec(&e->u.bin.r, range_env, cte_env, mode, out_err);
  }
  case QIR_EXPR_NOT:
    return qir_bind_expr_rec(&e->u.bin.l, range_env, cte_env, mode, out_err);
  case QIR_EXPR_CASE:
    if (e->u.case_.arg) {
      AdbxStatus rc =
          qir_bind_expr_rec(&e->u.case_.arg, range_env, cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens ? e->u.case_.whens[i] : NULL;
      if (!w)
        continue;
      AdbxStatus rc =
          qir_bind_expr_rec(&w->when_expr, range_env, cte_env, mode, out_err);
      if (rc != OK)
        return rc;
      rc = qir_bind_expr_rec(&w->then_expr, range_env, cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    return qir_bind_expr_rec(&e->u.case_.else_expr, range_env, cte_env, mode,
                             out_err);
  case QIR_EXPR_WINDOWFUNC:
    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      AdbxStatus rc = qir_bind_expr_rec(&e->u.window.func.args[i], range_env,
                                        cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      AdbxStatus rc = qir_bind_expr_rec(&e->u.window.partition_by[i], range_env,
                                        cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      AdbxStatus rc = qir_bind_expr_rec(&e->u.window.order_by[i], range_env,
                                        cte_env, mode, out_err);
      if (rc != OK)
        return rc;
    }
    return OK;
  case QIR_EXPR_SUBQUERY:
    if (!e->u.subquery)
      return OK;
    // Expression subqueries inherit both outer range aliases and visible CTEs,
    // which is what makes correlated references bindable.
    return qir_bind_query_rec(e->u.subquery, range_env, cte_env, out_err);
  case QIR_EXPR_UNSUPPORTED:
  default:
    ADBX_ERR_SETF(out_err, QIR_BINDERR_UNSUPPORTED,
                  "Unsupported expression cannot be bound.");
    return ERR;
  }
}

/* Traverses 'q' and asigns binding metadata to the different entities owned by
 * 'q'. Returns OK if it can bind all the entities, else ERR. */
static AdbxStatus qir_bind_query_rec(QirQuery *q,
                                     const QirRangeFrame *outer_ranges,
                                     const QirCteFrame *outer_ctes,
                                     QirBindErr *out_err) {
  if (!q)
    return ERR;
  if (q->status != QIR_OK) {
    ADBX_ERR_SETF(out_err, QIR_BINDERR_UNSUPPORTED, "%s",
                  q->status_reason ? q->status_reason
                                   : "Unsupported query structure.");
    return ERR;
  }

  for (uint32_t i = 0; i < q->nctes; i++) {
    QirCte *cte = q->ctes ? q->ctes[i] : NULL;
    if (!cte || !cte->query)
      continue;
    // CTE bodies can see outer CTEs and only the earlier siblings of the same
    // WITH clause. They must not see themselves, later siblings, or any outer
    // query range aliases.
    QirCteFrame cte_prefix = {
        .query = q,
        .nvisible = i,
        .outer = outer_ctes,
    };
    AdbxStatus rc = qir_bind_query_rec(cte->query, NULL, &cte_prefix, out_err);
    if (rc != OK)
      return rc;
    if (qir_validate_cte_shape(cte, out_err) != OK)
      return ERR;
  }

  // The main query body can reference every local CTE plus any CTEs inherited
  // from enclosing query blocks.
  QirCteFrame cte_all = {.query = q, .nvisible = q->nctes, .outer = outer_ctes};
  QirRangeFrame range_all = {
      .query = q,
      .njoins_visible = q->njoins,
      .outer = outer_ranges,
  };
  QirRangeFrame range_join_on = {
      .query = q,
      .njoins_visible = 0,
      .outer = outer_ranges,
  };

  if (q->from_root) {
    AdbxStatus rc = qir_bind_from_item(q->from_root, &cte_all, out_err);
    if (rc != OK)
      return rc;
    if (q->from_root->kind == QIR_FROM_SUBQUERY && q->from_root->u.subquery) {
      // FROM-subqueries are isolated from outer range aliases, they can only
      // reference CTEs
      rc =
          qir_bind_query_rec(q->from_root->u.subquery, NULL, &cte_all, out_err);
      if (rc != OK)
        return rc;
    }
  }

  // Bind joins left-to-right. Each ON clause sees the accumulated left chain
  // plus its own RHS, but not aliases introduced by successive joins.
  for (uint32_t i = 0; i < q->njoins; i++) {
    QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j)
      continue;

    AdbxStatus rc = qir_bind_from_item(j->rhs, &cte_all, out_err);
    if (rc != OK)
      return rc;
    if (j->rhs && j->rhs->kind == QIR_FROM_SUBQUERY && j->rhs->u.subquery) {
      // JOIN rhs subqueries follow the same non-LATERAL rule as
      // FROM-subqueries.
      rc = qir_bind_query_rec(j->rhs->u.subquery, NULL, &cte_all, out_err);
      if (rc != OK)
        return rc;
    }
    if (j->on) {
      range_join_on.njoins_visible = i + 1;
      AdbxStatus rc = qir_bind_expr_rec(&j->on, &range_join_on, &cte_all,
                                        QIR_BIND_EXPR_NORMAL, out_err);
      if (rc != OK)
        return rc;
    }
  }

  // Expand SELECT * / alias.* only after local derived sources have been bound,
  // so we can rewrite the select list using the exact derived output names.
  if (qir_resolve_select(q, &range_all, out_err) != OK)
    return ERR;

  for (uint32_t i = 0; i < q->nselect; i++) {
    QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value)
      continue;
    AdbxStatus rc = qir_bind_expr_rec(&si->value, &range_all, &cte_all,
                                      QIR_BIND_EXPR_NORMAL, out_err);
    if (rc != OK)
      return rc;
  }

  if (q->where) {
    AdbxStatus rc = qir_bind_expr_rec(&q->where, &range_all, &cte_all,
                                      QIR_BIND_EXPR_NORMAL, out_err);
    if (rc != OK)
      return rc;
  }

  for (uint32_t i = 0; i < q->n_group_by; i++) {
    AdbxStatus rc =
        qir_bind_expr_rec(q->group_by ? &q->group_by[i] : NULL, &range_all,
                          &cte_all, QIR_BIND_EXPR_ALLOW_OUTPUT_ALIAS, out_err);
    if (rc != OK)
      return rc;
  }

  if (q->having) {
    AdbxStatus rc =
        qir_bind_expr_rec(&q->having, &range_all, &cte_all,
                          QIR_BIND_EXPR_ALLOW_OUTPUT_ALIAS, out_err);
    if (rc != OK)
      return rc;
  }

  for (uint32_t i = 0; i < q->n_order_by; i++) {
    AdbxStatus rc =
        qir_bind_expr_rec(q->order_by ? &q->order_by[i] : NULL, &range_all,
                          &cte_all, QIR_BIND_EXPR_ALLOW_OUTPUT_ALIAS, out_err);
    if (rc != OK)
      return rc;
  }

  // Union branches are siblings: they share the lead query's outer context and
  // CTE visibility, but do not become outer ranges of one another.
  for (QirQuery *branch = q->union_next; branch; branch = branch->union_next) {
    AdbxStatus rc = qir_bind_query_rec(branch, outer_ranges, &cte_all, out_err);
    if (rc != OK)
      return rc;
  }

  return OK;
}

AdbxTriStatus bind_query_ir(QirQuery *q, QirBindErr *out_err) {
  QirBindErr local_err = {0};
  QirBindErr *err = out_err ? out_err : &local_err;

  ADBX_ERR_CLEAR(err, QIR_BINDERR_NONE);
  if (!q) {
    ADBX_ERR_SETF(err, QIR_BINDERR_INPUT, "Invalid binder input.");
    return ERR;
  }

  if (qir_bind_query_rec(q, NULL, NULL, err) == OK)
    return YES;
  if (err->code == QIR_BINDERR_INPUT)
    return ERR;
  return NO;
}

static QirTouchKind qir_bound_touch_kind(const QirColRef *cr) {
  const QirFromItem *fi = cr ? cr->binding_from : NULL;
  if (!fi)
    return QIR_TOUCH_UNKNOWN;
  if (fi->kind == QIR_FROM_BASE_REL)
    return fi->binding_cte ? QIR_TOUCH_DERIVED : QIR_TOUCH_BASE;
  if (fi->kind == QIR_FROM_SUBQUERY || fi->kind == QIR_FROM_VALUES)
    return QIR_TOUCH_DERIVED;
  return QIR_TOUCH_UNKNOWN;
}

// Forward declaration for mutual recursion (expr can contain subquery; query
// contains expr).
static AdbxStatus qir_walk_query_chain_rec(const QirQuery *q, QirScope scope,
                                           QirTouchFn fn, void *ctx);

/* Walks one expression tree and invokes 'fn' for every colref it encounters.
 * Unsupported expressions are skipped here because structural validation lives
 * elsewhere. */
static AdbxStatus qir_walk_expr_rec(const QirQuery *owner_query,
                                    const QirExpr *e, QirScope scope,
                                    QirTouchFn fn, void *ctx) {
  if (!fn)
    return ERR;
  if (!e)
    return OK;

  switch (e->kind) {
  case QIR_EXPR_COLREF: {
    QirTouchKind kind = qir_bound_touch_kind(&e->u.colref);
    if (e->u.colref.column.name && strcmp(e->u.colref.column.name, "*") == 0) {
      // Star touches cannot be mapped to one direct column, so the walker
      // surfaces them as UNKNOWN.
      kind = QIR_TOUCH_UNKNOWN;
    }
    return fn(scope, owner_query, &e->u.colref, kind, ctx);
  }

  case QIR_EXPR_PARAM:
  case QIR_EXPR_LITERAL:
    return OK;

  case QIR_EXPR_FUNCALL:
    for (uint32_t i = 0; i < e->u.funcall.nargs; i++) {
      AdbxStatus rc =
          qir_walk_expr_rec(owner_query, e->u.funcall.args[i], scope, fn, ctx);
      if (rc != OK)
        return rc;
    }
    return OK;

  case QIR_EXPR_CAST:
    return qir_walk_expr_rec(owner_query, e->u.cast.expr, scope, fn, ctx);

  case QIR_EXPR_OP:
    if (qir_walk_expr_rec(owner_query, e->u.op.lhs, scope, fn, ctx) != OK)
      return ERR;
    for (uint32_t i = 0; i < e->u.op.nargs; i++) {
      AdbxStatus rc =
          qir_walk_expr_rec(owner_query, e->u.op.args[i], scope, fn, ctx);
      if (rc != OK)
        return rc;
    }
    return OK;

  case QIR_EXPR_AND:
  case QIR_EXPR_OR:
    if (qir_walk_expr_rec(owner_query, e->u.bin.l, scope, fn, ctx) != OK)
      return ERR;
    return qir_walk_expr_rec(owner_query, e->u.bin.r, scope, fn, ctx);

  case QIR_EXPR_NOT:
    return qir_walk_expr_rec(owner_query, e->u.bin.l, scope, fn, ctx);

  case QIR_EXPR_CASE:
    if (qir_walk_expr_rec(owner_query, e->u.case_.arg, scope, fn, ctx) != OK)
      return ERR;
    for (uint32_t i = 0; i < e->u.case_.nwhens; i++) {
      QirCaseWhen *w = e->u.case_.whens[i];
      if (!w)
        continue;
      if (qir_walk_expr_rec(owner_query, w->when_expr, scope, fn, ctx) != OK)
        return ERR;
      if (qir_walk_expr_rec(owner_query, w->then_expr, scope, fn, ctx) != OK)
        return ERR;
    }
    return qir_walk_expr_rec(owner_query, e->u.case_.else_expr, scope, fn, ctx);

  case QIR_EXPR_WINDOWFUNC:
    for (uint32_t i = 0; i < e->u.window.func.nargs; i++) {
      AdbxStatus rc = qir_walk_expr_rec(owner_query, e->u.window.func.args[i],
                                        scope, fn, ctx);
      if (rc != OK)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_partition_by; i++) {
      AdbxStatus rc = qir_walk_expr_rec(
          owner_query, e->u.window.partition_by[i], scope, fn, ctx);
      if (rc != OK)
        return rc;
    }
    for (uint32_t i = 0; i < e->u.window.n_order_by; i++) {
      AdbxStatus rc = qir_walk_expr_rec(owner_query, e->u.window.order_by[i],
                                        scope, fn, ctx);
      if (rc != OK)
        return rc;
    }
    return OK;

  case QIR_EXPR_SUBQUERY:
    if (!e->u.subquery)
      return OK;
    return qir_walk_query_chain_rec(e->u.subquery, QIR_SCOPE_NESTED, fn, ctx);

  case QIR_EXPR_UNSUPPORTED:
  default:
    return OK;
  }
}

/* Walks one query block plus its nested queries, but not its union_next
 * siblings. The chain walk is handled separately so later set-op branches are
 * visited exactly once. */
static AdbxStatus qir_walk_one_query_rec(const QirQuery *q, QirScope scope,
                                         QirTouchFn fn, void *ctx) {
  if (!q || !fn)
    return ERR;

  // CTE bodies are nested relative to the query block that defines them.
  for (uint32_t i = 0; i < q->nctes; i++) {
    const QirCte *cte = q->ctes ? q->ctes[i] : NULL;
    if (!cte || !cte->query)
      continue;
    if (qir_walk_query_chain_rec(cte->query, QIR_SCOPE_NESTED, fn, ctx) != OK)
      return ERR;
  }

  if (q->from_root && q->from_root->kind == QIR_FROM_SUBQUERY &&
      q->from_root->u.subquery) {
    if (qir_walk_query_chain_rec(q->from_root->u.subquery, QIR_SCOPE_NESTED, fn,
                                 ctx) != OK) {
      return ERR;
    }
  }

  for (uint32_t i = 0; i < q->njoins; i++) {
    const QirJoin *j = q->joins ? q->joins[i] : NULL;
    if (!j)
      continue;

    if (j->rhs && j->rhs->kind == QIR_FROM_SUBQUERY && j->rhs->u.subquery) {
      if (qir_walk_query_chain_rec(j->rhs->u.subquery, QIR_SCOPE_NESTED, fn,
                                   ctx) != OK) {
        return ERR;
      }
    }

    if (j->on && qir_walk_expr_rec(q, j->on, scope, fn, ctx) != OK)
      return ERR;
  }

  for (uint32_t i = 0; i < q->nselect; i++) {
    const QirSelectItem *si = q->select_items ? q->select_items[i] : NULL;
    if (!si || !si->value)
      continue;
    if (qir_walk_expr_rec(q, si->value, scope, fn, ctx) != OK)
      return ERR;
  }

  if (q->where && qir_walk_expr_rec(q, q->where, scope, fn, ctx) != OK)
    return ERR;

  for (uint32_t i = 0; i < q->n_group_by; i++) {
    QirExpr *e = q->group_by ? q->group_by[i] : NULL;
    if (!e)
      continue;
    if (qir_walk_expr_rec(q, e, scope, fn, ctx) != OK)
      return ERR;
  }

  if (q->having && qir_walk_expr_rec(q, q->having, scope, fn, ctx) != OK)
    return ERR;

  for (uint32_t i = 0; i < q->n_order_by; i++) {
    QirExpr *e = q->order_by ? q->order_by[i] : NULL;
    if (!e)
      continue;
    if (qir_walk_expr_rec(q, e, scope, fn, ctx) != OK)
      return ERR;
  }

  return OK;
}

/* Walks one query plus its union_next chain. Sibling set-op branches stay in
 * the same MAIN/NESTED scope as the lead query. */
static AdbxStatus qir_walk_query_chain_rec(const QirQuery *q, QirScope scope,
                                           QirTouchFn fn, void *ctx) {
  if (!q || !fn)
    return ERR;

  for (const QirQuery *cur = q; cur; cur = cur->union_next) {
    if (qir_walk_one_query_rec(cur, scope, fn, ctx) != OK)
      return ERR;
  }
  return OK;
}

AdbxStatus qir_walk_touches(const QirQuery *q, QirTouchFn fn, void *ctx) {
  if (!q || !fn)
    return ERR;
  return qir_walk_query_chain_rec(q, QIR_SCOPE_MAIN, fn, ctx);
}

/* Renders a FROM item into 'out' and returns out->data (or "" on error).
 * Ownership: caller owns 'out' and controls its lifetime.
 * Side effects: resets and writes into 'out'. */
const char *qir_from_to_str(const QirFromItem *fi, StrBuf *out) {
  if (!out)
    return "";
  sb_reset(out);

  if (!fi) {
    (void)sb_append_bytes(out, "<null>", 6);
    return out->data ? out->data : "";
  }

  switch (fi->kind) {
  case QIR_FROM_BASE_REL: {
    if (fi->binding_cte && fi->binding_cte->name.name &&
        fi->binding_cte->name.name[0] != '\0') {
      (void)sb_append_bytes(out, "cte(", 4);
      (void)sb_append_bytes(out, fi->binding_cte->name.name,
                            strlen(fi->binding_cte->name.name));
      (void)sb_append_bytes(out, ")", 1);
    } else {
      const char *schema = fi->u.rel.schema.name;
      const char *table = fi->u.rel.name.name;
      if (schema && schema[0] != '\0') {
        (void)sb_append_bytes(out, schema, strlen(schema));
        (void)sb_append_bytes(out, ".", 1);
      }
      if (table && table[0] != '\0') {
        (void)sb_append_bytes(out, table, strlen(table));
      } else {
        (void)sb_append_bytes(out, "<unknown>", 9);
      }
    }
    break;
  }
  case QIR_FROM_SUBQUERY:
    (void)sb_append_bytes(out, "subquery", 8);
    break;
  case QIR_FROM_VALUES:
    (void)sb_append_bytes(out, "values", 6);
    break;
  case QIR_FROM_UNSUPPORTED:
  default:
    (void)sb_append_bytes(out, "unsupported", 11);
    break;
  }

  if (fi->alias.name && fi->alias.name[0] != '\0') {
    (void)sb_append_bytes(out, " AS ", 4);
    (void)sb_append_bytes(out, fi->alias.name, strlen(fi->alias.name));
  }

  return sb_to_cstr(out);
}

/* Renders a column reference into 'out' and returns out->data (or "" on error).
 * Ownership: caller owns 'out' and controls its lifetime.
 * Side effects: resets and writes into 'out'. */
const char *qir_colref_to_str(const QirColRef *cr, StrBuf *out) {
  if (!out)
    return "";
  sb_reset(out);

  if (!cr) {
    (void)sb_append_bytes(out, "<null>", 6);
    return out->data ? out->data : "";
  }

  if (cr->qualifier.name && cr->qualifier.name[0] != '\0') {
    (void)sb_append_bytes(out, cr->qualifier.name, strlen(cr->qualifier.name));
    (void)sb_append_bytes(out, ".", 1);
  }

  if (cr->column.name && cr->column.name[0] != '\0') {
    (void)sb_append_bytes(out, cr->column.name, strlen(cr->column.name));
  } else {
    (void)sb_append_bytes(out, "<unknown>", 9);
  }

  return sb_to_cstr(out);
}

/* Renders a function call into 'out' and returns out->data (or "" on error).
 * Ownership: caller owns 'out' and controls its lifetime.
 * Side effects: resets and writes into 'out'. */
const char *qir_func_to_str(const QirFuncCall *fn, StrBuf *out) {
  if (!out)
    return "";
  sb_reset(out);

  if (!fn) {
    (void)sb_append_bytes(out, "<null>", 6);
    return out->data ? out->data : "";
  }

  if (fn->schema.name && fn->schema.name[0] != '\0') {
    (void)sb_append_bytes(out, fn->schema.name, strlen(fn->schema.name));
    (void)sb_append_bytes(out, ".", 1);
  }

  if (fn->name.name && fn->name.name[0] != '\0') {
    (void)sb_append_bytes(out, fn->name.name, strlen(fn->name.name));
  } else {
    (void)sb_append_bytes(out, "<unknown>", 9);
  }

  (void)sb_append_bytes(out, "()", 2);
  return sb_to_cstr(out);
}
