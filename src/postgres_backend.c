#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdalign.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libpq-fe.h>
#include <pg_query.h>

#include "conn_catalog.h"
#include "json_codec.h"
#include "log.h"
#include "postgres_backend.h"
#include "query_result.h"
#include "safety_policy.h"
#include "string_op.h"
#include "utils.h"

#define PG_QUERY_MAX_BYTES 8192

/* ------------------------------- internals ------------------------------- */

typedef struct PgImpl {
  PGconn *conn;
  SafetyPolicy policy;
  uint8_t policy_applied; // 1 if the policy has already been enforced
                          // at session level, else 0
  Arena *type_name_arena; // owns cached type-name strings and array
  struct PgOidInfo *type_names;
  uint32_t type_names_len;
} PgImpl;

typedef struct PgOidInfo {
  uint32_t oid;
  char *readable_v;
} PgOidInfo;

/* Releases the cached OID->type-name mapping owned by 'p'.
 * It borrows 'p' and does not transfer ownership.
 * Side effects: frees the arena blocks that back cached metadata.
 * Returns void.
 */
static void pg_type_name_cache_reset(PgImpl *p) {
  if (!p)
    return;
  arena_destroy(p->type_name_arena);
  p->type_name_arena = NULL;
  p->type_names = NULL;
  p->type_names_len = 0;
}

/* Loads a sorted Postgres OID->typname cache for one live connection.
 * It borrows 'p' and stores the cache in p->type_name_arena on success.
 * Side effects: executes one read-only catalog query and allocates cache
 * storage with connection lifetime.
 * Returns OK on success, ERR on invalid input, query failure, or allocation/
 * parse error.
 */
static AdbxStatus pg_type_name_cache_load(PgImpl *p) {
  if (!p || !p->conn)
    return ERR;

  // The query returns all the "real" data types; all the OID excluding arrays
  // (prefixed with _) and tables, views, materialized views, foreign tables,
  // partitioned tables
  PGresult *res =
      PQexec(p->conn, "SELECT t.oid, t.typname "
                      "FROM pg_catalog.pg_type t "
                      "WHERE t.typname NOT LIKE '\\_%' "
                      "AND t.typtype <> 'p' "
                      " AND NOT EXISTS ("
                      "   SELECT 1 FROM pg_catalog.pg_class c"
                      "   WHERE c.reltype = t.oid"
                      "      AND c.relkind IN ('r', 'v', 'm', 'f', 'p')"
                      "  ) "
                      "ORDER BY t.oid");
  if (!res)
    return ERR;
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    PQclear(res);
    return ERR;
  }

  int ntuples = PQntuples(res);
  if (ntuples < 0) {
    PQclear(res);
    return ERR;
  }

  Arena *tmp_arena = arena_create(NULL, NULL);
  if (!tmp_arena) {
    PQclear(res);
    return ERR;
  }

  PgOidInfo *tmp_infos = NULL;
  if (ntuples > 0) {
    size_t infos_bytes = (size_t)ntuples * sizeof(*tmp_infos);
    if (infos_bytes > UINT32_MAX) {
      goto clean_n_return;
    }
    tmp_infos = (PgOidInfo *)arena_alloc(tmp_arena, (uint32_t)infos_bytes);
    if (!tmp_infos) {
      goto clean_n_return;
    }
  }

  for (int row = 0; row < ntuples; row++) {
    if (PQgetisnull(res, row, 0) || PQgetisnull(res, row, 1)) {
      goto clean_n_return;
    }

    const char *oid_txt = PQgetvalue(res, row, 0);
    const char *typname = PQgetvalue(res, row, 1);
    if (!oid_txt || !typname) {
      goto clean_n_return;
    }

    errno = 0;
    char *end = NULL;
    unsigned long parsed = strtoul(oid_txt, &end, 10);
    if (errno != 0 || end == oid_txt || *end != '\0' || parsed > UINT32_MAX) {
      goto clean_n_return;
    }

    char *cached_name =
        (char *)arena_add_nul(tmp_arena, (void *)typname, strlen(typname));
    if (!cached_name) {
      goto clean_n_return;
    }

    tmp_infos[row].oid = (uint32_t)parsed;
    tmp_infos[row].readable_v = cached_name;
  }

  PQclear(res);

  pg_type_name_cache_reset(p);
  p->type_name_arena = tmp_arena;
  p->type_names = tmp_infos;
  p->type_names_len = (uint32_t)ntuples;
  return OK;

clean_n_return:
  arena_destroy(tmp_arena);
  PQclear(res);
  return ERR;
}

/* Compares one lookup OID key against one cached PgOidInfo entry for bsearch().
 * It borrows both pointers and does not allocate memory.
 * Returns <0 when key oid is smaller, >0 when larger, 0 on exact match.
 */
static int pg_oid_info_cmp(const void *key_v, const void *elem_v) {
  if (!key_v || !elem_v)
    return 0;

  uint32_t key_oid = *(const uint32_t *)key_v;
  const PgOidInfo *elem = (const PgOidInfo *)elem_v;
  if (key_oid < elem->oid)
    return -1;
  if (key_oid > elem->oid)
    return 1;
  return 0;
}

// --------------------------- QueryIR helpers (Postgres) --------------------

/* Transfers an owned NUL-terminated string into the arena.
 * Ownership: caller transfers ownership of 'owned' to the arena.
 * Side effects: allocates arena memory, frees 'owned'.
 * Returns NULL on error. */
static char *pg_arena_transfer(Arena *a, char *owned) {
  if (!a || !owned) {
    free(owned);
    return NULL;
  }
  char *dst = (char *)arena_add_nul(a, owned, (uint32_t)strlen(owned));
  free(owned);
  return dst;
}

/* Transfers an owned string into the arena after ASCII-lowercasing it.
 * Ownership: returns an arena-owned string; frees the input buffer.
 * Side effects: frees 'owned' and allocates arena memory.
 * Returns NULL on allocation failure or if input is NULL. */
static char *pg_arena_transfer_lower(Arena *a, char *owned) {
  if (!a || !owned) {
    free(owned);
    return NULL;
  }
  for (char *p = owned; *p != '\0'; ++p) {
    *p = (char)tolower((unsigned char)*p);
  }
  return pg_arena_transfer(a, owned);
}

/* Parses an alias object and returns its name or NULL if missing.
 * We accept both a direct alias object and an {"Alias":{...}} wrapper because
 * libpg_query shape varies by version.
 * Ownership: returned string is arena-owned.
 * Side effects: allocates arena memory and frees a temporary string.
 * Returns NULL if alias is absent or invalid. */
static char *pg_parse_alias_name(const JsonGetter *alias_obj, Arena *a) {
  if (!alias_obj || !a)
    return NULL;

  const JsonGetter *src = alias_obj;
  JsonGetter inner = {0};
  if (jsget_object(alias_obj, "Alias", &inner) == YES) {
    src = &inner;
  }

  char *tmp = NULL;
  if (jsget_string_decode_alloc(src, "aliasname", &tmp) != YES)
    return NULL;
  return pg_arena_transfer_lower(a, tmp);
}

/* Gets a decoded string from one of two candidate keys.
 * libpg_query uses "str" vs "sval" for strings depending on node/versions.
 * Ownership: returns a malloc'd string; caller must free or transfer.
 * Side effects: allocates memory.
 * Returns YES/NO/ERR. */
static AdbxTriStatus pg_get_string_field(const JsonGetter *jg, const char *k1,
                                         const char *k2, char **out) {
  int rc = jsget_string_decode_alloc(jg, k1, out);
  if (rc == YES || rc == ERR)
    return rc;
  if (!k2)
    return NO;
  return jsget_string_decode_alloc(jg, k2, out);
}

/* Allocates and initializes a QirQuery inside the arena.
 * Ownership: returned pointer is owned by the arena.
 * Side effects: allocates arena memory.
 * Returns NULL on error. */
static inline QirQuery *pg_qir_new_query(Arena *a) {
  if (!a)
    return NULL;
  QirQuery *q = (QirQuery *)arena_calloc(a, (uint32_t)sizeof(*q));
  if (!q)
    return NULL;
  q->status = QIR_OK;
  q->kind = QIR_STMT_SELECT;
  q->stmt_flags = QIR_STMTF_NONE;
  q->limit_value = -1;
  return q;
}

/* Allocates a QirExpr inside the arena.
 * Ownership: returned pointer is owned by the arena.
 * Side effects: allocates arena memory.
 * Returns NULL on error. */
static inline QirExpr *pg_qir_new_expr(Arena *a, QirExprKind kind) {
  QirExpr *e = (QirExpr *)arena_calloc(a, (uint32_t)sizeof(*e));
  if (!e)
    return NULL;
  e->kind = kind;
  return e;
}

/* Parses a SELECT statement object into QirQuery (forward decl).
 * Ownership: all nodes/arrays are arena-owned.
 * Side effects: may set query flags.
 * Returns OK/ERR on allocation failure. */
static AdbxStatus pg_parse_select_stmt(const JsonGetter *jg, Arena *a,
                                       QirQuery *q);

/* Parses a ColumnRef node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set has_star or mark QIR_UNSUPPORTED.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_colref(const JsonGetter *jg, Arena *a, QirQuery *q) {
  if (!jg || !a || !q)
    return NULL;

  JsonArrIter it = {0};
  int rc = jsget_array_objects_begin(jg, "fields", &it);
  if (rc != YES)
    return NULL;

  char *parts[3] = {0};
  uint32_t nparts = 0;
  int saw_star = 0;

  // The code caps at 2 segments (it allows up to 2 for alias.column). If more
  // than 2 segments arrive (like schema.table.column), it marks the query as
  // unsupported and discards extra segments to avoid undefined behavior.
  JsonGetter elem = {0};
  while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
    JsonGetter sub = {0};
    if (jsget_object(&elem, "A_Star", &sub) == YES) {
      saw_star = 1;
      continue;
    }

    if (jsget_object(&elem, "String", &sub) == YES) {
      char *tmp = NULL;
      if (pg_get_string_field(&sub, "str", "sval", &tmp) != YES)
        return NULL;
      if (nparts < 3) {
        parts[nparts++] = pg_arena_transfer_lower(a, tmp);
        if (!parts[nparts - 1])
          return NULL;
      } else {
        free(tmp);
      }
      continue;
    }

    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported column reference");
  }

  if (saw_star) {
    q->has_star = true;
    if (nparts > 1) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported column reference");
      return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }
    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_COLREF);
    if (!e)
      return NULL;
    e->u.colref.qualifier.name =
        (nparts == 1) ? parts[0] : (char *)arena_add_nul(a, (void *)"", 0);
    e->u.colref.column.name = (char *)arena_add_nul(a, (void *)"*", 1);
    return e;
  }
  if (nparts == 0 || nparts > 2) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported column reference");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_COLREF);
  if (!e)
    return NULL;

  if (nparts == 1) {
    e->u.colref.qualifier.name = (char *)arena_add_nul(a, (void *)"", 0);
    e->u.colref.column.name = parts[0];
  } else {
    e->u.colref.qualifier.name = parts[0];
    e->u.colref.column.name = parts[1];
  }

  return e;
}

/* Parses a simple literal (A_Const).
 * Ownership: returned expression is arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_literal(const JsonGetter *jg, Arena *a, QirQuery *q) {
  if (!jg || !a || !q)
    return NULL;

  JsonGetter vjg = {0};
  if (jsget_object(jg, "val", &vjg) != YES) {
    vjg = *jg; // libpg_query JSON uses top-level ival/fval/sval/isnull
  }

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_LITERAL);
  if (!e)
    return NULL;

  JsonGetter ijg = {0};
  if (jsget_object(&vjg, "ival", &ijg) == YES) {
    int64_t i64 = 0;
    AdbxTriStatus irc = jsget_i64(&ijg, "ival", &i64);
    if (irc == ERR)
      return NULL;
    if (irc == NO) {
      // libpg_query encodes integer zero as an empty {"ival": {}} object for
      // some A_Const shapes. Treating that as 0 preserves valid SQL literals
      // instead of rejecting expressions like COALESCE(col, 0).
      i64 = 0;
    }
    e->u.lit.kind = QIR_LIT_INT64;
    e->u.lit.v.i64 = i64;
    return e;
  }

  JsonGetter fjg = {0};
  if (jsget_object(&vjg, "fval", &fjg) == YES) {
    char *tmp = NULL;
    if (jsget_string_decode_alloc(&fjg, "fval", &tmp) != YES)
      return NULL;
    char *end = NULL;
    double f64 = strtod(tmp, &end);
    if (!end || *end != '\0') {
      free(tmp);
      return NULL;
    }
    free(tmp);
    e->u.lit.kind = QIR_LIT_FLOAT64;
    e->u.lit.v.f64 = f64;
    return e;
  }

  JsonGetter sjg = {0};
  if (jsget_object(&vjg, "sval", &sjg) == YES) {
    char *tmp = NULL;
    if (jsget_string_decode_alloc(&sjg, "sval", &tmp) != YES)
      return NULL;
    char *s = pg_arena_transfer(a, tmp);
    if (!s)
      return NULL;
    e->u.lit.kind = QIR_LIT_STRING;
    e->u.lit.v.s = s;
    return e;
  }

  int isnull = 0;
  if (jsget_bool01(&vjg, "isnull", &isnull) == YES && isnull) {
    e->u.lit.kind = QIR_LIT_NULL;
    return e;
  }

  JsonGetter bjg0 = {0};
  if (jsget_object(&vjg, "boolval", &bjg0) == YES) {
    int b01 = 0;
    if (jsget_bool01(&bjg0, "boolval", &b01) == YES) {
      e->u.lit.kind = QIR_LIT_BOOL;
      e->u.lit.v.b = (b01 != 0);
      return e;
    }
    // maybe it's a bug but sometimes 'false' gets encoded as a "boolval"
    // empty object
    e->u.lit.kind = QIR_LIT_BOOL;
    e->u.lit.v.b = false;
    return e;
  }

  JsonGetter ijg2 = {0};
  if (jsget_object(&vjg, "Integer", &ijg2) == YES) {
    int64_t i64 = 0;
    AdbxTriStatus irc = jsget_i64(&ijg2, "ival", &i64);
    if (irc == ERR)
      return NULL;
    if (irc == NO) {
      // Keep Integer nodes consistent with A_Const handling when zero is
      // encoded as an empty object by the parser JSON.
      i64 = 0;
    }
    e->u.lit.kind = QIR_LIT_INT64;
    e->u.lit.v.i64 = i64;
    return e;
  }

  JsonGetter fjg2 = {0};
  if (jsget_object(&vjg, "Float", &fjg2) == YES) {
    char *tmp = NULL;
    if (pg_get_string_field(&fjg2, "str", "sval", &tmp) != YES)
      return NULL;
    char *end = NULL;
    double f64 = strtod(tmp, &end);
    if (!end || *end != '\0') {
      free(tmp);
      return NULL;
    }
    free(tmp);
    e->u.lit.kind = QIR_LIT_FLOAT64;
    e->u.lit.v.f64 = f64;
    return e;
  }

  JsonGetter sjg2 = {0};
  if (jsget_object(&vjg, "String", &sjg2) == YES) {
    char *tmp = NULL;
    if (pg_get_string_field(&sjg2, "str", "sval", &tmp) != YES)
      return NULL;
    char *s = pg_arena_transfer(a, tmp);
    if (!s)
      return NULL;
    e->u.lit.kind = QIR_LIT_STRING;
    e->u.lit.v.s = s;
    return e;
  }

  JsonGetter njg = {0};
  if (jsget_object(&vjg, "Null", &njg) == YES) {
    e->u.lit.kind = QIR_LIT_NULL;
    return e;
  }

  qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported literal");
  return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
}

/* Parses an expression node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set query flags.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_expr(const JsonGetter *jg, Arena *a, QirQuery *q);

/* Parses a WindowDef node into a QirWindowFunc.
 * Ownership: all arrays are arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED for unsupported shapes.
 * Returns OK/ERR on allocation failure. */
static AdbxStatus pg_parse_window_def(const JsonGetter *wg, Arena *a,
                                      QirQuery *q, QirWindowFunc *wf) {
  if (!wg || !a || !q || !wf)
    return ERR;

  // Named/ref windows are not resolved yet; mark unsupported.
  char *tmp = NULL;
  if (jsget_string_decode_alloc(wg, "refname", &tmp) == YES) {
    if (tmp[0] != '\0')
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported window reference");
    free(tmp);
  }
  if (jsget_string_decode_alloc(wg, "name", &tmp) == YES) {
    if (tmp[0] != '\0')
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported window reference");
    free(tmp);
  }

  // PARTITION BY
  PtrVec parts = {0};
  JsonArrIter it = {0};
  if (jsget_array_objects_begin(wg, "partitionClause", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(wg, &it, &elem)) == YES) {
      QirExpr *expr = pg_parse_expr(&elem, a, q);
      if (!expr) {
        rc = ERR;
        break;
      }
      if (ptrvec_push(&parts, expr) != OK) {
        rc = ERR;
        break;
      }
    }
    if (rc == ERR)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported window clause");
  }
  wf->partition_by = (QirExpr **)ptrvec_flatten(&parts, a);
  wf->n_partition_by = parts.len;
  ptrvec_clean(&parts);

  // ORDER BY (SortBy list)
  PtrVec orders = {0};
  if (jsget_array_objects_begin(wg, "orderClause", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(wg, &it, &elem)) == YES) {
      JsonGetter sjg = {0};
      if (jsget_object(&elem, "SortBy", &sjg) != YES) {
        rc = ERR;
        break;
      }

      JsonGetter njg = {0};
      if (jsget_object(&sjg, "node", &njg) != YES) {
        rc = ERR;
        break;
      }
      QirExpr *expr = pg_parse_expr(&njg, a, q);
      if (!expr) {
        rc = ERR;
        break;
      }
      if (ptrvec_push(&orders, expr) != OK) {
        rc = ERR;
        break;
      }
    }
    if (rc == ERR)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported window clause");
  }
  wf->order_by = (QirExpr **)ptrvec_flatten(&orders, a);
  wf->n_order_by = orders.len;
  ptrvec_clean(&orders);

  // Frame options (bitmask; 0 means no explicit frame).
  int64_t frame = 0;
  int frc = jsget_i64(wg, "frameOptions", &frame);
  if (frc == ERR) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported window frame");
    wf->has_frame = true;
  } else {
    wf->has_frame = (frc == YES && frame != 0);
  }

  return OK;
}

/* Parses a CaseExpr node into a QirExpr.
 * Ownership: returned expression and its children are arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED on malformed or unknown shapes.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_caseexpr(const JsonGetter *jg, Arena *a, QirQuery *q) {
  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_CASE);
  if (!e)
    return NULL;

  e->u.case_.arg = NULL;
  e->u.case_.whens = NULL;
  e->u.case_.nwhens = 0;
  e->u.case_.else_expr = NULL;

  // CASE has an optional argument expression.
  JsonGetter ajg = {0};
  int rc = jsget_object(jg, "arg", &ajg);
  if (rc == ERR)
    return NULL;
  if (rc == YES) {
    e->u.case_.arg = pg_parse_expr(&ajg, a, q);
    if (!e->u.case_.arg)
      return NULL;
  }

  // CASE has a list of WHEN/THEN arms.
  JsonArrIter it = {0};
  rc = jsget_array_objects_begin(jg, "args", &it);
  if (rc == ERR)
    return NULL;
  if (rc == NO) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported column reference");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  PtrVec whens = {0};
  JsonGetter elem = {0};
  while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
    JsonGetter wjg = {0};
    if (jsget_object(&elem, "CaseWhen", &wjg) != YES) {
      rc = ERR;
      break;
    }

    JsonGetter wexpr = {0};
    JsonGetter wres = {0};
    if (jsget_object(&wjg, "expr", &wexpr) != YES) {
      rc = ERR;
      break;
    }
    if (jsget_object(&wjg, "result", &wres) != YES) {
      rc = ERR;
      break;
    }

    QirCaseWhen *w = (QirCaseWhen *)arena_calloc(a, (uint32_t)sizeof(*w));
    if (!w) {
      rc = ERR;
      break;
    }
    w->when_expr = pg_parse_expr(&wexpr, a, q);
    w->then_expr = pg_parse_expr(&wres, a, q);
    if (!w->when_expr || !w->then_expr) {
      rc = ERR;
      break;
    }
    if (ptrvec_push(&whens, w) != OK) {
      rc = ERR;
      break;
    }
  }

  if (rc == ERR) {
    ptrvec_clean(&whens);
    return NULL;
  }
  if (whens.len == 0) {
    ptrvec_clean(&whens);
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported CASE expression");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  uint32_t nwhens = (uint32_t)whens.len;
  e->u.case_.whens = (QirCaseWhen **)ptrvec_flatten(&whens, a);
  ptrvec_clean(&whens);
  if (!e->u.case_.whens)
    return NULL;
  e->u.case_.nwhens = nwhens;

  // Optional ELSE clause.
  JsonGetter ejg = {0};
  rc = jsget_object(jg, "defresult", &ejg);
  if (rc == ERR)
    return NULL;
  if (rc == YES) {
    e->u.case_.else_expr = pg_parse_expr(&ejg, a, q);
    if (!e->u.case_.else_expr)
      return NULL;
  }

  return e;
}

/* Parses a boolean expression list into a left-deep binary tree.
 * Ownership: returned expression is arena-owned.
 * Side effects: none (aside from allocations).
 * Returns NULL on allocation error. */
static QirExpr *pg_fold_bool_expr(Arena *a, QirQuery *q, QirExprKind kind,
                                  QirExpr **items, uint32_t nitems) {
  if (!a || !q || !items || nitems == 0)
    return NULL;
  if (nitems == 1)
    return items[0];

  QirExpr *acc = items[0];
  for (uint32_t i = 1; i < nitems; i++) {
    QirExpr *e = pg_qir_new_expr(a, kind);
    if (!e)
      return NULL;
    e->u.bin.l = acc;
    e->u.bin.r = items[i];
    acc = e;
  }
  return acc;
}

/* Parses a BoolExpr node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_bool_expr(const JsonGetter *jg, Arena *a,
                                   QirQuery *q) {
  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, "args", &it) != YES)
    return NULL;

  PtrVec args = {0};
  JsonGetter elem = {0};
  int rc = 0;
  while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
    QirExpr *arg = pg_parse_expr(&elem, a, q);
    if (!arg)
      break;
    if (ptrvec_push(&args, arg) != OK)
      break;
  }

  if (rc == ERR || args.len == 0) {
    ptrvec_clean(&args);
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported CASE expression");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  QirExprKind kind = QIR_EXPR_UNSUPPORTED;
  char *op = NULL;
  if (jsget_string_decode_alloc(jg, "boolop", &op) == YES) {
    if (strcmp(op, "AND_EXPR") == 0)
      kind = QIR_EXPR_AND;
    else if (strcmp(op, "OR_EXPR") == 0)
      kind = QIR_EXPR_OR;
    else if (strcmp(op, "NOT_EXPR") == 0)
      kind = QIR_EXPR_NOT;
    free(op);
  }

  if (kind == QIR_EXPR_UNSUPPORTED) {
    free(args.items);
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported boolean expression");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  QirExpr *res = NULL;
  if (kind == QIR_EXPR_NOT) {
    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_NOT);
    if (e)
      e->u.bin.l = (QirExpr *)args.items[0];
    res = e;
  } else {
    res = pg_fold_bool_expr(a, q, kind, (QirExpr **)args.items, args.len);
  }

  ptrvec_clean(&args);
  return res;
}

// This generated allowlist is a pure parser-policy input. Keep only operator
// tokens whose semantics we already model safely in
// meta/pg_safe_operators.json.
#include "pg_safe_operator.generated.inc"

/* Compares one operator token key against one sorted allowlist entry.
 * It borrows both inputs and performs no allocations.
 * Returns negative/zero/positive following strcmp() ordering.
 */
static int pg_operator_token_cmp(const void *key_v, const void *elem_v) {
  const char *key = (const char *)key_v;
  const char *const *elem = (const char *const *)elem_v;
  if (!key || !elem || !*elem)
    return 0;
  return strcmp(key, *elem);
}

/* Returns YES when 'op_name' is one supported built-in Postgres operator token.
 * It borrows 'op_name' and performs no allocations.
 * Returns NO when the token is not allowlisted, ERR on invalid input.
 */
static AdbxTriStatus pg_is_supported_operator_token(const char *op_name) {
  if (!op_name || op_name[0] == '\0')
    return ERR;

  const char *const *match = (const char *const *)bsearch(
      op_name, PG_SAFE_OPERATOR_TOKENS,
      sizeof(PG_SAFE_OPERATOR_TOKENS) / sizeof(PG_SAFE_OPERATOR_TOKENS[0]),
      sizeof(PG_SAFE_OPERATOR_TOKENS[0]), pg_operator_token_cmp);
  return match ? YES : NO;
}

/* Records one unsupported-operator diagnostic including the operator token when
 * available. It borrows 'op_name' and copies the final message into 'a'.
 * Side effects: updates q->status and q->status_reason.
 * Returns void.
 */
static void pg_set_unsupported_operator(QirQuery *q, Arena *a,
                                        const char *op_name) {
  if (!q || !a)
    return;

  if (!op_name || op_name[0] == '\0') {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported operator");
    return;
  }

  char msg[128];
  int n = snprintf(msg, sizeof(msg), "unsupported operator '%s'", op_name);
  if (n < 0 || (size_t)n >= sizeof(msg)) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported operator");
    return;
  }
  qir_set_status(q, a, QIR_UNSUPPORTED, msg);
}

/* Extracts one unqualified operator token from a backend name array.
 * It returns the exact libpg_query token (for example '=', '~~*',
 * 'NOT BETWEEN'). Qualified OPERATOR(schema.op) syntax is rejected here.
 * Ownership: on YES, '*out_name' is arena-owned by 'a'.
 * Side effects: allocates arena memory and may set QIR_UNSUPPORTED when the
 * operator-name shape is not supported.
 * Returns YES on success, NO on unsupported operator-name shape, ERR on
 * malformed input or allocation failure.
 */
static AdbxTriStatus pg_parse_name_token_array(const JsonGetter *jg,
                                               const char *field_name, Arena *a,
                                               QirQuery *q,
                                               const char **out_name) {
  if (!jg || !field_name || !a || !q || !out_name)
    return ERR;
  *out_name = NULL;

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, field_name, &it) != YES)
    return ERR;

  JsonGetter elem = {0};
  if (jsget_array_objects_next(jg, &it, &elem) != YES)
    return ERR;

  JsonGetter sjg = {0};
  if (jsget_object(&elem, "String", &sjg) != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported operator name");
    return NO;
  }

  char *tmp = NULL;
  if (pg_get_string_field(&sjg, "str", "sval", &tmp) != YES)
    return ERR;

  if (jsget_array_objects_next(jg, &it, &elem) == YES) {
    free(tmp);
    qir_set_status(q, a, QIR_UNSUPPORTED,
                   "qualified operator syntax is not supported");
    return NO;
  }

  char *name = pg_arena_transfer(a, tmp);
  if (!name)
    return ERR;
  *out_name = name;
  return YES;
}

/* Parses A_Expr.rexpr into a dense array of expression arguments.
 * Scalar rhs values become one-element arrays; rexpr.List.items becomes a
 * multi-argument array preserving source order.
 * Ownership: on YES, '*out_args' is arena-owned by 'a'.
 * Side effects: allocates arena memory.
 * Returns YES on success, ERR on malformed input or allocation failure.
 */
static AdbxTriStatus pg_parse_aexpr_args(const JsonGetter *jg, Arena *a,
                                         QirQuery *q, QirExpr ***out_args,
                                         uint32_t *out_nargs) {
  if (!jg || !a || !q || !out_args || !out_nargs)
    return ERR;
  *out_args = NULL;
  *out_nargs = 0;

  JsonGetter rjg = {0};
  if (jsget_object(jg, "rexpr", &rjg) != YES)
    return ERR;

  JsonGetter listjg = {0};
  if (jsget_object(&rjg, "List", &listjg) == YES) {
    PtrVec args = {0};
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(&listjg, "items", &it) != YES)
      return ERR;

    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(&listjg, &it, &elem)) == YES) {
      QirExpr *arg = pg_parse_expr(&elem, a, q);
      if (!arg) {
        ptrvec_clean(&args);
        return ERR;
      }
      if (ptrvec_push(&args, arg) != OK) {
        ptrvec_clean(&args);
        return ERR;
      }
    }
    if (rc == ERR) {
      ptrvec_clean(&args);
      return ERR;
    }

    *out_args = (QirExpr **)ptrvec_flatten(&args, a);
    *out_nargs = args.len;
    ptrvec_clean(&args);
    return YES;
  }

  QirExpr *arg = pg_parse_expr(&rjg, a, q);
  if (!arg)
    return ERR;

  QirExpr **arr = (QirExpr **)arena_calloc(a, (uint32_t)sizeof(QirExpr *));
  if (!arr)
    return ERR;
  arr[0] = arg;
  *out_args = arr;
  *out_nargs = 1;
  return YES;
}

/* Parses an A_Expr node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_aexpr(const JsonGetter *jg, Arena *a, QirQuery *q) {
  char *akind = NULL;
  int krc = jsget_string_decode_alloc(jg, "kind", &akind);
  if (krc == ERR)
    return NULL;
  if (krc != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported operator expression");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  // lexpr is absent for unary prefix operators (e.g. "-x", "+x").
  QirExpr *lhs = NULL;
  JsonGetter ljg = {0};
  if (jsget_object(jg, "lexpr", &ljg) == YES) {
    lhs = pg_parse_expr(&ljg, a, q);
    if (!lhs) {
      free(akind);
      return NULL;
    }
  }

  const char *op_name = NULL;
  AdbxTriStatus nrc = pg_parse_name_token_array(jg, "name", a, q, &op_name);
  if (nrc == ERR) {
    free(akind);
    return NULL;
  }
  if (nrc != YES) {
    free(akind);
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  AdbxTriStatus orc = pg_is_supported_operator_token(op_name);
  if (orc == ERR) {
    free(akind);
    return NULL;
  }
  if (orc != YES) {
    free(akind);
    pg_set_unsupported_operator(q, a, op_name);
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  QirOpClass cls = QIR_OP_OTHER;
  if (strcmp(akind, "AEXPR_OP") == 0 && strcmp(op_name, "=") == 0)
    cls = QIR_OP_EQ;
  else if (strcmp(akind, "AEXPR_IN") == 0 && strcmp(op_name, "=") == 0)
    cls = QIR_OP_IN;
  free(akind);

  QirExpr **args = NULL;
  uint32_t nargs = 0;
  if (pg_parse_aexpr_args(jg, a, q, &args, &nargs) != YES)
    return NULL;

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_OP);
  if (!e)
    return NULL;
  e->u.op.cls = cls;
  e->u.op.lhs = lhs;
  e->u.op.args = args;
  e->u.op.nargs = nargs;
  e->u.op.op_name = op_name;
  return e;
}

/* Parses a SubLink node into either a generic operator wrapper or a bare
 * subquery expression.
 * Ownership: returned expressions are arena-owned by 'a'.
 * Returns the parsed expression on success, NULL on allocation/parse failure.
 */
static QirExpr *pg_parse_sublink(const JsonGetter *jg, Arena *a, QirQuery *q) {
  if (!jg || !a || !q)
    return NULL;

  JsonGetter subjg = {0};
  if (jsget_object(jg, "subselect", &subjg) != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported subquery");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }
  JsonGetter seljg = {0};
  if (jsget_object(&subjg, "SelectStmt", &seljg) != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported subquery");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }
  QirQuery *sq = pg_qir_new_query(a);
  if (!sq)
    return NULL;
  pg_parse_select_stmt(&seljg, a, sq);

  QirExpr *subexpr = pg_qir_new_expr(a, QIR_EXPR_SUBQUERY);
  if (!subexpr)
    return NULL;
  subexpr->u.subquery = sq;

  char *sublink_type = NULL;
  if (jsget_string_decode_alloc(jg, "subLinkType", &sublink_type) != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported subquery");
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
  }

  const char *op_name = pg_arena_transfer(a, sublink_type);
  if (!op_name)
    return NULL;

  JsonGetter tjg = {0};
  bool has_testexpr = (jsget_object(jg, "testexpr", &tjg) == YES);
  QirExpr *lhs = NULL;
  if (has_testexpr) {
    lhs = pg_parse_expr(&tjg, a, q);
    if (!lhs)
      return NULL;
  }

  QirExpr **args = (QirExpr **)arena_calloc(a, (uint32_t)sizeof(QirExpr *));
  if (!args)
    return NULL;
  args[0] = subexpr;

  // We parse [NOT] IN (SELECT ...) as a QIR_OP_IN.
  // We parse [NOT] EXISTS/ANY as a QIR_OP_OTHER.
  QirOpClass cls = QIR_OP_OTHER;
  if (has_testexpr && strcmp(op_name, "ANY_SUBLINK") == 0) {
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "operName", &it) == YES) {
      const char *oper_name = NULL;
      AdbxTriStatus orc =
          pg_parse_name_token_array(jg, "operName", a, q, &oper_name);
      if (orc == ERR)
        return NULL;
      if (orc != YES)
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    } else {
      cls = QIR_OP_IN;
    }
  } else if (has_testexpr) {
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "operName", &it) == YES) {
      const char *oper_name = NULL;
      AdbxTriStatus orc =
          pg_parse_name_token_array(jg, "operName", a, q, &oper_name);
      if (orc == ERR)
        return NULL;
      if (orc != YES)
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);

      orc = pg_is_supported_operator_token(oper_name);
      if (orc == ERR)
        return NULL;
      if (orc != YES) {
        pg_set_unsupported_operator(q, a, oper_name);
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
      }
    }
  }

  QirExpr *op = pg_qir_new_expr(a, QIR_EXPR_OP);
  if (!op)
    return NULL;
  op->u.op.cls = cls;
  op->u.op.lhs = lhs;
  op->u.op.args = args;
  op->u.op.nargs = 1;
  op->u.op.op_name = op_name;
  return op;
}

/* Builds an INT64 literal expression with the provided value.
 * Ownership: returned expression is arena-owned by 'a'.
 * Returns the new literal expression on success, NULL on allocation failure.
 */
static QirExpr *pg_new_i64_literal(Arena *a, int64_t value) {
  if (!a)
    return NULL;

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_LITERAL);
  if (!e)
    return NULL;
  e->u.lit.kind = QIR_LIT_INT64;
  e->u.lit.v.i64 = value;
  return e;
}

/* Wraps 'then_expr' behind a searched CASE WHEN 'cond_expr' THEN ... END.
 * It borrows the already arena-owned child expressions and returns a new
 * arena-owned CASE node that references them without taking over ownership.
 * Side effects: allocates one CASE expression and one WHEN clause in 'a'.
 * Returns the wrapped CASE expression on success, NULL on allocation failure.
 */
static QirExpr *pg_wrap_expr_with_filter_case(Arena *a, QirExpr *cond_expr,
                                              QirExpr *then_expr) {
  if (!a || !cond_expr || !then_expr)
    return NULL;

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_CASE);
  if (!e)
    return NULL;

  QirCaseWhen *w = (QirCaseWhen *)arena_calloc(a, (uint32_t)sizeof(*w));
  if (!w)
    return NULL;
  w->when_expr = cond_expr;
  w->then_expr = then_expr;

  QirCaseWhen **whens =
      (QirCaseWhen **)arena_calloc(a, (uint32_t)sizeof(*whens));
  if (!whens)
    return NULL;
  whens[0] = w;

  e->u.case_.arg = NULL;
  e->u.case_.whens = whens;
  e->u.case_.nwhens = 1;
  e->u.case_.else_expr = NULL;
  return e;
}

/* Rewrites PostgreSQL aggregate FILTER syntax into CASE-wrapped arguments.
 * It borrows 'filter_jg' and mutates the caller-owned temporary args vector and
 * star flag in place before they are flattened into the final QirFuncCall.
 * Side effects: parses the filter expression, allocates CASE/literal nodes in
 * 'a', and may mark the query unsupported when FILTER has no argument to wrap.
 * Returns OK on success, ERR on allocation/parse failure.
 */
static AdbxStatus pg_normalize_filter_to_case(const JsonGetter *filter_jg,
                                              Arena *a, QirQuery *q,
                                              PtrVec *args, int *is_star01) {
  if (!filter_jg || !a || !q || !args || !is_star01)
    return ERR;

  QirExpr *filter_expr = pg_parse_expr(filter_jg, a, q);
  if (!filter_expr)
    return ERR;

  if (*is_star01 != 0) {
    if (args->len != 0) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported FILTER aggregate");
      return OK;
    }

    // COUNT(*) FILTER (...) becomes COUNT(CASE WHEN ... THEN 1 END) in IR so
    // the validator can analyze the filter without needing a dedicated node.
    QirExpr *one = pg_new_i64_literal(a, 1);
    if (!one)
      return ERR;
    QirExpr *wrapped = pg_wrap_expr_with_filter_case(a, filter_expr, one);
    if (!wrapped)
      return ERR;

    if (ptrvec_push(args, wrapped) != OK)
      return ERR;
    *is_star01 = 0;
    return OK;
  }

  if (args->len == 0) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported FILTER aggregate");
    return OK;
  }

  for (uint32_t i = 0; i < args->len; i++) {
    QirExpr *wrapped = pg_wrap_expr_with_filter_case(a, filter_expr,
                                                     (QirExpr *)args->items[i]);
    if (!wrapped)
      return ERR;
    args->items[i] = wrapped;
  }
  return OK;
}

/* Parses a FuncCall node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_func_call(const JsonGetter *jg, Arena *a,
                                   QirQuery *q) {
  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, "funcname", &it) != YES)
    return NULL;
  // these will represent malloc'd strings that will have to be transferred
  // to the arena
  char *fname_to_tr = NULL;
  char *schema_to_tr = NULL;

  JsonGetter elem = {0};
  // the first string is the funcion name if there are no more strings, or
  // it's the schema name if there's another string inside the object
  if (jsget_array_objects_next(jg, &it, &elem) != YES)
    goto fail;
  JsonGetter sjg = {0};
  if (jsget_object(&elem, "String", &sjg) != YES)
    goto fail;
  if (pg_get_string_field(&sjg, "str", "sval", &fname_to_tr) != YES)
    goto fail;
  // check if there are two strings so the one we found before was the schema
  // and not the fname
  int rc;
  if ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
    schema_to_tr = fname_to_tr;
    if (jsget_object(&elem, "String", &sjg) != YES)
      goto fail;
    if (pg_get_string_field(&sjg, "str", "sval", &fname_to_tr) != YES)
      goto fail;
  }
  if (rc == ERR)
    goto fail;

  // we don't expect more than 2 strings
  if (jsget_array_objects_next(jg, &it, &elem) != NO)
    goto fail;

  char *fname = pg_arena_transfer_lower(a, fname_to_tr);
  if (!fname)
    goto fail;
  fname_to_tr = NULL; // avoid double-free

  char *schema;
  if (schema_to_tr) {
    schema = pg_arena_transfer_lower(a, schema_to_tr);
  } else {
    schema = arena_add_nul(a, (void *)"", 0);
  }
  if (!schema)
    goto fail;
  schema_to_tr = NULL;

  QirFuncCall fc = {0};
  fc.schema.name = schema;
  fc.name.name = fname;

  // parse args
  PtrVec args = {0};
  AdbxTriStatus args_rc = jsget_array_objects_begin(jg, "args", &it);
  if (args_rc == ERR)
    goto fail;
  if (args_rc == YES) {
    JsonGetter arg = {0};
    while ((rc = jsget_array_objects_next(jg, &it, &arg)) == YES) {
      QirExpr *ae = pg_parse_expr(&arg, a, q);
      if (!ae) {
        rc = ERR;
        break;
      }
      if (ptrvec_push(&args, ae) != OK) {
        rc = ERR;
        break;
      }
    }
  }
  if (rc == ERR) {
    ptrvec_clean(&args);
    goto fail;
  }
  int is_distinct = 0;
  if (jsget_bool01(jg, "agg_distinct", &is_distinct) == YES) {
    fc.is_distinct = (is_distinct != 0);
  } else {
    fc.is_distinct = false;
  }
  int is_star = 0;
  if (jsget_bool01(jg, "agg_star", &is_star) == YES) {
    fc.is_star = (is_star != 0);
  } else {
    fc.is_star = false;
  }

  // Normalizes FILTER clauses into CASE-wrapped aggregate arguments.
  // That's because we don't care about modelling SQL perfectly, we just have
  // to model the safety of the SQL.
  JsonGetter filterjg = {0};
  AdbxTriStatus frc = jsget_object(jg, "agg_filter", &filterjg);
  if (frc == ERR) {
    ptrvec_clean(&args);
    goto fail;
  }
  if (frc == YES) {
    if (pg_normalize_filter_to_case(&filterjg, a, q, &args, &is_star) != OK) {
      ptrvec_clean(&args);
      goto fail;
    }
  }
  fc.is_star = (is_star != 0);

  if (args.len > 0) {
    fc.args = (QirExpr **)ptrvec_flatten(&args, a);
    fc.nargs = args.len;
    if (!fc.args) {
      ptrvec_clean(&args);
      goto fail;
    }
  }
  ptrvec_clean(&args);

  // Window function: FuncCall with OVER clause.
  JsonGetter overjg = {0};
  if (jsget_object(jg, "over", &overjg) == YES) {
    JsonGetter wdjg = {0};
    if (jsget_object(&overjg, "WindowDef", &wdjg) == YES) {
      overjg = wdjg;
    }

    QirExpr *we = pg_qir_new_expr(a, QIR_EXPR_WINDOWFUNC);
    if (!we)
      goto fail;
    we->u.window.func = fc;
    we->u.window.partition_by = NULL;
    we->u.window.n_partition_by = 0;
    we->u.window.order_by = NULL;
    we->u.window.n_order_by = 0;
    we->u.window.has_frame = false;

    if (pg_parse_window_def(&overjg, a, q, &we->u.window) != OK)
      goto fail;
    return we;
  }

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_FUNCALL);
  if (!e)
    goto fail;
  e->u.funcall = fc;
  return e;

fail:
  free(fname_to_tr);
  if (schema_to_tr && schema_to_tr != fname_to_tr)
    free(schema_to_tr);
  return NULL;
}

/* Parses a function-like AST node that only carries an "args" array.
 * Ownership: returned expression is arena-owned.
 * Side effects: recursively parses child expressions and may propagate
 * query status changes from nested expressions.
 * Returns a QIR_EXPR_FUNCALL on success, NULL on allocation/parse failure.
 */
static QirExpr *pg_parse_simple_func_like_call(const JsonGetter *jg, Arena *a,
                                               QirQuery *q, const char *fname) {
  if (!jg || !a || !q || !fname)
    return NULL;

  QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_FUNCALL);
  if (!e)
    return NULL;

  e->u.funcall.schema.name = (char *)arena_add_nul(a, (void *)"", 0);
  e->u.funcall.name.name =
      (char *)arena_add_nul(a, (void *)fname, strlen(fname));
  if (!e->u.funcall.schema.name || !e->u.funcall.name.name)
    return NULL;

  PtrVec args = {0};
  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, "args", &it) != YES)
    return NULL;

  JsonGetter argjg = {0};
  int rc = 0;
  while ((rc = jsget_array_objects_next(jg, &it, &argjg)) == YES) {
    QirExpr *arg = pg_parse_expr(&argjg, a, q);
    if (!arg) {
      rc = ERR;
      break;
    }
    if (ptrvec_push(&args, arg) != OK) {
      rc = ERR;
      break;
    }
  }
  if (rc == ERR) {
    ptrvec_clean(&args);
    return NULL;
  }

  if (args.len > 0) {
    e->u.funcall.args = (QirExpr **)ptrvec_flatten(&args, a);
    e->u.funcall.nargs = args.len;
    if (!e->u.funcall.args) {
      ptrvec_clean(&args);
      return NULL;
    }
  }
  ptrvec_clean(&args);
  return e;
}

/* Parses a CoalesceExpr node by normalizing it to a regular function call.
 * Returns a QIR_EXPR_FUNCALL on success, NULL on allocation/parse failure.
 */
static QirExpr *pg_parse_coalesce_expr(const JsonGetter *jg, Arena *a,
                                       QirQuery *q) {
  return pg_parse_simple_func_like_call(jg, a, q, "coalesce");
}

/* Parses a MinMaxExpr node by normalizing it to greatest/least function calls.
 * Returns a QIR_EXPR_FUNCALL on success, an unsupported expression for
 * unknown operators, or NULL on allocation/parse failure.
 */
static QirExpr *pg_parse_minmax_expr(const JsonGetter *jg, Arena *a,
                                     QirQuery *q) {
  if (!jg || !a || !q)
    return NULL;

  JsonStrSpan op = {0};
  if (jsget_string_span(jg, "op", &op) != YES)
    return NULL;

  if (op.len == (sizeof("IS_GREATEST") - 1) &&
      memcmp(op.ptr, "IS_GREATEST", sizeof("IS_GREATEST") - 1) == 0)
    return pg_parse_simple_func_like_call(jg, a, q, "greatest");
  if (op.len == (sizeof("IS_LEAST") - 1) &&
      memcmp(op.ptr, "IS_LEAST", sizeof("IS_LEAST") - 1) == 0)
    return pg_parse_simple_func_like_call(jg, a, q, "least");

  qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported min/max expression");
  return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
}

/* Parses a TypeName into a QirTypeRef.
 * Ownership: type names are arena-owned.
 * Side effects: allocates arena memory.
 * Returns OK on success, ERR on parse/allocation failure. */
static AdbxStatus pg_parse_typename(const JsonGetter *jg, Arena *a,
                                    QirTypeRef *out) {
  if (!jg || !a || !out)
    return ERR;

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, "names", &it) != YES)
    return ERR;

  char *parts[2] = {
      0}; // we only model schema.name; extra parts get folded into name
  uint32_t nparts = 0;
  StrBuf sb;
  sb_init(&sb);
  int use_sb = 0;

  JsonGetter elem = {0};
  int rc = 0;
  while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
    JsonGetter sjg = {0};
    if (jsget_object(&elem, "String", &sjg) != YES) {
      rc = ERR;
      break;
    }
    char *tmp = NULL;
    if (pg_get_string_field(&sjg, "str", "sval", &tmp) != YES) {
      rc = ERR;
      break;
    }

    if (!use_sb && nparts < 2) {
      parts[nparts++] = pg_arena_transfer_lower(a, tmp);
      if (!parts[nparts - 1]) {
        rc = ERR;
        break;
      }
    } else {
      if (!use_sb) {
        if (sb_append_bytes(&sb, parts[0], strlen(parts[0])) != OK) {
          free(tmp);
          rc = ERR;
          break;
        }
        if (nparts > 1) {
          if (sb_append_bytes(&sb, ".", 1) != OK) {
            free(tmp);
            rc = ERR;
            break;
          }
          if (sb_append_bytes(&sb, parts[1], strlen(parts[1])) != OK) {
            free(tmp);
            rc = ERR;
            break;
          }
        }
        use_sb = 1;
      }
      if (sb_append_bytes(&sb, ".", 1) != OK) {
        free(tmp);
        rc = ERR;
        break;
      }
      if (sb_append_bytes(&sb, tmp, strlen(tmp)) != OK) {
        free(tmp);
        rc = ERR;
        break;
      }
      free(tmp);
    }
  }

  if (rc == ERR || nparts == 0) {
    sb_clean(&sb);
    return ERR;
  }

  if (use_sb) {
    char *name = (char *)arena_calloc(a, (uint32_t)(sb.len + 1));
    sb_clean(&sb);
    if (!name)
      return ERR;
    memcpy(name, sb.data, sb.len);
    name[sb.len] = '\0';
    out->schema.name = (char *)arena_add_nul(a, (void *)"", 0);
    out->name.name = name;
    return OK;
  }

  sb_clean(&sb);
  if (nparts == 1) {
    out->schema.name = (char *)arena_add_nul(a, (void *)"", 0);
    out->name.name = parts[0];
  } else {
    out->schema.name = parts[0];
    out->name.name = parts[1];
  }
  return OK;
}

/* Parses an EXPLAIN wrapper and maps the inner supported statement to QueryIR.
 * It borrows the JSON object and mutates the caller-owned query.
 * Side effects: sets statement wrapper flags and parses the wrapped SELECT.
 * We intentionally record only the ANALYZE option because it changes
 * execution semantics; other EXPLAIN options are accepted but not modeled.
 * Returns OK on success, ERR on allocation failure.
 */
static AdbxStatus pg_parse_explain_stmt(const JsonGetter *jg, Arena *a,
                                        QirQuery *q) {
  if (!jg || !a || !q)
    return ERR;

  qir_query_add_stmt_flags(q, (uint32_t)QIR_STMTF_EXPLAIN);

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, "options", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
      JsonGetter dejg = {0};
      if (jsget_object(&elem, "DefElem", &dejg) != YES) {
        qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported EXPLAIN option");
        return OK;
      }

      JsonStrSpan defname;
      if (jsget_string_span(&dejg, "defname", &defname) != YES)
        return ERR;
      if (defname.len == (sizeof("analyze") - 1) &&
          memcmp(defname.ptr, "analyze", sizeof("analyze") - 1) == 0) {
        qir_query_add_stmt_flags(q, (uint32_t)QIR_STMTF_ANALYZE);
      }
    }
    if (rc == ERR)
      return ERR;
  }

  JsonGetter qjg = {0};
  if (jsget_object(jg, "query", &qjg) != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported EXPLAIN target");
    return OK;
  }

  JsonGetter seljg = {0};
  if (jsget_object(&qjg, "SelectStmt", &seljg) != YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported EXPLAIN target");
    return OK;
  }

  return pg_parse_select_stmt(&seljg, a, q);
}

/* Parses an expression node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set query flags.
 * Returns NULL on allocation failure. */
static QirExpr *pg_parse_expr(const JsonGetter *jg, Arena *a, QirQuery *q) {
  if (!jg || !a || !q)
    return NULL;

  JsonGetter sub = {0};
  if (jsget_object(jg, "ColumnRef", &sub) == YES) {
    return pg_parse_colref(&sub, a, q);
  }

  if (jsget_object(jg, "ParamRef", &sub) == YES) {
    uint32_t n = 0;
    if (jsget_u32(&sub, "number", &n) != YES)
      return NULL;

    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_PARAM);
    if (!e)
      return NULL;
    e->u.param_index = (int)n;
    return e;
  }

  if (jsget_object(jg, "A_Const", &sub) == YES) {
    return pg_parse_literal(&sub, a, q);
  }

  if (jsget_object(jg, "A_Expr", &sub) == YES) {
    return pg_parse_aexpr(&sub, a, q);
  }

  if (jsget_object(jg, "BoolExpr", &sub) == YES) {
    return pg_parse_bool_expr(&sub, a, q);
  }

  if (jsget_object(jg, "NullTest", &sub) == YES) {
    JsonGetter argjg = {0};
    if (jsget_object(&sub, "arg", &argjg) != YES)
      return NULL;
    QirExpr *arg = pg_parse_expr(&argjg, a, q);
    if (!arg)
      return NULL;

    char *ntype = NULL;
    if (jsget_string_decode_alloc(&sub, "nulltesttype", &ntype) != YES) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported NULL test");
      return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }

    if (strcmp(ntype, "IS_NULL") != 0 && strcmp(ntype, "IS_NOT_NULL") != 0) {
      free(ntype);
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported NULL test");
      return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }

    // NullTest is a unary predicate. Model it as a generic backend operator
    // instead of pretending it is a binary comparison against a NULL literal.
    char *op_name = (char *)arena_add_nul(a, (void *)ntype, strlen(ntype));
    free(ntype);
    if (!op_name)
      return NULL;

    QirExpr *res = pg_qir_new_expr(a, QIR_EXPR_OP);
    if (!res)
      return NULL;
    res->u.op.cls = QIR_OP_OTHER;
    res->u.op.lhs = arg;
    res->u.op.args = NULL;
    res->u.op.nargs = 0;
    res->u.op.op_name = op_name;
    return res;
  }

  if (jsget_object(jg, "FuncCall", &sub) == YES) {
    return pg_parse_func_call(&sub, a, q);
  }

  if (jsget_object(jg, "CoalesceExpr", &sub) == YES) {
    return pg_parse_coalesce_expr(&sub, a, q);
  }

  if (jsget_object(jg, "MinMaxExpr", &sub) == YES) {
    return pg_parse_minmax_expr(&sub, a, q);
  }

  if (jsget_object(jg, "CaseExpr", &sub) == YES) {
    return pg_parse_caseexpr(&sub, a, q);
  }

  if (jsget_object(jg, "SubLink", &sub) == YES) {
    return pg_parse_sublink(&sub, a, q);
  }

  if (jsget_object(jg, "TypeCast", &sub) == YES) {
    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_CAST);
    if (!e)
      return NULL;

    JsonGetter argjg = {0};
    if (jsget_object(&sub, "arg", &argjg) != YES)
      return NULL;
    QirExpr *arg = pg_parse_expr(&argjg, a, q);
    if (!arg)
      return NULL;
    e->u.cast.expr = arg;

    JsonGetter tnjg = {0};
    if (jsget_object(&sub, "typeName", &tnjg) != YES) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported cast type");
      return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }
    JsonGetter tnjg2 = {0};
    if (jsget_object(&tnjg, "TypeName", &tnjg2) == YES) {
      tnjg = tnjg2;
    }
    if (pg_parse_typename(&tnjg, a, &e->u.cast.type) != OK) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported cast type");
      return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }
    return e;
  }

  qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported expression");
  return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
}

/* Parses a RangeVar node into a QirFromItem.
 * Ownership: returned node is arena-owned.
 * Side effects: none.
 * Returns NULL on allocation failure. */
static QirFromItem *pg_parse_rangevar(const JsonGetter *jg, Arena *a) {
  QirFromItem *fi = arena_calloc(a, (uint32_t)sizeof(QirFromItem));
  if (!fi)
    return NULL;
  fi->kind = QIR_FROM_BASE_REL;

  char *tmp = NULL;
  if (jsget_string_decode_alloc(jg, "relname", &tmp) == YES) {
    fi->u.rel.name.name = pg_arena_transfer_lower(a, tmp);
  }

  tmp = NULL;
  if (jsget_string_decode_alloc(jg, "schemaname", &tmp) == YES) {
    fi->u.rel.schema.name = pg_arena_transfer_lower(a, tmp);
  } else {
    fi->u.rel.schema.name = (char *)arena_add_nul(a, (void *)"", 0);
  }

  // alias
  JsonGetter ajg = {0};
  if (jsget_object(jg, "alias", &ajg) == YES) {
    fi->alias.name = pg_parse_alias_name(&ajg, a);
  }

  if (!fi->alias.name)
    fi->alias.name = (char *)arena_add_nul(a, (void *)"", 0);
  return fi;
}

/* Parses alias column list (AS v(x,y,...) ) into arena-owned QirIdent array.
 * Ownership: returned array is arena-owned.
 * Side effects: allocates arena memory.
 * Returns OK/ERR. */
static AdbxStatus pg_parse_alias_colnames(const JsonGetter *alias_obj, Arena *a,
                                          QirIdent **out_cols,
                                          uint32_t *out_ncols) {
  if (!alias_obj || !a || !out_cols || !out_ncols)
    return ERR;
  *out_cols = NULL;
  *out_ncols = 0;

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(alias_obj, "colnames", &it) != YES)
    return OK;

  PtrVec cols = {0};
  JsonGetter elem = {0};
  int rc = 0;
  while ((rc = jsget_array_objects_next(alias_obj, &it, &elem)) == YES) {
    JsonGetter sjg = {0};
    if (jsget_object(&elem, "String", &sjg) != YES) {
      rc = ERR;
      break;
    }
    char *tmp = NULL;
    if (pg_get_string_field(&sjg, "str", "sval", &tmp) != YES) {
      rc = ERR;
      break;
    }
    char *name = pg_arena_transfer_lower(a, tmp);
    if (!name) {
      rc = ERR;
      break;
    }
    if (ptrvec_push(&cols, name) != OK) {
      rc = ERR;
      break;
    }
  }
  if (rc == ERR) {
    ptrvec_clean(&cols);
    return ERR;
  }
  if (cols.len > 0) {
    QirIdent *arr =
        (QirIdent *)arena_calloc(a, (uint32_t)(cols.len * sizeof(QirIdent)));
    if (!arr) {
      ptrvec_clean(&cols);
      return ERR;
    }
    for (uint32_t i = 0; i < cols.len; i++) {
      arr[i].name = (const char *)cols.items[i];
    }
    *out_cols = arr;
    *out_ncols = cols.len;
  }
  ptrvec_clean(&cols);
  return OK;
}

/* Resolves a RangeVar against the query's CTE list.
 * If the relname matches a CTE and no schema is specified, mark it as CTE_REF.
 * Ownership: uses pointers already owned by the query arena.
 * Side effects: mutates fi->kind and fi->u.cte_name on match. */
static void pg_resolve_cte_ref(const QirQuery *q, QirFromItem *fi) {
  if (!q || !fi)
    return;
  if (fi->kind != QIR_FROM_BASE_REL)
    return;
  if (!fi->u.rel.schema.name || fi->u.rel.schema.name[0] != '\0')
    return;
  if (!fi->u.rel.name.name || fi->u.rel.name.name[0] == '\0')
    return;

  for (uint32_t i = 0; i < q->nctes; i++) {
    const QirCte *cte = q->ctes ? q->ctes[i] : NULL;
    if (!cte || !cte->name.name)
      continue;
    if (strcmp(cte->name.name, fi->u.rel.name.name) == 0) {
      fi->kind = QIR_FROM_CTE_REF;
      fi->u.cte_name = cte->name;
      return;
    }
  }
}

/* Resolves CTE references for all FROM and JOIN items in a query,
 * including union_next branches. Uses the father's (q's) CTE list.
 * Ownership: uses pointers already owned by the query arena.
 * Side effects: may mark FROM/JOIN items as CTE_REF. */
static void pg_resolve_cte_refs_in_query(const QirQuery *q) {
  if (!q)
    return;
  for (const QirQuery *cur = q; cur; cur = cur->union_next) {
    if (cur->from_root)
      pg_resolve_cte_ref(q, cur->from_root);
    for (uint32_t i = 0; i < cur->njoins; i++) {
      QirJoin *j = cur->joins ? cur->joins[i] : NULL;
      if (j && j->rhs)
        pg_resolve_cte_ref(q, j->rhs);
    }
  }
}

/* Parses a range item or join and populates froms/joins (left-deep).
 * Ownership: from/joins vectors own their temporary buffers.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns OK/ERR. */
static AdbxStatus pg_parse_from_item(const JsonGetter *jg, Arena *a,
                                     QirQuery *q, PtrVec *froms, PtrVec *joins);

/* Parses a join expression into from-items and joins (left-deep).
 * Ownership: join nodes are arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns OK/ERR. */
static AdbxStatus pg_parse_join_expr(const JsonGetter *jg, Arena *a,
                                     QirQuery *q, PtrVec *froms,
                                     PtrVec *joins) {
  // left
  JsonGetter ljg = {0};
  if (jsget_object(jg, "larg", &ljg) != YES)
    return ERR;
  if (pg_parse_from_item(&ljg, a, q, froms, joins) != OK)
    return ERR;

  // join type
  int jointype = 0;
  int64_t v = 0;
  if (jsget_i64(jg, "jointype", &v) == YES) {
    jointype = (int)v;
  } else {
    // libpg_query may encode enums as strings; map common join names.
    char *tmp = NULL;
    if (jsget_string_decode_alloc(jg, "jointype", &tmp) == YES && tmp) {
      if (strcmp(tmp, "JOIN_INNER") == 0)
        jointype = 0;
      else if (strcmp(tmp, "JOIN_LEFT") == 0)
        jointype = 1;
      else if (strcmp(tmp, "JOIN_FULL") == 0)
        jointype = 2;
      else if (strcmp(tmp, "JOIN_RIGHT") == 0)
        jointype = 3;
      else {
        jointype = -1;
        qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported join type");
      }
      free(tmp);
    }
  }
  if (jsget_exists_nonnull(jg, "usingClause") == YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "JOIN USING not supported");
  }
  if (jsget_exists_nonnull(jg, "isNatural") == YES) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "NATURAL JOIN not supported");
  }

  QirJoin *j = arena_calloc(a, (uint32_t)sizeof(QirJoin));
  if (!j)
    return ERR;
  switch (jointype) {
  case 0:
    j->kind = QIR_JOIN_INNER;
    break;
  case 1:
    j->kind = QIR_JOIN_LEFT;
    break;
  case 2:
    j->kind = QIR_JOIN_FULL;
    break;
  case 3:
    j->kind = QIR_JOIN_RIGHT;
    break;
  default:
    j->kind = QIR_JOIN_UNSUPPORTED;
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported join type");
    break;
  }

  // right
  JsonGetter rjg = {0};
  if (jsget_object(jg, "rarg", &rjg) != YES)
    return ERR;

  if (jsget_object(&rjg, "RangeVar", &rjg) == YES) {
    j->rhs = pg_parse_rangevar(&rjg, a);
    if (j->rhs)
      pg_resolve_cte_ref(q, j->rhs);
  } else if (jsget_object(&rjg, "RangeSubselect", &rjg) == YES) {
    JsonGetter ssjg = rjg;
    int lat = 0;
    if (jsget_bool01(&ssjg, "lateral", &lat) == YES && lat) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "LATERAL subquery not supported");
    }
    JsonGetter subjg = {0};
    if (jsget_object(&ssjg, "subquery", &subjg) == YES) {
      JsonGetter seljg = {0};
      if (jsget_object(&subjg, "SelectStmt", &seljg) == YES) {
        QirFromItem *fi = arena_calloc(a, (uint32_t)sizeof(QirFromItem));
        if (fi) {
          fi->kind = QIR_FROM_SUBQUERY;
          fi->alias.name = (char *)arena_add_nul(a, (void *)"", 0);
          fi->u.values.colnames = NULL;
          fi->u.values.ncolnames = 0;
          fi->u.subquery = pg_qir_new_query(a);
          if (jsget_exists_nonnull(&seljg, "valuesLists") == YES) {
            fi->kind = QIR_FROM_VALUES;
          } else if (fi->u.subquery) {
            pg_parse_select_stmt(&seljg, a, fi->u.subquery);
          }
          JsonGetter ajg = {0};
          if (jsget_object(&ssjg, "alias", &ajg) == YES) {
            fi->alias.name = pg_parse_alias_name(&ajg, a);
            if (fi->kind == QIR_FROM_VALUES) {
              if (pg_parse_alias_colnames(&ajg, a, &fi->u.values.colnames,
                                          &fi->u.values.ncolnames) != OK) {
                qir_set_status(q, a, QIR_UNSUPPORTED, "invalid VALUES alias");
              }
            }
          }
          if (fi->kind == QIR_FROM_VALUES &&
              (!fi->alias.name || fi->alias.name[0] == '\0')) {
            qir_set_status(q, a, QIR_UNSUPPORTED, "VALUES requires an alias");
          }
          j->rhs = fi;
        }
      }
    }
  } else {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported join rhs");
    j->rhs = arena_calloc(a, (uint32_t)sizeof(QirFromItem));
    if (j->rhs)
      j->rhs->kind = QIR_FROM_UNSUPPORTED;
  }

  // ON clause (no quals with INNER join is treated as CROSS JOIN).
  JsonGetter qjg = {0};
  if (jsget_object(jg, "quals", &qjg) == YES) {
    j->on = pg_parse_expr(&qjg, a, q);
  } else if (j->kind == QIR_JOIN_INNER) {
    j->kind = QIR_JOIN_CROSS;
  }

  if (ptrvec_push(joins, j) != OK)
    return ERR;
  return OK;
}

/* Parses a range item or join and populates froms/joins (left-deep).
 * Ownership: from/join nodes are arena-owned.
 * Side effects: may mark QIR_UNSUPPORTED.
 * Returns OK/ERR. */
static AdbxStatus pg_parse_from_item(const JsonGetter *jg, Arena *a,
                                     QirQuery *q, PtrVec *froms,
                                     PtrVec *joins) {
  if (!jg || !a || !q)
    return ERR;

  JsonGetter rvjg = {0};
  if (jsget_object(jg, "RangeVar", &rvjg) == YES) {
    QirFromItem *fi = pg_parse_rangevar(&rvjg, a);
    if (!fi)
      return ERR;
    pg_resolve_cte_ref(q, fi);
    return ptrvec_push(froms, fi);
  }

  JsonGetter jjg = {0};
  if (jsget_object(jg, "JoinExpr", &jjg) == YES) {
    return pg_parse_join_expr(&jjg, a, q, froms, joins);
  }

  JsonGetter ssjg = {0};
  if (jsget_object(jg, "RangeSubselect", &ssjg) == YES) {
    int lat = 0;
    if (jsget_bool01(&ssjg, "lateral", &lat) == YES && lat) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "LATERAL subquery not supported");
    }
    QirFromItem *fi = arena_calloc(a, (uint32_t)sizeof(QirFromItem));
    if (!fi)
      return ERR;
    fi->kind = QIR_FROM_SUBQUERY;
    fi->alias.name = (char *)arena_add_nul(a, (void *)"", 0);
    fi->u.values.colnames = NULL;
    fi->u.values.ncolnames = 0;

    JsonGetter subjg = {0};
    if (jsget_object(&ssjg, "subquery", &subjg) == YES) {
      JsonGetter seljg = {0};
      if (jsget_object(&subjg, "SelectStmt", &seljg) == YES) {
        if (jsget_exists_nonnull(&seljg, "valuesLists") == YES) {
          fi->kind = QIR_FROM_VALUES;
        } else {
          fi->u.subquery = pg_qir_new_query(a);
          if (fi->u.subquery)
            pg_parse_select_stmt(&seljg, a, fi->u.subquery);
        }
      } else if (jsget_object(&subjg, "ValuesStmt", &seljg) == YES) {
        qir_set_status(q, a, QIR_UNSUPPORTED, "VALUES in FROM not supported");
      }
    }

    JsonGetter ajg = {0};
    if (jsget_object(&ssjg, "alias", &ajg) == YES) {
      fi->alias.name = pg_parse_alias_name(&ajg, a);
      if (fi->kind == QIR_FROM_VALUES) {
        if (pg_parse_alias_colnames(&ajg, a, &fi->u.values.colnames,
                                    &fi->u.values.ncolnames) != OK) {
          qir_set_status(q, a, QIR_UNSUPPORTED, "invalid VALUES alias");
        }
      }
    }

    if (fi->kind == QIR_FROM_VALUES &&
        (!fi->alias.name || fi->alias.name[0] == '\0')) {
      qir_set_status(q, a, QIR_UNSUPPORTED, "VALUES requires an alias");
    }

    return ptrvec_push(froms, fi);
  }

  qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported FROM item");
  return OK;
}

/* Parses the body of a SELECT branch: targetList, FROM, JOIN, GROUP BY,
 * HAVING, WHERE, and flags (DISTINCT). Does NOT parse CTEs, ORDER BY, or
 * LIMIT — those belong to the "father" query in a set-operation chain.
 * For a non-set-op SELECT, the caller invokes both this and the outer-level
 * parsing on the same JSON object. */
static AdbxStatus pg_parse_select_body(const JsonGetter *jg, Arena *a,
                                       QirQuery *q) {
  if (!jg || !a || !q)
    return ERR;

  // Flags
  if (jsget_exists_nonnull(jg, "distinctClause") == YES)
    q->has_distinct = true;

  // targetList
  PtrVec sels = {0};
  JsonArrIter it = {0};
  if (jsget_array_objects_begin(jg, "targetList", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
      JsonGetter rjg = {0};
      if (jsget_object(&elem, "ResTarget", &rjg) != YES) {
        rc = ERR;
        break;
      }

      QirSelectItem *si =
          (QirSelectItem *)arena_calloc(a, (uint32_t)sizeof(QirSelectItem));
      if (!si) {
        rc = ERR;
        break;
      }

      char *tmp = NULL;
      if (jsget_string_decode_alloc(&rjg, "name", &tmp) == YES) {
        si->out_alias.name = pg_arena_transfer_lower(a, tmp);
      } else {
        si->out_alias.name = (char *)arena_add_nul(a, (void *)"", 0);
      }

      JsonGetter vjg = {0};
      if (jsget_object(&rjg, "val", &vjg) != YES) {
        rc = ERR;
        break;
      }
      QirExpr *expr = pg_parse_expr(&vjg, a, q);
      if (!expr) {
        rc = ERR;
        break;
      }
      si->value = expr;

      if (ptrvec_push(&sels, si) != OK) {
        rc = ERR;
        break;
      }
    }
    if (rc == ERR)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported SELECT list");
  }

  q->select_items = (QirSelectItem **)ptrvec_flatten(&sels, a);
  q->nselect = sels.len;
  ptrvec_clean(&sels);

  // fromClause
  PtrVec froms = {0};
  PtrVec joins = {0};
  if (jsget_array_objects_begin(jg, "fromClause", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
      if (pg_parse_from_item(&elem, a, q, &froms, &joins) != OK) {
        rc = ERR;
        break;
      }
    }
    if (rc == ERR)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported FROM clause");
  }

  if (froms.len > 1) {
    qir_set_status(q, a, QIR_UNSUPPORTED,
                   "multiple FROM items are not supported");
  } else if (froms.len == 1) {
    q->from_root = (QirFromItem *)froms.items[0];
  }
  ptrvec_clean(&froms);

  q->joins = (QirJoin **)ptrvec_flatten(&joins, a);
  q->njoins = joins.len;
  ptrvec_clean(&joins);

  // GROUP BY
  PtrVec groups = {0};
  if (jsget_array_objects_begin(jg, "groupClause", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
      QirExpr *expr = pg_parse_expr(&elem, a, q);
      if (!expr) {
        rc = ERR;
        break;
      }
      if (ptrvec_push(&groups, expr) != OK) {
        rc = ERR;
        break;
      }
    }
    if (rc == ERR)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported GROUP BY");
  }
  q->group_by = (QirExpr **)ptrvec_flatten(&groups, a);
  q->n_group_by = groups.len;
  ptrvec_clean(&groups);

  // HAVING
  JsonGetter hvg = {0};
  int hrc = jsget_object(jg, "havingClause", &hvg);
  if (hrc == ERR) {
    qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported HAVING");
  } else if (hrc == YES) {
    q->having = pg_parse_expr(&hvg, a, q);
    if (!q->having)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported HAVING");
  }

  // WHERE
  JsonGetter wjg = {0};
  if (jsget_object(jg, "whereClause", &wjg) == YES) {
    q->where = pg_parse_expr(&wjg, a, q);
  }

  return OK;
}

/* Recursively flattens a set-operation binary tree (UNION/INTERSECT/EXCEPT)
 * into a list of leaf branches in SQL order (left-to-right). Each leaf is
 * parsed via pg_parse_select_body into a new QirQuery. */
static AdbxStatus pg_parse_setop_branches(const JsonGetter *jg, Arena *a,
                                          PtrVec *branches) {
  JsonGetter ljg = {0}, rjg = {0};
  if (jsget_object(jg, "larg", &ljg) == YES &&
      jsget_object(jg, "rarg", &rjg) == YES) {
    // Internal set-op node: recurse left then right for SQL order.
    if (pg_parse_setop_branches(&ljg, a, branches) != OK)
      return ERR;
    return pg_parse_setop_branches(&rjg, a, branches);
  }

  // Leaf branch: parse body into a new QirQuery.
  QirQuery *branch = pg_qir_new_query(a);
  if (!branch)
    return ERR;
  pg_parse_select_body(jg, a, branch);
  if (ptrvec_push(branches, branch) != OK)
    return ERR;
  return OK;
}

static AdbxStatus pg_parse_select_stmt(const JsonGetter *jg, Arena *a,
                                       QirQuery *q) {
  if (!jg || !a || !q)
    return ERR;

  // --- 1. CTEs (always on the outer/father node) ---
  JsonArrIter it = {0};
  JsonGetter wcjg = {0};
  if (jsget_object(jg, "withClause", &wcjg) == YES) {
    PtrVec ctes = {0};
    if (jsget_array_objects_begin(&wcjg, "ctes", &it) == YES) {
      JsonGetter elem = {0};
      int rc = 0;
      while ((rc = jsget_array_objects_next(&wcjg, &it, &elem)) == YES) {
        JsonGetter ctejg = {0};
        if (jsget_object(&elem, "CommonTableExpr", &ctejg) != YES) {
          rc = ERR;
          break;
        }

        QirCte *cte = arena_calloc(a, (uint32_t)sizeof(QirCte));
        if (!cte) {
          rc = ERR;
          break;
        }

        char *tmp = NULL;
        if (jsget_string_decode_alloc(&ctejg, "ctename", &tmp) == YES) {
          cte->name.name = pg_arena_transfer_lower(a, tmp);
        }

        JsonGetter cqjg = {0};
        if (jsget_object(&ctejg, "ctequery", &cqjg) == YES) {
          JsonGetter seljg = {0};
          if (jsget_object(&cqjg, "SelectStmt", &seljg) == YES) {
            cte->query = pg_qir_new_query(a);
            if (cte->query)
              pg_parse_select_stmt(&seljg, a, cte->query);
          }
        }

        if (ptrvec_push(&ctes, cte) != OK) {
          rc = ERR;
          break;
        }
      }
      if (rc == ERR)
        qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported CTE");
    }
    q->ctes = (QirCte **)ptrvec_flatten(&ctes, a);
    q->nctes = ctes.len;
    ptrvec_clean(&ctes);
  }

  // --- 2. Set-op detection and body parsing ---
  JsonGetter setop_ljg = {0};
  if (jsget_object(jg, "larg", &setop_ljg) == YES) {
    // Set-op (UNION/INTERSECT/EXCEPT): flatten binary tree into linked list.
    PtrVec branches = {0};
    if (pg_parse_setop_branches(jg, a, &branches) != OK) {
      ptrvec_clean(&branches);
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported set operation");
    } else if (branches.len > 0) {
      // First branch body → father query.
      QirQuery *first = (QirQuery *)branches.items[0];
      q->has_star = first->has_star;
      q->has_distinct = first->has_distinct;
      q->select_items = first->select_items;
      q->nselect = first->nselect;
      q->from_root = first->from_root;
      q->joins = first->joins;
      q->njoins = first->njoins;
      q->where = first->where;
      q->group_by = first->group_by;
      q->n_group_by = first->n_group_by;
      q->having = first->having;
      if (first->status != QIR_OK)
        qir_set_status(q, a, first->status, first->status_reason);

      // Link remaining branches via union_next.
      QirQuery *prev = q;
      for (uint32_t i = 1; i < branches.len; i++) {
        prev->union_next = (QirQuery *)branches.items[i];
        prev = prev->union_next;
      }
    }
    ptrvec_clean(&branches);
  } else {
    // Simple SELECT: parse body directly into q.
    pg_parse_select_body(jg, a, q);
  }

  // --- 3. Outer-level flags ---
  if (jsget_exists_nonnull(jg, "limitOffset") == YES)
    q->has_offset = true;

  // --- 4. ORDER BY (always on the outer/father) ---
  PtrVec orders = {0};
  if (jsget_array_objects_begin(jg, "sortClause", &it) == YES) {
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
      JsonGetter sjg = {0};
      if (jsget_object(&elem, "SortBy", &sjg) != YES) {
        rc = ERR;
        break;
      }

      JsonGetter njg = {0};
      if (jsget_object(&sjg, "node", &njg) != YES) {
        rc = ERR;
        break;
      }
      QirExpr *expr = pg_parse_expr(&njg, a, q);
      if (!expr) {
        rc = ERR;
        break;
      }
      expr = qir_resolve_order_alias(q, a, expr);

      if (ptrvec_push(&orders, expr) != OK) {
        rc = ERR;
        break;
      }
    }
    if (rc == ERR)
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported ORDER BY");
  }
  q->order_by = (QirExpr **)ptrvec_flatten(&orders, a);
  q->n_order_by = orders.len;
  ptrvec_clean(&orders);

  // --- 5. Resolve CTE references (walks union chain) ---
  if (q->nctes > 0) {
    pg_resolve_cte_refs_in_query(q);
  }

  // --- 6. LIMIT ---
  JsonGetter lcjg = {0};
  if (jsget_object(jg, "limitCount", &lcjg) == YES) {
    JsonGetter acjg = {0};
    if (jsget_object(&lcjg, "A_Const", &acjg) == YES) {
      QirExpr *lit = pg_parse_literal(&acjg, a, q);
      if (lit && lit->kind == QIR_EXPR_LITERAL &&
          lit->u.lit.kind == QIR_LIT_INT64) {
        if (lit->u.lit.v.i64 >= 0 && lit->u.lit.v.i64 <= INT32_MAX) {
          q->limit_value = (int32_t)lit->u.lit.v.i64;
        } else {
          qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported LIMIT");
        }
      } else {
        qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported LIMIT");
      }
    } else {
      qir_set_status(q, a, QIR_UNSUPPORTED, "unsupported LIMIT");
    }
  }

  return OK;
}

/* Creates a QueryIR for a SQL string using libpg_query JSON AST.
 * Ownership: out handle owns all allocations; caller must destroy it.
 * Side effects: allocates arena memory and may write one typed diagnostic into
 * 'out_err' when returning ERR.
 * Returns OK on success (including parse/unsupported), ERR on allocation
 * failure or invalid input. */
static AdbxStatus pg_make_query_ir(DbBackend *db, const char *sql,
                                   QirQueryHandle *out, DbErr *out_err) {
  (void)db;
  if (!sql || !out) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "postgres query-ir creation failed: invalid input.");
    return ERR;
  }

  if (qir_handle_init(out) != OK) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "postgres query-ir creation failed: qir_handle_init failed.");
    return ERR;
  }
  QirQuery *q = out->q;

  PgQueryParseResult res = pg_query_parse(sql);
  if (res.error) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, res.error->message);
    pg_query_free_parse_result(res);
    return OK;
  }

  if (!res.parse_tree) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, "parse error");
    pg_query_free_parse_result(res);
    return OK;
  }

  JsonGetter root = {0};
  if (jsget_create(&root, res.parse_tree, strlen(res.parse_tree)) != OK) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, "parse error");
    goto free_pg_parse_result;
  }

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(&root, "stmts", &it) != YES) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, "parse error");
    goto free_pg_parse_result;
  }

  JsonGetter stmt = {0};
  if (jsget_array_objects_next(&root, &it, &stmt) != YES) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, "parse error");
    goto free_pg_parse_result;
  }

  // multiple statements are a parse error
  if (jsget_array_objects_next(&root, &it, &stmt) == YES) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, "multiple statements");
    goto free_pg_parse_result;
  }

  JsonGetter raw = {0};
  if (jsget_object(&stmt, "RawStmt", &raw) == YES) {
    stmt = raw;
  }

  JsonGetter stg = {0};
  if (jsget_object(&stmt, "stmt", &stg) != YES) {
    qir_set_status(q, &out->arena, QIR_PARSE_ERROR, "parse error");
    goto free_pg_parse_result;
  }

  JsonGetter seljg = {0};
  if (jsget_object(&stg, "SelectStmt", &seljg) == YES) {
    if (pg_parse_select_stmt(&seljg, &out->arena, q) != OK) {
      ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                    "postgres query-ir creation failed while building the "
                    "QueryIR tree.");
      qir_handle_destroy(out);
      jsget_destroy(&root);
      pg_query_free_parse_result(res);
      return ERR;
    }
  } else {
    JsonGetter exjg = {0};
    if (jsget_object(&stg, "ExplainStmt", &exjg) == YES) {
      if (pg_parse_explain_stmt(&exjg, &out->arena, q) != OK) {
        ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                      "postgres query-ir creation failed while building the "
                      "QueryIR tree.");
        qir_handle_destroy(out);
        jsget_destroy(&root);
        pg_query_free_parse_result(res);
        return ERR;
      }
    } else {
      qir_set_status(q, &out->arena, QIR_UNSUPPORTED,
                     "unsupported statement type");
    }
  }

free_pg_parse_result:
  jsget_destroy(&root);
  pg_query_free_parse_result(res);
  return OK;
}

/* Stores one libpq-derived diagnostic into 'out_err'.
 * It borrows 'conn'/'prefix' and does not allocate. Side effects: reads libpq
 * connection error state and overwrites the caller-owned error snapshot.
 * Returns void; NULL 'out_err' is accepted and becomes a no-op.
 */
static void pg_set_pg_err(DbErr *out_err, PGconn *conn, const char *prefix) {
  const char *e = conn ? PQerrorMessage(conn) : "no connection";
  if (!prefix || prefix[0] == '\0')
    prefix = "postgres error";
  ADBX_ERR_SETF(out_err, DBERR_GENERIC, "%s: %s", prefix, e ? e : "");
}

/* Executes one or more SQL commands (separated by ';') and requires COMMAND_OK.
 * Use this to send sql statements that don't return tuples.
 * Ownership: borrows all inputs and writes diagnostics into caller-owned
 * 'out_err'. Side effects: sends one query through libpq.
 * Returns OK on command success, ERR on bad input or if the query failed.
 */
static AdbxStatus pg_exec_command(PgImpl *p, const char *sql, DbErr *out_err) {
  ADBX_ERR_CLEAR(out_err, DBERR_NONE);
  if (!p || !sql) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "postgres command execution failed: invalid input.");
    return ERR;
  }

  if (!p->conn) {
    pg_set_pg_err(out_err, p->conn, NULL);
    return ERR;
  }

  PGresult *res = PQexec(p->conn, sql);
  if (!res) {
    pg_set_pg_err(out_err, p->conn, "PQexec failed");
    return ERR;
  }

  ExecStatusType st = PQresultStatus(res);
  if (st != PGRES_COMMAND_OK) {
    // Could be error, or could be tuples.
    // caller should use pg_exec() for tuples
    pg_set_pg_err(out_err, p->conn, sql);
    PQclear(res);
    return ERR;
  }

  PQclear(res);
  return OK;
}

/* Executes one or more SQL commands (separated by ';') ignoring their errors
 * if any is returned. */
static void pg_exec_command_ignore(PgImpl *p, const char *sql) {
  if (!p || !p->conn)
    return;
  PGresult *res = PQexec(p->conn, sql);
  if (res)
    PQclear(res);
}

/* Best-effort rollback, ignore errors. */
static void pg_rollback(PgImpl *p) { pg_exec_command_ignore(p, "ROLLBACK"); }

/* Executes commands so the current session of 'p' complies with 'p->policy'.
 * Must be called before running any query and the caller must checks this
 * returned one before sending any query.
 * Ownership: borrows 'p'; writes diagnostics into caller-owned 'out_err'.
 * Side effects: mutates backend session state and flips 'p->policy_applied' on
 * success.
 * Returns OK when the policy was applied, ERR on bad input or SQL/libpq
 * failure.
 */
static AdbxStatus pg_apply_policy(PgImpl *p, DbErr *out_err) {
  ADBX_ERR_CLEAR(out_err, DBERR_NONE);
  if (!p || !p->conn) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "postgres policy application failed: invalid backend state.");
    return ERR;
  }
  // bad things can happen if we let the max bytes to be low like 1/2...
  // this is a safe bound
  // ignore failure, this is not strictly necessary
  pg_exec_command_ignore(p, "SET application_name to \'db-explorer\'");

  // safetyguards are optional, treat 0 as not set

  char buf[256];
  SafetyPolicy policy = p->policy;

  snprintf(buf, sizeof(buf), "SET default_transaction_read_only = %s",
           policy.read_only > 0 ? "on" : "off");

  // Ignore failure: older versions / permissions might differ.
  // read-only will be enforced per query.
  pg_exec_command_ignore(p, buf);

  if (policy.statement_timeout_ms > 0) {
    snprintf(buf, sizeof(buf), "SET statement_timeout = %u",
             policy.statement_timeout_ms);
    if (pg_exec_command(p, buf, out_err) != OK)
      return ERR;
  }

  p->policy_applied = 1;
  return OK;
}

/* Executes 'sql' and returns the result inside 'out_res'. It verify the result
 * is just one.
 * Ownership: borrows all inputs; caller owns '*out_res' on success and owns
 * 'out_err'. Side effects: sends one query via libpq.
 * Returns OK when exactly one result is produced, ERR on bad input/libpq
 * failure/multiple results.
 */
static AdbxStatus pg_exec_single_result(PgImpl *p, const char *sql,
                                        PGresult **out_res, DbErr *out_err) {
  ADBX_ERR_CLEAR(out_err, DBERR_NONE);
  if (!p || !p->conn || !sql || !out_res) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "postgres single-result execution failed: invalid input.");
    return ERR;
  }
  assert(p);
  assert(p->conn);
  assert(sql);
  assert(out_res);

  *out_res = NULL;

  if (PQsendQuery(p->conn, sql) != 1) {
    pg_set_pg_err(out_err, p->conn, "PQsendQuery failed");
    return ERR;
  }

  PGresult *first = NULL;
  PGresult *extra = NULL;

  for (;;) {
    PGresult *res = PQgetResult(p->conn);
    if (!res)
      break;

    if (!first) {
      first = res;
    } else {
      // second result => multi-statement or multiple commands
      extra = res;
      // consume remaining results to keep connection usable
      while ((res = PQgetResult(p->conn)) != NULL) {
        PQclear(res);
      }
      break;
    }
  }

  if (extra) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "multiple statements/results are not allowed");
    PQclear(extra);
    if (first)
      PQclear(first);
    return ERR;
  }

  if (!first) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "no result returned");
    return ERR;
  }

  *out_res = first;
  return OK;
}

/* Executes one parameterized SQL statement and returns exactly one PGresult.
 * Ownership: borrows all inputs; caller owns '*out_res' on success.
 * Side effects: sends one query over libpq and writes diagnostics into
 * caller-owned 'out_err'.
 * Error semantics: returns OK when exactly one result is produced, ERR on bad
 * input/libpq failure/multiple results.
 */
static AdbxStatus pg_exec_single_result_bound(PgImpl *p, const char *sql,
                                              const DbExecParam *params,
                                              uint32_t nparams,
                                              PGresult **out_res,
                                              DbErr *out_err) {
  assert(p);
  assert(p->conn);
  assert(sql);
  assert(out_res);
  assert(!(nparams > 0 && !params));

  ADBX_ERR_CLEAR(out_err, DBERR_NONE);
  if (!p || !p->conn || !sql || !out_res) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "postgres bound execution failed: invalid input.");
    return ERR;
  }
  if ((nparams > 0 && !params) || nparams > MAX_TOKEN_PARAMS) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "invalid bound execution input");
    return ERR;
  }

  *out_res = NULL;

  const char *param_values[MAX_TOKEN_PARAMS];
  Oid param_types[MAX_TOKEN_PARAMS];
  for (uint32_t i = 0; i < nparams; i++) {
    const DbExecParam *in = &params[i];
    if (!in->value) {
      ADBX_ERR_SETF(out_err, DBERR_GENERIC, "invalid bound parameter payload");
      return ERR;
    }
    // libpq text binds require NUL-terminated strings.
    // This entity is NOT responsible for validating params values.
    assert(in->value[in->value_len] == '\0');
    param_values[i] = in->value;
    param_types[i] = (Oid)in->pg_oid;
  }

  // param_values and _types may be unitialized if nparams is 0, but it's safe
  // since PQsendQueryParams do not dereference them if nparams is 0.
  if (PQsendQueryParams(p->conn, sql, (int)nparams, param_types, param_values,
                        NULL, NULL, 0) != 1) {
    pg_set_pg_err(out_err, p->conn, "PQsendQueryParams failed");
    return ERR;
  }

  PGresult *first = NULL;
  PGresult *extra = NULL;

  for (;;) {
    PGresult *res = PQgetResult(p->conn);
    if (!res)
      break;

    if (!first) {
      first = res;
    } else {
      // second result => multi-statement or multiple commands
      extra = res;
      // consume remaining results to keep connection usable
      while ((res = PQgetResult(p->conn)) != NULL) {
        PQclear(res);
      }
      break;
    }
  }

  if (extra) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "multiple statements/results are not allowed");
    PQclear(extra);
    if (first)
      PQclear(first);
    return ERR;
  }

  if (!first) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "no result returned");
    return ERR;
  }

  *out_res = first;
  return OK;
}

/* Verifies that the connected role is safe for read-only usage. This is a
 * one-time guardrail executed at connect time. Returns:
 * - OK: role passed the check.
 * - ERR: query failed, malformed output, or role deemed unsafe.
 *
 * Side effects:
 * - Executes one SQL statement on the active connection.
 * - Stores human-readable error into 'out_err' on failure.
 */
static AdbxStatus pg_check_safe_read_only_role(PgImpl *p, DbErr *out_err) {
  if (!p || !p->conn) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "read-only role check failed: invalid backend state.");
    return ERR;
  }

  static const char *kSql = "SELECT "
                            "  NOT r.rolsuper "
                            "  AND NOT r.rolcreatedb "
                            "  AND NOT r.rolcreaterole "
                            "  AND NOT r.rolreplication "
                            "  AND NOT r.rolbypassrls "
                            "  AND NOT EXISTS ( "
                            "    SELECT 1 "
                            "    FROM pg_catalog.pg_class c "
                            "    WHERE c.relkind IN ('r','p') "
                            "      AND ( "
                            "        has_table_privilege(c.oid, 'INSERT') "
                            "        OR has_table_privilege(c.oid, 'UPDATE') "
                            "        OR has_table_privilege(c.oid, 'DELETE') "
                            "        OR has_table_privilege(c.oid, 'TRUNCATE') "
                            "      ) "
                            "  ) AS is_safe_read_only_role "
                            "FROM pg_catalog.pg_roles r "
                            "WHERE r.rolname = current_user";

  PGresult *res = NULL;
  DbErr db_err;
  ADBX_ERR_CLEAR(&db_err, DBERR_NONE);
  if (pg_exec_single_result(p, kSql, &res, &db_err) != OK) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "%s",
                  db_err.msg[0] != '\0' ? db_err.msg
                                        : "read-only role check failed");
    return ERR;
  }

  int ok = ERR;
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "read-only role check failed: %s",
                  PQerrorMessage(p->conn));
    goto done;
  }
  if (PQntuples(res) != 1 || PQnfields(res) < 1 || PQgetisnull(res, 0, 0)) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "read-only role check returned unexpected result");
    goto done;
  }

  const char *v = PQgetvalue(res, 0, 0);
  if (!v || (strcmp(v, "t") != 0 && strcmp(v, "true") != 0)) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "connected role is not safe for read-only mode");
    goto done;
  }

  ok = OK;
done:
  PQclear(res);
  return ok;
}

/* --------------------------- DbBackend vtable --------------------------- */

static AdbxStatus pg_connect(DbBackend *db, const ConnProfile *profile,
                             const SafetyPolicy *policy, const char *pwd,
                             DbErr *out_err) {
  if (!db || !db->impl || !profile || !policy) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "postgres connect failed: invalid input pointers.");
    return ERR;
  }
  PgImpl *p = (PgImpl *)db->impl;

  // when created, NULL is assigned to p->conn. If it's not NULL, there's
  // another open connection
  if (p->conn) {
    PQfinish(p->conn);
    p->conn = NULL;
  }
  pg_type_name_cache_reset(p);

  const char *port_str = NULL;
  char portbuf[16];
  if (profile->port > 0) {
    snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)profile->port);
    port_str = portbuf;
  }

  const char *keys[] = {"host",     "port",    "dbname", "user",
                        "password", "options", NULL};
  const char *vals[] = {profile->host, port_str, profile->db_name,
                        profile->user, pwd,      profile->options,
                        NULL};

  p->conn = PQconnectdbParams(keys, vals, 0);
  if (!p->conn) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "PQconnectdb returned NULL");
    return ERR;
  }

  if (PQstatus(p->conn) != CONNECTION_OK) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC, "connection failed: %s",
                  PQerrorMessage(p->conn));
    PQfinish(p->conn);
    p->conn = NULL;
    return ERR;
  }

  /* Enforce a one-time role audit at connection time and fail closed on
   * mismatch. */
  if (policy->read_only) {
    if (pg_check_safe_read_only_role(p, out_err) != OK) {
      PQfinish(p->conn);
      p->conn = NULL;
      return ERR;
    }
  }

  p->policy = *policy;
  p->policy_applied = 0;
  if (pg_type_name_cache_load(p) != OK) {
    TLOG("WARN - failed to preload Postgres type-name cache; falling back to "
         "OID strings in column metadata");
  }
  return OK;
}

static AdbxTriStatus pg_is_connected(DbBackend *db) {
  if (!db || !db->impl)
    return ERR;
  PgImpl *p = (PgImpl *)db->impl;
  if (!p->conn)
    return NO;
  return (PQstatus(p->conn) == CONNECTION_OK) ? YES : NO;
}

static void pg_disconnect(DbBackend *db) {
  if (!db || !db->impl)
    return;
  PgImpl *p = (PgImpl *)db->impl;
  if (p->conn) {
    PQfinish(p->conn);
    p->conn = NULL;
  }
  pg_type_name_cache_reset(p);
  p->policy_applied = 0;
}

static void pg_destroy(DbBackend *db) {
  if (!db || !db->impl)
    return;
  pg_disconnect(db);
  PgImpl *p = (PgImpl *)db->impl;
  free(p);
  free(db);
}

/* Executes one SQL statement (optionally with bound positional params) and
 * materializes either one QueryResult or one backend tool-error message.
 * Side effects: executes SQL, enforces policy/transactions, and writes
 * diagnostics into one local transient buffer.
 * Returns OK when 'out_res' is populated with either a QueryResult or a
 * tool-error message, ERR only on catastrophic allocation/input failures.
 */
static AdbxStatus pg_exec_impl(DbBackend *db, const char *sql,
                               const DbExecParam *params, uint32_t nparams,
                               const QueryResultBuildPolicy *qb_policy,
                               DbExecResult *out_res) {
  DbErr db_err;
  const char *safe_msg;
  QueryResult *qr = NULL;
  PGresult *res = NULL;

  ADBX_ERR_CLEAR(&db_err, DBERR_NONE);

  if (!db || !db->impl || !sql || !out_res || (nparams > 0 && params == NULL)) {
    ADBX_ERR_SETF(&db_err, DBERR_INPUT,
                  "unexpected input before executing the query");
    goto fail_bad_input;
  };

  PgImpl *p = (PgImpl *)db->impl;
  if (!p->conn) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "not connected");
    goto fail;
  }

  // even if this limit is version-dependent, it's a defensive check
  if (strlen(sql) > PG_QUERY_MAX_BYTES) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "SQL exceeds 8192 bytes (libpq query buffer limit)");
    goto fail;
  }

  // apply safety policy
  if (!(p->policy_applied)) {
    if (pg_apply_policy(p, &db_err) != OK) {
      goto fail;
    }
  }

  // start counting for query execution time
  uint64_t t0 = now_ms_monotonic();

  // start a read-only transaction for every query
  if (p->policy.read_only) {
    if (pg_exec_command(p, "BEGIN READ ONLY", &db_err) != OK) {
      goto fail;
    }
  } else {
    if (pg_exec_command(p, "BEGIN", &db_err) != OK) {
      goto fail;
    }
  }

  if (nparams > 0) {
    if (pg_exec_single_result_bound(p, sql, params, nparams, &res, &db_err) !=
        OK)
      goto fail;
  } else {
    if (pg_exec_single_result(p, sql, &res, &db_err) != OK)
      goto fail;
  }

  ExecStatusType st = PQresultStatus(res);

  // if backend error, rollback and return error
  if (st == PGRES_FATAL_ERROR || st == PGRES_BAD_RESPONSE ||
      st == PGRES_NONFATAL_ERROR) {
    // capture error string
    const char *msg = PQresultErrorMessage(res);
    if (!msg || !*msg)
      msg = PQerrorMessage(p->conn);

    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "%s", msg ? msg : "query failed");

    goto fail;
  }

  // Right now, the agent can't send commands like set, delete...
  // so the status should be PGRES_TUPLES_OK.
  if (st == PGRES_TUPLES_OK) {
    int ncols = PQnfields(res);
    int ntuples = PQntuples(res);

    if (ncols < 0)
      ncols = 0;
    if (ntuples < 0)
      ntuples = 0;

    uint32_t out_cols = (uint32_t)ncols;
    uint32_t out_rows = (uint32_t)ntuples;

    uint8_t result_truncated = 0;
    if (p->policy.max_rows > 0 && out_rows > p->policy.max_rows) {
      out_rows = p->policy.max_rows;
      result_truncated = 1;
    }

    qr = qr_create((uint32_t)ncols, out_rows, result_truncated,
                   p->policy.max_payload_bytes);
    if (!qr) {
      ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "qr_create failed");
      goto fail;
    }
    QueryResultBuilder qb = {0};
    if (qb_init(&qb, qr, qb_policy) != OK) {
      ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "qb_init failed");
      goto fail;
    }

    // Column metadata
    for (uint32_t c = 0; c < out_cols; c++) {
      const char *name = PQfname(res, c);
      // Store empty strings if metadata missing
      if (!name)
        name = "";

      Oid oid = PQftype(res, c);
      char oidbuf[32];
      const PgOidInfo *found = NULL;
      if (p->type_names) {
        found =
            (const PgOidInfo *)bsearch(&oid, p->type_names, p->type_names_len,
                                       sizeof(*p->type_names), pg_oid_info_cmp);
      }

      const char *type_name;
      if (found) {
        type_name = found->readable_v;
      } else {
        // Keep column metadata stable even if the cache is unavailable or the
        // server reports an OID we did not preload.
        snprintf(oidbuf, sizeof(oidbuf), "oid: %u", (unsigned)oid);
        type_name = oidbuf;
      }

      if (qb_set_col(&qb, c, name, type_name, (uint32_t)oid) != OK) {
        ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "qb_set_col failed");
        goto fail;
      }
    }

    // Fill cells (enforces max_query_bytes by stopping when the cap is hit).
    int stop = 0;
    for (uint32_t r = 0; r < out_rows; r++) {
      for (uint32_t c = 0; c < (uint32_t)ncols; c++) {

        char *val;
        size_t val_len = 0;
        if (PQgetisnull(res, r, c))
          val = NULL;
        else {
          val = PQgetvalue(res, (int)r, (int)c);
          val_len = (size_t)PQgetlength(res, (int)r, (int)c);
        }

        int src = qb_set_cell(&qb, r, c, val, val_len);
        if (src == NO) {
          qr->result_truncated = 1;
          qr->nrows = r;
          stop = 1;
          break;
        }
        if (src == ERR) {
          ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "qb_set_cell failed");
          goto fail;
        }
      }
      if (stop)
        break;
    }
  } else {
    // Error status
    const char *msg = PQresStatus(st);
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "%s",
                  msg ? msg : "unexpected PGresult status");
    goto fail;
  }

  PQclear(res);
  res = NULL;

  // commit transaction
  if (pg_exec_command(p, "COMMIT", &db_err) != OK) {
    // If commit fails, try rollback
    pg_rollback(p);
    if (db_err.msg[0] != '\0') {
      char commit_detail[ADBX_ERRMSG_MAX];
      snprintf(commit_detail, sizeof(commit_detail), "%s", db_err.msg);
      ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "COMMIT failure: %.200s",
                    commit_detail);
    } else {
      ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "COMMIT failure");
    }
    goto fail;
  }

  uint64_t t1 = now_ms_monotonic();
  qr->exec_ms = (t1 >= t0) ? (t1 - t0) : 0;

  if (db_exec_result_set_qr(out_res, qr) != OK) {
    qr_destroy(qr);
    return ERR;
  }
  return OK;

fail:
  if (!out_res)
    return ERR; // catastrophic

  TLOG("ERROR - pg_exec failed: %s",
       db_err.msg[0] != '\0' ? db_err.msg : "unknown");
  // rollback is safe even if we haven't executed anything
  pg_rollback(p);
  if (res)
    PQclear(res);
  if (qr)
    qr_destroy(qr);
fail_bad_input:
  safe_msg = (db_err.msg[0] != '\0') ? db_err.msg : "Unknown backend error.";
  return db_exec_result_set_tool_err(out_res, "PostgreSQL execution failed: %s",
                                     safe_msg);
}

static AdbxStatus pg_exec(DbBackend *db, const char *sql,
                          const QueryResultBuildPolicy *qb_policy,
                          DbExecResult *out_res) {
  return pg_exec_impl(db, sql, NULL, 0, qb_policy, out_res);
}

static AdbxStatus pg_exec_bound(DbBackend *db, const char *sql,
                                const DbExecParam *params, uint32_t nparams,
                                const QueryResultBuildPolicy *qb_policy,
                                DbExecResult *out_res) {
  return pg_exec_impl(db, sql, params, nparams, qb_policy, out_res);
}

/* Maps one PostgreSQL relkind code to the backend-neutral DbRelationKind.
 * It borrows no heap state and allocates nothing.
 * Returns one supported DbRelationKind, or DBREL_KIND_NONE on unsupported
 * input.
 */
static DbRelationKind pg_relkind_from_char(char relkind) {
  switch (relkind) {
  case 'r':
    return DBREL_KIND_TABLE;
  case 'v':
    return DBREL_KIND_VIEW;
  case 'm':
    return DBREL_KIND_MATVIEW;
  case 'f':
    return DBREL_KIND_FOREIGN_TABLE;
  default:
    return DBREL_KIND_NONE;
  }
}

/* Parses one libpq textual boolean representation into 0/1.
 * It borrows 'txt' and 'out01' and allocates no memory.
 * Returns YES on successful parse, NO on unrecognized text, ERR on invalid
 * input.
 */
static AdbxTriStatus pg_bool_text_parse01(const char *txt, int *out01) {
  if (!txt || !out01)
    return ERR;
  if (strcmp(txt, "t") == 0 || strcmp(txt, "true") == 0) {
    *out01 = 1;
    return YES;
  }
  if (strcmp(txt, "f") == 0 || strcmp(txt, "false") == 0) {
    *out01 = 0;
    return YES;
  }
  return NO;
}

// the order of the columns of the query below
enum {
  RELF_SCHEMA_NAME = 0,
  RELF_RELATION_NAME = 1,
  RELF_RELATION_KIND = 2,
  RELF_COLUMN_NAME = 3,
  RELF_COLUMN_TYPE = 4,
  RELF_IS_PRIMARY_KEY = 5,
  RELF_IS_FOREIGN_KEY = 6,
  RELF_REF_SCHEMA_NAME = 7,
  RELF_REF_RELATION_NAME = 8,
  RELF_REF_COLUMN_NAME = 9,
  RELF_COUNT = 10,
};
/* Describes one schema-qualified PostgreSQL relation and materializes
 * backend-owned metadata for its columns and key references.
 * Returns OK when 'out_res' is populated with either relation metadata or a
 * tool error, ERR only on catastrophic input or allocation failure.
 */
static AdbxStatus pg_describe_relation(DbBackend *db, const char *schema_name,
                                       const char *relation_name,
                                       DbDescribeResult *out_res) {
  static const char *kSql =
      "SELECT ns.nspname AS schema_name, "
      "       cls.relname AS relation_name, "
      "       cls.relkind::text AS relation_kind, "
      "       a.attname AS column_name, "
      "       pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type, "
      "       CASE WHEN pk.conrelid IS NOT NULL THEN TRUE ELSE FALSE END "
      "         AS is_primary_key, "
      "       CASE WHEN fk.ref_relation_name IS NOT NULL THEN TRUE ELSE FALSE "
      "         END AS is_foreign_key, "
      "       fk.ref_schema_name AS references_schema, "
      "       fk.ref_relation_name AS references_relation, "
      "       fk.ref_column_name AS references_column "
      "FROM pg_catalog.pg_class cls "
      "JOIN pg_catalog.pg_namespace ns "
      "  ON ns.oid = cls.relnamespace "
      "LEFT JOIN pg_catalog.pg_attribute a "
      "  ON a.attrelid = cls.oid "
      " AND a.attnum > 0 "
      " AND NOT a.attisdropped "
      "LEFT JOIN pg_catalog.pg_constraint pk "
      "  ON pk.conrelid = cls.oid "
      " AND pk.contype = 'p' "
      " AND a.attnum = ANY (pk.conkey) "
      "LEFT JOIN LATERAL ( "
      "    SELECT refns.nspname AS ref_schema_name, "
      "           refcls.relname AS ref_relation_name, "
      "           refatt.attname AS ref_column_name "
      "    FROM pg_catalog.pg_constraint c "
      "    CROSS JOIN LATERAL generate_subscripts(c.conkey, 1) AS s(i) "
      "    JOIN pg_catalog.pg_class refcls "
      "      ON refcls.oid = c.confrelid "
      "    JOIN pg_catalog.pg_namespace refns "
      "      ON refns.oid = refcls.relnamespace "
      "    JOIN pg_catalog.pg_attribute refatt "
      "      ON refatt.attrelid = c.confrelid "
      "     AND refatt.attnum = c.confkey[s.i] "
      "    WHERE c.contype = 'f' "
      "      AND c.conrelid = cls.oid "
      "      AND a.attnum = c.conkey[s.i] "
      "    ORDER BY c.oid, s.i "
      "    LIMIT 1 "
      ") fk ON a.attnum IS NOT NULL "
      "WHERE ns.nspname = $1 "
      "  AND cls.relname = $2 "
      "  AND cls.relkind IN ('r', 'v', 'm', 'f') "
      "ORDER BY a.attnum";
  DbErr db_err;
  const char *safe_msg = NULL;
  PGresult *res = NULL;
  DbRelationInfo *info = NULL;

  ADBX_ERR_CLEAR(&db_err, DBERR_NONE);
  if (!db || !db->impl || !schema_name || !relation_name || !out_res ||
      schema_name[0] == '\0' || relation_name[0] == '\0') {
    ADBX_ERR_SETF(&db_err, DBERR_INPUT,
                  "unexpected input before describing the relation");
    goto fail_bad_input;
  }

  PgImpl *p = (PgImpl *)db->impl;
  if (!p->conn) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "not connected");
    goto fail;
  }

  const DbExecParam params[] = {
      {.value = schema_name, .value_len = (uint32_t)strlen(schema_name)},
      {.value = relation_name, .value_len = (uint32_t)strlen(relation_name)},
  };
  if (pg_exec_single_result_bound(p, kSql, params, (uint32_t)ARRLEN(params),
                                  &res, &db_err) != OK) {
    goto fail;
  }

  ExecStatusType st = PQresultStatus(res);
  if (st == PGRES_FATAL_ERROR || st == PGRES_BAD_RESPONSE ||
      st == PGRES_NONFATAL_ERROR) {
    const char *msg = PQresultErrorMessage(res);
    if (!msg || !*msg)
      msg = PQerrorMessage(p->conn);
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "%s",
                  msg ? msg : "relation describe failed");
    goto fail;
  }
  if (st != PGRES_TUPLES_OK) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "%s",
                  PQresStatus(st) ? PQresStatus(st)
                                  : "unexpected PGresult status");
    goto fail;
  }

  int nfields = PQnfields(res);
  int nrows = PQntuples(res);
  if (nrows < 1) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "relation \"%s.%s\" not found or unsupported", schema_name,
                  relation_name);
    goto fail;
  }
  if (nfields < RELF_COUNT) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "relation describe returned unexpected shape");
    goto fail;
  }

  if (PQgetisnull(res, 0, RELF_SCHEMA_NAME) ||
      PQgetisnull(res, 0, RELF_RELATION_NAME) ||
      PQgetisnull(res, 0, RELF_RELATION_KIND)) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "relation describe returned incomplete metadata");
    goto fail;
  }

  const char *schema_txt = PQgetvalue(res, 0, RELF_SCHEMA_NAME);
  const char *relation_txt = PQgetvalue(res, 0, RELF_RELATION_NAME);
  const char *kind_txt = PQgetvalue(res, 0, RELF_RELATION_KIND);
  if (!schema_txt || !relation_txt || !kind_txt || kind_txt[0] == '\0') {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "relation describe returned invalid identity metadata");
    goto fail;
  }

  DbRelationKind kind = pg_relkind_from_char(kind_txt[0]);
  if (kind == DBREL_KIND_NONE) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "relation describe returned unsupported relation kind");
    goto fail;
  }

  uint32_t ncols = 0;
  for (int row = 0; row < nrows; row++) {
    if (!PQgetisnull(res, row, RELF_COLUMN_NAME))
      ncols++;
  }

  info = db_relation_info_create(ncols);
  if (!info) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "db_relation_info_create failed");
    goto fail;
  }
  if (db_relation_info_set_identity(info, schema_txt, relation_txt, kind) !=
      OK) {
    ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                  "db_relation_info_set_identity failed");
    goto fail;
  }

  uint32_t out_col = 0;
  for (int row = 0; row < nrows; row++) {
    if (PQgetisnull(res, row, RELF_COLUMN_NAME))
      continue;

    const char *col_name = PQgetvalue(res, row, RELF_COLUMN_NAME);
    const char *col_type = PQgetisnull(res, row, RELF_COLUMN_TYPE)
                               ? NULL
                               : PQgetvalue(res, row, RELF_COLUMN_TYPE);

    int is_primary_key = 0;
    int is_foreign_key = 0;
    if (pg_bool_text_parse01(PQgetvalue(res, row, RELF_IS_PRIMARY_KEY),
                             &is_primary_key) != YES ||
        pg_bool_text_parse01(PQgetvalue(res, row, RELF_IS_FOREIGN_KEY),
                             &is_foreign_key) != YES) {
      ADBX_ERR_SETF(&db_err, DBERR_GENERIC,
                    "relation describe returned invalid boolean metadata");
      goto fail;
    }

    const char *ref_schema = PQgetisnull(res, row, RELF_REF_SCHEMA_NAME)
                                 ? NULL
                                 : PQgetvalue(res, row, RELF_REF_SCHEMA_NAME);
    const char *ref_relation =
        PQgetisnull(res, row, RELF_REF_RELATION_NAME)
            ? NULL
            : PQgetvalue(res, row, RELF_REF_RELATION_NAME);
    const char *ref_column = PQgetisnull(res, row, RELF_REF_COLUMN_NAME)
                                 ? NULL
                                 : PQgetvalue(res, row, RELF_REF_COLUMN_NAME);

    if (db_relation_info_set_col(info, out_col, col_name, col_type,
                                 (uint8_t)is_primary_key,
                                 (uint8_t)is_foreign_key, ref_schema,
                                 ref_relation, ref_column) != OK) {
      ADBX_ERR_SETF(&db_err, DBERR_GENERIC, "db_relation_info_set_col failed");
      goto fail;
    }
    out_col++;
  }

  PQclear(res);
  res = NULL;

  if (db_describe_result_set_relation_info(out_res, info) != OK) {
    db_relation_info_destroy(info);
    return ERR;
  }
  return OK;

fail:
  if (!out_res)
    return ERR;

  TLOG("ERROR - pg_describe_relation failed: %s",
       db_err.msg[0] != '\0' ? db_err.msg : "unknown");
  if (res)
    PQclear(res);
  if (info)
    db_relation_info_destroy(info);
fail_bad_input:
  safe_msg = (db_err.msg[0] != '\0') ? db_err.msg : "Unknown backend error.";
  return db_describe_result_set_tool_err(
      out_res, "PostgreSQL relation describe failed: %s", safe_msg);
}

// This generated allowlist is a pure-function policy input. Keep time/random
// and other server-state-dependent functions out of
// meta/pg_safe_functions.json.
#include "pg_safe_func.generated.inc"

static const DbSafeFuncList *pg_safe_functions(DbBackend *db) {
  (void)db;
  static const DbSafeFuncList list = {
      .names = PG_SAFE_FUNCS,
      .count = (uint32_t)(sizeof(PG_SAFE_FUNCS) / sizeof(PG_SAFE_FUNCS[0]))};
  return &list;
}

/* ------------------------- constructor ------------------------- */

static const DbBackendVTable PG_VT = {.connect = pg_connect,
                                      .is_connected = pg_is_connected,
                                      .disconnect = pg_disconnect,
                                      .destroy = pg_destroy,
                                      .exec = pg_exec,
                                      .exec_bound = pg_exec_bound,
                                      .describe_relation = pg_describe_relation,
                                      .make_query_ir = pg_make_query_ir,
                                      .safe_functions = pg_safe_functions};

DbBackend *postgres_backend_create(void) {
  DbBackend *db = (DbBackend *)xcalloc(1, sizeof(DbBackend));

  PgImpl *impl = (PgImpl *)xcalloc(1, sizeof(PgImpl));

  impl->conn = NULL;

  db->vt = &PG_VT;
  db->impl = impl;
  return db;
}
