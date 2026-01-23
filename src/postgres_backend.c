#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pg_query.h>
#include <libpq-fe.h>

#include "postgres_backend.h"
#include "query_result.h"
#include "safety_policy.h"
#include "utils.h"
#include "conn_catalog.h"
#include "log.h"
#include "json_codec.h"
#include "string_op.h"

#define PG_QUERY_MAX_BYTES 8192

/* ------------------------------- internals ------------------------------- */

typedef struct PgImpl {
    PGconn *conn;
    SafetyPolicy policy;
    uint8_t policy_applied;     // 1 if the policy has already been enforced
                                // at session level, else 0
    char last_err[1024];
} PgImpl;

// --------------------------- QueryIR helpers (Postgres) --------------------

/* Transfers an owned NUL-terminated string into the arena.
 * Ownership: caller transfers ownership of 'owned' to the arena.
 * Side effects: allocates arena memory, frees 'owned'.
 * Returns NULL on error. */
static char *pg_arena_transfer(PlArena *a, char *owned) {
    if (!a || !owned) return NULL;
    char *dst = (char *)pl_arena_add(a, owned, (uint32_t)strlen(owned));
    free(owned);
    return dst;
}

/* Parses an alias object and returns its name or NULL if missing.
 * We accept both a direct alias object and an {"Alias":{...}} wrapper because
 * libpg_query shape varies by version.
 * Ownership: returned string is arena-owned.
 * Side effects: allocates arena memory and frees a temporary string.
 * Returns NULL if alias is absent or invalid. */
static char *pg_parse_alias_name(const JsonGetter *alias_obj, PlArena *a) {
    if (!alias_obj || !a) return NULL;

    const JsonGetter *src = alias_obj;
    JsonGetter inner = {0};
    if (jsget_object(alias_obj, "Alias", &inner) == YES) {
        src = &inner;
    }

    char *tmp = NULL;
    if (jsget_string_decode_alloc(src, "aliasname", &tmp) != YES) return NULL;
    return pg_arena_transfer(a, tmp);
}

/* Gets a decoded string from one of two candidate keys.
 * libpg_query uses "str" vs "sval" for strings depending on node/versions.
 * Ownership: returns a malloc'd string; caller must free or transfer.
 * Side effects: allocates memory.
 * Returns YES/NO/ERR. */
static int pg_get_string_field(const JsonGetter *jg, const char *k1, const char *k2,
                               char **out) {
    int rc = jsget_string_decode_alloc(jg, k1, out);
    if (rc == YES || rc == ERR) return rc;
    if (!k2) return NO;
    return jsget_string_decode_alloc(jg, k2, out);
}

/* Allocates and initializes a QirQuery inside the arena.
 * Ownership: returned pointer is owned by the arena.
 * Side effects: allocates arena memory.
 * Returns NULL on error. */
static inline QirQuery *pg_qir_new_query(PlArena *a) {
    if (!a) return NULL;
    QirQuery *q = (QirQuery *)pl_arena_alloc(a, (uint32_t)sizeof(*q));
    if (!q) return NULL;
    q->status = QIR_OK;
    q->kind = QIR_STMT_SELECT;
    q->limit_value = -1;
    return q;
}

/* Allocates a QirExpr inside the arena.
 * Ownership: returned pointer is owned by the arena.
 * Side effects: allocates arena memory.
 * Returns NULL on error. */
static inline QirExpr *pg_qir_new_expr(PlArena *a, QirExprKind kind) {
    QirExpr *e = (QirExpr *)pl_arena_alloc(a, (uint32_t)sizeof(*e));
    if (!e) return NULL;
    e->kind = kind;
    return e;
}

/* Parses a ColumnRef node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set q->has_star or q->has_unsupported.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_colref(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    if (!jg || !a || !q) return NULL;

    JsonArrIter it = {0};
    int rc = jsget_array_objects_begin(jg, "fields", &it);
    if (rc != YES) return NULL;

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
            if (pg_get_string_field(&sub, "str", "sval", &tmp) != YES) return NULL;
            if (nparts < 3) {
                parts[nparts++] = pg_arena_transfer(a, tmp);
                if (!parts[nparts - 1]) return NULL;
            } else {
                free(tmp);
            }
            continue;
        }

        q->has_unsupported = true;
    }

    if (saw_star) {
        q->has_star = true;
        if (nparts > 1) {
            q->has_unsupported = true;
            return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
        }
        QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_COLREF);
        if (!e) return NULL;
        e->u.colref.qualifier.name = (nparts == 1) ? parts[0]
            : (char *)pl_arena_add(a, (void *)"", 0);
        e->u.colref.column.name = (char *)pl_arena_add(a, (void *)"*", 1);
        return e;
    }
    if (nparts == 0 || nparts > 2) {
        q->has_unsupported = true;
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }

    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_COLREF);
    if (!e) return NULL;

    if (nparts == 1) {
        e->u.colref.qualifier.name = (char *)pl_arena_add(a, (void *)"", 0);
        e->u.colref.column.name = parts[0];
    } else {
        e->u.colref.qualifier.name = parts[0];
        e->u.colref.column.name = parts[1];
    }

    return e;
}

/* Parses a simple literal (A_Const).
 * Ownership: returned expression is arena-owned.
 * Side effects: may set q->has_unsupported.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_literal(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    if (!jg || !a || !q) return NULL;

    JsonGetter vjg = {0};
    if (jsget_object(jg, "val", &vjg) != YES) {
        vjg = *jg; // libpg_query JSON uses top-level ival/fval/sval/isnull
    }

    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_LITERAL);
    if (!e) return NULL;

        JsonGetter ijg = {0};
        if (jsget_object(&vjg, "ival", &ijg) == YES) {
            int64_t i64 = 0;
            if (jsget_i64(&ijg, "ival", &i64) != YES) return NULL;
            e->u.lit.kind = QIR_LIT_INT64;
            e->u.lit.v.i64 = i64;
            return e;
        }

        JsonGetter fjg = {0};
        if (jsget_object(&vjg, "fval", &fjg) == YES) {
            char *tmp = NULL;
            if (jsget_string_decode_alloc(&fjg, "fval", &tmp) != YES) return NULL;
            char *end = NULL;
            double f64 = strtod(tmp, &end);
            if (!end || *end != '\0') { free(tmp); return NULL; }
            free(tmp);
            e->u.lit.kind = QIR_LIT_FLOAT64;
            e->u.lit.v.f64 = f64;
            return e;
        }

        JsonGetter sjg = {0};
        if (jsget_object(&vjg, "sval", &sjg) == YES) {
            char *tmp = NULL;
            if (jsget_string_decode_alloc(&sjg, "sval", &tmp) != YES) return NULL;
            char *s = pg_arena_transfer(a, tmp);
            if (!s) return NULL;
            e->u.lit.kind = QIR_LIT_STRING;
            e->u.lit.v.s = s;
            return e;
        }

        int isnull = 0;
        if (jsget_bool01(&vjg, "isnull", &isnull) == YES && isnull) {
            e->u.lit.kind = QIR_LIT_NULL;
            return e;
        }

        JsonGetter ijg2 = {0};
        if (jsget_object(&vjg, "Integer", &ijg2) == YES) {
            int64_t i64 = 0;
            if (jsget_i64(&ijg2, "ival", &i64) != YES) return NULL;
            e->u.lit.kind = QIR_LIT_INT64;
            e->u.lit.v.i64 = i64;
            return e;
        }

        JsonGetter fjg2 = {0};
        if (jsget_object(&vjg, "Float", &fjg2) == YES) {
            char *tmp = NULL;
            if (pg_get_string_field(&fjg2, "str", "sval", &tmp) != YES) return NULL;
            char *end = NULL;
            double f64 = strtod(tmp, &end);
            if (!end || *end != '\0') { free(tmp); return NULL; }
            free(tmp);
            e->u.lit.kind = QIR_LIT_FLOAT64;
            e->u.lit.v.f64 = f64;
            return e;
        }

        JsonGetter sjg2 = {0};
        if (jsget_object(&vjg, "String", &sjg2) == YES) {
            char *tmp = NULL;
            if (pg_get_string_field(&sjg2, "str", "sval", &tmp) != YES) return NULL;
            char *s = pg_arena_transfer(a, tmp);
            if (!s) return NULL;
            e->u.lit.kind = QIR_LIT_STRING;
            e->u.lit.v.s = s;
            return e;
        }

        JsonGetter njg = {0};
        if (jsget_object(&vjg, "Null", &njg) == YES) {
            e->u.lit.kind = QIR_LIT_NULL;
            return e;
        }

    q->has_unsupported = true;
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
}

/* Parses an expression node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set query flags.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_expr(const JsonGetter *jg, PlArena *a, QirQuery *q);

/* Parses a boolean expression list into a left-deep binary tree.
 * Ownership: returned expression is arena-owned.
 * Side effects: none (aside from allocations).
 * Returns NULL on allocation error. */
static QirExpr *pg_fold_bool_expr(
    PlArena *a, QirQuery *q, QirExprKind kind, QirExpr **items, uint32_t nitems
) {
    if (!a || !q || !items || nitems == 0) return NULL;
    if (nitems == 1) return items[0];

    QirExpr *acc = items[0];
    for (uint32_t i = 1; i < nitems; i++) {
        QirExpr *e = pg_qir_new_expr(a, kind);
        if (!e) return NULL;
        e->u.bin.l = acc;
        e->u.bin.r = items[i];
        acc = e;
    }
    return acc;
}

/* Parses a BoolExpr node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set q->has_unsupported.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_bool_expr(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "args", &it) != YES) return NULL;

    PtrVec args = {0};
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
        QirExpr *arg = pg_parse_expr(&elem, a, q);
        if (!arg) break;
        if (ptrvec_push(&args, arg) != OK) break;
    }

    if (rc == ERR || args.len == 0) {
        ptrvec_clean(&args);
        q->has_unsupported = true;
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }

    QirExprKind kind = QIR_EXPR_UNSUPPORTED;
    char *op = NULL;
    if (jsget_string_decode_alloc(jg, "boolop", &op) == YES) {
        if (strcmp(op, "AND_EXPR") == 0) kind = QIR_EXPR_AND;
        else if (strcmp(op, "OR_EXPR") == 0) kind = QIR_EXPR_OR;
        else if (strcmp(op, "NOT_EXPR") == 0) kind = QIR_EXPR_NOT;
        free(op);
    }

    if (kind == QIR_EXPR_UNSUPPORTED) {
        free(args.items);
        q->has_unsupported = true;
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }

    QirExpr *res = NULL;
    if (kind == QIR_EXPR_NOT) {
        QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_NOT);
        if (e) e->u.bin.l = (QirExpr *)args.items[0];
        res = e;
    } else {
        res = pg_fold_bool_expr(a, q, kind, (QirExpr **)args.items, args.len);
    }

    ptrvec_clean(&args);
    return res;
}

/* Parses an A_Expr node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set q->has_unsupported.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_aexpr(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "name", &it) != YES) return NULL;

    JsonGetter elem = {0};
    if (jsget_array_objects_next(jg, &it, &elem) != YES) return NULL;

    JsonGetter sjg = {0};
    if (jsget_object(&elem, "String", &sjg) != YES) return NULL;

    char *op = NULL;
    if (pg_get_string_field(&sjg, "str", "sval", &op) != YES) return NULL;

    // parse operands
    JsonGetter ljg = {0};
    JsonGetter rjg = {0};
    if (jsget_object(jg, "lexpr", &ljg) != YES) { free(op); return NULL; }
    if (jsget_object(jg, "rexpr", &rjg) != YES) { free(op); return NULL; }

    QirExpr *lhs = pg_parse_expr(&ljg, a, q);
    QirExpr *rhs = pg_parse_expr(&rjg, a, q);
    if (!lhs || !rhs) { free(op); return NULL; }

    QirExprKind kind = QIR_EXPR_UNSUPPORTED;
    if (strcmp(op, "=") == 0) kind = QIR_EXPR_EQ;
    else if (strcmp(op, "<>") == 0) kind = QIR_EXPR_NE;
    else if (strcmp(op, "!=") == 0) kind = QIR_EXPR_NE;
    else if (strcmp(op, ">") == 0) kind = QIR_EXPR_GT;
    else if (strcmp(op, ">=") == 0) kind = QIR_EXPR_GE;
    else if (strcmp(op, "<") == 0) kind = QIR_EXPR_LT;
    else if (strcmp(op, "<=") == 0) kind = QIR_EXPR_LE;
    free(op);

    if (kind == QIR_EXPR_UNSUPPORTED) {
        q->has_unsupported = true;
        return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
    }

    QirExpr *e = pg_qir_new_expr(a, kind);
    if (!e) return NULL;
    e->u.bin.l = lhs;
    e->u.bin.r = rhs;
    return e;
}

/* Parses a FuncCall node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set q->has_unsupported.
 * Returns NULL on allocation error. */
static QirExpr *pg_parse_func_call(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "funcname", &it) != YES) return NULL;

    // Join funcname parts with '.'
    StrBuf sb = {0};
    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
        JsonGetter sjg = {0};
        if (jsget_object(&elem, "String", &sjg) != YES) { rc = ERR; break; }
        char *tmp = NULL;
        if (pg_get_string_field(&sjg, "str", "sval", &tmp) != YES) { rc = ERR; break; }
        if (sb.len > 0 && sb_append_bytes(&sb, ".", 1) != OK) { free(tmp); rc = ERR; break; }
        if (sb_append_bytes(&sb, tmp, strlen(tmp)) != OK) { free(tmp); rc = ERR; break; }
        free(tmp);
    }

    if (rc == ERR || sb.len == 0) {
        sb_clean(&sb);
        return NULL;
    }

    char *fname = (char *)pl_arena_add(a, sb.data, (uint32_t)sb.len);
    sb_clean(&sb);
    if (!fname) return NULL;

    QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_FUNCALL);
    if (!e) return NULL;
    e->u.funcall.name.name = fname;

    // parse args
    PtrVec args = {0};
    if (jsget_array_objects_begin(jg, "args", &it) == YES) {
        JsonGetter arg = {0};
        while ((rc = jsget_array_objects_next(jg, &it, &arg)) == YES) {
            QirExpr *ae = pg_parse_expr(&arg, a, q);
            if (!ae) { rc = ERR; break; }
            if (ptrvec_push(&args, ae) != OK) { rc = ERR; break; }
        }
    }

    if (args.len > 0) {
        e->u.funcall.args = (QirExpr **)ptrvec_flatten(&args, a);
        e->u.funcall.nargs = args.len;
    }
    ptrvec_clean(&args);
    return e;
}

/* Parses a TypeName into a QirTypeRef.
 * Ownership: type names are arena-owned.
 * Side effects: allocates arena memory.
 * Returns OK on success, ERR on parse/allocation failure. */
static int pg_parse_typename(const JsonGetter *jg, PlArena *a, QirTypeRef *out) {
    if (!jg || !a || !out) return ERR;

    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "names", &it) != YES) return ERR;

    char *parts[2] = {0}; // we only model schema.name; extra parts get folded into name
    uint32_t nparts = 0;
    StrBuf sb = {0};
    int use_sb = 0;

    JsonGetter elem = {0};
    int rc = 0;
    while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
        JsonGetter sjg = {0};
        if (jsget_object(&elem, "String", &sjg) != YES) { rc = ERR; break; }
        char *tmp = NULL;
        if (jsget_string_decode_alloc(&sjg, "str", &tmp) != YES) { rc = ERR; break; }

        if (!use_sb && nparts < 2) {
            parts[nparts++] = pg_arena_transfer(a, tmp);
            if (!parts[nparts - 1]) { rc = ERR; break; }
        } else {
            if (!use_sb) {
                if (sb_append_bytes(&sb, parts[0], strlen(parts[0])) != OK) { free(tmp); rc = ERR; break; }
                if (nparts > 1) {
                    if (sb_append_bytes(&sb, ".", 1) != OK) { free(tmp); rc = ERR; break; }
                    if (sb_append_bytes(&sb, parts[1], strlen(parts[1])) != OK) { free(tmp); rc = ERR; break; }
                }
                use_sb = 1;
            }
            if (sb_append_bytes(&sb, ".", 1) != OK) { free(tmp); rc = ERR; break; }
            if (sb_append_bytes(&sb, tmp, strlen(tmp)) != OK) { free(tmp); rc = ERR; break; }
            free(tmp);
        }
    }

    if (rc == ERR || nparts == 0) {
        sb_clean(&sb);
        return ERR;
    }

    if (use_sb) {
        char *name = (char *)pl_arena_add(a, sb.data, (uint32_t)sb.len);
        sb_clean(&sb);
        if (!name) return ERR;
        out->schema.name = (char *)pl_arena_add(a, (void *)"", 0);
        out->name.name = name;
        return OK;
    }

    sb_clean(&sb);
    if (nparts == 1) {
        out->schema.name = (char *)pl_arena_add(a, (void *)"", 0);
        out->name.name = parts[0];
    } else {
        out->schema.name = parts[0];
        out->name.name = parts[1];
    }
    return OK;
}

/* Parses a SELECT statement object into QirQuery (forward decl).
 * Ownership: all nodes/arrays are arena-owned.
 * Side effects: may set query flags.
 * Returns OK/ERR on allocation failure. */
static int pg_parse_select_stmt(const JsonGetter *jg, PlArena *a, QirQuery *q);

/* Parses an expression node into a QirExpr.
 * Ownership: returned expression is arena-owned.
 * Side effects: may set query flags.
 * Returns NULL on allocation failure. */
static QirExpr *pg_parse_expr(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    if (!jg || !a || !q) return NULL;

    JsonGetter sub = {0};
    if (jsget_object(jg, "ColumnRef", &sub) == YES) {
        return pg_parse_colref(&sub, a, q);
    }

    if (jsget_object(jg, "ParamRef", &sub) == YES) {
        uint32_t n = 0;
        if (jsget_u32(&sub, "number", &n) != YES) return NULL;
        QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_PARAM);
        if (!e) return NULL;
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

    if (jsget_object(jg, "FuncCall", &sub) == YES) {
        return pg_parse_func_call(&sub, a, q);
    }

    if (jsget_object(jg, "SubLink", &sub) == YES) {
        q->has_subquery = true;
        QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_SUBQUERY);
        if (!e) return NULL;

        JsonGetter subjg = {0};
        if (jsget_object(&sub, "subselect", &subjg) != YES) {
            q->has_unsupported = true;
            return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
        }
        JsonGetter seljg = {0};
        if (jsget_object(&subjg, "SelectStmt", &seljg) != YES) {
            q->has_unsupported = true;
            return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
        }
        QirQuery *sq = pg_qir_new_query(a);
        if (!sq) return NULL;
        e->u.subquery = sq;
        pg_parse_select_stmt(&seljg, a, sq);
        return e;
    }

    if (jsget_object(jg, "TypeCast", &sub) == YES) {
        q->has_cast = true;
        QirExpr *e = pg_qir_new_expr(a, QIR_EXPR_CAST);
        if (!e) return NULL;

        JsonGetter argjg = {0};
        if (jsget_object(&sub, "arg", &argjg) != YES) return NULL;
        QirExpr *arg = pg_parse_expr(&argjg, a, q);
        if (!arg) return NULL;
        e->u.cast.expr = arg;

        JsonGetter tnjg = {0};
        if (jsget_object(&sub, "typeName", &tnjg) != YES) {
            q->has_unsupported = true;
            return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
        }
        JsonGetter tnjg2 = {0};
        if (jsget_object(&tnjg, "TypeName", &tnjg2) == YES) {
            tnjg = tnjg2;
        }
        if (pg_parse_typename(&tnjg, a, &e->u.cast.type) != OK) {
            q->has_unsupported = true;
            return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
        }
        return e;
    }

    q->has_unsupported = true;
    return pg_qir_new_expr(a, QIR_EXPR_UNSUPPORTED);
}

/* Parses a RangeVar node into a QirFromItem.
 * Ownership: returned node is arena-owned.
 * Side effects: none.
 * Returns NULL on allocation failure. */
static QirFromItem *pg_parse_rangevar(const JsonGetter *jg, PlArena *a) {
    QirFromItem *fi = pl_arena_alloc(a, (uint32_t)sizeof(QirFromItem));
    if (!fi) return NULL;
    fi->kind = QIR_FROM_BASE_REL;

    char *tmp = NULL;
    if (jsget_string_decode_alloc(jg, "relname", &tmp) == YES) {
        fi->u.rel.name.name = pg_arena_transfer(a, tmp);
    }

    tmp = NULL;
    if (jsget_string_decode_alloc(jg, "schemaname", &tmp) == YES) {
        fi->u.rel.schema.name = pg_arena_transfer(a, tmp);
    } else {
        fi->u.rel.schema.name = (char *)pl_arena_add(a, (void *)"", 0);
    }

    // alias
    JsonGetter ajg = {0};
    if (jsget_object(jg, "alias", &ajg) == YES) {
        fi->alias.name = pg_parse_alias_name(&ajg, a);
    }

    if (!fi->alias.name) fi->alias.name = (char *)pl_arena_add(a, (void *)"", 0);
    return fi;
}

/* Parses a range item or join and populates froms/joins (left-deep).
 * Ownership: from/joins vectors own their temporary buffers.
 * Side effects: may set q->has_unsupported.
 * Returns OK/ERR. */
static int pg_parse_from_item(
    const JsonGetter *jg, PlArena *a, QirQuery *q, PtrVec *froms, PtrVec *joins
);

/* Parses a join expression into from-items and joins (left-deep).
 * Ownership: join nodes are arena-owned.
 * Side effects: may set q->has_unsupported.
 * Returns OK/ERR. */
static int pg_parse_join_expr(
    const JsonGetter *jg, PlArena *a, QirQuery *q, PtrVec *froms, PtrVec *joins
) {
    // left
    JsonGetter ljg = {0};
    if (jsget_object(jg, "larg", &ljg) != YES) return ERR;
    if (pg_parse_from_item(&ljg, a, q, froms, joins) != OK) return ERR;

    // join type
    int jointype = 0;
    int64_t v = 0;
    if (jsget_i64(jg, "jointype", &v) == YES) jointype = (int)v;
    if (jsget_exists_nonnull(jg, "usingClause") == YES) q->has_unsupported = true;
    if (jsget_exists_nonnull(jg, "isNatural") == YES) q->has_unsupported = true;

    QirJoin *j = pl_arena_alloc(a, (uint32_t)sizeof(QirJoin));
    if (!j) return ERR;
    switch (jointype) {
        case 0: j->kind = QIR_JOIN_INNER; break;
        case 1: j->kind = QIR_JOIN_LEFT; break;
        case 2: j->kind = QIR_JOIN_FULL; break;
        case 3: j->kind = QIR_JOIN_RIGHT; break;
        default: j->kind = QIR_JOIN_UNSUPPORTED; q->has_unsupported = true; break;
    }

    // right
    JsonGetter rjg = {0};
    if (jsget_object(jg, "rarg", &rjg) != YES) return ERR;

    if (jsget_object(&rjg, "RangeVar", &rjg) == YES) {
        j->rhs = pg_parse_rangevar(&rjg, a);
    } else if (jsget_object(&rjg, "RangeSubselect", &rjg) == YES) {
        q->has_subquery = true;
        JsonGetter ssjg = rjg;
        JsonGetter subjg = {0};
        if (jsget_object(&ssjg, "subquery", &subjg) == YES) {
            JsonGetter seljg = {0};
            if (jsget_object(&subjg, "SelectStmt", &seljg) == YES) {
                QirFromItem *fi = pl_arena_alloc(a, (uint32_t)sizeof(QirFromItem));
                if (fi) {
                    fi->kind = QIR_FROM_SUBQUERY;
                    fi->alias.name = (char *)pl_arena_add(a, (void *)"", 0);
                    fi->u.subquery = pg_qir_new_query(a);
                    if (fi->u.subquery) {
                        pg_parse_select_stmt(&seljg, a, fi->u.subquery);
                    }
                    JsonGetter ajg = {0};
                    if (jsget_object(&ssjg, "alias", &ajg) == YES) {
                        fi->alias.name = pg_parse_alias_name(&ajg, a);
                    }
                    j->rhs = fi;
                }
            }
        }
    } else {
        q->has_unsupported = true;
        j->rhs = pl_arena_alloc(a, (uint32_t)sizeof(QirFromItem));
        if (j->rhs) j->rhs->kind = QIR_FROM_UNSUPPORTED;
    }

    // ON clause
    JsonGetter qjg = {0};
    if (jsget_object(jg, "quals", &qjg) == YES) {
        j->on = pg_parse_expr(&qjg, a, q);
    }

    if (ptrvec_push(joins, j) != OK) return ERR;
    return OK;
}

/* Parses a range item or join and populates froms/joins (left-deep).
 * Ownership: from/join nodes are arena-owned.
 * Side effects: may set q->has_unsupported.
 * Returns OK/ERR. */
static int pg_parse_from_item(
    const JsonGetter *jg, PlArena *a, QirQuery *q, PtrVec *froms, PtrVec *joins
) {
    if (!jg || !a || !q) return ERR;

    JsonGetter rvjg = {0};
    if (jsget_object(jg, "RangeVar", &rvjg) == YES) {
        QirFromItem *fi = pg_parse_rangevar(&rvjg, a);
        if (!fi) return ERR;
        return ptrvec_push(froms, fi);
    }

    JsonGetter jjg = {0};
    if (jsget_object(jg, "JoinExpr", &jjg) == YES) {
        return pg_parse_join_expr(&jjg, a, q, froms, joins);
    }

    JsonGetter ssjg = {0};
    if (jsget_object(jg, "RangeSubselect", &ssjg) == YES) {
        q->has_subquery = true;
        QirFromItem *fi = pl_arena_alloc(a, (uint32_t)sizeof(QirFromItem));
        if (!fi) return ERR;
        fi->kind = QIR_FROM_SUBQUERY;
        fi->alias.name = (char *)pl_arena_add(a, (void *)"", 0);

        JsonGetter subjg = {0};
        if (jsget_object(&ssjg, "subquery", &subjg) == YES) {
            JsonGetter seljg = {0};
            if (jsget_object(&subjg, "SelectStmt", &seljg) == YES) {
                fi->u.subquery = pg_qir_new_query(a);
                if (fi->u.subquery) pg_parse_select_stmt(&seljg, a, fi->u.subquery);
            }
        }

        JsonGetter ajg = {0};
        if (jsget_object(&ssjg, "alias", &ajg) == YES) {
            fi->alias.name = pg_parse_alias_name(&ajg, a);
        }

        return ptrvec_push(froms, fi);
    }

    q->has_unsupported = true;
    return OK;
}

/* Parses a SELECT statement object into QirQuery.
 * Ownership: all nodes/arrays are arena-owned.
 * Side effects: sets query flags and fills lists.
 * Returns OK/ERR on allocation failure. */
static int pg_parse_select_stmt(const JsonGetter *jg, PlArena *a, QirQuery *q) {
    if (!jg || !a || !q) return ERR;

    // Flags
    if (jsget_exists_nonnull(jg, "distinctClause") == YES) q->has_distinct = true;
    if (jsget_exists_nonnull(jg, "groupClause") == YES) q->has_group_by = true;
    if (jsget_exists_nonnull(jg, "havingClause") == YES) q->has_having = true;
    if (jsget_exists_nonnull(jg, "sortClause") == YES) q->has_order_by = true;
    if (jsget_exists_nonnull(jg, "limitOffset") == YES) q->has_offset = true;
    if (jsget_exists_nonnull(jg, "withClause") == YES) q->has_subquery = true;

    // targetList
    PtrVec sels = {0};
    JsonArrIter it = {0};
    if (jsget_array_objects_begin(jg, "targetList", &it) == YES) {
        JsonGetter elem = {0};
        int rc = 0;
        while ((rc = jsget_array_objects_next(jg, &it, &elem)) == YES) {
            JsonGetter rjg = {0};
            if (jsget_object(&elem, "ResTarget", &rjg) != YES) { rc = ERR; break; }

            QirSelectItem *si = (QirSelectItem *)pl_arena_alloc(a, (uint32_t)sizeof(QirSelectItem));
            if (!si) { rc = ERR; break; }

            char *tmp = NULL;
            if (jsget_string_decode_alloc(&rjg, "name", &tmp) == YES) {
                si->out_alias.name = pg_arena_transfer(a, tmp);
            } else {
                si->out_alias.name = (char *)pl_arena_add(a, (void *)"", 0);
            }

            JsonGetter vjg = {0};
            if (jsget_object(&rjg, "val", &vjg) != YES) { rc = ERR; break; }
            QirExpr *expr = pg_parse_expr(&vjg, a, q);
            if (!expr || expr->kind != QIR_EXPR_COLREF) {
                q->has_unsupported = true;
                si->value.qualifier.name = (char *)pl_arena_add(a, (void *)"", 0);
                si->value.column.name = (char *)pl_arena_add(a, (void *)"", 0);
            } else {
                si->value = expr->u.colref;
            }

            if (ptrvec_push(&sels, si) != OK) { rc = ERR; break; }
        }
        if (rc == ERR) q->has_unsupported = true;
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
        if (rc == ERR) q->has_unsupported = true;
    }

    q->from_items = (QirFromItem **)ptrvec_flatten(&froms, a);
    q->nfrom = froms.len;
    ptrvec_clean(&froms);

    q->joins = (QirJoin **)ptrvec_flatten(&joins, a);
    q->njoins = joins.len;
    ptrvec_clean(&joins);

    // WHERE
    JsonGetter wjg = {0};
    if (jsget_object(jg, "whereClause", &wjg) == YES) {
        q->where = pg_parse_expr(&wjg, a, q);
    }

    // CTEs
    JsonGetter wcjg = {0};
    if (jsget_object(jg, "withClause", &wcjg) == YES) {
            PtrVec ctes = {0};
            if (jsget_array_objects_begin(&wcjg, "ctes", &it) == YES) {
                JsonGetter elem = {0};
                int rc = 0;
                while ((rc = jsget_array_objects_next(&wcjg, &it, &elem)) == YES) {
                    JsonGetter ctejg = {0};
                    if (jsget_object(&elem, "CommonTableExpr", &ctejg) != YES) { rc = ERR; break; }

                    QirCte *cte = pl_arena_alloc(a, (uint32_t)sizeof(QirCte));
                    if (!cte) { rc = ERR; break; }

                    char *tmp = NULL;
                    if (jsget_string_decode_alloc(&ctejg, "ctename", &tmp) == YES) {
                        cte->name.name = pg_arena_transfer(a, tmp);
                    }

                    JsonGetter cqjg = {0};
                    if (jsget_object(&ctejg, "ctequery", &cqjg) == YES) {
                        JsonGetter seljg = {0};
                        if (jsget_object(&cqjg, "SelectStmt", &seljg) == YES) {
                            cte->query = pg_qir_new_query(a);
                            if (cte->query) pg_parse_select_stmt(&seljg, a, cte->query);
                        }
                    }

                    if (ptrvec_push(&ctes, cte) != OK) { rc = ERR; break; }
                }
                if (rc == ERR) q->has_unsupported = true;
            }
            q->ctes = (QirCte **)ptrvec_flatten(&ctes, a);
            q->nctes = ctes.len;
            ptrvec_clean(&ctes);
    }

    // LIMIT
    JsonGetter lcjg = {0};
    if (jsget_object(jg, "limitCount", &lcjg) == YES) {
        JsonGetter acjg = {0};
        if (jsget_object(&lcjg, "A_Const", &acjg) == YES) {
            QirExpr *lit = pg_parse_literal(&acjg, a, q);
            if (lit && lit->kind == QIR_EXPR_LITERAL && lit->u.lit.kind == QIR_LIT_INT64) {
                if (lit->u.lit.v.i64 >= 0 && lit->u.lit.v.i64 <= INT32_MAX) {
                    q->limit_value = (int32_t)lit->u.lit.v.i64;
                } else {
                    q->has_unsupported = true;
                }
            } else {
                q->has_unsupported = true;
            }
        } else {
            q->has_unsupported = true;
        }
    }

    return OK;
}

/* Creates a QueryIR for a SQL string using libpg_query JSON AST.
 * Ownership: out handle owns all allocations; caller must destroy it.
 * Side effects: allocates arena memory.
 * Returns OK on success (including parse/unsupported), ERR on allocation failure. */
static int pg_make_query_ir(DbBackend *db, const char *sql, QirQueryHandle *out) {
    (void)db;
    if (!sql || !out) return ERR;

    if (qir_handle_init(out) != OK) return ERR;
    QirQuery *q = out->q;

    PgQueryParseResult res = pg_query_parse(sql);
    if (res.error) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    if (!res.parse_tree) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    JsonGetter root = {0};
    if (jsget_init(&root, res.parse_tree, strlen(res.parse_tree)) != OK) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    JsonArrIter it = {0};
    if (jsget_array_objects_begin(&root, "stmts", &it) != YES) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    JsonGetter stmt = {0};
    if (jsget_array_objects_next(&root, &it, &stmt) != YES) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    // multiple statements are a parse error
    if (jsget_array_objects_next(&root, &it, &stmt) == YES) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    JsonGetter raw = {0};
    if (jsget_object(&stmt, "RawStmt", &raw) == YES) {
        stmt = raw;
    }

    JsonGetter stg = {0};
    if (jsget_object(&stmt, "stmt", &stg) != YES) {
        q->status = QIR_PARSE_ERROR;
        pg_query_free_parse_result(res);
        return OK;
    }

    JsonGetter seljg = {0};
    if (jsget_object(&stg, "SelectStmt", &seljg) == YES) {
        if (pg_parse_select_stmt(&seljg, &out->arena, q) != OK) {
            qir_handle_destroy(out);
            pg_query_free_parse_result(res);
            return ERR;
        }
    } else {
        q->status = QIR_UNSUPPORTED;
    }

    if (q->status == QIR_OK && q->has_unsupported) {
        q->status = QIR_UNSUPPORTED;
    }

    pg_query_free_parse_result(res);
    return OK;
}

/* Copyies 'msg' inside the last_err of 'p'. 'msg' may be NULL. */
static void pg_set_err(PgImpl *p, const char *msg) {
    if (!p) return;
    if (!msg) msg = "unknown error";
    snprintf(p->last_err, sizeof(p->last_err), "%s", msg);
}

/* Copyies 'prefix' + the last error that happened at 'conn' to 'p'. */
static void pg_set_err_pg(PgImpl *p, PGconn *conn, const char *prefix) {
    const char *e = conn ? PQerrorMessage(conn) : "no connection";
    if (!prefix) prefix = "postgres error";
    snprintf(p->last_err, sizeof(p->last_err), "%s: %s", prefix, e ? e : "");
}

/* Executes one or more SQL commands (separated by ';') and requires COMMAND_OK.
 * Use this to send sql statements that don't return tuples. Returns ERR on bad
 * input or if the query produced an error. Stores error inside 'p'. */
static int pg_exec_command(PgImpl *p, const char *sql) {
    if (!p || !sql) return ERR;

    if (!p->conn) {
        pg_set_err_pg(p, p->conn, NULL);
        return ERR;
    }

    PGresult *res = PQexec(p->conn, sql);
    if (!res) {
        pg_set_err_pg(p, p->conn, "PQexec failed");
        return ERR;
    }
    

    ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK) {
        // Could be error, or could be tuples.
        // caller should use pg_exec() for tuples
        pg_set_err_pg(p, p->conn, sql);
        PQclear(res);
        return ERR;
    }

    PQclear(res);
    return OK;
}

/* Executes one or more SQL commands (separated by ';') ignoring their errors
 * if any is returned. */
static void pg_exec_command_ignore(PgImpl *p, const char *sql) {
    if (!p || !p->conn) return;
    PGresult *res = PQexec(p->conn, sql);
    if (res) PQclear(res);
}

/* Best-effort rollback, ignore errors. */
static void pg_rollback(PgImpl *p) {
    pg_exec_command(p, "ROLLBACK");
}

/* Executes commands so the current session of 'p' complies with 'p->policy'.
 * Must be called before running any query and the caller must checks this 
 * returned one before sending any query. Stores error inside 'p'. */
static int pg_apply_policy(PgImpl *p) {
    if (!p || !p->conn) return ERR;
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
        if (pg_exec_command(p, buf) != OK) return ERR;
    }
    
    p->policy_applied = 1;
    return OK;
}

/* Executes 'sql' and returns the result inside 'out_res'. It verify the result
 * is just one. If there are more results it doesn't store anything and returns
 * ERR (single statement policy). */
static int pg_exec_single_result(PgImpl *p, const char *sql, PGresult **out_res) {
    if (!p || !p->conn || !sql || !out_res) return ERR;
    *out_res = NULL;

    if (PQsendQuery(p->conn, sql) != 1) {
        pg_set_err_pg(p, p->conn, "PQsendQuery failed");
        return ERR;
    }

    PGresult *first = NULL;
    PGresult *extra = NULL;

    for (;;) {
        PGresult *res = PQgetResult(p->conn);
        if (!res) break;

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
        pg_set_err(p, "multiple statements/results are not allowed");
        PQclear(extra);
        if (first) PQclear(first);
        return ERR;
    }

    if (!first) {
        pg_set_err(p, "no result returned");
        return ERR;
    }

    *out_res = first;
    return OK;
}

/* --------------------------- DbBackend vtable --------------------------- */

static int pg_connect(DbBackend *db, const ConnProfile *profile,
                        const SafetyPolicy *policy, const char *pwd) {
    if (!db || !db->impl || !profile || !policy) return ERR;
    PgImpl *p = (PgImpl *)db->impl;

    // when created, NULL is assigned to p->conn. If it's not NULL, there's
    // another open connection
    if (p->conn) {
        PQfinish(p->conn);
        p->conn = NULL;
    }

    const char *port_str = NULL;
    char portbuf[16];
    if (profile->port > 0) {
        snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)profile->port);
        port_str = portbuf;
    }

    const char *keys[] = {
        "host", "port", "dbname", "user", "password", "options", NULL
    };
    const char *vals[] = {
        profile->host,
        port_str,
        profile->db_name,
        profile->user,
        pwd,
        profile->options,
        NULL
    };

    p->conn = PQconnectdbParams(keys, vals, 0);
    if (!p->conn) {
        pg_set_err(p, "PQconnectdb returned NULL");
        return ERR;
    }

    if (PQstatus(p->conn) != CONNECTION_OK) {
        pg_set_err_pg(p, p->conn, "connection failed");
        PQfinish(p->conn);
        p->conn = NULL;
        return ERR;
    }
    
    p->policy = *policy;
    p->policy_applied = 0;
    return OK;
}

static int pg_is_connected(DbBackend *db) {
    if (!db || !db->impl) return ERR;
    PgImpl *p = (PgImpl *)db->impl;
    if (!p->conn) return NO;
    return (PQstatus(p->conn) == CONNECTION_OK) ? YES : NO;
}

static void pg_disconnect(DbBackend *db) {
    if (!db || !db->impl) return;
    PgImpl *p = (PgImpl *)db->impl;
    if (p->conn) {
        PQfinish(p->conn);
        p->conn = NULL;
    }
    p->policy_applied = 0;
}

static void pg_destroy(DbBackend *db) {
    if (!db || !db->impl) return;
    pg_disconnect(db);
    PgImpl *p = (PgImpl *)db->impl;
    free(p);
    free(db);
}

static int pg_exec(DbBackend *db, const McpId *request_id, const char *sql,
                        QueryResult **out_qr) {
    
    const char *err_msg;
    QueryResult *qr = NULL;
    PGresult *res = NULL;

    // Error logging logic, if we called a function that sets the error like
    // pg_exec_command(), we use that error... else, we create the message.

    if (!db || !db->impl || !sql || !out_qr) {
        err_msg = "unexpected input before executing the query";
        goto fail_bad_input;
    };
    *out_qr = NULL;

    PgImpl *p = (PgImpl *)db->impl;
    if (!p->conn) {
        pg_set_err(p, "not connected");
        goto fail;
    }

    // even if this limit is version-dependent, it's a defensive check
    if (strlen(sql) > PG_QUERY_MAX_BYTES) {
        pg_set_err(p, "SQL exceeds 8192 bytes (libpq query buffer limit)");
        goto fail;
    }
    
    // apply safety policy
    if (!(p->policy_applied)) {
        if (pg_apply_policy(p) != OK) {
            goto fail;
        }
    }

    // start counting for query execution time
    uint64_t t0 = now_ms_monotonic();

    // start a read-only transaction for every query
    if (p->policy.read_only) {
        if (pg_exec_command(p, "BEGIN READ ONLY") != OK) {
            goto fail;
        }
    } else {
        if (pg_exec_command(p, "BEGIN") != OK) {
            goto fail;
        }
    }

    if (pg_exec_single_result(p, sql, &res) != OK) {
        goto fail;
    }

    ExecStatusType st = PQresultStatus(res);

    // if backend error, rollback and return error
    if (st == PGRES_FATAL_ERROR || st == PGRES_BAD_RESPONSE || st == PGRES_NONFATAL_ERROR) {
        // capture error string
        const char *msg = PQresultErrorMessage(res);
        if (!msg || !*msg) msg = PQerrorMessage(p->conn);

        pg_set_err(p, msg ? msg : "query failed");

        goto fail;
    }

    // Right now, the agent can't send commands like set, delete...
    // so the status should be PGRES_TUPLES_OK.
    // TODO: allow agent to run commands if permitted by user
    if (st == PGRES_TUPLES_OK) {
        int ncols = PQnfields(res);
        int ntuples = PQntuples(res);

        if (ncols < 0) ncols = 0;
        if (ntuples < 0) ntuples = 0;

        uint32_t out_cols = (uint32_t)ncols;
        uint32_t out_rows = (uint32_t)ntuples;

        uint8_t result_truncated = 0;
        if (p->policy.max_rows > 0 && out_rows > p->policy.max_rows) {
            out_rows = p->policy.max_rows;
            result_truncated = 1;
        }

        qr = qr_create_ok(request_id, (uint32_t)ncols, out_rows,
                          result_truncated, p->policy.max_query_bytes);
        if (!qr) {
            pg_set_err(p, "qr_create_ok error");
            goto fail;
        }

        // Column metadata
        for (uint32_t c = 0; c < out_cols; c++) {
            const char *name = PQfname(res, c);
            // Store empty strings if metadata missing
            if (!name) name = "";

            Oid oid = PQftype(res, c);
            char typebuf[32];
            // materialize Oid to a textual representation
            snprintf(typebuf, sizeof(typebuf), "%u", (unsigned)oid);

            if (qr_set_col(qr, c, name, typebuf) < 0) {
                pg_set_err(p, "qr_set_col failed");
                goto fail;
            }
        }

        // Fill cells (enforces max_query_bytes by stopping when the cap is hit).
        int stop = 0;
        for (uint32_t r = 0; r < out_rows; r++) {
            for (uint32_t c = 0; c < (uint32_t)ncols; c++) {
                
                char *val;
                if (PQgetisnull(res, r, c)) val = NULL;
                else val = PQgetvalue(res, (int)r, (int)c);

                int src = qr_set_cell(qr, r, c, val);
                if (src == NO) {
                    qr->result_truncated = 1;
                    qr->nrows = r;
                    stop = 1;
                    break;
                }
                if (src == ERR) {
                    pg_set_err(p, "qr_set_cell failed");
                    goto fail;
                }
            }
            if (stop) break;
        }
    } else {
        // Error status
        const char *msg = PQresStatus(st);
        pg_set_err(p, msg ? msg : "unexpected PGresult status");
        goto fail;
    }

    PQclear(res);
    res = NULL;

    // commit transaction
    if (pg_exec_command(p, "COMMIT") != OK) {
        // If commit fails, try rollback
        pg_rollback(p);
        pg_set_err(p, "COMMIT failure");
        goto fail;
    }

    uint64_t t1 = now_ms_monotonic();
    qr->exec_ms = (t1 >= t0) ? (t1 - t0) : 0;

    *out_qr = qr;
    return (*out_qr ? OK : ERR);

fail:
    if (!out_qr) return ERR; // catastrophic

    err_msg = p->last_err;
    TLOG("ERROR - pg_exec failed: %s", err_msg ? err_msg : "unknown");
    // rollback is safe even if we haven't executed anything
    pg_rollback(p);
    if (res) PQclear(res);
    if (qr) qr_destroy(qr);
fail_bad_input:
    // if bad input, we can't rely on the buffer for the error of PgImpl
    *out_qr = qr_create_err(request_id, err_msg);
    return (*out_qr ? OK : ERR);
}

/* ------------------------- constructor ------------------------- */

static const DbBackendVTable PG_VT = {
    .connect = pg_connect,
    .is_connected = pg_is_connected,
    .disconnect = pg_disconnect,
    .destroy = pg_destroy,
    .exec = pg_exec,
    .make_query_ir = pg_make_query_ir
};

DbBackend *postgres_backend_create(void) {
    DbBackend *db = (DbBackend *)xcalloc(1, sizeof(DbBackend));

    PgImpl *impl = (PgImpl *)xcalloc(1, sizeof(PgImpl));

    impl->conn = NULL;
    impl->last_err[0] = '\0';

    db->vt = &PG_VT;
    db->impl = impl;
    return db;
}
