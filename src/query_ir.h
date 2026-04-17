#ifndef QUERY_IR_H
#define QUERY_IR_H

#include <stdbool.h>
#include <stdint.h>

#include "adbx_err.h"
#include "arena.h"
#include "string_op.h"
#include "utils.h"

// DB-agnostic IR for a restricted SQL subset.
// Built by backend-specific parsers (e.g., Postgres via libpg_query AST).
//
// This represent enough structure to enforce acceptance policy + Sensitive
//    Mode rules.
// This design intentionally leavers some constructs as *_UNSUPPORTED to
//    account for db-specific constructs.
// This represent the intention of a query.

// ----------------------------
// Status / diagnostics
// ----------------------------

// set by QueryValidator
typedef enum QirStatus {
  QIR_OK = 0,
  QIR_PARSE_ERROR,
  QIR_UNSUPPORTED
} QirStatus;

typedef enum QirStmtKind {
  QIR_STMT_SELECT = 1 // only SELECT supported for now
} QirStmtKind;

// This models how is that statement wrapped/executed
typedef enum QirStmtFlags {
  QIR_STMTF_NONE = 0,
  QIR_STMTF_EXPLAIN = 1u << 0,
  QIR_STMTF_ANALYZE = 1u << 1,
} QirStmtFlags;

// Used by validators/touch-extractors to distinguish top-level query scope
// from any nested query (CTE body, subquery in FROM, scalar subquery, EXISTS,
// etc.).
typedef enum QirScope { QIR_SCOPE_MAIN = 0, QIR_SCOPE_NESTED = 1 } QirScope;

// Identifiers are stored as normalized strings by the backend parser.
// For v1, the backend must lower-case identifiers so validator matching is
// case-insensitive, even if the source SQL used quotes.
typedef struct QirIdent {
  const char *name; // never NULL; may be "" if backend couldn't recover a name.
} QirIdent;

typedef struct QirQuery QirQuery;
typedef struct QirExpr QirExpr;
typedef struct QirCaseWhen QirCaseWhen;
typedef struct QirCaseExpr QirCaseExpr;
typedef struct QirFromItem QirFromItem;
typedef struct QirCte QirCte;

// alias.column
typedef struct QirColRef {
  QirIdent qualifier;              // table alias
  QirIdent column;                 // column name
  const QirFromItem *binding_from; // bound visible range item
  uint32_t correlation_depth;      // 0=local, 1+=outer scope
} QirColRef;

// schema.table (views treated as tables)
typedef struct QirRelRef {
  QirIdent schema; // optional; name=="" if absent
  QirIdent name;   // table/view name
} QirRelRef;

// schema.type (or just type)
typedef struct QirTypeRef {
  QirIdent schema; // optional; name=="" if absent
  QirIdent name;   // type name (may include dots if backend couldn't split)
} QirTypeRef;

// ----------------------------
// Expressions
// ----------------------------

typedef enum QirExprKind {
  QIR_EXPR_COLREF = 1, // alias.column
  QIR_EXPR_PARAM,      // $n
  QIR_EXPR_LITERAL,    // backend may produce; validator may reject depending on
                       // policy
  QIR_EXPR_FUNCALL,    // f(args...)
  QIR_EXPR_CAST,       // expr::type
  QIR_EXPR_OP,         // generalized backend operator expression

  QIR_EXPR_AND, // lhs AND rhs
  QIR_EXPR_OR,
  QIR_EXPR_NOT,

  QIR_EXPR_CASE,       // CASE [arg] WHEN cond THEN expr ... [ELSE expr] END
  QIR_EXPR_WINDOWFUNC, // func(...) OVER (...)
  QIR_EXPR_SUBQUERY,   // nested SELECT value referenced from another expr
  QIR_EXPR_UNSUPPORTED // anything not modeled safely
} QirExprKind;

typedef enum QirLiteralKind {
  QIR_LIT_NULL = 0,
  QIR_LIT_BOOL,
  QIR_LIT_INT64,
  QIR_LIT_FLOAT64,
  QIR_LIT_STRING,
  QIR_LIT_UNSUPPORTED
} QirLiteralKind;

typedef struct QirLiteral {
  QirLiteralKind kind;
  union {
    bool b;
    int64_t i64;
    double f64;
    const char *s; // string literal (already unescaped by backend)
  } v;
} QirLiteral;

typedef struct QirFuncCall {
  // For v1 policy, function names are treated as unqualified identifiers.
  // Schema is optional; backends should set schema.name="" if unqualified.
  QirIdent name;
  QirIdent schema;
  QirExpr **args;
  uint32_t nargs;
  bool is_distinct;
  bool is_star;
} QirFuncCall;

typedef struct QirWindowFunc {
  QirFuncCall func;
  QirExpr **partition_by;
  uint32_t n_partition_by;
  QirExpr **order_by;
  uint32_t n_order_by;
  bool has_frame;
} QirWindowFunc;

typedef enum QirOpClass {
  QIR_OP_EQ = 1, // lhs = arg0
  QIR_OP_IN,     // lhs IN (arg0, arg1, ...)
  QIR_OP_OTHER   // any other supported backend operator
} QirOpClass;

// Generalized backend operator expression.
// The strings and child pointers are arena-owned by the enclosing
// QirQueryHandle.
typedef struct QirOpExpr {
  QirOpClass cls;
  QirExpr *lhs; // NULL only for unary operations such as EXISTS.
  QirExpr **args;
  uint32_t nargs;
  const char *op_name; // the exact backend token recovered from the AST
} QirOpExpr;

// One WHEN ... THEN ... clause inside a CASE expression.
typedef struct QirCaseWhen {
  QirExpr *when_expr;
  QirExpr *then_expr;
} QirCaseWhen;

// CASE expression with optional argument and ELSE clause.
typedef struct QirCaseExpr {
  QirExpr *arg;        // NULL for "CASE WHEN ..." form
  QirCaseWhen **whens; // ordered WHEN/THEN clauses
  uint32_t nwhens;
  QirExpr *else_expr; // NULL if ELSE is absent
} QirCaseExpr;

// Binary/logical expression storage.
// For QIR_EXPR_NOT, only bin.l is used; bin.r must be NULL.
typedef struct QirBinExpr {
  QirExpr *l;
  QirExpr *r;
} QirBinExpr;

// QirExpr is the core expression node used throughout the IR (SELECT, WHERE,
// GROUP BY, HAVING, ORDER BY, function args, etc.).
struct QirExpr {
  QirExprKind kind;
  union {
    QirColRef colref;    // QIR_EXPR_COLREF
    int param_index;     // QIR_EXPR_PARAM (n in $n), >=1
    QirLiteral lit;      // QIR_EXPR_LITERAL
    QirFuncCall funcall; // QIR_EXPR_FUNCALL
    struct {
      QirExpr *expr;
      QirTypeRef type;
    } cast;               // QIR_EXPR_CAST
    QirOpExpr op;         // QIR_EXPR_OP
    QirBinExpr bin;       // AND/OR/NOT
    QirCaseExpr case_;    // CASE
    QirWindowFunc window; // WINDOWFUNC
    QirQuery *subquery;   // QIR_EXPR_SUBQUERY
  } u;
};

// ----------------------------
// FROM items / joins
// ----------------------------

typedef enum QirFromKind {
  QIR_FROM_BASE_REL = 1, // table/view reference
  QIR_FROM_SUBQUERY,     // derived table: FROM (SELECT ...) AS alias
  QIR_FROM_VALUES,       // FROM (VALUES ...) AS alias
  QIR_FROM_UNSUPPORTED
} QirFromKind;

struct QirFromItem {
  QirFromKind kind;

  // Policy: every range item must have an alias; references must use that
  // alias.
  QirIdent alias;

  union {
    QirRelRef rel;        // BASE_REL
    QirQuery *subquery;   // SUBQUERY
    struct {              // VALUES
      QirIdent *colnames; // optional column list from AS v(x,y)
      uint32_t ncolnames;
    } values;
  } u;

  // Binder-owned metadata. When non-NULL, this BASE_REL syntactically refers
  // to a visible CTE name rather than a database relation.
  const QirCte *binding_cte;
};

// Join modeling
typedef enum QirJoinKind {
  QIR_JOIN_INNER = 1,
  QIR_JOIN_LEFT,
  QIR_JOIN_RIGHT,
  QIR_JOIN_FULL,
  QIR_JOIN_CROSS,
  QIR_JOIN_UNSUPPORTED
} QirJoinKind;

typedef struct QirJoin {
  QirJoinKind kind;
  QirFromItem *rhs;
  QirExpr *on; // NULL for CROSS; may be UNSUPPORTED if backend couldn't map
} QirJoin;

// ----------------------------
// SELECT items
// ----------------------------

// Each select item is an expression with a mandatory output alias.
typedef struct QirSelectItem {
  QirExpr *value;     // expression (arena-owned)
  QirIdent out_alias; // mandatory alias
} QirSelectItem;

// ----------------------------
// CTE
// ----------------------------

struct QirCte {
  QirIdent name;
  QirQuery *query;
};

// ----------------------------
// Query
// ----------------------------

struct QirQuery {
  QirStatus status;
  const char *status_reason; // arena-owned; NULL if unset. Indicates the
                             // reason why the status is not QIR_OK
  QirStmtKind kind;
  QirStmtFlags stmt_flags;

  // Conservative feature flags (backend sets these).
  bool has_star; // SELECT * or table.*
  bool has_distinct;
  bool has_offset;

  // CTEs
  QirCte **ctes;
  uint32_t nctes;

  // SELECT list
  QirSelectItem **select_items;
  uint32_t nselect;

  // FROM clause
  // We only allow a single FROM item in v1.
  QirFromItem *from_root;

  // JOIN (for each FROM item, we represent joins in a flat list)
  QirJoin **joins;
  uint32_t njoins;

  // WHERE
  QirExpr *where; // may be NULL

  // GROUP BY items (expressions)
  QirExpr **group_by;
  uint32_t n_group_by;

  // HAVING
  QirExpr *having; // may be NULL

  // ORDER BY items (expressions)
  QirExpr **order_by;
  uint32_t n_order_by;

  // LIMIT
  // limit_value: -1 means missing.
  int32_t limit_value;

  // Set operations (UNION ALL / INTERSECT / EXCEPT / …).
  // The lead query holds CTEs, stmt_flags, kind, limit, ORDER BY, etc.
  // Each union_next is a branch with its own body (SELECT, FROM, WHERE, ...)
  // but default metadata (nctes=0, limit_value=-1, etc.).
  QirQuery *union_next; // NULL when no set operation follows
};

// Handle that owns the arena backing a QueryIR.
typedef struct QirQueryHandle {
  Arena arena; // owns all allocations reachable from q
  QirQuery *q; // pointer inside arena
} QirQueryHandle;

/*-------------------------------- FLAG HELPERS -----------------------------*/
/* Sets one or more statement wrapper 'flags' to 'q' while preserving
 * invariants.
 */
static inline void qir_query_add_stmt_flags(QirQuery *q, uint32_t flags) {
  if (!q)
    return;
  q->stmt_flags = (QirStmtFlags)(((uint32_t)q->stmt_flags) | flags);
}

/* Returns 1 when the query is wrapped by EXPLAIN or EXPLAIN ANALYZE.
 */
static inline int qir_query_is_explain(const QirQuery *q) {
  return q && ((((uint32_t)q->stmt_flags) & (uint32_t)QIR_STMTF_EXPLAIN) != 0);
}

/* Returns 1 when the query is wrapped by EXPLAIN ANALYZE.
 */
static inline int qir_query_is_explain_analyze(const QirQuery *q) {
  return q && ((((uint32_t)q->stmt_flags) & (uint32_t)QIR_STMTF_ANALYZE) != 0);
}
/*---------------------------------------------------------------------------*/

// ----------------------------
// Touch walking
// ----------------------------

typedef enum QirTouchKind {
  QIR_TOUCH_BASE = 1, // qualifier resolves to BASE_REL alias
  QIR_TOUCH_DERIVED,  // qualifier resolves to SUBQUERY, bound CTE, or VALUES
  QIR_TOUCH_UNKNOWN   // qualifier is still unbound or cannot be mapped to one
                      // direct column reference (for example alias.*)
} QirTouchKind;

typedef AdbxStatus (*QirTouchFn)(QirScope scope, const QirQuery *owner_query,
                                 const QirColRef *colref, QirTouchKind kind,
                                 void *ctx);

// ----------------------------
// Memory / ownership
// ----------------------------
//
// Backends allocate QirQuery/QirExpr/etc. inside the arena owned by
// QirQueryHandle. Validators should treat all pointers as read-only.

// Initializes a QirQueryHandle and allocates a blank QirQuery inside it.
// Returns OK on success, ERR on bad input or allocation failure.
AdbxStatus qir_handle_init(QirQueryHandle *h);

// Frees the arena owned by the handle and resets it.
void qir_handle_destroy(QirQueryHandle *h);

typedef enum QirBindErrCode {
  QIR_BINDERR_NONE = 0,
  QIR_BINDERR_INPUT,
  QIR_BINDERR_UNSUPPORTED,
  QIR_BINDERR_UNRESOLVED_COLREF,
  QIR_BINDERR_UNRESOLVED_CTE,
  QIR_BINDERR_AMBIGUOUS_COLREF,
  QIR_BINDERR_AMBIGUOUS_CTE,
} QirBindErrCode;

typedef struct QirBindErr {
  QirBindErrCode code;
  char msg[ADBX_ERRMSG_MAX];
} QirBindErr;

/* Binds column references and FROM/JOIN base relations against visible query
 * scopes and CTE names. It annotates the IR in-place and performs no heap
 * allocations. Returns YES on success, NO on unresolved/ambiguous bindings,
 * and ERR on invalid input. */
AdbxTriStatus bind_query_ir(QirQuery *q, QirBindErr *out_err);

/* Walks every column reference reachable from 'q' in stable depth-first order.
 * The walker expects bind_query_ir() to have populated bound metadata in
 * QirColRef/QirFromItem. It visits colrefs from the lead query, nested queries,
 * and set-op branches, passing MAIN for the lead query chain and NESTED for any
 * nested query block. Unsupported expressions are skipped here because callers
 * should enforce structural acceptance separately. Returns OK on success, ERR on
 * invalid input or when the callback returns ERR. */
AdbxStatus qir_walk_touches(const QirQuery *q, QirTouchFn fn, void *ctx);

/* Renders a FROM item into 'out' and returns out->data (or "" on error).
 * Returned string is NUL-term. Ownership: caller owns 'out' and controls its
 * lifetime. Side effects: resets and writes into 'out'. */
const char *qir_from_to_str(const QirFromItem *fi, StrBuf *out);

/* Renders a column reference into 'out' and returns out->data (or "" on error).
 * Returned string is NUL-term. Ownership: caller owns 'out' and controls its
 * lifetime. Side effects: resets and writes into 'out'. */
const char *qir_colref_to_str(const QirColRef *cr, StrBuf *out);

/* Renders a function call into 'out' and returns out->data (or "" on error).
 * Returned string is NUL-term. Ownership: caller owns 'out' and controls its
 * lifetime. Side effects: resets and writes into 'out'. */
const char *qir_func_to_str(const QirFuncCall *fn, StrBuf *out);

/* Sets query status and (optional) reason once; first status wins.
 * Ownership: copies reason into arena when provided.
 * Side effects: mutates q->status and q->status_reason.
 * Error semantics: no return value; on invalid input it is a no-op. */
void qir_set_status(QirQuery *q, Arena *arena, QirStatus status,
                    const char *reason);

/* Resolves ORDER BY alias references to SELECT item expressions.
 * Ownership: returned pointer is owned by the QueryIR arena.
 * Side effects: may mark QIR_UNSUPPORTED on ambiguous aliases.
 * Returns the resolved expression or the original expression if no match. */
QirExpr *qir_resolve_order_alias(QirQuery *q, Arena *arena, QirExpr *expr);

#endif // QUERY_IR_H
