#ifndef QUERY_IR_H
#define QUERY_IR_H

#include <stdbool.h>
#include <stdint.h>

#include "pl_arena.h"
#include "string_op.h"

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

// alias.column
typedef struct QirColRef {
  QirIdent qualifier; // table alias
  QirIdent column;    // column name
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

typedef struct QirQuery QirQuery;
typedef struct QirExpr QirExpr;
typedef struct QirCaseWhen QirCaseWhen;
typedef struct QirCaseExpr QirCaseExpr;

typedef enum QirExprKind {
  QIR_EXPR_COLREF = 1, // alias.column
  QIR_EXPR_PARAM,      // $n
  QIR_EXPR_LITERAL,    // backend may produce; validator may reject depending on
                       // policy
  QIR_EXPR_FUNCALL,    // f(args...)
  QIR_EXPR_CAST,       // expr::type

  QIR_EXPR_EQ, // lhs = rhs
  QIR_EXPR_NE, // lhs != rhs
  QIR_EXPR_GT,
  QIR_EXPR_GE,
  QIR_EXPR_LT,
  QIR_EXPR_LE,
  QIR_EXPR_LIKE,     // lhs LIKE rhs
  QIR_EXPR_NOT_LIKE, // lhs NOT LIKE rhs

  QIR_EXPR_IN, // lhs IN (item, item, ...)

  QIR_EXPR_AND, // lhs AND rhs
  QIR_EXPR_OR,
  QIR_EXPR_NOT,

  QIR_EXPR_CASE,       // CASE [arg] WHEN cond THEN expr ... [ELSE expr] END
  QIR_EXPR_WINDOWFUNC, // func(...) OVER (...)
  QIR_EXPR_SUBQUERY,   // scalar subquery, EXISTS, IN (SELECT...), etc.
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

// IN(...). Note that IN(SELECT...) is modelled setting items[0] to a QirExpr
// representing a QirQuery
typedef struct QirInExpr {
  QirExpr *lhs;
  QirExpr **items; // items inside IN(...)
  uint32_t nitems;
} QirInExpr;

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

// Example: 'l' = 'r'
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
    QirBinExpr bin;       // EQ, AND
    QirInExpr in_;        // IN
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
  QIR_FROM_CTE_REF,      // FROM cte_name AS alias
  QIR_FROM_VALUES,       // FROM (VALUES ...) AS alias
  QIR_FROM_UNSUPPORTED
} QirFromKind;

typedef struct QirFromItem {
  QirFromKind kind;

  // Policy: every range item must have an alias; references must use that
  // alias.
  QirIdent alias;

  union {
    QirRelRef rel;        // BASE_REL
    QirQuery *subquery;   // SUBQUERY
    QirIdent cte_name;    // CTE_REF
    struct {              // VALUES
      QirIdent *colnames; // optional column list from AS v(x,y)
      uint32_t ncolnames;
    } values;
  } u;
} QirFromItem;

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

typedef struct QirCte {
  QirIdent name;
  QirQuery *query;
} QirCte;

// ----------------------------
// Query
// ----------------------------

struct QirQuery {
  QirStatus status;
  const char *status_reason; // arena-owned; NULL if unset. Indicates the
                             // reason why the status is not QIR_OK
  QirStmtKind kind;

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
  // TODO: this should be a single FROM
  QirFromItem **from_items;
  uint32_t nfrom;

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
};

// Handle that owns the arena backing a QueryIR.
typedef struct QirQueryHandle {
  PlArena arena; // owns all allocations reachable from q
  QirQuery *q;   // pointer inside arena
} QirQueryHandle;

// ----------------------------
// Touch extraction results
// ----------------------------

typedef enum QirTouchKind {
  QIR_TOUCH_BASE = 1, // qualifier resolves to BASE_REL alias
  QIR_TOUCH_DERIVED,  // qualifier resolves to SUBQUERY or CTE_REF alias
  QIR_TOUCH_UNKNOWN   // qualifier didn't resolve (policy violation) or
                      // unsupported
} QirTouchKind;

typedef struct QirTouch {
  QirScope scope;               // where the qualifier.column is being used
  QirTouchKind kind;            // what the qualifier is
  QirColRef col;                // qualifier.column as written
  const QirQuery *source_query; // query block that owns this touch
} QirTouch;

// A minimal touch report. Extractor should include touches from:
// - select list
// - where
// - join ON expressions (if joins allowed globally)
// - recursively into nested queries (scope=NESTED)
typedef struct QirTouchReport {
  PlArena arena; // owns touch nodes and arrays
  QirTouch **touches;
  uint32_t ntouches;

  // Convenience flags
  bool has_unknown_touches; // true if any touch.kind == UNKNOWN
  bool has_unsupported;     // true if unsupported exprs encountered
} QirTouchReport;

// ----------------------------
// Memory / ownership
// ----------------------------
//
// Backends allocate QirQuery/QirExpr/etc. inside the arena owned by
// QirQueryHandle. Validators should treat all pointers as read-only.

// Initializes a QirQueryHandle and allocates a blank QirQuery inside it.
// Returns OK on success, ERR on bad input or allocation failure.
int qir_handle_init(QirQueryHandle *h);

// Frees the arena owned by the handle and resets it.
void qir_handle_destroy(QirQueryHandle *h);

// Frees a touch report allocated by qir_extract_touches().
void qir_touch_report_destroy(QirTouchReport *tr);

/* Extract touches from a QueryIR.
 * The QirTouchReport checks that all the columns are referenced in the form
 * 'alias.column'.
 * It also checks that all the alias referenced are present in the QirQuery.
 * If one of these 2 conditions is false, the specific QirTouch that references
 * the column is set to QIR_TOUCH_UNKNOWN.
 * It only handles column references, it doesn't check wheter all the subqueries
 * or CTEs have an alias.
 *
 * - scope is MAIN for the top-level query and NESTED for any nested query.
 * - The extractor is conservative: if it sees unsupported expressions or cannot
 *   resolve a qualifier, it marks UNKNOWN touches and has_unsupported. */
QirTouchReport *qir_extract_touches(const QirQuery *q);

/* Renders a FROM item into 'out' and returns out->data (or "" on error).
 * Ownership: caller owns 'out' and controls its lifetime.
 * Side effects: resets and writes into 'out'. */
const char *qir_from_to_str(const QirFromItem *fi, StrBuf *out);

/* Renders a column reference into 'out' and returns out->data (or "" on error).
 * Ownership: caller owns 'out' and controls its lifetime.
 * Side effects: resets and writes into 'out'. */
const char *qir_colref_to_str(const QirColRef *cr, StrBuf *out);

/* Renders a function call into 'out' and returns out->data (or "" on error).
 * Ownership: caller owns 'out' and controls its lifetime.
 * Side effects: resets and writes into 'out'. */
const char *qir_func_to_str(const QirFuncCall *fn, StrBuf *out);

/* Sets query status and (optional) reason once; first status wins.
 * Ownership: copies reason into arena when provided.
 * Side effects: mutates q->status and q->status_reason.
 * Error semantics: no return value; on invalid input it is a no-op. */
void qir_set_status(QirQuery *q, PlArena *arena, QirStatus status,
                    const char *reason);

/* Resolves ORDER BY alias references to SELECT item expressions.
 * Ownership: returned pointer is owned by the QueryIR arena.
 * Side effects: may mark QIR_UNSUPPORTED on ambiguous aliases.
 * Returns the resolved expression or the original expression if no match. */
QirExpr *qir_resolve_order_alias(QirQuery *q, PlArena *arena, QirExpr *expr);

#endif // QUERY_IR_H
