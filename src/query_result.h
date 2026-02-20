#ifndef QUERY_RESULT_H
#define QUERY_RESULT_H

#include <stddef.h>
#include <stdint.h>

#include "mcp_id.h"

typedef struct ValidatorPlan ValidatorPlan;
typedef struct DbTokenStore DbTokenStore;

typedef enum QRColValueType {
  QRCOL_V_PLAINTEXT = 0,
  QRCOL_V_TOKEN = 1,
} QRColType;

typedef struct QRColumn {
  char *name;
  char *type; // type name in text format like "int4", "text", "date"
  QRColType value_type;
  uint32_t pg_oid; // db-specific metadata used for tokenized binds
} QRColumn;

typedef enum QRStatus { QR_OK = 0, QR_ERROR = 1, QR_TOOL_ERROR = 2 } QRStatus;

// indicates errors that may be encountered at the protol layer
typedef enum {
  QRERR_INTERNAL = -32603, // unexpected condition encountered
  QRERR_INPARAM = -32602,  // invalid or malformed parameters
  QRERR_INREQ = -32600,    // invalid request object
  QRERR_INMETHOD = -32601, // invalid/not found method
  QRERR_PARSER = -32700,   // invalid JSON was received

  QRERR_RESOURCE = -30001, // resource (e.g. ConnectionName) not found
} QrErrorCode;

/* It's a materialized, DB-agnostic query result. It owns cols and cells. */
typedef struct QueryResult {
  McpId id; // id of the request
  QRStatus status;
  uint64_t exec_ms; // execution time in ms for both OK and ERROR

  union {
    // valid if QR_OK
    struct {
      uint32_t ncols;
      QRColumn *cols; // malloc'd array of ncols length

      uint32_t nrows;
      uint32_t nrows_alloc;      // allocated rows for cells storage
      char **cells;              // length (nrows_alloc * ncols). To access an
                                 // element: cells[row*ncols + col];
      uint8_t result_truncated;  // 1 if output row count is lower than the
                                 // row count of the query executed
      uint64_t max_query_bytes;  // 0 = unlimited
      uint64_t used_query_bytes; // bytes stored across all non-NULL cells
    };

    // valid if QR_ERROR or QR_TOOL_ERROR
    struct {
      char *err_msg;
      QrErrorCode err_code; // only meaningful for QR_ERROR
    };
  };

} QueryResult;

/* Builder context used while populating one QueryResult.
 * Ownership:
 * - qr is borrowed and mutated.
 * - plan is borrowed and read-only.
 * - store is borrowed and may be mutated when token values are created.
 */
typedef struct QueryResultBuilder {
  QueryResult *qr;
  const ValidatorPlan *plan;
  DbTokenStore *store;
  uint32_t generation;
} QueryResultBuilder;

/* Initializes one QueryResultBuilder context.
 * It borrows all inputs; ownership stays with caller.
 * Returns OK on success, ERR on invalid input.
 */
int qb_init(QueryResultBuilder *qb, QueryResult *qr, const ValidatorPlan *plan,
            DbTokenStore *store, uint32_t generation);

/* Copies one column metadata entry into qb->qr at position 'col'.
 * If type is NULL it's stored as "unknown".
 * Side effects: mutates qb->qr columns and sets sensitive metadata from plan.
 * Returns OK on success, ERR on bad input or out-of-bounds.
 */
int qb_set_col(QueryResultBuilder *qb, uint32_t col, const char *name,
               const char *type, uint32_t pg_oid);

/* Stores one cell value in qb->qr at [row,col].
 * If the output column is sensitive, this function stores a token string
 * instead of plaintext and updates qb->store.
 * If value is NULL, stores SQL NULL.
 *
 * Returns:
 *  YES -> success
 *  NO  -> max_query_bytes reached; caller must stop populating rows
 *  ERR -> bad input or out-of-bounds
 */
int qb_set_cell(QueryResultBuilder *qb, uint32_t row, uint32_t col,
                const char *value, size_t v_len);

/* Creates a QueryResult with allocated storage for cells (all NULL).
 * If 'id' is non-NULL, makes an internal copy (string ids are duplicated).
 * If 'id' is NULL, the id field is zeroed; caller can set it later.
 * Returns NULL on allocation failure. */
QueryResult *qr_create_ok(const McpId *id, uint32_t ncols, uint32_t nrows,
                          uint8_t result_truncated, uint64_t max_query_bytes);

/* Creates a QueryResult that represents a protocol error (JSON-RPC error).
 * If 'id' is NULL, the id field is zeroed. Returns NULL on failure. */
QueryResult *qr_create_err(const McpId *id, QrErrorCode code,
                           const char *err_msg);

/* Creates a QueryResult that represents a tool execution error.
 * Serialized as a successful JSON-RPC result with isError=true.
 * If 'id' is NULL, the id field is zeroed. Returns NULL on failure. */
QueryResult *qr_create_tool_err(const McpId *id, const char *err_msg);

/* Creates a QueryResult with a single text column named "message" and one row.
 * If 'id' is NULL, the id field is zeroed. If msg is NULL, stores an empty
 * string. Returns NULL on failure. */
QueryResult *qr_create_msg(const McpId *id, const char *msg);

/* Frees all owned memory, 'qr' itself too. */
void qr_destroy(QueryResult *qr);

/* Returns the QRColumn at 'col' inside 'qr'. Returns NULL on bad input or if
 * that column is unset. */
const QRColumn *qr_get_col(const QueryResult *qr, uint32_t col);

/* Returns pointer to cell string (owned by qr) or NULL if SQL NULL/out of
 * range. */
const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col);

/* Returns YES if cell is SQL NULL, NO if non-NULL, ERR on error. */
int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col);

#endif
