#ifndef QUERY_RESULT_H
#define QUERY_RESULT_H

#include <stdint.h>
#include <stddef.h>

#include "mcp_id.h"

typedef struct QRColumn {
    char *name;
    char *type;     // type name in text format like "int4", "text", "date"
} QRColumn;

typedef enum QRStatus {
    QR_OK = 0,
    QR_ERROR = 1
} QRStatus;

/* It's a materialized, DB-agnostic query result. It owns cols and cells. */
typedef struct QueryResult {
    McpId id;           // id of the request
    QRStatus status;
    uint64_t exec_ms;   // execution time in ms for both OK and ERROR

    union {
        // valid if QR_OK
        struct {
            uint32_t ncols;
            QRColumn *cols;     // malloc'd array of ncols length

            uint32_t nrows;
            uint32_t nrows_alloc; // allocated rows for cells storage
            char **cells;       // length (nrows_alloc * ncols). To access an
                                // element: cells[row*ncols + col];
            uint8_t result_truncated;  // 1 if output row count is lower than the
                                       // row count of the query executed
            uint64_t max_query_bytes;  // 0 = unlimited
            uint64_t used_query_bytes; // bytes stored across all non-NULL cells
        };

        // valid if QR_ERROR
        char *err_msg;
    };

    // valid if QR_ERROR

} QueryResult;

/* Creates a QueryResult with allocated storage for cells (all NULL).
 * Ownership: makes an internal copy of 'id' (string ids are duplicated).
 * Error semantics: returns NULL on allocation failure or invalid id. */
QueryResult *qr_create_ok(const McpId *id, uint32_t ncols, uint32_t nrows,
                          uint8_t result_truncated, uint64_t max_query_bytes);

/* Creates a QueryResult that represents an error. malloc 'err_msg'.
 * Ownership: makes an internal copy of 'id'. Returns NULL on failure. */
QueryResult *qr_create_err(const McpId *id, const char *err_msg);

/* Creates a QueryResult with a single text column named "message" and one row.
 * If msg is NULL, stores an empty string. Returns NULL on failure. */
QueryResult *qr_create_msg(const McpId *id, const char *msg);

/* Frees all owned memory, 'qr' itself too. */
void qr_destroy(QueryResult *qr);

/* Copies 'name' and 'type' inside the column metadata of 'qr' at position
 * 'col'. If type is NULL it's stored as "unknown". Returns OK on success, ERR
 * on bad input or out-of-bounds. */
int qr_set_col(QueryResult *qr, uint32_t col, const char *name,
        const char *type);

/* Returns the QRColumn at 'col' inside 'qr'. Returns NULL on bad input or if
 * that column is unset. */
const QRColumn *qr_get_col(const QueryResult *qr, uint32_t col);

/* Copies 'value' inside the cell of 'qr' located based on 'row' and 'col'.
 * Overwrites existing value in that cell. If value is NULL stores SQL NULL.
 *
 * Returns:
 *  YES -> success
 *  NO  -> max_query_bytes reached; caller must stop setting cells
 *  ERR -> bad input or out-of-bounds
 */
int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col, const char *value);

/* Returns pointer to cell string (owned by qr) or NULL if SQL NULL/out of
 * range. */
const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col);

/* Returns YES if cell is SQL NULL, NO if non-NULL, ERR on error. */
int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col);

#endif
