#ifndef QUERY_RESULT_H
#define QUERY_RESULT_H

#include <stdint.h>
#include <stddef.h>

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
    uint32_t id;        // id of the request
    QRStatus status;

    union {
        // valid if QR_OK
        struct {
            uint32_t ncols;
            QRColumn *cols;     // malloc'd array of ncols length

            uint32_t nrows;
            char **cells;       // length (nrows * ncols). To access an element:
                                // cells[row*ncols + col];
            uint64_t exec_ms;
            uint8_t truncated;  // 1 if output row count is lower that the row count
                                // of the query executed
        };

        // valid if QR_ERROR
        char *err_msg;
    };

    // valid if QR_ERROR

} QueryResult;

/* Creates a QueryResult with allocated storage for cells (all NULL). Can't
 * return NULL. */
QueryResult *qr_create_ok(uint32_t id, uint32_t ncols, uint32_t nrows, uint8_t truncated);

/* Creates a QueryResult that represents an error. malloc 'err_msg'. Can't
 * return NULL. */
QueryResult *qr_create_err(uint32_t id, const char *err_msg);

/* Creates a QueryResult with a single text column named "message" and one row.
 * If msg is NULL, stores an empty string. Can't return NULL. */
QueryResult *qr_create_msg(uint32_t id, const char *msg);

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
 * Overwrite existing value in that cell. If value is NULL stores SQL NULL.
 * Returns OK on success, ERR on bad input or out-of-bounds. */
int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col, const char *value);
/* Like qr_set_cell but copies at most 'cap' bytes from 'value'. */
int qr_set_cell_capped(QueryResult *qr, uint32_t row, uint32_t col,
                        const char *value, uint32_t cap);

/* Returns pointer to cell string (owned by qr) or NULL if SQL NULL/out of
 * range. */
const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col);

/* Returns YES if cell is SQL NULL, NO if non-NULL, ERR on error. */
int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col);

#endif
