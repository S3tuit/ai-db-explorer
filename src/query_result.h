#ifndef QUERY_RESULT_H
#define QUERY_RESULT_H

#include <stdint.h>
#include <stddef.h>

typedef struct QRColumn {
    char *name;
    char *type;     // type name in text format like "int4", "text", "date"
} QRColumn;

/* This is the only entity accepted by the Serializer. It's a materialized,
 * DB-agnostic query result. It owns cols and cells. */
typedef struct QueryResult {
    uint32_t id;        // id of the request
    uint32_t ncols;
    QRColumn *cols;     // malloc'd array of ncols length

    uint32_t nrows;
    char **cells;       // length (nrows * ncols). To access an element:
                        // cells[row*ncols + col];
    uint64_t exec_ms;
    uint8_t truncated;  // 1 if output row count is lower that the row count
                        // of the query executed
} QueryResult;

/* Creates a QueryResult with allocated storage for cells (all NULL). */
QueryResult *qr_create(uint32_t id, uint32_t ncols, uint32_t nrows, uint8_t truncated);

/* Frees all owned memory, 'qr' itself too. */
void qr_destroy(QueryResult *qr);

/* Copies 'name' and 'type' inside the column metadata of 'qr' at position
 * 'col'. If type is NULL it's stored as "unknown". Returns 1 if the column was
 * set, 0 if name and type are NULL, -1 on error. */
int qr_set_col(QueryResult *qr, uint32_t col, const char *name,
        const char *type);

/* Returns the QRColumn at 'col' inside 'qr'. Returns NULL on bad input or if
 * that column is unset. */
const QRColumn *qr_get_col(const QueryResult *qr, uint32_t col);

/* Copies 'value' inside the cell of 'qr' located based on 'row' and 'col'.
 * Overwrite existing value in that cell. If value is NULL stores SQL NULL.
 * Returns 1 on success, 0 if 'qr' is NULL, -1 on error. */
int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col, const char *value);

/* Returns pointer to cell string (owned by qr) or NULL if SQL NULL/out of
 * range. */
const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col);

/* Returns 1 if cell is SQL NULL, 0 if non-NULL, -1 on error. */
int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col);

#endif
