#include "query_result.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Returns 1 if 'row' and 'col' form a valid index to access cells of 'qr'. */
static inline int idx_ok(const QueryResult *qr, uint32_t row, uint32_t col) {
    return qr && qr->cols && qr->cells && row < qr->nrows && col < qr->ncols;
}

/* Returns a pointer to a duplicated value of 's' or NULL if 's' is NULL. */
static inline char *dup_or_null(const char *s) {
    if (!s) return NULL;
    char *p = strdup(s);
    if (!p) {
        fprintf(stderr, "strdup. returned NULL on a non-NULL input.\n");
        exit(1);
    }
    return p;
}

QueryResult *qr_create(uint32_t id, uint32_t ncols, uint32_t nrows, uint8_t truncated) {
    // Overflow check for nrows*ncols
    if (ncols != 0 && nrows > (UINT32_MAX / ncols)) {
        return NULL;
    }
    QueryResult *qr = xmalloc(sizeof(*qr));
    size_t ncells = (size_t)ncols * (size_t)nrows;

    qr->cols = (QRColumn *)xcalloc(ncols, sizeof(QRColumn));
    qr->cells = (char **)xcalloc(ncells, sizeof(char *));

    qr->id = id;
    qr->ncols = ncols;
    qr->nrows = nrows;
    qr->exec_ms = 0;
    qr->truncated = truncated;

    return qr;
}

void qr_destroy(QueryResult *qr) {
    if (!qr) return;

    // free all the storage for cells
    if (qr->cells && qr->ncols > 0 && qr->nrows > 0) {
        size_t ncells = (size_t)qr->ncols * (size_t)qr->nrows;
        for (size_t i = 0; i < ncells; i++) {
            free(qr->cells[i]); 
        }
    }
    free(qr->cells);

    // free all the storage for cols
    if (qr->cols) {
        for (uint32_t c = 0; c < qr->ncols; c++) {
            free(qr->cols[c].name);
            free(qr->cols[c].type);
        }
    }
    free(qr->cols);
    free(qr);
}

int qr_set_col(QueryResult *qr, uint32_t col, const char *name,
        const char *type) {
    if (!qr) return 0;

    // out-of-bounds
    if (!qr->cols || col >= qr->ncols) return -1;

    // name is required, only type can be NULL
    if (!name) return -1;

    // we must not change 'name' and 'type' because qr should outlive them
    char *new_name = dup_or_null(name);
    // type can be NULL
    const char *safe_type = type ? type : "unknown";
    char *new_type = dup_or_null(safe_type);

    // free if that col was already populated
    free(qr->cols[col].name);
    free(qr->cols[col].type);

    qr->cols[col].name = new_name;
    qr->cols[col].type = new_type;
    return 1;
}

const QRColumn *qr_get_col(const QueryResult *qr, uint32_t col) {
    if (!qr || col >= qr->ncols) return NULL;

    // since cols are allocated using calloc, they're all 0. Returns NULL if
    // they're all 0
    QRColumn zero = {0};
    if (memcmp(&qr->cols[col], &zero, sizeof(QRColumn)) == 0) return NULL;
    return &qr->cols[col];
}

int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col, const char *value) {
    if (!qr) return 0;
    if (!idx_ok(qr, row, col)) return -1;

    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;

    // Overwrite existing value
    free(qr->cells[idx]);
    
    // value may be NULL and it's ok to store NULL, it means SQL NULL
    char *copy = dup_or_null(value);
    qr->cells[idx] = copy;
    return 1;
}

const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col) {
    if (!idx_ok(qr, row, col)) return NULL;
    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
    return qr->cells[idx];
}

int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col) {
    if (!idx_ok(qr, row, col)) return -1;
    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
    return (qr->cells[idx] == NULL) ? 1 : 0;
}

