#include "query_result.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Returns YES if 'row' and 'col' form a valid index to access cells of 'qr'. */
static inline int idx_ok(const QueryResult *qr, uint32_t row, uint32_t col) {
    return (qr && qr->cols && qr->cells && row < qr->nrows && col < qr->ncols)
        ? YES : NO;
}

/* Returns a pointer to a duplicated value of 's' of at most 'cap' bytes or
 * NULL if 's' is NULL. If the string is truncated appends "...\0" without
 * going past the 'cap'. */
static inline char *dupn_or_null(const char *s, size_t cap) {
    if (!s || cap < 4) return NULL;

    size_t n = strnlen(s, cap);

    if (n == cap) { // not terminated within cap -> truncate to "...".
        char *p = xmalloc(cap);
        memcpy(p, s, cap - 4); 
        p[cap - 4] = '.';
        p[cap - 3] = '.';
        p[cap - 2] = '.';
        p[cap - 1] = '\0';
        return p;
    }

    // fully fits (including '\0')
    char *p = xmalloc(n + 1);
    memcpy(p, s, n + 1); // includes '\0'
    return p;
}

static inline char *dup_or_null(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s);

    // fully fits (including '\0')
    char *p = xmalloc(n + 1);
    memcpy(p, s, n + 1); // includes '\0'
    return p;
}

QueryResult *qr_create_ok(uint32_t id, uint32_t ncols, uint32_t nrows, uint8_t truncated) {
    // Overflow check for nrows*ncols
    if (ncols != 0 && nrows > (UINT32_MAX / ncols)) {
        return NULL;
    }
    QueryResult *qr = xmalloc(sizeof(*qr));
    size_t ncells = (size_t)ncols * (size_t)nrows;

    qr->cols = (QRColumn *)xcalloc(ncols, sizeof(QRColumn));
    qr->cells = (char **)xcalloc(ncells, sizeof(char *));

    qr->id = id;
    qr->status = QR_OK;
    qr->ncols = ncols;
    qr->nrows = nrows;
    qr->exec_ms = 0;
    qr->truncated = truncated;

    return qr;
}

QueryResult *qr_create_err(uint32_t id, const char *err_msg) {
    QueryResult *qr = xmalloc(sizeof(*qr));

    qr->id = id;
    qr->status = QR_ERROR;

    const char *err = err_msg ? err_msg : "";
    size_t len = strlen(err) + 1; // null term
    qr->err_msg = xmalloc(len);
    memcpy(qr->err_msg, err, len);

    return qr;
}

void qr_destroy(QueryResult *qr) {
    if (!qr) return;
   
    // if it represent an error
    if (qr->status == QR_ERROR) {
        free(qr->err_msg);
        free(qr);
        return;
    }

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
    if (!qr) return ERR;

    // out-of-bounds
    if (!qr->cols || col >= qr->ncols) return ERR;

    // name is required, only type can be NULL
    if (!name) return ERR;

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
    return OK;
}

const QRColumn *qr_get_col(const QueryResult *qr, uint32_t col) {
    if (!qr || col >= qr->ncols) return NULL;

    // since cols are allocated using calloc, they're all 0. Returns NULL if
    // they're all 0
    QRColumn zero = {0};
    if (memcmp(&qr->cols[col], &zero, sizeof(QRColumn)) == 0) return NULL;
    return &qr->cols[col];
}

int qr_set_cell_capped(QueryResult *qr, uint32_t row, uint32_t col,
                        const char *value, uint32_t cap) {
    if (!qr) return ERR;
    if (!idx_ok(qr, row, col)) return ERR;

    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;

    // Overwrite existing value
    free(qr->cells[idx]);
    
    // value may be NULL and it's ok to store NULL, it means SQL NULL
    char *copy = dupn_or_null(value, (size_t)cap);
    qr->cells[idx] = copy;
    return OK;
}

int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col, const char *value) {
    if (!qr) return ERR;
    if (!idx_ok(qr, row, col)) return ERR;

    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;

    // Overwrite existing value
    free(qr->cells[idx]);
    
    // value may be NULL and it's ok to store NULL, it means SQL NULL
    char *copy = dup_or_null(value);
    qr->cells[idx] = copy;
    return OK;
}

const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col) {
    if (!idx_ok(qr, row, col)) return NULL;
    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
    return qr->cells[idx];
}

int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col) {
    if (!idx_ok(qr, row, col)) return ERR;
    size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
    return (qr->cells[idx] == NULL) ? YES : NO;
}
