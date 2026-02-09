#include "query_result.h"
#include "string_op.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Returns YES if 'row' and 'col' form a valid index to write into cells. */
static inline int idx_ok_set(const QueryResult *qr, uint32_t row,
                             uint32_t col) {
  return (qr && qr->cols && qr->cells && row < qr->nrows_alloc &&
          col < qr->ncols)
             ? YES
             : NO;
}

/* Returns YES if 'row' and 'col' form a valid index to read from cells. */
static inline int idx_ok_get(const QueryResult *qr, uint32_t row,
                             uint32_t col) {
  return (qr && qr->cols && qr->cells && row < qr->nrows && col < qr->ncols)
             ? YES
             : NO;
}

QueryResult *qr_create_ok(const McpId *id, uint32_t ncols, uint32_t nrows,
                          uint8_t result_truncated, uint64_t max_query_bytes) {
  QueryResult *qr = xmalloc(sizeof(*qr));
  size_t ncells = (size_t)ncols * (size_t)nrows;

  qr->cols = (QRColumn *)xcalloc(ncols, sizeof(QRColumn));
  qr->cells = (char **)xcalloc(ncells, sizeof(char *));

  if (id) {
    if (mcp_id_copy(&qr->id, id) != OK) {
      free(qr->cells);
      free(qr->cols);
      free(qr);
      return NULL;
    }
  } else {
    qr->id = (McpId){0};
  }
  qr->status = QR_OK;
  qr->ncols = ncols;
  qr->nrows = nrows;
  qr->nrows_alloc = nrows;
  qr->exec_ms = 0;
  qr->result_truncated = result_truncated;
  qr->max_query_bytes = max_query_bytes;
  qr->used_query_bytes = 0;

  return qr;
}

QueryResult *qr_create_err(const McpId *id, const char *err_msg) {
  QueryResult *qr = xmalloc(sizeof(*qr));

  if (id) {
    if (mcp_id_copy(&qr->id, id) != OK) {
      free(qr);
      return NULL;
    }
  } else {
    qr->id = (McpId){0};
  }
  qr->status = QR_ERROR;
  qr->exec_ms = 0;

  const char *err = err_msg ? err_msg : "";
  size_t len = strlen(err) + 1; // null term
  qr->err_msg = xmalloc(len);
  memcpy(qr->err_msg, err, len);

  return qr;
}

QueryResult *qr_create_msg(const McpId *id, const char *msg) {
  QueryResult *qr = qr_create_ok(id, 1, 1, 0, 0);
  qr_set_col(qr, 0, "message", "text");
  qr_set_cell(qr, 0, 0, msg ? msg : "");
  return qr;
}

void qr_destroy(QueryResult *qr) {
  if (!qr)
    return;

  mcp_id_clean(&qr->id);

  // if it represent an error
  if (qr->status == QR_ERROR) {
    free(qr->err_msg);
    free(qr);
    return;
  }

  // free all the storage for cells
  if (qr->cells && qr->ncols > 0 && qr->nrows_alloc > 0) {
    size_t ncells = (size_t)qr->ncols * (size_t)qr->nrows_alloc;
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
  if (!qr)
    return ERR;

  // out-of-bounds
  if (!qr->cols || col >= qr->ncols)
    return ERR;

  // name is required, only type can be NULL
  if (!name)
    return ERR;

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
  if (!qr || col >= qr->ncols)
    return NULL;

  // since cols are allocated using calloc, they're all 0. Returns NULL if
  // they're all 0
  QRColumn zero = {0};
  if (memcmp(&qr->cols[col], &zero, sizeof(QRColumn)) == 0)
    return NULL;
  return &qr->cols[col];
}

/* Stores a cell value while enforcing max_query_bytes; on NO the caller must
 * stop populating rows to keep a fully-formed result set. */
int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col,
                const char *value) {
  if (!qr)
    return ERR;
  if (!idx_ok_set(qr, row, col))
    return ERR;

  size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;

  // account for previous value before overwriting
  if (qr->cells[idx]) {
    qr->used_query_bytes -= strlen(qr->cells[idx]);
  }

  size_t val_len = value ? strlen(value) : 0;
  if (qr->max_query_bytes > 0 &&
      qr->used_query_bytes + val_len > qr->max_query_bytes) {
    // do not overwrite when the cap would be exceeded
    if (qr->cells[idx]) {
      qr->used_query_bytes += strlen(qr->cells[idx]);
    }
    return NO;
  }

  // Overwrite existing value
  free(qr->cells[idx]);

  // value may be NULL and it's ok to store NULL, it means SQL NULL
  char *copy = dup_or_null(value);
  qr->cells[idx] = copy;
  qr->used_query_bytes += val_len;
  return YES;
}

const char *qr_get_cell(const QueryResult *qr, uint32_t row, uint32_t col) {
  if (!idx_ok_get(qr, row, col))
    return NULL;
  size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
  return qr->cells[idx];
}

int qr_is_null(const QueryResult *qr, uint32_t row, uint32_t col) {
  if (!idx_ok_get(qr, row, col))
    return ERR;
  size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
  return (qr->cells[idx] == NULL) ? YES : NO;
}
