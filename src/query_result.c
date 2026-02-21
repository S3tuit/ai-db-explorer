#include "query_result.h"
#include "sensitive_tok.h"
#include "string_op.h"
#include "utils.h"
#include "validator.h"

#include <assert.h>
#include <limits.h>
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

/* Duplicates exactly 'len' bytes from 's' and appends trailing NUL.
 * It borrows 's' and allocates returned storage that caller must free.
 * Side effects: heap allocation.
 * Error semantics: returns owned C-string on success, NULL on invalid input or
 * allocation failure.
 */
static char *dup_n_or_null(const char *s, size_t len) {
  if (!s)
    return NULL;
  if (len > SIZE_MAX - 1u)
    return NULL;

  char *dst = (char *)xmalloc(len + 1u);
  memcpy(dst, s, len);
  dst[len] = '\0';
  return dst;
}

/* Copies one column metadata entry into QueryResult storage.
 * It borrows inputs and mutates caller-owned 'qr'.
 * Side effects: allocates/frees column metadata strings.
 * Error semantics: returns OK on success, ERR on invalid input/out-of-bounds.
 */
static int qr_set_col(QueryResult *qr, uint32_t col, const char *name,
                      const char *type, QRColType value_type, uint32_t pg_oid) {
  if (!qr || !qr->cols || col >= qr->ncols || !name)
    return ERR;

  char *new_name = dup_or_null(name);
  const char *safe_type = type ? type : "unknown";
  char *new_type = dup_or_null(safe_type);
  if (!new_name || !new_type) {
    free(new_name);
    free(new_type);
    return ERR;
  }

  free(qr->cols[col].name);
  free(qr->cols[col].type);

  qr->cols[col].name = new_name;
  qr->cols[col].type = new_type;
  qr->cols[col].value_type = value_type;
  qr->cols[col].pg_oid = pg_oid;
  return OK;
}

/* Stores a cell value while enforcing max_query_bytes.
 * It borrows 'value' and mutates caller-owned 'qr'.
 * Side effects: allocates/frees cell strings and updates used_query_bytes.
 * Error semantics: returns YES on success, NO when byte cap would be exceeded,
 * ERR on invalid input/out-of-bounds/allocation failure.
 */
static int qr_set_cell(QueryResult *qr, uint32_t row, uint32_t col,
                       const char *value, size_t v_len) {
  if (!qr)
    return ERR;
  if (!idx_ok_set(qr, row, col))
    return ERR;
  if (!value && v_len != 0)
    return ERR;

  size_t idx = (size_t)row * (size_t)qr->ncols + (size_t)col;
  size_t val_len = value ? v_len : 0;

  size_t prev_len = 0;
  if (qr->cells[idx])
    prev_len = strlen(qr->cells[idx]);

  if (qr->max_query_bytes > 0) {
    uint64_t next_used = qr->used_query_bytes - (uint64_t)prev_len;
    if (next_used + (uint64_t)val_len > qr->max_query_bytes) {
      return NO;
    }
  }

  char *copy = value ? dup_n_or_null(value, val_len) : NULL;
  if (value && !copy)
    return ERR;

  free(qr->cells[idx]);
  qr->cells[idx] = copy;
  qr->used_query_bytes =
      (qr->used_query_bytes - (uint64_t)prev_len) + (uint64_t)val_len;
  return YES;
}

int qr_set_id(QueryResult *qr, const McpId *id) {
  if (!qr || !id)
    return ERR;

  McpId tmp = {0};
  if (id->kind == MCP_ID_INT) {
    mcp_id_init_u32(&tmp, id->u32);
  } else if (id->kind == MCP_ID_STR) {
    if (!id->str)
      return ERR;
    if (mcp_id_init_str_copy(&tmp, id->str) != OK)
      return ERR;
  } else {
    return ERR;
  }

  mcp_id_clean(&qr->id);
  qr->id = tmp;
  return OK;
}

QueryResult *qr_create_ok(const McpId *id, uint32_t ncols, uint32_t nrows,
                          uint8_t result_truncated, uint64_t max_query_bytes) {
  QueryResult *qr = xmalloc(sizeof(*qr));
  size_t ncells = (size_t)ncols * (size_t)nrows;

  qr->cols = (QRColumn *)xcalloc(ncols, sizeof(QRColumn));
  qr->cells = (char **)xcalloc(ncells, sizeof(char *));

  qr->id = (McpId){0};
  if (id) {
    if (qr_set_id(qr, id) != OK) {
      free(qr->cells);
      free(qr->cols);
      free(qr);
      return NULL;
    }
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

/* Shared helper for QR_ERROR and QR_TOOL_ERROR. */
static QueryResult *qr_create_err_impl(const McpId *id, QRStatus status,
                                       QrErrorCode code, const char *err_msg) {
  QueryResult *qr = xmalloc(sizeof(*qr));

  qr->id = (McpId){0};
  if (id) {
    if (qr_set_id(qr, id) != OK) {
      free(qr);
      return NULL;
    }
  }
  qr->status = status;
  qr->exec_ms = 0;
  qr->err_code = code;

  const char *err = err_msg ? err_msg : "";
  size_t len = strlen(err) + 1; // null term
  qr->err_msg = xmalloc(len);
  memcpy(qr->err_msg, err, len);

  return qr;
}

QueryResult *qr_create_err(const McpId *id, QrErrorCode code,
                           const char *err_msg) {
  return qr_create_err_impl(id, QR_ERROR, code, err_msg);
}

QueryResult *qr_create_tool_err(const McpId *id, const char *err_msg) {
  return qr_create_err_impl(id, QR_TOOL_ERROR, 0, err_msg);
}

QueryResult *qr_create_msg(const McpId *id, const char *msg) {
  QueryResult *qr = qr_create_ok(id, 1, 1, 0, 0);
  if (!qr)
    return NULL;

  if (qr_set_col(qr, 0, "message", "text", QRCOL_V_PLAINTEXT, 0) != OK ||
      qr_set_cell(qr, 0, 0, msg ? msg : "", msg ? strlen(msg) : 0) != YES) {
    qr_destroy(qr);
    return NULL;
  }
  return qr;
}

void qr_destroy(QueryResult *qr) {
  if (!qr)
    return;

  mcp_id_clean(&qr->id);

  // if it represents an error (protocol or tool)
  if (qr->status == QR_ERROR || qr->status == QR_TOOL_ERROR) {
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

int qb_init(QueryResultBuilder *qb, QueryResult *qr,
            const QueryResultBuildPolicy *policy) {
  if (!qb || !qr)
    return ERR;
  qb->qr = qr;
  if (policy) {
    qb->plan = policy->plan;
    qb->store = policy->store;
    qb->generation = policy->generation;
  } else {
    qb->plan = NULL;
    qb->store = NULL;
    qb->generation = 0;
  }
  return OK;
}

int qb_set_col(QueryResultBuilder *qb, uint32_t col, const char *name,
               const char *type, uint32_t pg_oid) {
  if (!qb || !qb->qr)
    return ERR;

  QRColType kind = QRCOL_V_PLAINTEXT;
  if (qb->plan) {
    const ValidatorColPlan *vcol =
        (const ValidatorColPlan *)parr_cat(qb->plan->cols, col);
    if (!vcol)
      return ERR;
    kind = (vcol->kind == VCOL_OUT_TOKEN) ? QRCOL_V_TOKEN : QRCOL_V_PLAINTEXT;
  }
  return qr_set_col(qb->qr, col, name, type, kind, pg_oid);
}

const QRColumn *qr_get_col(const QueryResult *qr, uint32_t col) {
  if (!qr || col >= qr->ncols)
    return NULL;
  assert(qr->cols);

  // since a QRColumn must have name valorized, returns NULL if it's not
  if (!qr->cols[col].name)
    return NULL;
  return &qr->cols[col];
}

int qb_set_cell(QueryResultBuilder *qb, uint32_t row, uint32_t col,
                const char *value, size_t v_len) {
  if (!qb || !qb->qr)
    return ERR;
  if (!value && v_len != 0)
    return ERR;

  if (!qb->plan)
    return qr_set_cell(qb->qr, row, col, value, v_len);

  const ValidatorColPlan *vcol =
      (const ValidatorColPlan *)parr_cat(qb->plan->cols, col);
  if (!vcol)
    return ERR;
  if (vcol->kind != VCOL_OUT_TOKEN) {
    // not sensitive
    return qr_set_cell(qb->qr, row, col, value, v_len);
  }

  // SQL NULL stays NULL even on sensitive columns.
  if (!value)
    return qr_set_cell(qb->qr, row, col, NULL, 0);
  if (!qb->store)
    return ERR;
  if (!vcol->col_id || vcol->col_id[0] == '\0')
    return ERR;
  if (vcol->col_id_len == 0)
    return ERR;
  if (v_len > UINT32_MAX)
    return ERR;

  const QRColumn *qcol = qr_get_col(qb->qr, col);
  if (!qcol)
    return ERR;

  SensitiveTokIn in = {
      .value = value,
      .value_len = (uint32_t)v_len,
      .col_ref = vcol->col_id,
      .col_ref_len = vcol->col_id_len,
      .pg_oid = qcol->pg_oid,
  };
  char tok[SENSITIVE_TOK_BUFSZ];
  int tok_len = stok_store_create_token(qb->store, qb->generation, &in, tok);
  if (tok_len < 0)
    return ERR;

  return qr_set_cell(qb->qr, row, col, tok, (size_t)tok_len);
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
