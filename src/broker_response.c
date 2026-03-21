#include "broker_response.h"

#include "json_codec.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
  BRESP_KIND_RPC_ERROR = 1,
  BRESP_KIND_TOOL_ERROR = 2,
  BRESP_KIND_QUERY_RESULT = 3,
  BRESP_KIND_CONN_PROFILES = 4,
} BrokerResponseKind;

typedef struct {
  char *connection_name;
  DbKind kind;
  int read_only;
} BrespConnProfileSummary;

struct BrokerResponse {
  McpId id;
  BrokerResponseKind kind;
  union {
    struct {
      BrespErrorCode code;
      char *msg;
    } rpc_error;        // valid if BRESP_KIND_RPC_ERR
    char *tool_err_msg; // valid if BRESP_KIND_TOOL_ERROR
    QueryResult *qr;    // valid if BRESP_KIND_QUERY_RESULT
    struct {
      BrespConnProfileSummary *profiles;
      size_t n_profiles;
    } conn_profiles; // valid if BRESP_KIND_CONN_PROFILES
  };
};

/* Formats one owned message string from printf-style inputs.
 * It borrows 'fmt' and 'args' and returns a heap-allocated string owned by
 * caller. Returns an allocated empty string when formatting fails.
 */
static char *bresp_format_msg_v(const char *fmt, va_list args) {
  const char *safe_fmt = fmt ? fmt : "";

  va_list args_len;
  va_copy(args_len, args);
  int need = vsnprintf(NULL, 0, safe_fmt, args_len);
  va_end(args_len);
  if (need < 0) {
    char *fallback = xmalloc(1);
    fallback[0] = '\0';
    return fallback;
  }

  size_t buf_len = (size_t)need + 1u;
  char *msg = xmalloc(buf_len);
  int written = vsnprintf(msg, buf_len, safe_fmt, args);
  if (written < 0 || (size_t)written >= buf_len)
    msg[0] = '\0';
  return msg;
}

/* Copies 'src' into 'dst' using McpId ownership rules.
 * It borrows both pointers.
 * Side effects: may allocate memory for string-backed ids.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
static AdbxStatus bresp_copy_id(McpId *dst, const McpId *src) {
  if (!dst || !src)
    return ERR;

  memset(dst, 0, sizeof(*dst));
  switch (src->kind) {
  case MCP_ID_INT:
    mcp_id_init_u32(dst, src->u32);
    return OK;
  case MCP_ID_STR:
    if (!src->str)
      return ERR;
    return mcp_id_init_str_copy(dst, src->str);
  default:
    return ERR;
  }
}

/* Allocates one empty BrokerResponse and deep-copies the request id.
 * It borrows 'id'.
 * Returns a caller-owned BrokerResponse on success, NULL on invalid input.
 */
static BrokerResponse *bresp_create_base(const McpId *id) {
  if (!id)
    return NULL;

  BrokerResponse *bresp = xmalloc(sizeof(*bresp));
  if (bresp_copy_id(&bresp->id, id) != OK) {
    free(bresp);
    return NULL;
  }

  return bresp;
}

/* Appends the request id to one JSON-RPC object.
 * Returns OK on success, ERR on invalid input, unsupported id kind, or append
 * failure.
 */
static AdbxStatus bresp_append_id(StrBuf *sb, const McpId *id) {
  if (!sb || !id)
    return ERR;

  switch (id->kind) {
  case MCP_ID_INT:
    return json_kv_u64(sb, "id", id->u32);
  case MCP_ID_STR:
    return json_kv_str(sb, "id", id->str ? id->str : "");
  default:
    return ERR;
  }
}

/* Returns the stable wire-format backend name used in tool responses.
 * It borrows no dynamic memory and performs no allocations.
 * Returns a stable string on success, NULL for unsupported backend kinds.
 */
static const char *bresp_db_kind_name(DbKind kind) {
  switch (kind) {
  case DB_KIND_POSTGRES:
    return "postgres";
  default:
    return NULL;
  }
}

/* Appends the structuredContent object for one successful QueryResult.
 * Returns OK on success, ERR on invalid input or serialization failure.
 */
static AdbxStatus bresp_append_qr_structured(StrBuf *sb,
                                             const QueryResult *qr) {
  uint64_t cell_count_u64 = 0;
  if (!sb || !qr)
    return ERR;

  cell_count_u64 = (uint64_t)qr->nrows * (uint64_t)qr->ncols;
  if (cell_count_u64 > SIZE_MAX)
    return ERR;
  if (qr->ncols > 0 && !qr->cols)
    return ERR;
  if (cell_count_u64 > 0 && !qr->cells)
    return ERR;

  if (json_kv_obj_begin(sb, "structuredContent") != OK)
    return ERR;
  if (json_kv_u64(sb, "exec_ms", qr->exec_ms) != OK)
    return ERR;
  if (json_kv_arr_begin(sb, "columns") != OK)
    return ERR;
  for (uint32_t c = 0; c < qr->ncols; c++) {
    const QRColumn *col = qr_get_col(qr, c);
    const char *name = "";
    const char *type = "";

    if (col) {
      name = col->name ? col->name : "";
      type = col->type ? col->type : "";
    }

    if (json_obj_begin(sb) != OK)
      return ERR;
    if (json_kv_str(sb, "name", name) != OK)
      return ERR;
    if (json_kv_str(sb, "type", type) != OK)
      return ERR;
    if (json_obj_end(sb) != OK)
      return ERR;
  }
  if (json_arr_end(sb) != OK)
    return ERR;

  if (json_kv_arr_begin(sb, "rows") != OK)
    return ERR;
  for (uint32_t r = 0; r < qr->nrows; r++) {
    if (json_arr_begin(sb) != OK)
      return ERR;
    for (uint32_t c = 0; c < qr->ncols; c++) {
      const char *cell = qr_get_cell(qr, r, c);
      if (!cell) {
        if (json_arr_elem_null(sb) != OK)
          return ERR;
      } else {
        if (json_arr_elem_str(sb, cell) != OK)
          return ERR;
      }
    }
    if (json_arr_end(sb) != OK)
      return ERR;
  }
  if (json_arr_end(sb) != OK)
    return ERR;

  if (json_kv_u64(sb, "rowcount", qr->nrows) != OK)
    return ERR;
  if (json_kv_bool(sb, "resultTruncated", qr->result_truncated ? 1 : 0) != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;

  return OK;
}

/* Appends one successful QueryResult payload.
 * Returns OK on success, ERR on invalid input or serialization failure.
 */
static AdbxStatus bresp_append_qr_result(StrBuf *sb, const QueryResult *qr) {
  if (!sb || !qr)
    return ERR;

  if (json_kv_obj_begin(sb, "result") != OK)
    return ERR;
  if (json_kv_arr_begin(sb, "content") != OK)
    return ERR;
  if (json_obj_begin(sb) != OK)
    return ERR;
  if (json_kv_str(sb, "type", "text") != OK)
    return ERR;
  if (json_kv_str(sb, "text", "Query executed successfully.") != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;
  if (json_arr_end(sb) != OK)
    return ERR;
  if (bresp_append_qr_structured(sb, qr) != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;

  return OK;
}

/* Appends the structuredContent object for list_database_connections.
 * Returns OK on success, ERR on invalid input or serialization failure.
 */
static AdbxStatus bresp_append_conn_profiles_structured(
    StrBuf *sb, const BrespConnProfileSummary *profiles, size_t n_profiles) {
  if (!sb)
    return ERR;
  if (n_profiles > 0 && !profiles)
    return ERR;

  if (json_kv_obj_begin(sb, "structuredContent") != OK)
    return ERR;
  if (json_kv_arr_begin(sb, "connections") != OK)
    return ERR;
  for (size_t i = 0; i < n_profiles; i++) {
    const BrespConnProfileSummary *profile = &profiles[i];
    const char *kind_name = bresp_db_kind_name(profile->kind);
    if (!profile->connection_name || !kind_name)
      return ERR;

    if (json_obj_begin(sb) != OK)
      return ERR;
    if (json_kv_str(sb, "connectionName", profile->connection_name) != OK)
      return ERR;
    if (json_kv_str(sb, "type", kind_name) != OK)
      return ERR;
    if (json_kv_bool(sb, "readOnly", profile->read_only ? 1 : 0) != OK)
      return ERR;
    if (json_obj_end(sb) != OK)
      return ERR;
  }
  if (json_arr_end(sb) != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;

  return OK;
}

/* Appends one list_database_connections payload.
 * Returns OK on success, ERR on invalid input or serialization failure.
 */
static AdbxStatus bresp_append_conn_profiles_result(
    StrBuf *sb, const BrespConnProfileSummary *profiles, size_t n_profiles) {
  if (!sb)
    return ERR;

  if (json_kv_obj_begin(sb, "result") != OK)
    return ERR;
  if (json_kv_arr_begin(sb, "content") != OK)
    return ERR;
  if (json_obj_begin(sb) != OK)
    return ERR;
  if (json_kv_str(sb, "type", "text") != OK)
    return ERR;
  if (json_kv_str(sb, "text", "Database connections listed successfully.") !=
      OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;
  if (json_arr_end(sb) != OK)
    return ERR;
  if (bresp_append_conn_profiles_structured(sb, profiles, n_profiles) != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;

  return OK;
}

/* Appends one JSON-RPC error object to the root response.
 * Returns OK on success, ERR on invalid input or serialization failure.
 */
static AdbxStatus bresp_append_rpc_error(StrBuf *sb,
                                         const BrokerResponse *bresp) {
  if (!sb || !bresp)
    return ERR;

  if (json_kv_obj_begin(sb, "error") != OK)
    return ERR;
  if (json_kv_l(sb, "code", (long)bresp->rpc_error.code) != OK)
    return ERR;
  if (json_kv_str(sb, "message",
                  bresp->rpc_error.msg ? bresp->rpc_error.msg : "") != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;
  return OK;
}

/* Appends one tool-error payload to the root response.
 * Returns OK on success, ERR on invalid input or serialization failure.
 */
static AdbxStatus bresp_append_tool_error(StrBuf *sb,
                                          const BrokerResponse *bresp) {
  if (!sb || !bresp)
    return ERR;

  if (json_kv_obj_begin(sb, "result") != OK)
    return ERR;
  if (json_kv_arr_begin(sb, "content") != OK)
    return ERR;
  if (json_obj_begin(sb) != OK)
    return ERR;
  if (json_kv_str(sb, "type", "text") != OK)
    return ERR;
  if (json_kv_str(sb, "text", bresp->tool_err_msg ? bresp->tool_err_msg : "") !=
      OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;
  if (json_arr_end(sb) != OK)
    return ERR;
  if (json_kv_bool(sb, "isError", 1) != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;
  return OK;
}

BrokerResponse *bresp_create_query_result(const McpId *id, QueryResult *qr) {
  BrokerResponse *bresp = NULL;
  if (!id || !qr)
    return NULL;

  bresp = bresp_create_base(id);
  if (!bresp)
    return NULL;

  bresp->kind = BRESP_KIND_QUERY_RESULT;
  bresp->qr = qr;
  return bresp;
}

BrokerResponse *bresp_create_conn_profiles(const McpId *id,
                                           const ConnProfile *profiles,
                                           size_t n_profiles) {
  BrokerResponse *bresp = NULL;
  BrespConnProfileSummary *summaries = NULL;

  if (!id)
    return NULL;
  if (n_profiles > 0 && !profiles)
    return NULL;

  bresp = bresp_create_base(id);
  if (!bresp)
    return NULL;

  if (n_profiles > 0) {
    summaries = xmalloc(n_profiles * sizeof(*summaries));
    for (size_t i = 0; i < n_profiles; i++) {
      if (!profiles[i].connection_name ||
          !bresp_db_kind_name(profiles[i].kind)) {
        for (size_t j = 0; j < i; j++)
          free(summaries[j].connection_name);
        free(summaries);
        bresp_destroy(bresp);
        return NULL;
      }
      summaries[i].connection_name = dup_or_null(profiles[i].connection_name);
      if (!summaries[i].connection_name) {
        for (size_t j = 0; j < i; j++)
          free(summaries[j].connection_name);
        free(summaries);
        bresp_destroy(bresp);
        return NULL;
      }
      summaries[i].kind = profiles[i].kind;
      summaries[i].read_only = profiles[i].safe_policy.read_only ? 1 : 0;
    }
  }

  bresp->kind = BRESP_KIND_CONN_PROFILES;
  bresp->conn_profiles.profiles = summaries;
  bresp->conn_profiles.n_profiles = n_profiles;
  return bresp;
}

BrokerResponse *bresp_create_err(const McpId *id, BrespErrorCode code,
                                 const char *fmt, ...) {
  BrokerResponse *bresp = NULL;
  va_list args;

  if (!id)
    return NULL;

  bresp = bresp_create_base(id);
  if (!bresp)
    return NULL;

  va_start(args, fmt);
  bresp->rpc_error.msg = bresp_format_msg_v(fmt, args);
  va_end(args);
  bresp->kind = BRESP_KIND_RPC_ERROR;
  bresp->rpc_error.code = code;
  return bresp;
}

BrokerResponse *bresp_create_tool_err(const McpId *id, const char *fmt, ...) {
  BrokerResponse *bresp = NULL;
  va_list args;

  if (!id)
    return NULL;

  bresp = bresp_create_base(id);
  if (!bresp)
    return NULL;

  va_start(args, fmt);
  bresp->tool_err_msg = bresp_format_msg_v(fmt, args);
  va_end(args);
  bresp->kind = BRESP_KIND_TOOL_ERROR;
  return bresp;
}

void bresp_destroy(BrokerResponse *bresp) {
  if (!bresp)
    return;

  mcp_id_clean(&bresp->id);
  switch (bresp->kind) {
  case BRESP_KIND_RPC_ERROR:
    free(bresp->rpc_error.msg);
    break;
  case BRESP_KIND_TOOL_ERROR:
    free(bresp->tool_err_msg);
    break;
  case BRESP_KIND_QUERY_RESULT:
    qr_destroy(bresp->qr);
    break;
  case BRESP_KIND_CONN_PROFILES:
    for (size_t i = 0; i < bresp->conn_profiles.n_profiles; i++) {
      free(bresp->conn_profiles.profiles[i].connection_name);
    }
    free(bresp->conn_profiles.profiles);
    break;
  default:
    break;
  }

  free(bresp);
}

AdbxStatus bresp_to_jsonrpc(const BrokerResponse *bresp, StrBuf *out_json) {
  if (!bresp || !out_json)
    return ERR;

  sb_reset(out_json);

  if (json_rpc_begin(out_json) != OK)
    goto err;
  if (bresp_append_id(out_json, &bresp->id) != OK)
    goto err;

  switch (bresp->kind) {
  case BRESP_KIND_RPC_ERROR:
    if (bresp_append_rpc_error(out_json, bresp) != OK)
      goto err;
    break;
  case BRESP_KIND_TOOL_ERROR:
    if (bresp_append_tool_error(out_json, bresp) != OK)
      goto err;
    break;
  case BRESP_KIND_QUERY_RESULT:
    if (bresp_append_qr_result(out_json, bresp->qr) != OK)
      goto err;
    break;
  case BRESP_KIND_CONN_PROFILES:
    if (bresp_append_conn_profiles_result(
            out_json, bresp->conn_profiles.profiles,
            bresp->conn_profiles.n_profiles) != OK)
      goto err;
    break;
  default:
    goto err;
  }

  if (json_obj_end(out_json) != OK)
    goto err;

  return OK;

err:
  sb_reset(out_json);
  return ERR;
}
