#include "db_backend.h"
#include "postgres_backend.h"

#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#ifdef ADBX_TEST_MODE
static DbBackendFactory g_db_test_factory = NULL;

void db_backend_set_test_factory(DbBackendFactory factory) {
  g_db_test_factory = factory;
}
#endif

DbBackend *db_backend_create(DbKind kind) {
#ifdef ADBX_TEST_MODE
  if (g_db_test_factory)
    return g_db_test_factory(kind);
#endif

  switch (kind) {
  case DB_KIND_POSTGRES:
    return postgres_backend_create();
  default:
    return NULL;
  }
}

/* Formats one tool-error message from printf-style inputs.
 * It borrows 'fmt' and 'args' and returns a newly allocated message owned by
 * caller.
 * Side effects: allocates heap memory.
 * Error semantics: returns an allocated empty string when formatting fails.
 */
static char *db_format_msg_v(const char *fmt, va_list args) {
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

/* Returns YES when 'kind' is one supported DbRelationKind, NO when the enum is
 * outside the supported set.
 * It borrows no heap state and allocates nothing.
 */
static AdbxTriStatus db_relation_kind_is_valid(DbRelationKind kind) {
  switch (kind) {
  case DBREL_KIND_TABLE:
  case DBREL_KIND_VIEW:
  case DBREL_KIND_MATVIEW:
  case DBREL_KIND_FOREIGN_TABLE:
    return YES;
  case DBREL_KIND_NONE:
  default:
    return NO;
  }
}

/* Copies one NUL-terminated string into the arena and returns the arena-owned
 * copy.
 */
static inline char *db_relation_info_copy_str(Arena *arena, const char *src) {
  if (!arena || !src)
    return NULL;
  char *out = NULL;
  if (arena_add_nul(arena, (void *)src, (uint32_t)strlen(src),
                    (void **)&out) != OK) {
    return NULL;
  }
  return out;
}

DbRelationInfo *db_relation_info_create(uint32_t ncols) {
  DbRelationInfo *info = xcalloc(1, sizeof(*info));
  if (arena_init(&info->arena, NULL, NULL) != OK) {
    free(info);
    return NULL;
  }

  if (ncols > 0) {
    size_t cols_bytes = (size_t)ncols * sizeof(*info->cols);
    if (cols_bytes > UINT32_MAX) {
      arena_clean(&info->arena);
      free(info);
      return NULL;
    }
    if (arena_calloc(&info->arena, (uint32_t)cols_bytes,
                     (void **)&info->cols) != OK) {
      arena_clean(&info->arena);
      free(info);
      return NULL;
    }
  }

  info->ncols = ncols;
  return info;
}

AdbxStatus db_relation_info_set_identity(DbRelationInfo *info,
                                         const char *schema_name,
                                         const char *relation_name,
                                         DbRelationKind kind) {
  char *schema_copy = NULL;
  char *relation_copy = NULL;

  if (!info || !schema_name || !relation_name || schema_name[0] == '\0' ||
      relation_name[0] == '\0' || db_relation_kind_is_valid(kind) != YES) {
    return ERR;
  }

  schema_copy = db_relation_info_copy_str(&info->arena, schema_name);
  relation_copy = db_relation_info_copy_str(&info->arena, relation_name);
  if (!schema_copy || !relation_copy)
    return ERR;

  info->schema_name = schema_copy;
  info->relation_name = relation_copy;
  info->kind = kind;
  return OK;
}

AdbxStatus db_relation_info_set_col(
    DbRelationInfo *info, uint32_t idx, const char *name, const char *type,
    uint8_t is_primary_key, uint8_t is_foreign_key, const char *ref_schema_name,
    const char *ref_relation_name, const char *ref_column_name) {
  static const char *UNKNOWN_TYPE = "unknown";
  DbRelationColumn *col = NULL;

  if (!info || !name || name[0] == '\0' || idx >= info->ncols ||
      (info->ncols > 0 && !info->cols)) {
    return ERR;
  }

  is_primary_key = (is_primary_key != 0);
  is_foreign_key = (is_foreign_key != 0);

  if (!is_foreign_key) {
    if (ref_schema_name || ref_relation_name || ref_column_name)
      return ERR;
  } else if (!ref_schema_name || !ref_relation_name || !ref_column_name ||
             ref_schema_name[0] == '\0' || ref_relation_name[0] == '\0' ||
             ref_column_name[0] == '\0') {
    return ERR;
  }

  col = &info->cols[idx];
  col->name = db_relation_info_copy_str(&info->arena, name);
  col->type =
      db_relation_info_copy_str(&info->arena, type ? type : UNKNOWN_TYPE);
  if (!col->name || !col->type)
    return ERR;

  col->is_primary_key = is_primary_key;
  col->is_foreign_key = is_foreign_key;

  if (is_foreign_key) {
    col->ref_schema_name =
        db_relation_info_copy_str(&info->arena, ref_schema_name);
    col->ref_relation_name =
        db_relation_info_copy_str(&info->arena, ref_relation_name);
    col->ref_column_name =
        db_relation_info_copy_str(&info->arena, ref_column_name);
    if (!col->ref_schema_name || !col->ref_relation_name ||
        !col->ref_column_name) {
      return ERR;
    }
  }

  return OK;
}

void db_relation_info_destroy(DbRelationInfo *info) {
  if (!info)
    return;
  arena_clean(&info->arena);
  free(info);
}

void db_exec_result_init(DbExecResult *out) {
  if (!out)
    return;
  out->kind = DBEXEC_RESULT_NONE;
  out->qr = NULL;
}

void db_exec_result_clean(DbExecResult *out) {
  if (!out)
    return;

  if (out->kind == DBEXEC_RESULT_QUERY_RESULT) {
    qr_destroy(out->qr);
  } else if (out->kind == DBEXEC_RESULT_TOOL_ERR) {
    free(out->tool_err_msg);
  }

  db_exec_result_init(out);
}

AdbxStatus db_exec_result_set_qr(DbExecResult *out, QueryResult *qr) {
  if (!out || !qr || out->kind != DBEXEC_RESULT_NONE)
    return ERR;

  out->kind = DBEXEC_RESULT_QUERY_RESULT;
  out->qr = qr;
  return OK;
}

AdbxStatus db_exec_result_set_tool_err(DbExecResult *out, const char *fmt,
                                       ...) {
  if (!out || out->kind != DBEXEC_RESULT_NONE)
    return ERR;

  va_list args;
  va_start(args, fmt);
  char *msg = db_format_msg_v(fmt, args);
  va_end(args);

  out->kind = DBEXEC_RESULT_TOOL_ERR;
  out->tool_err_msg = msg;
  return OK;
}

void db_describe_result_init(DbDescribeResult *out) {
  if (!out)
    return;
  out->kind = DBDESCRIBE_RESULT_NONE;
  out->relation_info = NULL;
}

void db_describe_result_clean(DbDescribeResult *out) {
  if (!out)
    return;

  if (out->kind == DBDESCRIBE_RESULT_RELATION_INFO) {
    db_relation_info_destroy(out->relation_info);
  } else if (out->kind == DBDESCRIBE_RESULT_TOOL_ERR) {
    free(out->tool_err_msg);
  }

  db_describe_result_init(out);
}

AdbxStatus db_describe_result_set_relation_info(DbDescribeResult *out,
                                                DbRelationInfo *info) {
  if (!out || !info || out->kind != DBDESCRIBE_RESULT_NONE)
    return ERR;

  out->kind = DBDESCRIBE_RESULT_RELATION_INFO;
  out->relation_info = info;
  return OK;
}

AdbxStatus db_describe_result_set_tool_err(DbDescribeResult *out,
                                           const char *fmt, ...) {
  if (!out || out->kind != DBDESCRIBE_RESULT_NONE)
    return ERR;

  va_list args;
  va_start(args, fmt);
  char *msg = db_format_msg_v(fmt, args);
  va_end(args);

  out->kind = DBDESCRIBE_RESULT_TOOL_ERR;
  out->tool_err_msg = msg;
  return OK;
}
