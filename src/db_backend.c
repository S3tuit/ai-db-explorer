#include "db_backend.h"
#include "postgres_backend.h"

#include <stdarg.h>
#include <stdio.h>

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
static char *db_exec_result_format_msg_v(const char *fmt, va_list args) {
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

AdbxStatus db_exec_result_set_tool_err(DbExecResult *out, const char *fmt, ...) {
  if (!out || out->kind != DBEXEC_RESULT_NONE)
    return ERR;

  va_list args;
  va_start(args, fmt);
  char *msg = db_exec_result_format_msg_v(fmt, args);
  va_end(args);

  out->kind = DBEXEC_RESULT_TOOL_ERR;
  out->tool_err_msg = msg;
  return OK;
}
