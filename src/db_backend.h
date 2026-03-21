#ifndef DB_BACKEND_H
#define DB_BACKEND_H

#include "adbx_err.h"
#include "conn_catalog.h"
#include "query_ir.h"
#include "query_result.h"
#include "safety_policy.h"
#include "utils.h"

/* DB-agnostic interface that defines all the functions a db backend must have
 * in order to be used. */
typedef struct DbBackend DbBackend;

// Maximum number of bound parameters accepted by token-aware execution paths.
#define MAX_TOKEN_PARAMS 10u

typedef struct DbSafeFuncList {
  const char **names; // sorted, lowercase, unqualified names
  uint32_t count;
} DbSafeFuncList;

typedef enum {
  DBERR_NONE = 0,
  DBERR_INPUT,
  DBERR_GENERIC,
} DbErrCode;

typedef struct {
  DbErrCode code;
  char msg[ADBX_ERRMSG_MAX];
} DbErr;

/* DB-facing bind parameter used by bound execution APIs.
 * Ownership: all pointers are borrowed for the duration of one db_exec_bound().
 * v1 uses Postgres OID metadata; other backends may ignore pg_oid.
 */
typedef struct DbExecParam {
  const char *value; // text value (borrowed)
  uint32_t value_len;
  uint32_t pg_oid;
} DbExecParam;

typedef enum {
  DBEXEC_RESULT_NONE = 0,
  DBEXEC_RESULT_QUERY_RESULT = 1,
  DBEXEC_RESULT_TOOL_ERR = 2,
} DbExecResultKind;

/* Result of one db_exec()/db_exec_bound() call.
 * Ownership:
 * - when kind is DBEXEC_RESULT_QUERY_RESULT, caller owns 'qr' and must destroy
 *   it with qr_destroy() or db_exec_result_clean().
 * - when kind is DBEXEC_RESULT_TOOL_ERR, caller owns 'tool_err_msg' and must
 *   free it or call db_exec_result_clean().
 */
typedef struct DbExecResult {
  DbExecResultKind kind;
  union {
    QueryResult *qr;
    char *tool_err_msg;
  };
} DbExecResult;

typedef struct DbBackendVTable {
  // Establishes a connection described by 'profile' using 'pwd' when needed.
  // The SafetyPolicy is borrowed and copied inside the backend. 'out_err' is
  // optional and receives one typed diagnostic on failure.
  // Returns ok/err.
  AdbxStatus (*connect)(DbBackend *db, const ConnProfile *profile,
                        const SafetyPolicy *policy, const char *pwd,
                        DbErr *out_err);

  // Returns YES if connected, NO if not, ERR on bad input. This should be a
  // cheap check that doesn't perform networking.
  AdbxTriStatus (*is_connected)(DbBackend *db);

  // Closes the active connection, if any. Safe to call multiple times.
  void (*disconnect)(DbBackend *db);

  // Closes the connection of 'db' and frees its allocation
  void (*destroy)(DbBackend *db);

  // Executes 'sql' statement and materializes a QueryResult. The QueryResult
  // represents only successful tabular output. Tool-level execution failures
  // are returned via 'out_res->tool_err_msg'. Safe to call with unitialized
  // 'out_res'. 'qb_policy' is optional; when non-NULL it drives sensitive-token
  // tokenization during materialization.
  // Returns OK when 'out_res' is populated with either a QueryResult or a tool
  // error message, ERR on invalid input or backend/internal failure.
  AdbxStatus (*exec)(DbBackend *db, const char *sql,
                     const QueryResultBuildPolicy *qb_policy,
                     DbExecResult *out_res);

  // Executes one SQL statement with positional bind parameters.
  // 'params[i]' maps to SQL placeholder $(i+1). Safe to call with unitialized
  // 'out_res'. Returns OK when 'out_res' is populated with either a QueryResult
  // or a tool error message, ERR on invalid input or backend/internal failure.
  AdbxStatus (*exec_bound)(DbBackend *db, const char *sql,
                           const DbExecParam *params, uint32_t nparams,
                           const QueryResultBuildPolicy *qb_policy,
                           DbExecResult *out_res);

  // Creates a QirQueryHandle starting from 'sql'. The backend owns and
  // populates the handle, and the caller must destroy it via
  // qir_handle_destroy(). 'out_err' is optional and receives one typed
  // diagnostic only when the function returns ERR.
  AdbxStatus (*make_query_ir)(DbBackend *db, const char *sql,
                              QirQueryHandle *out, DbErr *out_err);

  // Returns a list of functions that are safe to execute (v1 uses name only).
  const DbSafeFuncList *(*safe_functions)(DbBackend *db);
} DbBackendVTable;

struct DbBackend {
  const DbBackendVTable *vt;
  // TODO: use the same interfae pattern as src/secret_store.h
  void *impl; // db specific
};

/* Returns the right DbBackend based on 'kind'. */
DbBackend *db_backend_create(DbKind kind);

/* Initializes one DbExecResult to empty state.
 * It borrows 'out' and does not allocate. NULL input is ignored.
 */
void db_exec_result_init(DbExecResult *out);

/* Releases any payload owned by 'out' and resets it to the empty state.
 * It borrows 'out'. NULL input is ignored.
 */
void db_exec_result_clean(DbExecResult *out);

/* Stores one successful QueryResult into 'out'.
 * It borrows 'out' and takes ownership of 'qr'. 'out' must be empty.
 * Returns OK on success, ERR on invalid input or non-empty output state.
 */
AdbxStatus db_exec_result_set_qr(DbExecResult *out, QueryResult *qr);

/* Stores one formatted tool-error message into 'out'.
 * It borrows 'out' and formatting inputs. 'out' must be empty.
 * Returns OK on success, ERR on invalid input, allocation failure, or
 * non-empty output state.
 */
AdbxStatus db_exec_result_set_tool_err(DbExecResult *out, const char *fmt, ...);

/* Small helpers */
static inline AdbxStatus db_connect(DbBackend *db, const ConnProfile *profile,
                                    const SafetyPolicy *policy, const char *pwd,
                                    DbErr *out_err) {
  ADBX_ERR_CLEAR(out_err, DBERR_NONE);
  if (!db || !db->vt || !db->vt->connect) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "db_connect failed: invalid backend input. This is probably "
                  "a bug, please report it.");
    return ERR;
  }
  AdbxStatus rc = db->vt->connect(db, profile, policy, pwd, out_err);
  if (rc == ERR && out_err && out_err->code == DBERR_NONE) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "db_connect failed without backend diagnostics.");
  }
  return rc;
}
static inline AdbxTriStatus db_is_connected(DbBackend *db) {
  if (!db || !db->vt || !db->vt->is_connected)
    return ERR;
  return db->vt->is_connected(db);
}
static inline void db_disconnect(DbBackend *db) {
  if (!db || !db->vt || !db->vt->disconnect)
    return;
  db->vt->disconnect(db);
}
static inline void db_destroy(DbBackend *db) {
  if (!db || !db->vt || !db->vt->destroy)
    return;
  db->vt->destroy(db);
}
static inline AdbxStatus db_exec(DbBackend *db, const char *sql,
                                 const QueryResultBuildPolicy *qb_policy,
                                 DbExecResult *out_res) {
  db_exec_result_init(out_res);
  if (!db || !db->vt || !db->vt->exec || !out_res)
    return ERR;
  return db->vt->exec(db, sql, qb_policy, out_res);
}

static inline AdbxStatus db_exec_bound(DbBackend *db, const char *sql,
                                       const DbExecParam *params,
                                       uint32_t nparams,
                                       const QueryResultBuildPolicy *qb_policy,
                                       DbExecResult *out_res) {
  db_exec_result_init(out_res);
  if (!db || !db->vt || !db->vt->exec_bound || !out_res)
    return ERR;
  return db->vt->exec_bound(db, sql, params, nparams, qb_policy, out_res);
}

static inline AdbxStatus db_make_query_ir(DbBackend *db, const char *sql,
                                          QirQueryHandle *out, DbErr *out_err) {
  ADBX_ERR_CLEAR(out_err, DBERR_NONE);
  if (!db || !db->vt || !db->vt->make_query_ir) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "db_make_query_ir failed: invalid backend input. This is "
                  "probably a bug, please report it.");
    return ERR;
  }
  AdbxStatus rc = db->vt->make_query_ir(db, sql, out, out_err);
  if (rc == ERR && out_err && out_err->code == DBERR_NONE) {
    ADBX_ERR_SETF(out_err, DBERR_GENERIC,
                  "db_make_query_ir failed without backend diagnostics.");
  }
  return rc;
}

static inline const DbSafeFuncList *db_safe_functions(DbBackend *db) {
  if (!db || !db->vt || !db->vt->safe_functions)
    return NULL;
  return db->vt->safe_functions(db);
}
/* ------------------------------- for tests --------------------------------*/

#ifdef ADBX_TEST_MODE
typedef DbBackend *(*DbBackendFactory)(DbKind kind);

/* Installs one test-only backend factory override used by db_backend_create().
 * Passing NULL clears the override.
 */
void db_backend_set_test_factory(DbBackendFactory factory);
#endif

#endif
