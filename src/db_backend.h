#ifndef DB_BACKEND_H
#define DB_BACKEND_H

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

/* DB-facing bind parameter used by bound execution APIs.
 * Ownership: all pointers are borrowed for the duration of one db_exec_bound().
 * v1 uses Postgres OID metadata; other backends may ignore pg_oid.
 */
typedef struct DbExecParam {
  const char *value; // text value (borrowed)
  uint32_t value_len;
  uint32_t pg_oid;
} DbExecParam;

typedef struct DbBackendVTable {
  // Establishes a connection described by 'profile' using 'pwd' when needed.
  // The SafetyPolicy is borrowed and copied inside the backend.
  // Returns ok/err.
  AdbxStatus (*connect)(DbBackend *db, const ConnProfile *profile,
                        const SafetyPolicy *policy, const char *pwd);

  // Returns YES if connected, NO if not, ERR on bad input. This should be a
  // cheap check that doesn't perform networking.
  AdbxTriStatus (*is_connected)(DbBackend *db);

  // Closes the active connection, if any. Safe to call multiple times.
  void (*disconnect)(DbBackend *db);

  // Closes the connection of 'db' and frees its allocation
  void (*destroy)(DbBackend *db);

  // Executes 'sql' statement and materializes a QueryResult. The QueryResult
  // may represent an error or a good response. The id field of the result is
  // left zeroed; the caller is responsible for stamping it.
  // 'qb_policy' is optional; when non-NULL it drives sensitive-token
  // tokenization during materialization.
  // Returns OK if it was able to allocate a QueryResult, ERR otherwise.
  AdbxStatus (*exec)(DbBackend *db, const char *sql,
                     const QueryResultBuildPolicy *qb_policy,
                     QueryResult **out_qr);

  // Executes one SQL statement with positional bind parameters.
  // 'params[i]' maps to SQL placeholder $(i+1).
  // Returns OK if it was able to allocate a QueryResult, ERR otherwise.
  AdbxStatus (*exec_bound)(DbBackend *db, const char *sql,
                           const DbExecParam *params, uint32_t nparams,
                           const QueryResultBuildPolicy *qb_policy,
                           QueryResult **out_qr);

  // Creates a QirQueryHandle starting from 'sql'. The backend owns and
  // populates the handle, and the caller must destroy it via
  // qir_handle_destroy().
  AdbxStatus (*make_query_ir)(DbBackend *db, const char *sql,
                              QirQueryHandle *out);

  // Returns a list of functions that are safe to execute (v1 uses name only).
  const DbSafeFuncList *(*safe_functions)(DbBackend *db);

  // Returns the latest error detected by db. The returned string is owned by
  // 'db'
  const char *(*last_error)(DbBackend *db);
} DbBackendVTable;

struct DbBackend {
  const DbBackendVTable *vt;
  // TODO: use the same interfae pattern as src/secret_store.h
  void *impl; // db specific
};

/* Returns the right DbBackend based on 'kind'. */
DbBackend *db_backend_create(DbKind kind);

/* Small helpers */
static inline AdbxStatus db_connect(DbBackend *db, const ConnProfile *profile,
                                    const SafetyPolicy *policy,
                                    const char *pwd) {
  if (!db || !db->vt || !db->vt->connect)
    return ERR;
  return db->vt->connect(db, profile, policy, pwd);
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
                                 QueryResult **out_qr) {
  if (!db || !db->vt || !db->vt->exec)
    return ERR;
  return db->vt->exec(db, sql, qb_policy, out_qr);
}

static inline AdbxStatus db_exec_bound(DbBackend *db, const char *sql,
                                       const DbExecParam *params,
                                       uint32_t nparams,
                                       const QueryResultBuildPolicy *qb_policy,
                                       QueryResult **out_qr) {
  if (!db || !db->vt || !db->vt->exec_bound)
    return ERR;
  return db->vt->exec_bound(db, sql, params, nparams, qb_policy, out_qr);
}

static inline AdbxStatus db_make_query_ir(DbBackend *db, const char *sql,
                                          QirQueryHandle *out) {
  if (!db || !db->vt || !db->vt->make_query_ir)
    return ERR;
  return db->vt->make_query_ir(db, sql, out);
}

static inline const DbSafeFuncList *db_safe_functions(DbBackend *db) {
  if (!db || !db->vt || !db->vt->safe_functions)
    return NULL;
  return db->vt->safe_functions(db);
}

#endif
