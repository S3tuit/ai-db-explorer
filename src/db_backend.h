#ifndef DB_BACKEND_H
#define DB_BACKEND_H

#include "safety_policy.h"
#include "query_result.h"
#include "conn_catalog.h"
#include "utils.h"

/* DB-agnostic interface that defines all the functions a db backend must have
 * in order to be used. */
typedef struct DbBackend DbBackend;

typedef struct DbBackendVTable {
    // Establishes a connection described by 'profile' using 'pwd' when needed.
    // The SafetyPolicy is borrowed and copied inside the backend.
    // Returns ok/err.
    int (*connect)(DbBackend *db, const ConnProfile *profile,
                    const SafetyPolicy *policy, const char *pwd);

    // Returns YES if connected, NO if not, ERR on bad input. This should be a
    // cheap check that doesn't perform networking.
    int (*is_connected)(DbBackend *db);

    // Closes the active connection, if any. Safe to call multiple times.
    void (*disconnect)(DbBackend *db);
    
    // Closes the connection of 'db' and frees its allocation
    void (*destroy)(DbBackend *db);

    // Executes 'sql' statement, materializes a QueryResult having 'request_id'
    // as id. The QueryResult may represent an error or a good response. This
    // returns OK if it was able to allocate a QueryResult, ERR if it wasn't
    // able to allocate it.
    int (*exec) (DbBackend *db, uint32_t request_id, const char *sql,
                    QueryResult **out_qr);
} DbBackendVTable;

struct DbBackend {
    const DbBackendVTable *vt;
    void *impl; // db specific
};

/* Small helpers */
static inline int db_connect(DbBackend *db, const ConnProfile *profile,
                             const SafetyPolicy *policy, const char *pwd) {
    if (!db || !db->vt || !db->vt->connect) return ERR;
    return db->vt->connect(db, profile, policy, pwd);
}
static inline int db_is_connected(DbBackend *db) {
    if (!db || !db->vt || !db->vt->is_connected) return ERR;
    return db->vt->is_connected(db);
}
static inline void db_disconnect(DbBackend *db) {
    if (!db || !db->vt || !db->vt->disconnect) return;
    db->vt->disconnect(db);
}
static inline void db_destroy(DbBackend *db) {
    if (!db || !db->vt || !db->vt->destroy) return;
    db->vt->destroy(db);
}
static inline int db_exec(DbBackend *db, uint32_t request_id, const char *sql,
                          QueryResult **out_qr) {
    if (!db || !db->vt || !db->vt->exec) return ERR;
    return db->vt->exec(db, request_id, sql, out_qr);
}

#endif
