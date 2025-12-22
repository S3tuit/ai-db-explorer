#ifndef DB_BACKEND_H
#define DB_BACKEND_H

#include "safety_policy.h"
#include "query_result.h"

/* DB-agnostic interface that defines all the functions a db backend must have
 * in order to be used. */
typedef struct DbBackend DbBackend;

typedef struct DbBackendVTable {
    int (*connect)(DbBackend *db, const char *conninfo);
    void (*close)(DbBackend *db);

    // Executes 'sql' statement, making sure it complies with 'policy', and
    // materializes a QueryResult (must call qr_create_ok()) having 'request_id'
    // as id. Returns 1 on success, -1 on error.
    int (*exec) (DbBackend *db, const SafetyPolicy *policy, uint32_t request_id,
                    const char *sql, QueryResult **out_qr);
} DbBackendVTable;

struct DbBackend {
    const DbBackendVTable *vt;
    void *impl; // db specific
};

#endif
