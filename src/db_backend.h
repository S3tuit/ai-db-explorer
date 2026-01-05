#ifndef DB_BACKEND_H
#define DB_BACKEND_H

#include "safety_policy.h"
#include "query_result.h"

/* DB-agnostic interface that defines all the functions a db backend must have
 * in order to be used. */
typedef struct DbBackend DbBackend;

typedef struct DbBackendVTable {
    // Estabilishes a connection at 'conninfo' and makes sure statements sent
    // using 'db' comply with 'policy'. 'db' takes ownership of 'policy'.
    // Returns ok/err.
    int (*connect)(DbBackend *db, const char *conninfo, const SafetyPolicy *policy);
    
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

#endif
