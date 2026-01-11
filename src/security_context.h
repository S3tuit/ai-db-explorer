#ifndef SECURITY_CTX_H
#define SECURITY_CTX_H

#include "utils.h"
#include "postgres_backend.h"
#include "safety_policy.h"
#include "conn_catalog.h"

#include <string.h>

// THIS IS JUST A DUMMY FOR NOW
typedef struct {
    int i;
} SecurityContext;

static inline SecurityContext *security_ctx_create() {
    SecurityContext *ctx = xmalloc(sizeof(*ctx));
    return ctx;
}

static inline void security_ctx_destroy(SecurityContext *ctx) {
    free(ctx);
}

/* Best-effort connector. Returns:
 *  YES -> *out_db is connected and owned by caller.
 *  NO  -> known db, but connection failed.
 *  ERR -> unknown db or bad input. */
static inline int security_ctx_connect(SecurityContext *ctx,
        const char *dbname, DbBackend **out_db) {
    if (!ctx || !dbname || !out_db) return ERR;
    *out_db = NULL;

    if (strcmp(dbname, "postgres") != 0) return ERR;

    DbBackend *db = postgres_backend_create();
    SafetyPolicy policy = {0};
    if (safety_policy_init(&policy, NULL, NULL, NULL, NULL) != OK) {
        db_destroy(db);
        return NO;
    }

    ConnProfile profile = {0};
    profile.connection_name = "pg_dummy";
    profile.kind = DB_KIND_POSTGRES;
    profile.host = "localhost";
    profile.port = 5432;
    profile.db_name = "postgres";
    profile.user = "postgres";
    profile.password_ref = NULL;
    profile.options = NULL;

    const char *pwd = "postgres";
    if (db_connect(db, &profile, &policy, pwd) != OK) {
        db_destroy(db);
        return NO;
    }

    *out_db = db;
    return YES;
}

#endif
