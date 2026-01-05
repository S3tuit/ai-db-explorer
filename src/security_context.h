#ifndef SECURITY_CTX_H
#define SECURITY_CTX_H

#include "utils.h"
#include "postgres_backend.h"
#include "safety_policy.h"

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
        db->vt->destroy(db);
        return NO;
    }

    const char *conninfo =
        "host=localhost port=5432 dbname=postgres user=postgres password=postgres";
    if (db->vt->connect(db, conninfo, &policy) != OK) {
        db->vt->destroy(db);
        return NO;
    }

    *out_db = db;
    return YES;
}

#endif
