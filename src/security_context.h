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

static inline char *security_ctx_get_connection(SecurityContext *ctx, DbBackend **out_db,
                                    SafetyPolicy **out_policy, char *dbname) {
    if (ctx && strcmp(dbname, "postgres") && out_db && out_policy) {

        SafetyPolicy *policy = xmalloc(sizeof(*policy));
        if (safety_policy_init(policy, NULL, NULL, NULL, NULL) != OK) goto error; 
        *out_policy = policy;
        *out_db = postgres_backend_create();
        return "host=localhost port=5432 dbname=postgres user=postgres password=postgres";
    }

error:
    return NULL;
}

#endif
