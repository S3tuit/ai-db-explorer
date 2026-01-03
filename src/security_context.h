#ifndef SECURITY_CTX_H
#define SECURITY_CTX_H

#include "utils.h"

// THIS IS JUST A DUMMY FOR NOW
typedef struct {
    int i;
} SecurityContext;

static inline SecurityContext *security_ctx_create() {
    SecurityContext *s = xmalloc(sizeof(*s));
    return s;
}

static inline void security_ctx_destroy(SecurityContext *s) {
    free(s);
}

#endif
