#ifndef SECURITY_CTX_H
#define SECURITY_CTX_H

#include "utils.h"

// THIS IS JUST A DUMMY FOR NOW
typedef struct {
    int i;
} SecurityContext;

SecurityContext *security_ctx_create() {
    SecurityContext *s = xmalloc(sizeof(*s));
    return s;
}

void security_ctx_destroy(SecurityContext *s) {
    free(s);
}

#endif
