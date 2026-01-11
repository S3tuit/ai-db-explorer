#ifndef SECRET_STORE_H
#define SECRET_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils.h"

typedef struct {uint32_t dummy;} SecretStore;

// Create a SecretStore instance.
// Even if it is "stateless", this helps injecting mocks and keeping platform
// code isolated.
static inline SecretStore *secret_store_create() {
    SecretStore *sc = xmalloc(sizeof(*sc));
    return sc;
}

// Destroy the store and release resources.
static inline void secret_store_destroy(SecretStore *store) {
    free(store);
}

// Lookup a secret by reference key. On success allocates a NUL-terminated
// string into *out_buf (caller must free). Returns OK/ERR.
static inline int secret_store_get(SecretStore *store, const char *secret_ref, char **out_buf) {
    if (!store || !secret_ref || !out_buf) return ERR;

    if (strcmp(secret_ref, "dummy") == 0) {
        const char *secret = "postgres";
        size_t n = strlen(secret) + 1;

        char *buf = (char *)xmalloc(n);
        memcpy(buf, secret, n);

        *out_buf = buf;
        return OK;
    }

    return ERR;
}

#endif
