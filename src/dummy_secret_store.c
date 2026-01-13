#ifdef DUMMY_SECRET_STORE_WARNING

// WARNING.
// Dummy implementation that returns secrets based on hard-coded references.
// This is used for integration tests only

#include "secret_store.h"
#include "utils.h"

#include <string.h>
#include <stdio.h>


typedef struct {
    const char *ref;
    const char *secret;
} SecretPair;

// Update this table to change hard-coded secrets for the dummy store.
static const SecretPair DUMMY_SECRETS[] = {
    {"MyPostgres", "my_secret"},
    {"AnotherPostgres", "another_secret"},
};

/* Writes a dummy secret for a known reference.
 * Ownership: caller owns 'out' and should zero+clean after use.
 * Side effects: none.
 * Error semantics: ERR on bad input, missing ref, or allocation failure. */
static int secret_store_dummy_get(SecretStore *store, const char *secret_ref,
                                  StrBuf *out) {
    (void)store;
    if (!secret_ref || !out) return ERR;

    sb_zero_mem(out);
    out->len = 0;

    const char *secret = NULL;
    for (size_t i = 0; i < ARRLEN(DUMMY_SECRETS); i++) {
        if (strcmp(DUMMY_SECRETS[i].ref, secret_ref) == 0) {
            secret = DUMMY_SECRETS[i].secret;
            break;
        }
    }
    if (!secret) return ERR;

    size_t n = strlen(secret);

    char *dst = NULL;
    if (sb_prepare_for_write(out, n + 1, &dst) != OK) return ERR;
    memcpy(dst, secret, n + 1);
    return OK;
}

/* Destroys the dummy store.
 * Ownership: consumes the store pointer.
 * Side effects: none.
 * Error semantics: no return value. */
static void secret_store_dummy_destroy(SecretStore *store) {
    free(store);
}

static const SecretStoreVTable SECRET_STORE_DUMMY_VT = {
    .get = secret_store_dummy_get,
    .destroy = secret_store_dummy_destroy,
};

/* Creates a SecretStore using the dummy vtable.
 * Ownership: caller owns the returned store and must destroy it.
 * Side effects: logs a warning to stderr.
 * Error semantics: returns NULL on allocation failure. */
SecretStore *secret_store_create(void) {
    SecretStore *store = xmalloc(sizeof(*store));
    store->vt = &SECRET_STORE_DUMMY_VT;
    fprintf(stderr, "WARNING: using dummy secret store (DUMMY_SECRET_STORE_WARNING).\n");
    return store;
}

/* Destroys the SecretStore using its vtable.
 * Ownership: consumes the store pointer.
 * Side effects: none.
 * Error semantics: no return value. */
void secret_store_destroy(SecretStore *store) {
    if (!store || !store->vt || !store->vt->destroy) return;
    store->vt->destroy(store);
}

/* Writes the secret into 'out' as a NUL-terminated string.
 * Ownership: caller owns 'out' and should zero+clean it after use.
 * Side effects: none for dummy; real impl may access OS stores.
 * Error semantics: OK on success, ERR on failure. */
int secret_store_get(SecretStore *store, const char *secret_ref, StrBuf *out) {
    if (!store || !store->vt || !store->vt->get) return ERR;
    return store->vt->get(store, secret_ref, out);
}
#endif
