#ifndef DUMMY_SECRET_STORE_WARNING
#include "secret_store.h"
#include "utils.h"

// Placeholder for the real secret store implementation.

/* Returns ERR since no real secret store is wired yet.
 * Ownership: caller owns 'out' and should zero+clean it after use.
 * Side effects: none.
 * Error semantics: always ERR. */
static int secret_store_real_get(SecretStore *store, const char *secret_ref,
                                 StrBuf *out) {
  (void)store;
  (void)secret_ref;
  if (out) {
    out->len = 0;
  }
  return ERR;
}

/* Destroys the real store placeholder.
 * Ownership: consumes the store pointer.
 * Side effects: none.
 * Error semantics: no return value. */
static void secret_store_real_destroy(SecretStore *store) { free(store); }

static const SecretStoreVTable SECRET_STORE_REAL_VT = {
    .get = secret_store_real_get,
    .destroy = secret_store_real_destroy,
};

/* Creates a SecretStore placeholder.
 * Ownership: caller owns the returned store and must destroy it.
 * Side effects: none.
 * Error semantics: returns NULL on allocation failure. */
SecretStore *secret_store_create(void) {
  SecretStore *store = xmalloc(sizeof(*store));
  store->vt = &SECRET_STORE_REAL_VT;
  return store;
}

/* Destroys the SecretStore using its vtable.
 * Ownership: consumes the store pointer.
 * Side effects: none.
 * Error semantics: no return value. */
void secret_store_destroy(SecretStore *store) {
  if (!store || !store->vt || !store->vt->destroy)
    return;
  store->vt->destroy(store);
}

/* Writes the secret into 'out' as a NUL-terminated string.
 * Ownership: caller owns 'out' and should zero+clean it after use.
 * Side effects: real impl may access OS stores (not yet).
 * Error semantics: OK on success, ERR on failure. */
int secret_store_get(SecretStore *store, const char *secret_ref, StrBuf *out) {
  if (!store || !store->vt || !store->vt->get)
    return ERR;
  return store->vt->get(store, secret_ref, out);
}
#endif
