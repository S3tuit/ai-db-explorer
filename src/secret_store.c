#include "secret_store.h"

/* Tries one backend probe and enforces fail-closed semantics.
 * It borrows probe/out_store and performs no allocations.
 * Side effects: may initialize one backend through the probe callback.
 * Error semantics: returns YES when backend created, NO when unavailable, ERR
 * when probe reports failure.
 */
static AdbxTriStatus
ss_try_probe(AdbxTriStatus (*probe)(SecretStore **), SecretStore **out_store) {
  if (!probe || !out_store)
    return ERR;
  *out_store = NULL;
  AdbxTriStatus rc = probe(out_store);
  if (rc != YES && rc != NO && rc != ERR)
    return ERR;
  return rc;
}

SecretStore *secret_store_create(void) {
#ifdef DUMMY_SECRET_STORE_WARNING
  return secret_store_dummy_backend_create();
#endif

  SecretStore *store = NULL;

#ifdef __APPLE__
  AdbxTriStatus krc = ss_try_probe(secret_store_keychain_backend_probe, &store);
  if (krc == YES)
    return store;
  if (krc == ERR)
    return NULL;
#endif

#ifdef HAVE_LIBSECRET
  AdbxTriStatus lrc =
      ss_try_probe(secret_store_libsecret_backend_probe, &store);
  if (lrc == YES)
    return store;
  if (lrc == ERR)
    return NULL;
#endif

  AdbxTriStatus frc = ss_try_probe(secret_store_file_backend_probe, &store);
  if (frc == YES)
    return store;
  return NULL;
}

/* ----------------------------------- HELPERS ----------------------------- */
void secret_store_destroy(SecretStore *store) {
  if (!store || !store->vt || !store->vt->destroy)
    return;
  store->vt->destroy(store);
}

AdbxTriStatus secret_store_get(SecretStore *store, const char *secret_ref,
                               StrBuf *out) {
  if (!store || !store->vt || !store->vt->get)
    return ERR;
  return store->vt->get(store, secret_ref, out);
}

AdbxStatus secret_store_set(SecretStore *store, const char *secret_ref,
                            const char *secret) {
  if (!store || !store->vt || !store->vt->set)
    return ERR;
  return store->vt->set(store, secret_ref, secret);
}

AdbxStatus secret_store_delete(SecretStore *store, const char *secret_ref) {
  if (!store || !store->vt || !store->vt->delete)
    return ERR;
  return store->vt->delete(store, secret_ref);
}

AdbxStatus secret_store_wipe_all(SecretStore *store) {
  if (!store || !store->vt || !store->vt->wipe_all)
    return ERR;
  return store->vt->wipe_all(store);
}
