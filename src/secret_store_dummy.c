#ifdef DUMMY_SECRET_STORE_WARNING

// WARNING.
// Dummy implementation that returns secrets based on hard-coded references.
// This is used for integration tests only

#include "log.h"
#include "secret_store.h"
#include "utils.h"

#include <stdio.h>
#include <string.h>

typedef struct {
  const char *connection_name;
  const char *secret;
} SecretPair;

// Update this table to change hard-coded secrets for the dummy store.
static const SecretPair DUMMY_SECRETS[] = {
    {"MyPostgres", "my_secret"},
    {"AnotherPostgres", "another_secret"},
    {"SuperPostgres", "postgres"},
};

/* Writes a dummy secret for a known reference.
 * Ownership: caller owns 'out' and should zero+clean after use.
 * Side effects: none.
 * Error semantics: YES on success, NO when ref is missing, ERR on bad input or
 * allocation failure. */
static AdbxTriStatus secret_store_dummy_get(SecretStore *store,
                                            const SecretRefInfo *ref,
                                            StrBuf *out) {
  (void)store;
  if (!ref || !ref->connection_name || !out)
    return ERR;

  sb_zero_clean(out);
  out->len = 0;

  const char *secret = NULL;
  for (size_t i = 0; i < ARRLEN(DUMMY_SECRETS); i++) {
    if (strcmp(DUMMY_SECRETS[i].connection_name, ref->connection_name) == 0) {
      secret = DUMMY_SECRETS[i].secret;
      break;
    }
  }
  if (!secret)
    return NO;

  size_t n = strlen(secret);

  char *dst = NULL;
  if (sb_prepare_for_write(out, n + 1, &dst) != OK)
    return ERR;
  memcpy(dst, secret, n + 1);
  return YES;
}

/* Dummy set is intentionally unsupported.
 * Ownership: borrows inputs.
 * Side effects: none.
 * Error semantics: always ERR.
 */
static AdbxStatus secret_store_dummy_set(SecretStore *store,
                                         const SecretRefInfo *ref,
                                         const char *secret) {
  (void)store;
  (void)ref;
  (void)secret;
  return ERR;
}

/* Dummy delete is intentionally unsupported.
 * Ownership: borrows inputs.
 * Side effects: none.
 * Error semantics: always ERR.
 */
static AdbxStatus secret_store_dummy_delete(SecretStore *store,
                                            const SecretRefInfo *ref) {
  (void)store;
  (void)ref;
  return ERR;
}

/* Dummy namespace wipe is intentionally unsupported.
 * Ownership: borrows inputs.
 * Side effects: none.
 * Error semantics: always ERR.
 */
static AdbxStatus
secret_store_dummy_wipe_namespace(SecretStore *store,
                                  const char *cred_namespace) {
  (void)store;
  (void)cred_namespace;
  return ERR;
}

/* Dummy wipe-all is intentionally unsupported.
 * Ownership: borrows input.
 * Side effects: none.
 * Error semantics: always ERR.
 */
static AdbxStatus secret_store_dummy_wipe_all(SecretStore *store) {
  (void)store;
  return ERR;
}

/* Destroys the dummy store.
 * Ownership: consumes the store pointer.
 * Side effects: none.
 * Error semantics: no return value. */
static void secret_store_dummy_destroy(SecretStore *store) { free(store); }

/* Returns the last backend error for dummy store.
 * Ownership: borrows 'store'; returned string is static.
 * Side effects: none.
 * Error semantics: always returns one descriptive static string.
 */
static const char *secret_store_dummy_last_error(SecretStore *store) {
  (void)store;
  return "dummy secret store does not persist secrets";
}

/* Returns the last backend error category for dummy store.
 * Ownership: borrows 'store'.
 * Side effects: none.
 * Error semantics: returns SSERR_NONE because this backend is test-only.
 */
static SecretStoreErrCode
secret_store_dummy_last_error_code(SecretStore *store) {
  (void)store;
  return SSERR_NONE;
}

static const SecretStoreVTable SECRET_STORE_DUMMY_VT = {
    .get = secret_store_dummy_get,
    .set = secret_store_dummy_set,
    .delete = secret_store_dummy_delete,
    .wipe_namespace = secret_store_dummy_wipe_namespace,
    .wipe_all = secret_store_dummy_wipe_all,
    .destroy = secret_store_dummy_destroy,
    .last_error = secret_store_dummy_last_error,
    .last_error_code = secret_store_dummy_last_error_code,
};

SecretStore *secret_store_dummy_backend_create(void) {
  SecretStore *store = xmalloc(sizeof(*store));
  store->vt = &SECRET_STORE_DUMMY_VT;
  fprintf(stderr,
          "WARNING: using dummy secret store (DUMMY_SECRET_STORE_WARNING).\n");
  return store;
}

#endif
