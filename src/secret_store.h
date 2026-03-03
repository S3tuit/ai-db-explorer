#ifndef SECRET_STORE_H
#define SECRET_STORE_H

#include <stddef.h>
#include <stdint.h>

#include "string_op.h"
#include "utils.h"

typedef struct SecretStore SecretStore;
typedef struct SecretStoreVTable SecretStoreVTable;

struct SecretStoreVTable {
  // Writes a NUL-terminated secret into 'out'.
  // Returns YES when found, NO when missing, ERR on failure.
  AdbxTriStatus (*get)(SecretStore *store, const char *secret_ref, StrBuf *out);
  // Stores one NUL-terminated secret.
  AdbxStatus (*set)(SecretStore *store, const char *secret_ref,
                    const char *secret);
  // Deletes one stored secret.
  AdbxStatus (*delete)(SecretStore *store, const char *secret_ref);
  // Deletes all stored secrets in this store namespace.
  AdbxStatus (*wipe_all)(SecretStore *store);
  // Destroys the store and releases resources.
  void (*destroy)(SecretStore *store);
};

struct SecretStore {
  const SecretStoreVTable *vt;
};

// Create a SecretStore instance.
// If DUMMY_SECRET_STORE_WARNING is defined, it uses a hard-coded dummy store
// and emits a warning on creation.
// Ownership: caller owns the returned store and must destroy it.
// Side effects: none.
// Error semantics: returns NULL on allocation failure.
SecretStore *secret_store_create(void);

/* -------------------------------- HELPERS -------------------------------- */

void secret_store_destroy(SecretStore *store);

AdbxTriStatus secret_store_get(SecretStore *store, const char *secret_ref,
                               StrBuf *out);

AdbxStatus secret_store_set(SecretStore *store, const char *secret_ref,
                            const char *secret);

AdbxStatus secret_store_delete(SecretStore *store, const char *secret_ref);

AdbxStatus secret_store_wipe_all(SecretStore *store);

/* ---------------------------- SUPPORTED STORES --------------------------- */

/* Creates the file-backed SecretStore implementation.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Side effects: may touch filesystem paths and files used by the backend.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_file_backend_create(void);

/* Creates the macOS Keychain-backed SecretStore implementation.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Side effects: may probe OS Keychain services.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_keychain_backend_create(void);

/* Creates the libsecret-backed SecretStore implementation.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Side effects: may probe D-Bus Secret Service endpoints.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_libsecret_backend_create(void);

#ifdef DUMMY_SECRET_STORE_WARNING
/* Creates a dummy SecretStore implementation. Use this only in test env.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_dummy_backend_create(void);
#endif

#endif
