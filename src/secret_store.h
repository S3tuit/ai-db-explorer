#ifndef SECRET_STORE_H
#define SECRET_STORE_H

#include <stddef.h>
#include <stdint.h>

#include "string_op.h"
#include "utils.h"

typedef struct SecretStore SecretStore;
typedef struct SecretStoreVTable SecretStoreVTable;

typedef enum {
  SSERR_NONE = 0,
  SSERR_INPUT,     // function received a bad input, this represent a likely
                   // internal bug
  SSERR_ENV,       // the env where the system is running has something missing
  SSERR_DIR,       // there are problems in the directory where we store the
                   // credentials file
  SSERR_CRED_FILE, // errors with the credentials file itself
  SSERR_PARSE,     // credenials file is malformed
  SSERR_WRITE,     // I/O error
} SecretStoreErrCode;

typedef struct {
  const char *cred_namespace;
  const char *connection_name;
} SecretRefInfo;

typedef struct {
  SecretRefInfo *items; // owned array
  size_t n_items;
} SecretRefList;

/* SecretStore supports concurrent reads, but mutating operations are not
 * guaranteed to form one multi-process transaction across backends. Current
 * production code relies on a higher-level single-writer discipline: the
 * credential manager is the only process that mutates stored secrets, while
 * the rest of the application only reads them. Callers must preserve that
 * invariant or provide external writer serialization.
 */
struct SecretStoreVTable {
  // Writes a NUL-terminated secret into 'out'.
  // Returns YES when found, NO when missing, ERR on failure.
  AdbxTriStatus (*get)(SecretStore *store, const SecretRefInfo *ref,
                       StrBuf *out);
  // Stores/replace one NUL-terminated secret.
  AdbxStatus (*set)(SecretStore *store, const SecretRefInfo *ref,
                    const char *secret);
  // Deletes one stored secret.
  AdbxStatus (*delete)(SecretStore *store, const SecretRefInfo *ref);
  // Lists all stored references.
  AdbxStatus (*list_refs)(SecretStore *store, SecretRefList *out);
  // Deletes all stored secrets in one namespace.
  AdbxStatus (*wipe_namespace)(SecretStore *store, const char *cred_namespace);
  // Deletes all stored secrets in this store namespace.
  AdbxStatus (*wipe_all)(SecretStore *store);
  // Destroys the store and releases resources.
  void (*destroy)(SecretStore *store);
  // Returns backend-specific last error text for diagnostics.
  // Safe to return NULL since our wrapper secret_store_last_error handles it.
  const char *(*last_error)(SecretStore *store);
  // Returns backend-specific error category for diagnostics.
  // Safe to return SSERR_NONE when there is no detail.
  SecretStoreErrCode (*last_error_code)(SecretStore *store);
};

struct SecretStore {
  const SecretStoreVTable *vt;
};

// Create a SecretStore instance.
// If DUMMY_SECRET_STORE_WARNING is defined, it uses a hard-coded dummy store
// and emits a warning on creation.
// Ownership: caller owns the returned store and must destroy it.
// Side effects: none.
// Error semantics: returns NULL when no backend can be initialized safely.
SecretStore *secret_store_create(void);

/* -------------------------------- HELPERS -------------------------------- */

void secret_store_destroy(SecretStore *store);

AdbxTriStatus secret_store_get(SecretStore *store, const SecretRefInfo *ref,
                               StrBuf *out);

AdbxStatus secret_store_set(SecretStore *store, const SecretRefInfo *ref,
                            const char *secret);

AdbxStatus secret_store_delete(SecretStore *store, const SecretRefInfo *ref);

AdbxStatus secret_store_list_refs(SecretStore *store, SecretRefList *out);

AdbxStatus secret_store_wipe_namespace(SecretStore *store,
                                       const char *cred_namespace);

AdbxStatus secret_store_wipe_all(SecretStore *store);

void secret_ref_list_clean(SecretRefList *list);

/* Returns backend-specific last error text.
 * Error semantics: returns empty string when unavailable or no error detail.
 */
const char *secret_store_last_error(SecretStore *store);

/* Returns backend-specific last error category.
 * Error semantics: returns SSERR_NONE when unavailable or no error detail.
 */
SecretStoreErrCode secret_store_last_error_code(SecretStore *store);

/* ---------------------------- SUPPORTED STORES --------------------------- */

/* Creates the file-backed SecretStore implementation.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Side effects: may touch filesystem paths and files used by the backend.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_file_backend_create(void);
/* Probes and creates the file-backed SecretStore.
 * Ownership: on YES, caller owns *out_store and must destroy it.
 * Side effects: may touch filesystem paths and files used by the backend.
 * Error semantics: returns YES on success, ERR on invalid input or
 * initialization failure.
 */
AdbxTriStatus secret_store_file_backend_probe(SecretStore **out_store);

/* Creates the macOS Keychain-backed SecretStore implementation.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Side effects: may probe OS Keychain services.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_keychain_backend_create(void);
/* Probes and creates the macOS Keychain-backed SecretStore.
 * Ownership: on YES, caller owns *out_store and must destroy it.
 * Side effects: may probe OS Keychain services.
 * Error semantics: returns YES on success, NO when backend unavailable, ERR on
 * runtime failures.
 */
AdbxTriStatus secret_store_keychain_backend_probe(SecretStore **out_store);

/* Creates the libsecret-backed SecretStore implementation.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Side effects: may probe D-Bus Secret Service endpoints.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_libsecret_backend_create(void);
/* Probes and creates the libsecret-backed SecretStore.
 * Ownership: on YES, caller owns *out_store and must destroy it.
 * Side effects: may probe D-Bus Secret Service endpoints.
 * Error semantics: returns YES on success, NO when backend unavailable, ERR on
 * runtime failures.
 */
AdbxTriStatus secret_store_libsecret_backend_probe(SecretStore **out_store);

#ifdef DUMMY_SECRET_STORE_WARNING
/* Creates a dummy SecretStore implementation. Use this only in test env.
 * Ownership: returned SecretStore owned by caller and must be destroyed.
 * Error semantics: returns NULL on failure.
 */
SecretStore *secret_store_dummy_backend_create(void);
#endif

#endif
