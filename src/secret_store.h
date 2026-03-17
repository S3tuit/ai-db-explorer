#ifndef SECRET_STORE_H
#define SECRET_STORE_H

#include <stddef.h>
#include <stdint.h>

#include "adbx_err.h"
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
  SecretStoreErrCode code;
  char msg[ADBX_ERRMSG_MAX];
} SecretStoreErr;

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
                       StrBuf *out, SecretStoreErr *out_err);
  // Stores/replace one NUL-terminated secret.
  AdbxStatus (*set)(SecretStore *store, const SecretRefInfo *ref,
                    const char *secret, SecretStoreErr *out_err);
  // Deletes one stored secret.
  AdbxStatus (*delete)(SecretStore *store, const SecretRefInfo *ref,
                       SecretStoreErr *out_err);
  // Deletes all stored secrets in one namespace.
  AdbxStatus (*wipe_namespace)(SecretStore *store, const char *cred_namespace,
                               SecretStoreErr *out_err);
  // Deletes all stored secrets in this store namespace.
  AdbxStatus (*wipe_all)(SecretStore *store, SecretStoreErr *out_err);
  // Destroys the store and releases resources.
  void (*destroy)(SecretStore *store);
};

struct SecretStore {
  const SecretStoreVTable *vt;
};

/* Creates one SecretStore instance using the backend selected for this machine.
 * On success, caller owns the returned store and must destroy it.
 * Returns NULL when no backend can be initialized safely; if 'out_err' is not
 * NULL, it receives one non-allocating typed error snapshot.
 */
SecretStore *secret_store_create(SecretStoreErr *out_err);

/* -------------------------------- HELPERS -------------------------------- */

void secret_store_destroy(SecretStore *store);

AdbxTriStatus secret_store_get(SecretStore *store, const SecretRefInfo *ref,
                               StrBuf *out, SecretStoreErr *out_err);

AdbxStatus secret_store_set(SecretStore *store, const SecretRefInfo *ref,
                            const char *secret, SecretStoreErr *out_err);

AdbxStatus secret_store_delete(SecretStore *store, const SecretRefInfo *ref,
                               SecretStoreErr *out_err);

AdbxStatus secret_store_wipe_namespace(SecretStore *store,
                                       const char *cred_namespace,
                                       SecretStoreErr *out_err);

AdbxStatus secret_store_wipe_all(SecretStore *store, SecretStoreErr *out_err);

/* ---------------------------- SUPPORTED STORES --------------------------- */

/* Probes and creates the file-backed SecretStore.
 * Ownership: on YES, caller owns *out_store and must destroy it.
 * Side effects: may touch filesystem paths and files used by the backend.
 * Error semantics: returns YES on success, ERR on invalid input or
 * initialization failure.
 */
AdbxTriStatus secret_store_file_backend_probe(SecretStore **out_store,
                                              SecretStoreErr *out_err);

/* Probes and creates the macOS Keychain-backed SecretStore.
 * Ownership: on YES, caller owns *out_store and must destroy it.
 * Side effects: may probe OS Keychain services.
 * Error semantics: returns YES on success, NO when backend unavailable, ERR on
 * runtime failures.
 */
AdbxTriStatus secret_store_keychain_backend_probe(SecretStore **out_store,
                                                  SecretStoreErr *out_err);

/* Probes and creates the libsecret-backed SecretStore.
 * Ownership: on YES, caller owns *out_store and must destroy it.
 * Side effects: may probe D-Bus Secret Service endpoints.
 * Error semantics: returns YES on success, NO when backend unavailable, ERR on
 * runtime failures.
 */
AdbxTriStatus secret_store_libsecret_backend_probe(SecretStore **out_store,
                                                   SecretStoreErr *out_err);

#endif
