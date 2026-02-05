#ifndef SECRET_STORE_H
#define SECRET_STORE_H

#include <stddef.h>
#include <stdint.h>

#include "string_op.h"
#include "utils.h"

typedef struct SecretStore SecretStore;
typedef struct SecretStoreVTable SecretStoreVTable;

struct SecretStoreVTable {
  // Writes a NUL-terminated secret into 'out' and returns OK/ERR.
  int (*get)(SecretStore *store, const char *secret_ref, StrBuf *out);
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

// Destroy the store and release resources.
// Ownership: consumes the store pointer.
// Side effects: may release OS secrets later.
// Error semantics: no return value.
void secret_store_destroy(SecretStore *store);

// Lookup a secret by reference key.
// Ownership: caller owns 'out' and should zero+clean it after use.
// Side effects: may access OS secrets later.
// Error semantics: OK on success, ERR on failure.
int secret_store_get(SecretStore *store, const char *secret_ref, StrBuf *out);

/* Small helpers */
static inline int ss_get(SecretStore *store, const char *secret_ref,
                         StrBuf *out) {
  if (!store || !store->vt || !store->vt->get)
    return ERR;
  return store->vt->get(store, secret_ref, out);
}

static inline void ss_destroy(SecretStore *store) {
  if (!store || !store->vt || !store->vt->destroy)
    return;
  store->vt->destroy(store);
}

#endif
