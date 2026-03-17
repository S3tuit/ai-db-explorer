#include "secret_store.h"

AdbxTriStatus secret_store_keychain_backend_probe(SecretStore **out_store,
                                                  SecretStoreErr *out_err) {
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  if (!out_store)
    return ERR;
  *out_store = NULL;
  ADBX_ERR_SETF(out_err, SSERR_ENV,
                "keychain backend is unavailable on this platform.");
  return NO;
}

// TODO: implement this and add its integration test.
