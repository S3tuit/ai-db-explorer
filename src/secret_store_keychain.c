#include "secret_store.h"

AdbxTriStatus secret_store_keychain_backend_probe(SecretStore **out_store) {
  if (!out_store)
    return ERR;
  *out_store = NULL;

#if defined(__APPLE__)
  return ERR;
#else
  return NO;
#endif
}

SecretStore *secret_store_keychain_backend_create(void) {
  SecretStore *store = NULL;
  return (secret_store_keychain_backend_probe(&store) == YES) ? store : NULL;
}
