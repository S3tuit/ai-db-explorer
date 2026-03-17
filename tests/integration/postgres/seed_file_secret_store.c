#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config_dir.h"
#include "file_io.h"
#include "secret_store.h"

#define TEST_NAMESPACE "IntegrationPostgres"
#define SSTORE_CFG_FILENAME "secret_store_backend"

typedef struct {
  const char *connection_name;
  const char *secret;
} SecretPair;

static const SecretPair DUMMY_SECRETS[] = {
    {"MyPostgres", "my_secret"},
    {"AnotherPostgres", "another_secret"},
    {"SuperPostgres", "postgres"},
};

/* Writes the backend-selector file expected by secret_store_create().
 */
static AdbxStatus seed_backend_selector(const ConfDir *app) {
  static const uint8_t file_backend[] = "file\n";

  if (!app || app->fd < 0)
    return ERR;

  AdbxTriStatus rc = write_atomic(app->fd, SSTORE_CFG_FILENAME, file_backend,
                                  sizeof(file_backend) - 1, NULL);
  return (rc == YES) ? OK : ERR;
}

/* Wipes and repopulates the integration namespace inside one file-backed
 * secret store. It borrows 'store' and allocates no memory.
 * Side effects: deletes and rewrites secrets under TEST_NAMESPACE.
 */
static AdbxStatus seed_store_entries(SecretStore *store,
                                     SecretStoreErr *out_err) {
  if (!store)
    return ERR;

  if (store->vt->wipe_namespace(store, TEST_NAMESPACE, out_err) != OK)
    return ERR;

  for (size_t i = 0; i < sizeof(DUMMY_SECRETS) / sizeof(DUMMY_SECRETS[0]);
       i++) {
    SecretRefInfo ref = {
        .cred_namespace = TEST_NAMESPACE,
        .connection_name = DUMMY_SECRETS[i].connection_name,
    };
    if (store->vt->set(store, &ref, DUMMY_SECRETS[i].secret, out_err) != OK)
      return ERR;
  }

  return OK;
}

/* Opens the default app dir, pins the backend to the file store, and seeds
 * the integration credentials used by the Postgres Docker tests.
 * It allocates one temporary app error string and one store instance, both
 * freed before returning.
 * Returns 0 on success, 1 on any failure after printing a diagnostic to stderr.
 */
int main(void) {
  ConfDir app = {.fd = -1, .path = NULL};
  char *app_err = NULL;
  SecretStore *store = NULL;
  SecretStoreErr ss_err;
  int exit_code = 1;

  if (confdir_default_open(&app, NULL, &app_err) != OK) {
    fprintf(stderr, "failed to open default app dir: %s\n",
            app_err ? app_err : "unknown error");
    goto out;
  }

  if (seed_backend_selector(&app) != OK) {
    fprintf(stderr, "failed to write %s under %s\n", SSTORE_CFG_FILENAME,
            app.path ? app.path : "<unknown>");
    goto out;
  }

  if (secret_store_file_backend_probe(&store, &ss_err) != YES || !store) {
    fprintf(stderr, "failed to open file secret store: %s\n",
            ss_err.msg[0] ? ss_err.msg : "unknown error");
    goto out;
  }

  if (seed_store_entries(store, &ss_err) != OK) {
    fprintf(stderr, "failed to seed integration secrets: %s\n",
            ss_err.msg[0] ? ss_err.msg : "unknown error");
    goto out;
  }

  exit_code = 0;

out:
  if (store)
    store->vt->destroy(store);
  confdir_clean(&app);
  free(app_err);
  return exit_code;
}
