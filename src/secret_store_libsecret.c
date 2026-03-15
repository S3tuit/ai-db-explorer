#include "secret_store.h"

#ifndef __linux__

AdbxTriStatus secret_store_libsecret_backend_probe(SecretStore **out_store) {
  if (!out_store)
    return ERR;
  *out_store = NULL;
  return NO;
}

#else /* __linux__ */

#include <dlfcn.h>
#include <glib.h>
#include <libsecret/secret.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------- function-pointer types ------------------------- */

typedef gchar *(*fn_password_lookup_sync)(const SecretSchema *, GCancellable *,
                                          GError **, ...);
typedef gboolean (*fn_password_store_sync)(const SecretSchema *, const gchar *,
                                           const gchar *, const gchar *,
                                           GCancellable *, GError **, ...);
typedef gboolean (*fn_password_clear_sync)(const SecretSchema *, GCancellable *,
                                           GError **, ...);
typedef GList *(*fn_password_search_sync)(const SecretSchema *,
                                          SecretSearchFlags, GCancellable *,
                                          GError **, ...);
typedef SecretService *(*fn_service_get_sync)(SecretServiceFlags,
                                              GCancellable *, GError **);
typedef void (*fn_password_wipe)(gchar *);
typedef void (*fn_object_unref)(gpointer);
typedef void (*fn_error_free)(GError *);
typedef GHashTable *(*fn_retrievable_get_attributes)(SecretRetrievable *);
typedef gpointer (*fn_hash_table_lookup)(GHashTable *, gconstpointer);
typedef void (*fn_hash_table_unref)(GHashTable *);
typedef void (*fn_list_free)(GList *);

/* ------------------------------- API table ------------------------------ */

typedef struct {
  void *handle;
  fn_password_lookup_sync lookup;
  fn_password_store_sync store;
  fn_password_clear_sync clear;
  fn_password_search_sync search;
  fn_service_get_sync svc_get;
  fn_password_wipe pw_wipe;
  fn_object_unref obj_unref;
  fn_error_free err_free;
  fn_retrievable_get_attributes retrievable_get_attrs;
  fn_hash_table_lookup hash_lookup;
  fn_hash_table_unref hash_unref;
  fn_list_free list_free;
} LsApi;

static LsApi ls_api;

/* ----------------------------- loader state ----------------------------- */

typedef enum {
  LS_UNTRIED = 0,
  LS_AVAILABLE,
  LS_MISSING,
  LS_BROKEN,
} LsState;

static LsState ls_state = LS_UNTRIED;
static pthread_once_t ls_once = PTHREAD_ONCE_INIT;

/* ------------------------------- schema --------------------------------- */

#define LS_APP_TAG "adbxplorer"

static const SecretSchema ls_schema = {
    .name = "com.adbxplorer.secret-store.v1",
    .flags = SECRET_SCHEMA_NONE,
    .attributes =
        {
            {"app", SECRET_SCHEMA_ATTRIBUTE_STRING},
            {"credentialNamespace", SECRET_SCHEMA_ATTRIBUTE_STRING},
            {"connectionName", SECRET_SCHEMA_ATTRIBUTE_STRING},
            {NULL, 0},
        },
};

/* ----------------------------- backend store ---------------------------- */

typedef struct {
  SecretStore base;
  SecretStoreErrCode last_err_code;
  char last_err_msg[256];
} LibsecretStore;

/* Loads libsecret and every required symbol exactly once for the process.
 * It stores the loaded handle and function table in the static globals above;
 * no caller-owned allocations are returned.
 * Side effects: calls dlopen/dlsym, updates global loader state, and probes the
 * default Secret Service with an opened session. This is security-relevant
 * because we only mark the backend available when the service is actually
 * usable, not merely when the shared object exists.
 */
static void ls_load(void) {
  memset(&ls_api, 0, sizeof(ls_api));

  void *h = dlopen("libsecret-1.so.0", RTLD_NOW | RTLD_LOCAL);
  if (!h) {
    ls_state = LS_MISSING;
    return;
  }

  ls_api.handle = h;
  ls_api.lookup =
      (fn_password_lookup_sync)dlsym(h, "secret_password_lookup_sync");
  ls_api.store = (fn_password_store_sync)dlsym(h, "secret_password_store_sync");
  ls_api.clear = (fn_password_clear_sync)dlsym(h, "secret_password_clear_sync");
  ls_api.search =
      (fn_password_search_sync)dlsym(h, "secret_password_search_sync");
  ls_api.svc_get = (fn_service_get_sync)dlsym(h, "secret_service_get_sync");
  ls_api.pw_wipe = (fn_password_wipe)dlsym(h, "secret_password_wipe");
  ls_api.obj_unref = (fn_object_unref)dlsym(h, "g_object_unref");
  ls_api.err_free = (fn_error_free)dlsym(h, "g_error_free");
  ls_api.retrievable_get_attrs = (fn_retrievable_get_attributes)dlsym(
      h, "secret_retrievable_get_attributes");
  ls_api.hash_lookup = (fn_hash_table_lookup)dlsym(h, "g_hash_table_lookup");
  ls_api.hash_unref = (fn_hash_table_unref)dlsym(h, "g_hash_table_unref");
  ls_api.list_free = (fn_list_free)dlsym(h, "g_list_free");

  if (!ls_api.lookup || !ls_api.store || !ls_api.clear || !ls_api.search ||
      !ls_api.svc_get || !ls_api.pw_wipe || !ls_api.obj_unref ||
      !ls_api.err_free || !ls_api.retrievable_get_attrs ||
      !ls_api.hash_lookup || !ls_api.hash_unref || !ls_api.list_free) {
    ls_state = LS_BROKEN;
    return;
  }

  GError *err = NULL;
  SecretService *svc = ls_api.svc_get(SECRET_SERVICE_OPEN_SESSION, NULL, &err);
  if (err) {
    ls_api.err_free(err);
    ls_state = LS_BROKEN;
    return;
  }
  if (!svc) {
    ls_state = LS_BROKEN;
    return;
  }

  ls_api.obj_unref(svc);
  ls_state = LS_AVAILABLE;
}

/* Clears one backend error snapshot before a new operation.
 */
static void ls_clear_err(LibsecretStore *s) {
  if (!s)
    return;
  s->last_err_code = SSERR_NONE;
  s->last_err_msg[0] = '\0';
}

/* Stores one formatted backend error snapshot for diagnostics.
 * It borrows 's' and 'fmt'; it allocates no returned memory.
 */
static void ls_set_err(LibsecretStore *s, SecretStoreErrCode code,
                       const char *fmt, ...) {
  if (!s)
    return;

  s->last_err_code = code;
  s->last_err_msg[0] = '\0';
  if (!fmt)
    return;

  va_list ap;
  va_start(ap, fmt);
  (void)vsnprintf(s->last_err_msg, sizeof(s->last_err_msg), fmt, ap);
  va_end(ap);
}

/* Validates one typed secret reference before a backend operation.
 * It borrows 's', 'ref', and 'op_name'; it allocates no memory.
 * Side effects: updates the backend error snapshot on invalid input.
 * Returns OK on valid namespace+connection-name pairs, ERR on invalid input.
 */
static AdbxStatus ls_validate_ref(LibsecretStore *s, const SecretRefInfo *ref,
                                  const char *op_name) {
  if (!s || !op_name || !ref || !ref->cred_namespace || !ref->connection_name) {
    ls_set_err(s, SSERR_INPUT,
               "libsecret %s failed: invalid input pointers. This is probably "
               "a bug, please, report it.",
               op_name ? op_name : "operation");
    return ERR;
  }

  if (ref->cred_namespace[0] == '\0' || ref->connection_name[0] == '\0') {
    ls_set_err(s, SSERR_INPUT,
               "libsecret %s failed: secret reference fields cannot be empty. "
               "This is probably a bug, please, report it.",
               op_name);
    return ERR;
  }

  return OK;
}

/* Appends one looked-up password into 'out' using the SecretStore get
 * contract. Clears the error of 'base' before execution.
 * It borrows 'base' and 'ref'; 'out' remains caller-owned and is reset by this
 * function before use.
 * Returns YES when the secret exists, NO when it is missing, ERR on failure.
 * failures.
 */
static AdbxTriStatus ls_get(SecretStore *base, const SecretRefInfo *ref,
                            StrBuf *out) {
  if (!base || !out)
    return ERR;

  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (ls_validate_ref(s, ref, "get") != OK)
    return ERR;

  sb_zero_clean(out);
  sb_init(out);

  GError *err = NULL;
  gchar *pw = ls_api.lookup(&ls_schema, NULL, &err, "app", LS_APP_TAG,
                            "credentialNamespace", ref->cred_namespace,
                            "connectionName", ref->connection_name, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret lookup failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!pw)
    return NO;

  size_t n = strlen(pw);
  AdbxStatus rc = sb_append_bytes(out, pw, n + 1);
  ls_api.pw_wipe(pw);

  if (rc != OK) {
    ls_set_err(s, SSERR_WRITE,
               "libsecret get failed: unable to allocate the output buffer. "
               "Please, retry.");
    return ERR;
  }

  return YES;
}

/* Stores or replaces one secret in the default libsecret collection.
 */
static AdbxStatus ls_set(SecretStore *base, const SecretRefInfo *ref,
                         const char *secret) {
  if (!base)
    return ERR;

  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (ls_validate_ref(s, ref, "set") != OK)
    return ERR;
  if (!secret) {
    ls_set_err(s, SSERR_INPUT,
               "libsecret set failed: NULL secret. This is probably a bug, "
               "please, report it.");
    return ERR;
  }

  char label[256];
  (void)snprintf(label, sizeof(label), "adbxplorer %s/%s", ref->cred_namespace,
                 ref->connection_name);

  GError *err = NULL;
  gboolean ok = ls_api.store(&ls_schema, SECRET_COLLECTION_DEFAULT, label,
                             secret, NULL, &err, "app", LS_APP_TAG,
                             "credentialNamespace", ref->cred_namespace,
                             "connectionName", ref->connection_name, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret store failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!ok) {
    ls_set_err(s, SSERR_ENV, "libsecret store returned failure.");
    return ERR;
  }

  return OK;
}

/* Deletes one secret inside 'base' identified by 'ref'.
 */
static AdbxStatus ls_delete(SecretStore *base, const SecretRefInfo *ref) {
  if (!base)
    return ERR;

  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (ls_validate_ref(s, ref, "delete") != OK)
    return ERR;

  GError *err = NULL;
  gboolean ok = ls_api.clear(&ls_schema, NULL, &err, "app", LS_APP_TAG,
                             "credentialNamespace", ref->cred_namespace,
                             "connectionName", ref->connection_name, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret delete failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!ok) {
    ls_set_err(s, SSERR_ENV, "libsecret delete returned failure.");
    return ERR;
  }

  return OK;
}

/* Deletes every adbxplorer-owned secret stored inside 'base' in the namespace
 * 'cred_namespace'. Returns OK on success, ERR on invalid input or libsecret
 * failures.
 */
static AdbxStatus ls_wipe_namespace(SecretStore *base,
                                    const char *cred_namespace) {
  if (!base)
    return ERR;

  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (!cred_namespace || cred_namespace[0] == '\0') {
    ls_set_err(s, SSERR_INPUT,
               "libsecret namespace wipe failed: invalid namespace. This is "
               "probably a bug, please, report it.");
    return ERR;
  }

  GError *err = NULL;
  gboolean ok = ls_api.clear(&ls_schema, NULL, &err, "app", LS_APP_TAG,
                             "credentialNamespace", cred_namespace, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret wipe_namespace failed: %s",
               err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!ok) {
    ls_set_err(s, SSERR_ENV, "libsecret wipe_namespace returned failure.");
    return ERR;
  }

  return OK;
}

/* Deletes every adbxplorer-owned secret across all namespaces.
 * Returns OK on success, ERR on invalid input or libsecret failures.
 */
static AdbxStatus ls_wipe_all(SecretStore *base) {
  if (!base)
    return ERR;

  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  GError *err = NULL;
  gboolean ok = ls_api.clear(&ls_schema, NULL, &err, "app", LS_APP_TAG, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret wipe_all failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!ok) {
    ls_set_err(s, SSERR_ENV, "libsecret wipe_all returned failure.");
    return ERR;
  }

  return OK;
}

/* Destroys one libsecret backend wrapper instance.
 * It consumes 'base', which was allocated by
 * secret_store_libsecret_backend_probe().
 */
static void ls_destroy(SecretStore *base) { free(base); }

/* Returns the last backend error message.
 * It borrows 'base' and returns a pointer owned by the backend wrapper.
 * Returns an empty string on invalid input.
 */
static const char *ls_last_error(SecretStore *base) {
  if (!base)
    return "";
  return ((LibsecretStore *)base)->last_err_msg;
}

/* Returns the last backend error category.
 */
static SecretStoreErrCode ls_last_error_code(SecretStore *base) {
  if (!base)
    return SSERR_NONE;
  return ((LibsecretStore *)base)->last_err_code;
}

static const SecretStoreVTable LS_VTABLE = {
    .get = ls_get,
    .set = ls_set,
    .delete = ls_delete,
    .wipe_namespace = ls_wipe_namespace,
    .wipe_all = ls_wipe_all,
    .destroy = ls_destroy,
    .last_error = ls_last_error,
    .last_error_code = ls_last_error_code,
};

AdbxTriStatus secret_store_libsecret_backend_probe(SecretStore **out_store) {
  if (!out_store)
    return ERR;
  *out_store = NULL;

  if (pthread_once(&ls_once, ls_load) != 0)
    return ERR;

  switch (ls_state) {
  case LS_MISSING:
    return NO;
  case LS_BROKEN:
  case LS_UNTRIED:
    return ERR;
  case LS_AVAILABLE:
    break;
  }

  LibsecretStore *s = (LibsecretStore *)xcalloc(1, sizeof(*s));
  s->base.vt = &LS_VTABLE;
  *out_store = &s->base;
  return YES;
}

#endif /* __linux__ */
