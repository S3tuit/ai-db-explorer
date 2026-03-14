#include "secret_store.h"

#ifndef __linux__

AdbxTriStatus secret_store_libsecret_backend_probe(SecretStore **out_store) {
  if (!out_store)
    return ERR;
  *out_store = NULL;
  return NO;
}

#else /* __linux__ */

#include "string_op.h"
#include "utils.h"

#include <dlfcn.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -------------------- GLib / libsecret type stubs ----------------------- */

typedef struct {
  uint32_t domain;
  int code;
  char *message;
} LsGError;

typedef enum { LS_SCHEMA_NONE = 0 } LsSchemaFlags;
typedef enum { LS_ATTR_STRING = 0 } LsSchemaAttrType;

typedef struct {
  const char *name;
  LsSchemaAttrType type;
} LsSchemaAttr;

typedef struct {
  const char *name;
  LsSchemaFlags flags;
  LsSchemaAttr attributes[32];
  int reserved;
  void *reserved1;
  void *reserved2;
  void *reserved3;
  void *reserved4;
  void *reserved5;
  void *reserved6;
  void *reserved7;
} LsSchema;

/* ----------------------- function-pointer types ------------------------- */

typedef char *(*fn_password_lookup_sync)(const LsSchema *, void *, LsGError **,
                                         ...);
typedef int (*fn_password_store_sync)(const LsSchema *, const char *,
                                      const char *, const char *, void *,
                                      LsGError **, ...);
typedef int (*fn_password_clear_sync)(const LsSchema *, void *, LsGError **,
                                      ...);
typedef void (*fn_password_free)(char *);
typedef void *(*fn_service_get_sync)(int, void *, LsGError **);
typedef void (*fn_object_unref)(void *);
typedef void (*fn_error_free)(LsGError *);

/* ------------------------------- API table ------------------------------ */

static struct {
  fn_password_lookup_sync lookup;
  fn_password_store_sync store;
  fn_password_clear_sync clear;
  fn_password_free pw_free;
  fn_service_get_sync svc_get;
  fn_object_unref obj_unref;
  fn_error_free err_free;
} ls_api;

/* ----------------------------- loader state ----------------------------- */

typedef enum {
  LS_UNTRIED = 0,
  LS_AVAILABLE,
  LS_MISSING,
  LS_BROKEN,
} LsState;

static LsState ls_state = LS_UNTRIED;
static pthread_once_t ls_once = PTHREAD_ONCE_INIT;

/* Loads libsecret-1.so.0 and resolves all required symbols.
 * Also performs a reachability check via secret_service_get_sync() so that
 * LS_AVAILABLE truly means "library present AND service reachable".
 */
static void ls_load(void) {
  void *h = dlopen("libsecret-1.so.0", RTLD_NOW | RTLD_LOCAL);
  if (!h) {
    ls_state = LS_MISSING;
    return;
  }

  /* dlsym through h finds symbols in libsecret and its transitive deps
   * (libglib, libgobject) even with RTLD_LOCAL. */
  ls_api.lookup =
      (fn_password_lookup_sync)dlsym(h, "secret_password_lookup_sync");
  ls_api.store =
      (fn_password_store_sync)dlsym(h, "secret_password_store_sync");
  ls_api.clear =
      (fn_password_clear_sync)dlsym(h, "secret_password_clear_sync");
  ls_api.pw_free = (fn_password_free)dlsym(h, "secret_password_free");
  ls_api.svc_get = (fn_service_get_sync)dlsym(h, "secret_service_get_sync");
  ls_api.obj_unref = (fn_object_unref)dlsym(h, "g_object_unref");
  ls_api.err_free = (fn_error_free)dlsym(h, "g_error_free");

  if (!ls_api.lookup || !ls_api.store || !ls_api.clear || !ls_api.pw_free ||
      !ls_api.svc_get || !ls_api.obj_unref || !ls_api.err_free) {
    ls_state = LS_BROKEN;
    return;
  }

  /* Verify the D-Bus Secret Service is reachable. */
  LsGError *err = NULL;
  void *svc = ls_api.svc_get(0 /* SECRET_SERVICE_NONE */, NULL, &err);
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

/* ------------------------------- schema --------------------------------- */

#define LS_APP_TAG "adbxplorer"

static const LsSchema ls_schema = {
    .name = "com.adbxplorer.secret-store.v1",
    .flags = LS_SCHEMA_NONE,
    .attributes =
        {
            {"app", LS_ATTR_STRING},
            {"credentialNamespace", LS_ATTR_STRING},
            {"connectionName", LS_ATTR_STRING},
            {NULL, 0},
        },
};

/* ----------------------------- backend store ---------------------------- */

typedef struct {
  SecretStore base;
  SecretStoreErrCode last_err_code;
  char last_err_msg[256];
} LibsecretStore;

static void ls_clear_err(LibsecretStore *s) {
  s->last_err_code = SSERR_NONE;
  s->last_err_msg[0] = '\0';
}

static void ls_set_err(LibsecretStore *s, SecretStoreErrCode code,
                        const char *fmt, ...) {
  s->last_err_code = code;
  s->last_err_msg[0] = '\0';
  if (!fmt)
    return;
  va_list ap;
  va_start(ap, fmt);
  (void)vsnprintf(s->last_err_msg, sizeof(s->last_err_msg), fmt, ap);
  va_end(ap);
}

/* Validates a secret reference for backend operations. */
static AdbxStatus ls_validate_ref(LibsecretStore *s, const SecretRefInfo *ref,
                                   const char *op) {
  if (!ref || !ref->cred_namespace || !ref->connection_name) {
    ls_set_err(s, SSERR_INPUT,
               "libsecret %s: invalid input pointers. "
               "This is probably a bug, please, report it.",
               op);
    return ERR;
  }
  if (ref->cred_namespace[0] == '\0' || ref->connection_name[0] == '\0') {
    ls_set_err(s, SSERR_INPUT,
               "libsecret %s: secret reference fields cannot be empty. "
               "This is probably a bug, please, report it.",
               op);
    return ERR;
  }
  return OK;
}

/* ------------------------------ VTable ops ------------------------------ */

static AdbxTriStatus ls_get(SecretStore *base, const SecretRefInfo *ref,
                             StrBuf *out) {
  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (!out) {
    ls_set_err(s, SSERR_INPUT,
               "libsecret get: NULL output buffer. "
               "This is probably a bug, please, report it.");
    return ERR;
  }
  if (ls_validate_ref(s, ref, "get") != OK)
    return ERR;

  LsGError *err = NULL;
  char *pw = ls_api.lookup(&ls_schema, NULL, &err, "app", LS_APP_TAG,
                            "credentialNamespace", ref->cred_namespace,
                            "connectionName", ref->connection_name, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret lookup failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!pw)
    return NO;

  AdbxStatus rc = sb_append_bytes(out, pw, strlen(pw));
  ls_api.pw_free(pw);

  if (rc != OK) {
    ls_set_err(s, SSERR_ENV, "libsecret get: failed to copy secret to buffer");
    return ERR;
  }
  return YES;
}

static AdbxStatus ls_set(SecretStore *base, const SecretRefInfo *ref,
                          const char *secret) {
  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (ls_validate_ref(s, ref, "set") != OK)
    return ERR;
  if (!secret) {
    ls_set_err(s, SSERR_INPUT,
               "libsecret set: NULL secret. "
               "This is probably a bug, please, report it.");
    return ERR;
  }

  char label[256];
  (void)snprintf(label, sizeof(label), "adbxplorer %s/%s", ref->cred_namespace,
                 ref->connection_name);

  LsGError *err = NULL;
  int ok = ls_api.store(&ls_schema, "default" /* SECRET_COLLECTION_DEFAULT */,
                         label, secret, NULL, &err, "app", LS_APP_TAG,
                         "credentialNamespace", ref->cred_namespace,
                         "connectionName", ref->connection_name, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret store failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  if (!ok) {
    ls_set_err(s, SSERR_ENV, "libsecret store returned failure");
    return ERR;
  }
  return OK;
}

static AdbxStatus ls_delete(SecretStore *base, const SecretRefInfo *ref) {
  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (ls_validate_ref(s, ref, "delete") != OK)
    return ERR;

  LsGError *err = NULL;
  (void)ls_api.clear(&ls_schema, NULL, &err, "app", LS_APP_TAG,
                      "credentialNamespace", ref->cred_namespace,
                      "connectionName", ref->connection_name, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret delete failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  return OK;
}

static AdbxStatus ls_wipe_namespace(SecretStore *base,
                                     const char *cred_namespace) {
  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  if (!cred_namespace || cred_namespace[0] == '\0') {
    ls_set_err(s, SSERR_INPUT,
               "libsecret wipe_namespace: namespace cannot be NULL or empty. "
               "This is probably a bug, please, report it.");
    return ERR;
  }

  LsGError *err = NULL;
  (void)ls_api.clear(&ls_schema, NULL, &err, "app", LS_APP_TAG,
                      "credentialNamespace", cred_namespace, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret wipe_namespace failed: %s",
               err->message);
    ls_api.err_free(err);
    return ERR;
  }
  return OK;
}

static AdbxStatus ls_wipe_all(SecretStore *base) {
  LibsecretStore *s = (LibsecretStore *)base;
  ls_clear_err(s);

  LsGError *err = NULL;
  (void)ls_api.clear(&ls_schema, NULL, &err, "app", LS_APP_TAG, NULL);
  if (err) {
    ls_set_err(s, SSERR_ENV, "libsecret wipe_all failed: %s", err->message);
    ls_api.err_free(err);
    return ERR;
  }
  return OK;
}

static void ls_destroy(SecretStore *base) { free(base); }

static const char *ls_last_error(SecretStore *base) {
  return ((LibsecretStore *)base)->last_err_msg;
}

static SecretStoreErrCode ls_last_error_code(SecretStore *base) {
  return ((LibsecretStore *)base)->last_err_code;
}

/* -------------------------------- VTable -------------------------------- */

static const SecretStoreVTable LS_VTABLE = {
    .get = ls_get,
    .set = ls_set,
    .delete = ls_delete,
    .list_refs = NULL,
    .wipe_namespace = ls_wipe_namespace,
    .wipe_all = ls_wipe_all,
    .destroy = ls_destroy,
    .last_error = ls_last_error,
    .last_error_code = ls_last_error_code,
};

/* -------------------------------- probe --------------------------------- */

AdbxTriStatus secret_store_libsecret_backend_probe(SecretStore **out_store) {
  if (!out_store)
    return ERR;
  *out_store = NULL;

  (void)pthread_once(&ls_once, ls_load);

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
