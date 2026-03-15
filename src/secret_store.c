#include "secret_store.h"

#include "config_dir.h"
#include "file_io.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
  ConfDir app;
  int l_fd;
} SstoreLock;

typedef enum {
  SSTORE_KIND_NONE = 0,
  SSTORE_KIND_LIBSECRET,
  SSTORE_KIND_KEYCHAIN,
  SSTORE_KIND_FILE,
} SstoreKind;

#define SSTORE_CFG_FILENAME "secret_store_backend"
#define SSTORE_LOCK_FILENAME "secret_store_backend.lock"
#define SSTORE_CFG_MAX_BYTES 32u

/* Writes one formatted error into '*out_err' once. Leaves '*out_err' unchanged
 * on allocation failure or when it already contains a message.
 */
static void sstore_set_err(char **out_err, const char *fmt, ...) {
  if (!out_err || !fmt || *out_err)
    return;

  char tmp[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
  va_end(ap);
  if (n < 0)
    return;

  size_t len = (size_t)n;
  if (len >= sizeof(tmp))
    len = sizeof(tmp) - 1;

  char *msg = (char *)xmalloc(len + 1);
  memcpy(msg, tmp, len);
  msg[len] = '\0';
  *out_err = msg;
}

/* Returns the persisted token associated to one backend kind.
 * Returns NULL for invalid/unsupported enum values.
 */
static const char *sstore_kind_name(SstoreKind kind) {
  switch (kind) {
  case SSTORE_KIND_LIBSECRET:
    return "libsecret";
  case SSTORE_KIND_KEYCHAIN:
    return "keychain";
  case SSTORE_KIND_FILE:
    return "file";
  default:
    return NULL;
  }
}

/* Parses 's' into an enum kind and assigns it to 'out_kind'.
 * Returns OK on exact token match, ERR on invalid input or unknown tokens.
 */
static AdbxStatus sstore_str_to_kind(const char *s, SstoreKind *out_kind) {
  if (!s || !out_kind)
    return ERR;

  if (strcmp(s, "libsecret") == 0) {
    *out_kind = SSTORE_KIND_LIBSECRET;
    return OK;
  }
  if (strcmp(s, "keychain") == 0) {
    *out_kind = SSTORE_KIND_KEYCHAIN;
    return OK;
  }
  if (strcmp(s, "file") == 0) {
    *out_kind = SSTORE_KIND_FILE;
    return OK;
  }
  return ERR;
}

/* Acquires the backend-selection lock under the default app dir.
 * It writes one owned app-dir handle and one lock fd into 'out_lock'; caller
 * must later call sstore_release_lock().
 * Returns OK on success, else, ERR and modifies 'out_err' if not NULL.
 */
static AdbxStatus sstore_acquire_lock(SstoreLock *out_lock, char **out_err) {
  if (!out_lock) {
    sstore_set_err(out_err,
                   "secret store hit an unsafe state during backend selection "
                   "lock acquisition. This is probably a bug, please, report "
                   "it.");
    return ERR;
  }

  int l_fd = -1;
  ConfDir app = {.fd = -1, .path = NULL};

  char *app_err = NULL;
  if (confdir_default_open(&app, NULL, &app_err) != OK) {
    sstore_set_err(out_err,
                   "secret store failed to open the default app directory: %s",
                   app_err ? app_err : "unknown error");
    free(app_err);
    return ERR;
  }

  int flags = O_CREAT | O_WRONLY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif

  l_fd = openat(app.fd, SSTORE_LOCK_FILENAME, flags, 0600);
  if (l_fd < 0) {
    sstore_set_err(out_err,
                   "failed to open secret-store backend lock file at %s/%s: %s",
                   app.path, SSTORE_LOCK_FILENAME, strerror(errno));
    goto err;
  }

  struct flock exclusive_lock = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
  if (fcntl(l_fd, F_SETLK, &exclusive_lock) < 0) {
    if (errno == EACCES || errno == EAGAIN) {
      sstore_set_err(out_err, "another process is selecting the secret-store "
                              "backend. Please, retry.");
    } else {
      sstore_set_err(out_err,
                     "failed to acquire secret-store backend lock at %s/%s: %s",
                     app.path, SSTORE_LOCK_FILENAME, strerror(errno));
    }
    goto err;
  }

  out_lock->app = app;
  out_lock->l_fd = l_fd;
  return OK;

err:
  if (l_fd >= 0)
    close(l_fd);
  confdir_clean(&app);
  return ERR;
}

/* Releases one backend-selection lock acquired by sstore_acquire_lock().
 */
static void sstore_release_lock(SstoreLock *lock) {
  if (!lock)
    return;

  if (lock->app.fd >= 0)
    (void)unlinkat(lock->app.fd, SSTORE_LOCK_FILENAME, 0);
  if (lock->l_fd >= 0) {
    (void)close(lock->l_fd);
    lock->l_fd = -1;
  }
  confdir_clean(&lock->app);
}

/* Reads one persisted backend choice from 'dir' and writes the parsed enum into
 * '*out_kind'. Returns YES when the persisted file exists and is valid, NO when
 * it is missing, ERR on invalid input, file-policy failures, I/O errors, or
 * malformed contents.
 */
static AdbxTriStatus sstore_read_pinned_kind(ConfDir *dir, SstoreKind *out_kind,
                                             char **out_err) {
  if (!dir || dir->fd < 0 || !dir->path || !out_kind)
    return ERR;

  int flags = O_RDONLY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif

  int fd = openat(dir->fd, SSTORE_CFG_FILENAME, flags);
  if (fd < 0) {
    if (errno == ENOENT)
      return NO;
    sstore_set_err(out_err,
                   "failed to open the secret-store backend configuration at "
                   "%s/%s: %s",
                   dir->path, SSTORE_CFG_FILENAME, strerror(errno));
    return ERR;
  }

  AdbxTriStatus rc = ERR; // what gets returned
  StrBuf sb;
  sb_init(&sb);

  if (validate_uown_file(fd, 0600) != OK) {
    sstore_set_err(out_err,
                   "secret-store backend configuration at %s/%s violates the "
                   "expected owner/permission policy.",
                   dir->path, SSTORE_CFG_FILENAME);
    goto cleanup;
  }

  if (fileio_sb_read_limit_fd(fd, SSTORE_CFG_MAX_BYTES, &sb) != OK) {
    sstore_set_err(out_err,
                   "failed to read the secret-store backend configuration at "
                   "%s/%s: %s",
                   dir->path, SSTORE_CFG_FILENAME, strerror(errno));
    goto cleanup;
  }

  char *kind_txt = sb.data;

  while (sb.len > 0 &&
         (kind_txt[sb.len - 1] == '\n' || kind_txt[sb.len - 1] == '\r')) {
    kind_txt[--sb.len] = '\0';
  }

  if (sstore_str_to_kind(kind_txt, out_kind) != OK) {
    sstore_set_err(out_err,
                   "secret-store backend configuration at %s/%s is malformed. "
                   "Expected one of: file, keychain, libsecret.",
                   dir->path, SSTORE_CFG_FILENAME);
    goto cleanup;
  }

  rc = YES;

cleanup:
  sb_zero_clean(&sb);
  (void)close(fd);
  return rc;
}

/* Persists the string representation of 'kind' into the configuration file
 * inside 'dir' as a single-line file. It borrows 'dir' and allocates no
 * returned storage.
 */
static AdbxStatus sstore_write_pinned_kind(ConfDir *dir, SstoreKind kind,
                                           char **out_err) {
  if (!dir || dir->fd < 0 || !dir->path) {
    sstore_set_err(out_err,
                   "secret store hit an unsafe state while persisting the "
                   "selected backend. This is probably a bug, please, report "
                   "it.");
    return ERR;
  }

  const char *kind_name = sstore_kind_name(kind);
  if (!kind_name) {
    sstore_set_err(out_err,
                   "secret store hit an unsupported backend selection. This is "
                   "probably a bug, please, report it.");
    return ERR;
  }

  char buf[32];
  // mimic linux, append a new line at the end
  int n = snprintf(buf, sizeof(buf), "%s\n", kind_name);
  if (n <= 0 || (size_t)n >= sizeof(buf)) {
    sstore_set_err(out_err,
                   "secret store failed to serialize the selected backend. "
                   "This is probably a bug, please, report it.");
    return ERR;
  }

  AdbxTriStatus wrc = write_atomic(dir->fd, SSTORE_CFG_FILENAME,
                                   (const uint8_t *)buf, (size_t)n, NULL);
  if (wrc != YES) {
    sstore_set_err(
        out_err,
        "failed to persist the selected secret-store backend at %s/%s: %s",
        dir->path, SSTORE_CFG_FILENAME,
        (wrc == NO) ? "lock contention" : strerror(errno));
    return ERR;
  }

  return OK;
}

/* Opens one specific backend kind and returns an initialized store.
 * It writes one caller-owned SecretStore into '*out_store'.
 * May initialize OS- or filesystem-backed secret-store state via
 * the selected backend probe callback.
 */
static AdbxStatus sstore_open_selected_kind(SstoreKind kind,
                                            SecretStore **out_store,
                                            char **out_err) {
  if (!out_store) {
    sstore_set_err(out_err,
                   "secret store hit an unsafe state while opening the "
                   "selected backend. This is probably a bug, please, report "
                   "it.");
    return ERR;
  }

  *out_store = NULL;

  AdbxTriStatus rc = ERR;
  const char *kind_name = sstore_kind_name(kind);
  switch (kind) {
  case SSTORE_KIND_LIBSECRET:
    rc = secret_store_libsecret_backend_probe(out_store);
    break;
  case SSTORE_KIND_KEYCHAIN:
    rc = secret_store_keychain_backend_probe(out_store);
    break;
  case SSTORE_KIND_FILE:
    rc = secret_store_file_backend_probe(out_store);
    break;
  default:
    sstore_set_err(out_err,
                   "secret store hit an unsupported persisted backend. This is "
                   "probably a bug, please, report it.");
    return ERR;
  }

  if (rc == YES)
    return OK;
  if (rc == NO) {
    sstore_set_err(out_err,
                   "the configured secret-store backend '%s' is unavailable in "
                   "this environment.",
                   kind_name ? kind_name : "unknown");
    return ERR;
  }

  sstore_set_err(out_err,
                 "the configured secret-store backend '%s' failed to "
                 "initialize.",
                 kind_name ? kind_name : "unknown");
  return ERR;
}

/* Chooses the first available backend for a machine on first run.
 * It writes the selected enum into '*out_kind' and the initialized backend
 * into '*out_store'; caller owns '*out_store' on success.
 * Side effects: may initialize one or more backends while probing candidates.
 */
static AdbxStatus sstore_choose_initial_store(SstoreKind *out_kind,
                                              SecretStore **out_store,
                                              char **out_err) {
  if (!out_kind || !out_store) {
    sstore_set_err(out_err,
                   "secret store hit an unsafe state while choosing the first "
                   "backend. This is probably a bug, please, report it.");
    return ERR;
  }

  *out_store = NULL;
  *out_kind = SSTORE_KIND_NONE;

#if defined(__APPLE__)
  const SstoreKind candidates[] = {SSTORE_KIND_KEYCHAIN, SSTORE_KIND_FILE};
#elif defined(HAVE_LIBSECRET)
  const SstoreKind candidates[] = {SSTORE_KIND_LIBSECRET, SSTORE_KIND_FILE};
#else
  const SstoreKind candidates[] = {SSTORE_KIND_FILE};
#endif

  for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
    SstoreKind kind = candidates[i];
    SecretStore *store = NULL;
    AdbxTriStatus rc = ERR;

    switch (kind) {
    case SSTORE_KIND_LIBSECRET:
      rc = secret_store_libsecret_backend_probe(&store);
      break;
    case SSTORE_KIND_KEYCHAIN:
      rc = secret_store_keychain_backend_probe(&store);
      break;
    case SSTORE_KIND_FILE:
      rc = secret_store_file_backend_probe(&store);
      break;
    default:
      rc = ERR;
      break;
    }

    if (rc == YES) {
      *out_kind = kind;
      *out_store = store;
      return OK;
    }
    if (rc == ERR) {
      sstore_set_err(
          out_err,
          "preferred secret-store backend '%s' failed to initialize; refusing "
          "to silently fall back to a different backend.",
          sstore_kind_name(kind));
      return ERR;
    }
  }

  sstore_set_err(out_err,
                 "no supported secret-store backend is available in this "
                 "environment.");
  return ERR;
}

/* Resolves the backend kind for this machine and, on first run, persists the
 * choice under the app dir. On first-run success it may also return an already
 * opened store in '*out_store' to avoid probing twice.
 * It writes one enum into '*out_kind' and one optional caller-owned store into
 * '*out_store'.
 * Returns OK on success, ERR on invalid input, lock/app-dir failures, malformed
 * persisted state, backend-probe failures, or persist failures.
 */
static AdbxStatus sstore_resolve_backend(SstoreKind *out_kind,
                                         SecretStore **out_store,
                                         char **out_err) {
  if (!out_kind || !out_store) {
    sstore_set_err(out_err,
                   "secret store hit an unsafe state while resolving the "
                   "backend. This is probably a bug, please, report it.");
    return ERR;
  }

  *out_kind = SSTORE_KIND_NONE;
  *out_store = NULL;

  SstoreKind kind = SSTORE_KIND_NONE;
  ConfDir app = {.fd = -1, .path = NULL};
  char *app_err = NULL;
  if (confdir_default_open(&app, NULL, &app_err) != OK) {
    sstore_set_err(out_err,
                   "secret store failed to open the default app directory: %s",
                   app_err ? app_err : "unknown error");
    free(app_err);
    return ERR;
  }

  // first, we try to read the persisted secret store that will be present if
  // this is not the first time the system runs on this machine
  AdbxTriStatus read_rc = sstore_read_pinned_kind(&app, &kind, out_err);
  confdir_clean(&app);
  if (read_rc == YES) {
    *out_kind = kind;
    return OK;
  }
  if (read_rc == ERR)
    return ERR;

  // we suppose this is the first time the system runs on this machine so we try
  // to identify the secret store to use from now on
  SstoreLock lock = {.app = {.fd = -1, .path = NULL}, .l_fd = -1};
  if (sstore_acquire_lock(&lock, out_err) != OK)
    return ERR;

  AdbxStatus rc = ERR;

  SecretStore *store = NULL;
  if (sstore_choose_initial_store(&kind, &store, out_err) != OK)
    goto cleanup;

  if (sstore_write_pinned_kind(&lock.app, kind, out_err) != OK) {
    secret_store_destroy(store);
    goto cleanup;
  }

  *out_kind = kind;
  *out_store = store;
  rc = OK;

cleanup:
  sstore_release_lock(&lock);
  return rc;
}

SecretStore *secret_store_create(char **out_err) {
#ifdef DUMMY_SECRET_STORE_WARNING
  (void)out_err;
  return secret_store_dummy_backend_create();
#endif

  SecretStore *store = NULL;
  SstoreKind kind = SSTORE_KIND_NONE;
  if (sstore_resolve_backend(&kind, &store, out_err) != OK)
    return NULL;

  if (store)
    return store;

  if (sstore_open_selected_kind(kind, &store, out_err) != OK)
    return NULL;

  return store;
}

void secret_store_destroy(SecretStore *store) {
  if (!store || !store->vt || !store->vt->destroy)
    return;
  store->vt->destroy(store);
}

AdbxTriStatus secret_store_get(SecretStore *store, const SecretRefInfo *ref,
                               StrBuf *out) {
  if (!store || !store->vt || !store->vt->get)
    return ERR;
  return store->vt->get(store, ref, out);
}

AdbxStatus secret_store_set(SecretStore *store, const SecretRefInfo *ref,
                            const char *secret) {
  if (!store || !store->vt || !store->vt->set)
    return ERR;
  return store->vt->set(store, ref, secret);
}

AdbxStatus secret_store_delete(SecretStore *store, const SecretRefInfo *ref) {
  if (!store || !store->vt || !store->vt->delete)
    return ERR;
  return store->vt->delete(store, ref);
}

AdbxStatus secret_store_wipe_namespace(SecretStore *store,
                                       const char *cred_namespace) {
  if (!store || !store->vt || !store->vt->wipe_namespace)
    return ERR;
  return store->vt->wipe_namespace(store, cred_namespace);
}

AdbxStatus secret_store_wipe_all(SecretStore *store) {
  if (!store || !store->vt || !store->vt->wipe_all)
    return ERR;
  return store->vt->wipe_all(store);
}

const char *secret_store_last_error(SecretStore *store) {
  if (!store || !store->vt || !store->vt->last_error)
    return "";
  const char *msg = store->vt->last_error(store);
  return msg ? msg : "";
}

SecretStoreErrCode secret_store_last_error_code(SecretStore *store) {
  if (!store || !store->vt || !store->vt->last_error_code)
    return SSERR_NONE;
  return store->vt->last_error_code(store);
}
