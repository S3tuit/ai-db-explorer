#define _GNU_SOURCE

#include "private_dir.h"
#include "file_io.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

/* Closes '*fd' when valid and resets it to -1.
 * It borrows 'fd' and does not allocate memory.
 * Side effects: closes one kernel file descriptor when present.
 * Error semantics: none (best-effort close helper).
 */
static void privdir_safe_close(int *fd) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
}

/* Writes one formatted error message into '*out_err' once.
 * Ownership: allocates one caller-owned string when formatting succeeds.
 * Side effects: may allocate heap memory.
 * Error semantics: best-effort; leaves '*out_err' unchanged on allocation or
 * formatting failure.
 */
static void privdir_set_err(char **out_err, const char *fmt, ...) {
  if (!out_err || !fmt || *out_err)
    return;

  va_list ap;
  va_start(ap, fmt);
  va_list ap_copy;
  va_copy(ap_copy, ap);
  int n = vsnprintf(NULL, 0, fmt, ap_copy);
  va_end(ap_copy);
  if (n < 0) {
    va_end(ap);
    return;
  }

  char *msg = xmalloc((size_t)n + 1u);
  if (vsnprintf(msg, (size_t)n + 1u, fmt, ap) < 0) {
    free(msg);
    va_end(ap);
    return;
  }
  va_end(ap);
  *out_err = msg;
}

/* Returns open(2) flags for directory descriptors that must not follow a final
 * symlink.
 */
static int privdir_dir_open_flags(void) {
  int flags = O_RDONLY | O_DIRECTORY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  return flags;
}

/* Returns open(2) flags for internal control files that must not follow a
 * final symlink.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int privdir_control_open_flags(void) {
  int flags = O_CREAT | O_RDWR;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  return flags;
}

/* Creates or opens one child directory under 'parent_fd' and verifies the
 * result stays a current-user 0700 directory.
 * It borrows 'parent_fd' and 'name'; on success it returns an owned fd in
 * 'out_fd' that the caller must close.
 * Returns OK on success, ERR on invalid input, metadata
 * failure, symlink/type mismatch, or directory policy violation.
 */
static AdbxStatus privdir_open_or_create_dir_at(int parent_fd, const char *name,
                                                int *out_fd) {
  if (parent_fd < 0 || !name || !out_fd)
    return ERR;

  *out_fd = -1;

  if (mkdirat(parent_fd, name, 0700) != 0) {
    if (errno != EEXIST) {
      return ERR;
    }
  }

  int fd;
  if ((fd = openat(parent_fd, name, privdir_dir_open_flags())) < 0)
    return ERR;

  if (validate_uown_dir(fd, 0700) != OK) {
    privdir_safe_close(&fd);
    return ERR;
  }

  *out_fd = fd;
  return OK;
}

/* Opens one lock file under 'dir_fd' and acquires a non-blocking exclusive
 * broker-instance lock.
 * It borrows 'dir_fd' and 'name'; on success it returns an owned fd in
 * 'out_fd' that must stay open for the whole broker lifetime.
 * Returns YES on success, NO when another runtime already
 * holds the lock, ERR on invalid input or any filesystem failure.
 */
static AdbxTriStatus privdir_acquire_lock_at(int dir_fd, const char *name,
                                             int *out_fd) {
  if (dir_fd < 0 || !name || !out_fd)
    return ERR;

  *out_fd = -1;
  int fd = openat(dir_fd, name, privdir_control_open_flags(), 0600);
  if (fd < 0)
    return ERR;

  int rc = ERR; // returned value
  if (validate_uown_file(fd, 0600) != OK) {
    goto err_fd;
  }

  struct flock exclusive_lock = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};

  if (fcntl(fd, F_SETLK, &exclusive_lock) < 0) {
    if (errno == EACCES || errno == EAGAIN) {
      rc = NO;
    }
    goto err_fd;
  }

  *out_fd = fd;
  return YES;

err_fd:
  privdir_safe_close(&fd);
  return rc;
}

/* Writes one fresh broker shared secret token under 'secret_fd' atomically.
 * It borrows 'secret_fd' and 'token'; the caller retains token ownership.
 * Side effects: creates or replaces the secret token file inside secret_fd.
 * Error semantics: returns OK on success, ERR on invalid input, random-write
 * contention, or any filesystem failure.
 */
static AdbxStatus
privdir_write_token_file(int secret_fd,
                         const uint8_t token[PRIVDIR_TOKEN_LEN]) {
  if (secret_fd < 0 || !token) {
    errno = EINVAL;
    return ERR;
  }

  AdbxTriStatus rc = write_atomic(secret_fd, PRIVDIR_TOKEN_FILENAME, token,
                                  PRIVDIR_TOKEN_LEN, NULL);
  if (rc == YES)
    return OK;
  if (rc == NO)
    errno = EWOULDBLOCK;
  return ERR;
}

/* Removes broker-runtime artifacts under 'app_fd' but leaves the app dir
 * itself in place.
 * It borrows the input fd values and closes any valid descriptors after the
 * best-effort unlinkat cleanup.
 * Error semantics: none (cleanup helper is best-effort).
 */
static void privdir_cleanup_runtime_parts(int base_fd, int app_fd, int run_fd,
                                          int secret_fd, int lock_fd) {
  // removes files inside dirs
  if (run_fd >= 0) {
    (void)unlinkat(run_fd, PRIVDIR_SOCK_FILENAME, 0);
    privdir_safe_close(&run_fd);
  }
  if (secret_fd >= 0) {
    (void)unlinkat(secret_fd, PRIVDIR_TOKEN_FILENAME, 0);
    privdir_safe_close(&secret_fd);
  }

  // removes dirs
  if (app_fd >= 0) {
    (void)unlinkat(app_fd, PRIVDIR_RUN_DIRNAME, AT_REMOVEDIR);
    (void)unlinkat(app_fd, PRIVDIR_SECRET_DIRNAME, AT_REMOVEDIR);
  }

  // release lock if acquired
  if (app_fd >= 0 && lock_fd >= 0) {
    (void)unlinkat(app_fd, PRIVDIR_LOCK_FILENAME, 0);
    privdir_safe_close(&lock_fd);
  }

  privdir_safe_close(&app_fd);
  privdir_safe_close(&base_fd);
}

/* Returns one normalized absolute directory path with exactly one trailing '/'.
 * It borrows 'path' and returns one caller-owned heap string.
 * Side effects: allocates heap memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *privdir_dup_abs_dir(const char *path) {
  if (!path || path[0] != '/')
    return NULL;

  size_t len = strlen(path);
  while (len > 1 && path[len - 1] == '/')
    len--;

  int needs_slash = (len == 0 || path[len - 1] != '/');
  size_t out_len = len + (needs_slash ? 1u : 0u);
  char *out = xmalloc(out_len + 1u);
  memcpy(out, path, len);
  if (needs_slash)
    out[len++] = '/';
  out[len] = '\0';
  return out;
}

/* Resolves the parent base directory from environment variables.
 * It allocates and returns one caller-owned absolute path string and reports
 * whether the /tmp/ fallback was used in 'out_tmp_fallback'.
 * Side effects: reads process environment and allocates heap memory.
 * Error semantics: returns NULL only on unexpected allocation/formatting
 * failure.
 */
static char *resolve_base_path(int *out_tmp_fallback) {
  const char *env_val = NULL;
  if (out_tmp_fallback)
    *out_tmp_fallback = 0;

#ifdef __linux__
  env_val = getenv("XDG_RUNTIME_DIR");
#elif defined(__APPLE__)
  env_val = getenv("TMPDIR");
#endif

  if (env_val && env_val[0] == '/')
    return privdir_dup_abs_dir(env_val);

  if (out_tmp_fallback)
    *out_tmp_fallback = 1;
  return privdir_dup_abs_dir("/tmp");
}

/* Returns the application directory path for one resolved base directory.
 * It borrows 'base' and returns one caller-owned heap string.
 * Side effects: allocates heap memory.
 * Error semantics: returns NULL on invalid input or snprintf failure.
 */
static char *privdir_build_app_dir(const char *base, int use_tmp_fallback) {
  if (!base)
    return NULL;

  int n = 0;
  if (use_tmp_fallback) {
    n = snprintf(NULL, 0, "%s%s-%u/", base, PRIVDIR_APP_DIRNAME,
                 (unsigned)getuid());
  } else {
    n = snprintf(NULL, 0, "%s%s/", base, PRIVDIR_APP_DIRNAME);
  }
  if (n < 0)
    return NULL;

  char *app_dir = xmalloc((size_t)n + 1u);
  if (use_tmp_fallback) {
    if (snprintf(app_dir, (size_t)n + 1u, "%s%s-%u/", base, PRIVDIR_APP_DIRNAME,
                 (unsigned)getuid()) < 0) {
      free(app_dir);
      return NULL;
    }
  } else {
    if (snprintf(app_dir, (size_t)n + 1u, "%s%s/", base, PRIVDIR_APP_DIRNAME) <
        0) {
      free(app_dir);
      return NULL;
    }
  }
  return app_dir;
}

/* Extracts the last path component from one resolved app dir path.
 * It borrows 'pd' and returns one caller-owned heap string in '*out_name'.
 * Side effects: allocates heap memory.
 * Error semantics: returns OK on success, ERR on invalid or inconsistent input.
 */
static AdbxStatus privdir_dup_app_leaf_name(const PrivDir *pd,
                                            char **out_name) {
  if (!pd || !pd->base || !pd->app_dir || !out_name)
    return ERR;

  *out_name = NULL;
  size_t base_len = strlen(pd->base);
  size_t app_len = strlen(pd->app_dir);
  if (app_len <= base_len + 1u)
    return ERR;
  if (strncmp(pd->base, pd->app_dir, base_len) != 0)
    return ERR;
  if (pd->app_dir[app_len - 1] != '/')
    return ERR;

  size_t leaf_len = app_len - base_len - 1u;
  const char *leaf = pd->app_dir + base_len;
  if (leaf_len == 0 || memchr(leaf, '/', leaf_len) != NULL)
    return ERR;

  char *name = xmalloc(leaf_len + 1u);
  memcpy(name, leaf, leaf_len);
  name[leaf_len] = '\0';
  *out_name = name;
  return OK;
}

PrivDir *privdir_resolve(const char *input_base, char **out_err) {
  if (out_err)
    *out_err = NULL;

  char *base = NULL;
  char *app_dir = NULL;
  char *run_dir = NULL;
  char *secret_dir = NULL;
  char *sock_path = NULL;
  char *token_path = NULL;
  int used_tmp_fallback = 0;

  if (!input_base) {
    base = resolve_base_path(&used_tmp_fallback);
    if (!base) {
      privdir_set_err(out_err,
                      "private-dir: failed to resolve the base path. Make sure "
                      "to set XDG_RUNTIME_DIR or equivalent in your env.");
      return NULL;
    }
  } else {
    if (input_base[0] != '/') {
      privdir_set_err(out_err, "private-dir: invalid path in input, expected "
                               "an absolute path starting with '/'.");
      return NULL;
    }
    base = privdir_dup_abs_dir(input_base);
    if (!base) {
      privdir_set_err(out_err, "failed to normalize private-dir base: %s",
                      input_base);
      return NULL;
    }
  }

  app_dir = privdir_build_app_dir(base, used_tmp_fallback);
  if (!app_dir) {
    privdir_set_err(out_err, "failed to build private-dir app path at: %s",
                    base);
    goto err;
  }

  size_t run_len = strlen(app_dir) + strlen(PRIVDIR_RUN_DIRNAME) + 2u;
  run_dir = xmalloc(run_len);
  (void)snprintf(run_dir, run_len, "%s%s/", app_dir, PRIVDIR_RUN_DIRNAME);

  size_t secret_len = strlen(app_dir) + strlen(PRIVDIR_SECRET_DIRNAME) + 2u;
  secret_dir = xmalloc(secret_len);
  (void)snprintf(secret_dir, secret_len, "%s%s/", app_dir,
                 PRIVDIR_SECRET_DIRNAME);

  size_t sock_len = strlen(run_dir) + strlen(PRIVDIR_SOCK_FILENAME) + 1u;
  sock_path = xmalloc(sock_len);
  (void)snprintf(sock_path, sock_len, "%s%s", run_dir, PRIVDIR_SOCK_FILENAME);

  struct sockaddr_un sun_check = {0};
  if (strlen(sock_path) >= sizeof(sun_check.sun_path)) {
    privdir_set_err(out_err, "private-dir socket path is too long: %s",
                    sock_path);
    goto err;
  }

  size_t token_len = strlen(secret_dir) + strlen(PRIVDIR_TOKEN_FILENAME) + 1u;
  token_path = xmalloc(token_len);
  (void)snprintf(token_path, token_len, "%s%s", secret_dir,
                 PRIVDIR_TOKEN_FILENAME);

  PrivDir *pd = xcalloc(1, sizeof(*pd));
  pd->base = base;
  pd->app_dir = app_dir;
  pd->run_dir = run_dir;
  pd->secret_dir = secret_dir;
  pd->sock_path = sock_path;
  pd->token_path = token_path;
  return pd;

err:
  free(base);
  free(app_dir);
  free(run_dir);
  free(secret_dir);
  free(sock_path);
  free(token_path);
  return NULL;
}

PrivDirBrokerRuntime *
privdir_broker_runtime_open(const PrivDir *pd,
                            uint8_t out_secret_token[PRIVDIR_TOKEN_LEN]) {
  if (!pd || !pd->base || !pd->app_dir)
    return NULL;

  int base_fd = -1;
  int app_fd = -1;
  int run_fd = -1;
  int secret_fd = -1;
  int lock_fd = -1;
  char *app_name = NULL;

  base_fd = open(pd->base, privdir_dir_open_flags());
  if (base_fd < 0)
    goto err_close;

  if (privdir_dup_app_leaf_name(pd, &app_name) != OK) {
    errno = EINVAL;
    goto err_close;
  }

  if (privdir_open_or_create_dir_at(base_fd, app_name, &app_fd) != OK)
    goto err_close;

  AdbxTriStatus lock_rc =
      privdir_acquire_lock_at(app_fd, PRIVDIR_LOCK_FILENAME, &lock_fd);
  if (lock_rc != YES)
    goto err_close;

  if (privdir_open_or_create_dir_at(app_fd, PRIVDIR_RUN_DIRNAME, &run_fd) != OK)
    goto err_clean;
  if (privdir_open_or_create_dir_at(app_fd, PRIVDIR_SECRET_DIRNAME,
                                    &secret_fd) != OK)
    goto err_clean;

  uint8_t token[PRIVDIR_TOKEN_LEN] = {0};
  if (fill_random(token, sizeof(token)) != OK)
    goto err_clean;
  if (privdir_write_token_file(secret_fd, token) != OK)
    goto err_clean;

  PrivDirBrokerRuntime *rt = xcalloc(1, sizeof(*rt));
  rt->base_fd = base_fd;
  rt->app_fd = app_fd;
  rt->run_fd = run_fd;
  rt->secret_fd = secret_fd;
  rt->lock_fd = lock_fd;

  if (out_secret_token)
    memcpy(out_secret_token, token, sizeof(token));
  free(app_name);
  return rt;

err_close:
  free(app_name);
  privdir_safe_close(&lock_fd);
  privdir_safe_close(&run_fd);
  privdir_safe_close(&secret_fd);
  privdir_safe_close(&app_fd);
  privdir_safe_close(&base_fd);
  return NULL;

err_clean:
  free(app_name);
  privdir_cleanup_runtime_parts(base_fd, app_fd, run_fd, secret_fd, lock_fd);
  return NULL;
}

void privdir_broker_runtime_clean(PrivDirBrokerRuntime *rt) {
  if (!rt)
    return;
  privdir_cleanup_runtime_parts(rt->base_fd, rt->app_fd, rt->run_fd,
                                rt->secret_fd, rt->lock_fd);
  free(rt);
}

void privdir_clean(PrivDir *pd) {
  if (!pd)
    return;
  free(pd->base);
  free(pd->app_dir);
  free(pd->run_dir);
  free(pd->secret_dir);
  free(pd->sock_path);
  free(pd->token_path);
  free(pd);
}
