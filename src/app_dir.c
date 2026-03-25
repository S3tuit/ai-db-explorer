#define _GNU_SOURCE

#include "app_dir.h"
#include "file_io.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
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
static void appdir_safe_close(int *fd) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
}

/* Diagnoses why one opened directory fd failed current-user/mode validation.
 * Reads inode metadata from 'dir_fd' and writes one typed error snapshot when
 * 'out_err' is not NULL. Error semantics: none; best-effort diagnostic helper.
 */
static void appdir_describe_dir_validation_failure(int dir_fd,
                                                   const char *path_desc,
                                                   mode_t expected_mode,
                                                   AppDirErr *out_err) {
  if (!out_err)
    return;

  if (!path_desc)
    path_desc = "<unknown>";
  if (dir_fd < 0) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir hit invalid state while validating directory "
                  "'%s'.",
                  path_desc);
    return;
  }

  struct stat st = {0};
  if (fstat(dir_fd, &st) != 0) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                  "failed to stat directory '%s': %s.", path_desc,
                  strerror(errno));
    return;
  }

  if (!S_ISDIR(st.st_mode)) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                  "app-dir path '%s' must be a directory.", path_desc);
    return;
  }
  if (st.st_uid != getuid()) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                  "app-dir path '%s' must be owned by the current user.",
                  path_desc);
    return;
  }
  if ((st.st_mode & 0777) != expected_mode) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                  "app-dir path '%s' has mode %03o and could not be "
                  "repaired to %03o.",
                  path_desc, (unsigned)(st.st_mode & 0777),
                  (unsigned)expected_mode);
    return;
  }

  ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                "app-dir path '%s' failed directory validation.", path_desc);
}

/* Diagnoses why one opened control file fd failed current-user/mode
 * validation. Reads inode metadata from 'file_fd' and writes one typed error
 * snapshot when 'out_err' is not NULL. Error semantics: none; best-effort
 * diagnostic helper.
 */
static void appdir_describe_file_validation_failure(int file_fd,
                                                    const char *path_desc,
                                                    mode_t expected_mode,
                                                    AppDirErr *out_err) {
  if (!out_err)
    return;

  if (!path_desc)
    path_desc = "<unknown>";
  if (file_fd < 0) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir hit invalid state while validating file '%s'.",
                  path_desc);
    return;
  }

  struct stat st = {0};
  if (fstat(file_fd, &st) != 0) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME, "failed to stat file '%s': %s.",
                  path_desc, strerror(errno));
    return;
  }

  if (!S_ISREG(st.st_mode)) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                  "app-dir control path '%s' must be a regular file.",
                  path_desc);
    return;
  }
  if (st.st_uid != getuid()) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                  "app-dir control path '%s' must be owned by the current "
                  "user.",
                  path_desc);
    return;
  }
  if ((st.st_mode & 0777) != expected_mode) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                  "app-dir control path '%s' has mode %03o and could not "
                  "be repaired to %03o.",
                  path_desc, (unsigned)(st.st_mode & 0777),
                  (unsigned)expected_mode);
    return;
  }

  ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                "app-dir control path '%s' failed file validation.", path_desc);
}

/* Returns open(2) flags for directory descriptors that must not follow a final
 * symlink.
 */
static int appdir_dir_open_flags(void) {
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
 */
static int appdir_control_open_flags(void) {
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
 * It borrows 'parent_fd', 'name', and 'path_desc'; on success it returns an
 * owned fd in 'out_fd' that the caller must close.
 * Returns OK on success, ERR on invalid input, metadata
 * failure, symlink/type mismatch, or directory policy violation.
 */
static AdbxStatus appdir_open_or_create_dir_at(int parent_fd, const char *name,
                                               const char *path_desc,
                                               int *out_fd,
                                               AppDirErr *out_err) {
  if (parent_fd < 0 || !name || !out_fd) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir hit invalid input while preparing '%s'.",
                  path_desc ? path_desc : "<unknown>");
    return ERR;
  }

  *out_fd = -1;

  if (mkdirat(parent_fd, name, 0700) != 0) {
    if (errno != EEXIST) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                    "failed to create directory '%s': %s.",
                    path_desc ? path_desc : name, strerror(errno));
      return ERR;
    }
  }

  int fd;
  if ((fd = openat(parent_fd, name, appdir_dir_open_flags())) < 0) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                  "failed to open directory '%s': %s.",
                  path_desc ? path_desc : name, strerror(errno));
    return ERR;
  }

  if (validate_uown_dir(fd, 0700) != OK) {
    appdir_describe_dir_validation_failure(fd, path_desc ? path_desc : name,
                                           0700, out_err);
    appdir_safe_close(&fd);
    return ERR;
  }

  *out_fd = fd;
  return OK;
}

/* Opens one lock file under 'dir_fd' and acquires a non-blocking exclusive
 * broker-instance lock.
 * It borrows 'dir_fd', 'name', and 'path_desc'; on success it returns an owned
 * fd in 'out_fd' that must stay open for the whole broker lifetime.
 * Returns YES on success, NO when another runtime already
 * holds the lock, ERR on invalid input or any filesystem failure.
 */
static AdbxTriStatus appdir_acquire_lock_at(int dir_fd, const char *name,
                                            const char *path_desc, int *out_fd,
                                            AppDirErr *out_err) {
  if (dir_fd < 0 || !name || !out_fd) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir hit invalid input while acquiring lock '%s'.",
                  path_desc ? path_desc : "<unknown>");
    return ERR;
  }

  *out_fd = -1;
  int fd = openat(dir_fd, name, appdir_control_open_flags(), 0600);
  if (fd < 0) {
    if (errno == ELOOP) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                    "app-dir lock path '%s' must not be a symlink.",
                    path_desc ? path_desc : name);
    } else {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                    "failed to open app-dir lock '%s': %s.",
                    path_desc ? path_desc : name, strerror(errno));
    }
    return ERR;
  }

  int rc = ERR; // returned value
  if (validate_uown_file(fd, 0600) != OK) {
    appdir_describe_file_validation_failure(fd, path_desc ? path_desc : name,
                                            0600, out_err);
    goto err_fd;
  }

  struct flock exclusive_lock = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};

  if (fcntl(fd, F_SETLK, &exclusive_lock) < 0) {
    if (errno == EACCES || errno == EAGAIN) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_LOCK,
                    "another broker already owns the app-dir lock '%s'.",
                    path_desc ? path_desc : name);
      rc = NO;
    } else {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_LOCK,
                    "failed to acquire app-dir lock '%s': %s.",
                    path_desc ? path_desc : name, strerror(errno));
    }
    goto err_fd;
  }

  *out_fd = fd;
  return YES;

err_fd:
  appdir_safe_close(&fd);
  return rc;
}

/* Writes one fresh broker shared secret token under 'secret_fd' atomically.
 * It borrows 'secret_fd', 'token', and 'path_desc'; the caller retains token
 * ownership.
 * Side effects: creates or replaces the secret token file inside secret_fd.
 * Error semantics: returns OK on success, ERR on invalid input, random-write
 * contention, or any filesystem failure.
 */
static AdbxStatus appdir_write_token_file(int secret_fd,
                                          const uint8_t token[APPDIR_TOKEN_LEN],
                                          const char *path_desc,
                                          AppDirErr *out_err) {
  if (secret_fd < 0 || !token) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir hit invalid state while writing token '%s'.",
                  path_desc ? path_desc : APPDIR_TOKEN_FILENAME);
    return ERR;
  }

  AdbxTriStatus rc = write_atomic(secret_fd, APPDIR_TOKEN_FILENAME, token,
                                  APPDIR_TOKEN_LEN, NULL);
  if (rc == YES)
    return OK;
  if (rc == NO) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                  "failed to write secret token '%s' because another process "
                  "is concurrently updating it.",
                  path_desc ? path_desc : APPDIR_TOKEN_FILENAME);
    return ERR;
  }
  ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                "failed to write secret token '%s': %s.",
                path_desc ? path_desc : APPDIR_TOKEN_FILENAME, strerror(errno));
  return ERR;
}

/* Removes broker-runtime artifacts under 'app_fd' but leaves the app dir
 * itself in place.
 * It borrows the input fd values and closes any valid descriptors after the
 * best-effort unlinkat cleanup.
 * Error semantics: none (cleanup helper is best-effort).
 */
static void appdir_cleanup_runtime_parts(int app_fd, int run_fd, int secret_fd,
                                         int lock_fd) {
  // removes files inside dirs
  if (run_fd >= 0) {
    (void)unlinkat(run_fd, APPDIR_SOCK_FILENAME, 0);
    appdir_safe_close(&run_fd);
  }
  if (secret_fd >= 0) {
    (void)unlinkat(secret_fd, APPDIR_TOKEN_FILENAME, 0);
    appdir_safe_close(&secret_fd);
  }

  // removes dirs
  if (app_fd >= 0) {
    (void)unlinkat(app_fd, APPDIR_RUN_DIRNAME, AT_REMOVEDIR);
    (void)unlinkat(app_fd, APPDIR_SECRET_DIRNAME, AT_REMOVEDIR);
  }

  // release lock if acquired
  if (app_fd >= 0 && lock_fd >= 0) {
    (void)unlinkat(app_fd, APPDIR_LOCK_FILENAME, 0);
    appdir_safe_close(&lock_fd);
  }

  appdir_safe_close(&app_fd);
}

/* Returns one normalized absolute directory path with exactly one trailing '/'.
 * It borrows 'path' and returns one caller-owned heap string.
 * Side effects: allocates heap memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *appdir_dup_abs_dir(const char *path) {
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

/* Resolves the parent runtime directory from environment variables.
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
    return appdir_dup_abs_dir(env_val);

  if (out_tmp_fallback)
    *out_tmp_fallback = 1;
  return appdir_dup_abs_dir("/tmp");
}

/* Returns the default application directory path for one resolved base
 * directory.
 * It borrows 'base' and returns one caller-owned heap string.
 * Side effects: allocates heap memory.
 * Error semantics: returns NULL on invalid input or snprintf failure.
 */
static char *appdir_build_app_dir(const char *base, int use_tmp_fallback) {
  if (!base)
    return NULL;

  int n = 0;
  if (use_tmp_fallback) {
    n = snprintf(NULL, 0, "%s%s-%u/", base, APPDIR_APP_DIRNAME,
                 (unsigned)getuid());
  } else {
    n = snprintf(NULL, 0, "%s%s/", base, APPDIR_APP_DIRNAME);
  }
  if (n < 0)
    return NULL;

  char *app_dir = xmalloc((size_t)n + 1u);
  if (use_tmp_fallback) {
    if (snprintf(app_dir, (size_t)n + 1u, "%s%s-%u/", base, APPDIR_APP_DIRNAME,
                 (unsigned)getuid()) < 0) {
      free(app_dir);
      return NULL;
    }
  } else {
    if (snprintf(app_dir, (size_t)n + 1u, "%s%s/", base, APPDIR_APP_DIRNAME) <
        0) {
      free(app_dir);
      return NULL;
    }
  }
  return app_dir;
}

/* Creates or opens one app runtime directory at 'app_dir' and validates it as
 * a current-user 0700 directory. It borrows 'app_dir', allocates one
 * temporary trimmed path internally, and returns one owned fd in '*out_fd'.
 * Side effects: may create the directory and opens one fd on success.
 * Error semantics: returns OK on success, ERR on invalid input, parent-missing
 * failures, symlink/type mismatch, or directory policy violation.
 */
static AdbxStatus appdir_open_or_create_app_dir(const char *app_dir,
                                                int *out_fd,
                                                AppDirErr *out_err) {
  if (!app_dir || !out_fd) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir hit invalid input while preparing the app "
                  "directory.");
    return ERR;
  }

  *out_fd = -1;

  size_t len = strlen(app_dir);
  while (len > 1 && app_dir[len - 1] == '/')
    len--;

  char *fs_path = xmalloc(len + 1u);
  memcpy(fs_path, app_dir, len);
  fs_path[len] = '\0';

  struct stat st = {0};
  if (lstat(fs_path, &st) == 0) {
    if (S_ISLNK(st.st_mode)) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_POLICY,
                    "app-dir app path '%s' must not be a symlink.", app_dir);
      free(fs_path);
      return ERR;
    }
    if (!S_ISDIR(st.st_mode)) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_PATH,
                    "app-dir app path '%s' already exists and is not a "
                    "directory.",
                    app_dir);
      free(fs_path);
      return ERR;
    }
  } else if (errno != ENOENT) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                  "failed to inspect app-dir app path '%s': %s.", app_dir,
                  strerror(errno));
    free(fs_path);
    return ERR;
  }

  if (mkdir(fs_path, 0700) != 0 && errno != EEXIST) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                  "failed to create app-dir app path '%s': %s.", app_dir,
                  strerror(errno));
    free(fs_path);
    return ERR;
  }

  int fd = open(fs_path, appdir_dir_open_flags());
  free(fs_path);
  if (fd < 0) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_RUNTIME,
                  "failed to open app-dir app path '%s': %s.", app_dir,
                  strerror(errno));
    return ERR;
  }

  if (validate_uown_dir(fd, 0700) != OK) {
    appdir_describe_dir_validation_failure(fd, app_dir, 0700, out_err);
    appdir_safe_close(&fd);
    return ERR;
  }

  *out_fd = fd;
  return OK;
}

AppDir *appdir_resolve(const char *app_dir_input, AppDirErr *out_err) {
  ADBX_ERR_CLEAR(out_err, APPDIR_ERR_NONE);

  char *app_dir = NULL;
  char *run_dir = NULL;
  char *secret_dir = NULL;
  char *sock_path = NULL;
  char *token_path = NULL;
  int used_tmp_fallback = 0;

  if (!app_dir_input) {
    char *base = resolve_base_path(&used_tmp_fallback);
    if (!base) {
      ADBX_ERR_SETF(
          out_err, APPDIR_ERR_ENV,
          "app-dir failed to resolve the runtime base path. Make sure "
          "XDG_RUNTIME_DIR or the platform equivalent is set.");
      return NULL;
    }
    app_dir = appdir_build_app_dir(base, used_tmp_fallback);
    free(base);
    if (!app_dir) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_ENV,
                    "app-dir failed to build the default app path.");
      return NULL;
    }
  } else {
    if (app_dir_input[0] != '/') {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_PATH,
                    "app-dir '%s' must be an absolute path "
                    "starting with '/'.",
                    app_dir_input);
      return NULL;
    }
    app_dir = appdir_dup_abs_dir(app_dir_input);
    if (!app_dir) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_PATH,
                    "app-dir failed to normalize app-dir '%s'.", app_dir_input);
      return NULL;
    }
    if (strcmp(app_dir, "/") == 0) {
      ADBX_ERR_SETF(out_err, APPDIR_ERR_PATH,
                    "app-dir '%s' must be a non-root absolute "
                    "directory.",
                    app_dir_input);
      goto err;
    }
  }

  size_t run_len = strlen(app_dir) + strlen(APPDIR_RUN_DIRNAME) + 2u;
  run_dir = xmalloc(run_len);
  (void)snprintf(run_dir, run_len, "%s%s/", app_dir, APPDIR_RUN_DIRNAME);

  size_t secret_len = strlen(app_dir) + strlen(APPDIR_SECRET_DIRNAME) + 2u;
  secret_dir = xmalloc(secret_len);
  (void)snprintf(secret_dir, secret_len, "%s%s/", app_dir,
                 APPDIR_SECRET_DIRNAME);

  size_t sock_len = strlen(run_dir) + strlen(APPDIR_SOCK_FILENAME) + 1u;
  sock_path = xmalloc(sock_len);
  (void)snprintf(sock_path, sock_len, "%s%s", run_dir, APPDIR_SOCK_FILENAME);

  struct sockaddr_un sun_check = {0};
  if (strlen(sock_path) >= sizeof(sun_check.sun_path)) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_PATH,
                  "app-dir socket path is too long: %s", sock_path);
    goto err;
  }

  size_t token_len = strlen(secret_dir) + strlen(APPDIR_TOKEN_FILENAME) + 1u;
  token_path = xmalloc(token_len);
  (void)snprintf(token_path, token_len, "%s%s", secret_dir,
                 APPDIR_TOKEN_FILENAME);

  AppDir *appd = xcalloc(1, sizeof(*appd));
  appd->app_dir = app_dir;
  appd->run_dir = run_dir;
  appd->secret_dir = secret_dir;
  appd->sock_path = sock_path;
  appd->token_path = token_path;
  return appd;

err:
  free(app_dir);
  free(run_dir);
  free(secret_dir);
  free(sock_path);
  free(token_path);
  return NULL;
}

AppDirBrokerRuntime *
appdir_broker_runtime_open(const AppDir *appd,
                           uint8_t out_secret_token[APPDIR_TOKEN_LEN],
                           AppDirErr *out_err) {
  ADBX_ERR_CLEAR(out_err, APPDIR_ERR_NONE);
  if (!appd || !appd->app_dir) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_INPUT,
                  "app-dir runtime open received invalid input.");
    return NULL;
  }

  int app_fd = -1;
  int run_fd = -1;
  int secret_fd = -1;
  int lock_fd = -1;

  if (appdir_open_or_create_app_dir(appd->app_dir, &app_fd, out_err) != OK)
    goto err_close;

  char *lock_path = path_join(appd->app_dir, APPDIR_LOCK_FILENAME);
  const char *lock_desc = lock_path ? lock_path : APPDIR_LOCK_FILENAME;
  AdbxTriStatus lock_rc = appdir_acquire_lock_at(app_fd, APPDIR_LOCK_FILENAME,
                                                 lock_desc, &lock_fd, out_err);
  free(lock_path);
  if (lock_rc != YES)
    goto err_close;

  if (appdir_open_or_create_dir_at(app_fd, APPDIR_RUN_DIRNAME, appd->run_dir,
                                   &run_fd, out_err) != OK)
    goto err_clean;
  if (appdir_open_or_create_dir_at(app_fd, APPDIR_SECRET_DIRNAME,
                                   appd->secret_dir, &secret_fd, out_err) != OK)
    goto err_clean;

  uint8_t token[APPDIR_TOKEN_LEN] = {0};
  if (fill_random(token, sizeof(token)) != OK) {
    ADBX_ERR_SETF(out_err, APPDIR_ERR_ENV,
                  "failed to generate one broker secret token.");
    goto err_clean;
  }
  if (appdir_write_token_file(secret_fd, token, appd->token_path, out_err) !=
      OK)
    goto err_clean;

  AppDirBrokerRuntime *rt = xcalloc(1, sizeof(*rt));
  rt->app_fd = app_fd;
  rt->run_fd = run_fd;
  rt->secret_fd = secret_fd;
  rt->lock_fd = lock_fd;

  if (out_secret_token)
    memcpy(out_secret_token, token, sizeof(token));
  return rt;

err_close:
  appdir_safe_close(&lock_fd);
  appdir_safe_close(&run_fd);
  appdir_safe_close(&secret_fd);
  appdir_safe_close(&app_fd);
  return NULL;

err_clean:
  appdir_cleanup_runtime_parts(app_fd, run_fd, secret_fd, lock_fd);
  return NULL;
}

void appdir_broker_runtime_clean(AppDirBrokerRuntime *rt) {
  if (!rt)
    return;
  appdir_cleanup_runtime_parts(rt->app_fd, rt->run_fd, rt->secret_fd,
                               rt->lock_fd);
  free(rt);
}

void appdir_clean(AppDir *appd) {
  if (!appd)
    return;
  free(appd->app_dir);
  free(appd->run_dir);
  free(appd->secret_dir);
  free(appd->sock_path);
  free(appd->token_path);
  free(appd);
}
