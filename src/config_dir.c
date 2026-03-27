#include "config_dir.h"

#include "file_io.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CFG_FILENAME "config.json"

/* Writes one formatted error message into '*out_err' once.
 * It borrows all inputs and allocates one heap string owned by caller.
 * Side effects: may allocate memory.
 * Error semantics: best-effort; leaves '*out_err' unchanged on allocation
 * failure.
 */
static void cfg_set_err(char **out_err, const char *fmt, ...) {
  if (!out_err || !fmt || *out_err)
    return;

  char tmp[256];
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

/* Stores one config-dir error code when the caller asked for it.
 * It borrows 'out_code' and performs no allocations.
 * Side effects: writes one enum value.
 * Error semantics: none.
 */
static inline void cfg_set_code(ConfDirErrCode *out_code, ConfDirErrCode code) {
  if (!out_code)
    return;
  *out_code = code;
}

/* Returns YES when 'path' is an absolute path.
 * It borrows input and performs no allocations.
 * Error semantics: returns NO for NULL/empty/non-absolute paths.
 */
static AdbxTriStatus is_abs_path(const char *path) {
  if (!path || path[0] != '/')
    return NO;
  return YES;
}

/* Releases one fd/path pair shared by ConfDir and ConfFile cleanup.
 * It consumes '*fd' and '*path'.
 * Side effects: closes one fd and frees one heap string when present.
 * Error semantics: none.
 */
static void cfg_handle_clean(int *fd, char **path) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
  if (path) {
    free(*path);
    *path = NULL;
  }
}

/* Returns open(2) flags for directories where final symlinks are allowed.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int cfg_dir_open_flags_follow(void) {
  int flags = O_RDONLY | O_DIRECTORY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
  return flags;
}

/* Returns open(2) flags for app-owned directories where final symlinks are not
 * allowed.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int cfg_dir_open_flags_nofollow(void) {
  int flags = cfg_dir_open_flags_follow();
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  return flags;
}

/* Returns open(2) flags for regular files where final symlinks are allowed.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int cfg_file_open_flags_follow(void) {
  int flags = O_RDONLY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
  return flags;
}

/* Returns open(2) flags for app-owned regular files where final symlinks are
 * not allowed.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int cfg_file_open_flags_nofollow(void) {
  int flags = cfg_file_open_flags_follow();
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  return flags;
}

/* Validates that one opened fd refers to a regular file.
 * It borrows 'fd' and does not allocate.
 * Side effects: reads filesystem metadata.
 * Error semantics: returns OK on success, ERR on invalid input or non-regular
 * fd.
 */
static AdbxStatus cfg_validate_regular_file_fd(int fd) {
  if (fd < 0)
    return ERR;

  struct stat st = {0};
  if (fstat(fd, &st) != 0)
    return ERR;
  return S_ISREG(st.st_mode) ? OK : ERR;
}

/* Resolves the default config base directory path and stores it inside
 * 'out_base'. If this valorizes 'out_attempt_create' with 1, then mutating
 * callers should attemp creating the Linux fallback directory. It allocates one
 * heap string in '*out_base' owned by caller. Side effects: reads
 * HOME/XDG_CONFIG_HOME only; no filesystem access. Returns OK on success, ERR
 * on invalid env or allocation failure and optionally sets
 * '*out_code'/'*out_err'.
 */
static AdbxStatus cfg_resolve_default_base_dir(char **out_base,
                                               int *out_attempt_create,
                                               ConfDirErrCode *out_code,
                                               char **out_err) {
  if (!out_base || !out_attempt_create)
    return ERR;

  *out_base = NULL;
  *out_attempt_create = 0;
  if (out_code)
    *out_code = CONFDIR_ERR_NONE;
  if (out_err)
    *out_err = NULL;

  char *base = NULL;

#ifdef __linux__
  const char *xdg = getenv("XDG_CONFIG_HOME");
  if (xdg) {
    if (is_abs_path(xdg) != YES || xdg[0] == '\0') {
      cfg_set_code(out_code, CONFDIR_ERR_ENV);
      cfg_set_err(out_err,
                  "invalid XDG_CONFIG_HOME: expected an absolute path.");
      return ERR;
    }
    base = strdup(xdg);
    if (!base) {
      cfg_set_code(out_code, CONFDIR_ERR_DIR);
      cfg_set_err(out_err,
                  "failed to allocate the default config base-dir path.");
      return ERR;
    }
  }
#endif

  if (!base) {
    const char *home = getenv("HOME");
    if (is_abs_path(home) != YES || home[0] == '\0') {
      cfg_set_code(out_code, CONFDIR_ERR_ENV);
      cfg_set_err(out_err,
                  "failed to resolve default config base dir: set absolute "
                  "HOME%s.",
#ifdef __linux__
                  " or XDG_CONFIG_HOME"
#else
                  ""
#endif
      );
      return ERR;
    }

#ifdef __APPLE__
    base = path_join(home, "Library/Application Support");
#else
    // safe to attemp creating $HOME/.config on Linux
    base = path_join(home, ".config");
    *out_attempt_create = 1;
#endif
    if (!base) {
      cfg_set_code(out_code, CONFDIR_ERR_DIR);
      cfg_set_err(out_err,
                  "failed to allocate the default config base-dir path.");
      return ERR;
    }
  }

  *out_base = base;
  return OK;
}

/* Opens one explicit user-managed config file and validates its final target.
 * It borrows 'path' and writes one caller-owned fd into '*out_fd'.
 * Side effects: opens one file descriptor.
 * Error semantics: returns OK on success, ERR on invalid input, open failure,
 * or non-regular targets.
 */
static AdbxStatus cfg_open_explicit_file(const char *path, int *out_fd) {
  if (!path || !out_fd)
    return ERR;
  *out_fd = -1;

  int fd = open(path, cfg_file_open_flags_follow());
  if (fd < 0)
    return ERR;

  if (cfg_validate_regular_file_fd(fd) != OK) {
    (void)close(fd);
    return ERR;
  }

  *out_fd = fd;
  return OK;
}

/* Creates or opens the owned app dir under one config base dir.
 * It borrows 'base_fd'; on success it returns an owned fd in '*out_fd' that
 * caller must close.
 * Side effects: may create CONFDIR_APP_DIRNAME and may chmod it to 0700.
 * Error semantics: returns OK on success, ERR on invalid input, open failure,
 * or directory-policy mismatch.
 */
static AdbxStatus cfg_open_or_create_app_dir_at(int base_fd, int *out_fd) {
  if (base_fd < 0 || !out_fd)
    return ERR;
  *out_fd = -1;

  if (mkdirat(base_fd, CONFDIR_APP_DIRNAME, 0700) != 0 && errno != EEXIST)
    return ERR;

  int app_fd =
      openat(base_fd, CONFDIR_APP_DIRNAME, cfg_dir_open_flags_nofollow());
  if (app_fd < 0)
    return ERR;

  if (validate_uown_dir(app_fd, 0700) != OK) {
    (void)close(app_fd);
    return ERR;
  }

  *out_fd = app_fd;
  return OK;
}

/* Opens one app-owned config file and validates its ownership/mode policy.
 * It borrows 'app_fd' and 'name'; on success it returns an owned fd in
 * '*out_fd' that caller must close.
 * Side effects: opens one file descriptor and may chmod the file to 0600.
 * Error semantics: returns OK on success, ERR on invalid input, open failure,
 * or file-policy mismatch.
 */
static AdbxStatus
cfg_open_validated_default_file_at(int app_fd, const char *name, int *out_fd) {
  if (app_fd < 0 || !name || !out_fd)
    return ERR;
  *out_fd = -1;

  int fd = openat(app_fd, name, cfg_file_open_flags_nofollow());
  if (fd < 0)
    return ERR;

  if (validate_uown_file(fd, 0600) != OK) {
    (void)close(fd);
    return ERR;
  }

  *out_fd = fd;
  return OK;
}

void confdir_clean(ConfDir *cd) {
  if (!cd)
    return;
  cfg_handle_clean(&cd->fd, &cd->path);
}

void conffile_clean(ConfFile *cf) {
  if (!cf)
    return;
  cfg_handle_clean(&cf->fd, &cf->path);
}

AdbxStatus confdir_default_resolve(char **out_path, ConfDirErrCode *out_code,
                                   char **out_err) {
  if (!out_path)
    return ERR;

  *out_path = NULL;
  if (out_code)
    *out_code = CONFDIR_ERR_NONE;
  if (out_err)
    *out_err = NULL;

  char *base = NULL;
  int attempt_create = 0;
  if (cfg_resolve_default_base_dir(&base, &attempt_create, out_code, out_err) !=
      OK) {
    return ERR;
  }
  (void)attempt_create;

  char *app_path = path_join(base, CONFDIR_APP_DIRNAME);
  free(base);
  if (!app_path) {
    cfg_set_code(out_code, CONFDIR_ERR_DIR);
    cfg_set_err(out_err, "failed to resolve default app directory.");
    return ERR;
  }

  *out_path = app_path;
  return OK;
}

AdbxStatus confdir_resolve_config_path(const char *input_path, char **out_path,
                                       char **out_err) {
  if (!out_path)
    return ERR;

  *out_path = NULL;
  if (out_err)
    *out_err = NULL;

  if (input_path && input_path[0] != '\0') {
    if (is_abs_path(input_path) != YES) {
      cfg_set_err(out_err,
                  "failed to resolve the configuration file. The path should "
                  "be absolute, starting with '/'.");
      return ERR;
    }

    *out_path = strdup(input_path);
    if (!*out_path) {
      cfg_set_err(out_err, "failed to copy input config path. Please retry.");
      return ERR;
    }
    return OK;
  }

  char *app_path = NULL;
  if (confdir_default_resolve(&app_path, NULL, out_err) != OK)
    return ERR;

  *out_path = path_join(app_path, CFG_FILENAME);
  free(app_path);
  if (!*out_path) {
    cfg_set_err(out_err,
                "failed to resolve default config path. Please, retry.");
    return ERR;
  }
  return OK;
}

AdbxStatus confdir_default_open(ConfDir *out, ConfDirErrCode *out_code,
                                char **out_err) {
  if (!out)
    return ERR;
  out->fd = -1;
  out->path = NULL;
  if (out_code)
    *out_code = CONFDIR_ERR_NONE;
  if (out_err)
    *out_err = NULL;

  char *base = NULL;
  char *app_path = NULL;
  int attempt_create = 0;
  int base_fd = -1;
  int app_fd = -1;
  if (cfg_resolve_default_base_dir(&base, &attempt_create, out_code, out_err) !=
      OK) {
    return ERR;
  }

  if (attempt_create && mkdir(base, 0700) != 0 && errno != EEXIST) {
    cfg_set_code(out_code, CONFDIR_ERR_DIR);
    cfg_set_err(out_err,
                "unable to create config dir at: %s. Please, create it and "
                "retry.",
                base);
    free(base);
    return ERR;
  }

  base_fd = open(base, cfg_dir_open_flags_follow());
  if (base_fd < 0) {
    cfg_set_code(out_code, CONFDIR_ERR_DIR);
    cfg_set_err(out_err, "failed to open default config base directory: %s",
                base);
    free(base);
    return ERR;
  }

  if (cfg_open_or_create_app_dir_at(base_fd, &app_fd) != OK) {
    cfg_set_code(out_code, CONFDIR_ERR_DIR);
    cfg_set_err(out_err, "failed to prepare app config directory under: %s",
                base);
    goto err;
  }

  app_path = path_join(base, CONFDIR_APP_DIRNAME);
  if (!app_path) {
    cfg_set_code(out_code, CONFDIR_ERR_DIR);
    cfg_set_err(out_err, "failed to resolve default app directory.");
    goto err;
  }

  out->fd = app_fd;
  out->path = app_path;
  app_fd = -1;
  app_path = NULL;
  free(base);
  (void)close(base_fd);
  return OK;

err:
  if (app_fd >= 0)
    (void)close(app_fd);
  if (base_fd >= 0)
    (void)close(base_fd);
  free(app_path);
  free(base);
  return ERR;
}

AdbxStatus confdir_open(const char *input_path, ConfFile *out, char **out_err) {
  if (!out)
    return ERR;
  out->fd = -1;
  out->path = NULL;
  if (out_err)
    *out_err = NULL;

  if (input_path && input_path[0] != '\0') {
    if (confdir_resolve_config_path(input_path, &out->path, out_err) != OK)
      return ERR;

    if (cfg_open_explicit_file(out->path, &out->fd) != OK) {
      cfg_set_err(out_err,
                  "configuration file does not exist or is invalid: %s",
                  out->path);
      conffile_clean(out);
      return ERR;
    }
    return OK;
  }

  ConfDir app = {.fd = -1, .path = NULL};
  int file_fd = -1;

  if (confdir_default_open(&app, NULL, out_err) != OK)
    goto err;

  out->path = path_join(app.path, CFG_FILENAME);
  if (!out->path) {
    cfg_set_err(out_err,
                "failed to resolve default config path. Please, retry.");
    goto err;
  }

  if (cfg_open_validated_default_file_at(app.fd, CFG_FILENAME, &file_fd) !=
      OK) {
    if (errno == ENOENT) {
      cfg_set_err(out_err, "default configuration file does not exist: %s",
                  out->path);
    } else {
      cfg_set_err(out_err, "failed to open validated config file: %s",
                  out->path);
    }
    goto err;
  }

  out->fd = file_fd;
  file_fd = -1;

  confdir_clean(&app);
  return OK;

err:
  if (file_fd >= 0)
    (void)close(file_fd);
  confdir_clean(&app);
  conffile_clean(out);
  return ERR;
}
