#include "config_dir.h"

#include "file_io.h"
#include "string_op.h"
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

static const char CFG_DEFAULT_JSON[] =
    "{\n"
    "  \"version\": \"1.0\",\n"
    "  \"safetyPolicy\": {},\n"
    "  \"databases\": [\n"
    "    {\n"
    "      \"type\": \"postgres\",\n"
    "      \"connectionName\": \"DummyConnection\",\n"
    "      \"host\": \"dummyhost\",\n"
    "      \"port\": 5432,\n"
    "      \"username\": \"dummyuser\",\n"
    "      \"database\": \"dummydb\"\n"
    "    }\n"
    "  ]\n"
    "}\n";

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

/* Duplicates one [start, end) character range into a NUL-terminated string.
 * It borrows input pointers and returns heap memory owned by caller.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *dup_range(const char *start, const char *end) {
  if (!start || !end || end < start)
    return NULL;
  size_t len = (size_t)(end - start);
  char *out = (char *)xmalloc(len + 1);
  memcpy(out, start, len);
  out[len] = '\0';
  return out;
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

/* Returns open(2) flags for directory descriptors that must not follow a final
 * symlink.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int cfg_dir_open_flags(void) {
  int flags = O_RDONLY | O_DIRECTORY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  return flags;
}

/* Returns open(2) flags for regular files that must not follow a final symlink.
 * Side effects: none.
 * Error semantics: returns a valid flag mask for local open/openat calls.
 */
static int cfg_file_open_flags(void) {
  int flags = O_RDONLY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  return flags;
}

/* Validates that one absolute file path already exists and resolves to a
 * regular file without a final symlink.
 * It borrows 'path' and does not allocate resources that outlive the call.
 * Returns OK on success, ERR on invalid input, missing files,
 * or non-regular targets.
 */
static AdbxStatus cfg_validate_existing_reg_file(const char *path) {
  if (is_abs_path(path) != YES)
    return ERR;

  int file_fd = open(path, cfg_file_open_flags());
  if (file_fd < 0) {
    return ERR;
  }

  struct stat st = {0};
  if (fstat(file_fd, &st) != 0) {
    close(file_fd);
    return ERR;
  }
  close(file_fd);

  if (S_ISREG(st.st_mode)) {
    return OK;
  }
  return ERR;
}

/* Builds one config-file path inside an application config directory.
 * It borrows 'app_dir' and returns one heap string owned by caller.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on invalid input or allocation failure.
 */
static char *confdir_build_config_path(const char *app_dir) {
  if (!app_dir)
    return NULL;
  return path_join(app_dir, CFG_FILENAME);
}

AdbxStatus confdir_get_default_base_dir(int *out_fd, char **out_path,
                                        char **out_err) {
  if (!out_fd)
    return ERR;
  *out_fd = -1;
  if (out_err)
    *out_err = NULL;
  if (out_path)
    *out_path = NULL;

  char *base = NULL;
  int attempt_creat = 0;

#ifdef __linux__
  const char *xdg = getenv("XDG_CONFIG_HOME");
  if (xdg) {
    if (is_abs_path(xdg) != YES || xdg[0] == '\0') {
      cfg_set_err(out_err,
                  "invalid XDG_CONFIG_HOME: expected an absolute path.");
      return ERR;
    }
    base = strdup(xdg); // allocate so every path can lead to free()
  }
#endif

  if (!base) {
    const char *home = getenv("HOME");
    if (is_abs_path(home) != YES || home[0] == '\0') {
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
    base = path_join(home, ".config");
    attempt_creat = 1;
#endif
    if (!base) {
      cfg_set_err(out_err,
                  "failed to allocate the default config base-dir path.");
      return ERR;
    }
  }

  if (attempt_creat) {
    if (mkdir(base, 0700) != 0 && errno != EEXIST) {
      cfg_set_err(
          out_err,
          "unable to create config dir at: %s. Please, create it and retry.",
          base);
      free(base);
      return ERR;
    }
  }

  int dir_fd = open(base, cfg_dir_open_flags());
  if (dir_fd < 0) {
    free(base);
    return ERR;
  }

  if (out_path) {
    *out_path = base;
  } else {
    free(base);
  }
  *out_fd = dir_fd;
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

  int app_fd = openat(base_fd, CONFDIR_APP_DIRNAME, cfg_dir_open_flags());
  if (app_fd < 0)
    return ERR;

  if (validate_uown_dir(app_fd, 0700) != OK) {
    (void)close(app_fd);
    return ERR;
  }

  *out_fd = app_fd;
  return OK;
}

/* Creates one new default config.json under 'dir_fd'.
 * It borrows 'dir_fd' and writes built-in JSON into a newly created regular
 * file.
 * Error semantics: returns YES on success, NO if the 'name' was already
 * present, ERR on invalid input or I/O failure.
 */
static AdbxTriStatus cfg_create_default_file_at(int dir_fd, const char *name) {
  if (dir_fd < 0 || !name)
    return ERR;

  int flags = O_WRONLY | O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif

  int fd = openat(dir_fd, name, flags, 0600);
  if (fd < 0) {
    if (errno != EEXIST) {
      return ERR;
    }
    return NO;
  }

  const uint8_t *src = (const uint8_t *)CFG_DEFAULT_JSON;
  size_t len = strlen(CFG_DEFAULT_JSON);
  if (fileio_write_exact_fd(fd, src, len) != OK) {
    return ERR;
  }
  return YES;
}

/* Ensures the default config.json exists under one owned app dir.
 * It borrows 'app_fd' and 'name'.
 * Side effects: may create one new file or chmod an existing app-owned file to
 * 0600.
 * Error semantics: returns OK on success, ERR on invalid input, open failure,
 * or file-policy mismatch.
 */
static AdbxStatus cfg_ensure_default_file_at(int app_fd, const char *name) {
  if (app_fd < 0 || !name)
    return ERR;

  int fd = openat(app_fd, name, cfg_file_open_flags());
  if (fd >= 0) {
    AdbxStatus rc = validate_uown_file(fd, 0600);
    (void)close(fd);
    return rc;
  }
  if (errno != ENOENT)
    return ERR;

  AdbxTriStatus rc = cfg_create_default_file_at(app_fd, name);
  if (rc == YES)
    return OK;
  if (rc == ERR)
    return ERR;

  fd = openat(app_fd, name, cfg_file_open_flags());
  if (fd < 0)
    return ERR;
  AdbxStatus rv = validate_uown_file(fd, 0600);
  (void)close(fd);
  return rv;
}

AdbxStatus confdir_resolve(const char *input_path, char **out_path,
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
    if (cfg_validate_existing_reg_file(input_path) != OK) {
      cfg_set_err(out_err,
                  "configuration file does not exist or is invalid: %s",
                  input_path);
      return ERR;
    }

    char *path = strdup(input_path);
    if (!path) {
      cfg_set_err(out_err, "failed to copy input config path. Please retry.");
      return ERR;
    }
    *out_path = path;
    return OK;
  }

  // base dir resolution based on env

  char *base_dir = NULL; // used just for logging
  char *app_dir = NULL;
  char *path = NULL;
  int base_fd = -1;
  int app_fd = -1;

  if (confdir_get_default_base_dir(&base_fd, &base_dir, out_err) != OK) {
    cfg_set_err(out_err, "failed to open default config base directory: %s",
                base_dir);
    goto err;
  }

  if (cfg_open_or_create_app_dir_at(base_fd, &app_fd) != OK) {
    cfg_set_err(out_err, "failed to prepare app config directory under: %s",
                base_dir);
    goto err;
  }

  app_dir = path_join(base_dir, CONFDIR_APP_DIRNAME);
  if (!app_dir) {
    cfg_set_err(out_err,
                "failed to resolve default app directory. Please, retry.");
    goto err;
  }

  path = confdir_build_config_path(app_dir);
  if (!path) {
    cfg_set_err(out_err,
                "failed to resolve default config path. Please, retry.");
    goto err;
  }

  if (cfg_ensure_default_file_at(app_fd, CFG_FILENAME) != OK) {
    cfg_set_err(out_err, "failed to ensure default config file exists: %s",
                path);
    goto err;
  }

  (void)close(app_fd);
  (void)close(base_fd);
  free(base_dir);
  free(app_dir);
  *out_path = path;
  return OK;

err:
  if (app_fd >= 0)
    (void)close(app_fd);
  if (base_fd >= 0)
    (void)close(base_fd);
  free(base_dir);
  free(app_dir);
  free(path);
  return ERR;
}
