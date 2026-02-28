#include "config_dir.h"

#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CFG_APPNAME "ai-dbexplorer"
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

/* Joins two path fragments as "a/b".
 * It borrows inputs and returns a heap string owned by caller.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *join_path2(const char *a, const char *b) {
  if (!a || !b)
    return NULL;
  size_t a_len = strlen(a);
  size_t b_len = strlen(b);
  int has_slash = (a_len > 0 && a[a_len - 1] == '/');
  size_t out_len = a_len + (has_slash ? 0u : 1u) + b_len;
  char *out = (char *)xmalloc(out_len + 1);
  if (has_slash) {
    snprintf(out, out_len + 1, "%s%s", a, b);
  } else {
    snprintf(out, out_len + 1, "%s/%s", a, b);
  }
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

/* Ensures one directory exists; if missing it is created.
 * It borrows 'path' and performs filesystem writes as needed.
 * Side effects: may create directory on disk.
 * Error semantics: returns OK on success, ERR on non-dir conflicts or syscall
 * failures.
 */
static AdbxStatus ensure_one_dir(const char *path) {
  if (!path || path[0] == '\0')
    return ERR;

  if (mkdir(path, 0700) == 0)
    return OK;

  if (errno != EEXIST)
    return ERR;

  struct stat st = {0};
  if (stat(path, &st) != 0)
    return ERR;
  return S_ISDIR(st.st_mode) ? OK : ERR;
}

/* Ensures all parent directories in 'dir_path' exist.
 * It borrows 'dir_path' and allocates one temporary mutable copy.
 * Side effects: may create multiple directories.
 * Error semantics: returns OK on success, ERR on invalid input or mkdir/stat
 * failures.
 */
static AdbxStatus ensure_dir_tree(const char *dir_path) {
  if (!dir_path || dir_path[0] == '\0')
    return ERR;

  char *tmp = strdup(dir_path);
  if (!tmp)
    return ERR;

  size_t len = strlen(tmp);
  for (size_t i = 1; i < len; i++) {
    if (tmp[i] != '/')
      continue;
    tmp[i] = '\0';
    if (tmp[0] != '\0' && ensure_one_dir(tmp) != OK) {
      free(tmp);
      return ERR;
    }
    tmp[i] = '/';
  }

  AdbxStatus rc = ensure_one_dir(tmp);
  free(tmp);
  return rc;
}

/* Assign the parent directory of 'path' at 'out_dir'.
 * It borrows 'path'. On success, returns one heap string in '*out_dir' owned by
 * caller.
 * Side effects: allocates memory.
 * Error semantics: returns OK on success, ERR on malformed path input.
 */
static AdbxStatus path_parent_dir(const char *path, char **out_dir) {
  if (!path || !out_dir)
    return ERR;
  *out_dir = NULL;

  size_t len = strlen(path);
  if (len == 0 || path[len - 1] == '/')
    return ERR;

  const char *last = strrchr(path, '/');
  if (!last) {
    *out_dir = strdup(".");
    return *out_dir ? OK : ERR;
  }
  if (last == path) {
    *out_dir = strdup("/");
    return *out_dir ? OK : ERR;
  }

  *out_dir = dup_range(path, last);
  return *out_dir ? OK : ERR;
}

/* Creates one new config file atomically with built-in default content.
 * It borrows 'path'.
 * Side effects: creates and writes one file with mode 0600.
 * Error semantics: returns OK on success, ERR on any open/write/fchmod/close
 * failure.
 */
static AdbxStatus create_default_config_file(const char *path) {
  if (!path)
    return ERR;

  int flags = O_WRONLY | O_CREAT | O_EXCL;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  int fd = open(path, flags, 0600);
  if (fd < 0)
    return ERR;

  const uint8_t *src = (const uint8_t *)CFG_DEFAULT_JSON;
  size_t left = strlen(CFG_DEFAULT_JSON);
  while (left > 0) {
    ssize_t n = write(fd, src, left);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      (void)close(fd);
      (void)unlink(path);
      return ERR;
    }
    if (n == 0) {
      (void)close(fd);
      (void)unlink(path);
      return ERR;
    }
    src += (size_t)n;
    left -= (size_t)n;
  }

  if (fchmod(fd, 0600) != 0) {
    (void)close(fd);
    (void)unlink(path);
    return ERR;
  }

  if (close(fd) != 0) {
    (void)unlink(path);
    return ERR;
  }
  return OK;
}

/* Ensures config file exists and is a regular file.
 * It borrows 'path'.
 * Side effects: may create one new file with default content.
 * Error semantics: returns OK on success, ERR on type mismatches or I/O
 * failures.
 */
static AdbxStatus ensure_config_file(const char *path) {
  if (!path)
    return ERR;

  struct stat st = {0};
  if (lstat(path, &st) == 0) {
    return S_ISREG(st.st_mode) ? OK : ERR;
  }
  if (errno != ENOENT)
    return ERR;

  if (create_default_config_file(path) == OK)
    return OK;

  if (errno != EEXIST)
    return ERR;

  if (lstat(path, &st) != 0)
    return ERR;
  return S_ISREG(st.st_mode) ? OK : ERR;
}

/* Resolves default config path for current OS/environment.
 * It borrows environment variables and returns one heap path owned by caller.
 * Side effects: allocates memory.
 * Error semantics: returns NULL when required environment context is missing or
 * invalid.
 */
static char *resolve_default_config_path(void) {
  const char *xdg = getenv("XDG_CONFIG_HOME");
  if (is_abs_path(xdg) == YES && xdg[0] != '\0') {
    char *dir = join_path2(xdg, CFG_APPNAME);
    char *path = join_path2(dir, CFG_FILENAME);
    free(dir);
    return path;
  }

  const char *home = getenv("HOME");
  if (is_abs_path(home) != YES || home[0] == '\0')
    return NULL;

#ifdef __APPLE__
  char *base = join_path2(home, "Library/Application Support");
#else
  char *base = join_path2(home, ".config");
#endif
  if (!base)
    return NULL;

  char *dir = join_path2(base, CFG_APPNAME);
  free(base);
  if (!dir)
    return NULL;

  char *path = join_path2(dir, CFG_FILENAME);
  free(dir);
  return path;
}

AdbxStatus confdir_resolve(const char *input_path, char **out_path,
                           char **out_err) {
  if (!out_path)
    return ERR;
  *out_path = NULL;
  if (out_err)
    *out_err = NULL;

  char *path = NULL;
  if (input_path && input_path[0] != '\0') {
    path = strdup(input_path);
    if (!path) {
      cfg_set_err(out_err, "failed to copy input config path. Please retry.");
      return ERR;
    }
  } else {
    path = resolve_default_config_path();
    if (!path) {
      cfg_set_err(out_err, "failed to resolve default config path.");
      return ERR;
    }
  }

  char *dir = NULL;
  if (path_parent_dir(path, &dir) != OK) {
    cfg_set_err(out_err, "invalid config path: %s", path);
    free(path);
    return ERR;
  }

  if (ensure_dir_tree(dir) != OK) {
    if (!dir) {
      cfg_set_err(out_err, "failed to create config directory at: <null>");
    } else {
      cfg_set_err(out_err, "failed to create config directory at: %s", dir);
    }
    free(dir);
    free(path);
    return ERR;
  }
  free(dir);

  if (ensure_config_file(path) != OK) {
    cfg_set_err(out_err, "failed to ensure config file exists: %s", path);
    free(path);
    return ERR;
  }

  *out_path = path;
  return OK;
}
