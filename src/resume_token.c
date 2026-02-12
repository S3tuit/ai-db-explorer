#define _GNU_SOURCE

#include "resume_token.h"

#include "file_io.h"
#include "proc_identity.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define RESTOK_DIR_NAME "ai-dbexplorer-mcp"

/* Logs and marks resume persistence as disabled for this process.
 * Ownership: borrows 'store' and 'reason'; no allocations.
 * Side effects: updates 'store->enabled' and writes to stderr.
 * Error semantics: none (best-effort logging).
 */
static void restok_disable(ResumeTokenStore *store, const char *reason) {
  if (!store || store->enabled == NO)
    return;

  if (!reason)
    reason = "unknown reason";
  fprintf(stderr,
          "resume_token: %s; session resume disabled for this process\n",
          reason);
  store->enabled = NO;
}

/* Resolves the process-local base directory used by token persistence.
 * Ownership: returns heap path owned by caller, or NULL on failure.
 * Side effects: reads environment variables.
 * Error semantics: returns non-NULL on success, NULL on invalid environment.
 */
static char *restok_resolve_dir_path(void) {
  const char *env_val = NULL;
#ifdef __linux__
  env_val = getenv("XDG_RUNTIME_DIR");
#elif defined(__APPLE__)
  env_val = getenv("TMPDIR");
#endif

  if (env_val && env_val[0] == '/') {
    size_t len = strlen(env_val);
    while (len > 1 && env_val[len - 1] == '/')
      len--;

    size_t out_len = len + 1 + strlen(RESTOK_DIR_NAME) + 1;
    char *dir = xmalloc(out_len);
    (void)snprintf(dir, out_len, "%.*s/%s", (int)len, env_val, RESTOK_DIR_NAME);
    return dir;
  }

  char fallback[128];
  int n = snprintf(fallback, sizeof(fallback), "/tmp/%s-%u", RESTOK_DIR_NAME,
                   (unsigned)getuid());
  if (n < 0 || (size_t)n >= sizeof(fallback))
    return NULL;

  char *dir = xmalloc((size_t)n + 1);
  memcpy(dir, fallback, (size_t)n + 1);
  return dir;
}

/* Builds the token file path "token-<pid>-<start_time>" under 'dir_path'.
 * Ownership: borrows 'dir_path'/'id'; returns heap path owned by caller.
 * Side effects: allocates memory.
 * Error semantics: returns non-NULL on success, NULL on invalid input.
 */
static char *restok_build_token_path(const char *dir_path,
                                     const ProcIdentity *id) {
  if (!dir_path || !id || id->pid <= 1 || id->start_time_ticks == 0)
    return NULL;

  char filename[128];
  int fn = snprintf(filename, sizeof(filename), "token-%ld-%llu", (long)id->pid,
                    (unsigned long long)id->start_time_ticks);
  if (fn < 0 || (size_t)fn >= sizeof(filename))
    return NULL;

  size_t dir_len = strlen(dir_path);
  size_t out_len = dir_len + 1 + (size_t)fn + 1;
  char *token_path = xmalloc(out_len);
  (void)snprintf(token_path, out_len, "%s/%s", dir_path, filename);
  return token_path;
}

/* Validates strict directory policy: directory, owned by this uid, mode 0700.
 * Ownership: borrows 'path'; does not allocate.
 * Side effects: performs stat-like filesystem metadata reads.
 * Error semantics: returns OK on strict match, ERR otherwise.
 */
static int restok_validate_dir_policy(const char *path) {
  if (!path)
    return ERR;

  struct stat st;
  if (lstat(path, &st) != 0)
    return ERR;
  if (!S_ISDIR(st.st_mode))
    return ERR;
  if (st.st_uid != getuid())
    return ERR;
  if ((st.st_mode & 0777) != 0700)
    return ERR;
  return OK;
}

/* Ensures the resume-token directory exists and matches strict policy.
 * Ownership: borrows 'path'; does not allocate.
 * Side effects: may create directory and call chmod.
 * Error semantics: returns OK on success, ERR on create/validation failure.
 */
static int restok_ensure_dir_policy(const char *path) {
  if (!path)
    return ERR;

  if (mkdir(path, 0700) == 0) {
    // We force exact mode regardless of process umask.
    if (chmod(path, 0700) != 0)
      return ERR;
    return OK;
  }
  if (errno != EEXIST)
    return ERR;
  return restok_validate_dir_policy(path);
}

/* Validates strict token-file policy: regular file, current uid, mode 0600.
 * Ownership: borrows 'st'; does not allocate.
 * Side effects: none.
 * Error semantics: returns OK on strict match, ERR otherwise.
 */
static int restok_validate_token_stat(const struct stat *st) {
  if (!st)
    return ERR;
  if (!S_ISREG(st->st_mode))
    return ERR;
  if (st->st_uid != getuid())
    return ERR;
  if ((st->st_mode & 0777) != 0600)
    return ERR;
  return OK;
}

/* Deletes 'path' if present.
 * Ownership: borrows 'path'; no allocations.
 * Side effects: performs unlink syscall.
 * Error semantics: returns OK when absent/deleted, ERR on invalid input or
 * unlink failure.
 */
static int restok_delete_path(const char *path) {
  if (!path)
    return ERR;
  if (unlink(path) != 0 && errno != ENOENT)
    return ERR;
  return OK;
}

int restok_init(ResumeTokenStore *store) {
  if (!store)
    return ERR;

  memset(store, 0, sizeof(*store));
  store->enabled = NO;

  ProcIdentity id = {0};
  if (procid_parent_identity(&id) != OK) {
    fprintf(stderr,
            "Could not verify parent process start time, session resume "
            "disabled for this instance\n");
    return NO;
  }

  char *dir_path = restok_resolve_dir_path();
  char *token_path = restok_build_token_path(dir_path, &id);
  if (!dir_path || !token_path) {
    free(dir_path);
    free(token_path);
    fprintf(stderr, "resume_token: failed to allocate token paths; session "
                    "resume disabled for this instance\n");
    return NO;
  }

  store->dir_path = dir_path;
  store->token_path = token_path;

  if (restok_ensure_dir_policy(store->dir_path) != OK) {
    restok_disable(store, "directory policy check failed");
    return NO;
  }

  store->enabled = YES;
  return YES;
}

int restok_load(ResumeTokenStore *store, uint8_t out[ADBX_RESUME_TOKEN_LEN]) {
  if (!store || !out)
    return ERR;
  if (store->enabled != YES)
    return NO;
  if (!store->dir_path || !store->token_path)
    return NO;

  if (restok_validate_dir_policy(store->dir_path) != OK) {
    restok_disable(store, "directory permissions/ownership are too open");
    return NO;
  }

  struct stat st;
  if (lstat(store->token_path, &st) != 0) {
    if (errno == ENOENT)
      // there's no token to be read
      return NO;
    restok_disable(store, "token file metadata read failed");
    return NO;
  }

  if (restok_validate_token_stat(&st) != OK) {
    restok_disable(store, "token file permissions/ownership are too open");
    return NO;
  }

  // read exact token length
  size_t nread = 0;
  if (fileio_read_limit(store->token_path, ADBX_RESUME_TOKEN_LEN, out,
                        &nread) != OK)
    return ERR;
  if (nread == ADBX_RESUME_TOKEN_LEN) {
    return YES;
  }

  // Corrupted token files are treated as stale and removed immediately.
  fprintf(stderr, "Token file corrupted, treating as stale\n");
  if (restok_delete_path(store->token_path) != OK) {
    restok_disable(store, "failed to delete corrupted token file");
  }
  return NO;
}

int restok_store(ResumeTokenStore *store,
                 const uint8_t token[ADBX_RESUME_TOKEN_LEN]) {
  if (!store || !token)
    return ERR;
  if (store->enabled != YES)
    return OK;
  if (!store->dir_path || !store->token_path)
    return ERR;

  if (restok_ensure_dir_policy(store->dir_path) != OK) {
    restok_disable(store, "directory permissions/ownership are too open");
    return ERR;
  }

  if (fileio_write_exact(store->token_path, token, ADBX_RESUME_TOKEN_LEN,
                         0700) != OK) {
    fprintf(stderr, "Failed to write token file: %s\n", strerror(errno));
    restok_disable(store, "token file write failed");
    return ERR;
  }

  struct stat st;
  errno = 0;
  int src = lstat(store->token_path, &st);
  if (src == 0 && restok_validate_token_stat(&st) != OK) {
    restok_disable(store, "token file permissions/ownership are too open");
    return ERR;
  }
  if (src != 0 && errno != ENOENT) {
    restok_disable(store, "token file metadata read failed");
    return ERR;
  }

  return OK;
}

int restok_delete(ResumeTokenStore *store) {
  if (!store)
    return ERR;
  if (store->enabled != YES)
    return OK;
  if (!store->dir_path || !store->token_path)
    return ERR;

  if (restok_validate_dir_policy(store->dir_path) != OK) {
    restok_disable(store, "directory permissions/ownership are too open");
    return ERR;
  }

  struct stat st;
  errno = 0;
  int src = lstat(store->token_path, &st);
  if (src == 0 && restok_validate_token_stat(&st) != OK) {
    restok_disable(store, "token file permissions/ownership are too open");
    return ERR;
  }
  if (src != 0 && errno != ENOENT) {
    restok_disable(store, "token file metadata read failed");
    return ERR;
  }

  if (restok_delete_path(store->token_path) != OK) {
    restok_disable(store, "failed to delete token file");
    return ERR;
  }
  return OK;
}

void restok_clean(ResumeTokenStore *store) {
  if (!store)
    return;

  free(store->dir_path);
  free(store->token_path);
  store->dir_path = NULL;
  store->token_path = NULL;
  store->enabled = NO;
}
