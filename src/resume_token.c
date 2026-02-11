#define _GNU_SOURCE

#include "resume_token.h"

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

/* Frees current path fields and resets them to NULL.
 * Ownership: consumes owned strings in 'store'.
 * Side effects: frees heap memory and mutates 'store'.
 * Error semantics: none.
 */
static void restok_reset_paths(ResumeTokenStore *store) {
  if (!store)
    return;
  free(store->dir_path);
  free(store->token_path);
  store->dir_path = NULL;
  store->token_path = NULL;
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

/* Reads exactly RESUME_TOKEN_LEN bytes and verifies EOF immediately after.
 * Ownership: borrows 'path'; writes to caller-owned 'out'.
 * Side effects: opens/reads/closes file descriptor.
 * Error semantics: returns YES when token length is exact, NO on malformed
 * length or read failure, ERR on invalid input.
 */
static int restok_read_exact_token(const char *path,
                                   uint8_t out[RESUME_TOKEN_LEN]) {
  if (!path || !out)
    return ERR;

  int flags = O_RDONLY;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  int fd = open(path, flags);
  if (fd < 0)
    return NO;

  size_t off = 0;
  while (off < RESUME_TOKEN_LEN) {
    ssize_t n = read(fd, out + off, RESUME_TOKEN_LEN - off);
    if (n == 0)
      break;
    if (n < 0) {
      if (errno == EINTR)
        continue;
      (void)close(fd);
      return NO;
    }
    off += (size_t)n;
  }

  if (off != RESUME_TOKEN_LEN) {
    (void)close(fd);
    return NO;
  }

  uint8_t extra = 0;
  for (;;) {
    ssize_t n = read(fd, &extra, 1);
    if (n == 0)
      break;
    if (n < 0) {
      if (errno == EINTR)
        continue;
      (void)close(fd);
      return NO;
    }
    // Any extra byte means the file is longer than expected.
    (void)close(fd);
    return NO;
  }

  if (close(fd) != 0)
    return NO;
  return YES;
}

/* Writes exactly RESUME_TOKEN_LEN bytes to 'path' with strict 0600 policy.
 * Ownership: borrows 'path' and 'token'; no allocations.
 * Side effects: opens/writes/chmods/closes file and may unlink on failure.
 * Error semantics: returns OK on success, ERR on invalid input or I/O failure.
 */
static int restok_write_exact_token(const char *path,
                                    const uint8_t token[RESUME_TOKEN_LEN]) {
  if (!path || !token)
    return ERR;

  int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  int fd = open(path, flags, 0600);
  if (fd < 0)
    return ERR;

  size_t off = 0;
  while (off < RESUME_TOKEN_LEN) {
    ssize_t n = write(fd, token + off, RESUME_TOKEN_LEN - off);
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
    off += (size_t)n;
  }

  if (fchmod(fd, 0600) != 0) {
    (void)close(fd);
    (void)unlink(path);
    return ERR;
  }

  struct stat st;
  if (fstat(fd, &st) != 0 || restok_validate_token_stat(&st) != OK) {
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

/* Initializes process-scoped resume-token persistence and policy checks.
 * Ownership: writes owned paths into caller-owned 'store'; caller must invoke
 * restok_clean() to free them.
 * Side effects: reads parent process identity, creates/validates storage
 * directory, and logs+disables persistence on policy failure.
 * Error semantics: returns OK when enabled, NO when disabled fail-safe, ERR on
 * invalid input.
 */
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

/* Loads a persisted raw token when present and policy-compliant.
 * Ownership: borrows 'store'; writes into caller-owned 'out'.
 * Side effects: performs filesystem I/O, may delete corrupted files, and may
 * disable resume persistence for this process.
 * Error semantics: returns YES when loaded, NO when absent/corrupted/disabled,
 * ERR on invalid input.
 */
int restok_load(ResumeTokenStore *store, uint8_t out[RESUME_TOKEN_LEN]) {
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
      return NO;
    restok_disable(store, "token file metadata read failed");
    return NO;
  }

  if (restok_validate_token_stat(&st) != OK) {
    restok_disable(store, "token file permissions/ownership are too open");
    return NO;
  }

  int rrc = restok_read_exact_token(store->token_path, out);
  if (rrc == YES)
    return YES;
  if (rrc == ERR)
    return ERR;

  // Corrupted token files are treated as stale and removed immediately.
  fprintf(stderr, "Token file corrupted, treating as stale\n");
  if (restok_delete_path(store->token_path) != OK) {
    restok_disable(store, "failed to delete corrupted token file");
  }
  return NO;
}

/* Persists a raw resume token for this process identity scope.
 * Ownership: borrows 'store' and 'token'; no caller-owned allocations.
 * Side effects: performs filesystem I/O and may disable persistence on policy
 * or write failures.
 * Error semantics: returns OK on success or when already disabled, ERR on
 * invalid input or write failure.
 */
int restok_store(ResumeTokenStore *store,
                 const uint8_t token[RESUME_TOKEN_LEN]) {
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

  if (restok_write_exact_token(store->token_path, token) != OK) {
    fprintf(stderr, "Failed to write token file: %s\n", strerror(errno));
    restok_disable(store, "token file write failed");
    return ERR;
  }
  return OK;
}

/* Deletes the persisted token file for this process identity scope.
 * Ownership: borrows 'store'; no allocations.
 * Side effects: performs filesystem I/O and may disable persistence on policy
 * failure.
 * Error semantics: returns OK when deleted/absent/disabled, ERR on invalid
 * input or filesystem failure.
 */
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

/* Releases heap memory owned by 'store' and resets it.
 * Ownership: consumes internal owned strings; does not free 'store'.
 * Side effects: frees memory and resets flags.
 * Error semantics: none.
 */
void restok_clean(ResumeTokenStore *store) {
  if (!store)
    return;
  restok_reset_paths(store);
  store->enabled = NO;
}
