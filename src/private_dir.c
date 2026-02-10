#define _GNU_SOURCE

#include "file_io.h"
#include "private_dir.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

/* ----------------------------- Static helpers ----------------------------- */

/* mkdir with mode 0700. If the dir already exists, verify it is a directory,
 * owned by us, and has mode 0700. Returns OK/ERR. */
static int mkdir_0700(const char *path) {
  if (mkdir(path, 0700) == 0) {
    /* Override umask to ensure exact permissions. */
    if (chmod(path, 0700) != 0)
      return ERR;
    return OK;
  }

  if (errno != EEXIST)
    return ERR;

  /* Dir already exists — validate ownership and mode. */
  struct stat st;
  if (stat(path, &st) != 0)
    return ERR;
  if (!S_ISDIR(st.st_mode))
    return ERR;
  if (st.st_uid != getuid())
    return ERR;
  if ((st.st_mode & 0777) != 0700)
    return ERR;

  return OK;
}

/* Resolves the base directory path from environment variables.
 * Returns a malloc'd string or NULL on failure. */
static char *resolve_base_path(void) {
  const char *env_val = NULL;

#ifdef __linux__
  env_val = getenv("XDG_RUNTIME_DIR");
#elif defined(__APPLE__)
  env_val = getenv("TMPDIR");
#endif

  char *base = NULL;

  if (env_val && env_val[0] == '/' && strlen(env_val) < 200) {
    /* Use env var: <env_val>/ai-dbexplorer/ */
    size_t len = strlen(env_val);
    /* Strip trailing slash if present for consistent formatting. */
    while (len > 1 && env_val[len - 1] == '/')
      len--;

    size_t base_len = len + 1 + strlen(PRIVDIR_APPNAME) + 2; /* /app/ + NUL */
    base = xmalloc(base_len);
    snprintf(base, base_len, "%.*s/%s/", (int)len, env_val, PRIVDIR_APPNAME);
  } else {
    /* Fallback: /tmp/ai-dbexplorer-<uid>/ */
    char buf[128];
    int n = snprintf(buf, sizeof(buf), "/tmp/%s-%u/", PRIVDIR_APPNAME,
                     (unsigned)getuid());
    if (n < 0 || (size_t)n >= sizeof(buf))
      return NULL;
    base = xmalloc((size_t)n + 1);
    memcpy(base, buf, (size_t)n + 1);
  }

  return base;
}

/* ------------------------------- Public API ------------------------------- */

PrivDir *privdir_resolve(const char *input_base) {
  char *base; // malloc'd string

  // no base in input, resolve from env vars
  if (!input_base || input_base[0] != '/') {
    base = resolve_base_path();
    if (!base) {
      return NULL;
    }

    // validate the path in input
  } else {
    size_t len = strlen(input_base);

    /* Ensure trailing slash. */
    int needs_slash = (len == 0 || input_base[len - 1] != '/');
    size_t alloc = len + (needs_slash ? 1 : 0) + 1;
    base = xmalloc(alloc);
    memcpy(base, input_base, len);
    if (needs_slash)
      base[len++] = '/';
    base[len] = '\0';
  }

  /* Validate that the full socket path fits in sun_path. */
  struct sockaddr_un sun_check;
  size_t base_len = strlen(base);
  size_t sock_full_len =
      base_len + strlen("run/") + strlen(PRIVDIR_SOCK_FILENAME);
  if (sock_full_len >= sizeof(sun_check.sun_path)) {
    free(base);
    return NULL;
  }

  /* Build all paths relative to base. */
  size_t run_len = base_len + strlen("run/") + 1;
  char *run_dir = xmalloc(run_len);
  snprintf(run_dir, run_len, "%srun/", base);

  size_t secret_len = base_len + strlen("secret/") + 1;
  char *secret_dir = xmalloc(secret_len);
  snprintf(secret_dir, secret_len, "%ssecret/", base);

  size_t sock_len = strlen(run_dir) + strlen(PRIVDIR_SOCK_FILENAME) + 1;
  char *sock_path = xmalloc(sock_len);
  snprintf(sock_path, sock_len, "%s%s", run_dir, PRIVDIR_SOCK_FILENAME);

  size_t token_len = strlen(secret_dir) + strlen(PRIVDIR_TOKEN_FILENAME) + 1;
  char *token_path = xmalloc(token_len);
  snprintf(token_path, token_len, "%s%s", secret_dir, PRIVDIR_TOKEN_FILENAME);

  PrivDir *pd = xcalloc(1, sizeof(PrivDir));
  pd->base = base;
  pd->run_dir = run_dir;
  pd->secret_dir = secret_dir;
  pd->sock_path = sock_path;
  pd->token_path = token_path;

  return pd;
}

int privdir_create_layout(const PrivDir *pd) {
  if (!pd)
    return ERR;
  if (mkdir_0700(pd->base) != OK)
    return ERR;
  if (mkdir_0700(pd->run_dir) != OK)
    return ERR;
  if (mkdir_0700(pd->secret_dir) != OK)
    return ERR;
  return OK;
}

int privdir_generate_token(const PrivDir *pd) {
  if (!pd || !pd->token_path)
    return ERR;

  uint8_t token[PRIVDIR_TOKEN_LEN];
  if (fill_random(token, sizeof(token)) != OK)
    return ERR;

  int fd = open(pd->token_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0)
    return ERR;

  ssize_t written = write(fd, token, sizeof(token));
  if (written != (ssize_t)sizeof(token)) {
    close(fd);
    unlink(pd->token_path);
    return ERR;
  }

  if (fchmod(fd, 0600) != 0) {
    close(fd);
    unlink(pd->token_path);
    return ERR;
  }

  close(fd);
  return OK;
}

/* Reads token_path into a temporary buffer and enforces exact token length.
 * Ownership: borrows 'pd'; writes token bytes into caller-owned 'out'.
 * Side effects: performs filesystem I/O and clears temporary token memory.
 * Error semantics: returns OK when exactly PRIVDIR_TOKEN_LEN bytes are read,
 * ERR on invalid input, read failure, or invalid token length. */
int privdir_read_token(const PrivDir *pd, uint8_t *out) {
  if (!pd || !pd->token_path || !out)
    return ERR;

  StrBuf sb = {0};
  if (fileio_read_all_limit(pd->token_path, PRIVDIR_TOKEN_LEN, &sb) != OK)
    return ERR;
  if (sb.len != PRIVDIR_TOKEN_LEN) {
    sb_zero_clean(&sb);
    return ERR;
  }

  memcpy(out, sb.data, PRIVDIR_TOKEN_LEN);
  sb_zero_clean(&sb);
  return OK;
}

void privdir_cleanup(const PrivDir *pd) {
  if (!pd)
    return;
  /* Best-effort: ignore errors. */
  if (pd->sock_path)
    (void)unlink(pd->sock_path);
  if (pd->token_path)
    (void)unlink(pd->token_path);
  if (pd->secret_dir)
    (void)rmdir(pd->secret_dir);
  if (pd->run_dir)
    (void)rmdir(pd->run_dir);
  if (pd->base)
    (void)rmdir(pd->base);
}

void privdir_free(PrivDir *pd) {
  if (!pd)
    return;
  free(pd->base);
  free(pd->run_dir);
  free(pd->secret_dir);
  free(pd->sock_path);
  free(pd->token_path);
  free(pd);
}
