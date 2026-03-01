#ifndef PRIVATE_DIR_H
#define PRIVATE_DIR_H

#include <stddef.h>
#include <stdint.h>

#include "handshake_codec.h"
#include "utils.h"

#define PRIVDIR_APPNAME "adbxplorer"
#define PRIVDIR_SOCK_FILENAME "broker.sock"
#define PRIVDIR_TOKEN_FILENAME "token"
#define PRIVDIR_TOKEN_LEN ADBX_SHARED_TOKEN_LEN

/* All strings are owned by the struct and freed by privdir_free(). */
typedef struct PrivDir {
  char *base;       /* e.g. /run/user/1000/adbxplorer/          */
  char *run_dir;    /* e.g. /run/user/1000/adbxplorer/run/      */
  char *secret_dir; /* e.g. /run/user/1000/adbxplorer/secret/   */
  char *sock_path;  /* e.g. /run/user/1000/adbxplorer/run/broker.sock */
  char *token_path; /* e.g. /run/user/1000/adbxplorer/secret/token    */
} PrivDir;

/* Reads env vars, computes all paths, validates sun_path fit. If 'base' is not
 * NULL it uses it as the base directory instead of resolving from env vars.
 * 'base' must be an absolute path. No filesystem side effects. Returns NULL on
 * error. */
PrivDir *privdir_resolve(const char *base);

/* mkdir with 0700 for base, run/, secret/. Verifies ownership if dir already
 * exists. Returns OK/ERR. */
AdbxStatus privdir_create_layout(const PrivDir *pd);

/* Fills PRIVDIR_TOKEN_LEN bytes from OS CSPRNG, writes to token_path with 0600
 * perms.
 * Returns OK/ERR. */
AdbxStatus privdir_generate_token(const PrivDir *pd);

/* Reads PRIVDIR_TOKEN_LEN bytes from token_path into 'out'. Returns OK/ERR. */
AdbxStatus privdir_read_token(const PrivDir *pd, uint8_t *out);

/* Best-effort unlink of socket + token, rmdir of all dirs. */
void privdir_cleanup(const PrivDir *pd);

/* Frees struct and owned strings (does NOT remove files). */
void privdir_free(PrivDir *pd);

#endif
