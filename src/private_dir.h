#ifndef PRIVATE_DIR_H
#define PRIVATE_DIR_H

#include <stdint.h>

#include "handshake_codec.h"
#include "utils.h"

// TODO: right now we don't support the server running with a different UID than
// the broker because the broker rejects it at handshake and because PrivDir
// creates files with 0600 perms. Add a way to let user choose to accept
// different UIDs.

#define PRIVDIR_APP_DIRNAME "adbxplorer"
#define PRIVDIR_RUN_DIRNAME "run"
#define PRIVDIR_SECRET_DIRNAME "secret"
#define PRIVDIR_SOCK_FILENAME "broker.sock"
#define PRIVDIR_TOKEN_FILENAME "token"
#define PRIVDIR_LOCK_FILENAME ".broker.lock"
#define PRIVDIR_TOKEN_LEN ADBX_SHARED_TOKEN_LEN

/* All strings are owned by the struct and freed by privdir_clean(). */
typedef struct PrivDir {
  char *base;       // e.g. /run/user/1000/ or /tmp/
  char *app_dir;    // e.g. /run/user/1000/adbxplorer/ or /tmp/adbxplorer-1000/
  char *run_dir;    // e.g. /run/user/1000/adbxplorer/run/
  char *secret_dir; // e.g. /run/user/1000/adbxplorer/secret/
  char *sock_path;  // e.g. /run/user/1000/adbxplorer/run/broker.sock
  char *token_path; // e.g. /run/user/1000/adbxplorer/secret/token
} PrivDir;

/* Broker-owned runtime rooted in one resolved private directory.
 * The runtime owns all file descriptors for one active broker instance.
 */
typedef struct PrivDirBrokerRuntime {
  int base_fd;
  int app_fd;
  int run_fd;
  int secret_fd;
  int lock_fd;
} PrivDirBrokerRuntime;

/* Reads env vars, computes all paths, and validates sun_path fit. If 'base' is
 * not NULL it uses it as the parent directory instead of resolving from env
 * vars. The app directory is "<base>/adbxplorer/" except when resolution falls
 * back to /tmp/, in which case it becomes "/tmp/adbxplorer-<uid>/".
 * 'base' must be an absolute path. No filesystem side effects.
 * On ERR, '*out_err' may receive an allocated error string that caller must
 * free. Returns NULL on error.
 */
PrivDir *privdir_resolve(const char *base, char **out_err);

/* Creates/opens the broker runtime rooted at 'pd', acquires the single-broker
 * lock, prepares run/ and secret/ directories, and writes a fresh shared token
 * file. If 'out_secret_token' is not NULL, it receives the generated token.
 * Returns owned runtime on success, NULL on any validation, lock, or I/O
 * failure.
 */
PrivDirBrokerRuntime *
privdir_broker_runtime_open(const PrivDir *pd,
                            uint8_t out_secret_token[PRIVDIR_TOKEN_LEN]);

/* Releases one broker runtime, removing socket/token/lock artifacts and best-
 * effort removing run/ and secret/ before closing owned descriptors. The app
 * dir itself is left in place.
 */
void privdir_broker_runtime_clean(PrivDirBrokerRuntime *rt);

/* Frees one resolved path set and all owned strings. */
void privdir_clean(PrivDir *pd);

#endif
