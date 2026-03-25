#ifndef APP_DIR_H
#define APP_DIR_H

#include <stdint.h>

#include "adbx_err.h"
#include "handshake_codec.h"
#include "utils.h"

// TODO: right now we don't support the server running with a different UID than
// the broker because the broker rejects it at handshake and because the
// app-dir runtime creates files with 0600 perms. Add a way to let user choose
// to accept different UIDs.

#define APPDIR_APP_DIRNAME "adbxplorer"
#define APPDIR_RUN_DIRNAME "run"
#define APPDIR_SECRET_DIRNAME "secret"
#define APPDIR_SOCK_FILENAME "broker.sock"
#define APPDIR_TOKEN_FILENAME "token"
#define APPDIR_LOCK_FILENAME ".broker.lock"
#define APPDIR_TOKEN_LEN ADBX_SHARED_TOKEN_LEN

typedef enum {
  APPDIR_ERR_NONE = 0,
  APPDIR_ERR_INPUT,
  APPDIR_ERR_ENV,
  APPDIR_ERR_PATH,
  APPDIR_ERR_POLICY,
  APPDIR_ERR_RUNTIME,
  APPDIR_ERR_LOCK,
} AppDirErrCode;

typedef struct {
  AppDirErrCode code;
  char msg[ADBX_ERRMSG_MAX];
} AppDirErr;

/* All strings are owned by the struct and freed by appdir_clean(). */
typedef struct AppDir {
  char *app_dir;    // e.g. /run/user/1000/adbxplorer/ or /tmp/adbxplorer-1000/
  char *run_dir;    // e.g. /run/user/1000/adbxplorer/run/
  char *secret_dir; // e.g. /run/user/1000/adbxplorer/secret/
  char *sock_path;  // e.g. /run/user/1000/adbxplorer/run/broker.sock
  char *token_path; // e.g. /run/user/1000/adbxplorer/secret/token
} AppDir;

/* Broker-owned runtime rooted in one resolved app directory.
 * The runtime owns all file descriptors for one active broker instance.
 */
typedef struct AppDirBrokerRuntime {
  int app_fd;
  int run_fd;
  int secret_fd;
  int lock_fd;
} AppDirBrokerRuntime;

/* Reads env vars, computes all paths, and validates sun_path fit.
 * - When 'app_dir_input' is NULL, the function resolves the parent runtime
 *   directory from env vars and derives an app dir named 'adbxplorer/' there,
 *   or '/tmp/adbxplorer-<uid>/' on the /tmp fallback path.
 * - When 'app_dir_input' is not NULL, it is treated as the exact app runtime
 *   directory and must be an absolute path.
 * No filesystem side effects.
 * On ERR, 'out_err' may receive one non-allocating typed error snapshot.
 * Returns NULL on error.
 */
AppDir *appdir_resolve(const char *app_dir_input, AppDirErr *out_err);

/* Creates/opens the broker runtime rooted at 'appd', acquires the single-broker
 * lock, prepares run/ and secret/ directories, and writes a fresh shared token
 * file. If 'out_secret_token' is not NULL, it receives the generated token.
 * On ERR, 'out_err' may receive one non-allocating typed error snapshot.
 * Returns owned runtime on success, NULL on any validation, lock, entropy, or
 * I/O failure.
 */
AppDirBrokerRuntime *
appdir_broker_runtime_open(const AppDir *appd,
                           uint8_t out_secret_token[APPDIR_TOKEN_LEN],
                           AppDirErr *out_err);

/* Releases one broker runtime, removing socket/token/lock artifacts and best-
 * effort removing run/ and secret/ before closing owned descriptors. The app
 * dir itself is left in place.
 */
void appdir_broker_runtime_clean(AppDirBrokerRuntime *rt);

/* Frees one resolved path set and all owned strings. */
void appdir_clean(AppDir *appd);

#endif
