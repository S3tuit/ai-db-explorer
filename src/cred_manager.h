#ifndef CRED_MANAGER_H
#define CRED_MANAGER_H

#include "utils.h"

typedef enum {
  CRED_MAN_SYNC = 0,
  CRED_MAN_TEST,
  CRED_MAN_PRUNE,
  CRED_MAN_RESET,
  CRED_MAN_LIST,
} CredManagerCommand;

typedef struct {
  CredManagerCommand cmd;
  const char *connection_name; // borrowed; optional for sync/test
} CredManagerReq;

/* Executes one credential-management command using the requested config input.
 *
 * Behavior:
 * - CRED_MAN_SYNC uses 'config_input' with confdir_open(); passing NULL means
 *   "use the default config path".
 * - CRED_MAN_TEST uses 'config_input' with confdir_open(); passing NULL means
 *   "use the default config path".
 * - CRED_MAN_PRUNE uses 'config_input' with confdir_open(); passing NULL means
 *   "use the default config path".
 * - CRED_MAN_RESET ignores 'config_input' and removes all adbxplorer-managed
 *   credential state.
 * - CRED_MAN_LIST may use 'config_input' when the implementation needs to
 *   report status for one config; otherwise it may be NULL.
 *
 * Ownership:
 * - It borrows 'req' and 'config_input'.
 * - On ERR, '*out_err' may receive a heap-allocated diagnostic string that the
 *   caller must free.
 *
 * Side effects:
 * - May open config files, read/write credential state, prompt on the
 *   terminal, and talk to the active SecretStore backend.
 *
 * Error semantics:
 * - Returns OK on success.
 * - Returns ERR on invalid input, config resolution/load failures,
 *   secret-store failures, terminal interaction failures, or backend test
 *   failures. On ERR, '*out_err' contains best-effort user-facing context.
 */
AdbxStatus cred_manager_execute(const CredManagerReq *req,
                                const char *config_input, char **out_err);

#endif
