#ifndef CRED_MANAGER_H
#define CRED_MANAGER_H

#include "utils.h"

typedef enum {
  CRED_MAN_SYNC = 0,
  CRED_MAN_TEST,
  CRED_MAN_RESET,
} CredManagerCommand;

typedef enum {
  CRED_MAN_RESET_SCOPE_NONE = 0,
  CRED_MAN_RESET_SCOPE_ALL,
  CRED_MAN_RESET_SCOPE_NAMESPACE,
} CredManagerResetScope;

typedef struct {
  CredManagerCommand cmd;
  union {
    const char *connection_name; // borrowed; optional for sync/test
    const char *cred_namespace;  // borrowed; optional for reset --namespace
  };
  CredManagerResetScope reset_scope; // valid only for CRED_MAN_RESET
} CredManagerReq;

/* Executes one credential-management command using the requested config input.
 *
 * Behavior:
 * - CRED_MAN_SYNC uses 'config_input' with confdir_open(); passing NULL means
 *   "use the default config path".
 * - CRED_MAN_TEST uses 'config_input' with confdir_open(); passing NULL means
 *   "use the default config path".
 * - CRED_MAN_RESET ignores 'config_input'.
 * - For CRED_MAN_RESET with reset_scope == CRED_MAN_RESET_SCOPE_ALL, the
 *   implementation removes all adbxplorer-managed credential state.
 * - For CRED_MAN_RESET with
 *   reset_scope == CRED_MAN_RESET_SCOPE_NAMESPACE, 'req->cred_namespace' must
 *   be a non-empty namespace string and the implementation removes only that
 *   namespace's secrets and state file.
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
