#ifndef CRED_MANAGER_H
#define CRED_MANAGER_H

#include "utils.h"

typedef enum {
  CRED_MAN_SYNC = 0,
  CRED_MAN_TEST,
  CRED_MAN_PRUNE,
  CRED_MAN_LIST,
} CredManagerCommand;

typedef struct {
  CredManagerCommand cmd;
  const char *connection_name; // borrowed; optional for sync/test
  int is_everything;           // valid only for prune
} CredManagerReq;

/* Executes one credential-management command using the requested config input.
 *
 * Behavior:
 * - CRED_MAN_SYNC requires 'config_input' to identify the config file to sync.
 * - CRED_MAN_TEST requires 'config_input' to identify the config file to test.
 * - CRED_MAN_PRUNE with is_everything == 0 requires 'config_input' so the
 *   implementation can resolve the credential namespace from the config.
 * - CRED_MAN_PRUNE with is_everything != 0 must ignore 'config_input' and
 *   remove all adbxplorer-managed credential state.
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
