#ifndef PROC_IDENTITY_H
#define PROC_IDENTITY_H

#include <stdint.h>
#include <sys/types.h>

#include "utils.h"

typedef struct ProcIdentity {
  pid_t pid;
  uint64_t start_time_ticks;
} ProcIdentity;

/* Resolves a stable identity for the MCP host process launching this server.
 * Ownership: writes into caller-owned 'out'; does not allocate.
 * Side effects: reads process metadata from OS proc interfaces.
 * Error semantics: returns OK on success, ERR on unsupported platform or parse
 * I/O failure.
 */
AdbxStatus procid_parent_identity(ProcIdentity *out);

#endif
