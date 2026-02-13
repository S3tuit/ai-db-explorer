#ifndef RESUME_TOKEN_H
#define RESUME_TOKEN_H

#include <stdint.h>

#include "handshake_codec.h"

/* All strings are owned by the struct and freed by restok_clean(). */
typedef struct ResumeTokenStore {
  int enabled;      /* YES when persistence is active, NO when fail-disabled. */
  char *dir_path;   /* e.g. /run/user/1000/ai-dbexplorer-mcp */
  char *token_path; /* e.g. /run/user/1000/ai-dbexplorer-mcp/token-<pid>-<ts> */
} ResumeTokenStore;

/* Initializes process-scoped resume-token persistence.
 * Ownership: writes owned paths into caller-owned 'store'. Caller must call
 * restok_clean() exactly once to free owned memory.
 * Side effects: resolves parent identity, creates/validates storage directory,
 * and can log+disable resume when policy checks fail.
 * Error semantics: returns YES when enabled, NO when disabled fail-safe for
 * this process, ERR on invalid input.
 */
int restok_init(ResumeTokenStore *store);

/* Loads a persisted token into 'out' if available.
 * Ownership: borrows 'store'; writes into caller-owned 'out'.
 * Side effects: performs filesystem I/O; may delete corrupted files; may
 * disable resume for this process on policy/critical failures.
 * Error semantics: returns YES when token is loaded, NO when absent/corrupted/
 * disabled, ERR on invalid input.
 */
int restok_load(ResumeTokenStore *store, uint8_t out[ADBX_RESUME_TOKEN_LEN]);

/* Stores the provided raw token bytes for this process scope.
 * Ownership: borrows 'store' and 'token'; no caller-owned allocations.
 * Side effects: performs filesystem I/O; may disable resume for this process
 * on policy/write failures.
 * Error semantics: returns OK on success or no-op when disabled, ERR on invalid
 * input or write failure.
 */
int restok_store(ResumeTokenStore *store,
                 const uint8_t token[ADBX_RESUME_TOKEN_LEN]);

/* Deletes the persisted token for this process scope.
 * Ownership: borrows 'store'; no caller-owned allocations.
 * Side effects: performs filesystem I/O.
 * Error semantics: returns OK when deleted/absent/disabled, ERR on invalid
 * input or filesystem failure.
 */
int restok_delete(ResumeTokenStore *store);

/* Frees any memory owned by 'store' and resets it to empty.
 * Ownership: consumes internal owned strings but not 'store' itself.
 * Side effects: frees heap memory.
 * Error semantics: none.
 */
void restok_clean(ResumeTokenStore *store);

#endif
