#ifndef VAULT_H
#define VAULT_H

#include "utils.h"

/* Returns YES if the vault is opened, NO if it's closed.
 * Ownership: does not allocate.
 * Side effects: none.
 * Error semantics: YES/NO, ERR on invalid usage (not expected in v1). */
int vault_is_opened(void);

#endif
