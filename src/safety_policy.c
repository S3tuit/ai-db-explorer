#include "safety_policy.h"
#include "utils.h"

int safety_policy_init(SafetyPolicy *p, int *read_only,
                                uint32_t *max_rows, uint32_t *max_cell_bytes,
                                uint32_t *statement_timeout_ms) {
    if (!p) return ERR;

    p->read_only = read_only ? *read_only : 1;
    p->max_rows = max_rows ? *max_rows : 200;
    p->max_cell_bytes = max_cell_bytes ? *max_cell_bytes : 65536; // 64 kb
    // with a low max_cell_bytes many things can go wrong, for example if it's
    // 0 we can't even set null term, if it's 2 we can set the '...\0' in case
    // of a too long result. To avoid headaches set a min value
    if (p->max_cell_bytes < 56) p->max_cell_bytes = 56;
    p->statement_timeout_ms = statement_timeout_ms ? *statement_timeout_ms : 5000;
    return OK;
}
