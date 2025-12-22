#include "safety_policy.h"

int safety_policy_init_defaults(SafetyPolicy *p, int *read_only,
                                uint32_t *max_rows, uint32_t *max_cell_bytes,
                                uint32_t *statement_timeout_ms) {
    if (!p) return 0;

    p->read_only = read_only ? *read_only : 1;
    p->max_rows = max_rows ? *max_rows : 200;
    p->max_cell_bytes = max_cell_bytes ? *max_cell_bytes : 65536; // 64 kb
    p->statement_timeout_ms = statement_timeout_ms ? *statement_timeout_ms : 5000;
    return 1;
}
