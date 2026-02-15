#include "safety_policy.h"
#include "utils.h"

int safety_policy_init(SafetyPolicy *p, int *read_only, uint32_t *max_rows,
                       uint32_t *max_payload_bytes,
                       uint32_t *statement_timeout_ms) {
  if (!p)
    return ERR;

  p->read_only = read_only ? *read_only : 1;
  p->max_rows = max_rows ? *max_rows : 200;
  p->max_payload_bytes = max_payload_bytes ? *max_payload_bytes : 65536; // 64 kb
  p->statement_timeout_ms = statement_timeout_ms ? *statement_timeout_ms : 5000;
  p->column_mode = SAFETY_COLMODE_PSEUDONYMIZE;
  p->column_strategy = SAFETY_COLSTRAT_RANDOMIZED;
  return OK;
}
