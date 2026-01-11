#ifndef SAFETY_POLICY_H
#define SAFETY_POLICY_H

#include <stdint.h>

/*
 * DB-agnostic entity that stores safety knobs. These represent intent, each DB
 * backend decidesvhow to enforce them.
 *
 * If values different from 'read_only' are 0, it means unlimited.
 */
typedef struct SafetyPolicy {
    int read_only;                  // 1 = enforce read-only
    uint32_t statement_timeout_ms;
    uint32_t max_rows;              // how many rows the resulting QueryResult
                                    // will have at max. The DB may still compute
                                    // more rows.

    // TODO: this should become the max bytes of the whole QueryResult
    uint32_t max_cell_bytes;        // how many bytes can contain at most one
                                    // cell of the QueryResult.
} SafetyPolicy;

/*
 * Initializes 'p' with safe defaults if some pointer points to NULL. Returns
 * OK on success, ERR on bad input.
 */
int safety_policy_init(SafetyPolicy *p, int *read_only, uint32_t *max_rows,
                        uint32_t *max_cell_bytes, uint32_t *statement_timeout_ms);

#endif
