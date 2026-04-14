#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "conn_catalog.h"
#include "db_backend.h"
#include "packed_array.h"
#include "sensitive_tok.h"
#include "string_op.h"
#include "utils.h"

typedef enum ValidatorErrCode {
  VERR_NONE = 0,
  VERR_PARSE_FAIL,             /* parser/IR failure */
  VERR_UNSUPPORTED_QUERY,      /* unsupported query structure */
  VERR_ANALYZE_FAIL,           /* internal analysis failure */
  VERR_NO_TABLE_ALIAS,         /* missing table alias */
  VERR_NO_COLUMN_ALIAS,        /* missing/unknown column alias */
  VERR_STAR,                   /* SELECT * or alias.* */
  VERR_SENSITIVE_OUTSIDE_MAIN, /* sensitive col referenced outside main query */
  VERR_FUNC_UNSAFE,            /* unsafe function call */
  VERR_PARAM_OUTSIDE_WHERE,    /* params used outside WHERE */
  VERR_PARAM_NON_SENSITIVE,    /* params compared to non-sensitive column */
  VERR_SENSITIVE_SELECT_EXPR,  /* sensitive col not simple colref in SELECT */
  VERR_SENSITIVE_LOC,          /* sensitive col in disallowed clause */
  VERR_SENSITIVE_CMP,   /* sensitive col compared with non =/IN or non-param */
  VERR_WHERE_NOT_CONJ,  /* WHERE not AND-only */
  VERR_JOIN_NOT_INNER,  /* non-INNER join */
  VERR_JOIN_ON_INVALID, /* JOIN ON not AND/= or invalid operands */
  VERR_JOIN_ON_SENSITIVE,        /* JOIN ON references sensitive columns */
  VERR_PARAM_IDX_RANGE,          /* $n index outside provided parameter list */
  VERR_PARAM_DOMAIN_MISMATCH,    /* token parameter domain mismatch */
  VERR_PARAM_UNUSED,             /* provided token parameter not referenced */
  VERR_EXPLAIN_PARAMS_FORBIDDEN, /* EXPLAIN* cannot use bound input params */
  VERR_DISTINCT_SENSITIVE,       /* DISTINCT in sensitive mode */
  VERR_OFFSET_SENSITIVE,         /* OFFSET in sensitive mode */
  VERR_LIMIT_REQUIRED,           /* LIMIT missing in sensitive mode */
  VERR_LIMIT_EXCEEDS,            /* LIMIT too high in sensitive mode */
  VERR_SENSITIVE_IN_UNION         /* sensitive col in a set-op branch */
} ValidatorErrCode;

typedef struct ValidatorErr {
  ValidatorErrCode code;
  StrBuf msg;
} ValidatorErr;

typedef enum ValidatorColOutKind {
  VCOL_OUT_PLAINTEXT = 0,
  VCOL_OUT_TOKEN = 1
} ValidatorColOutKind;

typedef struct ValidatorColPlan {
  ValidatorColOutKind kind;
  const char *domain; // borrowed from ConnProfile; NULL for plaintext
  uint32_t domain_len;
} ValidatorColPlan;

// When the plan mode is VPLAN_MODE_PASSTHROUGH_PLAINTEXT, the validator
// doesn't know how many columns will result, but it knows they can safely be
// plaintext data.
typedef enum ValidatorPlanMode {
  VPLAN_MODE_SELECT = 0,
  VPLAN_MODE_PASSTHROUGH_PLAINTEXT = 1
} ValidatorPlanMode;

typedef struct ValidatorPlan {
  ValidatorPlanMode mode;
  PackedArray *cols; // entries are ValidatorColPlan when mode is SELECT
} ValidatorPlan;

/* Output contract for validate_query().
 * Ownership:
 * - caller initializes with vq_out_init() and cleans with vq_out_clean().
 * - plan/err storage is owned by this struct.
 */
typedef struct ValidateQueryOut {
  ValidatorPlan plan;
  ValidatorErr err;
} ValidateQueryOut;

/* Input contract for validate_query().
 * Ownership:
 * - all pointers are borrowed.
 * - sql must be a NUL-terminated C string. */
typedef struct ValidatorRequest {
  DbBackend *db;
  const ConnProfile *profile;
  const char *sql;
  const SensitiveTok *params; // borrowed positional list; params[i] => $(i+1)
  uint32_t nparams;
} ValidatorRequest;

/* Initializes one ValidateQueryOut.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
AdbxStatus vq_out_init(ValidateQueryOut *out);

/* Cleans one ValidateQueryOut and all memory it owns. */
void vq_out_clean(ValidateQueryOut *out);

/* Validates a SQL query against the global and sensitive-mode policies.
 * On success, returns OK and fills out->plan. SELECT-mode plans are aligned
 * with SELECT output columns; passthrough mode marks backend result columns as
 * plaintext regardless of shape. Also sets out->err.code=VERR_NONE.
 * On failure, returns ERR and fills out->err with a human-readable message.
 */
AdbxStatus validate_query(const ValidatorRequest *req, ValidateQueryOut *out);

#endif
