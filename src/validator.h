#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "conn_catalog.h"
#include "db_backend.h"
#include "string_op.h"

typedef enum ValidatorErrCode {
  VERR_NONE = 0,
  VERR_PARSE_FAIL,             /* parser/IR failure */
  VERR_UNSUPPORTED_QUERY,      /* unsupported query structure */
  VERR_ANALYZE_FAIL,           /* internal analysis failure */
  VERR_VAULT_CLOSED,           /* sensitive mode required but vault closed */
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
  VERR_JOIN_ON_SENSITIVE,  /* JOIN ON references sensitive columns */
  VERR_DISTINCT_SENSITIVE, /* DISTINCT in sensitive mode */
  VERR_OFFSET_SENSITIVE,   /* OFFSET in sensitive mode */
  VERR_LIMIT_REQUIRED,     /* LIMIT missing in sensitive mode */
  VERR_LIMIT_EXCEEDS       /* LIMIT too high in sensitive mode */
} ValidatorErrCode;

typedef struct ValidatorErr {
  ValidatorErrCode code;
  StrBuf *msg;
} ValidatorErr;

/* Validates a SQL query against the global and sensitive-mode policies.
 * Returns OK if the query is valid, else, ERR and allocates a human-readable
 * error message into err->msg (guaranteed).
 */
int validate_query(DbBackend *db, const ConnProfile *cp,
                   const SafetyPolicy *policy, const char *sql,
                   ValidatorErr *err);

#endif
