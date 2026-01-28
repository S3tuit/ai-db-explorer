#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "conn_catalog.h"
#include "db_backend.h"
#include "string_op.h"

/* Validates a SQL query against the global and sensitive-mode policies.
 * Returns OK if the query is valid, else, ERR and allocates a human-readable
 * error string into err_msg (guaranteed).
 *
 * Ownership: Caller takes ownership of err_msg.
 */
int validate_query(DbBackend *db, const ConnProfile *cp, const SafetyPolicy *policy,
                  const char *sql, StrBuf *err_msg);

#endif
