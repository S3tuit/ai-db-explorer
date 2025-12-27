#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libpq-fe.h>

#include "postgres_backend.h"
#include "query_result.h"
#include "safety_policy.h"
#include "utils.h"

/* ------------------------------- internals ------------------------------- */

typedef struct PgImpl {
    PGconn *conn;
    SafetyPolicy policy;
    uint8_t policy_applied;     // 1 if the policy has already been enforced
                                // at session level, else 0
    char last_err[1024];
} PgImpl;

/* Copyies 'msg' inside the last_err of 'p'. 'msg' may be NULL. */
static void pg_set_err(PgImpl *p, const char *msg) {
    if (!p) return;
    if (!msg) msg = "unknown error";
    snprintf(p->last_err, sizeof(p->last_err), "%s", msg);
}

/* Copyies 'prefix' + the last error that happened at 'conn' to 'p'. */
static void pg_set_err_pg(PgImpl *p, PGconn *conn, const char *prefix) {
    const char *e = conn ? PQerrorMessage(conn) : "no connection";
    if (!prefix) prefix = "postgres error";
    snprintf(p->last_err, sizeof(p->last_err), "%s: %s", prefix, e ? e : "");
}

/* Returns the millisecond of the system's monotonic time. It doesn't give
 * meaningfull info per se, it's used to calculate the ms taken between 2
 * actions. */
static uint64_t now_ms_monotonic(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* Executes one or more SQL commands (separated by ';') and requires COMMAND_OK.
 * Use this to send sql statements that don't return tuples. Returns ERR on bad
 * input or if the query produced an error. Stores error inside 'p'. */
static int pg_exec_command(PgImpl *p, const char *sql) {
    if (!p || !sql) return ERR;

    if (!p->conn) {
        pg_set_err_pg(p, p->conn, NULL);
        return ERR;
    }

    PGresult *res = PQexec(p->conn, sql);
    if (!res) {
        pg_set_err_pg(p, p->conn, "PQexec failed");
        return ERR;
    }
    

    ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK) {
        // Could be error, or could be tuples.
        // caller should use pg_exec() for tuples
        pg_set_err_pg(p, p->conn, sql);
        PQclear(res);
        return ERR;
    }

    PQclear(res);
    return OK;
}

/* Executes one or more SQL commands (separated by ';') ignoring their errors
 * if any is returned. */
static void pg_exec_command_ignore(PgImpl *p, const char *sql) {
    if (!p || !p->conn) return;
    PGresult *res = PQexec(p->conn, sql);
    if (res) PQclear(res);
}

/* Best-effort rollback, ignore errors. */
static void pg_rollback(PgImpl *p) {
    pg_exec_command(p, "ROLLBACK");
}

/* Executes commands so the current session of 'p' complies with 'p->policy'.
 * Must be called before running any query and the caller must checks this 
 * returned one before sending any query. Stores error inside 'p'. */
static int pg_apply_policy(PgImpl *p) {
    if (!p || !p->conn) return ERR;
    // bad things can happen if we let the max bytes to be low like 1/2...
    // this is a safe bound
    if (p->policy.max_cell_bytes < 256) {
        pg_set_err(p, "max_cell_bytes too low, it should be at least 256 bytes");
        return ERR;
    }

    // ignore failure, this is not strictly necessary
    pg_exec_command_ignore(p, "SET application_name to \'db-explorer\'");

    // safetyguards are optional, treat 0 as not set

    char buf[256];
    SafetyPolicy policy = p->policy;

    snprintf(buf, sizeof(buf), "SET default_transaction_read_only = %s",
            policy.read_only > 0 ? "on" : "off");

    // Ignore failure: older versions / permissions might differ.
    // read-only will be enforced per query.
    pg_exec_command_ignore(p, buf);

    if (policy.statement_timeout_ms > 0) {
        snprintf(buf, sizeof(buf), "SET statement_timeout = %u",
                 policy.statement_timeout_ms);
        if (pg_exec_command(p, buf) != OK) return ERR;
    }
    
    p->policy_applied = 1;
    return OK;
}

/* Executes 'sql' and returns the result inside 'out_res'. It verify the result
 * is just one. If there are more results it doesn't store anything and returns
 * ERR (single statement policy). */
static int pg_exec_single_result(PgImpl *p, const char *sql, PGresult **out_res) {
    if (!p || !p->conn || !sql || !out_res) return ERR;
    *out_res = NULL;

    if (PQsendQuery(p->conn, sql) != 1) {
        pg_set_err_pg(p, p->conn, "PQsendQuery failed");
        return ERR;
    }

    PGresult *first = NULL;
    PGresult *extra = NULL;

    for (;;) {
        PGresult *res = PQgetResult(p->conn);
        if (!res) break;

        if (!first) {
            first = res;
        } else {
            // second result => multi-statement or multiple commands
            extra = res;
            // consume remaining results to keep connection usable
            while ((res = PQgetResult(p->conn)) != NULL) {
                PQclear(res);
            }
            break;
        }
    }

    if (extra) {
        pg_set_err(p, "multiple statements/results are not allowed");
        PQclear(extra);
        if (first) PQclear(first);
        return ERR;
    }

    if (!first) {
        pg_set_err(p, "no result returned");
        return ERR;
    }

    *out_res = first;
    return OK;
}

/* --------------------------- DbBackend vtable --------------------------- */

static int pg_connect(DbBackend *db, const char *conninfo,
                        const SafetyPolicy *policy) {
    if (!db || !db->impl || !conninfo || !policy) return ERR;
    PgImpl *p = (PgImpl *)db->impl;

    // when created, NULL is assigned to p->conn. If it's not NULL, there's
    // another open connection
    if (p->conn) {
        PQfinish(p->conn);
        p->conn = NULL;
    }

    p->conn = PQconnectdb(conninfo);
    if (!p->conn) {
        pg_set_err(p, "PQconnectdb returned NULL");
        return ERR;
    }

    if (PQstatus(p->conn) != CONNECTION_OK) {
        pg_set_err_pg(p, p->conn, "connection failed");
        PQfinish(p->conn);
        p->conn = NULL;
        return ERR;
    }
    
    p->policy = *policy;
    p->policy_applied = 0;
    return OK;
}

static void pg_destroy(DbBackend *db) {
    if (!db || !db->impl) return;
    PgImpl *p = (PgImpl *)db->impl;

    if (p->conn) {
        PQfinish(p->conn);
        p->conn = NULL;
    }

    free(p);
    free(db);
}

static int pg_exec(DbBackend *db, uint32_t request_id, const char *sql,
                    QueryResult **out_qr) {
    
    const char *err_msg;
    QueryResult *qr = NULL;
    PGresult *res = NULL;

    // Error logging logic, if we called a function that sets the error like
    // pg_exec_command(), we use that error... else, we create the message.

    if (!db || !db->impl || !sql || !out_qr) {
        err_msg = "unexpected input before executing the query";
        goto fail_bad_input;
    };
    *out_qr = NULL;

    PgImpl *p = (PgImpl *)db->impl;
    if (!p->conn) {
        pg_set_err(p, "not connected");
        goto fail;
    }
    
    // apply safety policy
    if (!(p->policy_applied)) {
        if (pg_apply_policy(p) != OK) {
            goto fail;
        }
    }

    // start counting for query execution time
    uint64_t t0 = now_ms_monotonic();

    // start a read-only transaction for every query
    if (p->policy.read_only) {
        if (pg_exec_command(p, "BEGIN READ ONLY") != OK) {
            goto fail;
        }
    } else {
        if (pg_exec_command(p, "BEGIN") != OK) {
            goto fail;
        }
    }

    if (pg_exec_single_result(p, sql, &res) != OK) {
        goto fail;
    }

    ExecStatusType st = PQresultStatus(res);

    // if backend error, rollback and return error
    if (st == PGRES_FATAL_ERROR || st == PGRES_BAD_RESPONSE || st == PGRES_NONFATAL_ERROR) {
        // capture error string
        const char *msg = PQresultErrorMessage(res);
        if (!msg || !*msg) msg = PQerrorMessage(p->conn);

        pg_set_err(p, msg ? msg : "query failed");

        goto fail;
    }

    // Right now, the agent can't send commands like set, delete...
    // so the status should be PGRES_TUPLES_OK.
    // TODO: allow agent to run commands if permitted by user
    if (st == PGRES_TUPLES_OK) {
        int ncols = PQnfields(res);
        int ntuples = PQntuples(res);

        if (ncols < 0) ncols = 0;
        if (ntuples < 0) ntuples = 0;

        uint32_t out_cols = (uint32_t)ncols;
        uint32_t out_rows = (uint32_t)ntuples;

        uint8_t truncated = 0;
        if (p->policy.max_rows > 0 && out_rows > p->policy.max_rows) {
            out_rows = p->policy.max_rows;
            truncated = 1;
        }

        qr = qr_create_ok(request_id, (uint32_t)ncols, out_rows, truncated);
        if (!qr) {
            pg_set_err(p, "qr_create_ok error");
            goto fail;
        }

        // Column metadata
        for (uint32_t c = 0; c < out_cols; c++) {
            const char *name = PQfname(res, c);
            // Store empty strings if metadata missing
            if (!name) name = "";

            Oid oid = PQftype(res, c);
            char typebuf[32];
            // materialize Oid to a textual representation
            snprintf(typebuf, sizeof(typebuf), "%u", (unsigned)oid);

            if (qr_set_col(qr, c, name, typebuf) < 0) {
                pg_set_err(p, "qr_set_col failed");
                goto fail;
            }
        }

        // Fill cells (enforces max_cell_bytes by truncating before setting the
        // cell).
        uint32_t cap = p->policy.max_cell_bytes;
        for (uint32_t r = 0; r < out_rows; r++) {
            for (uint32_t c = 0; c < (uint32_t)ncols; c++) {
                
                char *val;
                if (PQgetisnull(res, r, c)) val = NULL;
                else val = PQgetvalue(res, (int)r, (int)c);

                if (qr_set_cell_capped(qr, r, c, val,
                            cap > 0 ? cap : UINT32_MAX) != OK) {
                    pg_set_err(p, "qr_set_cell failed");
                    goto fail;
                }
            }
        }
    } else {
        // Error status
        const char *msg = PQresStatus(st);
        pg_set_err(p, msg ? msg : "unexpected PGresult status");
        goto fail;
    }

    PQclear(res);
    res = NULL;

    // commit transaction
    if (pg_exec_command(p, "COMMIT") != OK) {
        // If commit fails, try rollback
        pg_rollback(p);
        pg_set_err(p, "COMMIT failure");
        goto fail;
    }

    uint64_t t1 = now_ms_monotonic();
    qr->exec_ms = (t1 >= t0) ? (t1 - t0) : 0;

    *out_qr = qr;
    return (*out_qr ? OK : ERR);

fail:
    if (!out_qr) return ERR; // catastrophic

    err_msg = p->last_err;
    // rollback is safe even if we haven't executed anything
    pg_rollback(p);
    if (res) PQclear(res);
    if (qr) qr_destroy(qr);
fail_bad_input:
    // if bad input, we can't rely on the buffer for the error of PgImpl
    *out_qr = qr_create_err(request_id, err_msg);
    return (*out_qr ? OK : ERR);
}

/* ------------------------- constructor ------------------------- */

static const DbBackendVTable PG_VT = {
    .connect = pg_connect,
    .destroy = pg_destroy,
    .exec = pg_exec
};

DbBackend *postgres_backend_create(void) {
    DbBackend *db = (DbBackend *)xcalloc(1, sizeof(DbBackend));

    PgImpl *impl = (PgImpl *)xcalloc(1, sizeof(PgImpl));

    impl->conn = NULL;
    impl->last_err[0] = '\0';

    db->vt = &PG_VT;
    db->impl = impl;
    return db;
}
