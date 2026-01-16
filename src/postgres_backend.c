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
#include "conn_catalog.h"
#include "log.h"

enum {
    PG_QUERY_MAX_BYTES = 8192
};

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

static int pg_connect(DbBackend *db, const ConnProfile *profile,
                        const SafetyPolicy *policy, const char *pwd) {
    if (!db || !db->impl || !profile || !policy) return ERR;
    PgImpl *p = (PgImpl *)db->impl;

    // when created, NULL is assigned to p->conn. If it's not NULL, there's
    // another open connection
    if (p->conn) {
        PQfinish(p->conn);
        p->conn = NULL;
    }

    const char *port_str = NULL;
    char portbuf[16];
    if (profile->port > 0) {
        snprintf(portbuf, sizeof(portbuf), "%u", (unsigned)profile->port);
        port_str = portbuf;
    }

    const char *keys[] = {
        "host", "port", "dbname", "user", "password", "options", NULL
    };
    const char *vals[] = {
        profile->host,
        port_str,
        profile->db_name,
        profile->user,
        pwd,
        profile->options,
        NULL
    };

    p->conn = PQconnectdbParams(keys, vals, 0);
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

static int pg_is_connected(DbBackend *db) {
    if (!db || !db->impl) return ERR;
    PgImpl *p = (PgImpl *)db->impl;
    if (!p->conn) return NO;
    return (PQstatus(p->conn) == CONNECTION_OK) ? YES : NO;
}

static void pg_disconnect(DbBackend *db) {
    if (!db || !db->impl) return;
    PgImpl *p = (PgImpl *)db->impl;
    if (p->conn) {
        PQfinish(p->conn);
        p->conn = NULL;
    }
    p->policy_applied = 0;
}

static void pg_destroy(DbBackend *db) {
    if (!db || !db->impl) return;
    pg_disconnect(db);
    PgImpl *p = (PgImpl *)db->impl;
    free(p);
    free(db);
}

static int pg_exec(DbBackend *db, const McpId *request_id, const char *sql,
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

    // even if this limit is version-dependent, it's a defensive check
    if (strlen(sql) > PG_QUERY_MAX_BYTES) {
        pg_set_err(p, "SQL exceeds 8192 bytes (libpq query buffer limit)");
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

        uint8_t result_truncated = 0;
        if (p->policy.max_rows > 0 && out_rows > p->policy.max_rows) {
            out_rows = p->policy.max_rows;
            result_truncated = 1;
        }

        qr = qr_create_ok(request_id, (uint32_t)ncols, out_rows,
                          result_truncated, p->policy.max_query_bytes);
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

        // Fill cells (enforces max_query_bytes by stopping when the cap is hit).
        int stop = 0;
        for (uint32_t r = 0; r < out_rows; r++) {
            for (uint32_t c = 0; c < (uint32_t)ncols; c++) {
                
                char *val;
                if (PQgetisnull(res, r, c)) val = NULL;
                else val = PQgetvalue(res, (int)r, (int)c);

                int src = qr_set_cell(qr, r, c, val);
                if (src == NO) {
                    qr->result_truncated = 1;
                    qr->nrows = r;
                    stop = 1;
                    break;
                }
                if (src == ERR) {
                    pg_set_err(p, "qr_set_cell failed");
                    goto fail;
                }
            }
            if (stop) break;
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
    TLOG("ERROR - pg_exec failed: %s", err_msg ? err_msg : "unknown");
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
    .is_connected = pg_is_connected,
    .disconnect = pg_disconnect,
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
