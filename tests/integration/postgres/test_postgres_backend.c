#include "test.h"

#include "postgres_backend.h"
#include "db_backend.h"
#include "utils.h"
#include "safety_policy.h"
#include "query_result.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* -------------------------------- helpers -------------------------------- */

static SafetyPolicy policy_default(void) {
    SafetyPolicy p = {0};
    p.read_only = 1;
    p.statement_timeout_ms = 2000;
    p.max_rows = 200;
    p.max_cell_bytes = 4096;
    return p;
}

/* libpq can read env vars, but specifying dbname keeps it explicit */
static const char *conninfo_env(void) {
    return "dbname=postgres";
}

static DbBackend *pg_connect_impl(const SafetyPolicy *policy, const char *file, int line) {
    DbBackend *pg = postgres_backend_create();
    ASSERT_TRUE_AT(pg != NULL, file, line);

    int rc = pg->vt->connect(pg, conninfo_env(), policy);
    ASSERT_TRUE_AT(rc == OK, file, line);

    return pg;
}
#define PG_CONNECT(policy) pg_connect_impl((policy), __FILE__, __LINE__)

static QueryResult *pg_exec_impl(DbBackend *pg, uint32_t id, const char *sql, const char *file, int line) {
    QueryResult *qr = NULL;
    int rc = pg->vt->exec(pg, id, sql, &qr);

    /* Contract: backend returns OK and always produces a QueryResult (OK or ERROR) */
    ASSERT_TRUE_AT(rc == OK, file, line);
    ASSERT_TRUE_AT(qr != NULL, file, line);
    ASSERT_TRUE_AT(qr->id == id, file, line);

    return qr;
}
#define PG_EXEC(pg, id, sql) pg_exec_impl((pg), (id), (sql), __FILE__, __LINE__)

static void assert_ok_qr(const QueryResult *qr, const char *file, int line) {
    ASSERT_TRUE_AT(qr != NULL, file, line);
    ASSERT_TRUE_AT(qr->status == QR_OK, file, line);
}
static void assert_err_qr(const QueryResult *qr, const char *file, int line) {
    ASSERT_TRUE_AT(qr != NULL, file, line);
    ASSERT_TRUE_AT(qr->status == QR_ERROR, file, line);
    ASSERT_TRUE_AT(qr->err_msg != NULL, file, line);
    ASSERT_TRUE_AT(qr->err_msg[0] != '\0', file, line);
}
#define ASSERT_OK_QR(qr)  assert_ok_qr((qr), __FILE__, __LINE__)
#define ASSERT_ERR_QR(qr) assert_err_qr((qr), __FILE__, __LINE__)

/* --------------------------------- tests --------------------------------- */

static void test_base_select_join(void) {
    SafetyPolicy p = policy_default();
    DbBackend *pg = PG_CONNECT(&p);

    QueryResult *qr = PG_EXEC(pg, 1,
        "SELECT z.name, r.race_name, z.height_cm "
        "FROM zfighters z "
        "JOIN races r ON r.id = z.race_id "
        "ORDER BY z.id");

    ASSERT_OK_QR(qr);
    ASSERT_TRUE(qr->ncols == 3);
    ASSERT_TRUE(qr->nrows >= 1);

    const QRColumn *c0 = qr_get_col((QueryResult *)qr, 0);
    const QRColumn *c1 = qr_get_col((QueryResult *)qr, 1);
    const QRColumn *c2 = qr_get_col((QueryResult *)qr, 2);
    ASSERT_TRUE(c0 && c1 && c2);
    ASSERT_STREQ(c0->name, "name");
    ASSERT_STREQ(c1->name, "race_name");
    ASSERT_STREQ(c2->name, "height_cm");

    int found_goku = 0;
    for (uint32_t row = 0; row < qr->nrows; row++) {
        const char *name = qr_get_cell(qr, row, 0);
        if (name && strcmp(name, "Goku") == 0) {
            found_goku = 1;
            break;
        }
    }
    ASSERT_TRUE(found_goku);

    qr_destroy(qr);
    pg->vt->destroy(pg);
}

static void test_select_null_cell(void) {
    SafetyPolicy p = policy_default();
    DbBackend *pg = PG_CONNECT(&p);

    QueryResult *qr = PG_EXEC(pg, 2, "SELECT NULL::text AS x, 'ok'::text AS y");

    ASSERT_OK_QR(qr);
    ASSERT_TRUE(qr->ncols == 2);
    ASSERT_TRUE(qr->nrows == 1);

    // Select NULL values should return NULL
    ASSERT_TRUE(qr_is_null(qr, 0, 0) == YES);
    ASSERT_TRUE(qr_get_cell(qr, 0, 0) == NULL);
    ASSERT_STREQ(qr_get_cell(qr, 0, 1), "ok");

    qr_destroy(qr);
    pg->vt->destroy(pg);
}

static void test_max_rows_truncates(void) {
    SafetyPolicy p = policy_default();
    p.max_rows = 3;
    DbBackend *pg = PG_CONNECT(&p);

    QueryResult *qr = PG_EXEC(pg, 3, "SELECT name FROM zfighters ORDER BY id");

    // Rows should be truncated when too many
    ASSERT_OK_QR(qr);
    ASSERT_TRUE(qr->ncols == 1);
    ASSERT_TRUE(qr->nrows == 3);
    ASSERT_TRUE(qr->truncated == 1);

    qr_destroy(qr);
    pg->vt->destroy(pg);
}

static void test_max_cell_bytes_caps_cell(void) {
    SafetyPolicy p = policy_default();
    p.max_cell_bytes = 256; // intentionally small
    DbBackend *pg = PG_CONNECT(&p);

    // A field should be cut when too long.
    QueryResult *qr = PG_EXEC(pg, 4, "SELECT repeat('a', 1000) AS big");

    ASSERT_OK_QR(qr);
    ASSERT_TRUE(qr->ncols == 1);
    ASSERT_TRUE(qr->nrows == 1);

    const char *cell = qr_get_cell(qr, 0, 0);
    ASSERT_TRUE(cell != NULL);

    // Must be <= max_cell_bytes-1 since they're C-string
    ASSERT_TRUE(strlen(cell) <= (size_t)(p.max_cell_bytes - 1));

    qr_destroy(qr);
    pg->vt->destroy(pg);
}

static void test_delete_fails_read_only(void) {
    SafetyPolicy p = policy_default();
    p.read_only = 1;
    DbBackend *pg = PG_CONNECT(&p);

    QueryResult *qr = PG_EXEC(pg, 5, "DELETE FROM zfighters WHERE name = 'Goku'");

    ASSERT_ERR_QR(qr);

    qr_destroy(qr);
    pg->vt->destroy(pg);
}

static void test_attempt_disable_readonly_still_cannot_write(void) {
    SafetyPolicy p = policy_default();
    p.read_only = 1;
    DbBackend *pg = PG_CONNECT(&p);

    QueryResult *qr1 = PG_EXEC(pg, 6, "SET default_transaction_read_only = off");
    ASSERT_ERR_QR(qr1);
    qr_destroy(qr1);

    QueryResult *qr2 = PG_EXEC(pg, 7, "DELETE FROM zfighters WHERE name = 'Vegeta'");
    ASSERT_ERR_QR(qr2);

    qr_destroy(qr2);
    pg->vt->destroy(pg);
}

static void test_long_running_query(void) {
    SafetyPolicy p = policy_default();
    p.statement_timeout_ms = 100;
    DbBackend *pg = PG_CONNECT(&p);

    QueryResult *qr = PG_EXEC(pg, 8, "SELECT pg_sleep(1);");
    ASSERT_ERR_QR(qr);

    qr_destroy(qr);
    pg->vt->destroy(pg);
}

void test_postgres_backend(void) {
    test_base_select_join();
    test_select_null_cell();
    test_max_rows_truncates();
    test_max_cell_bytes_caps_cell();
    test_delete_fails_read_only();
    test_attempt_disable_readonly_still_cannot_write();
    test_long_running_query();
}

int main(void) {
    test_postgres_backend();

    fprintf(stderr, "OK: test_postgres_backend\n");
    return 0;
}
