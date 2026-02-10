#include "test.h"

#include "conn_catalog.h"
#include "db_backend.h"
#include "postgres_backend.h"
#include "query_result.h"
#include "safety_policy.h"
#include "utils.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* -------------------------------- helpers -------------------------------- */

static SafetyPolicy policy_default(void) {
  SafetyPolicy p = {0};
  p.read_only = 1;
  p.statement_timeout_ms = 2000;
  p.max_rows = 200;
  p.max_query_bytes = 4096;
  return p;
}

static const char *env_or_null(const char *name) {
  const char *val = getenv(name);
  return (val && val[0] != '\0') ? val : NULL;
}

static uint16_t env_u16_or_zero(const char *name) {
  const char *val = getenv(name);
  if (!val || val[0] == '\0')
    return 0;
  unsigned long v = strtoul(val, NULL, 10);
  if (v > UINT16_MAX)
    return 0;
  return (uint16_t)v;
}

static ConnProfile profile_default(void) {
  ConnProfile p = {0};
  p.connection_name = "pg_test";
  p.kind = DB_KIND_POSTGRES;
  p.host = env_or_null("PGHOST");
  p.port = env_u16_or_zero("PGPORT");
  p.db_name = env_or_null("PGDATABASE");
  p.user = env_or_null("PGUSER");
  p.options = NULL;
  return p;
}

static DbBackend *pg_connect_impl(const SafetyPolicy *policy, const char *file,
                                  int line) {
  DbBackend *pg = postgres_backend_create();
  ASSERT_TRUE_AT(pg != NULL, file, line);

  ConnProfile profile = profile_default();
  if (!profile.db_name)
    profile.db_name = "postgres";
  const char *pwd = env_or_null("PGPASSWORD");
  int rc = db_connect(pg, &profile, policy, pwd);
  if (rc != OK) {
    // Avoid leaking pg/impl in assertion-fail paths.
    db_destroy(pg);
  }
  ASSERT_TRUE_AT(rc == OK, file, line);

  return pg;
}
#define PG_CONNECT(policy) pg_connect_impl((policy), __FILE__, __LINE__)

static QueryResult *pg_exec_impl(DbBackend *pg, const char *sql,
                                 const char *file, int line) {
  QueryResult *qr = NULL;
  int rc = db_exec(pg, sql, &qr);

  /* Contract: backend returns OK and always produces a QueryResult (OK or
   * ERROR) */
  ASSERT_TRUE_AT(rc == OK, file, line);
  ASSERT_TRUE_AT(qr != NULL, file, line);

  return qr;
}
#define PG_EXEC(pg, sql) pg_exec_impl((pg), (sql), __FILE__, __LINE__)

static void assert_ok_qr(const QueryResult *qr, const char *file, int line) {
  ASSERT_TRUE_AT(qr != NULL, file, line);
  ASSERT_TRUE_AT(qr->status == QR_OK, file, line);
}
static void assert_tool_err_qr(const QueryResult *qr, const char *file,
                               int line) {
  ASSERT_TRUE_AT(qr != NULL, file, line);
  ASSERT_TRUE_AT(qr->status == QR_TOOL_ERROR, file, line);
  ASSERT_TRUE_AT(qr->err_msg != NULL, file, line);
  ASSERT_TRUE_AT(qr->err_msg[0] != '\0', file, line);
}
#define ASSERT_OK_QR(qr) assert_ok_qr((qr), __FILE__, __LINE__)
#define ASSERT_TOOL_ERR_QR(qr) assert_tool_err_qr((qr), __FILE__, __LINE__)

/* --------------------------------- tests --------------------------------- */

static void test_base_select_join(void) {
  SafetyPolicy p = policy_default();
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr = PG_EXEC(pg, "SELECT z.name, r.race_name, z.height_cm "
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
  db_destroy(pg);
}

static void test_select_null_cell(void) {
  SafetyPolicy p = policy_default();
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr = PG_EXEC(pg, "SELECT NULL::text AS x, 'ok'::text AS y");

  ASSERT_OK_QR(qr);
  ASSERT_TRUE(qr->ncols == 2);
  ASSERT_TRUE(qr->nrows == 1);

  // Select NULL values should return NULL
  ASSERT_TRUE(qr_is_null(qr, 0, 0) == YES);
  ASSERT_TRUE(qr_get_cell(qr, 0, 0) == NULL);
  ASSERT_STREQ(qr_get_cell(qr, 0, 1), "ok");

  qr_destroy(qr);
  db_destroy(pg);
}

static void test_max_rows_truncates(void) {
  SafetyPolicy p = policy_default();
  p.max_rows = 3;
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr = PG_EXEC(pg, "SELECT name FROM zfighters ORDER BY id");

  // Rows should be truncated when too many
  ASSERT_OK_QR(qr);
  ASSERT_TRUE(qr->ncols == 1);
  ASSERT_TRUE(qr->nrows == 3);
  ASSERT_TRUE(qr->result_truncated == 1);

  qr_destroy(qr);
  db_destroy(pg);
}

static void test_max_query_bytes_truncates_result(void) {
  SafetyPolicy p = policy_default();
  p.max_query_bytes = 5; // allow only one 5-byte cell
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr = PG_EXEC(pg, "SELECT '12345' AS v "
                                "UNION ALL "
                                "SELECT '67890' AS v");

  ASSERT_OK_QR(qr);
  ASSERT_TRUE(qr->ncols == 1);
  ASSERT_TRUE(qr->nrows == 1);
  ASSERT_TRUE(qr->result_truncated == 1);
  ASSERT_STREQ(qr_get_cell(qr, 0, 0), "12345");

  qr_destroy(qr);
  db_destroy(pg);
}

static void test_delete_fails_read_only(void) {
  SafetyPolicy p = policy_default();
  p.read_only = 1;
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr = PG_EXEC(pg, "DELETE FROM zfighters WHERE name = 'Goku'");

  ASSERT_TOOL_ERR_QR(qr);

  qr_destroy(qr);
  db_destroy(pg);
}

static void test_attempt_disable_readonly_still_cannot_write(void) {
  SafetyPolicy p = policy_default();
  p.read_only = 1;
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr1 = PG_EXEC(pg, "SET default_transaction_read_only = off");
  ASSERT_TOOL_ERR_QR(qr1);
  qr_destroy(qr1);

  QueryResult *qr2 = PG_EXEC(pg, "DELETE FROM zfighters WHERE name = 'Vegeta'");
  ASSERT_TOOL_ERR_QR(qr2);

  qr_destroy(qr2);
  db_destroy(pg);
}

static void test_long_running_query(void) {
  SafetyPolicy p = policy_default();
  p.statement_timeout_ms = 100;
  DbBackend *pg = PG_CONNECT(&p);

  QueryResult *qr = PG_EXEC(pg, "SELECT pg_sleep(1);");
  ASSERT_TOOL_ERR_QR(qr);

  qr_destroy(qr);
  db_destroy(pg);
}

static void test_conn_options_applied(void) {
  SafetyPolicy p = policy_default();
  DbBackend *pg = postgres_backend_create();
  ASSERT_TRUE(pg != NULL);

  ConnProfile profile = profile_default();
  if (!profile.db_name)
    profile.db_name = "postgres";
  profile.options = "-c search_path=pg_catalog";
  const char *pwd = env_or_null("PGPASSWORD");
  int rc = db_connect(pg, &profile, &p, pwd);
  ASSERT_TRUE(rc == OK);

  QueryResult *qr = PG_EXEC(pg, "SHOW search_path");
  ASSERT_OK_QR(qr);
  ASSERT_TRUE(qr->ncols == 1);
  ASSERT_TRUE(qr->nrows == 1);
  ASSERT_STREQ(qr_get_cell(qr, 0, 0), "pg_catalog");

  qr_destroy(qr);
  db_destroy(pg);
}

void test_postgres_backend(void) {
  test_base_select_join();
  test_select_null_cell();
  test_max_rows_truncates();
  test_max_query_bytes_truncates_result();
  test_delete_fails_read_only();
  test_attempt_disable_readonly_still_cannot_write();
  test_long_running_query();
  test_conn_options_applied();
}

int main(void) {
  test_postgres_backend();

  fprintf(stderr, "OK: test_postgres_backend\n");
  return 0;
}
