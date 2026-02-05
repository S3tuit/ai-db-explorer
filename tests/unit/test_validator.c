#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "conn_catalog.h"
#include "postgres_backend.h"
#include "test.h"
#include "validator.h"

/* Writes JSON content to a temp file and returns its path.
 * Caller owns the returned path string and must unlink it. */
static char *write_tmp_config(const char *json) {
  char tmpl[] = "/tmp/adbxcfgXXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_TRUE(fd >= 0);

  size_t len = strlen(json);
  ssize_t n = write(fd, json, len);
  ASSERT_TRUE(n == (ssize_t)len);
  close(fd);

  return strdup(tmpl);
}

/* Builds a catalog with the test policy and returns it.
 * Ownership: caller owns the catalog and must destroy it. */
static ConnCatalog *load_test_catalog(void) {
  const char *json = "{"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"TestDb\","
                     "      \"host\": \"localhost\","
                     "      \"port\": 5432,"
                     "      \"username\": \"postgres\","
                     "      \"database\": \"postgres\","
                     "      \"safeFunctions\": ["
                     "        \"users.calc_balance\","
                     "        \"transfer_amount\""
                     "      ],"
                     "      \"columnPolicy\": {"
                     "        \"pseudonymize\": {"
                     "          \"deterministic\": ["
                     "            \"users.fiscal_code\","
                     "            \"users.card_code\","
                     "            \"private.cards.balance\","
                     "            \"expenses.receiver\""
                     "          ]"
                     "        }"
                     "      }"
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  unlink(path);
  free(path);
  return cat;
}

/* Runs a single validation and checks for accept/reject + error code.
 * Side effects: allocates and frees a validator error buffer. */
static void assert_validate_at(DbBackend *db, const ConnProfile *cp,
                               const SafetyPolicy *policy, const char *sql,
                               int expect_ok, ValidatorErrCode expect_code,
                               const char *expect_substr, const char *file,
                               int line) {
  StrBuf msg = {0};
  ValidatorErr err = {.code = VERR_NONE, .msg = &msg};

  int rc = validate_query(db, cp, policy, sql, &err);
  if (expect_ok) {
    if (rc != OK) {
      fprintf(stderr, "validate_query failed: code=%d msg=", (int)err.code);
      if (msg.data && msg.len > 0) {
        fprintf(stderr, "%.*s", (int)msg.len, msg.data);
      } else {
        fprintf(stderr, "(null)");
      }
      fprintf(stderr, "\n");
    }
    ASSERT_TRUE_AT(rc == OK, file, line);
    ASSERT_TRUE_AT(err.code == VERR_NONE, file, line);
  } else {
    if (rc != ERR) {
      fprintf(stderr, "validate_query unexpectedly OK\n");
    } else if (err.code != expect_code) {
      fprintf(stderr, "validate_query wrong code: got=%d expected=%d msg=",
              (int)err.code, (int)expect_code);
      if (msg.data && msg.len > 0) {
        fprintf(stderr, "%.*s", (int)msg.len, msg.data);
      } else {
        fprintf(stderr, "(null)");
      }
      fprintf(stderr, "\n");
    }
    ASSERT_TRUE_AT(rc == ERR, file, line);
    ASSERT_TRUE_AT(err.code == expect_code, file, line);
    if (expect_substr) {
      ASSERT_TRUE_AT(msg.data != NULL, file, line);
      size_t hay_len = msg.len;
      size_t needle_len = strlen(expect_substr);
      int found = 0;
      if (needle_len == 0) {
        found = 1;
      } else if (hay_len >= needle_len) {
        for (size_t i = 0; i + needle_len <= hay_len; i++) {
          if (memcmp(msg.data + i, expect_substr, needle_len) == 0) {
            found = 1;
            break;
          }
        }
      }
      ASSERT_TRUE_AT(found, file, line);
    }
  }

  sb_clean(&msg);
}
#define ASSERT_VALIDATE(db, cp, policy, sql, ok, code)                         \
  assert_validate_at((db), (cp), (policy), (sql), (ok), (code), NULL,          \
                     __FILE__, __LINE__)
#define ASSERT_VALIDATE_MSG(db, cp, policy, sql, ok, code, substr)             \
  assert_validate_at((db), (cp), (policy), (sql), (ok), (code), (substr),      \
                     __FILE__, __LINE__)

/* Basic validation cases for the policy rules and error codes. */
static void test_validator_accepts(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "TestDb");
  ASSERT_TRUE(cp != NULL);

  SafetyPolicy *policy = catalog_get_policy(cat);
  ASSERT_TRUE(policy != NULL);

  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  ASSERT_VALIDATE(db, cp, policy, "SELECT u.name FROM users u WHERE u.id = 1;",
                  1, VERR_NONE);

  db_destroy(db);
  catalog_destroy(cat);
}

static void test_validator_rejects_rules(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "TestDb");
  ASSERT_TRUE(cp != NULL);

  SafetyPolicy *policy = catalog_get_policy(cat);
  ASSERT_TRUE(policy != NULL);

  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  ASSERT_VALIDATE(db, cp, policy, "SELECT * FROM users u WHERE u.id = 1;", 0,
                  VERR_STAR);

  ASSERT_VALIDATE_MSG(db, cp, policy,
                      "SELECT u.name FROM users u JOIN balance ON u.id = "
                      "u.user_id WHERE u.id = 1;",
                      0, VERR_NO_TABLE_ALIAS, "Missing alias in JOIN item");

  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.status = $1;", 0,
                  VERR_PARAM_NON_SENSITIVE);

  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u LEFT JOIN expenses e ON "
                  "e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 10;",
                  0, VERR_JOIN_NOT_INNER);

  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.receiver = u.fiscal_code "
                  "WHERE u.id = 1 LIMIT 10;",
                  0, VERR_JOIN_ON_SENSITIVE);

  ASSERT_VALIDATE_MSG(
      db, cp, policy,
      "WITH tab1 AS (SELECT u.fiscal_code FROM users u WHERE u.id = 1) "
      "SELECT t.fiscal_code FROM tab1 t LIMIT 10;",
      0, VERR_SENSITIVE_OUTSIDE_MAIN, "fiscal_code");

  db_destroy(db);
  catalog_destroy(cat);
}

/* Runs the full set of validator_tests cases. */
static void test_validator_from_notes(void) {
  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat != NULL);
  ConnProfile *cp = catalog_get_by_name(cat, "TestDb");
  ASSERT_TRUE(cp != NULL);
  SafetyPolicy *policy = catalog_get_policy(cat);
  ASSERT_TRUE(policy != NULL);
  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db != NULL);

  /* ACCEPT cases */
  ASSERT_VALIDATE(db, cp, policy, "SELECT u.name FROM users u WHERE u.id = 1;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.country, COUNT(*) FROM users u INNER JOIN expenses "
                  "e ON e.user_id = u.id "
                  "WHERE u.status = true GROUP BY u.country;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT c.id, c.type FROM cards c WHERE c.type = (SELECT "
                  "ct.code FROM card_types ct WHERE ct.id = 1) "
                  "ORDER BY c.id LIMIT 50;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT LOWER(u.email) AS email_lc FROM users u WHERE u.id = 1;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.fiscal_code, u.name FROM users u WHERE u.id = 1 LIMIT 10;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code = $1 LIMIT 200;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code IN ($1, $2, $3) LIMIT 200;",
      1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT e.category, COUNT(*) FROM users u INNER JOIN "
                  "expenses e ON e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 GROUP BY e.category LIMIT 200;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT DISTINCT u.status FROM users u WHERE u.status = true;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.status = false LIMIT 10 OFFSET 10;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT transfer_amount(u.id, (SELECT u2.id FROM users u2 "
                  "WHERE u2.amount = 123), 50) AS bal "
                  "FROM users u WHERE u.id = 1;",
                  1, VERR_NONE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT users.calc_balance(u.id) AS bal FROM users u WHERE u.id = 1;", 1,
      VERR_NONE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT LOWER(c.balance) AS lb FROM public.cards c LIMIT 10;",
                  1, VERR_NONE);

  /* REJECT cases */
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.status, COUNT(*) FROM users u WHERE u.fiscal_code = $1 "
      "GROUP BY u.status HAVING COUNT(*) > $2 LIMIT 200;",
      0, VERR_PARAM_OUTSIDE_WHERE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT LOWER(c.balance) AS lb FROM private.cards c LIMIT 10;", 0,
      VERR_SENSITIVE_SELECT_EXPR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, u.name FROM users u WHERE u.status = $1 AND "
                  "u.fiscal_code = $2 LIMIT 200;",
                  0, VERR_PARAM_NON_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy, "SELECT * FROM users u WHERE u.id = 1;", 0,
                  VERR_STAR);
  ASSERT_VALIDATE(db, cp, policy, "SELECT u.* FROM users u WHERE u.id = 101;",
                  0, VERR_STAR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.name, e.* FROM users u INNER JOIN expenses e ON "
                  "e.user_id = u.id WHERE u.id = $1;",
                  0, VERR_STAR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.name FROM users u CROSS JOIN cards WHERE u.id = 1;",
                  0, VERR_NO_TABLE_ALIAS);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.name, e.amount FROM users u INNER JOIN expenses ON "
                  "expenses.user_id = u.id WHERE u.id = 1;",
                  0, VERR_ANALYZE_FAIL);
  ASSERT_VALIDATE_MSG(db, cp, policy,
                      "SELECT LOWER2(u.fiscal_code) FROM users u LIMIT 199;", 0,
                      VERR_FUNC_UNSAFE, "lower2");
  ASSERT_VALIDATE_MSG(db, cp, policy,
                      "SELECT u.id FROM users u WHERE u.fiscal_code = "
                      "'ABCDEF12G34H567I' LIMIT 200;",
                      0, VERR_SENSITIVE_CMP, "u.fiscal_code");
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code IN ('A', 'B') LIMIT 200;",
      0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code = $1 OR "
                  "u.status = false LIMIT 200;",
                  0, VERR_WHERE_NOT_CONJ);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE NOT (u.fiscal_code = $1) LIMIT 200;", 0,
      VERR_WHERE_NOT_CONJ);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code <> $1 LIMIT 200;", 0,
      VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code LIKE $1 LIMIT 200;", 0,
      VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code BETWEEN $1 AND "
                  "$2 LIMIT 200;",
                  0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code = (SELECT 'A' "
                  "FROM users x WHERE x.id = 1) "
                  "LIMIT 200;",
                  0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.receiver = u.fiscal_code "
                  "WHERE u.id = 1 LIMIT 200;",
                  0, VERR_JOIN_ON_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u LEFT JOIN expenses e ON "
                  "e.user_id = u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 10;",
                  0, VERR_JOIN_NOT_INNER);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u CROSS JOIN expenses e "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  0, VERR_JOIN_NOT_INNER);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.user_id = u.id OR e.user_id = 1 "
                  "WHERE u.fiscal_code = $2 LIMIT 200;",
                  0, VERR_JOIN_ON_INVALID);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.user_id > u.id "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  0, VERR_JOIN_ON_INVALID);
  // TODO: we might relax this; allow subqueries and functions inside JOIN ON
  // if both side are non-sensitive, func are safe and JOIN ON still has only
  // = and AND.
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "LOWER(e.category) = u.status "
                  "WHERE u.fiscal_code = $1 LIMIT 200;",
                  0, VERR_JOIN_ON_INVALID);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code, COUNT(*) FROM users u WHERE u.status "
                  "= true GROUP BY u.fiscal_code LIMIT 200;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.fiscal_code = $1 ORDER BY "
                  "u.fiscal_code LIMIT 200;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.status, COUNT(*) FROM users u WHERE u.status = "
                  "false GROUP BY u.status "
                  "HAVING MAX(u.fiscal_code) IS NOT NULL LIMIT 200;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT DISTINCT u.status FROM users u WHERE u.fiscal_code = $1;", 0,
      VERR_DISTINCT_SENSITIVE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code = $1 LIMIT 10 OFFSET 10;",
      0, VERR_OFFSET_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy,
                  "WITH tab1 AS (SELECT u.fiscal_code FROM users u WHERE u.id "
                  "= 2) SELECT t.fiscal_code FROM tab1 t LIMIT 10;",
                  0, VERR_SENSITIVE_OUTSIDE_MAIN);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.id = (SELECT x.id FROM "
                  "users x WHERE x.fiscal_code = $1) LIMIT 10;",
                  0, VERR_SENSITIVE_OUTSIDE_MAIN);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT t.id FROM (SELECT u.id FROM users u WHERE "
                  "u.fiscal_code = $1) t LIMIT 10;",
                  0, VERR_SENSITIVE_OUTSIDE_MAIN);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT public.u FROM users u WHERE u.id = 1;", 0,
                  VERR_NO_COLUMN_ALIAS);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.\"name\" FROM \"users\" u WHERE u.\"fiscaL_code\" "
                  "= 'A' LIMIT 10;",
                  0, VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy, "SELECT u.fiscal_code FROM users u;", 0,
                  VERR_LIMIT_REQUIRED);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.id FROM users u WHERE u.fiscal_code IN ($1, 'X') LIMIT 10;", 0,
      VERR_SENSITIVE_CMP);
  ASSERT_VALIDATE(db, cp, policy, "SELECT $1 AS trick FROM users u LIMIT 1;", 0,
                  VERR_PARAM_OUTSIDE_WHERE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code FROM users u LIMIT 201;", 0,
                  VERR_LIMIT_EXCEEDS);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT calc_balance(u.id) AS bal FROM users u WHERE u.id = 1;", 0,
      VERR_FUNC_UNSAFE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code AS fc FROM users u WHERE u.fiscal_code "
                  "= $1 ORDER BY fc LIMIT 10;",
                  0, VERR_SENSITIVE_LOC);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.fiscal_code AS fc, COUNT(*) FROM users u WHERE "
                  "u.status = true GROUP BY fc LIMIT 10;",
                  0, VERR_NO_COLUMN_ALIAS);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.status = $1 LIMIT 10;", 0,
                  VERR_PARAM_NON_SENSITIVE);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id FROM users u WHERE u.id = (SELECT x.id FROM "
                  "users x WHERE x.id = $1) LIMIT 10;",
                  0, VERR_PARAM_NON_SENSITIVE);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT u.status, MAX(u.fiscal_code) AS m FROM users u GROUP BY u.status "
      "HAVING m IS NOT NULL LIMIT 10;",
      0, VERR_NO_COLUMN_ALIAS);
  ASSERT_VALIDATE(
      db, cp, policy,
      "SELECT row_number() OVER (ORDER BY u.fiscal_code) AS rn FROM users u "
      "WHERE u.fiscal_code = $1 LIMIT 10;",
      0, VERR_SENSITIVE_SELECT_EXPR);
  ASSERT_VALIDATE(db, cp, policy,
                  "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON "
                  "e.receiver = $1 "
                  "WHERE u.fiscal_code = $2 LIMIT 10;",
                  0, VERR_PARAM_OUTSIDE_WHERE);

  db_destroy(db);
  catalog_destroy(cat);
}

int main(void) {
  test_validator_accepts();
  test_validator_rejects_rules();
  test_validator_from_notes();
  fprintf(stderr, "OK: test_validator\n");
  return 0;
}
