#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "test.h"
#include "conn_catalog.h"
#include "postgres_backend.h"
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
    const char *json =
        "{"
        "  \"databases\": ["
        "    {"
        "      \"type\": \"postgres\","
        "      \"connectionName\": \"TestDb\","
        "      \"host\": \"localhost\","
        "      \"port\": 5432,"
        "      \"username\": \"postgres\","
        "      \"database\": \"postgres\","
        "      \"safeFunctions\": ["
        "        \"user.calc_balance\","
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
static void assert_validate_at(
        DbBackend *db,
        const ConnProfile *cp,
        const SafetyPolicy *policy,
        const char *sql,
        int expect_ok,
        ValidatorErrCode expect_code,
        const char *file,
        int line) {
    StrBuf msg = {0};
    ValidatorErr err = { .code = VERR_NONE, .msg = &msg };

    int rc = validate_query(db, cp, policy, sql, &err);
    if (expect_ok) {
        if (rc != OK) {
            fprintf(stderr, "validate_query failed: code=%d msg=",
                    (int)err.code);
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
                    (int)err.code,
                    (int)expect_code);
            if (msg.data && msg.len > 0) {
                fprintf(stderr, "%.*s", (int)msg.len, msg.data);
            } else {
                fprintf(stderr, "(null)");
            }
            fprintf(stderr, "\n");
        }
        ASSERT_TRUE_AT(rc == ERR, file, line);
        ASSERT_TRUE_AT(err.code == expect_code, file, line);
    }

    sb_clean(&msg);
}
#define ASSERT_VALIDATE(db, cp, policy, sql, ok, code) \
    assert_validate_at((db), (cp), (policy), (sql), (ok), (code), __FILE__, __LINE__)

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

    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "SELECT u.name FROM users u WHERE u.id = 1;",
        1,
        VERR_NONE);

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

    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "SELECT * FROM users u WHERE u.id = 1;",
        0,
        VERR_STAR);

    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "SELECT u.name FROM users u JOIN balance ON u.id = u.user_id WHERE u.id = 1;",
        0,
        VERR_NO_TABLE_ALIAS);

    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "SELECT u.id FROM users u WHERE u.status = $1;",
        0,
        VERR_PARAM_NON_SENSITIVE);

    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "SELECT u.id, e.amount FROM users u LEFT JOIN expenses e ON e.user_id = u.id "
        "WHERE u.fiscal_code = $1 LIMIT 10;",
        0,
        VERR_JOIN_NOT_INNER);

    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "SELECT u.id, e.amount FROM users u INNER JOIN expenses e ON e.receiver = u.fiscal_code "
        "WHERE u.id = 1 LIMIT 10;",
        0,
        VERR_JOIN_ON_SENSITIVE);

    fprintf(stderr, "Start here:\n");
    ASSERT_VALIDATE(
        db,
        cp,
        policy,
        "WITH tab1 AS (SELECT u.fiscal_code FROM users u WHERE u.id = 1) "
        "SELECT t.fiscal_code FROM tab1 t LIMIT 10;",
        0,
        VERR_SENSITIVE_OUTSIDE_MAIN);

    db_destroy(db);
    catalog_destroy(cat);
}

int main(void) {
    test_validator_accepts();
    test_validator_rejects_rules();
    fprintf(stderr, "OK: test_validator\n");
    return 0;
}
