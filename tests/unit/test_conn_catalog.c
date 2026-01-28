#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "test.h"
#include "conn_catalog.h"
#include "safety_policy.h"

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

/* Ensures missing safetyPolicy falls back to default values. */
static void test_missing_policy_defaults(void) {
  const char *json =
    "{"
    "  \"databases\": []"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);

  SafetyPolicy *p = catalog_get_policy(cat);
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(p->read_only == 1);
  ASSERT_TRUE(p->max_rows == 200);
  ASSERT_TRUE(p->max_query_bytes == 65536);
  ASSERT_TRUE(p->statement_timeout_ms == 5000);
  ASSERT_TRUE(catalog_count(cat) == 0);

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures missing policy fields fall back to defaults. */
static void test_policy_missing_fields_defaults(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"No UnSafe\""
    "  },"
    "  \"databases\": []"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);

  SafetyPolicy *p = catalog_get_policy(cat);
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(p->read_only == 0);
  ASSERT_TRUE(p->max_rows == 200);
  ASSERT_TRUE(p->max_query_bytes == 65536);
  ASSERT_TRUE(p->statement_timeout_ms == 5000);

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures maxQueryKiloBytes maps into max_query_bytes. */
static void test_policy_kilobytes(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"yes\","
    "    \"statementTimeoutMs\": 5000,"
    "    \"maxRowReturned\": 200,"
    "    \"maxQueryKiloBytes\": 1"
    "  },"
    "  \"databases\": []"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);

  SafetyPolicy *p = catalog_get_policy(cat);
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(p->max_query_bytes == 1024);

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures unknown safetyPolicy keys cause an error. */
static void test_policy_unknown_key_fails(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"yes\","
    "    \"unknown\": 1"
    "  },"
    "  \"databases\": []"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  unlink(path);
  free(path);
}

/* Ensures overflowed numeric fields fail validation. */
static void test_policy_overflow_fails(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"yes\","
    "    \"maxQueryKiloBytes\": 4294967295"
    "  },"
    "  \"databases\": []"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  unlink(path);
  free(path);
}

/* Validates that an empty databases array is accepted. */
static void test_empty_databases_ok(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"yes\","
    "    \"statementTimeoutMs\": 5000,"
    "    \"maxRowReturned\": 200"
    "  },"
    "  \"databases\": []"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(catalog_count(cat) == 0);

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures unknown keys inside a database entry reject the catalog. */
static void test_db_entry_unknown_key_fails(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"yes\","
    "    \"statementTimeoutMs\": 5000,"
    "    \"maxRowReturned\": 200"
    "  },"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"unknownKey\": \"oops\""
    "    }"
    "  ]"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  unlink(path);
  free(path);
}

/* Ensures a valid config maps fields to the right ConnProfile values. */
static void test_valid_config_maps_fields(void) {
  const char *json =
    "{"
    "  \"safetyPolicy\": {"
    "    \"readOnly\": \"no\","
    "    \"statementTimeoutMs\": 1234,"
    "    \"maxRowReturned\": 99"
    "  },"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"db.example\","
    "      \"port\": 5432,"
    "      \"username\": \"alice\","
    "      \"database\": \"mydb\","
    "      \"options\": \"sslmode=disable\""
    "    }"
    "  ]"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(catalog_count(cat) == 1);

  SafetyPolicy *p = catalog_get_policy(cat);
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(p->read_only == 0);
  ASSERT_TRUE(p->max_rows == 99);
  ASSERT_TRUE(p->statement_timeout_ms == 1234);

  ConnProfile *cp = catalog_get_by_name(cat, "MyPostgres");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->kind == DB_KIND_POSTGRES);
  ASSERT_STREQ(cp->connection_name, "MyPostgres");
  ASSERT_STREQ(cp->host, "db.example");
  ASSERT_TRUE(cp->port == 5432);
  ASSERT_STREQ(cp->user, "alice");
  ASSERT_STREQ(cp->db_name, "mydb");
  ASSERT_STREQ(cp->options, "sslmode=disable");

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures columnPolicy lowercases identifiers and accepts mixed case. */
static void test_column_policy_lowercase(void) {
  const char *json =
    "{"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"columnPolicy\": {"
    "        \"pseudonymize\": {"
    "          \"deterministic\": ["
    "            \"Users.Email\","
    "            \"Private.Users.Phone\""
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

  ConnProfile *cp = catalog_get_by_name(cat, "MyPostgres");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->col_policy.n_rules == 2);
  ASSERT_STREQ(cp->col_policy.rules[0].table, "users");
  ASSERT_STREQ(cp->col_policy.rules[0].col, "email");
  ASSERT_STREQ(cp->col_policy.rules[1].table, "users");
  ASSERT_STREQ(cp->col_policy.rules[1].col, "phone");

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures duplicated deterministic entries are de-duplicated. */
static void test_column_policy_dedup(void) {
  const char *json =
    "{"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"columnPolicy\": {"
    "        \"pseudonymize\": {"
    "          \"deterministic\": ["
    "            \"users.email\","
    "            \"USERS.EMAIL\","
    "            \"users.email\""
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

  ConnProfile *cp = catalog_get_by_name(cat, "MyPostgres");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->col_policy.n_rules == 1);
  ASSERT_STREQ(cp->col_policy.rules[0].table, "users");
  ASSERT_STREQ(cp->col_policy.rules[0].col, "email");

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures global rules win and schema list is preserved. */
static void test_column_policy_global_and_schema(void) {
  const char *json =
    "{"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"columnPolicy\": {"
    "        \"pseudonymize\": {"
    "          \"deterministic\": ["
    "            \"users.email\","
    "            \"private.users.email\""
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

  ConnProfile *cp = catalog_get_by_name(cat, "MyPostgres");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->col_policy.n_rules == 1);
  ASSERT_TRUE(cp->col_policy.rules[0].is_global == 1);
  ASSERT_TRUE(cp->col_policy.rules[0].n_schemas == 1);
  ASSERT_STREQ(cp->col_policy.rules[0].schemas[0], "private");

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

/* Ensures malformed columnPolicy entries fail catalog load. */
static void test_column_policy_malformed_fails(void) {
  const char *json =
    "{"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"columnPolicy\": {"
    "        \"pseudonymize\": {"
    "          \"deterministic\": ["
    "            \"justcolumn\""
    "          ]"
    "        }"
    "      }"
    "    }"
    "  ]"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  unlink(path);
  free(path);
}

/* Ensures randomized rules are rejected in v1. */
static void test_column_policy_randomized_fails(void) {
  const char *json =
    "{"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"columnPolicy\": {"
    "        \"pseudonymize\": {"
    "          \"randomizedd\": ["
    "            \"cards.number\""
    "          ]"
    "        }"
    "      }"
    "    }"
    "  ]"
    "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  unlink(path);
  free(path);
}

/* Validates connp_is_col_sensitive behavior for global and schema-scoped rules. */
static void test_connp_is_col_sensitive(void) {
  const char *json =
    "{"
    "  \"databases\": ["
    "    {"
    "      \"type\": \"postgres\","
    "      \"connectionName\": \"MyPostgres\","
    "      \"host\": \"127.0.0.1\","
    "      \"port\": 5432,"
    "      \"username\": \"user\","
    "      \"database\": \"db\","
    "      \"columnPolicy\": {"
    "        \"pseudonymize\": {"
    "          \"deterministic\": ["
    "            \"users.email\","
    "            \"private.users.name\""
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

  ConnProfile *cp = catalog_get_by_name(cat, "MyPostgres");
  ASSERT_TRUE(cp != NULL);

  ASSERT_TRUE(connp_is_col_sensitive(cp, "", "users", "email") == YES);
  ASSERT_TRUE(connp_is_col_sensitive(cp, "public", "users", "email") == YES);
  ASSERT_TRUE(connp_is_col_sensitive(cp, "private", "users", "email") == YES);

  ASSERT_TRUE(connp_is_col_sensitive(cp, "private", "users", "name") == YES);
  ASSERT_TRUE(connp_is_col_sensitive(cp, "public", "users", "name") == NO);
  ASSERT_TRUE(connp_is_col_sensitive(cp, "", "users", "name") == YES);

  ASSERT_TRUE(connp_is_col_sensitive(cp, "", "users", "age") == NO);

  catalog_destroy(cat);
  unlink(path);
  free(path);
}

int main(void) {
  test_missing_policy_defaults();
  test_policy_missing_fields_defaults();
  test_policy_kilobytes();
  test_policy_unknown_key_fails();
  test_policy_overflow_fails();
  test_empty_databases_ok();
  test_db_entry_unknown_key_fails();
  test_valid_config_maps_fields();
  test_column_policy_lowercase();
  test_column_policy_dedup();
  test_column_policy_global_and_schema();
  test_column_policy_malformed_fails();
  test_column_policy_randomized_fails();
  test_connp_is_col_sensitive();
  fprintf(stderr, "OK: test_conn_catalog\n");
  return 0;
}
