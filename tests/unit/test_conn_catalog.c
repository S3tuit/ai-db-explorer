#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "conn_catalog.h"
#include "safety_policy.h"
#include "test.h"

/* Ensures empty safetyPolicy falls back to default values. */
static void test_missing_policy_defaults(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"DefaultDb\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);

  SafetyPolicy *p = &cat->policy;
  ASSERT_TRUE(p->read_only == 1);
  ASSERT_TRUE(p->max_rows == 200);
  ASSERT_TRUE(p->max_payload_bytes == 65536);
  ASSERT_TRUE(p->statement_timeout_ms == 5000);
  ASSERT_TRUE(p->column_mode == SAFETY_COLMODE_PSEUDONYMIZE);
  ASSERT_TRUE(p->column_strategy == SAFETY_COLSTRAT_RANDOMIZED);
  ASSERT_TRUE(catalog_count(cat) == 1);

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Helper to get a ConnProfile that matches 'connection_name'. */
static ConnProfile *catalog_get_by_name(ConnCatalog *cat,
                                        const char *connection_name) {
  if (!cat || !connection_name)
    return NULL;
  for (size_t i = 0; i < cat->n_profiles; i++) {
    ConnProfile *p = &cat->profiles[i];
    if (p->connection_name &&
        strcmp(p->connection_name, connection_name) == 0) {
      return p;
    }
  }
  return NULL;
}

/* Ensures missing policy fields fall back to defaults. */
static void test_policy_missing_fields_defaults(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"readOnly\": \"No UnSafe\""
                     "  },"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"PartialDb\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);

  SafetyPolicy *p = &cat->policy;
  ASSERT_TRUE(p->read_only == 0);
  ASSERT_TRUE(p->max_rows == 200);
  ASSERT_TRUE(p->max_payload_bytes == 65536);
  ASSERT_TRUE(p->statement_timeout_ms == 5000);
  ASSERT_TRUE(p->column_mode == SAFETY_COLMODE_PSEUDONYMIZE);
  ASSERT_TRUE(p->column_strategy == SAFETY_COLSTRAT_RANDOMIZED);

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Ensures maxPayloadKiloBytes maps into max_payload_bytes. */
static void test_policy_kilobytes(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"readOnly\": \"yes\","
                     "    \"statementTimeoutMs\": 5000,"
                     "    \"maxRowReturned\": 200,"
                     "    \"maxPayloadKiloBytes\": 1"
                     "  },"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"KbDb\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat != NULL);

  SafetyPolicy *p = &cat->policy;
  ASSERT_TRUE(p->max_payload_bytes == 1024);

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Ensures unknown safetyPolicy keys cause an error. */
static void test_policy_unknown_key_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"readOnly\": \"yes\","
                     "    \"unknown\": 1"
                     "  },"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"UnknownPolicyDb\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures overflowed numeric fields fail validation. */
static void test_policy_overflow_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"readOnly\": \"yes\","
                     "    \"maxPayloadKiloBytes\": 4294967295"
                     "  },"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"OverflowDb\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures legacy maxQueryKiloBytes key is rejected. */
static void test_policy_legacy_payload_key_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"readOnly\": \"yes\","
                     "    \"maxQueryKiloBytes\": 64"
                     "  },"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"LegacyDb\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(
      strstr(err, "unknown key \"maxQueryKiloBytes\" in object") != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Validates that an empty databases array is rejected. */
static void test_empty_databases_ok(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
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
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures unknown keys inside a database entry reject the catalog. */
static void test_db_entry_unknown_key_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
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
  ASSERT_TRUE(strstr(err, "unknown key \"unknownKey\" in database entry") !=
              NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures connectionName uniqueness is enforced case-insensitively. */
static void test_db_connection_name_duplicate_case_insensitive_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db1\""
                     "    },"
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"mypostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db2\""
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures unknown keys in per-db safetyPolicy are rejected. */
static void test_db_safety_policy_unknown_key_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"safetyPolicy\": {"
                     "        \"maxRowReturned\": 20,"
                     "        \"unknown\": 1"
                     "      }"
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "unknown key \"unknown\" in object") != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures a valid config maps fields to the right ConnProfile values. */
static void test_valid_config_maps_fields(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"readOnly\": \"no unsafe\","
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

  SafetyPolicy *p = &cat->policy;
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
  free(err);
  unlink(path);
  free(path);
}

/* Ensures catalog lowercases identifiers and accepts mixed case. */
static void test_policies_lowercase(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"safeFunctions\": [\"mD1\", \"PrivaTe.Md2\"],"
                     "      \"sensitiveColumns\": ["
                     "        \"Users.Email\","
                     "        \"Private.Users.Phone\""
                     "      ]"
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

  ASSERT_TRUE(cp->safe_funcs.n_rules == 2);
  ASSERT_STREQ(cp->safe_funcs.rules[0].name, "md1");
  ASSERT_TRUE(cp->safe_funcs.rules[0].is_global == 1);
  ASSERT_STREQ(cp->safe_funcs.rules[1].name, "md2");
  ASSERT_TRUE(cp->safe_funcs.rules[1].is_global == 0);
  ASSERT_TRUE(cp->safe_funcs.rules[1].n_schemas == 1);
  ASSERT_STREQ(cp->safe_funcs.rules[1].schemas[0], "private");

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Ensures duplicated entries inside the policies are de-duplicated. */
static void test_policies_dedup(void) {
  const char *json =
      "{"
      "  \"version\": \"1.0\","
      "  \"safetyPolicy\": {},"
      "  \"databases\": ["
      "    {"
      "      \"type\": \"postgres\","
      "      \"connectionName\": \"MyPostgres\","
      "      \"host\": \"127.0.0.1\","
      "      \"port\": 5432,"
      "      \"username\": \"user\","
      "      \"database\": \"db\","
      "      \"safeFunctions\": [\"md1\", \"MD1\", \"public.md2\"],"
      "      \"sensitiveColumns\": ["
      "        \"users.email\","
      "        \"USERS.EMAIL\","
      "        \"users.email\""
      "      ]"
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

  ASSERT_TRUE(cp->safe_funcs.n_rules == 2);
  ASSERT_STREQ(cp->safe_funcs.rules[0].name, "md1");
  ASSERT_TRUE(cp->safe_funcs.rules[0].is_global == 1);
  ASSERT_STREQ(cp->safe_funcs.rules[1].name, "md2");
  ASSERT_TRUE(cp->safe_funcs.rules[1].is_global == 0);
  ASSERT_TRUE(cp->safe_funcs.rules[1].n_schemas == 1);
  ASSERT_STREQ(cp->safe_funcs.rules[1].schemas[0], "public");

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Ensures global rules win and schema list is preserved. */
static void test_policies_global_and_schema(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"safeFunctions\": [\"md1\", \"private.md1\"],"
                     "      \"sensitiveColumns\": ["
                     "        \"users.email\","
                     "        \"private.users.email\""
                     "      ]"
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
  ASSERT_TRUE(cp->safe_funcs.n_rules == 1);
  ASSERT_TRUE(cp->safe_funcs.rules[0].is_global == 1);
  ASSERT_TRUE(cp->safe_funcs.rules[0].n_schemas == 1);
  ASSERT_STREQ(cp->safe_funcs.rules[0].schemas[0], "private");

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Ensures malformed sensitiveColumns entries fail catalog load. */
static void test_column_policy_malformed_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"sensitiveColumns\": ["
                     "        \"justcolumn\""
                     "      ]"
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures malformed safeFunctions entries fail catalog load. */
static void test_safe_functions_malformed_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"safeFunctions\": [\"md1\", \"bad.\"],"
                     "      \"sensitiveColumns\": ["
                     "        \"users.email\""
                     "      ]"
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Ensures invalid columnPolicy.strategy is rejected. */
static void test_column_policy_randomized_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"columnPolicy\": {"
                     "      \"mode\": \"pseudonymize\","
                     "      \"strategy\": \"randomizedd\""
                     "    }"
                     "  },"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"sensitiveColumns\": ["
                     "        \"cards.number\""
                     "      ]"
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Validates connp_is_*_sensitive behavior for global and schema-scoped rules.
 */
static void test_connp_is_sensitive(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {},"
                     "  \"databases\": ["
                     "    {"
                     "      \"type\": \"postgres\","
                     "      \"connectionName\": \"MyPostgres\","
                     "      \"host\": \"127.0.0.1\","
                     "      \"port\": 5432,"
                     "      \"username\": \"user\","
                     "      \"database\": \"db\","
                     "      \"safeFunctions\": [\"md1\", \"private.md2\"],"
                     "      \"sensitiveColumns\": ["
                     "        \"users.email\","
                     "        \"private.users.name\""
                     "      ]"
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

  ASSERT_TRUE(connp_is_func_safe(cp, "", "md1") == YES);
  ASSERT_TRUE(connp_is_func_safe(cp, "public", "md1") == YES);
  ASSERT_TRUE(connp_is_func_safe(cp, "", "md2") == NO);
  ASSERT_TRUE(connp_is_func_safe(cp, "private", "md2") == YES);
  ASSERT_TRUE(connp_is_func_safe(cp, "public", "md2") == NO);
  ASSERT_TRUE(connp_is_func_safe(cp, "", "unknown") == NO);

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

int main(void) {
  test_missing_policy_defaults();
  test_policy_missing_fields_defaults();
  test_policy_kilobytes();
  test_policy_unknown_key_fails();
  test_policy_overflow_fails();
  test_policy_legacy_payload_key_fails();
  test_empty_databases_ok();
  test_db_entry_unknown_key_fails();
  test_db_connection_name_duplicate_case_insensitive_fails();
  test_db_safety_policy_unknown_key_fails();
  test_valid_config_maps_fields();
  test_policies_lowercase();
  test_policies_dedup();
  test_policies_global_and_schema();
  test_column_policy_malformed_fails();
  test_safe_functions_malformed_fails();
  test_column_policy_randomized_fails();
  test_connp_is_sensitive();
  fprintf(stderr, "OK: test_conn_catalog\n");
  return 0;
}
