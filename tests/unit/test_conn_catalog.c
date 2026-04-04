#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "conn_catalog.h"
#include "json_codec.h"
#include "safety_policy.h"
#include "test.h"

/* Ensures empty safetyPolicy falls back to default values. */
static void test_missing_policy_defaults(void) {
  const char *json = "{"
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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

/* Ensures configNamespace is required at the top level. */
static void test_missing_credential_namespace_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.1\","
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
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "$.configNamespace") != NULL);

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

/* Appends one NUL-terminated literal string to 'sb'.
 * It borrows both inputs and does not allocate beyond StrBuf growth.
 * Side effects: may grow 'sb'.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
static AdbxStatus sb_append_cstr(StrBuf *sb, const char *s) {
  if (!sb || !s)
    return ERR;
  return sb_append_bytes(sb, s, strlen(s));
}

/* Builds one minimal catalog with the requested config version and one
 * postgres database entry. 'db_extra_raw_json' is appended verbatim inside the
 * database object and must already be valid JSON object members.
 * Ownership: returns a caller-owned catalog on success; '*err_out' receives a
 * caller-owned error string on load failure. Internal temp files are deleted
 * before return.
 * Returns a catalog on success, NULL on catalog-load failure.
 */
static ConnCatalog *
load_catalog_with_version_and_db_extra_raw(char **err_out, const char *version,
                                           const char *db_extra_raw_json) {
  if (err_out)
    *err_out = NULL;

  ASSERT_TRUE(version != NULL);

  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(sb_append_cstr(&sb, "{") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"version\":\"") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, version) == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\",") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"configNamespace\":\"TestNamespace\",") ==
              OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"safetyPolicy\":{},") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"databases\":[{") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"type\":\"postgres\",") == OK);
  ASSERT_TRUE(
      sb_append_cstr(&sb, "\"connectionName\":\"SensitiveDomainsDb\",") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"host\":\"127.0.0.1\",") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"port\":5432,") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"username\":\"user\",") == OK);
  ASSERT_TRUE(sb_append_cstr(&sb, "\"database\":\"db\"") == OK);
  if (db_extra_raw_json && db_extra_raw_json[0] != '\0') {
    ASSERT_TRUE(sb_append_cstr(&sb, ",") == OK);
    ASSERT_TRUE(sb_append_cstr(&sb, db_extra_raw_json) == OK);
  }
  ASSERT_TRUE(sb_append_cstr(&sb, "}]}") == OK);

  char *json = sb_to_cstr(&sb);
  ASSERT_TRUE(json != NULL);

  char *path = write_tmp_config(json);
  ASSERT_TRUE(path != NULL);

  ConnCatalog *cat = catalog_load_from_file(path, err_out);
  unlink(path);
  free(path);
  sb_clean(&sb);
  return cat;
}

/* Asserts one public sensitive-domain lookup result through
 * connp_get_sensitive_domain(). It borrows all inputs and performs no
 * allocations.
 * Returns no value; aborts the test process on mismatch.
 */
static void
assert_sensitive_domain_lookup(const ConnProfile *cp, const char *schema,
                               const char *table, const char *column,
                               AdbxTriStatus exp_rc, const char *exp_domain) {
  SensDomainOut out = {0};
  AdbxTriStatus rc =
      connp_get_sensitive_domain(cp, schema, table, column, &out);
  ASSERT_TRUE(rc == exp_rc);
  if (exp_rc == YES)
    ASSERT_STREQ(out.domain, exp_domain);
}

/* Builds one minimal version 1.1 catalog with a single postgres connection and
 * the provided sensitiveDomains mini-language.
 *
 * fmt is a sequence of:
 * - 'd': next vararg is a domain name
 * - 'c': next vararg is one identifier belonging to the current domain
 *
 * The helper uses json_codec to build a valid config, loads it through the
 * normal catalog parser, then deletes the temporary file before returning.
 * It returns a caller-owned catalog on success, NULL on catalog-load failure.
 */
static ConnCatalog *load_sensitive_domains_catalog_fmt(char **err_out,
                                                       const char *fmt, ...) {
  if (err_out)
    *err_out = NULL;

  ASSERT_TRUE(fmt != NULL);
  ASSERT_TRUE(fmt[0] == 'd');

  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_obj_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_str(&sb, "version", "1.1") == OK);
  ASSERT_TRUE(json_kv_str(&sb, "configNamespace", "TestNamespace") == OK);
  ASSERT_TRUE(json_kv_obj_begin(&sb, "safetyPolicy") == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);
  ASSERT_TRUE(json_kv_arr_begin(&sb, "databases") == OK);
  ASSERT_TRUE(json_obj_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_str(&sb, "type", "postgres") == OK);
  ASSERT_TRUE(json_kv_str(&sb, "connectionName", "SensitiveDomainsDb") == OK);
  ASSERT_TRUE(json_kv_str(&sb, "host", "127.0.0.1") == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "port", 5432) == OK);
  ASSERT_TRUE(json_kv_str(&sb, "username", "user") == OK);
  ASSERT_TRUE(json_kv_str(&sb, "database", "db") == OK);
  ASSERT_TRUE(json_kv_obj_begin(&sb, "sensitiveDomains") == OK);

  va_list ap;
  va_start(ap, fmt);

  int in_domain = 0;
  int need_column = 0;
  for (size_t i = 0; fmt[i] != '\0'; i++) {
    const char *arg = va_arg(ap, const char *);
    ASSERT_TRUE(arg != NULL);
    ASSERT_TRUE(arg[0] != '\0');

    if (fmt[i] == 'd') {
      if (in_domain) {
        ASSERT_TRUE(need_column == 0);
        ASSERT_TRUE(json_arr_end(&sb) == OK);
      }
      ASSERT_TRUE(json_kv_arr_begin(&sb, arg) == OK);
      in_domain = 1;
      need_column = 1;
      continue;
    }

    ASSERT_TRUE(fmt[i] == 'c');
    ASSERT_TRUE(in_domain == 1);
    ASSERT_TRUE(json_arr_elem_str(&sb, arg) == OK);
    need_column = 0;
  }

  va_end(ap);

  ASSERT_TRUE(in_domain == 1);
  ASSERT_TRUE(need_column == 0);
  ASSERT_TRUE(json_arr_end(&sb) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);
  ASSERT_TRUE(json_arr_end(&sb) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);

  char *json = sb_to_cstr(&sb);
  ASSERT_TRUE(json != NULL);

  char *path = write_tmp_config(json);
  ASSERT_TRUE(path != NULL);

  ConnCatalog *cat = catalog_load_from_file(path, err_out);
  unlink(path);
  free(path);
  sb_clean(&sb);
  return cat;
}

/* Ensures missing policy fields fall back to defaults. */
static void test_policy_missing_fields_defaults(void) {
  const char *json = "{"
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
  ASSERT_TRUE(strstr(err, "unknown key \"maxQueryKiloBytes\" in object") !=
              NULL);

  free(err);
  unlink(path);
  free(path);
}

/* Validates that an empty databases array is rejected. */
static void test_empty_databases_ok(void) {
  const char *json = "{"
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
  ASSERT_STREQ(cat->credential_namespace, "TestNamespace");

  SafetyPolicy *p = &cat->policy;
  ASSERT_TRUE(p->read_only == 0);
  ASSERT_TRUE(p->max_rows == 99);
  ASSERT_TRUE(p->statement_timeout_ms == 1234);

  ConnProfile *cp = catalog_get_by_name(cat, "MyPostgres");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->kind == DB_KIND_POSTGRES);
  ASSERT_STREQ(cp->connection_name, "MyPostgres");
  ASSERT_STREQ(cp->secret_ref.cred_namespace, "TestNamespace");
  ASSERT_STREQ(cp->secret_ref.connection_name, "MyPostgres");
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "      \"sensitiveDomains\": {"
                     "        \"email\": [\"Users.Email\"],"
                     "        \"phone\": [\"Private.Users.Phone\"]"
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
  assert_sensitive_domain_lookup(cp, "public", "users", "email", YES, "email");
  assert_sensitive_domain_lookup(cp, "private", "users", "phone", YES, "phone");

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
      "  \"version\": \"1.1\","
      "  \"configNamespace\": \"TestNamespace\","
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
      "      \"sensitiveDomains\": {"
      "        \"email\": [\"users.email\", \"USERS.EMAIL\", \"users.email\"]"
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
  ASSERT_TRUE(cp->sens_policy.n_storage == 1);
  assert_sensitive_domain_lookup(cp, "public", "users", "email", YES, "email");

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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "      \"sensitiveDomains\": {"
                     "        \"email\": [\"users.email\", "
                     "                    \"private.users.email\"]"
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
  assert_sensitive_domain_lookup(cp, "public", "users", "email", YES, "email");
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "email");
  ASSERT_TRUE(cp->safe_funcs.n_rules == 1);
  ASSERT_TRUE(cp->safe_funcs.rules[0].is_global == 1);
  ASSERT_TRUE(cp->safe_funcs.rules[0].n_schemas == 1);
  ASSERT_STREQ(cp->safe_funcs.rules[0].schemas[0], "private");

  catalog_destroy(cat);
  free(err);
  unlink(path);
  free(path);
}

/* Ensures malformed safeFunctions entries fail catalog load. */
static void test_safe_functions_malformed_fails(void) {
  const char *json = "{"
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "      \"sensitiveDomains\": {"
                     "        \"email\": [\"users.email\"]"
                     "      }"
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
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "      \"sensitiveDomains\": {"
                     "        \"card\": [\"cards.number\"]"
                     "      }"
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

/* Validates public sensitive-domain lookup and safe-function behavior.
 */
static void test_connp_is_sensitive(void) {
  const char *json = "{"
                     "  \"version\": \"1.1\","
                     "  \"configNamespace\": \"TestNamespace\","
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
                     "      \"sensitiveDomains\": {"
                     "        \"email\": [\"users.email\"],"
                     "        \"name\": [\"private.users.name\"]"
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

  assert_sensitive_domain_lookup(cp, "", "users", "email", YES, "email");
  assert_sensitive_domain_lookup(cp, "public", "users", "email", YES, "email");
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "email");
  assert_sensitive_domain_lookup(cp, "private", "users", "name", YES, "name");
  assert_sensitive_domain_lookup(cp, "public", "users", "name", NO, NULL);
  assert_sensitive_domain_lookup(cp, "", "users", "name", YES, "name");
  assert_sensitive_domain_lookup(cp, "", "users", "age", NO, NULL);

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

/* Ensures version 1.1 catalogs parse sensitiveDomains into the new buckets and
 * resolve domains through connp_get_sensitive_domain().
 */
static void test_sensitive_domains_v11_parse(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dccdcc", "email", "anagrafica.email", "email_rappr_legale",
      "telefono", "private.registry.telefono", "numero_tel");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->sens_policy.n_storage == 4);
  ASSERT_TRUE(cp->sens_policy.exact_stc.n_rules == 1);
  ASSERT_TRUE(cp->sens_policy.glob_stc.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.exact_tc.n_rules == 1);
  ASSERT_TRUE(cp->sens_policy.glob_tc.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.exact_c.n_rules == 2);
  ASSERT_TRUE(cp->sens_policy.glob_c.n_rules == 0);

  SensDomainOut out = {0};
  ASSERT_TRUE(connp_get_sensitive_domain(cp, "private", "registry", "telefono",
                                         &out) == YES);
  ASSERT_STREQ(out.domain, "telefono");

  ASSERT_TRUE(connp_get_sensitive_domain(cp, "public", "anagrafica", "email",
                                         &out) == YES);
  ASSERT_STREQ(out.domain, "email");

  ASSERT_TRUE(connp_get_sensitive_domain(cp, "ignored", "whatever",
                                         "email_rappr_legale", &out) == YES);
  ASSERT_STREQ(out.domain, "email");

  ASSERT_TRUE(connp_get_sensitive_domain(cp, "ignored", "whatever",
                                         "numero_tel", &out) == YES);
  ASSERT_STREQ(out.domain, "telefono");

  catalog_destroy(cat);
  free(err);
}

/* Ensures version 1.1 accepts a database entry without sensitiveDomains. */
static void test_sensitive_domains_v11_absent_ok(void) {
  char *err = NULL;
  ConnCatalog *cat =
      load_catalog_with_version_and_db_extra_raw(&err, "1.1", NULL);
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->sens_policy.n_storage == 0);
  ASSERT_TRUE(cp->sens_policy.exact_stc.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.glob_stc.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.exact_tc.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.glob_tc.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.exact_c.n_rules == 0);
  ASSERT_TRUE(cp->sens_policy.glob_c.n_rules == 0);
  assert_sensitive_domain_lookup(cp, "public", "users", "email", NO, NULL);

  catalog_destroy(cat);
  free(err);
}

/* Ensures an empty sensitiveDomains object is accepted and yields no rules. */
static void test_sensitive_domains_v11_empty_object_ok(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{}");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->sens_policy.n_storage == 0);
  assert_sensitive_domain_lookup(cp, "public", "users", "email", NO, NULL);

  catalog_destroy(cat);
  free(err);
}

/* Ensures one domain with an empty identifier array is accepted and yields no
 * rules.
 */
static void test_sensitive_domains_v11_empty_array_ok(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{\"email\":[]}");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->sens_policy.n_storage == 0);
  assert_sensitive_domain_lookup(cp, "public", "users", "email", NO, NULL);

  catalog_destroy(cat);
  free(err);
}

/* Ensures duplicate identifiers inside the same domain are normalized and
 * deduplicated.
 */
static void test_sensitive_domains_v11_duplicates_same_domain_dedup(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dccc", "email", "users.email", "Users.Email", "users.email");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->sens_policy.n_storage == 1);
  ASSERT_TRUE(cp->sens_policy.exact_tc.n_rules == 1);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "email");

  catalog_destroy(cat);
  free(err);
}

/* Ensures two different domains cannot claim the same normalized pattern. */
static void test_sensitive_domains_v11_conflicting_pattern_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "email", "Users.Email", "telefono", "users.email");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "same normalized pattern resolves to different "
                          "domains") != NULL);

  free(err);
}

/* Ensures JSON-escaped keys and identifiers are decoded before normalization
 * and deduplication.
 */
static void test_sensitive_domains_v11_json_escapes_decode_and_dedup(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1",
      "\"sensitiveDomains\":{\"\\u0045mail\":[\"Users.\\u0045mail\","
      "\"users.email\"]}");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  ASSERT_TRUE(cp->sens_policy.n_storage == 1);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "email");

  catalog_destroy(cat);
  free(err);
}

/* Ensures mixed-case domain names and identifiers are lowercased on parse. */
static void test_sensitive_domains_v11_mixed_case_lowercased(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcc", "EmaiL", "Anagrafica.Email", "Email_Rappr_Legale");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "public", "anagrafica", "email", YES,
                                 "email");
  assert_sensitive_domain_lookup(cp, "private", "users", "email_rappr_legale",
                                 YES, "email");

  catalog_destroy(cat);
  free(err);
}

/* Ensures sensitiveDomains must be a JSON object. */
static void test_sensitive_domains_v11_non_object_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":[]");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(
      strstr(err, "$.databases[0].sensitiveDomains: expected object.") != NULL);
  free(err);
}

/* Ensures each sensitive domain value must be an array. */
static void test_sensitive_domains_v11_non_array_value_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{\"email\":\"users.email\"}");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "each domain value must be an array of strings.") !=
              NULL);
  free(err);
}

/* Ensures each sensitive domain array contains only JSON strings. */
static void test_sensitive_domains_v11_non_string_entry_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{\"email\":[123]}");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "each domain value must contain only strings.") !=
              NULL);
  free(err);
}

/* Ensures patterns with too many qualifier segments are rejected. */
static void test_sensitive_domains_v11_too_many_dots_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{\"email\":[\"a.b.c.d\"]}");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "expected [schema.][table.]column") != NULL);
  free(err);
}

/* Ensures patterns with empty segments are rejected. */
static void test_sensitive_domains_v11_empty_segment_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{\"email\":[\"schema..col\"]}");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "expected [schema.][table.]column") != NULL);
  free(err);
}

/* Ensures '*' is rejected when it appears in schema or table qualifiers. */
static void test_sensitive_domains_v11_qualifier_glob_fails(void) {
  char *err = NULL;
  ConnCatalog *cat = load_catalog_with_version_and_db_extra_raw(
      &err, "1.1", "\"sensitiveDomains\":{\"email\":[\"public.*.email\"]}");
  ASSERT_TRUE(cat == NULL);
  ASSERT_TRUE(err != NULL);
  ASSERT_TRUE(strstr(err, "'*' is allowed only in the column segment.") !=
              NULL);
  free(err);
}

/* Ensures exact schema.table.column wins over all lower-precedence matches. */
static void test_sensitive_domains_v11_precedence_exact_stc_first(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdcdcdcdcdc", "d1", "private.users.email", "d2",
      "private.users.e*", "d3", "users.email", "d4", "users.e*", "d5", "email",
      "d6", "e*");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d1");

  catalog_destroy(cat);
  free(err);
}

/* Ensures glob schema.table.pattern beats exact table.column. */
static void test_sensitive_domains_v11_precedence_glob_stc_over_exact_tc(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "d1", "private.users.e*", "d2", "users.email");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d1");

  catalog_destroy(cat);
  free(err);
}

/* Ensures exact table.column beats glob table.pattern. */
static void test_sensitive_domains_v11_precedence_exact_tc_over_glob_tc(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "d1", "users.email", "d2", "users.e*");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d1");

  catalog_destroy(cat);
  free(err);
}

/* Ensures glob table.pattern beats exact column. */
static void test_sensitive_domains_v11_precedence_glob_tc_over_exact_c(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "d1", "users.e*", "d2", "email");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d1");

  catalog_destroy(cat);
  free(err);
}

/* Ensures exact column beats glob column pattern. */
static void test_sensitive_domains_v11_precedence_exact_c_over_glob_c(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(&err, "dcdc", "d1",
                                                        "email", "d2", "e*");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d1");

  catalog_destroy(cat);
  free(err);
}

/* Ensures glob precedence prefers fewer '*' when two patterns otherwise match
 * the same lookup.
 */
static void test_sensitive_domains_v11_glob_prefers_fewer_stars(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "d1", "users.ema*l", "d2", "users.e*a*i*l");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d1");

  catalog_destroy(cat);
  free(err);
}

/* Ensures glob precedence prefers more literal characters after star-count
 * ties.
 */
static void test_sensitive_domains_v11_glob_prefers_more_literals(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "d3", "users.em*il", "d4", "users.e*il");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d3");

  catalog_destroy(cat);
  free(err);
}

/* Ensures lexicographic order breaks glob ties when star count and literal
 * length are equal.
 */
static void test_sensitive_domains_v11_glob_prefers_lexicographic(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "d4", "users.em*l", "d5", "users.e*il");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "d5");

  catalog_destroy(cat);
  free(err);
}

/* Ensures underqualified table.column fails closed when exact schema-qualified
 * rules match different domains, while fully qualified lookups stay
 * resolvable.
 */
static void
test_sensitive_domains_v11_underqualified_exact_stc_ambiguous(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "email", "users.email", "private_email",
      "private.users.email");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);

  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES,
                                 "private_email");
  assert_sensitive_domain_lookup(cp, "public", "users", "email", YES, "email");

  SensDomainOut out = {
      .domain = "stale",
      .err = {.code = CONNCAT_ERR_INTERNAL, .msg = "stale"},
  };
  ASSERT_TRUE(connp_get_sensitive_domain(cp, NULL, "users", "email", &out) ==
              ERR);
  ASSERT_TRUE(out.domain == NULL);
  ASSERT_TRUE(out.err.code == CONNCAT_ERR_AMBIGUOUS_DOMAIN);
  ASSERT_TRUE(strstr(out.err.msg, "users.email") != NULL);
  ASSERT_TRUE(strstr(out.err.msg, "email") != NULL);
  ASSERT_TRUE(strstr(out.err.msg, "private_email") != NULL);

  catalog_destroy(cat);
  free(err);
}

/* Ensures underqualified table.column fails closed when schema-qualified glob
 * rules match different domains.
 */
static void test_sensitive_domains_v11_underqualified_glob_stc_ambiguous(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcdc", "email", "users.em*l", "private_email",
      "private.users.emai*");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);

  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES,
                                 "private_email");
  assert_sensitive_domain_lookup(cp, "public", "users", "email", YES, "email");

  SensDomainOut out = {
      .domain = "stale",
      .err = {.code = CONNCAT_ERR_INTERNAL, .msg = "stale"},
  };
  ASSERT_TRUE(connp_get_sensitive_domain(cp, NULL, "users", "email", &out) ==
              ERR);
  ASSERT_TRUE(out.domain == NULL);
  ASSERT_TRUE(out.err.code == CONNCAT_ERR_AMBIGUOUS_DOMAIN);
  ASSERT_TRUE(strstr(out.err.msg, "users.email") != NULL);
  ASSERT_TRUE(strstr(out.err.msg, "email") != NULL);
  ASSERT_TRUE(strstr(out.err.msg, "private_email") != NULL);

  catalog_destroy(cat);
  free(err);
}

/* Ensures underqualified table.column falls back to one unique schema-
 * qualified rule when no table/column or column-only rules match.
 */
static void
test_sensitive_domains_v11_underqualified_unique_stc_fallback(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dc", "private_email", "private.users.email");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);

  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES,
                                 "private_email");

  SensDomainOut out = {
      .domain = "stale",
      .err = {.code = CONNCAT_ERR_INTERNAL, .msg = "stale"},
  };
  ASSERT_TRUE(connp_get_sensitive_domain(cp, NULL, "users", "email", &out) ==
              YES);
  ASSERT_STREQ(out.domain, "private_email");
  ASSERT_TRUE(out.err.code == CONNCAT_ERR_NONE);
  ASSERT_TRUE(out.err.msg[0] == '\0');

  catalog_destroy(cat);
  free(err);
}

/* Ensures underqualified table.column succeeds when a table/column match and a
 * schema-qualified fallback agree on the same domain.
 */
static void test_sensitive_domains_v11_underqualified_tc_and_stc_agree(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dcc", "email", "users.email", "private.users.email");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);

  SensDomainOut out = {
      .domain = "stale",
      .err = {.code = CONNCAT_ERR_INTERNAL, .msg = "stale"},
  };
  ASSERT_TRUE(connp_get_sensitive_domain(cp, NULL, "users", "email", &out) ==
              YES);
  ASSERT_STREQ(out.domain, "email");
  ASSERT_TRUE(out.err.code == CONNCAT_ERR_NONE);
  ASSERT_TRUE(out.err.msg[0] == '\0');

  catalog_destroy(cat);
  free(err);
}

/* Ensures connp_get_sensitive_domain resolves the main rule shapes and returns
 * NO when nothing matches.
 */
static void test_connp_get_sensitive_domain_shapes(void) {
  char *err = NULL;
  ConnCatalog *cat = load_sensitive_domains_catalog_fmt(
      &err, "dccdc", "email", "private.users.email", "users.name", "phone",
      "telefono");
  ASSERT_TRUE(cat != NULL);
  ASSERT_TRUE(err == NULL);

  ConnProfile *cp = catalog_get_by_name(cat, "SensitiveDomainsDb");
  ASSERT_TRUE(cp != NULL);
  assert_sensitive_domain_lookup(cp, "private", "users", "email", YES, "email");
  assert_sensitive_domain_lookup(cp, "public", "users", "name", YES, "email");
  assert_sensitive_domain_lookup(cp, "public", "any", "telefono", YES, "phone");
  assert_sensitive_domain_lookup(cp, "public", "users", "age", NO, NULL);

  catalog_destroy(cat);
  free(err);
}

/* Ensures connp_get_sensitive_domain validates required arguments. */
static void test_connp_get_sensitive_domain_invalid_input(void) {
  SensDomainOut out = {0};
  ASSERT_TRUE(connp_get_sensitive_domain(NULL, "public", "users", "email",
                                         &out) == ERR);
  ASSERT_TRUE(out.domain == NULL);
  ASSERT_TRUE(out.err.code == CONNCAT_ERR_INVALID_INPUT);
  ASSERT_TRUE(connp_get_sensitive_domain(&(ConnProfile){0}, "public", "users",
                                         NULL, &out) == ERR);
  ASSERT_TRUE(out.domain == NULL);
  ASSERT_TRUE(out.err.code == CONNCAT_ERR_INVALID_INPUT);
  ASSERT_TRUE(connp_get_sensitive_domain(&(ConnProfile){0}, "public", "users",
                                         "email", NULL) == NO);
}

int main(void) {
  test_missing_policy_defaults();
  test_missing_credential_namespace_fails();
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
  test_safe_functions_malformed_fails();
  test_column_policy_randomized_fails();
  test_connp_is_sensitive();
  test_sensitive_domains_v11_parse();
  test_sensitive_domains_v11_absent_ok();
  test_sensitive_domains_v11_empty_object_ok();
  test_sensitive_domains_v11_empty_array_ok();
  test_sensitive_domains_v11_duplicates_same_domain_dedup();
  test_sensitive_domains_v11_conflicting_pattern_fails();
  test_sensitive_domains_v11_json_escapes_decode_and_dedup();
  test_sensitive_domains_v11_mixed_case_lowercased();
  test_sensitive_domains_v11_non_object_fails();
  test_sensitive_domains_v11_non_array_value_fails();
  test_sensitive_domains_v11_non_string_entry_fails();
  test_sensitive_domains_v11_too_many_dots_fails();
  test_sensitive_domains_v11_empty_segment_fails();
  test_sensitive_domains_v11_qualifier_glob_fails();
  test_sensitive_domains_v11_precedence_exact_stc_first();
  test_sensitive_domains_v11_precedence_glob_stc_over_exact_tc();
  test_sensitive_domains_v11_precedence_exact_tc_over_glob_tc();
  test_sensitive_domains_v11_precedence_glob_tc_over_exact_c();
  test_sensitive_domains_v11_precedence_exact_c_over_glob_c();
  test_sensitive_domains_v11_glob_prefers_fewer_stars();
  test_sensitive_domains_v11_glob_prefers_more_literals();
  test_sensitive_domains_v11_glob_prefers_lexicographic();
  test_sensitive_domains_v11_underqualified_exact_stc_ambiguous();
  test_sensitive_domains_v11_underqualified_glob_stc_ambiguous();
  test_sensitive_domains_v11_underqualified_unique_stc_fallback();
  test_sensitive_domains_v11_underqualified_tc_and_stc_agree();
  test_connp_get_sensitive_domain_shapes();
  test_connp_get_sensitive_domain_invalid_input();
  fprintf(stderr, "OK: test_conn_catalog\n");
  return 0;
}
