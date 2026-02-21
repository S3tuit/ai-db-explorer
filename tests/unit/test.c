#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "test.h"

/* ----------------------------- IN-MEMORY I/O ----------------------------- */

FILE *memfile_impl(const char *input, const char *file, int line) {
  (void)file;
  (void)line;
#if defined(_GNU_SOURCE)
  FILE *f = fmemopen((void *)input, strlen(input), "r");
  ASSERT_TRUE_AT(f != NULL, file, line);
  return f;
#else
  /* portable fallback: tmpfile */
  FILE *f = tmpfile();
  if (!f)
    return NULL;
  fwrite(input, 1, strlen(input), f);
  fflush(f);
  fseek(f, 0, SEEK_SET);
  ASSERT_TRUE_AT(f != NULL, file, line);
  return f;
#endif
}

FILE *memfile_out_impl(const char *file, int line) {
  FILE *f = tmpfile();
  ASSERT_TRUE_AT(f != NULL, file, line);
  return f;
}

char *read_all(FILE *f) {
  fseek(f, 0, SEEK_END);
  long sz = ftell(f);
  fseek(f, 0, SEEK_SET);
  ASSERT_TRUE(sz >= 0);

  char *buf = xmalloc((size_t)sz + 1);

  size_t n = fread(buf, 1, (size_t)sz, f);
  buf[n] = '\0';
  return buf;
}

/* --------------------------------- HELPERS ------------------------------- */

char *write_tmp_config(const char *json) {
  ASSERT_TRUE(json);

  char tmpl[] = "/tmp/adbxcfgXXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_TRUE(fd >= 0);

  size_t len = strlen(json);
  ssize_t n = write(fd, json, len);
  ASSERT_TRUE(n == (ssize_t)len);
  ASSERT_TRUE(close(fd) == 0);

  return strdup(tmpl);
}

ConnCatalog *load_test_catalog(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"safetyPolicy\": {"
                     "    \"columnPolicy\": {"
                     "      \"mode\": \"pseudonymize\","
                     "      \"strategy\": \"deterministic\""
                     "    }"
                     "  },"
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
                     "      \"sensitiveColumns\": ["
                     "        \"users.fiscal_code\","
                     "        \"users.card_code\","
                     "        \"private.cards.balance\","
                     "        \"expenses.receiver\""
                     "      ]"
                     "    }"
                     "  ]"
                     "}";

  char *path = write_tmp_config(json);
  ASSERT_TRUE(path);
  char *err = NULL;
  ConnCatalog *cat = catalog_load_from_file(path, &err);
  free(err);

  (void)unlink(path);
  free(path);

  return cat;
}

ConnProfile make_profile(const char *connection_name,
                         SafetyColumnStrategy mode) {
  ConnProfile cp = {0};
  cp.connection_name = connection_name;
  cp.safe_policy.column_strategy = mode;
  return cp;
}

int get_validate_query_out(ValidateQueryOut *out, char *sql) {
  ASSERT_TRUE(out);

  ConnCatalog *cat = load_test_catalog();
  ASSERT_TRUE(cat);

  ConnProfile *cp = NULL;
  ASSERT_TRUE(catalog_list(cat, &cp, 1) == 1);
  ASSERT_TRUE(cp);

  SafetyPolicy *policy = &cp->safe_policy;
  ASSERT_TRUE(policy);

  DbBackend *db = postgres_backend_create();
  ASSERT_TRUE(db);

  ASSERT_TRUE(vq_out_init(out) == OK);
  ValidatorRequest vr = {.db = db, .profile = cp, .sql = sql};
  int rc = validate_query(&vr, out);

  db_destroy(db);
  catalog_destroy(cat);

  return rc;
}
