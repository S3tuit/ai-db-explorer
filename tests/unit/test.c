#include <fcntl.h>
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

ConnCatalog *catalog_load_from_file(const char *path, char **err_out) {
  char *err_msg = NULL;
  if (err_out)
    *err_out = NULL;

  ASSERT_TRUE(path != NULL);

  int fd = open(path, O_RDONLY);
  ASSERT_TRUE(fd >= 0);

  ConnCatalog *cat = catalog_load_from_fd(fd, &err_msg);
  close(fd);
  *err_out = err_msg;
  return cat;
}

ConnCatalog *load_test_catalog(void) {
  const char *json = "{"
                     "  \"version\": \"1.0\","
                     "  \"credentialNamespace\": \"TestNamespace\","
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
  cp.secret_ref.cred_namespace = "TestNamespace";
  cp.secret_ref.connection_name = connection_name;
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

char *make_tmp_dir(void) {
  char templ[] = "/tmp/adbxcred_XXXXXX";
  char *out = dup_or_null(templ);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(mkdtemp(out) != NULL);
  return out;
}

/* -------------------------------------- ENV ------------------------------ */

void restore_env(const char *name, const char *old_val, int had_old) {
  ASSERT_TRUE(name != NULL);
  if (!had_old) {
    ASSERT_TRUE(unsetenv(name) == 0);
    return;
  }
  ASSERT_TRUE(old_val != NULL);
  ASSERT_TRUE(setenv(name, old_val, 1) == 0);
}

void env_guard_begin(EnvGuard *g) {
  ASSERT_TRUE(g != NULL);
  memset(g, 0, sizeof(*g));

  const char *xdg = getenv("XDG_CONFIG_HOME");
  g->had_xdg = (xdg != NULL);
  g->xdg_old = xdg ? dup_or_null(xdg) : NULL;

  const char *home = getenv("HOME");
  g->had_home = (home != NULL);
  g->home_old = home ? dup_or_null(home) : NULL;
}

void env_guard_end(EnvGuard *g) {
  ASSERT_TRUE(g != NULL);
  restore_env("XDG_CONFIG_HOME", g->xdg_old, g->had_xdg);
  restore_env("HOME", g->home_old, g->had_home);
  free(g->xdg_old);
  free(g->home_old);
  memset(g, 0, sizeof(*g));
}
