#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "safety_policy.h"
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

/* ------------------------------ fake backend ------------------------------ */

typedef struct {
  int connected;
} FakeDbImpl;

static int g_fake_connect_calls = 0;
static int g_fake_disconnect_calls = 0;
static int g_fake_destroy_calls = 0;

/* Implements a deterministic auth rule for unit tests.
 * It borrows all inputs and updates only the fake backend state.
 * Side effects: marks the backend connected on success, writes one per-call
 * diagnostic into 'out_err' on failure, and bumps the shared connect counter.
 * Error semantics: returns OK only when 'pwd' equals
 * 'profile->connection_name'; returns ERR otherwise or on invalid input.
 */
static int fake_connect(DbBackend *db, const ConnProfile *profile,
                        const SafetyPolicy *policy, const char *pwd,
                        DbErr *out_err) {
  (void)policy;
  if (!db || !db->impl || !profile || !profile->connection_name || !pwd) {
    ADBX_ERR_SETF(out_err, DBERR_INPUT,
                  "fake backend connect failed: invalid input.");
    return ERR;
  }

  FakeDbImpl *impl = (FakeDbImpl *)db->impl;
  g_fake_connect_calls++;

  if (strcmp(pwd, profile->connection_name) == 0) {
    impl->connected = 1;
    return OK;
  }

  impl->connected = 0;
  ADBX_ERR_SETF(out_err, DBERR_GENERIC, "fake auth failed");
  return ERR;
}

/* Reports whether the shared fake backend currently considers itself
 * connected. It borrows 'db' and performs no allocations.
 * Error semantics: returns YES when connected, NO when disconnected, ERR on
 * invalid input.
 */
static int fake_is_connected(DbBackend *db) {
  if (!db || !db->impl)
    return ERR;
  FakeDbImpl *impl = (FakeDbImpl *)db->impl;
  return impl->connected ? YES : NO;
}

/* Disconnects the shared fake backend instance.
 * It borrows 'db' and performs no allocations.
 * Side effects: marks the fake backend disconnected and bumps the disconnect
 * counter.
 * Error semantics: none.
 */
static void fake_disconnect(DbBackend *db) {
  if (!db || !db->impl)
    return;
  FakeDbImpl *impl = (FakeDbImpl *)db->impl;
  impl->connected = 0;
  g_fake_disconnect_calls++;
}

/* Destroys one shared fake backend instance.
 * It consumes 'db' and frees the owned fake implementation.
 * Side effects: frees memory and bumps the destroy counter.
 * Error semantics: none.
 */
static void fake_destroy(DbBackend *db) {
  if (!db)
    return;
  g_fake_destroy_calls++;
  free(db->impl);
  free(db);
}

/* Fake exec implementation shared by unit tests that do not execute SQL.
 * It borrows all inputs and performs no allocations.
 * Side effects: none.
 * Error semantics: always returns ERR because these tests don't execute SQL.
 */
static int fake_exec(DbBackend *db, const char *sql,
                     const QueryResultBuildPolicy *qb_policy,
                     QueryResult **out_qr) {
  (void)db;
  (void)sql;
  (void)qb_policy;
  (void)out_qr;
  return ERR;
}

/* Fake safe-function provider for the shared unit-test backend.
 * It borrows 'db' and returns one static empty list.
 * Side effects: none.
 * Error semantics: never fails; may return an empty list.
 */
static const DbSafeFuncList *fake_safe_functions(DbBackend *db) {
  (void)db;
  static const DbSafeFuncList list = {0};
  return &list;
}

static const DbBackendVTable FAKE_VT = {
    .connect = fake_connect,
    .is_connected = fake_is_connected,
    .disconnect = fake_disconnect,
    .destroy = fake_destroy,
    .exec = fake_exec,
    .safe_functions = fake_safe_functions,
};

DbBackend *fake_backend_create(DbKind kind) {
  (void)kind;
  DbBackend *db = (DbBackend *)xmalloc(sizeof(*db));
  FakeDbImpl *impl = (FakeDbImpl *)xmalloc(sizeof(*impl));
  impl->connected = 0;
  db->vt = &FAKE_VT;
  db->impl = impl;
  return db;
}

void fake_backend_reset_counters(void) {
  g_fake_connect_calls = 0;
  g_fake_disconnect_calls = 0;
  g_fake_destroy_calls = 0;
}

int fake_backend_connect_calls(void) { return g_fake_connect_calls; }

int fake_backend_disconnect_calls(void) { return g_fake_disconnect_calls; }

int fake_backend_destroy_calls(void) { return g_fake_destroy_calls; }
