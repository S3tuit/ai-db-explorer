#include <stdio.h>
#include <string.h>
#include <time.h>

#include "conn_manager.h"
#include "safety_policy.h"
#include "test.h"
#include "utils.h"

/* ------------------------------- fakes --------------------------------- */

typedef struct {
  int connected;
} FakeDbImpl;

static int g_connect_calls = 0;
static int g_disconnect_calls = 0;
static int g_destroy_calls = 0;

static int fake_connect(DbBackend *db, const ConnProfile *profile,
                        const SafetyPolicy *policy, const char *pwd) {
  (void)profile;
  (void)policy;
  (void)pwd;
  if (!db || !db->impl)
    return ERR;
  FakeDbImpl *impl = (FakeDbImpl *)db->impl;
  impl->connected = 1;
  g_connect_calls++;
  return OK;
}

static int fake_is_connected(DbBackend *db) {
  if (!db || !db->impl)
    return ERR;
  FakeDbImpl *impl = (FakeDbImpl *)db->impl;
  return impl->connected ? YES : NO;
}

static void fake_disconnect(DbBackend *db) {
  if (!db || !db->impl)
    return;
  FakeDbImpl *impl = (FakeDbImpl *)db->impl;
  impl->connected = 0;
  g_disconnect_calls++;
}

static void fake_destroy(DbBackend *db) {
  if (!db)
    return;
  g_destroy_calls++;
  free(db->impl);
  free(db);
}

static int fake_exec(DbBackend *db, const McpId *request_id, const char *sql,
                     QueryResult **out_qr) {
  (void)db;
  (void)request_id;
  (void)sql;
  (void)out_qr;
  return ERR;
}

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

static DbBackend *fake_backend_create(DbKind kind) {
  (void)kind;
  DbBackend *db = (DbBackend *)xmalloc(sizeof(*db));
  FakeDbImpl *impl = (FakeDbImpl *)xmalloc(sizeof(*impl));
  impl->connected = 0;
  db->vt = &FAKE_VT;
  db->impl = impl;
  return db;
}

typedef struct {
  SecretStore base;
} FakeSecretStore;

static int fake_ss_get(SecretStore *store, const char *ref, StrBuf *out) {
  (void)store;
  (void)ref;
  const char *pwd = "pwd";
  return sb_append_bytes(out, pwd, strlen(pwd) + 1);
}

static void fake_ss_destroy(SecretStore *store) { free(store); }

static const SecretStoreVTable FAKE_SS_VT = {
    .get = fake_ss_get,
    .destroy = fake_ss_destroy,
};

static SecretStore *fake_secret_store_create(void) {
  FakeSecretStore *s = (FakeSecretStore *)xmalloc(sizeof(*s));
  s->base.vt = &FAKE_SS_VT;
  return (SecretStore *)s;
}

/* ------------------------------ helpers -------------------------------- */

/* Builds a tiny catalog with one profile, to validate ConnManager behavior
 * without relying on a real DB. */
static ConnCatalog *make_catalog(void) {
  ConnCatalog *cat = (ConnCatalog *)xcalloc(1, sizeof(*cat));
  cat->n_profiles = 1;
  cat->profiles = (ConnProfile *)xcalloc(1, sizeof(ConnProfile));

  ConnProfile *p = &cat->profiles[0];
  p->connection_name = dup_or_null("db1");
  p->kind = DB_KIND_POSTGRES;
  p->host = dup_or_null("localhost");
  p->port = 5432;
  p->db_name = dup_or_null("testdb");
  p->user = dup_or_null("user");
  p->options = NULL;

  int read_only = 1;
  uint32_t max_rows = 10;
  safety_policy_init(&cat->policy, &read_only, &max_rows, NULL, NULL);
  return cat;
}

static void reset_counters(void) {
  g_connect_calls = 0;
  g_disconnect_calls = 0;
  g_destroy_calls = 0;
}

/* ------------------------------- tests --------------------------------- */

/* Verifies lazy connection, reuse, and reaping behavior using a fake backend.
 */
static void test_conn_manager_lifecycle(void) {
  reset_counters();

  ConnCatalog *cat = make_catalog();
  SecretStore *ss = fake_secret_store_create();
  ConnManager *m = connm_create_with_factory(cat, ss, fake_backend_create);
  ASSERT_TRUE(m != NULL);

  DbBackend *b1 = connm_get_backend(m, "db1");
  ASSERT_TRUE(b1 != NULL);
  ASSERT_TRUE(g_connect_calls == 1);

  DbBackend *b2 = connm_get_backend(m, "db1");
  ASSERT_TRUE(b2 != NULL);
  ASSERT_TRUE(b1 == b2);
  ASSERT_TRUE(g_connect_calls == 1);

  connm_set_ttl_ms(m, 1);
  connm_mark_used(m, "db1");
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 2 * 1000 * 1000; // 2ms
  nanosleep(&ts, NULL);

  DbBackend *b3 = connm_get_backend(m, "db1");
  ASSERT_TRUE(b3 == b1);
  ASSERT_TRUE(g_disconnect_calls == 1);
  ASSERT_TRUE(g_connect_calls == 2);

  connm_destroy(m);
  ASSERT_TRUE(g_destroy_calls == 1);
}

int main(void) {
  test_conn_manager_lifecycle();
  fprintf(stderr, "OK: test_conn_manager\n");
  return 0;
}
