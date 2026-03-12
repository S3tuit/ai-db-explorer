#include <stdio.h>
#include <string.h>
#include <time.h>

#include "conn_manager.h"
#include "safety_policy.h"
#include "test.h"
#include "utils.h"

typedef struct {
  SecretStore base;
} FakeSecretStore;

/* Returns one deterministic password derived from the typed secret ref.
 * It borrows all inputs and appends 'ref->connection_name' into 'out'.
 * Side effects: grows 'out'.
 * Error semantics: returns YES on success, ERR on invalid input or append
 * failure.
 *
 * We intentionally mirror the fake backend auth rule from tests/unit/test.c:
 * fake db connections succeed only when pwd == connection_name. Returning the
 * connection name here keeps ConnManager tests isolated from the real secret
 * store while still exercising successful connection establishment.
 */
static AdbxTriStatus fake_ss_get(SecretStore *store, const SecretRefInfo *ref,
                                 StrBuf *out) {
  (void)store;
  if (!ref || !ref->connection_name || !out)
    return ERR;
  if (sb_append_bytes(out, ref->connection_name,
                      strlen(ref->connection_name) + 1) != OK) {
    return ERR;
  }
  return YES;
}

static AdbxStatus fake_ss_set(SecretStore *store, const SecretRefInfo *ref,
                              const char *secret) {
  (void)store;
  (void)ref;
  (void)secret;
  return OK;
}

static AdbxStatus fake_ss_delete(SecretStore *store, const SecretRefInfo *ref) {
  (void)store;
  (void)ref;
  return OK;
}

static AdbxStatus fake_ss_list_refs(SecretStore *store, SecretRefList *out) {
  (void)store;
  (void)out;
  return OK;
}

static AdbxStatus fake_ss_wipe_namespace(SecretStore *store,
                                         const char *cred_namespace) {
  (void)store;
  (void)cred_namespace;
  return OK;
}

static AdbxStatus fake_ss_wipe_all(SecretStore *store) {
  (void)store;
  return OK;
}

static void fake_ss_destroy(SecretStore *store) { free(store); }

static const SecretStoreVTable FAKE_SS_VT = {
    .get = fake_ss_get,
    .set = fake_ss_set,
    .delete = fake_ss_delete,
    .list_refs = fake_ss_list_refs,
    .wipe_namespace = fake_ss_wipe_namespace,
    .wipe_all = fake_ss_wipe_all,
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
  cat->credential_namespace = dup_or_null("TestNamespace");
  cat->n_profiles = 1;
  cat->profiles = (ConnProfile *)xcalloc(1, sizeof(ConnProfile));

  ConnProfile *p = &cat->profiles[0];
  p->connection_name = dup_or_null("db1");
  p->secret_ref.cred_namespace = cat->credential_namespace;
  p->secret_ref.connection_name = p->connection_name;
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
  fake_backend_reset_counters();
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

  ConnView c1 = {0};
  int rc = connm_get_connection(m, "db1", &c1);
  ASSERT_TRUE(rc == YES);
  DbBackend *b1 = c1.db;
  ASSERT_TRUE(b1 != NULL);
  ASSERT_TRUE(c1.profile != NULL);
  ASSERT_TRUE(fake_backend_connect_calls() == 1);

  ConnView c2 = {0};
  rc = connm_get_connection(m, "db1", &c2);
  ASSERT_TRUE(rc == YES);
  DbBackend *b2 = c2.db;
  ASSERT_TRUE(b2 != NULL);
  ASSERT_TRUE(b1 == b2);
  ASSERT_TRUE(fake_backend_connect_calls() == 1);

  connm_set_ttl_ms(m, 1);
  connm_mark_used(m, "db1");
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 2 * 1000 * 1000; // 2ms
  nanosleep(&ts, NULL);

  ConnView c3 = {0};
  rc = connm_get_connection(m, "db1", &c3);
  ASSERT_TRUE(rc == YES);
  DbBackend *b3 = c3.db;
  ASSERT_TRUE(b3 == b1);
  ASSERT_TRUE(fake_backend_disconnect_calls() == 1);
  ASSERT_TRUE(fake_backend_connect_calls() == 2);

  connm_destroy(m);
  ASSERT_TRUE(fake_backend_destroy_calls() == 1);
}

int main(void) {
  test_conn_manager_lifecycle();
  fprintf(stderr, "OK: test_conn_manager\n");
  return 0;
}
