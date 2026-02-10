#define _GNU_SOURCE

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "broker.h"
#include "conn_manager.h"
#include "secret_store.h"
#include "string_op.h"
#include "test.h"

/* --------------------------------- Fakes --------------------------------- */

static int fake_ss_get(SecretStore *store, const char *ref, StrBuf *out) {
  (void)store;
  (void)ref;
  (void)out;
  return OK;
}

static void fake_ss_destroy(SecretStore *store) { free(store); }

static const SecretStoreVTable FAKE_SS_VT = {
    .get = fake_ss_get,
    .destroy = fake_ss_destroy,
};

static SecretStore *fake_secret_store(void) {
  SecretStore *s = xcalloc(1, sizeof(*s));
  s->vt = &FAKE_SS_VT;
  return s;
}

/* Creates a ConnManager with an empty catalog. broker_destroy will free it. */
static ConnManager *make_empty_cm(void) {
  ConnCatalog *cat = xcalloc(1, sizeof(*cat));
  SecretStore *ss = fake_secret_store();
  return connm_create(cat, ss);
}

/* -------------------------------- Helpers -------------------------------- */

static char *make_sock_path(const char *tmpdir) {
  size_t len = strlen(tmpdir) + strlen("/broker.sock") + 1;
  char *path = xmalloc(len);
  snprintf(path, len, "%s/broker.sock", tmpdir);
  return path;
}

/* Connects a client socket to 'sock_path'. Returns the fd or -1 on error. */
static int connect_client(const char *sock_path) {
  int cfd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (cfd < 0)
    return -1;

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);

  if (connect(cfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(cfd);
    return -1;
  }
  return cfd;
}

static void msleep(int ms) {
  struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
  nanosleep(&ts, NULL);
}

static void *broker_run_thread(void *arg) {
  Broker *b = (Broker *)arg;
  broker_run(b);
  return NULL;
}

/* --------------------------------- Tests --------------------------------- */

static void test_create_null_args(void) {
  ConnManager *cm = make_empty_cm();

  /* NULL sock_path */
  ASSERT_TRUE(broker_create(NULL, cm, NULL) == NULL);

  /* NULL cm */
  ASSERT_TRUE(broker_create("/tmp/unused.sock", NULL, NULL) == NULL);

  /* cm was not consumed above (broker_create returned NULL), free it. */
  connm_destroy(cm);
}

static void test_create_and_destroy(void) {
  char tmpl[] = "/tmp/test_broker_XXXXXX";
  char *tmpdir = mkdtemp(tmpl);
  ASSERT_TRUE(tmpdir != NULL);

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();

  Broker *b = broker_create(sock, cm, NULL);
  ASSERT_TRUE(b != NULL);

  /* Socket file should exist after creation. */
  struct stat st;
  ASSERT_TRUE(stat(sock, &st) == 0);
  ASSERT_TRUE((st.st_mode & 0777) == 0600);

  /* Destroy cleans up. */
  broker_destroy(b);

  /* Socket file should be removed after destroy. */
  ASSERT_TRUE(stat(sock, &st) != 0);

  free(sock);
  (void)rmdir(tmpdir);
}

static void test_destroy_null(void) {
  /* Must not crash. */
  broker_destroy(NULL);
}

static void test_client_connect(void) {
  char tmpl[] = "/tmp/test_broker_XXXXXX";
  char *tmpdir = mkdtemp(tmpl);
  ASSERT_TRUE(tmpdir != NULL);

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();
  Broker *b = broker_create(sock, cm, NULL);
  ASSERT_TRUE(b != NULL);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  close(cfd);
  broker_destroy(b);
  free(sock);
  (void)rmdir(tmpdir);
}

static void test_session_counts_initial(void) {
  char tmpl[] = "/tmp/test_broker_XXXXXX";
  char *tmpdir = mkdtemp(tmpl);
  ASSERT_TRUE(tmpdir != NULL);

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();
  Broker *b = broker_create(sock, cm, NULL);
  ASSERT_TRUE(b != NULL);

  /* Fresh broker has zero sessions in both arrays. */
  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == 0);

  /* NULL broker returns 0. */
  ASSERT_TRUE(broker_active_count(NULL) == 0);
  ASSERT_TRUE(broker_idle_count(NULL) == 0);

  broker_destroy(b);
  free(sock);
  (void)rmdir(tmpdir);
}

static void test_disconnect_moves_to_idle(void) {
  char tmpl[] = "/tmp/test_broker_XXXXXX";
  char *tmpdir = mkdtemp(tmpl);
  ASSERT_TRUE(tmpdir != NULL);

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();
  Broker *b = broker_create(sock, cm, NULL);
  ASSERT_TRUE(b != NULL);

  /* Start broker_run in a background thread. */
  pthread_t tid;
  int prc = pthread_create(&tid, NULL, broker_run_thread, b);
  ASSERT_TRUE(prc == 0);

  /* Give the event loop time to start polling. */
  msleep(50);

  /* Connect a client. The poll loop should accept it. */
  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  /* Give the poll loop time to accept and register the session. */
  msleep(50);
  ASSERT_TRUE(broker_active_count(b) == 1);
  ASSERT_TRUE(broker_idle_count(b) == 0);

  /* Disconnect the client. The poll loop should detect POLLHUP and move
   * the session to idle. */
  close(cfd);
  msleep(50);

  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == 1);

  /* Cleanup: cancel the blocking event loop and join. */
  pthread_cancel(tid);
  pthread_join(tid, NULL);

  broker_destroy(b);
  free(sock);
  (void)rmdir(tmpdir);
}

static void test_multiple_disconnect_cycles(void) {
  char tmpl[] = "/tmp/test_broker_XXXXXX";
  char *tmpdir = mkdtemp(tmpl);
  ASSERT_TRUE(tmpdir != NULL);

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();
  Broker *b = broker_create(sock, cm, NULL);
  ASSERT_TRUE(b != NULL);

  pthread_t tid;
  int prc = pthread_create(&tid, NULL, broker_run_thread, b);
  ASSERT_TRUE(prc == 0);
  msleep(50);

  /* First connect/disconnect cycle. */
  int cfd1 = connect_client(sock);
  ASSERT_TRUE(cfd1 >= 0);
  msleep(50);
  close(cfd1);
  msleep(50);

  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == 1);

  /* Second connect/disconnect cycle — new session in active, then moved to
   * idle. The first idle session should still be there. */
  int cfd2 = connect_client(sock);
  ASSERT_TRUE(cfd2 >= 0);
  msleep(50);
  ASSERT_TRUE(broker_active_count(b) == 1);
  ASSERT_TRUE(broker_idle_count(b) == 1);

  close(cfd2);
  msleep(50);
  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == 2);

  pthread_cancel(tid);
  pthread_join(tid, NULL);

  broker_destroy(b);
  free(sock);
  (void)rmdir(tmpdir);
}

/* After MAX_IDLE_SESSIONS disconnect cycles idle is full. Further disconnects
 * should evict the oldest idle session, keeping idle_count capped. */
static void test_idle_sessions_cap(void) {
  char tmpl[] = "/tmp/test_broker_XXXXXX";
  char *tmpdir = mkdtemp(tmpl);
  ASSERT_TRUE(tmpdir != NULL);

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();
  Broker *b = broker_create(sock, cm, NULL);
  ASSERT_TRUE(b != NULL);

  pthread_t tid;
  int prc = pthread_create(&tid, NULL, broker_run_thread, b);
  ASSERT_TRUE(prc == 0);
  msleep(50);

  /* Fill idle_sessions to capacity. */
  int cycle = 0;
  while (cycle < MAX_IDLE_SESSIONS) {
    int cfd = connect_client(sock);
    ASSERT_TRUE(cfd >= 0);
    msleep(50);
    close(cfd);
    msleep(50);
    cycle++;
  }
  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == (size_t)MAX_IDLE_SESSIONS);

  /* Two more cycles past the cap: idle_count must stay at MAX_IDLE_SESSIONS. */
  for (int i = 0; i < 2; i++) {
    int cfd = connect_client(sock);
    ASSERT_TRUE(cfd >= 0);
    msleep(50);
    close(cfd);
    msleep(50);

    ASSERT_TRUE(broker_active_count(b) == 0);
    ASSERT_TRUE(broker_idle_count(b) == (size_t)MAX_IDLE_SESSIONS);
  }

  pthread_cancel(tid);
  pthread_join(tid, NULL);

  broker_destroy(b);
  free(sock);
  (void)rmdir(tmpdir);
}

int main(void) {
  test_create_null_args();
  test_create_and_destroy();
  test_destroy_null();
  test_client_connect();
  test_session_counts_initial();
  test_disconnect_moves_to_idle();
  test_multiple_disconnect_cycles();
  test_idle_sessions_cap();

  fprintf(stderr, "OK: test_broker\n");
  return 0;
}
