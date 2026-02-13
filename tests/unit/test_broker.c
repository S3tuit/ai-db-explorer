#define _GNU_SOURCE

#include <pthread.h>
#include <poll.h>
#include <errno.h>
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
#include "frame_codec.h"
#include "handshake_codec.h"
#include "secret_store.h"
#include "stdio_byte_channel.h"
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

/* Returns a stable non-zero secret token for broker_create test setup.
 * Ownership: returns pointer to static storage.
 * Side effects: none.
 * Error semantics: none.
 */
static const uint8_t *test_secret_token(void) {
  static const uint8_t token[SECRET_TOKEN_LEN] = {1};
  return token;
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

/* Writes exactly 'len' bytes from 'buf' to 'fd'.
 * It borrows 'fd' and 'buf' and does not allocate.
 * Side effects: performs blocking write syscalls on the socket.
 * Returns OK on full write, ERR on invalid input or syscall failure.
 */
static int write_all_fd(int fd, const void *buf, size_t len) {
  if (fd < 0 || (!buf && len != 0))
    return ERR;

  const uint8_t *p = (const uint8_t *)buf;
  size_t off = 0;
  while (off < len) {
    ssize_t n = write(fd, p + off, len - off);
    if (n > 0) {
      off += (size_t)n;
      continue;
    }
    if (n < 0 && errno == EINTR)
      continue;
    return ERR;
  }
  return OK;
}

/* Sends a length-prefixed frame using caller-controlled declared/payload sizes.
 * It borrows inputs and does not allocate.
 * Side effects: writes raw bytes to socket 'fd'.
 * Returns OK on successful write, ERR on invalid input or write failure.
 */
static int write_len_frame_raw(int fd, uint32_t declared_len,
                               const uint8_t *payload, size_t payload_len) {
  if (fd < 0 || (!payload && payload_len != 0))
    return ERR;

  uint8_t hdr[4];
  hdr[0] = (uint8_t)((declared_len >> 24) & 0xFFu);
  hdr[1] = (uint8_t)((declared_len >> 16) & 0xFFu);
  hdr[2] = (uint8_t)((declared_len >> 8) & 0xFFu);
  hdr[3] = (uint8_t)(declared_len & 0xFFu);

  if (write_all_fd(fd, hdr, sizeof(hdr)) != OK)
    return ERR;
  if (payload_len > 0 && write_all_fd(fd, payload, payload_len) != OK)
    return ERR;
  return OK;
}

/* Reads one broker handshake response frame and extracts status.
 * It borrows 'fd' using a non-owning wrapped channel.
 * Side effects: blocks up to 'timeout_ms' waiting for broker response data.
 * Returns OK when a valid handshake_resp_t frame is parsed into out_status,
 * ERR on timeout, malformed response, or I/O failure.
 */
static int read_handshake_status_with_timeout(int fd, int timeout_ms,
                                              handshake_status *out_status) {
  if (fd < 0 || timeout_ms < 0 || !out_status)
    return ERR;

  struct pollfd pfd = {
      .fd = fd,
      .events = POLLIN | POLLERR | POLLHUP,
      .revents = 0,
  };
  int prc = poll(&pfd, 1, timeout_ms);
  if (prc <= 0)
    return ERR;

  ByteChannel *ch = stdio_bytechannel_wrap_fd(fd, fd);
  if (!ch)
    return ERR;
  BufChannel *bc = bufch_create(ch);
  if (!bc) {
    bytech_destroy(ch);
    return ERR;
  }

  int rc = ERR;
  StrBuf payload = {0};
  if (frame_read_len(bc, &payload) != OK)
    goto done;
  handshake_resp_t resp = {0};
  if (handshake_resp_decode(&resp, (const uint8_t *)payload.data, payload.len) !=
      OK)
    goto done;
  *out_status = resp.status;
  rc = OK;

done:
  sb_clean(&payload);
  bufch_destroy(bc);
  return rc;
}

/* Initializes a valid handshake request that should be accepted by broker.
 * It borrows 'out_req' and writes in-place.
 * Side effects: none.
 * Returns OK on success, ERR on invalid input.
 */
static int make_valid_handshake_req(handshake_req_t *out_req) {
  if (!out_req)
    return ERR;
  memset(out_req, 0, sizeof(*out_req));
  out_req->magic = HANDSHAKE_MAGIC;
  out_req->version = HANDSHAKE_VERSION;
  memcpy(out_req->secret_token, test_secret_token(), SECRET_TOKEN_LEN);
  return OK;
}

static void msleep(int ms);
static void *broker_run_thread(void *arg);

/* Starts broker_run() in a background thread using a temporary socket path.
 * It allocates and returns tmpdir/sock paths owned by caller.
 * Side effects: creates Broker instance, starts background thread, and creates
 * filesystem entries under /tmp.
 * Returns OK on success, ERR on setup failures.
 */
static int start_running_broker(Broker **out_b, pthread_t *out_tid,
                                char **out_tmpdir, char **out_sock) {
  if (!out_b || !out_tid || !out_tmpdir || !out_sock)
    return ERR;
  *out_b = NULL;
  *out_tmpdir = NULL;
  *out_sock = NULL;

  const char *tmpl = "/tmp/test_broker_hs_XXXXXX";
  char *tmpdir = xmalloc(strlen(tmpl) + 1);
  strcpy(tmpdir, tmpl);
  if (!mkdtemp(tmpdir)) {
    free(tmpdir);
    return ERR;
  }

  char *sock = make_sock_path(tmpdir);
  ConnManager *cm = make_empty_cm();
  Broker *b = broker_create(sock, cm, test_secret_token());
  if (!b) {
    free(sock);
    (void)rmdir(tmpdir);
    free(tmpdir);
    connm_destroy(cm);
    return ERR;
  }

  int trc = pthread_create(out_tid, NULL, broker_run_thread, b);
  if (trc != 0) {
    broker_destroy(b);
    free(sock);
    (void)rmdir(tmpdir);
    free(tmpdir);
    return ERR;
  }

  msleep(50);
  *out_b = b;
  *out_tmpdir = tmpdir;
  *out_sock = sock;
  return OK;
}

/* Stops and tears down resources created by start_running_broker().
 * It borrows/destructs all inputs; accepts NULL values for best-effort cleanup.
 * Side effects: cancels/joins thread, destroys broker, and removes temp dir.
 * Error semantics: none.
 */
static void stop_running_broker(Broker *b, pthread_t tid, char *tmpdir,
                                char *sock) {
  if (b) {
    pthread_cancel(tid);
    pthread_join(tid, NULL);
    broker_destroy(b);
  }
  free(sock);
  if (tmpdir) {
    (void)rmdir(tmpdir);
    free(tmpdir);
  }
}

/* Asserts that broker has no active or idle sessions.
 * It borrows 'b' and does not allocate.
 * Side effects: none.
 * Error semantics: none (uses ASSERT_TRUE).
 */
static void assert_broker_empty(const Broker *b) {
  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == 0);
}

/* Waits until broker active session count matches 'target' or timeout.
 * It borrows 'b' and does not allocate memory.
 * Side effects: sleeps in small intervals while polling broker state.
 * Returns OK when target count is observed, ERR on invalid input or timeout.
 */
static int wait_for_active_count(const Broker *b, size_t target,
                                 int timeout_ms) {
  if (!b || timeout_ms < 0)
    return ERR;
  int elapsed = 0;
  struct timespec ts = {.tv_sec = 0, .tv_nsec = 10 * 1000000L};
  while (elapsed <= timeout_ms) {
    if (broker_active_count(b) == target)
      return OK;
    (void)nanosleep(&ts, NULL);
    elapsed += 10;
  }
  return ERR;
}

#define HS_RESP_TIMEOUT_MS 5000

/* Performs one broker handshake on an already connected client fd.
 * Ownership: borrows 'cfd'; uses non-owning channel wrapper so caller keeps fd
 * ownership.
 * Side effects: sends and receives one length-framed handshake message.
 * Error semantics: returns OK on successful transport and writes 'out_status';
 * returns ERR on framing/protocol I/O failures.
 */
static int client_handshake_on_fd(int cfd, handshake_status *out_status) {
  if (cfd < 0 || !out_status)
    return ERR;

  ByteChannel *ch = stdio_bytechannel_wrap_fd(cfd, cfd);
  if (!ch)
    return ERR;
  BufChannel *bc = bufch_create(ch);
  if (!bc) {
    bytech_destroy(ch);
    return ERR;
  }

  handshake_req_t req = {0};
  req.magic = HANDSHAKE_MAGIC;
  req.version = HANDSHAKE_VERSION;
  memcpy(req.secret_token, test_secret_token(), SECRET_TOKEN_LEN);
  uint8_t req_wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  if (handshake_req_encode(&req, req_wire) != OK) {
    bufch_destroy(bc);
    return ERR;
  }

  int rc = ERR;
  StrBuf payload = {0};
  if (frame_write_len(bc, req_wire, (uint32_t)sizeof(req_wire)) != OK)
    goto done;

  if (frame_read_len(bc, &payload) != OK)
    goto done;
  handshake_resp_t resp = {0};
  if (handshake_resp_decode(&resp, (const uint8_t *)payload.data, payload.len) !=
      OK) {
    goto done;
  }
  *out_status = resp.status;
  rc = OK;

done:
  sb_clean(&payload);
  bufch_destroy(bc);
  return rc;
}

/* Connects to broker socket and completes a successful handshake.
 * Ownership: borrows 'sock_path'; returns connected fd owned by caller.
 * Side effects: creates socket connection and performs framed handshake I/O.
 * Error semantics: returns fd on HS_OK, -1 on connect/handshake failure.
 */
static int connect_client_hs_ok(const char *sock_path) {
  int cfd = connect_client(sock_path);
  if (cfd < 0)
    return -1;

  handshake_status st = HS_ERR_INTERNAL;
  if (client_handshake_on_fd(cfd, &st) != OK || st != HS_OK) {
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

  Broker *b = broker_create(sock, cm, test_secret_token());
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
  Broker *b = broker_create(sock, cm, test_secret_token());
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
  Broker *b = broker_create(sock, cm, test_secret_token());
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
  Broker *b = broker_create(sock, cm, test_secret_token());
  ASSERT_TRUE(b != NULL);

  /* Start broker_run in a background thread. */
  pthread_t tid;
  int prc = pthread_create(&tid, NULL, broker_run_thread, b);
  ASSERT_TRUE(prc == 0);

  /* Give the event loop time to start polling. */
  msleep(50);

  /* Connect a client. The poll loop should accept it. */
  int cfd = connect_client_hs_ok(sock);
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
  Broker *b = broker_create(sock, cm, test_secret_token());
  ASSERT_TRUE(b != NULL);

  pthread_t tid;
  int prc = pthread_create(&tid, NULL, broker_run_thread, b);
  ASSERT_TRUE(prc == 0);
  msleep(50);

  /* First connect/disconnect cycle. */
  int cfd1 = connect_client_hs_ok(sock);
  ASSERT_TRUE(cfd1 >= 0);
  msleep(50);
  close(cfd1);
  msleep(50);

  ASSERT_TRUE(broker_active_count(b) == 0);
  ASSERT_TRUE(broker_idle_count(b) == 1);

  /* Second connect/disconnect cycle — new session in active, then moved to
   * idle. The first idle session should still be there. */
  int cfd2 = connect_client_hs_ok(sock);
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
  Broker *b = broker_create(sock, cm, test_secret_token());
  ASSERT_TRUE(b != NULL);

  pthread_t tid;
  int prc = pthread_create(&tid, NULL, broker_run_thread, b);
  ASSERT_TRUE(prc == 0);
  msleep(50);

  /* Fill idle_sessions to capacity. */
  int cycle = 0;
  while (cycle < MAX_IDLE_SESSIONS) {
    int cfd = connect_client_hs_ok(sock);
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
    int cfd = connect_client_hs_ok(sock);
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

/* Verifies handshake rejects frames with invalid magic and keeps broker state
 * unchanged.
 */
static void test_hs_bad_magic_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_req_t req = {0};
  ASSERT_TRUE(make_valid_handshake_req(&req) == OK);
  req.magic ^= 1u;
  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  ASSERT_TRUE(handshake_req_encode(&req, wire) == OK);
  ASSERT_TRUE(write_len_frame_raw(cfd, (uint32_t)sizeof(wire), wire,
                                  sizeof(wire)) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_BAD_MAGIC);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies handshake rejects frames with unsupported version. */
static void test_hs_bad_version_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_req_t req = {0};
  ASSERT_TRUE(make_valid_handshake_req(&req) == OK);
  req.version = (uint16_t)(HANDSHAKE_VERSION + 1u);
  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  ASSERT_TRUE(handshake_req_encode(&req, wire) == OK);
  ASSERT_TRUE(write_len_frame_raw(cfd, (uint32_t)sizeof(wire), wire,
                                  sizeof(wire)) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_BAD_VERSION);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies handshake rejects frames with invalid secret token. */
static void test_hs_bad_secret_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_req_t req = {0};
  ASSERT_TRUE(make_valid_handshake_req(&req) == OK);
  req.secret_token[0] ^= 0xFFu;
  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  ASSERT_TRUE(handshake_req_encode(&req, wire) == OK);
  ASSERT_TRUE(write_len_frame_raw(cfd, (uint32_t)sizeof(wire), wire,
                                  sizeof(wire)) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_TOKEN_UNKNOWN);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies handshake rejects frames whose declared size differs from the
 * expected request struct size.
 */
static void test_hs_len_mismatch_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_req_t req = {0};
  ASSERT_TRUE(make_valid_handshake_req(&req) == OK);
  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  ASSERT_TRUE(handshake_req_encode(&req, wire) == OK);
  size_t short_len = HANDSHAKE_REQ_WIRE_SIZE - 1u;
  ASSERT_TRUE(write_len_frame_raw(cfd, (uint32_t)short_len,
                                  wire, short_len) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_REQ);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies handshake rejects declared frame sizes that exceed codec limits. */
static void test_hs_declared_too_large_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  ASSERT_TRUE(write_len_frame_raw(cfd, UINT32_MAX, NULL, 0) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_REQ);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies truncated payload + peer EOF is treated as malformed handshake. */
static void test_hs_truncated_then_eof_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_req_t req = {0};
  ASSERT_TRUE(make_valid_handshake_req(&req) == OK);
  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  ASSERT_TRUE(handshake_req_encode(&req, wire) == OK);
  size_t partial = HANDSHAKE_REQ_WIRE_SIZE - 2u;
  ASSERT_TRUE(write_len_frame_raw(cfd, (uint32_t)HANDSHAKE_REQ_WIRE_SIZE, wire,
                                  partial) == OK);
  ASSERT_TRUE(shutdown(cfd, SHUT_WR) == 0);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_REQ);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies truncated payload with socket left open is bounded by handshake
 * timeout and rejected.
 */
static void test_hs_truncated_keep_open_timeout_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_req_t req = {0};
  ASSERT_TRUE(make_valid_handshake_req(&req) == OK);
  uint8_t wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  ASSERT_TRUE(handshake_req_encode(&req, wire) == OK);
  size_t partial = HANDSHAKE_REQ_WIRE_SIZE - 2u;
  ASSERT_TRUE(write_len_frame_raw(cfd, (uint32_t)HANDSHAKE_REQ_WIRE_SIZE, wire,
                                  partial) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_REQ);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies partial frame header with open socket is bounded by handshake
 * timeout and rejected.
 */
static void test_hs_partial_header_keep_open_timeout_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  uint8_t hdr_prefix[2] = {0x00, 0x00};
  ASSERT_TRUE(write_all_fd(cfd, hdr_prefix, sizeof(hdr_prefix)) == OK);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_REQ);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies clients that send no handshake bytes are timed out and rejected. */
static void test_hs_no_payload_timeout_rejected(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client(sock);
  ASSERT_TRUE(cfd >= 0);

  handshake_status st = HS_OK;
  ASSERT_TRUE(read_handshake_status_with_timeout(cfd, HS_RESP_TIMEOUT_MS, &st) ==
              OK);
  ASSERT_TRUE(st == HS_ERR_REQ);

  close(cfd);
  msleep(50);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies that a post-handshake truncated request frame is dropped after a
 * bounded timeout, preventing broker_run() stalls.
 */
static void test_post_hs_truncated_request_drops_session(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client_hs_ok(sock);
  ASSERT_TRUE(cfd >= 0);
  ASSERT_TRUE(wait_for_active_count(b, 1, 1000) == OK);

  static const uint8_t req_prefix[] =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\"";
  uint32_t declared_len = (uint32_t)(sizeof(req_prefix) + 2u);
  ASSERT_TRUE(write_len_frame_raw(cfd, declared_len, req_prefix,
                                  sizeof(req_prefix)) == OK);

  ASSERT_TRUE(wait_for_active_count(b, 0, 5000) == OK);
  ASSERT_TRUE(broker_idle_count(b) == 0);

  close(cfd);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
}

/* Verifies that oversize post-handshake request frames are rejected and the
 * active session is dropped.
 */
static void test_post_hs_oversized_request_drops_session(void) {
  Broker *b = NULL;
  pthread_t tid = {0};
  char *tmpdir = NULL;
  char *sock = NULL;
  ASSERT_TRUE(start_running_broker(&b, &tid, &tmpdir, &sock) == OK);

  int cfd = connect_client_hs_ok(sock);
  ASSERT_TRUE(cfd >= 0);
  ASSERT_TRUE(wait_for_active_count(b, 1, 1000) == OK);

  ASSERT_TRUE(write_len_frame_raw(cfd, UINT32_MAX, NULL, 0) == OK);

  ASSERT_TRUE(wait_for_active_count(b, 0, 2000) == OK);
  ASSERT_TRUE(broker_idle_count(b) == 0);

  close(cfd);
  assert_broker_empty(b);
  stop_running_broker(b, tid, tmpdir, sock);
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
  test_hs_bad_magic_rejected();
  test_hs_bad_version_rejected();
  test_hs_bad_secret_rejected();
  test_hs_len_mismatch_rejected();
  test_hs_declared_too_large_rejected();
  test_hs_truncated_then_eof_rejected();
  test_hs_truncated_keep_open_timeout_rejected();
  test_hs_partial_header_keep_open_timeout_rejected();
  test_hs_no_payload_timeout_rejected();
  test_post_hs_truncated_request_drops_session();
  test_post_hs_oversized_request_drops_session();

  fprintf(stderr, "OK: test_broker\n");
  return 0;
}
