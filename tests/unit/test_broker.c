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

/* Waits until socket 'fd' reports hangup/error.
 * It borrows 'fd' and does not allocate.
 * Side effects: blocks in poll up to timeout_ms.
 * Error semantics: returns OK on hangup/error observation, ERR on invalid input
 * or timeout.
 */
static int wait_for_fd_hangup(int fd, int timeout_ms) {
  if (fd < 0 || timeout_ms < 0)
    return ERR;

  int elapsed = 0;
  while (elapsed <= timeout_ms) {
    struct pollfd pfd = {
        .fd = fd,
        .events = POLLIN | POLLERR | POLLHUP | POLLNVAL,
        .revents = 0,
    };
    int prc = poll(&pfd, 1, 10);
    if (prc > 0 && (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)))
      return OK;
    elapsed += 10;
  }
  return ERR;
}

#define HS_RESP_TIMEOUT_MS 5000

/* Performs one broker handshake request/response exchange on a connected fd.
 * Ownership: borrows 'cfd'; uses non-owning channel wrapper so caller keeps fd
 * ownership. Borrows 'req' and writes response into caller-owned 'out_resp'.
 * Side effects: sends one frame and reads one frame on the socket.
 * Error semantics: returns OK on successful framed exchange and decode, ERR on
 * invalid input or transport/decode failure.
 */
static int client_handshake_req_on_fd(int cfd, const handshake_req_t *req,
                                      handshake_resp_t *out_resp) {
  if (cfd < 0 || !req || !out_resp)
    return ERR;

  ByteChannel *ch = stdio_bytechannel_wrap_fd(cfd, cfd);
  if (!ch)
    return ERR;
  BufChannel *bc = bufch_create(ch);
  if (!bc) {
    bytech_destroy(ch);
    return ERR;
  }

  uint8_t req_wire[HANDSHAKE_REQ_WIRE_SIZE] = {0};
  if (handshake_req_encode(req, req_wire) != OK) {
    bufch_destroy(bc);
    return ERR;
  }

  int rc = ERR;
  StrBuf payload = {0};
  if (frame_write_len(bc, req_wire, (uint32_t)sizeof(req_wire)) != OK)
    goto done;

  if (frame_read_len(bc, &payload) != OK)
    goto done;
  if (handshake_resp_decode(out_resp, (const uint8_t *)payload.data,
                            payload.len) != OK) {
    goto done;
  }
  rc = OK;

done:
  sb_clean(&payload);
  bufch_destroy(bc);
  return rc;
}

/* Performs a fresh-session handshake on a connected client fd.
 * Ownership: borrows 'cfd'. Optionally writes returned resume token.
 * Side effects: performs framed handshake I/O.
 * Error semantics: returns OK on transport/decode success and writes status.
 */
static int client_handshake_on_fd(
    int cfd, handshake_status *out_status,
    uint8_t out_resume_token[RESUME_TOKEN_LEN]) {
  if (cfd < 0 || !out_status)
    return ERR;

  handshake_req_t req = {0};
  req.magic = HANDSHAKE_MAGIC;
  req.version = HANDSHAKE_VERSION;
  memcpy(req.secret_token, test_secret_token(), SECRET_TOKEN_LEN);

  handshake_resp_t resp = {0};
  if (client_handshake_req_on_fd(cfd, &req, &resp) != OK)
    return ERR;

  *out_status = resp.status;
  if (out_resume_token)
    memcpy(out_resume_token, resp.resume_token, RESUME_TOKEN_LEN);
  return OK;
}

/* Performs a resume handshake on a connected client fd.
 * Ownership: borrows 'cfd' and 'resume_token'. Optionally writes new token.
 * Side effects: performs framed handshake I/O.
 * Error semantics: returns OK on transport/decode success and writes status.
 */
static int client_resume_handshake_on_fd(
    int cfd, const uint8_t resume_token[RESUME_TOKEN_LEN],
    handshake_status *out_status, uint8_t out_resume_token[RESUME_TOKEN_LEN]) {
  if (cfd < 0 || !resume_token || !out_status)
    return ERR;

  handshake_req_t req = {0};
  req.magic = HANDSHAKE_MAGIC;
  req.version = HANDSHAKE_VERSION;
  req.flags = HANDSHAKE_FLAG_RESUME;
  memcpy(req.secret_token, test_secret_token(), SECRET_TOKEN_LEN);
  memcpy(req.resume_token, resume_token, RESUME_TOKEN_LEN);

  handshake_resp_t resp = {0};
  if (client_handshake_req_on_fd(cfd, &req, &resp) != OK)
    return ERR;

  *out_status = resp.status;
  if (out_resume_token)
    memcpy(out_resume_token, resp.resume_token, RESUME_TOKEN_LEN);
  return OK;
}

/* Connects to broker socket and completes a successful fresh handshake.
 * Ownership: borrows 'sock_path'; returns connected fd owned by caller.
 * Side effects: creates socket connection and performs framed handshake I/O.
 * Error semantics: returns fd on HS_OK, -1 on connect/handshake failure.
 */
static int connect_client_hs_ok(
    const char *sock_path, uint8_t out_resume_token[RESUME_TOKEN_LEN]) {
  int cfd = connect_client(sock_path);
  if (cfd < 0)
    return -1;

  handshake_status st = HS_ERR_INTERNAL;
  if (client_handshake_on_fd(cfd, &st, out_resume_token) != OK || st != HS_OK) {
    close(cfd);
    return -1;
  }
  return cfd;
}

/* Opens a connection and performs a resume handshake, then closes the socket.
 * Ownership: borrows 'sock_path' and 'resume_token'; socket is always closed.
 * Side effects: creates socket and performs framed handshake I/O.
 * Error semantics: returns OK on transport/decode success and writes status.
 */
static int connect_client_resume_status(
    const char *sock_path, const uint8_t resume_token[RESUME_TOKEN_LEN],
    handshake_status *out_status, uint8_t out_new_resume_token[RESUME_TOKEN_LEN]) {
  if (!sock_path || !resume_token || !out_status)
    return ERR;

  int cfd = connect_client(sock_path);
  if (cfd < 0)
    return ERR;

  int rc = client_resume_handshake_on_fd(cfd, resume_token, out_status,
                                         out_new_resume_token);
  close(cfd);
  return rc;
}

/* Probes that broker can accept one fresh client handshake.
 * Ownership: borrows 'sock_path'; probe connection is closed before return.
 * Side effects: performs one successful client handshake + disconnect cycle.
 * Error semantics: none (uses ASSERT_TRUE).
 */
static void assert_broker_accepts_new_client(const char *sock_path) {
  int probe_fd = connect_client_hs_ok(sock_path, NULL);
  ASSERT_TRUE(probe_fd >= 0);
  close(probe_fd);
  msleep(50);
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

  /* Connect and complete initial handshake, capture resume token. */
  uint8_t tok1[RESUME_TOKEN_LEN] = {0};
  int cfd = connect_client_hs_ok(sock, tok1);
  ASSERT_TRUE(cfd >= 0);

  /* Disconnect the client so session becomes resumable. */
  close(cfd);
  msleep(50);

  /* Resume with the prior token; this proves the session moved to idle. */
  handshake_status st = HS_ERR_INTERNAL;
  ASSERT_TRUE(connect_client_resume_status(sock, tok1, &st, NULL) == OK);
  ASSERT_TRUE(st == HS_OK);

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
  uint8_t tok1[RESUME_TOKEN_LEN] = {0};
  int cfd1 = connect_client_hs_ok(sock, tok1);
  ASSERT_TRUE(cfd1 >= 0);
  close(cfd1);
  msleep(50);

  /* Second connect/disconnect cycle creates another resumable token. */
  uint8_t tok2[RESUME_TOKEN_LEN] = {0};
  int cfd2 = connect_client_hs_ok(sock, tok2);
  ASSERT_TRUE(cfd2 >= 0);
  close(cfd2);
  msleep(50);

  /* Both tokens should still be resumable. */
  handshake_status st = HS_ERR_INTERNAL;
  ASSERT_TRUE(connect_client_resume_status(sock, tok1, &st, NULL) == OK);
  ASSERT_TRUE(st == HS_OK);
  ASSERT_TRUE(connect_client_resume_status(sock, tok2, &st, NULL) == OK);
  ASSERT_TRUE(st == HS_OK);

  pthread_cancel(tid);
  pthread_join(tid, NULL);

  broker_destroy(b);
  free(sock);
  (void)rmdir(tmpdir);
}

/* After MAX_IDLE_SESSIONS disconnect cycles idle is full. Further disconnects
 * should evict old resume tokens while keeping newer ones resumable.
 */
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

  uint8_t tokens[MAX_IDLE_SESSIONS + 2][RESUME_TOKEN_LEN];

  /* Create more resumable sessions than idle capacity allows. */
  for (int i = 0; i < MAX_IDLE_SESSIONS + 2; i++) {
    int cfd = connect_client_hs_ok(sock, tokens[i]);
    ASSERT_TRUE(cfd >= 0);
    close(cfd);
    msleep(50);
  }

  /* Oldest token should be evicted once cap is exceeded. */
  handshake_status st = HS_ERR_INTERNAL;
  ASSERT_TRUE(connect_client_resume_status(sock, tokens[0], &st, NULL) == OK);
  ASSERT_TRUE(st == HS_ERR_TOKEN_UNKNOWN);

  /* Most recent token should still be resumable. */
  ASSERT_TRUE(connect_client_resume_status(
                  sock, tokens[MAX_IDLE_SESSIONS + 1], &st, NULL) == OK);
  ASSERT_TRUE(st == HS_OK);

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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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
  assert_broker_accepts_new_client(sock);
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

  uint8_t resume_tok[RESUME_TOKEN_LEN] = {0};
  int cfd = connect_client_hs_ok(sock, resume_tok);
  ASSERT_TRUE(cfd >= 0);

  static const uint8_t req_prefix[] =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\"";
  uint32_t declared_len = (uint32_t)(sizeof(req_prefix) + 2u);
  ASSERT_TRUE(write_len_frame_raw(cfd, declared_len, req_prefix,
                                  sizeof(req_prefix)) == OK);

  ASSERT_TRUE(wait_for_fd_hangup(cfd, 5000) == OK);

  close(cfd);

  /* Dropped malformed sessions must not remain resumable in idle storage. */
  handshake_status st = HS_OK;
  ASSERT_TRUE(connect_client_resume_status(sock, resume_tok, &st, NULL) == OK);
  ASSERT_TRUE(st == HS_ERR_TOKEN_UNKNOWN);
  assert_broker_accepts_new_client(sock);
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

  uint8_t resume_tok[RESUME_TOKEN_LEN] = {0};
  int cfd = connect_client_hs_ok(sock, resume_tok);
  ASSERT_TRUE(cfd >= 0);

  ASSERT_TRUE(write_len_frame_raw(cfd, UINT32_MAX, NULL, 0) == OK);

  ASSERT_TRUE(wait_for_fd_hangup(cfd, 3000) == OK);

  close(cfd);
  handshake_status st = HS_OK;
  ASSERT_TRUE(connect_client_resume_status(sock, resume_tok, &st, NULL) == OK);
  ASSERT_TRUE(st == HS_ERR_TOKEN_UNKNOWN);
  assert_broker_accepts_new_client(sock);
  stop_running_broker(b, tid, tmpdir, sock);
}

int main(void) {
  test_create_null_args();
  test_create_and_destroy();
  test_destroy_null();
  test_client_connect();
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
