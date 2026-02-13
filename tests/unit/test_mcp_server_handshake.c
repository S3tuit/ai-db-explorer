#define _GNU_SOURCE

#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "file_io.h"
#include "frame_codec.h"
#include "handshake_codec.h"
#include "mcp_server.h"
#include "private_dir.h"
#include "resume_token.h"
#include "stdio_byte_channel.h"
#include "test.h"
#include "utils.h"

typedef struct MockHandshakeStep {
  int expect_resume;
  uint8_t expect_resume_token[RESUME_TOKEN_LEN];
  handshake_status reply_status;
  uint8_t reply_resume_token[RESUME_TOKEN_LEN];
} MockHandshakeStep;

typedef struct MockBrokerCtx {
  const char *sock_path;
  uint8_t expected_secret[SECRET_TOKEN_LEN];
  MockHandshakeStep steps[2];
  size_t n_steps;
  int rc;
} MockBrokerCtx;

/* Creates and returns a unique temporary directory path.
 * Ownership: returns heap string owned by caller.
 * Side effects: creates a directory under /tmp.
 * Error semantics: asserts on setup failures and returns non-NULL path.
 */
static char *make_tmpdir(void) {
  char tmpl[] = "/tmp/test_mcp_hs_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  size_t len = strlen(dir);
  char *copy = xmalloc(len + 1);
  memcpy(copy, dir, len + 1);
  return copy;
}

/* Sets runtime environment variable used by resume token storage directory.
 * Ownership: borrows 'tmpdir'; no allocations.
 * Side effects: mutates process environment.
 * Error semantics: asserts on unsupported platform or setenv failures.
 */
static void set_runtime_env(const char *tmpdir) {
#ifdef __linux__
  ASSERT_TRUE(setenv("XDG_RUNTIME_DIR", tmpdir, 1) == 0);
#elif defined(__APPLE__)
  ASSERT_TRUE(setenv("TMPDIR", tmpdir, 1) == 0);
#else
  (void)tmpdir;
  ASSERT_TRUE(0 && "unsupported platform in test");
#endif
}

/* Sleeps for roughly 'ms' milliseconds.
 * Ownership: no allocations.
 * Side effects: blocks current thread.
 * Error semantics: none (best-effort sleep helper).
 */
static void msleep(int ms) {
  struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
  (void)nanosleep(&ts, NULL);
}

/* Waits until a socket path exists or timeout expires.
 * Ownership: borrows 'path'; no allocations.
 * Side effects: polls filesystem metadata and sleeps briefly.
 * Error semantics: returns OK when path appears, ERR on timeout/invalid input.
 */
static int wait_for_path(const char *path, int timeout_ms) {
  if (!path || timeout_ms < 0)
    return ERR;
  int elapsed = 0;
  while (elapsed <= timeout_ms) {
    if (access(path, F_OK) == 0)
      return OK;
    msleep(10);
    elapsed += 10;
  }
  return ERR;
}

/* Creates, binds, and listens on a Unix-domain socket path.
 * Ownership: borrows 'sock_path'; returns listen fd owned by caller.
 * Side effects: unlinks existing path and creates a listening socket inode.
 * Error semantics: returns fd on success, -1 on invalid input or syscall
 * failures.
 */
static int make_listen_socket(const char *sock_path) {
  if (!sock_path)
    return -1;

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  struct sockaddr_un addr = {0};
  addr.sun_family = AF_UNIX;
  if (strlen(sock_path) >= sizeof(addr.sun_path)) {
    (void)close(fd);
    return -1;
  }
  (void)unlink(sock_path);
  (void)strcpy(addr.sun_path, sock_path);

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    (void)close(fd);
    return -1;
  }
  if (listen(fd, 8) != 0) {
    (void)close(fd);
    return -1;
  }
  return fd;
}

/* Handles one handshake request/response exchange on accepted client socket.
 * Ownership: takes ownership of 'cfd' for this function duration and closes it
 * through channel cleanup.
 * Side effects: reads/writes one framed handshake message.
 * Error semantics: returns OK on protocol match and successful response write,
 * ERR on framing/protocol mismatch or I/O failure.
 */
static int mock_handle_handshake(int cfd, const uint8_t expected_secret[32],
                                 const MockHandshakeStep *step) {
  if (cfd < 0 || !expected_secret || !step)
    return ERR;

  BufChannel *bc = xmalloc(sizeof(*bc));
  if (bufch_stdio_openfd_init(bc, cfd, cfd) != OK) {
    return ERR;
  }

  StrBuf payload = {0};
  int rc = frame_read_len(bc, &payload);
  if (rc != OK) {
    sb_clean(&payload);
    bufch_destroy(bc);
    return ERR;
  }

  handshake_req_t req = {0};
  if (handshake_req_decode(&req, (const uint8_t *)payload.data, payload.len) !=
      OK) {
    sb_clean(&payload);
    bufch_destroy(bc);
    return ERR;
  }
  sb_clean(&payload);

  if (req.magic != HANDSHAKE_MAGIC || req.version != HANDSHAKE_VERSION) {
    bufch_destroy(bc);
    return ERR;
  }
  if (memcmp(req.secret_token, expected_secret, SECRET_TOKEN_LEN) != 0) {
    bufch_destroy(bc);
    return ERR;
  }

  int has_resume = ((req.flags & HANDSHAKE_FLAG_RESUME) != 0) ? YES : NO;
  if (step->expect_resume == YES) {
    if (has_resume != YES) {
      bufch_destroy(bc);
      return ERR;
    }
    if (memcmp(req.resume_token, step->expect_resume_token, RESUME_TOKEN_LEN) !=
        0) {
      bufch_destroy(bc);
      return ERR;
    }
  } else if (has_resume != NO) {
    bufch_destroy(bc);
    return ERR;
  }

  handshake_resp_t resp = {0};
  resp.magic = HANDSHAKE_MAGIC;
  resp.version = HANDSHAKE_VERSION;
  resp.status = step->reply_status;
  memcpy(resp.resume_token, step->reply_resume_token, RESUME_TOKEN_LEN);
  resp.idle_ttl_secs = 123;
  resp.abs_ttl_secs = 456;

  uint8_t wire[HANDSHAKE_RESP_WIRE_SIZE] = {0};
  if (handshake_resp_encode(&resp, wire) != OK) {
    bufch_destroy(bc);
    return ERR;
  }
  int wrc = frame_write_len(bc, wire, (uint32_t)sizeof(wire));
  bufch_destroy(bc);
  return (wrc == OK) ? OK : ERR;
}

/* Runs a scripted mock broker handshake server.
 * Ownership: borrows 'arg' pointer and fields.
 * Side effects: opens listening socket, accepts connections, and performs
 * framed handshake I/O.
 * Error semantics: writes final status into ctx->rc and returns NULL.
 */
static void *mock_broker_thread(void *arg) {
  MockBrokerCtx *ctx = (MockBrokerCtx *)arg;
  if (!ctx || !ctx->sock_path) {
    if (ctx)
      ctx->rc = ERR;
    return NULL;
  }

  ctx->rc = OK;
  int listen_fd = make_listen_socket(ctx->sock_path);
  if (listen_fd < 0) {
    ctx->rc = ERR;
    return NULL;
  }

  for (size_t i = 0; i < ctx->n_steps; i++) {
    struct pollfd pfd = {.fd = listen_fd, .events = POLLIN, .revents = 0};
    if (poll(&pfd, 1, 3000) != 1) {
      ctx->rc = ERR;
      break;
    }

    int cfd = accept(listen_fd, NULL, NULL);
    if (cfd < 0) {
      ctx->rc = ERR;
      break;
    }

    if (mock_handle_handshake(cfd, ctx->expected_secret, &ctx->steps[i]) !=
        OK) {
      ctx->rc = ERR;
      break;
    }
  }

  (void)close(listen_fd);
  (void)unlink(ctx->sock_path);
  return NULL;
}

/* Loads the persisted resume token for current runtime dir and compares it.
 * Ownership: borrows 'expected'; uses temporary store that is cleaned.
 * Side effects: reads and then deletes token file.
 * Error semantics: asserts on failures.
 */
static void
assert_persisted_token_eq(const uint8_t expected[RESUME_TOKEN_LEN]) {
  ResumeTokenStore store = {0};
  ASSERT_TRUE(restok_init(&store) == YES);

  uint8_t got[RESUME_TOKEN_LEN] = {0};
  ASSERT_TRUE(restok_load(&store, got) == YES);
  ASSERT_TRUE(memcmp(got, expected, RESUME_TOKEN_LEN) == 0);

  ASSERT_TRUE(restok_delete(&store) == OK);
  ASSERT_TRUE(rmdir(store.dir_path) == 0);
  restok_clean(&store);
}

/* Writes broker shared secret token file into resolved private directory.
 * Ownership: borrows 'pd' and token bytes.
 * Side effects: writes secret token file.
 * Error semantics: asserts on setup failures.
 */
static void setup_secret_token_file(const PrivDir *pd,
                                    const uint8_t secret[SECRET_TOKEN_LEN]) {
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(pd->token_path != NULL);
  ASSERT_TRUE(secret != NULL);

  ASSERT_TRUE(
      fileio_write_exact(pd->token_path, secret, SECRET_TOKEN_LEN, 0600) == OK);
}

/* Verifies first-time broker handshake stores returned resume token. */
static void test_handshake_new_session_persists_token(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  PrivDir *pd = privdir_resolve(tmpdir);
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(privdir_create_layout(pd) == OK);

  uint8_t secret[SECRET_TOKEN_LEN] = {0};
  for (size_t i = 0; i < SECRET_TOKEN_LEN; i++) {
    secret[i] = (uint8_t)(i + 1);
  }
  setup_secret_token_file(pd, secret);

  uint8_t issued[RESUME_TOKEN_LEN] = {0};
  for (size_t i = 0; i < RESUME_TOKEN_LEN; i++) {
    issued[i] = (uint8_t)(0xA0u + i);
  }

  MockBrokerCtx ctx = {0};
  ctx.sock_path = pd->sock_path;
  memcpy(ctx.expected_secret, secret, SECRET_TOKEN_LEN);
  ctx.n_steps = 1;
  ctx.steps[0].expect_resume = NO;
  ctx.steps[0].reply_status = HS_OK;
  memcpy(ctx.steps[0].reply_resume_token, issued, RESUME_TOKEN_LEN);

  pthread_t tid;
  ASSERT_TRUE(pthread_create(&tid, NULL, mock_broker_thread, &ctx) == 0);
  ASSERT_TRUE(wait_for_path(pd->sock_path, 1000) == OK);

  FILE *in = MEMFILE_OUT();
  FILE *out = MEMFILE_OUT();
  McpServer server = {0};
  McpServerInit init = {
      .in = in,
      .out = out,
      .privd = pd,
  };
  int init_rc = mcpser_init(&server, &init);
  if (init_rc != OK)
    fprintf(stderr, "mcpser_init failed: %s\n", mcpser_last_error(&server));
  ASSERT_TRUE(init_rc == OK);
  mcpser_clean(&server);
  fclose(in);
  fclose(out);

  ASSERT_TRUE(pthread_join(tid, NULL) == 0);
  ASSERT_TRUE(ctx.rc == OK);

  assert_persisted_token_eq(issued);

  privdir_cleanup(pd);
  privdir_free(pd);
  (void)rmdir(tmpdir);
  free(tmpdir);
}

/* Verifies expired/unknown resume token triggers delete and one fresh retry. */
static void test_handshake_retry_after_unknown_resume(void) {
  char *tmpdir = make_tmpdir();
  set_runtime_env(tmpdir);

  PrivDir *pd = privdir_resolve(tmpdir);
  ASSERT_TRUE(pd != NULL);
  ASSERT_TRUE(privdir_create_layout(pd) == OK);

  uint8_t secret[SECRET_TOKEN_LEN] = {0};
  for (size_t i = 0; i < SECRET_TOKEN_LEN; i++) {
    secret[i] = (uint8_t)(0x11u + i);
  }
  setup_secret_token_file(pd, secret);

  uint8_t stale[RESUME_TOKEN_LEN] = {0};
  for (size_t i = 0; i < RESUME_TOKEN_LEN; i++) {
    stale[i] = (uint8_t)(0x44u + i);
  }

  ResumeTokenStore prep = {0};
  ASSERT_TRUE(restok_init(&prep) == YES);
  ASSERT_TRUE(restok_store(&prep, stale) == OK);
  restok_clean(&prep);

  uint8_t fresh[RESUME_TOKEN_LEN] = {0};
  for (size_t i = 0; i < RESUME_TOKEN_LEN; i++) {
    fresh[i] = (uint8_t)(0x80u + i);
  }

  MockBrokerCtx ctx = {0};
  ctx.sock_path = pd->sock_path;
  memcpy(ctx.expected_secret, secret, SECRET_TOKEN_LEN);
  ctx.n_steps = 2;
  ctx.steps[0].expect_resume = YES;
  memcpy(ctx.steps[0].expect_resume_token, stale, RESUME_TOKEN_LEN);
  ctx.steps[0].reply_status = HS_ERR_TOKEN_UNKNOWN;
  ctx.steps[1].expect_resume = NO;
  ctx.steps[1].reply_status = HS_OK;
  memcpy(ctx.steps[1].reply_resume_token, fresh, RESUME_TOKEN_LEN);

  pthread_t tid;
  ASSERT_TRUE(pthread_create(&tid, NULL, mock_broker_thread, &ctx) == 0);
  ASSERT_TRUE(wait_for_path(pd->sock_path, 1000) == OK);

  FILE *in = MEMFILE_OUT();
  FILE *out = MEMFILE_OUT();
  McpServer server = {0};
  McpServerInit init = {
      .in = in,
      .out = out,
      .privd = pd,
  };
  int init_rc = mcpser_init(&server, &init);
  if (init_rc != OK) {
    fprintf(stderr, "mcpser_init failed: %s\n", mcpser_last_error(&server));
  }
  ASSERT_TRUE(init_rc == OK);
  mcpser_clean(&server);
  fclose(in);
  fclose(out);

  ASSERT_TRUE(pthread_join(tid, NULL) == 0);
  ASSERT_TRUE(ctx.rc == OK);

  assert_persisted_token_eq(fresh);

  privdir_cleanup(pd);
  privdir_free(pd);
  (void)rmdir(tmpdir);
  free(tmpdir);
}

int main(void) {
  test_handshake_new_session_persists_token();
  test_handshake_retry_after_unknown_resume();

  fprintf(stderr, "OK: test_mcp_server_handshake\n");
  return 0;
}
