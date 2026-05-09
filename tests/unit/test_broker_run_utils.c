#define _GNU_SOURCE

#include "test_broker_run_utils.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "file_io.h"
#include "frame_codec.h"
#include "secret_store.h"
#include "stdio_byte_channel.h"
#include "test.h"

typedef struct {
  SecretStore base;
} FakeSecretStore;

/* Returns one fake secret-store password without consulting external state.
 * It borrows all inputs and writes a single NUL byte into 'out'.
 * Side effects: grows 'out'.
 * Returns YES on success, ERR on invalid input or append failure.
 */
static AdbxTriStatus fake_ss_get(SecretStore *store, const SecretRefInfo *ref,
                                 StrBuf *out, SecretStoreErr *out_err) {
  (void)store;
  (void)ref;
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  if (!out)
    return ERR;
  return (sb_append_bytes(out, "", 1) == OK) ? YES : ERR;
}

/* Accepts every fake secret-store write/delete operation.
 * It borrows all inputs and performs no allocations.
 * Side effects: clears one caller-owned error object.
 * Returns OK on success.
 */
static AdbxStatus fake_ss_ok(SecretStore *store, const SecretRefInfo *ref,
                             const char *secret, SecretStoreErr *out_err) {
  (void)store;
  (void)ref;
  (void)secret;
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  return OK;
}

/* Accepts every fake secret-store delete operation.
 * It borrows all inputs and performs no allocations.
 * Side effects: clears one caller-owned error object.
 * Returns OK on success.
 */
static AdbxStatus fake_ss_delete(SecretStore *store, const SecretRefInfo *ref,
                                 SecretStoreErr *out_err) {
  (void)store;
  (void)ref;
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  return OK;
}

/* Accepts every fake secret-store namespace wipe.
 * It borrows all inputs and performs no allocations.
 * Side effects: clears one caller-owned error object.
 * Returns OK on success.
 */
static AdbxStatus fake_ss_wipe_namespace(SecretStore *store,
                                         const char *cred_namespace,
                                         SecretStoreErr *out_err) {
  (void)store;
  (void)cred_namespace;
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  return OK;
}

/* Accepts every fake secret-store global wipe.
 * It borrows all inputs and performs no allocations.
 * Side effects: clears one caller-owned error object.
 * Returns OK on success.
 */
static AdbxStatus fake_ss_wipe_all(SecretStore *store,
                                   SecretStoreErr *out_err) {
  (void)store;
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  return OK;
}

/* Frees one fake secret-store allocation.
 * It consumes 'store'.
 * Side effects: releases heap memory.
 * Error semantics: none.
 */
static void fake_ss_destroy(SecretStore *store) { free(store); }

static const SecretStoreVTable FAKE_SS_VT = {
    .get = fake_ss_get,
    .set = fake_ss_ok,
    .delete = fake_ss_delete,
    .wipe_namespace = fake_ss_wipe_namespace,
    .wipe_all = fake_ss_wipe_all,
    .destroy = fake_ss_destroy,
};

/* Allocates one fake secret store for broker unit tests.
 * It performs one heap allocation and returns the owned store.
 * Returns a caller-owned SecretStore on success, NULL on allocation failure.
 */
static SecretStore *fake_secret_store(void) {
  FakeSecretStore *s = xcalloc(1, sizeof(*s));
  if (!s)
    return NULL;
  s->base.vt = &FAKE_SS_VT;
  return (SecretStore *)s;
}

/* Creates one temporary app-dir path for broker tests.
 * It borrows 'out_tmpdir' and writes one owned parent tmpdir path into it.
 * Side effects: creates one temporary directory under /tmp.
 * Returns a caller-owned AppDir on success, NULL on invalid input or setup
 * failure.
 */
static AppDir *make_test_appdir(char **out_tmpdir) {
  if (!out_tmpdir)
    return NULL;
  *out_tmpdir = NULL;

  const char *tmpl = "/tmp/test_broker_run_XXXXXX";
  char *tmpdir = xmalloc(strlen(tmpl) + 1u);
  strcpy(tmpdir, tmpl);
  if (!mkdtemp(tmpdir)) {
    free(tmpdir);
    return NULL;
  }

  char *app_dir = path_join(tmpdir, APPDIR_APP_DIRNAME);
  if (!app_dir) {
    (void)rmdir(tmpdir);
    free(tmpdir);
    return NULL;
  }

  AppDir *appd = appdir_resolve(app_dir, NULL);
  free(app_dir);
  if (!appd) {
    (void)rmdir(tmpdir);
    free(tmpdir);
    return NULL;
  }

  *out_tmpdir = tmpdir;
  return appd;
}

/* Removes one temporary broker root directory when it still exists.
 * It borrows 'tmpdir' and allocates one temporary child path internally.
 * Side effects: removes broker test directories when present.
 * Error semantics: test helper; asserts paths are removed or already absent.
 */
static void cleanup_tmpdir_root(const char *tmpdir) {
  if (!tmpdir)
    return;

  char *app_dir = path_join(tmpdir, APPDIR_APP_DIRNAME);
  ASSERT_TRUE(app_dir != NULL);
  if (rmdir(app_dir) != 0)
    ASSERT_TRUE(errno == ENOENT);
  free(app_dir);

  if (rmdir(tmpdir) != 0)
    ASSERT_TRUE(errno == ENOENT);
}

/* Reads the broker shared secret token from one prepared private dir.
 * It borrows 'appd' and writes bytes into caller-owned 'out_secret'.
 * Side effects: reads the token file from disk.
 * Returns OK on success, ERR on invalid input or I/O failure.
 */
static AdbxStatus read_secret_token(const AppDir *appd,
                                    uint8_t out_secret[SECRET_TOKEN_LEN]) {
  if (!appd || !appd->token_path || !out_secret)
    return ERR;
  return fileio_read_exact(appd->token_path, SECRET_TOKEN_LEN, out_secret);
}

/* Connects one client socket to 'sock_path'.
 * It borrows 'sock_path' and returns one owned socket fd.
 * Side effects: creates and connects one Unix-domain socket.
 * Returns a non-negative fd on success, -1 on invalid input or connect
 * failure.
 */
static int connect_client(const char *sock_path) {
  if (!sock_path)
    return -1;

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

/* Sleeps for a short, bounded broker-test delay.
 * It borrows no dynamic memory and performs no allocations.
 * Side effects: blocks the current thread for approximately 'ms' milliseconds.
 * Error semantics: none.
 */
static void broker_test_msleep(int ms) {
  struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
  nanosleep(&ts, NULL);
}

/* Runs broker_run() in one background thread.
 * It borrows 'arg' as one Broker pointer.
 * Side effects: executes the broker event loop until cancellation.
 * Returns NULL on thread exit.
 */
static void *broker_run_thread(void *arg) {
  Broker *b = (Broker *)arg;
  broker_run(b);
  return NULL;
}

/* Performs one handshake request/response exchange on a connected fd.
 * It borrows 'cfd' and 'req' and writes the decoded response into 'out_resp'.
 * Side effects: sends and receives framed handshake bytes on the socket.
 * Returns OK on success, ERR on invalid input or transport/decode failure.
 */
static AdbxStatus client_handshake_req_on_fd(int cfd,
                                             const handshake_req_t *req,
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

  AdbxStatus rc = ERR;
  StrBuf payload;
  sb_init(&payload);
  if (frame_write_len(bc, req_wire, (uint32_t)sizeof(req_wire)) != OK)
    goto done;
  if (frame_read_len(bc, &payload, 0) != FRAME_READ_LEN_OK)
    goto done;
  if (handshake_resp_decode(out_resp, (const uint8_t *)payload.data,
                            payload.len) != OK)
    goto done;
  rc = OK;

done:
  sb_clean(&payload);
  bufch_destroy(bc);
  return rc;
}

/* Performs one fresh broker handshake on a connected client fd.
 * It borrows 'cfd' and 'secret_token' and optionally writes one resume token.
 * Side effects: performs framed handshake I/O.
 * Returns OK on success, ERR on invalid input or transport/decode failure.
 */
static AdbxStatus
client_handshake_on_fd(int cfd, const uint8_t secret_token[SECRET_TOKEN_LEN],
                       handshake_status *out_status,
                       uint8_t out_resume_token[RESUME_TOKEN_LEN]) {
  if (cfd < 0 || !secret_token || !out_status)
    return ERR;

  handshake_req_t req = {0};
  req.magic = HANDSHAKE_MAGIC;
  req.version = HANDSHAKE_VERSION;
  memcpy(req.secret_token, secret_token, SECRET_TOKEN_LEN);

  handshake_resp_t resp = {0};
  if (client_handshake_req_on_fd(cfd, &req, &resp) != OK)
    return ERR;

  *out_status = resp.status;
  if (out_resume_token)
    memcpy(out_resume_token, resp.resume_token, RESUME_TOKEN_LEN);
  return OK;
}

ConnManager *broker_test_make_empty_cm(void) {
  ConnCatalog *cat = xcalloc(1, sizeof(*cat));
  SecretStore *ss = fake_secret_store();
  if (!cat || !ss) {
    catalog_destroy(cat);
    secret_store_destroy(ss);
    return NULL;
  }
  return connm_create(cat, ss);
}

ConnManager *broker_test_make_cm_from_catalog(ConnCatalog *cat) {
  SecretStore *ss = NULL;
  ConnManager *cm = NULL;
  if (!cat)
    return NULL;

  ss = fake_secret_store();
  if (!ss) {
    catalog_destroy(cat);
    return NULL;
  }

  cm = connm_create(cat, ss);
  if (!cm) {
    catalog_destroy(cat);
    secret_store_destroy(ss);
    return NULL;
  }
  return cm;
}

AdbxStatus broker_test_start(BrokerRunTestCtx *ctx, ConnManager *cm) {
  char *tmpdir = NULL;
  AppDir *appd = NULL;
  Broker *b = NULL;
  int trc = 0;

  if (!ctx || !cm)
    return ERR;
  memset(ctx, 0, sizeof(*ctx));

  appd = make_test_appdir(&tmpdir);
  if (!appd) {
    connm_destroy(cm);
    return ERR;
  }

  b = broker_create(appd, cm, NULL);
  if (!b) {
    connm_destroy(cm);
    appdir_clean(appd);
    cleanup_tmpdir_root(tmpdir);
    free(tmpdir);
    return ERR;
  }

  if (read_secret_token(appd, ctx->secret) != OK) {
    broker_destroy(b);
    appdir_clean(appd);
    cleanup_tmpdir_root(tmpdir);
    free(tmpdir);
    return ERR;
  }

  trc = pthread_create(&ctx->tid, NULL, broker_run_thread, b);
  if (trc != 0) {
    broker_destroy(b);
    appdir_clean(appd);
    cleanup_tmpdir_root(tmpdir);
    free(tmpdir);
    memset(ctx, 0, sizeof(*ctx));
    return ERR;
  }

  broker_test_msleep(50);
  ctx->broker = b;
  ctx->app_dir = appd;
  ctx->tmpdir = tmpdir;
  return OK;
}

void broker_test_stop(BrokerRunTestCtx *ctx) {
  if (!ctx)
    return;

  if (ctx->broker) {
    pthread_cancel(ctx->tid);
    pthread_join(ctx->tid, NULL);
    broker_destroy(ctx->broker);
  }
  appdir_clean(ctx->app_dir);
  if (ctx->tmpdir) {
    cleanup_tmpdir_root(ctx->tmpdir);
    free(ctx->tmpdir);
  }
  memset(ctx, 0, sizeof(*ctx));
}

AdbxStatus broker_test_request_json(const BrokerRunTestCtx *ctx,
                                    const char *req_json,
                                    StrBuf *out_resp_json) {
  int cfd = -1;
  ByteChannel *ch = NULL;
  BufChannel *bc = NULL;
  handshake_status st = HS_ERR_INTERNAL;

  if (!ctx || !ctx->app_dir || !ctx->app_dir->sock_path || !req_json ||
      !out_resp_json)
    return ERR;

  cfd = connect_client(ctx->app_dir->sock_path);
  if (cfd < 0)
    goto err;
  if (client_handshake_on_fd(cfd, ctx->secret, &st, NULL) != OK || st != HS_OK)
    goto err;

  ch = stdio_bytechannel_wrap_fd(cfd, cfd);
  if (!ch)
    goto err;
  bc = bufch_create(ch);
  if (!bc) {
    bytech_destroy(ch);
    goto err;
  }

  sb_reset(out_resp_json);
  if (frame_write_len(bc, (const uint8_t *)req_json,
                      (uint32_t)strlen(req_json)) != OK)
    goto err;
  if (frame_read_len(bc, out_resp_json, 0) != FRAME_READ_LEN_OK)
    goto err;

  bufch_destroy(bc);
  close(cfd);
  return OK;

err:
  if (bc)
    bufch_destroy(bc);
  else if (ch)
    bytech_destroy(ch);
  if (cfd >= 0)
    close(cfd);
  if (out_resp_json)
    sb_reset(out_resp_json);
  return ERR;
}
