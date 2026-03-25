#ifndef TEST_BROKER_RUN_UTILS_H
#define TEST_BROKER_RUN_UTILS_H

#include <pthread.h>
#include <stdint.h>

#include "app_dir.h"
#include "broker.h"
#include "conn_catalog.h"
#include "conn_manager.h"
#include "handshake_codec.h"
#include "string_op.h"
#include "utils.h"

typedef struct BrokerRunTestCtx {
  Broker *broker;
  pthread_t tid;
  AppDir *app_dir;
  char *tmpdir;
  uint8_t secret[SECRET_TOKEN_LEN];
} BrokerRunTestCtx;

/* Creates one ConnManager backed by an empty catalog and a fake secret store.
 * It returns a caller-owned ConnManager on success, NULL on allocation/setup
 * failure.
 */
ConnManager *broker_test_make_empty_cm(void);

/* Creates one ConnManager backed by 'cat' and a fake secret store.
 * It consumes 'cat' regardless of success.
 * Returns a caller-owned ConnManager on success, NULL on allocation/setup
 * failure.
 */
ConnManager *broker_test_make_cm_from_catalog(ConnCatalog *cat);

/* Starts broker_run() in a background thread using one caller-supplied
 * ConnManager.
 * It borrows 'ctx', consumes 'cm' regardless of success, and fills 'ctx' on
 * success.
 * Side effects: creates temporary filesystem state, starts a background
 * thread, and initializes the shared-secret token inside 'ctx'.
 * Returns OK on success, ERR on invalid input or setup failure.
 */
AdbxStatus broker_test_start(BrokerRunTestCtx *ctx, ConnManager *cm);

/* Stops one broker started with broker_test_start() and releases all harness
 * resources stored in 'ctx'.
 * It borrows 'ctx' and resets it to zero state.
 * Side effects: cancels/joins the broker thread, destroys the broker, and
 * removes temporary filesystem state.
 * Error semantics: none; zero/empty contexts are ignored.
 */
void broker_test_stop(BrokerRunTestCtx *ctx);

/* Sends one JSON-RPC request to a freshly connected and successfully
 * handshaken broker client, then reads exactly one JSON response frame into
 * 'out_resp_json'.
 * It borrows 'ctx' and 'req_json'; caller owns initialized 'out_resp_json'.
 * Side effects: creates one transient client socket, performs handshake I/O,
 * writes one framed request, and reads one framed response.
 * Returns OK on success, ERR on invalid input, handshake failure, or transport
 * failure.
 */
AdbxStatus broker_test_request_json(const BrokerRunTestCtx *ctx,
                                    const char *req_json,
                                    StrBuf *out_resp_json);

#endif
