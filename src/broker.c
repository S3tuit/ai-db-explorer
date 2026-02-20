#define _GNU_SOURCE

#include "broker.h"
#include "frame_codec.h"
#include "handshake_codec.h"
#include "json_codec.h"
#include "log.h"
#include "packed_array.h"
#include "pl_arena.h"
#include "query_result.h"
#include "sensitive_tok.h"
#include "stdio_byte_channel.h"
#include "string_op.h"
#include "utils.h"
#include "validator.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

// Maximum size of a single request payload (bytes). Larger frames are rejected.
#define MAX_REQ_LEN (8u * 1024u * 1024u)
// Maximum number of token parameters accepted by run_sql_query_tokens.
#define MAX_TOKEN_PARAMS 10u
// Per-session storage cap for sensitive token values/keys.
#define SESSION_TOKEN_ARENA_CAP_BYTES (64u * 1024u * 1024u)

#ifdef ADBX_TEST_MODE
// Keep test timeout small but non-zero so timeout paths stay deterministic.
#define HANDSHAKE_READ_TIMEOUT_SEC 1
#define REQUEST_READ_TIMEOUT_SEC 1
#endif

#ifndef ADBX_TEST_MODE
#define HANDSHAKE_READ_TIMEOUT_SEC 3
#define REQUEST_READ_TIMEOUT_SEC 3
#endif

struct Broker {
  int listen_fd;   // file descriptor of the socket used to
                   // accept incoming connection requets
  ConnManager *cm; // owned
  char *sock_path; // owned, socket path for the broker

  uint8_t secret_token[SECRET_TOKEN_LEN];
  uint8_t has_secret_token; // 1 if secret_token is set, 0 if NULL was passed

  PackedArray *active_sessions; // polled for I/O, max MAX_CLIENTS
  PackedArray *idle_sessions;   // not polled, max MAX_IDLE_SESSIONS

  uint32_t idle_ttl_secs;
  uint32_t abs_ttl_secs;
};

/* This entity stores the usefull data to communicate with each Client. */
typedef struct BrokerMcpSession {
  BufChannel bc;
  int fd; // connection identity (owned by bc). -1 if disconnected but resumable
  uint8_t resume_token[RESUME_TOKEN_LEN]; // can be used to resume the session
  uint32_t generation;                    // session-wide token generation
  PlArena arena;          // stores per-session token value bytes
  PackedArray *db_stores; // owned array of DbTokenStore*
  time_t created_at;      // for absolute TTL
  time_t last_active;     // for idle TTL
} BrokerMcpSession;

/* Resolves a positive TTL override from environment in test builds.
 * It borrows 'name' and does not allocate memory.
 * Side effects: reads environment variables.
 * Returns fallback_ttl on parse/validation failure, otherwise parsed value.
 */
static uint32_t broker_ttl_from_env_or_default(const char *name,
                                               uint32_t fallback_ttl) {
#ifndef ADBX_TEST_MODE
  (void)name;
  return fallback_ttl;
#else
  if (!name || fallback_ttl == 0)
    return fallback_ttl;

  const char *raw = getenv(name);
  if (!raw || raw[0] == '\0')
    return fallback_ttl;

  char *end = NULL;
  errno = 0;
  unsigned long long parsed = strtoull(raw, &end, 10);
  if (errno != 0 || end == raw || *end != '\0')
    return fallback_ttl;
  if (parsed == 0 || parsed > UINT32_MAX)
    return fallback_ttl;
  return (uint32_t)parsed;
#endif
}

/* Closes '*fd' when it is valid and sets it to -1.
 * It borrows 'fd' and does not allocate memory.
 * Side effects: closes a kernel file descriptor.
 * Error semantics: none (best-effort close; this helper returns void).
 */
static inline void safe_close_fd(int *fd) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
}

/* Compares the arrays 'a' and 'b' of 'len' size in constant-time (to avoid
 * timing attacks). It borrows 'a' and 'b'.
 * Side effects: none.
 * Returns YES when arrays are equal, NO when different, ERR on invalid input.
 */
static int bytes_equal_ct(const uint8_t *a, const uint8_t *b, size_t len) {
  if (!a || !b)
    return ERR;

  // XOR/OR accumulation avoids early-exit timing leaks on first mismatch.
  uint8_t diff = 0;
  for (size_t i = 0; i < len; i++) {
    diff |= (uint8_t)(a[i] ^ b[i]);
  }
  return (diff == 0) ? YES : NO;
}

/* Writes one handshake response frame to a connected session. 'resume_token'
 * may be NULL.
 * Side effects: writes one length-prefixed frame to sess->bc.
 * Returns OK on successful write, ERR on invalid input or I/O failure.
 */
static int broker_write_handshake_resp(BrokerMcpSession *sess,
                                       handshake_status status,
                                       const uint8_t *resume_token,
                                       uint32_t idle_ttl_secs,
                                       uint32_t abs_ttl_secs) {
  if (!sess || idle_ttl_secs == 0 || abs_ttl_secs == 0)
    return ERR;

  handshake_resp_t resp = {0};
  resp.magic = HANDSHAKE_MAGIC;
  resp.version = HANDSHAKE_VERSION;
  resp.status = status;
  resp.idle_ttl_secs = idle_ttl_secs;
  resp.abs_ttl_secs = abs_ttl_secs;

  if (resume_token) {
    memcpy(resp.resume_token, resume_token, RESUME_TOKEN_LEN);
  }

  uint8_t wire[HANDSHAKE_RESP_WIRE_SIZE];
  if (handshake_resp_encode(&resp, wire) != OK)
    return ERR;
  return frame_write_len(&sess->bc, wire, (uint32_t)sizeof(wire));
}

/* Reads one framed handshake request and decodes it into 'out_req'.
 * It borrows 'sess' and 'out_req'; temporary frame storage is freed before
 * return.
 * Side effects: reads one frame from sess->bc and consumes it.
 * Returns OK on exact-size, well-formed handshake payload; ERR on framing
 * failure, timeout, malformed size, or invalid input.
 */
static int broker_read_handshake_req(BrokerMcpSession *sess,
                                     handshake_req_t *out_req) {
  if (!sess || !out_req)
    return ERR;

  StrBuf payload;
  sb_init(&payload);
  int rc = frame_read_len(&sess->bc, &payload);
  if (rc != OK) {
    sb_clean(&payload);
    return ERR;
  }

  if (handshake_req_decode(out_req, (const uint8_t *)payload.data,
                           payload.len) != OK) {
    sb_clean(&payload);
    return ERR;
  }
  sb_clean(&payload);
  return OK;
}

/* Sets the receive timeout on a socket file descriptor.
 * It borrows 'fd'; ownership is unchanged.
 * Side effects: updates SO_RCVTIMEO kernel state for the socket.
 * Returns OK on success, ERR on invalid input or setsockopt failure.
 */
static int broker_set_rcv_timeout_sec(int fd, int sec) {
  if (fd < 0 || sec < 0)
    return ERR;

  struct timeval tv = {.tv_sec = sec, .tv_usec = 0};
  if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0)
    return ERR;
  return OK;
}

/* Checks whether 'sess' exceeded idle or absolute TTL policy.
 * It borrows 'sess'; no allocation or ownership transfer.
 * Side effects: none.
 * Returns YES when expired, NO when still valid, ERR on invalid input.
 */
static int broker_session_is_expired(const BrokerMcpSession *sess, time_t now,
                                     uint32_t idle_ttl_secs,
                                     uint32_t abs_ttl_secs) {
  if (!sess || now < 0 || sess->created_at <= 0 || sess->last_active <= 0)
    return ERR;
  if (idle_ttl_secs == 0 || abs_ttl_secs == 0)
    return ERR;

  // Fail closed on backwards clock jumps.
  if (now < sess->created_at || now < sess->last_active)
    return YES;

  if ((now - sess->created_at) > (time_t)abs_ttl_secs)
    return YES;
  if ((now - sess->last_active) > (time_t)idle_ttl_secs)
    return YES;
  return NO;
}

/* Reaps one idle session, preferring lower indexes as an "older first"
 * heuristic.
 * It borrows 'idle'; removed objects are owned/freed by PackedArray cleanup.
 * Side effects: may remove one idle session from PackedArray.
 * Returns OK when one idle session is removed, ERR otherwise.
 */
static int broker_reap_one_idle_session(PackedArray *idle) {
  if (!idle)
    return ERR;

  size_t n = parr_len(idle);
  for (size_t i = 0; i < n; i++) {
    BrokerMcpSession *sess = (BrokerMcpSession *)parr_at(idle, (uint32_t)i);
    if (!sess)
      continue;

    // Idle sessions are expected to have no live fd.
    if (sess->fd < 0) {
      parr_drop_swap(idle, (uint32_t)i);
      return OK;
    }
  }
  return ERR;
}

/* Finds the first idle session whose resume token matches 'token'. Candidate
 * comparisons are constant-time to reduce timing side channels.
 * It borrows 'idle' and 'token'; no allocations.
 * Returns matching index [0..len) when found, -1 otherwise.
 */
static ssize_t broker_find_idle_by_token(const PackedArray *idle,
                                         const uint8_t *token) {
  if (!idle || !token)
    return -1;

  size_t n = parr_len(idle);
  for (size_t i = 0; i < n; i++) {
    const BrokerMcpSession *sess =
        (const BrokerMcpSession *)parr_cat(idle, (uint32_t)i);
    if (!sess)
      continue;
    int eq = bytes_equal_ct(token, sess->resume_token, RESUME_TOKEN_LEN);
    if (eq == YES) {
      return (ssize_t)i;
    }
  }

  return -1;
}

/* --------------------------------- Sessions ------------------------------ */

/* Releases BrokerMcpSession-owned dynamic members (excluding fd/channel).
 */
static void session_owned_clean(BrokerMcpSession *s) {
  if (!s)
    return;
  parr_destroy(s->db_stores);
  s->db_stores = NULL;
  pl_arena_clean(&s->arena);
  s->arena = (PlArena){0};
}

/* Cleanup callback for active session slots.
 * It borrows 'obj' as a PackedArray-owned slot and does not allocate.
 * Side effects: closes/destroys the live buffered channel and clears fd.
 * Error semantics: none (void cleanup callback).
 */
static void active_sessions_cleanup(void *obj, void *ctx) {
  (void)ctx;
  BrokerMcpSession *s = (BrokerMcpSession *)obj;
  if (!s)
    return;
  bufch_clean(&s->bc);
  s->fd = -1;
  session_owned_clean(s);
}

/* Cleanup callback for idle session slots.
 * It borrows 'obj'/'ctx' and does not allocate.
 * Side effects: none (idle slots do not own a live channel at cleanup time).
 * Error semantics: none (void cleanup callback).
 */
static void idle_sessions_cleanup(void *obj, void *ctx) {
  (void)ctx;
  BrokerMcpSession *s = (BrokerMcpSession *)obj;
  session_owned_clean(s);
}

/* Moves ownership of a pending live session into the active session array.
 * It transfers pending->bc and pending->fd to "active"-owned storage on
 * success; pending keeps ownership on failure.
 * Side effects: mutates 'active' and session metadata.
 * Returns inserted index on success, UINT32_MAX on failure.
 */
static uint32_t sessions_add_from_pending(PackedArray *active,
                                          BrokerMcpSession *pending,
                                          BrokerMcpSession **out_sess) {
  if (!active || !pending)
    return UINT32_MAX;
  if (out_sess)
    *out_sess = NULL;

  BrokerMcpSession *dst = NULL;
  uint32_t idx = parr_emplace(active, (void **)&dst);
  if (idx == UINT32_MAX || !dst)
    return UINT32_MAX;

  memset(dst, 0, sizeof(*dst));
  dst->bc = pending->bc;
  dst->fd = pending->fd;
  dst->arena = pending->arena;
  dst->db_stores = pending->db_stores;

  // pending no longer owns channel/fd after transfer.
  pending->fd = -1;
  memset(&pending->bc, 0, sizeof(pending->bc));
  pending->arena = (PlArena){0};
  pending->db_stores = NULL;

  if (out_sess)
    *out_sess = dst;
  return idx;
}

/* Moves one active session to idle-session storage.
 * It borrows 'active'/'idle'; session ownership stays in PackedArray
 * containers and no heap allocation escapes.
 * Side effects: tears down live I/O channel, mutates both arrays, may reap one
 * existing idle session, and refreshes last_active timestamp.
 */
static void session_move_to_idle(PackedArray *active, PackedArray *idle,
                                 uint32_t active_idx) {
  BrokerMcpSession *src = parr_at(active, active_idx);
  if (!src)
    return;

  /* Save fields we need before the active slot is cleaned/overwritten. */
  uint8_t token[RESUME_TOKEN_LEN];
  memcpy(token, src->resume_token, RESUME_TOKEN_LEN);
  uint32_t generation = src->generation;
  PlArena arena = src->arena;
  PackedArray *db_stores = src->db_stores;
  time_t created = src->created_at;

  /* Tear down the live connection. */
  bufch_clean(&src->bc);
  src->fd = -1;
  src->arena = (PlArena){0};
  src->db_stores = NULL;

  /* Remove from active (cleanup callback is safe — bufch_clean is
   * idempotent). */
  parr_drop_swap(active, active_idx);

  // Evict one idle session if at capacity
  if (parr_len(idle) >= MAX_IDLE_SESSIONS &&
      broker_reap_one_idle_session(idle) != OK) {
    return;
  }

  /* Insert into idle. */
  BrokerMcpSession *dst = NULL;
  uint32_t idx = parr_emplace(idle, (void **)&dst);
  if (idx == UINT32_MAX || !dst)
    return;

  memset(dst, 0, sizeof(*dst));
  dst->fd = -1;
  memcpy(dst->resume_token, token, RESUME_TOKEN_LEN);
  dst->generation = generation;
  dst->arena = arena;
  dst->db_stores = db_stores;
  dst->created_at = created;
  dst->last_active = time(NULL);
}

/* --------------------------------- Broker ------------------------------- */

/* Creates/binds/listens a Unix socket at 'path' for broker clients.
 * It borrows 'path' and does not allocate long-lived memory.
 * Side effects: unlinks old path, creates socket inode with mode 0600, and
 * opens a listening fd.
 * Returns listening fd on success, -1 on failure.
 */
static int make_listen_socket(const char *path) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  // Remove old socket file if it exists
  unlink(path);

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;

  // the path must fit inside .sun_path
  if (strlen(path) >= sizeof(addr.sun_path)) {
    safe_close_fd(&fd);
    return -1;
  }
  strcpy(addr.sun_path, path);

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    safe_close_fd(&fd);
    return -1;
  }
  if (chmod(path, 0600) != 0) {
    safe_close_fd(&fd);
    return -1;
  }
  if (listen(fd, 16) < 0) {
    safe_close_fd(&fd);
    return -1;
  }
  return fd;
}

/* Creates and initializes a Broker instance.
 * Takes ownership of 'cm'. Borrows and copies 'sock_path' and optional
 * 'secret_token'.
 * Side effects: allocates Broker/session arrays and creates the listen socket.
 * Returns a valid Broker* on success, NULL on any initialization failure.
 */
Broker *broker_create(const char *sock_path, ConnManager *cm,
                      const uint8_t *secret_token) {
  if (!cm || !sock_path)
    return NULL;
#ifndef ADBX_TEST_MODE
  if (!secret_token)
    return NULL;
#endif

  Broker *b = (Broker *)xcalloc(1, sizeof(Broker));

  b->listen_fd = -1;
  b->cm = cm;

  if (secret_token) {
    memcpy(b->secret_token, secret_token, SECRET_TOKEN_LEN);
    b->has_secret_token = 1;
  }

  b->idle_ttl_secs = broker_ttl_from_env_or_default("ADBX_TEST_IDLE_TTL_SEC",
                                                    (uint32_t)IDLE_TTL);
  b->abs_ttl_secs = broker_ttl_from_env_or_default("ADBX_TEST_ABS_TTL_SEC",
                                                   (uint32_t)ABSOLUTE_TTL);

  b->active_sessions = parr_create(sizeof(BrokerMcpSession));
  parr_set_cleanup(b->active_sessions, active_sessions_cleanup, NULL);

  b->idle_sessions = parr_create(sizeof(BrokerMcpSession));
  parr_set_cleanup(b->idle_sessions, idle_sessions_cleanup, NULL);

  b->sock_path = dup_or_null(sock_path);
  if (!b->sock_path) {
    broker_destroy(b);
    return NULL;
  }

  // Listening socket
  b->listen_fd = make_listen_socket(b->sock_path);
  if (b->listen_fd < 0) {
    broker_destroy(b);
    return NULL;
  }

  return b;
}

/* Destroys a Broker and all owned resources.
 * Owns and frees: session arrays, listen fd, socket path copy, and ConnManager.
 * Side effects: closes file descriptors and unlinks broker socket path.
 * Error semantics: none (void destructor; safe on NULL).
 */
void broker_destroy(Broker *b) {
  if (!b)
    return;

  parr_destroy(b->active_sessions);
  b->active_sessions = NULL;

  parr_destroy(b->idle_sessions);
  b->idle_sessions = NULL;

  if (b->listen_fd >= 0) {
    safe_close_fd(&b->listen_fd);
    b->listen_fd = -1;
  }

  if (b->sock_path)
    (void)unlink(b->sock_path);
  free(b->sock_path);
  b->sock_path = NULL;

  connm_destroy(b->cm);
  free(b);
}

/*-------------------------------- Tools Call -------------------------------*/

/* Cleans one packed-array slot that stores a DbTokenStore.
 * It borrows 'obj' and does not allocate memory.
 * Side effects: releases token-store owned arrays/hash/pools.
 * Error semantics: none (void cleanup callback).
 */
static void broker_db_store_cleanup(void *obj, void *ctx) {
  (void)ctx;
  DbTokenStore **slot = (DbTokenStore **)obj;
  if (!slot)
    return;
  stok_store_destroy(*slot);
  *slot = NULL;
}

/* Finds one DbTokenStore by exact connection name.
 * It borrows 'stores' and 'connection_name'; no allocations.
 * Side effects: none.
 * Error semantics: returns mutable store pointer on match, NULL otherwise.
 */
static DbTokenStore *broker_find_store(PackedArray *stores,
                                       const char *connection_name) {
  if (!stores || !connection_name)
    return NULL;

  size_t n = parr_len(stores);
  for (size_t i = 0; i < n; i++) {
    DbTokenStore **slot = (DbTokenStore **)parr_at(stores, (uint32_t)i);
    if (!slot || !*slot)
      continue;
    int eq = stok_store_matches_conn_name(*slot, connection_name);
    if (eq == YES)
      return *slot;
  }
  return NULL;
}

/* Resolves per-session store for the selected connection, lazily creating it.
 * It borrows 'sess' and 'profile' and returns a borrowed pointer in
 * '*out_store'.
 * Side effects: may append one DbTokenStore to an already-initialized session.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static int broker_get_or_init_store(BrokerMcpSession *sess,
                                    const ConnProfile *profile,
                                    DbTokenStore **out_store) {
  if (out_store)
    *out_store = NULL;
  if (!sess || !profile || !profile->connection_name || !out_store)
    return ERR;
  if (!sess->db_stores || sess->arena.head == NULL || sess->arena.tail == NULL)
    return ERR;

  assert(sess->arena.cap > 0);
  assert(sess->arena.block_sz > 0);

  DbTokenStore *found =
      broker_find_store(sess->db_stores, profile->connection_name);
  if (found) {
    *out_store = found;
    return OK;
  }

  DbTokenStore **slot = NULL;
  uint32_t idx = parr_emplace(sess->db_stores, (void **)&slot);
  if (idx == UINT32_MAX || !slot)
    return ERR;
  *slot = NULL;

  DbTokenStore *store = stok_store_create(profile, &sess->arena);
  if (!store) {
    parr_drop_swap(sess->db_stores, idx);
    return ERR;
  }

  *slot = store;
  *out_store = store;
  return OK;
}

/* Borrowed context used by run_sql tool handlers.
 * It bundles request-scoped entities so handlers keep narrow signatures.
 */
typedef struct BrokerRunSQLArgs {
  Broker *b;
  BrokerMcpSession *sess;
  JsonGetter *jg;
  McpId *id;
} BrokerRunSQLArgs;

/* Executes validated run_sql_query arguments and builds a QueryResult.
 * It borrows 'args' and allocates temporary strings and may allocate
 * '*out_query'; caller owns/destroys '*out_query'. Side effects: acquires/uses
 * DB connection through ConnManager and records connection usage on success.
 * Error semantics: this helper is fail-soft and returns void; it aims to set
 * '*out_query' to an error/result object and leaves it NULL only on
 * catastrophic allocation/copy failures.
 */
static void broker_run_sql_query(const BrokerRunSQLArgs *args,
                                 QueryResult **out_query) {
  assert(args != NULL);
  assert(args->b != NULL);
  assert(args->sess != NULL);
  assert(args->jg != NULL);
  assert(args->id != NULL);
  assert(out_query != NULL);

  Broker *b = args->b;
  BrokerMcpSession *sess = args->sess;
  JsonGetter *jg = args->jg;
  McpId *id = args->id;

  char *conn_name = NULL;
  char *query = NULL;
  if (jsget_string_decode_alloc(jg, "params.arguments.connectionName",
                                &conn_name) != YES ||
      jsget_string_decode_alloc(jg, "params.arguments.query", &query) != YES) {
    free(conn_name);
    free(query);
    *out_query = qr_create_err(id, QRERR_INPARAM, "Invalid tool arguments.");
    goto free_n_return;
  }

  TLOG("INFO - preparing for running %s", query);
  ConnView cv = {0};
  int rc = connm_get_connection(b->cm, conn_name, &cv);
  if (rc != YES || !cv.db || !cv.profile) {
    TLOG("ERROR - unable to connect to %s", conn_name);
    *out_query = qr_create_err(id, QRERR_RESOURCE,
                               "Unable to connect to the requested database.");
    goto free_n_return;
  }

  DbTokenStore *store = NULL;
  if (broker_get_or_init_store(sess, cv.profile, &store) != OK || !store) {
    TLOG("ERROR - failed to initialize session token store for %s", conn_name);
    *out_query = qr_create_tool_err(
        id, "Internal error while preparing sensitive token storage.");
    goto free_n_return;
  }

  ValidateQueryOut vout = {0};
  if (vq_out_init(&vout) != OK) {
    *out_query = qr_create_tool_err(
        id, "Internal error while preparing query validation.");
    goto free_n_return;
  }
  ValidatorRequest vreq = {.db = cv.db, .profile = cv.profile, .sql = query};
  if (validate_query(&vreq, &vout) != OK) {
    const char *err_desc = sb_to_cstr(&vout.err.msg);
    if (err_desc[0] == '\0') {
      err_desc = "Unknown error while validating the query. Please make sure "
                 "the query is valid and formatted correctly.";
    }
    *out_query = qr_create_tool_err(id, err_desc);
    vq_out_clean(&vout);
    goto free_n_return;
  }

  if (db_exec(cv.db, query, out_query) != OK) {
    vq_out_clean(&vout);
    TLOG("ERROR - error while communicating with %s", conn_name);
    *out_query = qr_create_tool_err(
        id, "Something went wrong while communicating with the database.");
    goto free_n_return;
  }
  // db_exec leaves the id zeroed; stamp it with the request id
  if (mcp_id_copy(&(*out_query)->id, id) != OK) {
    qr_destroy(*out_query);
    vq_out_clean(&vout);
    *out_query = NULL;
    goto free_n_return;
  }
  connm_mark_used(b->cm, conn_name);
  vq_out_clean(&vout);

free_n_return:
  free(conn_name);
  free(query);
}

/* Handles the 'run_sql_query_tokens' tool call and produces a QueryResult at
 * 'out_query'. It borrows 'args' and allocates temporary decoded token strings
 * that are always freed before return. Side effects: parses hostile JSON token
 * parameters and verifies session-bound token metadata (connection name +
 * generation). Error semantics: fail-soft helper; writes '*out_query' with a
 * protocol/tool error on malformed input and leaves '*out_query' NULL only on
 * catastrophic allocation failures.
 */
static void broker_run_sql_query_tokens(const BrokerRunSQLArgs *args,
                                        QueryResult **out_query) {
  assert(args != NULL);
  assert(args->b != NULL);
  assert(args->sess != NULL);
  assert(args->jg != NULL);
  assert(args->id != NULL);
  assert(out_query != NULL);

  Broker *b = args->b;
  BrokerMcpSession *sess = args->sess;
  JsonGetter *jg = args->jg;
  McpId *id = args->id;

  char *conn_name = NULL;
  char *query = NULL;
  if (jsget_string_decode_alloc(jg, "params.arguments.connectionName",
                                &conn_name) != YES ||
      jsget_string_decode_alloc(jg, "params.arguments.query", &query) != YES) {
    free(conn_name);
    free(query);
    *out_query = qr_create_err(id, QRERR_INPARAM, "Invalid tool arguments.");
    return;
  }

  ConnView cv = {0};
  int rc = connm_get_connection(b->cm, conn_name, &cv);
  if (rc != YES || !cv.db || !cv.profile) {
    *out_query = qr_create_err(id, QRERR_RESOURCE,
                               "Unable to connect to the requested database.");
    goto free_n_return;
  }

  DbTokenStore *store = NULL;
  if (broker_get_or_init_store(sess, cv.profile, &store) != OK || !store) {
    *out_query = qr_create_tool_err(
        id, "Internal error while preparing sensitive token storage.");
    goto free_n_return;
  }

  JsonArrIter it = {0};
  int arc = jsget_array_strings_begin(jg, "params.arguments.parameters", &it);
  if (arc != YES) {
    *out_query = qr_create_err(id, QRERR_INPARAM,
                               "Missing arguments.parameters array..");
    goto free_n_return;
  }
  if (it.count <= 0 || (uint32_t)it.count > MAX_TOKEN_PARAMS) {
    *out_query = qr_create_err(id, QRERR_INPARAM,
                               "Token parameters must contain 1..10 entries.");
    goto free_n_return;
  }

  JsonStrSpan sp = {0};
  for (;;) {
    int nrc = jsget_array_strings_next(jg, &it, &sp);
    if (nrc == NO)
      break;
    if (nrc != YES) {
      *out_query =
          qr_create_err(id, QRERR_INPARAM, "Invalid token parameter entry.");
      goto free_n_return;
    }

    char *tok = NULL;
    if (json_span_decode_alloc(&sp, &tok) != OK || !tok) {
      free(tok);
      *out_query =
          qr_create_err(id, QRERR_INPARAM, "Invalid token parameter entry.");
      goto free_n_return;
    }

    ParsedTokView parsed = {0};
    if (stok_parse_view_inplace(tok, &parsed) != OK) {
      free(tok);
      // TODO: log the wrong formatted token
      *out_query = qr_create_err(id, QRERR_INPARAM,
                                 "Invalid token format. Expected "
                                 "tok_<connection>_<generation>_<index>.");
      goto free_n_return;
    }

    if (strcmp(parsed.connection_name, conn_name) != 0) {
      free(tok);
      *out_query =
          qr_create_err(id, QRERR_INPARAM, "Token connection mismatch.");
      goto free_n_return;
    }
    if (parsed.generation != sess->generation) {
      free(tok);
      *out_query = qr_create_tool_err(
          id, "Stale token generation. Please run a fresh sensitive query.");
      goto free_n_return;
    }

    free(tok);
  }

  // Bound execution path will be added with validator plan + DB typed binds.
  *out_query = qr_create_tool_err(
      id, "run_sql_query_tokens is recognized, but bound execution is not "
          "implemented yet.");

free_n_return:
  free(conn_name);
  free(query);
}

/* Handles one framed broker request and produces one QueryResult.
 * It borrows 'b', 'sess', and request bytes. It may allocate '*out_res'; caller
 * owns/destroys '*out_res' on success.
 * Side effects: parses untrusted JSON, dispatches tool logic, and may touch DB
 * through tool handlers.
 * Returns OK when '*out_res' is populated with either success or tool/error
 * payload; returns ERR only for catastrophic parse/allocation/internal
 * failures.
 */
static int broker_handle_request(Broker *b, BrokerMcpSession *sess,
                                 const char *req, uint32_t req_len,
                                 QueryResult **out_res) {
  if (!b || !sess || !req || !out_res)
    return ERR;
  TLOG("INFO - handling a request of %u bytes", req_len);
  *out_res = NULL;

  McpId id = {0};
  JsonGetter jg;
  if (jsget_init(&jg, req, req_len) != OK)
    return ERR;
  int has_u32 = jsget_u32(&jg, "id", &id.u32);
  if (has_u32 == YES) {
    id.kind = MCP_ID_INT;
  } else {
    char *id_str = NULL;
    int s_rc = jsget_string_decode_alloc(&jg, "id", &id_str);
    if (s_rc == YES) {
      id.kind = MCP_ID_STR;
      id.str = id_str;
    } else {
      mcp_id_clean(&id);
      return ERR;
    }
  }

  int vrc = jsget_simple_rpc_validation(&jg);
  if (vrc != YES) {
    *out_res = qr_create_err(&id, QRERR_INREQ, "Invalid JSON-RPC request.");
    goto return_res;
  }
  // exec command
  JsonStrSpan method_sp = {0};
  if (jsget_string_span(&jg, "method", &method_sp) != YES) {
    *out_res =
        qr_create_err(&id, QRERR_INREQ, "Can't find the 'method' object.");
    goto return_res;
  }

  if (!STREQ(method_sp.ptr, method_sp.len, "tools/call")) {
    *out_res = qr_create_err(&id, QRERR_INMETHOD, "Tool not supported.");
    goto return_res;
  }

  JsonStrSpan name_sp = {0};
  if (jsget_string_span(&jg, "params.name", &name_sp) != YES) {
    *out_res =
        qr_create_err(&id, QRERR_INPARAM, "Tool call missing params.name.");
    goto return_res;
  }

  BrokerRunSQLArgs run_args = {
      .b = b,
      .sess = sess,
      .jg = &jg,
      .id = &id,
  };

  // Call the different tools
  if (STREQ(name_sp.ptr, name_sp.len, "run_sql_query")) {
    broker_run_sql_query(&run_args, out_res);
  } else if (STREQ(name_sp.ptr, name_sp.len, "run_sql_query_tokens")) {
    broker_run_sql_query_tokens(&run_args, out_res);

    // Unknown tools
  } else {
    *out_res = qr_create_err(&id, QRERR_INMETHOD, "Unknown tool.");
  }

return_res:
  // catastrophic
  if (!*out_res) {
    mcp_id_clean(&id);
    return ERR;
  }

  mcp_id_clean(&id);
  return OK;
}

/*------------------------------ Session Handling ---------------------------*/

/* Verifies that the connected Unix-socket peer has the current process UID.
 * It borrows 'cfd' and does not allocate memory.
 * Side effects: reads peer credential metadata via getsockopt/getpeereid.
 * Returns OK on UID match, ERR on mismatch or syscall failure.
 */
static int verify_peer_uid(int cfd) {
  uid_t expected_uid = getuid();
#ifdef __linux__
  struct ucred cred;
  socklen_t len = sizeof(cred);
  if (getsockopt(cfd, SOL_SOCKET, SO_PEERCRED, &cred, &len) < 0)
    return ERR;
  return (cred.uid == expected_uid) ? OK : ERR;
#else /* macOS / BSDs */
  uid_t euid;
  gid_t egid;
  if (getpeereid(cfd, &euid, &egid) < 0)
    return ERR;
  return (euid == expected_uid) ? OK : ERR;
#endif
}

/* Initializes token-state containers for one broker session.
 * It borrows and mutates 'sess'; ownership of created members stays in the
 * session and is later released by session cleanup callbacks.
 * Side effects: allocate/init the per-session PlArena and db_stores array.
 * Error semantics: returns OK when state is ready, ERR on invalid input,
 * inconsistent partial state, or allocation failure.
 */
static int broker_session_token_state_init(BrokerMcpSession *sess) {
  if (!sess)
    return ERR;

  // session should be empty/zeroed during initialization
  assert(pl_arena_is_zeroed(&sess->arena) == YES);
  assert(!sess->db_stores);

  uint32_t cap = SESSION_TOKEN_ARENA_CAP_BYTES;
  if (pl_arena_init(&sess->arena, NULL, &cap) != OK)
    return ERR;

  sess->db_stores = parr_create(sizeof(DbTokenStore *));
  if (!sess->db_stores) {
    pl_arena_clean(&sess->arena);
    return ERR;
  }
  parr_set_cleanup(sess->db_stores, broker_db_store_cleanup, NULL);

  return OK;
}

/* Checks whether 'sess' has correctly initialized the token-store containers
 * and is ready to store/retrieve tokens. */
inline static int broker_session_token_state_ok(BrokerMcpSession *sess) {
  if (!sess)
    return ERR;
  if (!sess->db_stores || pl_arena_is_ok(&sess->arena) != YES)
    return NO;
  return YES;
}

/* Performs broker-side handshake.
 * Takes ownership of 'cfd'. On success ownership moves into active_sessions;
 * on failure the fd/channel are closed before return.
 * Side effects: reads/writes handshake frames, mutates active/idle session
 * arrays, may reap idle sessions, and updates per-session timestamps/tokens.
 * Returns OK only when handshake succeeds and session is active; ERR otherwise.
 */
static int broker_do_handshake(Broker *b, int cfd) {
  if (!b || cfd < 0) {
    safe_close_fd(&cfd);
    return ERR;
  }

  if (verify_peer_uid(cfd) != OK) {
    TLOG("INFO - rejected client fd=%d: peer UID mismatch", cfd);
    safe_close_fd(&cfd);
    return ERR;
  }

  handshake_status status = HS_ERR_INTERNAL;
  uint8_t out_token[RESUME_TOKEN_LEN] = {0};
  BrokerMcpSession pending = {0};
  BrokerMcpSession *active_sess = NULL;
  uint32_t active_idx = UINT32_MAX;

  // The BufChannel owns the ByteChannel, which owns the fd.
  if (bufch_stdio_openfd_init(&pending.bc, cfd, cfd) != OK) {
    return ERR;
  }
  pending.fd = cfd;
  // BufChannel takes ownership of the file descriptor and is responsible for
  // closing it
  cfd = -1;

  if (broker_set_rcv_timeout_sec(pending.fd, HANDSHAKE_READ_TIMEOUT_SEC) !=
      OK) {
    status = HS_ERR_INTERNAL;
    goto send_n_close;
  }

  handshake_req_t req = {0};
  if (broker_read_handshake_req(&pending, &req) != OK) {
    status = HS_ERR_REQ;
    goto send_n_close;
  }

  if (req.magic != HANDSHAKE_MAGIC) {
    status = HS_ERR_BAD_MAGIC;
    goto send_n_close;
  }
  if (req.version != HANDSHAKE_VERSION) {
    status = HS_ERR_BAD_VERSION;
    goto send_n_close;
  }

  int secret_rc =
      bytes_equal_ct(req.secret_token, b->secret_token, SECRET_TOKEN_LEN);
  if (secret_rc != YES) {
    status = HS_ERR_TOKEN_UNKNOWN;
    goto send_n_close;
  }

  time_t now = time(NULL);
  if (now == (time_t)-1) {
    status = HS_ERR_INTERNAL;
    goto send_n_close;
  }

  // wants to resume a sessions
  if (req.flags & HANDSHAKE_FLAG_RESUME) {
    ssize_t idle_idx =
        broker_find_idle_by_token(b->idle_sessions, req.resume_token);
    if (idle_idx < 0) {
      status = HS_ERR_TOKEN_UNKNOWN;
      goto send_n_close;
    }

    BrokerMcpSession *idle_sess =
        (BrokerMcpSession *)parr_at(b->idle_sessions, (uint32_t)idle_idx);
    if (!idle_sess) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }

    time_t resume_created_at = idle_sess->created_at;
    int exp = broker_session_is_expired(idle_sess, now, b->idle_ttl_secs,
                                        b->abs_ttl_secs);
    if (exp == YES) {
      parr_drop_swap(b->idle_sessions, (uint32_t)idle_idx);
      status = HS_ERR_TOKEN_EXPIRED;
      goto send_n_close;
    }
    if (exp != NO) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }

    if (MAX_CLIENTS > 0 && parr_len(b->active_sessions) >= MAX_CLIENTS) {
      status = HS_ERR_FULL;
      goto send_n_close;
    }

    // Normalize token-state before ownership transfer to keep idle slot valid
    // if any later step fails.
    assert(broker_session_token_state_ok(idle_sess) == YES);
    if (broker_session_token_state_ok(idle_sess) != YES) {
      status = HS_ERR_INTERNAL;
      parr_drop_swap(b->idle_sessions, (uint32_t)idle_idx);
      TLOG("ERROR - found one half-initialized idle session.");
      goto send_n_close;
    }

    // we rotate new token on successful resume
    if (fill_random(out_token, RESUME_TOKEN_LEN) != OK) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }

    active_idx =
        sessions_add_from_pending(b->active_sessions, &pending, &active_sess);
    if (active_idx == UINT32_MAX || !active_sess) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }
    memcpy(active_sess->resume_token, out_token, RESUME_TOKEN_LEN);
    active_sess->generation = idle_sess->generation;
    active_sess->arena = idle_sess->arena;
    active_sess->db_stores = idle_sess->db_stores;
    active_sess->created_at = resume_created_at;
    active_sess->last_active = now;

    // idle session lost owenership of these entities
    idle_sess->arena = (PlArena){0};
    idle_sess->db_stores = NULL;

    // Remove stale idle record.
    parr_drop_swap(b->idle_sessions, (uint32_t)idle_idx);
    status = HS_OK;
  } else {
    // New session
    if (MAX_CLIENTS > 0 && parr_len(b->active_sessions) >= MAX_CLIENTS) {
      status = HS_ERR_FULL;
      goto send_n_close;
    }

    if (parr_len(b->idle_sessions) >= MAX_IDLE_SESSIONS &&
        broker_reap_one_idle_session(b->idle_sessions) != OK) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }

    if (fill_random(out_token, RESUME_TOKEN_LEN) != OK) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }

    active_idx =
        sessions_add_from_pending(b->active_sessions, &pending, &active_sess);
    if (active_idx == UINT32_MAX || !active_sess) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }
    memcpy(active_sess->resume_token, out_token, RESUME_TOKEN_LEN);
    active_sess->generation = 0;
    if (broker_session_token_state_init(active_sess) != OK) {
      status = HS_ERR_INTERNAL;
      goto send_n_close;
    }
    active_sess->created_at = now;
    active_sess->last_active = now;
    status = HS_OK;
  }

send_n_close: {
  BrokerMcpSession *resp_sess =
      (active_idx != UINT32_MAX && active_sess) ? active_sess : &pending;
  const uint8_t *resp_token = (status == HS_OK) ? out_token : NULL;
  if (broker_write_handshake_resp(resp_sess, status, resp_token,
                                  b->idle_ttl_secs, b->abs_ttl_secs) != OK) {
    // close whichever session object owns the channel at this point
    if (active_idx != UINT32_MAX) {
      parr_drop_swap(b->active_sessions, active_idx);
    } else {
      bufch_clean(&pending.bc);
      pending.fd = -1;
    }
    return ERR;
  }
}

  if (status != HS_OK) {
    if (active_idx != UINT32_MAX) {
      parr_drop_swap(b->active_sessions, active_idx);
    } else {
      bufch_clean(&pending.bc);
      pending.fd = -1;
    }
    return ERR;
  }

  // Keep bounded read timeouts after handshake too, so malformed/truncated
  // request frames cannot stall broker_run() indefinitely.
  if (broker_set_rcv_timeout_sec(active_sess->fd, REQUEST_READ_TIMEOUT_SEC) !=
      OK) {
    parr_drop_swap(b->active_sessions, active_idx);
    return ERR;
  }

  TLOG("INFO - accepted MCP client fd=%d", active_sess->fd);
  return OK;
}

/* Serializes one QueryResult as JSON-RPC and writes one length-prefixed frame.
 * It borrows 'sess' and 'q_res'.
 * Side effects: writes to the client channel.
 * Returns OK on successful encode/write, ERR on invalid input or write failure.
 */
static int broker_write_q_res(BrokerMcpSession *sess,
                              const QueryResult *q_res) {
  if (!q_res || !sess)
    return ERR;

  size_t response_len;
  char *response;
  int rc;
  if (qr_to_jsonrpc(q_res, &response, &response_len) != OK) {
    rc = ERR;
    goto clean_n_ret;
  }

  if (response_len > UINT32_MAX) {
    rc = ERR;
    goto clean_n_ret;
  }
  rc = frame_write_len(&sess->bc, response, (uint32_t)response_len);

clean_n_ret:
  free(response);
  return rc;
}

/* Runs the broker event loop.
 * It borrows and mutates Broker-owned session arrays and network descriptors.
 * Side effects: blocks in poll/accept/read/write, performs handshake
 * enforcement, and processes client tool requests.
 * Returns OK on clean stop (not currently reachable), ERR on fatal loop-level
 * failure.
 */
int broker_run(Broker *b) {
  if (!b)
    return ERR;

  struct pollfd pfds[MAX_CLIENTS + 1];

  for (;;) {
    // pollfd array: 1 for listen + up to max_clients active fds
    size_t nsessions = parr_len(b->active_sessions);
    size_t nfds = 1 + nsessions;

    // we reset the memory of pfds to be defensive
    memset(pfds, 0, nfds * sizeof(*pfds));

    // poll slot 0th = server socket
    pfds[0].fd = b->listen_fd;
    pfds[0].events = POLLIN;

    for (size_t i = 0; i < nsessions; i++) {
      BrokerMcpSession *sess = parr_at(b->active_sessions, (uint32_t)i);
      if (!sess)
        continue;
      pfds[1 + i].fd = sess->fd;
      pfds[1 + i].events = POLLIN;
      // revents is already 0 since memset above
    }

    int rc = poll(pfds, nfds, -1);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }

    // Handle client I/O for the sessions we polled this iteration.
    for (size_t i = 0; i < nsessions; /* increment inside */) {
      struct pollfd *pfd = &pfds[1 + i];

      // we don't increament 'i' when removing a session because the array
      // squash the next structures and fills the empty slot of the
      // removed session
      if (pfd->revents & (POLLHUP | POLLERR | POLLNVAL)) {
        session_move_to_idle(b->active_sessions, b->idle_sessions,
                             (uint32_t)i);
        nsessions--;
        continue;
      }

      if (pfd->revents & POLLIN) {
        BrokerMcpSession *sess = parr_at(b->active_sessions, (uint32_t)i);

        StrBuf req;
        sb_init(&req);
        QueryResult *q_res = NULL;
        uint64_t t0 = now_ms_monotonic();
        int rr = frame_read_len(&sess->bc, &req);
        if (rr != OK || req.len > MAX_REQ_LEN) {
          // framing error -> drop client
          TLOG("ERROR - drop client: frame_read_len rc=%d len=%zu", rr,
               req.len);
          sb_clean(&req);
          parr_drop_swap(b->active_sessions, (uint32_t)i);
          nsessions--;
          continue;
        }
        TLOG("INFO - received request len=%zu", req.len);

        if (req.len > MAX_REQ_LEN) {
          char buf[128];
          snprintf(buf, sizeof(buf),
                   "Error. Broker ignores message longer than %d bytes. "
                   "Please, respect the limit",
                   MAX_REQ_LEN);
          McpId id = {0};
          mcp_id_init_u32(&id, 0);
          q_res = qr_create_err(&id, QRERR_INREQ, buf);
          goto send_q_res;
        }

        int hr = broker_handle_request(b, sess, req.data, req.len, &q_res);

        if (hr != OK) {
          // Something bad happend, drop client
          fprintf(stderr, "Broker: request handling failed\n");
          sb_clean(&req);
          parr_drop_swap(b->active_sessions, (uint32_t)i);
          nsessions--;
          continue;
        }

        // Send response frame
      send_q_res:
        if (q_res && q_res->exec_ms == 0) {
          uint64_t t1 = now_ms_monotonic();
          q_res->exec_ms = (t1 >= t0) ? (t1 - t0) : 0;
        }
        if (broker_write_q_res(sess, q_res) != OK) {
          fprintf(stderr, "Broker: failed to write response\n");
          TLOG("ERROR - drop client: failed to write response");
          sb_clean(&req);
          qr_destroy(q_res);
          parr_drop_swap(b->active_sessions, (uint32_t)i);
          nsessions--;
          continue;
        }

        sb_clean(&req);
        qr_destroy(q_res);
      }

      i++;
    }

    // Accept one queued client after processing session events. This avoids
    // transient HS_ERR_FULL when an active client disconnected in the same poll
    // cycle.
    if (pfds[0].revents & POLLIN) {
      for (;;) {
        int cfd = accept(b->listen_fd, NULL, NULL);
        if (cfd < 0) {
          if (errno == EINTR)
            continue;
          // accept error; keep running
          break;
        }
        if (broker_do_handshake(b, cfd) != OK) {
          TLOG("INFO - rejected client fd=%d during handshake", cfd);
        }
        break;
        // TODO: For now we accept one at a time; loop accepts multiple
        // if queued
      }
    }
  }

  // TODO: create a signal to let Broker gracefully exit
  /* unreachable for now. */
  /* return 0; */
}
