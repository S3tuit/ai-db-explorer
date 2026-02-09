#define _GNU_SOURCE

#include "broker.h"
#include "frame_codec.h"
#include "json_codec.h"
#include "log.h"
#include "packed_array.h"
#include "query_result.h"
#include "stdio_byte_channel.h"
#include "string_op.h"
#include "utils.h"
#include "validator.h"

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

// Maximum size of a single request payload (bytes). Larger frames are rejected.
#define MAX_REQ_LEN (8u * 1024u * 1024u)

struct Broker {
  int listen_fd;   // file descriptor of the socket used to
                   // accept incoming connection requets
  ConnManager *cm; // owned
  char *sock_path; // owned, socket path for the broker

  PackedArray *active_sessions; // polled for I/O, max MAX_CLIENTS
  PackedArray *idle_sessions;   // not polled, max MAX_IDLE_SESSIONS
};

static inline void safe_close_fd(int *fd) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
}

/* --------------------------------- Sessions ------------------------------ */

/* Callback for active_sessions: tears down the live BufChannel. */
static void active_sessions_cleanup(void *obj, void *ctx) {
  (void)ctx;
  BrokerMcpSession *s = (BrokerMcpSession *)obj;
  if (!s)
    return;
  bufch_clean(&s->bc);
  s->fd = -1;
}

/* Callback for idle_sessions: BufChannel is already torn down, nothing to
 * close. */
static void idle_sessions_cleanup(void *obj, void *ctx) {
  (void)ctx;
  (void)obj;
}

/* Adds and initializes a new BrokerMcpSession to 'parr' identified by 'cfd'.
 * A pointer to the new session is assigned to 'out_sess' if not NULL. Returns
 * ok/err.*/
static int sessions_add(PackedArray *parr, int cfd,
                        BrokerMcpSession **out_sess) {
  if (!parr || !out_sess)
    return ERR;

  if (out_sess)
    *out_sess = NULL;

  BrokerMcpSession *sess = NULL;
  size_t idx = parr_emplace(parr, (void **)&sess);
  if (idx == SIZE_MAX || !sess)
    return ERR;

  // The BufChannel owns the ByteChannel, which owns the fd.
  ByteChannel *channel = stdio_bytechannel_open_fd(cfd, cfd);
  if (!channel) {
    safe_close_fd(&cfd);
    goto drop_n_return;
  }

  if (bufch_init(&sess->bc, channel) != OK) {
    bytech_destroy(channel);
    goto drop_n_return;
  }
  sess->fd = cfd;

  *out_sess = sess;
  return OK;

drop_n_return:
  // delete the session if we failed to init it
  parr_drop_swap(parr, idx);
  return ERR;
}

/* Moves the active session at 'active_idx' to 'idle'. Tears down the
 * BufChannel, sets fd = -1, updates last_active, then removes from 'active'.
 * If idle is full the oldest idle session is evicted first. */
static void session_move_to_idle(PackedArray *active, PackedArray *idle,
                                 size_t active_idx) {
  BrokerMcpSession *src = parr_at(active, active_idx);
  if (!src)
    return;

  /* Save fields we need before the active slot is cleaned/overwritten. */
  uint8_t token[RESUME_TOKEN_LEN];
  memcpy(token, src->resume_token, RESUME_TOKEN_LEN);
  time_t created = src->created_at;

  /* Tear down the live connection. */
  bufch_clean(&src->bc);
  src->fd = -1;

  /* Remove from active (cleanup callback is safe — bufch_clean is
   * idempotent). */
  parr_drop_swap(active, active_idx);

  /* Evict oldest idle session if at capacity. */
  if (parr_len(idle) >= MAX_IDLE_SESSIONS) {
    parr_drop_swap(idle, 0);
  }

  /* Insert into idle. */
  BrokerMcpSession *dst = NULL;
  size_t idx = parr_emplace(idle, (void **)&dst);
  if (idx == SIZE_MAX || !dst)
    return;

  memset(dst, 0, sizeof(*dst));
  dst->fd = -1;
  memcpy(dst->resume_token, token, RESUME_TOKEN_LEN);
  dst->created_at = created;
  dst->last_active = time(NULL);
}

/* --------------------------------- Broker ------------------------------- */

/* Creates and return the file descriptor of a socket that can be used incoming
 * connection requests. */
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

Broker *broker_create(const char *sock_path, ConnManager *cm) {
  if (!cm || !sock_path)
    return NULL;
  Broker *b = (Broker *)xcalloc(1, sizeof(Broker));

  b->listen_fd = -1;
  b->cm = cm;

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

size_t broker_active_count(const Broker *b) {
  return b ? parr_len(b->active_sessions) : 0;
}

size_t broker_idle_count(const Broker *b) {
  return b ? parr_len(b->idle_sessions) : 0;
}

/*-------------------------------- Tools Call -------------------------------*/

/* Executes the SQL inside the arguments of 'jg' and populates 'out_query' with
 * the result using 'id' as the id. 'out_query' may not be populated.
 *
 * Ownership: this borrows all the entities in input.*/
static void broker_run_sql_query(Broker *b, JsonGetter *jg, McpId *id,
                                 QueryResult **out_query) {
  char *conn_name = NULL;
  char *query = NULL;
  if (jsget_string_decode_alloc(jg, "params.arguments.connectionName",
                                &conn_name) != YES ||
      jsget_string_decode_alloc(jg, "params.arguments.query", &query) != YES) {
    free(conn_name);
    free(query);
    *out_query = qr_create_err(id, "Invalid tool arguments.");
    goto free_n_return;
  }

  TLOG("INFO - preparing for running %s", query);
  ConnView cv = {0};
  int rc = connm_get_connection(b->cm, conn_name, &cv);
  if (rc != YES || !cv.db || !cv.profile) {
    TLOG("ERROR - unable to connect to %s", conn_name);
    *out_query =
        qr_create_err(id, "Unable to connect to the requested database.");
    goto free_n_return;
  }

  StrBuf verr_msg = {0};
  ValidatorErr verr = {.code = VERR_NONE, .msg = &verr_msg};
  ValidatorRequest vreq = {.db = cv.db, .profile = cv.profile, .sql = query};
  if (validate_query(&vreq, &verr) != OK) {
    const char *err_desc = sb_to_cstr(verr.msg);
    if (err_desc[0] == '\0') {
      err_desc = "Unknown error while validating the query. Please make sure "
                 "the query is valid and formatted correctly.";
    }
    *out_query = qr_create_err(id, err_desc);
    sb_clean(&verr_msg);
    goto free_n_return;
  }
  sb_clean(&verr_msg);

  if (db_exec(cv.db, query, out_query) != OK) {
    TLOG("ERROR - error while communicating with %s", conn_name);
    *out_query = qr_create_err(
        id, "Something went wrong while communicating with the database.");
    goto free_n_return;
  }
  // db_exec leaves the id zeroed; stamp it with the request id
  if (mcp_id_copy(&(*out_query)->id, id) != OK) {
    qr_destroy(*out_query);
    *out_query = NULL;
    goto free_n_return;
  }
  connm_mark_used(b->cm, conn_name);

free_n_return:
  free(conn_name);
  free(query);
}

/* Core request handler:
 * - 'req' points to the incoming 'req_len' bytes.
 * - 'out_res' will be filled and may represent an error or a result of a query.
 *
 * Returns:
 *  OK on success (broker will populate 'out_res'),
 *  ERR on error (broker can't even populate 'out_res').
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
    *out_res = qr_create_err(&id, "Invalid JSON-RPC request.");
    goto return_res;
  }
  // exec command
  JsonStrSpan method_sp = {0};
  if (jsget_string_span(&jg, "method", &method_sp) != YES) {
    *out_res = qr_create_err(&id, "Can't find the 'method' object.");
    goto return_res;
  }

  if (!STREQ(method_sp.ptr, method_sp.len, "tools/call")) {
    *out_res = qr_create_err(&id, "Tool not supported.");
    goto return_res;
  }

  JsonStrSpan name_sp = {0};
  if (jsget_string_span(&jg, "params.name", &name_sp) != YES) {
    *out_res = qr_create_err(&id, "Tool call missing params.name.");
    goto return_res;
  }

  // Call the different tools
  if (STREQ(name_sp.ptr, name_sp.len, "run_sql_query")) {
    broker_run_sql_query(b, &jg, &id, out_res);

    // Unknown tools
  } else {
    *out_res = qr_create_err(&id, "Unknown tool.");
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

/* Returns OK if 'cfd' is a socket with peer credentials equal to the one of
 * this running process. Else, returns ERR. */
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

/* Frames 'q_res' and writers it to the Client at 'sess'. Returns OK/ERR. */
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
      BrokerMcpSession *sess = parr_at(b->active_sessions, i);
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

    // accept new client
    if (pfds[0].revents & POLLIN) { // if some fd has data to read
      for (;;) {
        int cfd = accept(b->listen_fd, NULL, NULL);
        if (cfd < 0) {
          if (errno == EINTR)
            continue;
          // accept error; keep running
          break;
        }
        if (verify_peer_uid(cfd) != OK) {
          TLOG("INFO - rejected client fd=%d: peer UID mismatch", cfd);
          safe_close_fd(&cfd);
          continue;
        }
        if (MAX_CLIENTS > 0 && parr_len(b->active_sessions) >= MAX_CLIENTS) {
          safe_close_fd(&cfd);
          break;
        }
        BrokerMcpSession *new_sess = NULL;
        if (sessions_add(b->active_sessions, cfd, &new_sess) != OK) {
          safe_close_fd(&cfd);
          break;
        }
        new_sess->created_at = time(NULL);
        new_sess->last_active = new_sess->created_at;
        TLOG("INFO - accepted MCP client fd=%d", cfd);
        break;
        // TODO: For now we accept one at a time; loop accepts multiple
        // if queued
      }
    }

    // Handle client I/O for the sessions we polled this iteration.
    for (size_t i = 0; i < nsessions; /* increment inside */) {
      struct pollfd *pfd = &pfds[1 + i];

      // we don't increament 'i' when removing a session because the array
      // squash the next structures and fills the empty slot of the
      // removed session
      if (pfd->revents & (POLLHUP | POLLERR | POLLNVAL)) {
        session_move_to_idle(b->active_sessions, b->idle_sessions, i);
        nsessions--;
        continue;
      }

      if (pfd->revents & POLLIN) {
        BrokerMcpSession *sess = parr_at(b->active_sessions, i);

        StrBuf req = {0};
        QueryResult *q_res = NULL;
        uint64_t t0 = now_ms_monotonic();
        int rr = frame_read_len(&sess->bc, &req);
        if (rr != OK || req.len > MAX_REQ_LEN) {
          // framing error -> drop client
          TLOG("ERROR - drop client: frame_read_len rc=%d len=%zu", rr,
               req.len);
          sb_clean(&req);
          parr_drop_swap(b->active_sessions, i);
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
          q_res = qr_create_err(&id, buf);
          goto send_q_res;
        }

        int hr = broker_handle_request(b, sess, req.data, req.len, &q_res);

        if (hr != OK) {
          // Something bad happend, drop client
          fprintf(stderr, "Broker: request handling failed\n");
          sb_clean(&req);
          parr_drop_swap(b->active_sessions, i);
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
          parr_drop_swap(b->active_sessions, i);
          nsessions--;
          continue;
        }

        sb_clean(&req);
        qr_destroy(q_res);
      }

      i++;
    }
  }

  // TODO: create a signal to let Broker gracefully exit
  /* unreachable for now. */
  /* return 0; */
}
