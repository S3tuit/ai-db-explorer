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

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

// TODO: we should be able to accept more than 1 McpServer with just one Broker.
// We should make the code async, and use a real connection pool.
#define MAX_CLIENTS 1
// Maximum size of a single request payload (bytes). Larger frames are rejected.
#define MAX_REQ_LEN (8u * 1024u * 1024u)

struct Broker {
  // TODO: abstract the fd because the Broker should have no knowledge if it's
  // using a socket/windows pipe
  int listen_fd;   // file descriptor of the socket used to
                   // accept incoming connection requets
  ConnManager *cm; // owned
  char *sock_path; // owned, socket path for the broker

  PackedArray *sessions;
};

static inline void safe_close_fd(int *fd) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
}

/* --------------------------------- Sessions ------------------------------ */

static int session_init(BrokerMcpSession *sess, int cfd) {
  if (!sess)
    return ERR;

  // The BufChannel owns the ByteChannel, which owns the fd.
  ByteChannel *channel = stdio_bytechannel_open_fd(cfd, cfd);
  if (!channel) {
    safe_close_fd(&cfd);
    return ERR;
  }

  if (bufch_init(&sess->bc, channel) != OK) {
    bytech_destroy(channel);
    return ERR;
  }
  sess->fd = cfd;
  return OK;
}

/* Callback function to pass to PackedArray to close the BufChannel owned by
 * the BrokerMcpSession 'obj'. */
static void sessions_cleanup(void *obj, void *ctx) {
  (void)ctx;
  BrokerMcpSession *s = (BrokerMcpSession *)obj;
  if (!s)
    return;
  bufch_clean(&s->bc);
  s->fd = -1;
}

/* Adds and initializes a new BrokerMcpSession to 'parr' identified by 'cfd'.
 * A pointer to the new session is assigned to 'out_sess' if not NULL. Returns
 * ok/err.*/
static int sessions_add(PackedArray *parr, int cfd,
                        BrokerMcpSession **out_sess) {
  if (out_sess)
    *out_sess = NULL;
  if (!parr)
    return ERR;

  BrokerMcpSession *sess = NULL;
  size_t idx = parr_emplace(parr, (void **)&sess);
  if (idx == SIZE_MAX || !sess)
    return ERR;

  if (session_init(sess, cfd) != OK) {
    // delete the session if we failed to init it
    parr_drop_swap(parr, idx);
    return ERR;
  }

  if (out_sess)
    *out_sess = sess;
  return OK;
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
  if (listen(fd, 16) < 0) {
    safe_close_fd(&fd);
    return -1;
  }
  return fd;
}

Broker *broker_create(const char *sock_path, ConnManager *cm) {
  if (!cm)
    return NULL;
  Broker *b = (Broker *)xcalloc(1, sizeof(Broker));

  b->listen_fd = -1;
  b->cm = cm;

  // The PackedArray is responsible for cleaning up the resources of the
  // sessions
  b->sessions = parr_create(sizeof(BrokerMcpSession));
  parr_set_cleanup(b->sessions, sessions_cleanup, NULL);

  const char *path = sock_path ? sock_path : SOCK_PATH;
  b->sock_path = dup_or_null(path);
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

  parr_destroy(b->sessions);
  b->sessions = NULL;

  if (b->listen_fd >= 0) {
    safe_close_fd(&b->listen_fd);
    b->listen_fd = -1;
  }

  free(b->sock_path);
  b->sock_path = NULL;

  connm_destroy(b->cm);
  free(b);
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
  if (!b || !sess || !req)
    return ERR;
  TLOG("INFO - handling a request of %u bytes", req_len);

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

  QueryResult *q_res = NULL;
  int vrc = jsget_simple_rpc_validation(&jg);
  if (vrc != YES) {
    q_res = qr_create_err(&id, "Invalid JSON-RPC request.");
    goto return_res;
  }
  // exec command
  JsonStrSpan method_sp = {0};
  if (jsget_string_span(&jg, "method", &method_sp) != YES) {
    q_res = qr_create_err(&id, "Can't find the 'method' object.");
    goto return_res;
  }

  if (!STREQ(method_sp.ptr, method_sp.len, "tools/call")) {
    q_res = qr_create_err(&id, "Tool not supported.");
    goto return_res;
  }

  JsonStrSpan name_sp = {0};
  if (jsget_string_span(&jg, "params.name", &name_sp) != YES ||
      name_sp.len != strlen("run_sql_query") ||
      memcmp(name_sp.ptr, "run_sql_query", name_sp.len) != 0) {
    q_res = qr_create_err(&id, "Unknown tool.");
    goto return_res;
  }

  char *conn_name = NULL;
  char *query = NULL;
  if (jsget_string_decode_alloc(&jg, "params.arguments.connectionName",
                                &conn_name) != YES ||
      jsget_string_decode_alloc(&jg, "params.arguments.query", &query) != YES) {
    free(conn_name);
    free(query);
    q_res = qr_create_err(&id, "Invalid tool arguments.");
    goto return_res;
  }

  TLOG("INFO - preparing for running %s", query);
  DbBackend *db = connm_get_backend(b->cm, conn_name);
  if (!db) {
    TLOG("ERROR - unable to connect to %s", conn_name);
    free(conn_name);
    free(query);
    q_res = qr_create_err(&id, "Unable to connect to the requested database.");
    goto return_res;
  }

  if (db_exec(db, &id, query, &q_res) != OK) {
    TLOG("ERROR - error while communicating with %s", conn_name);
    free(conn_name);
    free(query);
    q_res = qr_create_err(
        &id, "Something went wrong while communicating with the database.");
    goto return_res;
  }
  connm_mark_used(b->cm, conn_name);
  free(conn_name);
  free(query);

return_res:
  // catastrophic
  if (!q_res) {
    *out_res = NULL;
    mcp_id_clean(&id);
    return ERR;
  }

  *out_res = q_res;
  mcp_id_clean(&id);
  return OK;
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

  struct pollfd *pfds =
      (struct pollfd *)xmalloc((MAX_CLIENTS + 1) * sizeof(*pfds));

  for (;;) {
    // pollfd array: 1 for listen + up to max_clients client fds
    size_t nsessions = parr_len(b->sessions);
    size_t nfds = 1 + nsessions;

    // we reset the memory of pfds to be defensive
    memset(pfds, 0, nfds * sizeof(*pfds));

    // poll slot 0th = server socket
    pfds[0].fd = b->listen_fd;
    pfds[0].events = POLLIN;

    for (size_t i = 0; i < nsessions; i++) {
      BrokerMcpSession *sess = parr_at(b->sessions, i);
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
      free(pfds);
      return -1;
    }

    // accept new client
    if (pfds[0].revents & POLLIN) { // if some fd has data to read
      for (;;) {
        int cfd = accept(b->listen_fd, NULL, NULL);
        if (cfd < 0) {
          if (errno == EINTR)
            continue;
          // The socket is marked nonblocking and no connections are
          // present to be accepted
          // TODO: when sockets will be nonblocking
          // if (errno == EAGAIN || errno == EWOULDBLOCK) break;
          // accept error; keep running
          break;
        }
        if (MAX_CLIENTS > 0 && parr_len(b->sessions) >= MAX_CLIENTS) {
          safe_close_fd(&cfd);
          break;
        }
        if (sessions_add(b->sessions, cfd, NULL) != OK) {
          safe_close_fd(&cfd);
          break;
        }
        TLOG("INFO - accepted MCP client fd=%d", cfd);
        break;
        // TODO: For now we accept one at a time; loop accepts multiple
        // if queued
      }
    }

    // Handle client I/O for the sessions we polled this iteration.
    for (size_t i = 0; i < nsessions; /* increment inside */) {
      // we don't increament 'i' when removing a session because the array
      // squash the next structures and fills the empty slot of the
      // removed session
      struct pollfd *pfd = &pfds[1 + i];

      if (pfd->revents & (POLLHUP | POLLERR | POLLNVAL)) {
        parr_drop_swap(b->sessions, i);
        nsessions--;
        continue;
      }

      if (pfd->revents & POLLIN) {
        BrokerMcpSession *sess = parr_at(b->sessions, i);

        StrBuf req = {0};
        QueryResult *q_res = NULL;
        uint64_t t0 = now_ms_monotonic();
        int rr = frame_read_len(&sess->bc, &req);
        if (rr != OK || req.len > MAX_REQ_LEN) {
          // framing error -> drop client
          fprintf(stderr, "Broker: frame_read_len failed (rc=%d, len=%zu)\n",
                  rr, req.len);
          TLOG("ERROR - drop client: frame_read_len rc=%d len=%zu", rr,
               req.len);
          sb_clean(&req);
          parr_drop_swap(b->sessions, i);
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
          TLOG("ERROR - reject request: len=%zu exceeds MAX_REQ_LEN", req.len);
          McpId id = {0};
          mcp_id_init_u32(&id, 0);
          q_res = qr_create_err(&id, buf);
          goto send_q_res;
        }

        int hr = broker_handle_request(b, sess, req.data, req.len, &q_res);

        if (hr != OK) {
          // Something bad happend, drop client
          fprintf(stderr, "Broker: request handling failed\n");
          TLOG("ERROR - drop client: broker_handle_request failed");
          sb_clean(&req);
          parr_drop_swap(b->sessions, i);
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
          parr_drop_swap(b->sessions, i);
          nsessions--;
          continue;
        }

        sb_clean(&req);
        qr_destroy(q_res);
      }

      i++;
    }
  }
  free(pfds);

  // TODO: create a signal to let Broker gracefully exit
  /* unreachable for now. */
  /* return 0; */
}
