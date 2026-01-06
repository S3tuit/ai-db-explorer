#define _GNU_SOURCE

#include "broker.h"
#include "utils.h"
#include "frame_codec.h"
#include "json_codec.h"
#include "query_result.h"
#include "stdio_byte_channel.h"
#include "string_op.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <poll.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>


// TODO: make this configurable
#define MAX_CLIENTS 5
// Maximum size of a single request payload (bytes). Larger frames are rejected.
#define MAX_REQ_LEN (8u * 1024u * 1024u)

struct Broker {
    // TODO: abstract the fd because the Broker should have no knowledge if it's
    // using a socket/windows pipe
    int listen_fd;                  // file descriptor of the socket used to
                                    // accept incoming connection requets
    SecurityContext *security;      // owned
    char *sock_path;                // owned, socket path for the broker
    
    /* Array to handle sessions.
     * TODO: use an hashmap. */
    BrokerClientSession *sessions;
    size_t nsessions;
    size_t cap;
};

static inline void safe_close_fd(int *fd) {
  if (fd && *fd >= 0) {
    (void)close(*fd);
    *fd = -1;
  }
}

/* --------------------------------- Sessions ----------------------------- */

/* Makes sure 'b'->sessions has enough space to store 'need' bytes. */
static void sessions_ensure_cap(Broker *b, size_t need) {
    if (b->cap >= need) return;
    size_t newcap = b->cap ? b->cap * 2 : 8;
    if (newcap < need) newcap = need;

    BrokerClientSession *p = (BrokerClientSession *)xrealloc(b->sessions, newcap * sizeof(BrokerClientSession));

    // zero new area
    for (size_t i = b->cap; i < newcap; i++) {
        memset(&p[i], 0, sizeof(BrokerClientSession));
    }

    b->sessions = p;
    b->cap = newcap;
}

/* Initializes 'sess' and assigns 'cfd' to it. */
static int session_init(BrokerClientSession *sess, int cfd) {
    if (!sess) return ERR;
    
    // The BufChannel owns the ByteChannel, which owns the fd.
    ByteChannel *channel = stdio_bytechannel_open_fd(cfd, cfd);
    if (!channel) {
        safe_close_fd(&cfd);
        return ERR;
    }

    bufch_init(&sess->bc, channel);
    sess->fd = cfd;

    sess->current_dbname = NULL;
    sess->db = NULL;
    return OK;
}

/* Frees the entities owned by 's' but not its BufChannel. */
static void session_clean_db(BrokerClientSession *s) {
    if (s->db) {
        s->db->vt->destroy(s->db);
        s->db = NULL;
    }
    free(s->current_dbname);
    s->current_dbname = NULL;
}

/* Frees all the entities owned by 's' and closes its BufChannel. */
static void session_clean(BrokerClientSession *s) {
    if (s->db) {
        s->db->vt->destroy(s->db);
        s->db = NULL;
    }
    free(s->current_dbname);
    s->current_dbname = NULL;

    // we use clean and not destroy because they're embedded
    bufch_clean(&s->bc);
}


/* --------------------------------- Broker ------------------------------- */

/* Creates and return the file descriptor of a socket that can be used incoming
 * connection requests. */
static int make_listen_socket(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

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

/* Adds a new BrokerClientSession to 'b'->sessions that writes and read at
 * 'cfd'. */
static int broker_add_client(Broker *b, int cfd) {
    if (MAX_CLIENTS > 0 && b->nsessions >= MAX_CLIENTS) {
        safe_close_fd(&cfd);
        return NO;
    }
    sessions_ensure_cap(b, b->nsessions + 1);

    BrokerClientSession *s = &b->sessions[b->nsessions];
    if (session_init(s, cfd) != OK) {
        safe_close_fd(&cfd);
        return ERR;
    }
    b->nsessions++;

    return YES;
}

/* Remove the 'idx'th session from 'b'->sessions. Note: the sessions from 'idx'
 * till the end will change. */
static void broker_remove_session_at(Broker *b, size_t idx) {
    if (idx >= b->nsessions) return;
    session_clean(&b->sessions[idx]);

    // swap-remove
    size_t last = b->nsessions - 1;
    if (idx != last) {
        b->sessions[idx] = b->sessions[last];
    }
    b->nsessions--;
}

Broker *broker_create(const char *sock_path) {
    Broker *b = (Broker *)xcalloc(1, sizeof(Broker));

    b->listen_fd = -1;
    b->security = security_ctx_create();
    b->sessions = NULL;
    b->nsessions = 0;
    b->cap = 0;

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
    if (!b) return;

    // cleanup sessions
    for (size_t i = 0; i < b->nsessions; i++) {
        session_clean(&b->sessions[i]);
    }
    free(b->sessions);
    b->sessions = NULL;
    b->nsessions = 0;
    b->cap = 0;

    if (b->listen_fd >= 0) {
        safe_close_fd(&b->listen_fd);
        b->listen_fd = -1;
    }

    free(b->sock_path);
    b->sock_path = NULL;
    
    security_ctx_destroy(b->security);
    free(b);
}

/* Runs the command inside 'req' and returns a QueryResult (never NULL). The
 * expected command is:
 *
 * \connect dbname=""
 *
 * It's up to the caller to veryfy that after '\' there really is "connect".
 * This never returns NULL. */
static QueryResult *broker_cmd_connect(Broker *b, BrokerClientSession *sess,
                                    const char *req, uint32_t req_len,
                                    uint32_t id) {
    if (!b || !req) {
        return qr_create_err(id, "Abnormal situation detected in the broker while executing \\connect. Please retry.");
    }

    char *dbname = NULL;
    QueryResult *q_res = NULL;
    if (json_get_value(req, req_len, "%s", "params.dbname", &dbname) != YES) {
        goto clean_n_ret;
    }
    
    // if any previous connection, close what 'sess' owned
    session_clean_db(sess);

    int conn_rc = security_ctx_connect(b->security, dbname, &sess->db);
    if (conn_rc == ERR) {
        session_clean_db(sess);
        char buf[128];
        snprintf(buf, sizeof(buf), "Unknown dbname: %s", dbname);
        q_res = qr_create_err(id, buf);
        goto clean_n_ret;
    }
    if (conn_rc == NO || !sess->db) {
        session_clean_db(sess);
        char buf[128];
        snprintf(buf, sizeof(buf), "Unable to connect to: %s", dbname);
        q_res = qr_create_err(id, buf);
        goto clean_n_ret;
    }

    char buf[128];
    snprintf(buf, sizeof(buf), "Successfully connected to: %s", dbname);
    q_res = qr_create_msg(id, buf);

clean_n_ret:
    if (!q_res) q_res = qr_create_err(id, "Bad input in \\connect. Usage \\connect dbname=\"db_to_connect\"");
    free(dbname);
    return q_res;
}

/* Core request handler:
 * - 'req' points to the incoming 'req_len' bytes.
 * - 'out_res' will be filled and may represent an error or a result of a query.
 *
 * Returns:
 *  OK on success (broker will populate 'out_res'),
 *  ERR on error (broker can't even populate 'out_res').
 */
static int broker_handle_request(Broker *b, BrokerClientSession *sess,
                                    const char *req, uint32_t req_len,
                                    QueryResult **out_res) {
    if (!sess || !req) return ERR;

    char *method = NULL;
    uint32_t id;
    int rc = json_get_value(req, req_len, "%u%s", "id", &id, "method", &method);
    
    QueryResult *q_res = NULL;
    // generic errors
    if (rc != YES) {
        // fallback to 0 if id not present
        if (json_get_value(req, req_len, "%u", "id", &id) != YES) {
            id = 0;
            q_res = qr_create_err(id, "Something went wrong internally. Please retry.");
        } else {
            q_res = qr_create_err(id, "Something went wrong internally. Make sure the input was valid.");
        }
    
    // run query
    } else if (strcmp(method, BROK_EXEC_CMD) == 0) {
        // client is not connected to a database yet
        if (!sess->db) {
            q_res = qr_create_err(id, "Not connected to a database. Run \\connect dbname=\"your dbname\" .");
            goto return_res;
        }

        char *query = NULL;
        if (json_get_value(req, req_len, "%s", "params.sql", &query) != YES) {
            free(query);
            q_res = qr_create_err(id, "Unable to understand the input command. Make sure it was valid.");
            goto return_res;
        }

        DbBackend *db = sess->db;
        if (db->vt->exec(db, id, query, &q_res) != OK) {
            free(query);
            q_res = qr_create_err(id, "Something went wrong while communicating with the database.");
            goto return_res;
        }
        free(query);

    // commands
    } else if (strcmp(method, "connect") == 0) {
        q_res = broker_cmd_connect(b, sess, req, req_len, id);

    // unknown commands
    } else {
        // TODO: implement \help
        q_res = qr_create_err(id, "Unknown command. Run \\help for the available commands.");
        goto return_res;
    }

return_res:
    free(method);
    // catastrophic
    if (!q_res) {
        *out_res = NULL;
        return ERR;
    }

    *out_res = q_res;
    return OK;
}

/* Frames 'q_res' and writers it to the Client at 'sess'. Returns OK/ERR. */
static int broker_write_q_res(BrokerClientSession *sess, const QueryResult *q_res) {
    if (!q_res || !sess) return ERR;

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
    if (!b) return ERR;

    for (;;) {
        // pollfd array: 1 for listen + up to max_clients client fds
        size_t nfds = 1 + b->nsessions;
        
        // TODO: makes no sense to allocate/free each loop. Solve it.
        struct pollfd *pfds = (struct pollfd *)xmalloc(nfds * sizeof(*pfds));
        memset(pfds, 0, nfds * sizeof(*pfds));

        // poll slot 0th = server socket
        pfds[0].fd = b->listen_fd;
        pfds[0].events = POLLIN;

        for (size_t i = 0; i < b->nsessions; i++) {
            pfds[1 + i].fd = b->sessions[i].fd;
            pfds[1 + i].events = POLLIN;
            // revents is already 0 since memset above
        }

        int rc = poll(pfds, nfds, -1);
        if (rc < 0) {
            if (errno == EINTR) {
                free(pfds);
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
                    if (errno == EINTR) continue;
                    // The socket is marked nonblocking and no connections are
                    // present to be accepted
                    // TODO: when sockets will be nonblocking
                    // if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    // accept error; keep running
                    break;
                }
                if (broker_add_client(b, cfd) != 0) {
                    // fatal-ish; we keep going for robustness
                }
                break;
                // TODO: For now we accept one at a time; loop accepts multiple
                // if queued
            }
        }

        // Handle client I/O
        for (size_t i = 0; i < b->nsessions; /* increment inside */) {
            // we don't increament when removing a session because the array
            // squash the next structures and fills the empty slot of the
            // removed session
            struct pollfd *pfd = &pfds[1 + i];

            if (pfd->revents & (POLLHUP | POLLERR | POLLNVAL)) {
                broker_remove_session_at(b, i);
                continue;
            }

            if (pfd->revents & POLLIN) {
                BrokerClientSession *sess = &b->sessions[i];

                StrBuf req = {0};
                QueryResult *q_res = NULL;
                uint64_t t0 = now_ms_monotonic();
                // TODO: frame_codec and json_codec return different outputs,
                // one char **out, uint32_t *len... the other StrBuf **out.
                // Make them consistent.
                int rr = frame_read_len(&sess->bc, &req);
                if (rr != OK || req.len > MAX_REQ_LEN) {
                    // framing error -> drop client
                    sb_clean(&req);
                    broker_remove_session_at(b, i);
                    continue;
                }
                
                if (req.len > MAX_REQ_LEN) {
                    char buf[128];
                    snprintf(buf, sizeof(buf), "Error. Broker ignores message longer than %d bytes. Please, respect the limit", MAX_REQ_LEN);
                    q_res = qr_create_err(0, buf);
                    goto send_q_res;
                }
                
                int hr = broker_handle_request(b, sess, req.data, req.len, &q_res);

                if (hr != OK) {
                    // Something bad happend, drop client
                    sb_clean(&req);
                    broker_remove_session_at(b, i);
                    continue;
                }

                // Send response frame
send_q_res:
                if (q_res && q_res->exec_ms == 0) {
                    uint64_t t1 = now_ms_monotonic();
                    q_res->exec_ms = (t1 >= t0) ? (t1 - t0) : 0;
                }
                if (broker_write_q_res(sess, q_res) != OK) {
                    sb_clean(&req);
                    qr_destroy(q_res);
                    broker_remove_session_at(b, i);
                    continue;
                }

                sb_clean(&req);
                qr_destroy(q_res);
            }

            i++;
        }

        free(pfds);
    }

    // TODO: create a signal to let Broker gracefully exit
    /* unreachable for now. */
    /* return 0; */
}
