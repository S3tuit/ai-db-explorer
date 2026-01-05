#include "session_manager.h"
#include "json_codec.h"
#include "query_result.h"
#include "stdio_byte_channel.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>

/* Best-effort helper to set 'msg' as the last error of 's'. Safe even if 'msg'
 * is NULL. */
static void sm_set_err(SessionManager *s, const char *msg) {
    if (!s) return;
    if (!msg) msg = "unknown error";
    snprintf(s->last_err, sizeof(s->last_err), "%s", msg);
}

int session_init(SessionManager *s, FILE *in, FILE *out, DbBackend *db) {
    if (!s || !in || !out || !db || !db->vt) return ERR;

    memset(s, 0, sizeof(*s));
    s->db = db;
    s->next_id = 1;
    s->last_err[0] = '\0';

    int in_fd = fileno(in);
    int out_fd = fileno(out);
    if (in_fd < 0 || out_fd < 0) return ERR;

    ByteChannel *ch = stdio_bytechannel_wrap_fd(in_fd, -1);
    if (!ch) return ERR;
    s->r = command_reader_create(ch);
    if (!s->r) return ERR;
    ByteChannel *out_ch = stdio_bytechannel_wrap_fd(-1, out_fd);
    if (!out_ch) {
        command_reader_destroy(s->r);
        s->r = NULL;
        return ERR;
    }
    s->out_bc = bufch_create(out_ch);
    if (!s->out_bc) {
        bytech_destroy(out_ch);
        command_reader_destroy(s->r);
        s->r = NULL;
        return ERR;
    }

    return OK;
}

int session_run(SessionManager *s) {
    if (!s || !s->db || !s->db->vt) return ERR;

    for (;;) {
        Command *cmd = NULL;

        int rc = command_reader_read_cmd(s->r, &cmd);
        if (rc == NO) {
            // EOF
            return OK;
        }
        if (rc == ERR) {
            sm_set_err(s, "transport reader failed");
            command_destroy(cmd);
            return ERR;
        }

        if (cmd->type == CMD_META) {
            uint32_t id = s->next_id++;
            QueryResult *qr = qr_create_err(id, "meta commands not supported");
            if (!qr) {
                sm_set_err(s, "OOM creating meta command error");
                command_destroy(cmd);
                return ERR;
            }

            char *payload = NULL;
            size_t payload_len = 0;
            if (qr_to_jsonrpc(qr, &payload, &payload_len) != OK || !payload) {
                qr_destroy(qr);
                command_destroy(cmd);
                sm_set_err(s, "json encoding failed");
                return ERR;
            }

            if (frame_write_cl(s->out_bc, payload, payload_len) != OK) {
                free(payload);
                qr_destroy(qr);
                command_destroy(cmd);
                sm_set_err(s, "transport writer failed");
                return ERR;
            }

            free(payload);
            qr_destroy(qr);
            command_destroy(cmd);
            continue;
        }

        uint32_t id = s->next_id++;
        QueryResult *qr = NULL;

        // Backend contract: returns OK and always produces a QueryResult.
        // It returns ERR only on catastrophic errors. */
        int exec_rc = s->db->vt->exec(s->db, id, cmd->raw_sql, &qr);
        command_destroy(cmd);

        if (exec_rc != OK || !qr) {
            // catastrophic backend failure
            const char *msg = "internal error: backend did not produce a QueryResult";
            qr = qr_create_err(id, msg);
            if (!qr) {
                sm_set_err(s, "OOM creating fallback error QueryResult");
                return ERR;
            }
        }

        char *payload = NULL;
        size_t payload_len = 0;
        if (qr_to_jsonrpc(qr, &payload, &payload_len) != OK || !payload) {
            qr_destroy(qr);
            sm_set_err(s, "json encoding failed");
            return ERR;
        }

        if (frame_write_cl(s->out_bc, payload, payload_len) != OK) {
            free(payload);
            qr_destroy(qr);
            sm_set_err(s, "transport writer failed");
            return ERR;
        }

        free(payload);
        qr_destroy(qr);
    }
}

void session_clean(SessionManager *s) {
    if (!s) return;
    if (s->r) {
        command_reader_destroy(s->r);
        s->r = NULL;
    }
    if (s->out_bc) {
        bufch_destroy(s->out_bc);
        s->out_bc = NULL;
    }
}

const char *session_last_error(const SessionManager *s) {
    if (!s) return "SessionManager is NULL";
    return s->last_err;
}
