#include "session_manager.h"
#include "serializer.h"
#include "query_result.h"
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

    transport_r_init(&s->r, in);
    transport_w_init(&s->w, out);

    return OK;
}

int session_run(SessionManager *s) {
    if (!s || !s->db || !s->db->vt) return ERR;

    for (;;) {
        char *sql = NULL;

        int rc = transport_r_read_sql(&s->r, &sql);
        if (rc == NO) {
            // EOF
            return OK;
        }
        if (rc == ERR) {
            sm_set_err(s, "transport reader failed");
            free(sql);
            return ERR;
        }

        uint32_t id = s->next_id++;
        QueryResult *qr = NULL;

        // Backend contract: returns OK and always produces a QueryResult.
        // It returns ERR only on catastrophic errors. */
        int exec_rc = s->db->vt->exec(s->db, id, sql, &qr);
        free(sql);
        sql = NULL;

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

        if (serializer_qr_to_jsonrpc(qr, &payload, &payload_len) != OK || !payload) {
            qr_destroy(qr);
            sm_set_err(s, "serializer failed");
            return ERR;
        }

        if (transport_w_write(&s->w, payload, payload_len) != OK) {
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
    transport_r_clean(&s->r);
    transport_w_clean(&s->w);
}

const char *session_last_error(const SessionManager *s) {
    if (!s) return "SessionManager is NULL";
    return s->last_err;
}
