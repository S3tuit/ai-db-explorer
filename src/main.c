#include "transport_reader.h"
#include "transport_writer.h"
#include "db_backend.h"
#include "postgres_backend.h"
#include "serializer.h"
#include "query_result.h"
#include "safety_policy.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>


static const char *test_conninfo(void) {
    return "host=localhost port=5432 dbname=postgres user=postgres password=postgres";
}

int main(void) {
    TransportReader r;
    transport_r_init(&r, stdin);

    TransportWriter w;
    transport_w_init(&w, stdout);

    SafetyPolicy policy;
    safety_policy_init(&policy, NULL, NULL, NULL, NULL);

    DbBackend *pg = postgres_backend_create();
    if (!pg) {
        fprintf(stderr, "failed to create postgres backend\n");
        return 1;
    }

    if (pg->vt->connect(pg, test_conninfo(), &policy) != OK) {
        fprintf(stderr, "connect failed\n");
        pg->vt->destroy(pg);
        return 1;
    }

    uint32_t request_id = 1;

    for (;;) {
        char *sql = NULL;
        int rc = transport_r_read_sql(&r, &sql);

        if (rc == 0) break;              // EOF
        if (rc < 0) {
            perror("read");
            break;
        }

        QueryResult *qr = NULL;
        pg->vt->exec(pg, request_id++, sql, &qr);
        free(sql);
        sql = NULL;

        /* Contract: exec should always provide a QueryResult unless catastrophic. */

        char *payload = NULL;
        size_t payload_len = 0;

        if (serializer_qr_to_jsonrpc(qr, &payload, &payload_len) != 1 || !payload) {
            fprintf(stderr, "serializer failed\n");
            qr_destroy(qr);
            break;
        }

        if (transport_w_write(&w, payload, payload_len) != 1) {
            fprintf(stderr, "write failed\n");
            free(payload);
            qr_destroy(qr);
            break;
        }

        free(payload);
        qr_destroy(qr);
    }

    pg->vt->destroy(pg);

    transport_r_clean(&r);
    transport_w_clean(&w);
    return 0;
}

