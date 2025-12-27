#include "transport_writer.h"
#include "utils.h"

#include <string.h>

/* Write exactly n bytes to FILE*. Returns OK on success, ERR on error. */
static int write_all(FILE *out, const char *buf, size_t n) {
    if (n == 0) return OK;
    while (n > 0) {
        size_t wrote = fwrite(buf, 1, n, out);
        if (wrote == 0) {
            return ERR;
        }
        buf += wrote;
        n -= wrote;
    }
    return OK;
}

void transport_w_init(TransportWriter *w, FILE *out) {
    w->out = out;
}

void transport_w_clean(TransportWriter *w) {
    if (!w) return;
    w->out = NULL;
}

int transport_w_write(TransportWriter *w, const char *bytes, size_t n) {
    if (n == 0) {
        return OK;
    }

    if (w == NULL || w->out == NULL || bytes == NULL) {
        return ERR;
    }

    // header: Content-Length: <n>\r\n\r\n
    char header[64];
    int header_len = snprintf(header, sizeof(header),
                              "Content-Length: %zu\r\n\r\n", n);
    if (header_len < 0 || (size_t)header_len >= sizeof(header)) {
        // should never happen */
        return ERR;
    }

    // write header + payload
    if (write_all(w->out, header, (size_t)header_len) != OK) {
        return ERR;
    }
    if (write_all(w->out, bytes, n) != OK) {
        return ERR;
    }

    // flush so the agent sees the response immediately
    if (fflush(w->out) != 0) {
        return ERR;
    }

    return OK;
}
