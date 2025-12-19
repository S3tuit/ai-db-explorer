#include "transport_writer.h"

#include <string.h>

/* Write exactly n bytes to FILE*. Returns 1 on success, 0 if no bytes are 
 * written, -1 on error. */
static int write_all(FILE *out, const char *buf, size_t n) {
    if (n <= 0) return 0;
    while (n > 0) {
        size_t wrote = fwrite(buf, 1, n, out);
        if (wrote == 0) {
            return -1;
        }
        buf += wrote;
        n -= wrote;
    }
    return 1;
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
        return 0;
    }

    if (w == NULL || w->out == NULL || bytes == NULL) {
        return -1;
    }

    // header: Content-Length: <n>\r\n\r\n
    char header[64];
    int header_len = snprintf(header, sizeof(header),
                              "Content-Length: %zu\r\n\r\n", n);
    if (header_len < 0 || (size_t)header_len >= sizeof(header)) {
        // should never happen */
        return -1;
    }

    // write header + payload
    if (write_all(w->out, header, (size_t)header_len) <= 0) {
        return -1;
    }
    if (write_all(w->out, bytes, n) <= 0) {
        return -1;
    }

    // flush so the agent sees the response immediately
    if (fflush(w->out) != 0) {
        return -1;
    }

    return 1;
}

