#ifndef TRANSPORT_WRITE_H
#define TRANSPORT_WRITE_H

#include <stdio.h>
#include <stddef.h>

/* This entity writes LSP-framed messages into a FILE.
 * Here's an example of a LSP-framed message:
 *
 * Content-Length: {n}\r\n\r\n" + <n payload bytes>*/
typedef struct TransportWriter {
    FILE *out;
} TransportWriter;

/* Initialize a TrasportWriter. Does not take ownership of 'out' */
void transport_w_init(TransportWriter *w, FILE *out);

/* Writer one framed message. Returns:
 *  OK: on success (including zero bytes), flushes 'out'.
 *  ERR: on error. */
int transport_w_write(TransportWriter *w, const char *bytes, size_t n);

void transport_w_clean(TransportWriter *w);

#endif
