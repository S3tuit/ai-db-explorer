#ifndef TRANSPORT_READ_H
#define TRANSPORT_READ_H

#include <stdio.h>
#include <stddef.h>

typedef struct TransportReader {
    FILE *in;

    // unconsumed bytes from previos reads (example, content after ';')
    char *stash;
    size_t stash_len;
} TransportReader;

/* Initializes the reader, doesn't take ownership of FILE*. */
void transport_r_init(TransportReader *r, FILE *in);

/* Frees interla buffers. */
void transport_r_clean(TransportReader *r);

/* Reads next SQL statement from stdin. A statement ends when a semicolon ';'
 * is found that is not inside:
 *  - single quotes
 *  - double quotes
 * and the line containing the semicolon has ended (we have consumed '\n' or
 * EOF).
 *
 * Returns:
 *  1 -> success, *out_sql is malloc'd.
 *  0 -> clean EOF with no pending statement.
 * -1 -> error (malloc/read). */
int transport_r_read_sql(TransportReader *r, char **out_sql);

#endif
