#ifndef TRANSPORT_READ_H
#define TRANSPORT_READ_H

#include <stdio.h>
#include <stddef.h>

/* This entity reads SQL statements from a FILE. */
typedef struct TransportReader {
    FILE *in;

    // unconsumed bytes from previos reads (example, content after ';')
    char *stash;
    size_t stash_len;
} TransportReader;

/* Initializes the reader, doesn't take ownership of FILE*. */
void transport_r_init(TransportReader *r, FILE *in);

/* Frees internal buffers. */
void transport_r_clean(TransportReader *r);

/* Reads next SQL statement from its FILE. A statement ends when a semicolon ';'
 * is found that is not inside:
 *  - single quotes
 *  - double quotes
 * and we've skipped any whitespace directly after it (newline is optional).
 * Any non-whitespace bytes after the semicolon stay in the stash for the next
 * iteration, which allows multiple statements on the same line.
 *
 * Returns:
 *  1 -> success, *out_sql is malloc'd.
 *  0 -> clean EOF with no pending statement.
 * -1 -> error (malloc/read). */
int transport_r_read_sql(TransportReader *r, char **out_sql);

#endif
