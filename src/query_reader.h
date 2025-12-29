#ifndef QUERY_READER_H
#define QUERY_READER_H

#include <stddef.h>

#include "bufio.h"
#include "string_op.h"

/* This entity reads SQL statements from a buffered byte stream. */
typedef struct QueryReader {
    BufReader *br;  // owned
    StrBuf stash;   // unconsumed bytes from previous reads (after ';')
} QueryReader;

/* Creates the reader and takes ownership of 'ch' (via BufReader). */
QueryReader *query_reader_create(ByteChannel *ch);

/* Frees internal buffers and owned BufReader/ByteChannel. */
void query_reader_destroy(QueryReader *r);

/* Reads next SQL statement from its stream. A statement ends when a semicolon
 * ';' is found that is not inside:
 *  - single quotes
 *  - double quotes
 *
 * Any non-whitespace bytes after the semicolon stay in the stash for the next
 * iteration, which allows multiple statements on the same line.
 *
 * Returns:
 *  YES -> success, *out_sql is malloc'd.
 *  NO  -> clean EOF with no pending statement.
 *  ERR -> error (malloc/read). */
int query_reader_read_sql(QueryReader *r, char **out_sql);

#endif
