#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include <stdint.h>
#include <stdio.h>

#include "query_reader.h"
#include "bufio.h"
#include "frame_codec.h"
#include "db_backend.h"

/* This is the entity that owns the client-facing flow. It own who reads and
 * writes a given client. It also knows (but not owns) who is responsible to
 * execute queries for that client. */
typedef struct SessionManager {
    QueryReader *r;         // owned
    BufWriter *out_bw;      // owned
    DbBackend *db;          // not owned (caller owns)
    uint32_t next_id;       // monotonically increasing request id
    char last_err[256];     // last fatal error (best-effort)
} SessionManager;

/* Initializes the SessionManager to read from 'in' and write to 'out'.
 * The backend must already be created and connected. Returns OK on success,
 * ERR on bad input. */
int session_init(SessionManager *s, FILE *in, FILE *out, DbBackend *db);

/* Runs the main loop until EOF. Returns OK on clean EOF, ERR on fatal error. */
int session_run(SessionManager *s);

/* Cleans owned resources (reader/writer). Safe to call multiple times. */
void session_clean(SessionManager *s);

/* Returns a pointer to the last fatal error message (empty if none). */
const char *session_last_error(const SessionManager *s);

#endif
