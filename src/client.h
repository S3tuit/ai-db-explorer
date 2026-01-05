#ifndef CLIENT_H
#define CLIENT_H

#include <stdint.h>
#include <stdio.h>

#include "bufio.h"
#include "command_reader.h"

typedef struct Client {
    CommandReader *r;    // owned
    BufChannel *brok_bc; // owned, used to communicate with the Broker
    BufChannel *out_bc;  // owned, used to write to user
    uint32_t next_id;    // monotonically increasing request id
    char last_err[256];  // last fatal error (best-effort)
} Client;

/* Initializes the Client to read from 'in', write to 'out', and talk to the
 * broker at 'sock_path'. The Client doesn't take ownership of 'in' and 'out',
 * so it won't close them. Returns OK/ERR. */
int client_init(Client *c, FILE *in, FILE *out, const char *sock_path);

/* Runs the main loop until EOF. Returns OK on clean EOF, ERR on fatal error.
 * This handle errors logging to the user via JSONRPC and via stderr. If
 * a fatal error occurs, set 'c'->last_err and returns. */
int client_run(Client *c);

/* Cleans owned resources (reader/channels). Safe to call multiple times. */
void client_clean(Client *c);

/* Returns a pointer to the last fatal error message (empty if none). */
const char *client_last_error(const Client *c);

#endif
