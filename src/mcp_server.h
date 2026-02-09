#ifndef MCP_SERVER_H
#define MCP_SERVER_H

#include <stdint.h>
#include <stdio.h>

#include "bufio.h"

typedef struct McpServer {
  BufChannel *in_bc;   // owned, used to read from user
  BufChannel *brok_bc; // owned, used to communicate with the Broker
  BufChannel *out_bc;  // owned, used to write to user
  char last_err[256];  // last fatal error (best-effort)
} McpServer;

/* Initializes the McpServer to read from 'in', write to 'out', and talk to the
 * broker at 'sock_path'. The McpServer doesn't take ownership of 'in' and
 * 'out', so it won't close them. Returns OK/ERR. */
int mcpser_init(McpServer *s, FILE *in, FILE *out, const char *sock_path);

/* Runs the main loop until EOF. Returns OK on clean EOF, ERR on fatal error.
 * This handle errors logging to the user via JSONRPC and via stderr. If
 * a fatal error occurs, set 'c'->last_err and returns. */
int mcpser_run(McpServer *s);

/* Cleans owned resources (channels). Safe to call multiple times. */
void mcpser_clean(McpServer *s);

/* Returns a pointer to the last fatal error message (empty if none). */
const char *mcpser_last_error(const McpServer *s);

#endif
