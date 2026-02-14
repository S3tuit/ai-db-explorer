#ifndef MCP_SERVER_H
#define MCP_SERVER_H

#include <stdint.h>
#include <stdio.h>

#include "bufio.h"
#include "private_dir.h"
#include "resume_token.h"

typedef struct McpServerInit {
  FILE *in;             /* borrowed by McpServer */
  FILE *out;            /* borrowed by McpServer */
  const PrivDir *privd; /* borrowed */
} McpServerInit;

typedef struct McpServer {
  BufChannel in_bc;   // owned wrapper; underlying stdin fd is borrowed
  BufChannel brok_bc; // owned wrapper around broker socket
  BufChannel out_bc;  // owned wrapper; underlying stdout fd is borrowed
  const PrivDir *privd; /* borrowed runtime paths for lazy broker reconnect */
  ResumeTokenStore restok; /* owned process-scoped resume token store */
  uint32_t flags;          /* runtime state bits (MCPSER_F_*) */
  char last_err[256]; // last fatal error (best-effort)
} McpServer;

enum {
  MCPSER_F_BROKER_READY = 1u << 0,
};

/* Initializes the McpServer from 'init'. The McpServer borrows FILE handles
 * and does not close them. Broker connectivity is best-effort at init and is
 * retried lazily during request handling.
 * Returns OK/ERR. */
int mcpser_init(McpServer *s, const McpServerInit *init);

/* Runs the main loop until EOF. Returns OK on clean EOF, ERR on fatal error.
 * This handle errors logging to the user via JSONRPC and via stderr. If
 * a fatal error occurs, set 'c'->last_err and returns. */
int mcpser_run(McpServer *s);

/* Cleans owned resources (channels). Safe to call multiple times. */
void mcpser_clean(McpServer *s);

/* Returns a pointer to the last fatal error message (empty if none). */
const char *mcpser_last_error(const McpServer *s);

#endif
