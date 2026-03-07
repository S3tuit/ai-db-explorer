#ifndef BROKER_H
#define BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "bufio.h"
#include "byte_channel.h"
#include "conn_manager.h"
#include "db_backend.h"
#include "handshake_codec.h"
#include "private_dir.h"

// TODO: we should be able to accept more than 1 McpServer with just one Broker.
// We should make the code async, and use a real connection pool.
#define MAX_CLIENTS 1
#define MAX_IDLE_SESSIONS (MAX_CLIENTS * 2)

/* The entity is responsible for connecting to databases and running the
 * commands of the clients. */
typedef struct Broker Broker;

/* Run broker event loop (blocking).
 * Returns OK on clean stop, ERR on fatal error.
 *
 * TODO: now it runs forever until an unrecoverable error occurs. Sdd a stop
 * flag / signal handling.
 */
AdbxStatus broker_run(Broker *b);

/* Creates a Broker rooted at 'pd'. The Broker takes ownership of 'cm' and
 * internally acquires an owned private-dir runtime plus shared secret token. */
Broker *broker_create(const PrivDir *pd, ConnManager *cm);

/* Frees 'b' and its owned entities. */
void broker_destroy(Broker *b);

#endif
