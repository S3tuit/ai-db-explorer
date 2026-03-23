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

// This is an admission cap for the single threaded broker, not really a
// concurrency option. Right now, the bottleneck of concurrency is probably db
// calls. Each client request makes the broker wait for the db call to finish
// even if the broker could do something else in the meantime. Also, we don't
// really use a pool of connections right now; we just keep a pool of one
// connection per connectionName (see conn_manager).
#define MAX_CLIENTS 4
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

/* Creates a Broker rooted at 'pd'. On success the Broker takes ownership of
 * 'cm' and internally acquires an owned private-dir runtime plus shared secret
 * token. On failure ownership of 'cm' remains with caller. */
Broker *broker_create(const PrivDir *pd, ConnManager *cm);

/* Frees 'b' and its owned entities. */
void broker_destroy(Broker *b);

#endif
