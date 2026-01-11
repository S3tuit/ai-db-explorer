#ifndef BROKER_H
#define BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "byte_channel.h"
#include "bufio.h"
#include "db_backend.h"
#include "security_context.h"


#define SOCK_PATH "./build/aidbexplorer.sock"

/* The entity responsible for connecting to databases and running the commands
 * of the clients. */
typedef struct Broker Broker;

/* This entity stores the usefull data to communicate with each Client. */
typedef struct BrokerMcpSession {
    BufChannel bc;
    int fd;              // connection identity (owned by bc)
} BrokerMcpSession;

/* Run broker event loop (blocking).
 * Returns OK on clean stop, ERR on fatal error.
 *
 * TODO: now it runs forever until an unrecoverable error occurs. Sdd a stop
 * flag / signal handling.
 */
int broker_run(Broker *b);

// Valid commands for the Broker
#define BROK_EXEC_CMD "exec"

/* Create/destroy broker. If sock_path is NULL, it uses the default. */
Broker *broker_create(const char *sock_path);
void broker_destroy(Broker *b);

#endif
