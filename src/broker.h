#ifndef BROKER_H
#define BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "byte_channel.h"
#include "bufio.h"
#include "db_backend.h"
#include "conn_manager.h"


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

/* Creates a Broker. If sock_path is NULL, it uses the default. The Broker
 * takes ownership of 'cm'. */
Broker *broker_create(const char *sock_path, ConnManager *cm);

/* Frees 'b' and its owned entities. */
void broker_destroy(Broker *b);

#endif
