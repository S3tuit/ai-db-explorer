#ifndef BROKER_H
#define BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "byte_channel.h"
#include "bufio.h"
#include "db_backend.h"
#include "security_context.h"

/* The entity responsible for connecting to databases and running the commands
 * of the clients. */
typedef struct Broker Broker;

/* This entity stores the usefull data to communicate with each Client. */
typedef struct BrokerClientSession {
    BufReader br;
    BufWriter bw;

    /* Session state */
    char *current_dbname;   // owned string, may be NULL
    DbBackend *db;          // owned, may be NULL
} BrokerClientSession;

/* Create/destroy broker. */
Broker *broker_create();
void broker_destroy(Broker *b);

/* Run broker event loop (blocking).
 * Returns OK on clean stop, ERR on fatal error.
 *
 * TODO: now it runs forever until an unrecoverable error occurs. Sdd a stop
 * flag / signal handling.
 */
int broker_run(Broker *b);

#endif
