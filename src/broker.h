#ifndef BROKER_H
#define BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "bufio.h"
#include "byte_channel.h"
#include "conn_manager.h"
#include "db_backend.h"
#include "handshake_codec.h"

/* The entity is responsible for connecting to databases and running the
 * commands of the clients. */
typedef struct Broker Broker;

// TODO: we should be able to accept more than 1 McpServer with just one Broker.
// We should make the code async, and use a real connection pool.
#define MAX_CLIENTS 1
#define MAX_IDLE_SESSIONS (MAX_CLIENTS * 2)

#define ABSOLUTE_TTL (8 * 60 * 60) // 8 hours
#define IDLE_TTL (20 * 60)         // 20 minutes

/* This entity stores the usefull data to communicate with each Client. */
typedef struct BrokerMcpSession {
  BufChannel bc;
  int fd; // connection identity (owned by bc). -1 if disconnected but resumable
  uint8_t resume_token[RESUME_TOKEN_LEN]; // can be used to resume the session
  time_t created_at;                      // for absolute TTL
  time_t last_active;                     // for idle TTL
} BrokerMcpSession;

/* Run broker event loop (blocking).
 * Returns OK on clean stop, ERR on fatal error.
 *
 * TODO: now it runs forever until an unrecoverable error occurs. Sdd a stop
 * flag / signal handling.
 */
int broker_run(Broker *b);

/* Creates a Broker. The Broker takes ownership of 'cm'.
 * 'secret_token' is copied internally and used for handshake verification.
 * In test builds (ADBX_TEST_MODE), secret_token may be NULL to skip auth.
 * In production builds, NULL secret_token causes broker_create to fail. */
Broker *broker_create(const char *sock_path, ConnManager *cm,
                      const uint8_t *secret_token);

/* Frees 'b' and its owned entities. */
void broker_destroy(Broker *b);

/* Returns the number of active sessions (connected clients being polled). */
size_t broker_active_count(const Broker *b);

/* Returns the number of idle sessions (disconnected but resumable). */
size_t broker_idle_count(const Broker *b);

#endif
