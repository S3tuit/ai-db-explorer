#ifndef BROKER_H
#define BROKER_H

#include <stddef.h>
#include <stdint.h>

#include "bufio.h"
#include "byte_channel.h"
#include "conn_manager.h"
#include "db_backend.h"

/*-------------------------------- Handshake --------------------------------*/

#define HANDSHAKE_MAGIC 0x4D435042 /* "MCPB" */
#define HANDSHAKE_VERSION 1
#define SECRET_TOKEN_LEN 32 /* 256-bit random token */
#define RESUME_TOKEN_LEN 32

typedef struct {
  uint32_t magic;
  uint16_t version;
  uint16_t flags;                         /* bit 0: has_resume_token */
  uint8_t resume_token[RESUME_TOKEN_LEN]; /* zero-filled if new session */
  uint8_t secret_token[RESUME_TOKEN_LEN]; /* required during handshake */
} handshake_req_t;

#define HANDSHAKE_FLAG_RESUME (1u << 0)

typedef enum {
  HS_OK = 0,
  HS_ERR_BAD_MAGIC = 1,
  HS_ERR_BAD_VERSION = 2,
  HS_ERR_TOKEN_EXPIRED = 3,
  HS_ERR_TOKEN_UNKNOWN = 4,
  HS_ERR_FULL = 5, /* MAX_CLIENTS reached */
  HS_ERR_INTERNAL = 6,
} handshake_status;

typedef struct {
  uint32_t magic;
  uint16_t version;
  handshake_status status;                /* 0 = OK, nonzero = error code */
  uint8_t resume_token[RESUME_TOKEN_LEN]; /* the (new or resumed) token */
  uint32_t idle_ttl_secs;                 /* server communicates policy back */
  uint32_t abs_ttl_secs;
} handshake_resp_t;

/*---------------------------------------------------------------------------*/

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

/* Creates a Broker. If sock_path is NULL, it uses the default. The Broker
 * takes ownership of 'cm'. */
Broker *broker_create(const char *sock_path, ConnManager *cm);

/* Frees 'b' and its owned entities. */
void broker_destroy(Broker *b);

/* Returns the number of active sessions (connected clients being polled). */
size_t broker_active_count(const Broker *b);

/* Returns the number of idle sessions (disconnected but resumable). */
size_t broker_idle_count(const Broker *b);

#endif
