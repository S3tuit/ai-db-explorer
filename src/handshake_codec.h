#ifndef HANDSHAKE_CODEC_H
#define HANDSHAKE_CODEC_H

#include <stddef.h>
#include <stdint.h>

#define MCP_PROTOCOL_VERSION "2025-11-25"

/* Shared token length used by broker handshake secret and private-dir token
 * file representation. */
#define ADBX_SHARED_TOKEN_LEN 32u

/* Shared resume-token length used by broker and MCP-side token cache. */
#define ADBX_RESUME_TOKEN_LEN 32u

#define HANDSHAKE_MAGIC 0x4D435042u /* "MCPB" */
#define HANDSHAKE_VERSION 1

#define SECRET_TOKEN_LEN ADBX_SHARED_TOKEN_LEN /* 256-bit random token */
#define RESUME_TOKEN_LEN ADBX_RESUME_TOKEN_LEN

/* Handshake scalar fields on the wire are always encoded big-endian.
 * Byte arrays (tokens) are copied as raw bytes.
 */
#define HANDSHAKE_REQ_WIRE_SIZE                                                \
  (4u + 2u + 2u + RESUME_TOKEN_LEN + SECRET_TOKEN_LEN)
#define HANDSHAKE_RESP_WIRE_SIZE (4u + 2u + 2u + RESUME_TOKEN_LEN + 4u + 4u)

#define HANDSHAKE_FLAG_RESUME (1u << 0)

typedef struct {
  uint32_t magic;
  uint16_t version;
  uint16_t flags;                         /* bit 0: has_resume_token */
  uint8_t resume_token[RESUME_TOKEN_LEN]; /* ignored if has_resume_token is not
                                             set */
  uint8_t secret_token[SECRET_TOKEN_LEN]; /* required during handshake */
} handshake_req_t;

typedef enum {
  HS_OK = 0,
  HS_ERR_BAD_MAGIC = 1,
  HS_ERR_BAD_VERSION = 2,
  HS_ERR_TOKEN_EXPIRED = 3,
  HS_ERR_TOKEN_UNKNOWN = 4,
  HS_ERR_FULL = 5, /* MAX_CLIENTS reached */
  HS_ERR_REQ = 6,  /* malformed handshake request */
  HS_ERR_INTERNAL = 7,
} handshake_status;

typedef struct {
  uint32_t magic;
  uint16_t version;
  handshake_status status; /* 0 = OK, nonzero = error code (in-memory enum) */
  uint8_t resume_token[RESUME_TOKEN_LEN]; /* the (new or resumed) token */
  uint32_t idle_ttl_secs;                 /* server communicates policy back */
  uint32_t abs_ttl_secs;
} handshake_resp_t;
/* Encodes request into fixed-size big-endian wire format.
 * It borrows 'req' and writes exactly HANDSHAKE_REQ_WIRE_SIZE bytes to 'out'.
 * Returns OK on success, ERR on invalid input.
 */
int handshake_req_encode(const handshake_req_t *req,
                         uint8_t out[HANDSHAKE_REQ_WIRE_SIZE]);

/* Decodes request from fixed-size big-endian wire format.
 * It borrows 'wire' and writes decoded values to caller-owned 'out'.
 * Returns OK on success, ERR on invalid input or malformed size.
 */
int handshake_req_decode(handshake_req_t *out, const uint8_t *wire,
                         size_t wire_len);

/* Encodes response into fixed-size big-endian wire format.
 * It borrows 'resp' and writes exactly HANDSHAKE_RESP_WIRE_SIZE bytes to 'out'.
 * Returns OK on success, ERR on invalid input or unknown status code.
 */
int handshake_resp_encode(const handshake_resp_t *resp,
                          uint8_t out[HANDSHAKE_RESP_WIRE_SIZE]);

/* Decodes response from fixed-size big-endian wire format.
 * It borrows 'wire' and writes decoded values to caller-owned 'out'.
 * Returns OK on success, ERR on invalid input, malformed size, or unknown
 * status code.
 */
int handshake_resp_decode(handshake_resp_t *out, const uint8_t *wire,
                          size_t wire_len);

#endif
