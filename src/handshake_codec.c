#include "handshake_codec.h"

#include "utils.h"

#include <string.h>

/* Writes a 16-bit unsigned integer using big-endian byte order.
 * It borrows 'dst' and does not allocate memory.
 * Side effects: writes 2 bytes to destination buffer.
 * Error semantics: none.
 */
static void write_be16(uint8_t *dst, uint16_t v) {
  dst[0] = (uint8_t)((v >> 8) & 0xFFu);
  dst[1] = (uint8_t)(v & 0xFFu);
}

/* Writes a 32-bit unsigned integer using big-endian byte order.
 * It borrows 'dst' and does not allocate memory.
 * Side effects: writes 4 bytes to destination buffer.
 * Error semantics: none.
 */
static void write_be32(uint8_t *dst, uint32_t v) {
  dst[0] = (uint8_t)((v >> 24) & 0xFFu);
  dst[1] = (uint8_t)((v >> 16) & 0xFFu);
  dst[2] = (uint8_t)((v >> 8) & 0xFFu);
  dst[3] = (uint8_t)(v & 0xFFu);
}

/* Reads a 16-bit unsigned integer from big-endian bytes.
 * It borrows 'src' and does not allocate memory.
 * Side effects: none.
 * Error semantics: none.
 */
static uint16_t read_be16(const uint8_t *src) {
  return (uint16_t)(((uint16_t)src[0] << 8) | (uint16_t)src[1]);
}

/* Reads a 32-bit unsigned integer from big-endian bytes.
 * It borrows 'src' and does not allocate memory.
 * Side effects: none.
 * Error semantics: none.
 */
static uint32_t read_be32(const uint8_t *src) {
  return ((uint32_t)src[0] << 24) | ((uint32_t)src[1] << 16) |
         ((uint32_t)src[2] << 8) | (uint32_t)src[3];
}

/* Maps in-memory handshake status enum into fixed-width wire code.
 * It borrows 'out_code' and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns OK for known statuses, ERR for invalid inputs.
 */
static int handshake_status_to_wire(handshake_status st, uint16_t *out_code) {
  if (!out_code)
    return ERR;

  switch (st) {
  case HS_OK:
  case HS_ERR_BAD_MAGIC:
  case HS_ERR_BAD_VERSION:
  case HS_ERR_TOKEN_EXPIRED:
  case HS_ERR_TOKEN_UNKNOWN:
  case HS_ERR_FULL:
  case HS_ERR_REQ:
  case HS_ERR_INTERNAL:
    *out_code = (uint16_t)st;
    return OK;
  default:
    return ERR;
  }
}

/* Maps fixed-width wire status code back to handshake status enum.
 * It borrows 'out_status' and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns OK for known codes, ERR for invalid inputs.
 */
static int handshake_status_from_wire(uint16_t code,
                                      handshake_status *out_status) {
  if (!out_status)
    return ERR;

  switch (code) {
  case HS_OK:
  case HS_ERR_BAD_MAGIC:
  case HS_ERR_BAD_VERSION:
  case HS_ERR_TOKEN_EXPIRED:
  case HS_ERR_TOKEN_UNKNOWN:
  case HS_ERR_FULL:
  case HS_ERR_REQ:
  case HS_ERR_INTERNAL:
    *out_status = (handshake_status)code;
    return OK;
  default:
    return ERR;
  }
}

int handshake_req_encode(const handshake_req_t *req,
                         uint8_t out[HANDSHAKE_REQ_WIRE_SIZE]) {
  if (!req || !out)
    return ERR;

  write_be32(out + 0, req->magic);
  write_be16(out + 4, req->version);
  write_be16(out + 6, req->flags);
  memcpy(out + 8, req->resume_token, RESUME_TOKEN_LEN);
  memcpy(out + 8 + RESUME_TOKEN_LEN, req->secret_token, SECRET_TOKEN_LEN);
  return OK;
}

int handshake_req_decode(handshake_req_t *out, const uint8_t *wire,
                         size_t wire_len) {
  if (!out || !wire)
    return ERR;
  if (wire_len != HANDSHAKE_REQ_WIRE_SIZE)
    return ERR;

  memset(out, 0, sizeof(*out));
  out->magic = read_be32(wire + 0);
  out->version = read_be16(wire + 4);
  out->flags = read_be16(wire + 6);
  memcpy(out->resume_token, wire + 8, RESUME_TOKEN_LEN);
  memcpy(out->secret_token, wire + 8 + RESUME_TOKEN_LEN, SECRET_TOKEN_LEN);
  return OK;
}

int handshake_resp_encode(const handshake_resp_t *resp,
                          uint8_t out[HANDSHAKE_RESP_WIRE_SIZE]) {
  if (!resp || !out)
    return ERR;

  uint16_t status_code = 0;
  if (handshake_status_to_wire(resp->status, &status_code) != OK)
    return ERR;

  write_be32(out + 0, resp->magic);
  write_be16(out + 4, resp->version);
  write_be16(out + 6, status_code);
  memcpy(out + 8, resp->resume_token, RESUME_TOKEN_LEN);
  write_be32(out + 8 + RESUME_TOKEN_LEN, resp->idle_ttl_secs);
  write_be32(out + 12 + RESUME_TOKEN_LEN, resp->abs_ttl_secs);
  return OK;
}

int handshake_resp_decode(handshake_resp_t *out, const uint8_t *wire,
                          size_t wire_len) {
  if (!out || !wire)
    return ERR;
  if (wire_len != HANDSHAKE_RESP_WIRE_SIZE)
    return ERR;

  memset(out, 0, sizeof(*out));
  out->magic = read_be32(wire + 0);
  out->version = read_be16(wire + 4);

  uint16_t status_code = read_be16(wire + 6);
  if (handshake_status_from_wire(status_code, &out->status) != OK)
    return ERR;

  memcpy(out->resume_token, wire + 8, RESUME_TOKEN_LEN);
  out->idle_ttl_secs = read_be32(wire + 8 + RESUME_TOKEN_LEN);
  out->abs_ttl_secs = read_be32(wire + 12 + RESUME_TOKEN_LEN);
  return OK;
}
