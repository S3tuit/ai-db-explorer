#ifndef FRAME_CODEC_H
#define FRAME_CODEC_H

#include <stddef.h>
#include <stdint.h>

#include "bufio.h"
#include "string_op.h"
#include "utils.h"

/* MCP Hosts use different framing encoding;
 * - Codex uses newline-delimited JSON on stdio.
 * - Others use Content-Length framing. */
typedef enum {
  FRAME_RPC_STYLE_UNKNOWN = 0,
  FRAME_RPC_STYLE_CONTENT_LENGTH,
  FRAME_RPC_STYLE_JSONL,
} FrameRpcStyle;

typedef enum {
  FRAME_READ_LEN_OK = 0,
  FRAME_READ_LEN_ERR_INPUT = -1,
  FRAME_READ_LEN_ERR_IO = -2,
  FRAME_READ_LEN_ERR_OVERSIZE = -3,
  FRAME_READ_LEN_ERR_BUFFER = -4,
} FrameReadLenStatus;

/* Writes <n><n bytes from 'payload'> with big-endian uint32 length prefix. */
AdbxStatus frame_write_len(BufChannel *bc, const void *payload, uint32_t n);

/* Reads <n><n bytes> with big-endian uint32 length prefix and stores them into
 * 'out_payload'. When 'max_payload_len' is non-zero, frames larger than that
 * cap are rejected immediately after reading the length prefix and before
 * reading or allocating the payload body. The codec-level STRBUF_MAX_BYTES
 * bound is always enforced.
 */
FrameReadLenStatus frame_read_len(BufChannel *bc, StrBuf *out_payload,
                                  size_t max_payload_len);

/* Writes one MCP stdio payload using the requested framing style.
 * CONTENT_LENGTH writes a Content-Length header block; JSONL writes the payload
 * followed by '\n'.
 * Returns OK on success, ERR on invalid input or write failure.
 */
AdbxStatus frame_write_rpc(BufChannel *bc, const void *payload, size_t n,
                           FrameRpcStyle style);

/* Reads one MCP stdio payload while auto-detecting transport framing.
 * JSON objects/arrays beginning the frame are treated as newline-delimited
 * JSON; everything else is parsed as Content-Length framing.
 * On success, stores the payload in 'out_payload' and the detected style in
 * 'out_style'. Returns YES on success, NO on clean EOF before a new frame,
 * ERR on malformed framing.
 */
AdbxTriStatus frame_read_rpc(BufChannel *bc, StrBuf *out_payload,
                             FrameRpcStyle *out_style);

#endif
