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

/* Writes <n><n bytes from 'payload'> with big-endian uint32 length prefix. */
AdbxStatus frame_write_len(BufChannel *bc, const void *payload, uint32_t n);

/* Reads <n><n bytes> with big-endian uint32 length prefix and stores them into
 * 'out_payload'.
 * Returns OK on success, ERR on malformed frame, overflow, or I/O error. */
AdbxStatus frame_read_len(BufChannel *bc, StrBuf *out_payload);

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
