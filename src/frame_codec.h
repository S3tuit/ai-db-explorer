#ifndef FRAME_CODEC_H
#define FRAME_CODEC_H

#include <stdint.h>
#include <stddef.h>

#include "bufio.h"
#include "string_op.h"

/* Writes <n><n bytes from 'payload'> with big-endian uint32 length prefix. */
int frame_write_len(BufChannel *bc, const void *payload, uint32_t n);

/* Reads <n><n bytes> with big-endian uint32 length prefix and stores them into
 * 'out_payload'.
 * Returns OK on success, ERR on malformed frame, overflow, or I/O error. */
int frame_read_len(BufChannel *bc, StrBuf *out_payload);

/* Writes Content-Length framed payload:
 * "Content-Length: <n>\r\n\r\n" + payload.
 * NOTE: use this only to write things the user should see. */
int frame_write_cl(BufChannel *bc, const void *payload, size_t n);

#endif
