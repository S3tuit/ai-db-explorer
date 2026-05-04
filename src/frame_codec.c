#include "frame_codec.h"
#include "utils.h"

#include <arpa/inet.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

AdbxStatus frame_write_len(BufChannel *bc, const void *payload,
                           uint32_t hostlong) {
  if (!bc)
    return ERR;
  if (!payload && hostlong != 0)
    return ERR;

  uint32_t hdr = htonl(hostlong);

  return bufch_write2v(bc, &hdr, sizeof(hdr), payload, (size_t)hostlong);
}

FrameReadLenStatus frame_read_len(BufChannel *bc, StrBuf *out_payload,
                                  size_t max_payload_len) {
  if (!bc || !out_payload)
    return FRAME_READ_LEN_ERR_INPUT;

  out_payload->len = 0;

  // read first 4 bytes
  uint32_t netlong;
  if (bufch_read_exact(bc, &netlong, sizeof(netlong)) != OK)
    return FRAME_READ_LEN_ERR_IO;

  uint32_t n = ntohl(netlong);

  if (n > STRBUF_MAX_BYTES)
    return FRAME_READ_LEN_ERR_OVERSIZE;
  if (max_payload_len != 0 && (size_t)n > max_payload_len)
    return FRAME_READ_LEN_ERR_OVERSIZE;
#if SIZE_MAX < UINT32_MAX
  if (n > SIZE_MAX)
    return FRAME_READ_LEN_ERR_OVERSIZE;
#endif

  if (n == 0)
    return FRAME_READ_LEN_OK;

  char *dst = NULL;
  if (sb_prepare_for_write(out_payload, (size_t)n, &dst) != OK) {
    return FRAME_READ_LEN_ERR_BUFFER;
  }
  if (bufch_read_exact(bc, (unsigned char *)dst, (size_t)n) != OK)
    return FRAME_READ_LEN_ERR_IO;
  return FRAME_READ_LEN_OK;
}

/* Writes Content-Length framed payload:
 * "Content-Length: <n>\r\n\r\n" + payload.
 * NOTE: use this only to write things the user should see. */
static AdbxStatus frame_write_cl(BufChannel *bc, const void *payload,
                                 size_t n) {
  if (!bc)
    return ERR;
  if (!payload && n != 0)
    return ERR;

  char hdr[64];
  // since we write in ASCII digits endianness is irrelevant
  int rc = snprintf(hdr, sizeof(hdr), "Content-Length: %zu\r\n\r\n", n);
  if (rc < 0 || (size_t)rc >= sizeof(hdr))
    return ERR;

  return bufch_write2v(bc, hdr, (size_t)rc, payload, n);
}

/* Writes one newline-delimited JSON payload.
 * It borrows 'bc' and 'payload'.
 * Side effects: writes 'payload' plus one trailing '\n'.
 * Return conventions: returns OK on success, ERR on invalid input or write
 * failure.
 */
static AdbxStatus frame_write_jsonl(BufChannel *bc, const void *payload,
                                    size_t n) {
  static const char nl = '\n';

  if (!bc)
    return ERR;
  if (!payload && n != 0)
    return ERR;

  return bufch_write2v(bc, payload, n, &nl, sizeof(nl));
}

/* Returns YES when 'ch' is optional HTTP-style whitespace accepted around
 * header values, NO otherwise.
 */
static int frame_is_header_ws(char ch) {
  return (ch == ' ' || ch == '\t') ? YES : NO;
}

/* Lowercases one ASCII byte without locale effects so header matching stays
 * deterministic across environments.
 * Returns the lowercased ASCII byte, or 'ch' unchanged for non-uppercase bytes.
 */
static char frame_ascii_tolower(char ch) {
  if (ch >= 'A' && ch <= 'Z')
    return (char)(ch - 'A' + 'a');
  return ch;
}

/* Compares one header name span against 'expected' using ASCII-only
 * case-insensitive matching.
 * Return conventions: YES when the spans match, NO when they differ or inputs
 * are invalid.
 */
static int frame_header_name_eq(const char *name, size_t name_len,
                                const char *expected) {
  if (!name || !expected)
    return NO;

  size_t expected_len = strlen(expected);
  if (name_len != expected_len)
    return NO;

  for (size_t i = 0; i < name_len; i++) {
    if (frame_ascii_tolower(name[i]) != frame_ascii_tolower(expected[i]))
      return NO;
  }
  return YES;
}

/* Parses one decimal Content-Length value from a single header line value span.
 * It borrows 'src' and writes the validated payload size into caller-owned
 * 'out_len'.
 * Returns OK on a valid non-negative decimal length that fits local limits, ERR
 * on malformed syntax, overflow, or unsupported size.
 */
static AdbxStatus frame_parse_content_length_value(const char *src, size_t len,
                                                   size_t *out_len) {
  if (!src || !out_len)
    return ERR;
  *out_len = 0;

  size_t i = 0;
  while (i < len && frame_is_header_ws(src[i]) == YES)
    i++;
  if (i == len)
    return ERR;

  unsigned long long v = 0;
  int saw_digit = NO;
  while (i < len && src[i] >= '0' && src[i] <= '9') {
    unsigned digit = (unsigned)(src[i] - '0');
    if (v > (ULLONG_MAX - digit) / 10ULL)
      return ERR;
    v = (v * 10ULL) + digit;
    saw_digit = YES;
    i++;
  }
  if (saw_digit != YES)
    return ERR;

  while (i < len && frame_is_header_ws(src[i]) == YES)
    i++;
  if (i != len)
    return ERR;

  if (v > STRBUF_MAX_BYTES)
    return ERR;
  if (v > SIZE_MAX)
    return ERR;

  *out_len = (size_t)v;
  return OK;
}

/* Parses one complete Content-Length header block terminated by CRLFCRLF.
 * It borrows 'hdr' and writes the unique validated payload size into
 * caller-owned 'out_len'.
 * Returns OK on exactly one valid Content-Length header, ERR on malformed
 * lines, missing/duplicate Content-Length, overflow, or other framing
 * violations.
 */
static AdbxStatus parse_content_length(const char *hdr, size_t len,
                                       size_t *out_len) {
  if (!hdr || !out_len)
    return ERR;
  *out_len = 0;
  if (len < 4)
    return ERR;

  int seen_content_length = NO;
  size_t pos = 0;
  while (pos < len) {
    size_t line_end = pos;
    while ((line_end + 1) < len &&
           !(hdr[line_end] == '\r' && hdr[line_end + 1] == '\n')) {
      line_end++;
    }
    if ((line_end + 1) >= len)
      return ERR;

    // The first blank line must be the CRLF that terminates the header block.
    if (line_end == pos)
      return ((pos + 2) == len && seen_content_length == YES) ? OK : ERR;

    size_t colon = pos;
    while (colon < line_end && hdr[colon] != ':')
      colon++;
    if (colon == pos || colon == line_end)
      return ERR;

    if (frame_header_name_eq(hdr + pos, colon - pos, "Content-Length") == YES) {
      if (seen_content_length == YES)
        return ERR;
      if (frame_parse_content_length_value(
              hdr + colon + 1, line_end - (colon + 1), out_len) != OK) {
        return ERR;
      }
      seen_content_length = YES;
    }

    pos = line_end + 2;
  }

  return ERR;
}

/* Reads one newline-delimited JSON payload, trimming the trailing line ending.
 * It borrows 'bc' and writes payload bytes into caller-owned 'out_payload'.
 * Side effects: reads from the underlying channel, consumes exactly one
 * newline-terminated frame, and skips blank lines.
 * Return conventions: returns YES on a non-empty line payload, NO on clean EOF
 * before a new frame, ERR on oversized or truncated lines.
 */
static AdbxTriStatus frame_read_jsonl(BufChannel *bc, StrBuf *out_payload) {
  if (!bc || !out_payload)
    return ERR;

  out_payload->len = 0;
  sb_clean(out_payload);

  const size_t max_line_bytes = 1u << 20;
  for (;;) {
    ssize_t idx = bufch_find_buffered(bc, "\n", 1);
    if (idx >= 0) {
      size_t line_len = (size_t)idx + 1u;
      char *dst = NULL;
      if (sb_prepare_for_write(out_payload, line_len, &dst) != OK)
        return ERR;
      if (bufch_read_exact(bc, dst, line_len) != OK) {
        sb_clean(out_payload);
        return ERR;
      }

      size_t payload_len = line_len;
      if (payload_len > 0 && dst[payload_len - 1] == '\n')
        payload_len--;
      if (payload_len > 0 && dst[payload_len - 1] == '\r')
        payload_len--;

      out_payload->len = payload_len;
      if (payload_len == 0) {
        sb_clean(out_payload);
        continue;
      }
      return YES;
    }

    size_t avail = 0;
    (void)bufch_peek(bc, &avail);
    if (avail >= max_line_bytes)
      return ERR;

    AdbxTriStatus rc = bufch_ensure(bc, avail + 1u);
    if (rc == YES)
      continue;
    if (rc == NO)
      return (avail == 0) ? NO : ERR;
    return ERR;
  }
}

/* Detects whether the next MCP stdio frame uses JSONL or Content-Length
 * framing.
 * It borrows 'bc' and peeks buffered bytes without consuming them.
 * Side effects: may read from the underlying channel to inspect the first
 * payload byte.
 * Return conventions: returns YES on successful detection and writes the style
 * into 'out_style', NO on clean EOF before a frame, ERR on invalid input or
 * read failure.
 */
static AdbxTriStatus frame_detect_rpc_style(BufChannel *bc,
                                            FrameRpcStyle *out_style) {
  if (!bc || !out_style)
    return ERR;
  *out_style = FRAME_RPC_STYLE_UNKNOWN;

  AdbxTriStatus rc = bufch_ensure(bc, 1);
  if (rc != YES)
    return rc;

  size_t avail = 0;
  const uint8_t *buf = bufch_peek(bc, &avail);
  if (!buf || avail == 0)
    return ERR;

  if (buf[0] == '{' || buf[0] == '[') {
    *out_style = FRAME_RPC_STYLE_JSONL;
    return YES;
  }

  *out_style = FRAME_RPC_STYLE_CONTENT_LENGTH;
  return YES;
}

/* Reads Content-Length framed payload into out_payload.
 * Returns YES on success, NO on clean EOF before header, ERR on framing error.
 */
static AdbxTriStatus frame_read_cl(BufChannel *bc, StrBuf *out_payload) {
  if (!bc || !out_payload)
    return ERR;
  out_payload->len = 0;
  // Ensure no previous allocation leaks when reusing the StrBuf.
  sb_clean(out_payload);

  // Allow reasonable stdio header growth (for example Content-Type or trace
  // headers) while still bounding memory and scan work.
  const size_t max_hdr_scan = 4096;
  size_t hdr_len = 0;
  for (;;) {
    ssize_t idx = bufch_find_buffered(bc, "\r\n\r\n", 4);
    if (idx >= 0) {
      hdr_len = (size_t)idx + 4;
      break;
    }

    size_t avail = 0;
    (void)bufch_peek(bc, &avail);
    if (avail >= max_hdr_scan)
      return ERR;

    // Grow the buffered search window incrementally so live pipes do not block
    // waiting for the entire max_hdr_scan quota before we scan for CRLFCRLF.
    AdbxTriStatus rc = bufch_ensure(bc, avail + 1u);
    if (rc == YES)
      continue;
    if (rc == NO)
      return (avail == 0) ? NO : ERR;
    return ERR;
  }

  char *hdr = xmalloc(hdr_len + 1);
  if (bufch_read_exact(bc, hdr, hdr_len) != OK) {
    free(hdr);
    return ERR;
  }
  hdr[hdr_len] = '\0';

  size_t payload_len = 0;
  AdbxStatus prc = parse_content_length(hdr, hdr_len, &payload_len);
  free(hdr);
  if (prc != OK)
    return ERR;

  if (payload_len == 0)
    return YES;

  char *dst = NULL;
  if (sb_prepare_for_write(out_payload, payload_len, &dst) != OK) {
    return ERR;
  }
  if (bufch_read_exact(bc, dst, payload_len) != OK) {
    sb_clean(out_payload);
    return ERR;
  }
  return YES;
}

AdbxStatus frame_write_rpc(BufChannel *bc, const void *payload, size_t n,
                           FrameRpcStyle style) {
  if (style == FRAME_RPC_STYLE_JSONL)
    return frame_write_jsonl(bc, payload, n);
  return frame_write_cl(bc, payload, n);
}

AdbxTriStatus frame_read_rpc(BufChannel *bc, StrBuf *out_payload,
                             FrameRpcStyle *out_style) {
  if (!bc || !out_payload || !out_style)
    return ERR;

  FrameRpcStyle style = FRAME_RPC_STYLE_UNKNOWN;
  AdbxTriStatus rc = frame_detect_rpc_style(bc, &style);
  if (rc != YES)
    return rc;

  *out_style = style;
  if (style == FRAME_RPC_STYLE_JSONL)
    return frame_read_jsonl(bc, out_payload);
  return frame_read_cl(bc, out_payload);
}
