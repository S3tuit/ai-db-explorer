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

AdbxStatus frame_read_len(BufChannel *bc, StrBuf *out_payload) {
  if (!bc || !out_payload)
    return ERR;

  // read first 4 bytes
  uint32_t netlong;
  if (bufch_read_exact(bc, &netlong, sizeof(netlong)) != OK)
    return ERR;

  uint32_t n = ntohl(netlong);

  if (n > STRBUF_MAX_BYTES)
    return ERR;
#if SIZE_MAX < UINT32_MAX
  if (n > SIZE_MAX)
    return ERR;
#endif

  out_payload->len = 0;
  if (n == 0)
    return OK;

  char *dst = NULL;
  if (sb_prepare_for_write(out_payload, (size_t)n, &dst) != OK) {
    return ERR;
  }
  if (bufch_read_exact(bc, (unsigned char *)dst, (size_t)n) != OK)
    return ERR;
  return OK;
}

AdbxStatus frame_write_cl(BufChannel *bc, const void *payload, size_t n) {
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

/* Parses Content-Length from a header. */
static AdbxStatus parse_content_length(const char *hdr, size_t len,
                                       size_t *out_len) {
  if (!hdr || !out_len)
    return ERR;
  *out_len = 0;
  (void)len;

  const char *needle = "Content-Length:";
  const char *p = strstr(hdr, needle);
  if (!p)
    return ERR;
  p += strlen(needle);
  while (*p == ' ' || *p == '\t')
    p++;

  char *endptr = NULL;
  unsigned long long v = strtoull(p, &endptr, 10);
  if (endptr == p)
    return ERR;

  // we can't handle these much bytes and makes no sense doing it
  if (v > STRBUF_MAX_BYTES)
    return ERR;
  if (v > SIZE_MAX)
    return ERR;

  *out_len = (size_t)v;
  return OK;
}

AdbxTriStatus frame_read_cl(BufChannel *bc, StrBuf *out_payload) {
  if (!bc || !out_payload)
    return ERR;
  out_payload->len = 0;
  // Ensure no previous allocation leaks when reusing the StrBuf.
  sb_clean(out_payload);

  // Header is short: "Content-Length: " + up to 20 digits + "\r\n\r\n".
  // 52 bytes is a strict cap to avoid unbounded scanning.
  const size_t max_hdr_scan = 52;
  ssize_t idx = bufch_findn(bc, "\r\n\r\n", 4, max_hdr_scan);
  if (idx < 0) {
    // bufch_findn doesn't expose EOF vs "not found"; consult bc->eof.
    return bc->eof ? NO : ERR;
  }
  size_t hdr_len = (size_t)idx + 4;

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
