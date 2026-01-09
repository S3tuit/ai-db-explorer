#include "frame_codec.h"
#include "utils.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>

int frame_write_len(BufChannel *bc, const void *payload, uint32_t n) {
    if (!bc) return ERR;
    if (!payload && n != 0) return ERR;
    
    // write in big-endian
    unsigned char hdr[4];
    hdr[0] = (unsigned char)((n >> 24) & 0xFF);
    hdr[1] = (unsigned char)((n >> 16) & 0xFF);
    hdr[2] = (unsigned char)((n >> 8) & 0xFF);
    hdr[3] = (unsigned char)(n & 0xFF);

    return bufch_write2v(bc, hdr, sizeof(hdr), payload, (size_t)n);
}

int frame_read_len(BufChannel *bc, StrBuf *out_payload) {
    if (!bc || !out_payload) return ERR;

    // read first 4 bytes
    unsigned char hdr[4];
    if (bufch_read_n(bc, hdr, sizeof(hdr)) != OK) return ERR;

    // convert to little-endiat
    uint32_t n = ((uint32_t)hdr[0] << 24) |
                 ((uint32_t)hdr[1] << 16) |
                 ((uint32_t)hdr[2] << 8) |
                 ((uint32_t)hdr[3]);

    if (n > STRBUF_MAX_BYTES) return ERR;
#if SIZE_MAX < UINT32_MAX
    if (n > SIZE_MAX) return ERR;
#endif

    out_payload->len = 0;
    if (n == 0) return OK;

    char *dst = NULL;
    if (sb_prepare_for_write(out_payload, (size_t)n, &dst) != OK) {
        return ERR;
    }
    if (bufch_read_n(bc, (unsigned char *)dst, (size_t)n) != OK) return ERR;
    return OK;
}

int frame_write_cl(BufChannel *bc, const void *payload, size_t n) {
    if (!bc) return ERR;
    if (!payload && n != 0) return ERR;

    char hdr[64];
    // since we write in ASCII digits endianness is irrelevant
    int rc = snprintf(hdr, sizeof(hdr), "Content-Length: %zu\r\n\r\n", n);
    if (rc < 0 || (size_t)rc >= sizeof(hdr)) return ERR;

    return bufch_write2v(bc, hdr, (size_t)rc, payload, n);
}

/* Parses Content-Length from a header. */
static int parse_content_length(const char *hdr, size_t len, size_t *out_len) {
    if (!hdr || !out_len) return ERR;
    *out_len = 0;
    (void)len;

    const char *needle = "Content-Length:";
    const char *p = strstr(hdr, needle);
    if (!p) return ERR;
    p += strlen(needle);
    while (*p == ' ' || *p == '\t') p++;

    char *endptr = NULL;
    unsigned long long v = strtoull(p, &endptr, 10);
    if (endptr == p) return ERR;

    // we can't handle these much bytes and makes no sense doing it
    if (v > STRBUF_MAX_BYTES) return ERR;
    if (v > SIZE_MAX) return ERR;

    *out_len = (size_t)v;
    return OK;
}

int frame_read_cl(BufChannel *bc, StrBuf *out_payload) {
    if (!bc || !out_payload) return ERR;
    out_payload->len = 0;

    // Header is short: "Content-Length: " + up to 20 digits + "\r\n\r\n".
    // 52 bytes is a strict cap to avoid unbounded scanning.
    const size_t max_hdr_scan = 52;
    ssize_t idx = bufch_findn(bc, "\r\n\r\n", 4, max_hdr_scan);
    if (idx < 0) return NO;
    size_t hdr_len = (size_t)idx + 4;

    char *hdr = xmalloc(hdr_len + 1);
    if (bufch_read_n(bc, hdr, hdr_len) != OK) {
        free(hdr);
        return ERR;
    }
    hdr[hdr_len] = '\0';

    size_t payload_len = 0;
    int prc = parse_content_length(hdr, hdr_len, &payload_len);
    free(hdr);
    if (prc != OK) return ERR;

    if (payload_len == 0) return OK;

    char *dst = NULL;
    if (sb_prepare_for_write(out_payload, payload_len, &dst) != OK) {
        return ERR;
    }
    if (bufch_read_n(bc, dst, payload_len) != OK) {
        sb_clean(out_payload);
        return ERR;
    }
    return OK;
}
