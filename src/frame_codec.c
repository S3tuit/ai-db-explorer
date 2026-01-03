#include "frame_codec.h"
#include "utils.h"

#include <stdio.h>
#include <string.h>

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

    // TODO: we can create a method inside BufChannel so we can read bytes
    // and call a function passed in input. So we avoid double allocation.
    unsigned char *tmp = (unsigned char *)xmalloc(n);
    if (bufch_read_n(bc, tmp, (size_t)n) != OK) {
        free(tmp);
        return ERR;
    }
    if (sb_append_bytes(out_payload, tmp, (size_t)n) != OK) {
        free(tmp);
        return ERR;
    }
    free(tmp);
    return OK;
}

int frame_write_cl(BufChannel *bc, const void *payload, size_t n) {
    if (!bc) return ERR;
    if (!payload && n != 0) return ERR;

    char hdr[64];
    int rc = snprintf(hdr, sizeof(hdr), "Content-Length: %zu\r\n\r\n", n);
    if (rc < 0 || (size_t)rc >= sizeof(hdr)) return ERR;

    return bufch_write2v(bc, hdr, (size_t)rc, payload, n);
}
