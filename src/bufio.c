#include "bufio.h"
#include "utils.h"

#include <string.h>

#ifndef BUFIO_READ_CHUNK
#define BUFIO_READ_CHUNK 4096
#endif

/* ------------------------------- Reader ---------------------------------- */
BufReader *bufreader_create(ByteChannel *ch) {
    if (!ch) return NULL;
    BufReader *br = (BufReader *)xmalloc(sizeof(*br));
    br->ch  = ch;
    br->buf.data = NULL;
    br->buf.len  = 0;
    br->buf.cap  = 0;
    br->rpos = 0;
    br->eof  = 0;
    return br;
}

void bufreader_destroy(BufReader *br) {
    if (!br) return;
    sb_clean(&br->buf);
    br->rpos = 0;
    br->eof = 0;
    if (br->ch) {
        bytech_destroy(br->ch);
    }
    br->ch = NULL;
    free(br);
}

/* Returns how many unread bytes inside 'br'. */
static size_t br_avail(const BufReader *br) {
    if (br->buf.len < br->rpos) return 0;
    return br->buf.len - br->rpos;
}

// Discard buffered consumed bytes and compact internal buffer.
// Called automatically by some operations.
static void bufreader_compact(BufReader *br) {
    if (br->rpos == 0) return;
    // If everything consumed, reset
    if (br->rpos >= br->buf.len) {
        br->buf.len = 0;
        br->rpos = 0;
        return;
    }

    // Move remaining bytes to start
    size_t remain = br->buf.len - br->rpos;
    memmove(br->buf.data, br->buf.data + br->rpos, remain);
    br->buf.len = remain;
    br->rpos = 0;
}

// Reads once from the underlying channel and appends to buffer.
// Returns:
//  >0 bytes appended
//   0 if EOF encountered (and sets br->eof)
//  -1 on error
static ssize_t bufreader_fill(BufReader *br) {
    if (br->eof) return 0;

    // compacting heuristic to avoid unbounded growth.
    // If we've consumed a lot, compact before appending
    if (br->rpos > 0 && (br->rpos >= (br->buf.cap / 2) || br->rpos >= BUFIO_READ_CHUNK)) {
        bufreader_compact(br);
    }

    uint8_t tmp[BUFIO_READ_CHUNK];
    ssize_t n = bytech_read_some(br->ch, tmp, sizeof(tmp));
    if (n > 0) {
        if (sb_append_bytes(&br->buf, tmp, (size_t)n) != OK) {
            // Treat allocation failure as error
            return -1;
        }
        return n;
    }
    if (n == 0) {
        br->eof = 1;
        return 0;
    }
    // n < 0 => error
    return -1;
}

int bufreader_ensure(BufReader *br, size_t need) {
    while (br_avail(br) < need) {
        ssize_t n = bufreader_fill(br);
        if (n < 0) return ERR;
        if (n == 0) {
            // EOF before we got enough
            return NO;
        }
    }
    return YES;
}

const uint8_t *bufreader_peek(const BufReader *br, size_t *out_avail) {
    size_t avail = br_avail(br);
    *out_avail = avail;
    if (avail == 0) return NULL;
    return (const uint8_t *)(br->buf.data + br->rpos);
}

// Consume 'n' bytes from the buffer (must be <= available).
static void bufreader_consume(BufReader *br, size_t n) {
    size_t avail = br_avail(br);
    if (n > avail) n = avail; // defensive
    br->rpos += n;

    // If we've consumed everything, reset to avoid growth.
    if (br->rpos >= br->buf.len) {
        br->buf.len = 0;
        br->rpos = 0;
    }
}

int bufreader_read_n(BufReader *br, void *dst, size_t n) {
    if (bufreader_ensure(br, n) != YES) return ERR;
    memcpy(dst, br->buf.data + br->rpos, n);
    bufreader_consume(br, n);
    return OK;
}

ssize_t bufreader_find(const BufReader *br, const void *needle, size_t needle_len) {
    if (!needle || needle_len == 0) return 0;

    size_t avail = br_avail(br);
    if (avail < needle_len) return -1;

    const uint8_t *hay = (const uint8_t *)(br->buf.data + br->rpos);
    const uint8_t *ndl = (const uint8_t *)needle;

    // Simple O(n*m) scan; for small delimiters like "\r\n\r\n" this is fine
    for (size_t i = 0; i + needle_len <= avail; i++) {
        if (hay[i] == ndl[0] && memcmp(hay + i, ndl, needle_len) == 0) {
            return i;
        }
    }
    return -1;
}

/* ------------------------------- Writer ---------------------------------- */
BufWriter *bufwriter_create(ByteChannel *ch) {
    if (!ch) return NULL;
    BufWriter *bw = (BufWriter *)xmalloc(sizeof(*bw));
    bw->ch = ch;
    return bw;
}

void bufwriter_destroy(BufWriter *bw) {
    if (!bw) return;
    if (bw->ch) {
        bytech_destroy(bw->ch);
    }
    bw->ch = NULL;
    free(bw);
}

int bufwriter_write_all(BufWriter *bw, const void *src, size_t n) {
    if (!bw || !bw->ch) return ERR;

    size_t remaining = n;
    const unsigned char *curr = (const unsigned char*) src;

    while (remaining > 0) {
        ssize_t written = bytech_write_some(bw->ch, curr, remaining);
        if (written > 0) {
            curr += (size_t)written;
            remaining -= (size_t)written;
            continue;
        }
        // treat return of 0 or negative as error, defensive
        return ERR;
    }
    
    // we want the target (broker/client/agent) to see the message as soon as
    // we write it, so flush
    if (bytech_flush(bw->ch) != OK) return ERR;
    return OK;
}
