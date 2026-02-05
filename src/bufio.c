#include "bufio.h"
#include "utils.h"

#include <string.h>

#ifndef BUFIO_READ_CHUNK
#define BUFIO_READ_CHUNK 4096
#endif

int bufch_init(BufChannel *bc, ByteChannel *ch) {
  if (!bc || !ch)
    return ERR;
  bc->ch = ch;
  bc->buf.data = NULL;
  bc->buf.len = 0;
  bc->buf.cap = 0;
  bc->rpos = 0;
  bc->eof = 0;
  return OK;
}

BufChannel *bufch_create(ByteChannel *ch) {
  if (!ch)
    return NULL;
  BufChannel *bc = (BufChannel *)xmalloc(sizeof(*bc));
  if (bufch_init(bc, ch) != OK) {
    free(bc);
    return NULL;
  }
  return bc;
}

void bufch_clean(BufChannel *bc) {
  if (!bc)
    return;
  sb_clean(&bc->buf);
  bc->rpos = 0;
  bc->eof = 0;
  if (bc->ch) {
    bytech_destroy(bc->ch);
  }
  bc->ch = NULL;
}

void bufch_destroy(BufChannel *bc) {
  if (!bc)
    return;
  bufch_clean(bc);
  free(bc);
}

/* Returns how many unread bytes inside 'bc'. */
static size_t bc_avail(const BufChannel *bc) {
  if (bc->buf.len < bc->rpos)
    return 0;
  return bc->buf.len - bc->rpos;
}

// Discard buffered consumed bytes and compact internal buffer.
// Called automatically by some operations.
static void bufch_compact(BufChannel *bc) {
  if (bc->rpos == 0)
    return;
  // If everything consumed, reset
  if (bc->rpos >= bc->buf.len) {
    bc->buf.len = 0;
    bc->rpos = 0;
    return;
  }

  // Move remaining bytes to start
  size_t remain = bc->buf.len - bc->rpos;
  memmove(bc->buf.data, bc->buf.data + bc->rpos, remain);
  bc->buf.len = remain;
  bc->rpos = 0;
}

// Reads once from the underlying channel and appends to buffer.
// Returns:
//  >0 bytes appended
//   0 if EOF encountered (and sets bc->eof)
//  -1 on error
static ssize_t bufch_fill(BufChannel *bc) {
  if (bc->eof)
    return 0;

  // compacting heuristic to avoid unbounded growth.
  // If we've consumed a lot, compact before appending
  if (bc->rpos > 0 &&
      (bc->rpos >= (bc->buf.cap / 2) || bc->rpos >= BUFIO_READ_CHUNK)) {
    bufch_compact(bc);
  }

  uint8_t tmp[BUFIO_READ_CHUNK];

  // TODO: maybe for non-blocking we should handle EAGAIN and EWOULDBLOCK

  ssize_t n = bytech_read_some(bc->ch, tmp, sizeof(tmp));
  if (n > 0) {
    if (sb_append_bytes(&bc->buf, tmp, (size_t)n) != OK) {
      // Treat allocation failure as error
      return -1;
    }
    return n;
  }
  if (n == 0) {
    bc->eof = 1;
    return 0;
  }
  // n < 0 => error
  return -1;
}

// TODO: for non-blocking sockets, ensure is blocking and may stall other
// clients

int bufch_ensure(BufChannel *bc, size_t need) {
  while (bc_avail(bc) < need) {
    ssize_t n = bufch_fill(bc);
    if (n < 0)
      return ERR;
    if (n == 0) {
      // EOF before we got enough
      return NO;
    }
  }
  return YES;
}

const uint8_t *bufch_peek(const BufChannel *bc, size_t *out_avail) {
  size_t avail = bc_avail(bc);
  if (out_avail)
    *out_avail = avail;
  if (avail == 0)
    return NULL;
  return (const uint8_t *)(bc->buf.data + bc->rpos);
}

// Consume 'n' bytes from the buffer (must be <= available).
static void bufch_consume(BufChannel *bc, size_t n) {
  size_t avail = bc_avail(bc);
  if (n > avail)
    n = avail; // defensive
  bc->rpos += n;

  // If we've consumed everything, reset to avoid growth.
  if (bc->rpos >= bc->buf.len) {
    bc->buf.len = 0;
    bc->rpos = 0;
  }
}

int bufch_read_n(BufChannel *bc, void *dst, size_t n) {
  if (bufch_ensure(bc, n) != YES)
    return ERR;
  memcpy(dst, bc->buf.data + bc->rpos, n);
  bufch_consume(bc, n);
  return OK;
}

ssize_t bufch_findn(BufChannel *bc, const void *needle, size_t needle_len,
                    size_t max_dist) {
  if (!bc || !needle || needle_len == 0)
    return 0;

  size_t need = max_dist + needle_len;
  if (need < max_dist)
    return -1; // overflow

  // Ensure enough bytes so we can decide the search window.
  while (bc_avail(bc) < need) {
    int rc = bufch_ensure(bc, need);
    if (rc == YES)
      break;
    if (rc == NO)
      break; // EOF: use what we have
    return -1;
  }

  size_t avail = bc_avail(bc);
  if (avail == 0) {
    return -1;
  }
  if (avail < needle_len)
    return -1;

  size_t limit = max_dist;
  if (limit > avail - needle_len)
    limit = avail - needle_len;

  const uint8_t *hay = (const uint8_t *)(bc->buf.data + bc->rpos);
  const uint8_t *ndl = (const uint8_t *)needle;

  // Simple O(n*m) scan; for small delimiters like "\r\n\r\n" this is fine
  for (size_t i = 0; i <= limit; i++) {
    if (hay[i] == ndl[0] && memcmp(hay + i, ndl, needle_len) == 0) {
      return i;
    }
  }
  return -1;
}

ssize_t bufch_find_buffered(const BufChannel *bc, const void *needle,
                            size_t needle_len) {
  if (!needle || needle_len == 0)
    return 0;

  size_t avail = bc_avail(bc);
  if (avail < needle_len)
    return -1;

  const uint8_t *hay = (const uint8_t *)(bc->buf.data + bc->rpos);
  const uint8_t *ndl = (const uint8_t *)needle;

  // Simple O(n*m) scan; for small delimiters like "\r\n\r\n" this is fine
  for (size_t i = 0; i + needle_len <= avail; i++) {
    if (hay[i] == ndl[0] && memcmp(hay + i, ndl, needle_len) == 0) {
      return i;
    }
  }
  return -1;
}

/* Writers 'n' bytes starting from 'src' to the underlying channel of 'bc' but
 * does not perform any flush() call. */
static int bufch_write_all_no_flush(BufChannel *bc, const void *src, size_t n) {
  if (!bc || !bc->ch)
    return ERR;
  if (!src && n != 0)
    return ERR;
  if (n == 0)
    return OK;

  size_t remaining = n;
  const unsigned char *curr = (const unsigned char *)src;

  while (remaining > 0) {
    ssize_t written = bytech_write_some(bc->ch, curr, remaining);
    if (written > 0) {
      curr += (size_t)written;
      remaining -= (size_t)written;
      continue;
    }
    // treat return of 0 or negative as error, defensive
    return ERR;
  }
  return OK;
}

int bufch_write_all(BufChannel *bc, const void *src, size_t n) {
  if (bufch_write_all_no_flush(bc, src, n) != OK)
    return ERR;
  // we want the target (broker/client/agent) to see the message as soon as
  // we write it, so flush
  if (bytech_flush(bc->ch) != OK)
    return ERR;
  return OK;
}

int bufch_write2v(BufChannel *bc, const void *h, size_t hlen, const void *p,
                  size_t plen) {
  if (!bc || !bc->ch)
    return ERR;
  if ((!h && hlen != 0) || (!p && plen != 0))
    return ERR;
  if (hlen == 0 && plen == 0)
    return OK;

  // if the underlying channel does implement writev
  if (bc->ch->vt->writev_some) {

    // the pointer with the first byte to consume for header and payload
    const unsigned char *hptr = (const unsigned char *)h;
    const unsigned char *pptr = (const unsigned char *)p;

    // how many bytes left to be written for the header and payload
    size_t hleft = hlen;
    size_t pleft = plen;

    while (hleft > 0 || pleft > 0) {
      // handles partial writes when calling writev
      ByteChannelVec vecs[2];
      int vcnt = 0;
      if (hleft > 0) {
        vecs[vcnt].base = hptr;
        vecs[vcnt].len = hleft;
        vcnt++;
      }
      if (pleft > 0) {
        vecs[vcnt].base = pptr;
        vecs[vcnt].len = pleft;
        vcnt++;
      }
      ssize_t n = bytech_writev_some(bc->ch, vecs, vcnt);
      if (n <= 0)
        return ERR;

      size_t written = (size_t)n;
      if (hleft > 0) {
        if (written < hleft) {
          hptr += written;
          hleft -= written;
          continue;
        }
        // header fully written
        written -= hleft;
        hptr += hleft;
        hleft = 0;
      }
      if (written > 0 && pleft > 0) {
        pptr += written;
        pleft -= written;
      }
    }

    if (bytech_flush(bc->ch) != OK)
      return ERR;
    return OK;
  }

  // if no writev, then write 2 times and then call flush
  if (bufch_write_all_no_flush(bc, h, hlen) != OK)
    return ERR;
  if (bufch_write_all_no_flush(bc, p, plen) != OK)
    return ERR;
  if (bytech_flush(bc->ch) != OK)
    return ERR;
  return OK;
}
