#include <stdio.h>
#include <string.h>

#include "bufio.h"
#include "stdio_byte_channel.h"
#include "test.h"

// ByteChannel stub that intentionally returns short reads/writes.
// Used to ensure bufio loops until it completes the requested length.
typedef struct PartialByteChannelImpl {
  const unsigned char *rbuf;
  size_t rlen;
  size_t rpos;
  size_t read_chunk;
  unsigned char *wbuf;
  size_t wlen;
  size_t wcap;
  size_t write_chunk;
  int read_calls;
  int write_calls;
  int writev_calls;
} PartialByteChannelImpl;

// Grow the write buffer as needed for capturing output.
static void partial_ensure_cap(PartialByteChannelImpl *impl, size_t add) {
  size_t need = impl->wlen + add;
  if (need <= impl->wcap) return;
  size_t newcap = impl->wcap ? impl->wcap : 64;
  while (newcap < need) newcap *= 2;
  impl->wbuf = (unsigned char *)xrealloc(impl->wbuf, newcap);
  impl->wcap = newcap;
}

// Return at most read_chunk bytes to simulate partial reads.
static ssize_t partial_read_some(ByteChannel *ch, void *buf, size_t cap) {
  PartialByteChannelImpl *impl = (PartialByteChannelImpl *)ch->impl;
  if (!impl || !buf) return -1;
  if (cap == 0) return 0;
  if (impl->rpos >= impl->rlen) return 0;

  size_t remaining = impl->rlen - impl->rpos;
  size_t n = remaining < cap ? remaining : cap;
  if (impl->read_chunk > 0 && n > impl->read_chunk) n = impl->read_chunk;

  memcpy(buf, impl->rbuf + impl->rpos, n);
  impl->rpos += n;
  impl->read_calls++;
  return (ssize_t)n;
}

// Accept at most 'ch'->write_chunk bytes to simulate partial writes.
static ssize_t partial_write_some(ByteChannel *ch, const void *buf, size_t len) {
  PartialByteChannelImpl *impl = (PartialByteChannelImpl *)ch->impl;
  if (!impl || !buf) return -1;
  if (len == 0) return 0;

  size_t n = len;
  if (impl->write_chunk > 0 && n > impl->write_chunk) n = impl->write_chunk;
  partial_ensure_cap(impl, n);
  memcpy(impl->wbuf + impl->wlen, buf, n);
  impl->wlen += n;
  impl->write_calls++;
  return (ssize_t)n;
}

// Accepts at most 'ch'->write_chunk bytes and call writev. Simulates partial
// writev
static ssize_t partial_writev_some(ByteChannel *ch,
                                   const ByteChannelVec *vecs, int vcnt) {
  PartialByteChannelImpl *impl = (PartialByteChannelImpl *)ch->impl;
  if (!impl || !vecs || vcnt <= 0) return -1;

  size_t total = 0;
  for (int i = 0; i < vcnt; i++) {
    if (!vecs[i].base && vecs[i].len != 0) return -1;
    total += vecs[i].len;
  }
  if (total == 0) {
    impl->writev_calls++;
    return 0;
  }

  size_t limit = total;
  if (impl->write_chunk > 0 && limit > impl->write_chunk) limit = impl->write_chunk;
  partial_ensure_cap(impl, limit);
  size_t remaining = limit;
  for (int i = 0; i < vcnt; i++) {
    if (remaining == 0) break;
    if (vecs[i].len == 0) continue;
    size_t n = vecs[i].len;
    if (n > remaining) n = remaining;
    memcpy(impl->wbuf + impl->wlen, vecs[i].base, n);
    impl->wlen += n;
    remaining -= n;
  }
  impl->writev_calls++;
  return (ssize_t)limit;
}

// No-op flush to satisfy the vtable.
static int partial_flush(ByteChannel *ch) {
  (void)ch;
  return OK;
}

// No-op shutdown; nothing to release.
static int partial_shutdown_write(ByteChannel *ch) {
  (void)ch;
  return OK;
}

static BytePollable partial_get_pollable(const ByteChannel *ch) {
  (void)ch;
  return (BytePollable)-1;
}

static void partial_destroy(ByteChannel *ch) {
  if (!ch) return;
  PartialByteChannelImpl *impl = (PartialByteChannelImpl *)ch->impl;
  if (impl) {
    free(impl->wbuf);
    free(impl);
  }
  free(ch);
}

static const ByteChannelVTable PARTIAL_VT = {
  .read_some = partial_read_some,
  .write_some = partial_write_some,
  .writev_some = partial_writev_some,
  .flush = partial_flush,
  .shutdown_write = partial_shutdown_write,
  .get_pollable = partial_get_pollable,
  .destroy = partial_destroy
};

// Create a partial channel; caller can inspect impl for call counts and output.
static ByteChannel *partial_bytechannel_create(const unsigned char *rbuf, size_t rlen,
                                               size_t read_chunk, size_t write_chunk,
                                               PartialByteChannelImpl **out_impl) {
  ByteChannel *ch = (ByteChannel *)xmalloc(sizeof(ByteChannel));
  PartialByteChannelImpl *impl = (PartialByteChannelImpl *)xcalloc(1, sizeof(*impl));
  impl->rbuf = rbuf;
  impl->rlen = rlen;
  impl->read_chunk = read_chunk;
  impl->write_chunk = write_chunk;
  ch->vt = &PARTIAL_VT;
  ch->impl = impl;
  if (out_impl) *out_impl = impl;
  return ch;
}

static void test_bufch_peek_find_and_read(void) {
  FILE *in = MEMFILE_IN("hello world");
  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
  BufChannel bc;
  ASSERT_TRUE(bufch_init(&bc, ch) == OK);

  ASSERT_TRUE(bufch_ensure(&bc, 11) == YES);

  size_t avail = 0;
  const uint8_t *peek = bufch_peek(&bc, &avail);
  ASSERT_TRUE(avail == 11);
  ASSERT_TRUE(peek != NULL);
  ASSERT_TRUE(memcmp(peek, "hello world", 11) == 0);

  ASSERT_TRUE(bufch_find(&bc, "world", 5) == 6);

  char buf[8];
  ASSERT_TRUE(bufch_read_n(&bc, buf, 6) == OK);
  buf[6] = '\0';
  ASSERT_TRUE(strcmp(buf, "hello ") == 0);

  ASSERT_TRUE(bufch_read_n(&bc, buf, 5) == OK);
  buf[5] = '\0';
  ASSERT_TRUE(strcmp(buf, "world") == 0);

  ASSERT_TRUE(bufch_ensure(&bc, 1) == NO);

  bufch_clean(&bc);
  fclose(in);
}

static void test_bufch_partial_reads(void) {
  const char *msg = "hello world";
  PartialByteChannelImpl *impl = NULL;
  ByteChannel *ch = partial_bytechannel_create((const unsigned char *)msg, 11, 2, 0, &impl);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);
  ASSERT_TRUE(impl != NULL);

  char buf[12];
  ASSERT_TRUE(bufch_read_n(bc, buf, 11) == OK);
  buf[11] = '\0';
  ASSERT_TRUE(strcmp(buf, msg) == 0);
  ASSERT_TRUE(impl->read_calls > 1);

  bufch_destroy(bc);
}

static void test_bufch_partial_writes(void) {
  const char *msg = "hello world";
  PartialByteChannelImpl *impl = NULL;
  ByteChannel *ch = partial_bytechannel_create(NULL, 0, 0, 2, &impl);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);
  ASSERT_TRUE(impl != NULL);

  ASSERT_TRUE(bufch_write_all(bc, msg, 11) == OK);
  ASSERT_TRUE(impl->wlen == (size_t)11);
  ASSERT_TRUE(memcmp(impl->wbuf, msg, 11) == 0);
  ASSERT_TRUE(impl->write_calls > 1);

  bufch_destroy(bc);
}

static void test_bufch_zero_len(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);

  ASSERT_TRUE(bufch_write_all(bc, "ignored", 0) == OK);

  bufch_destroy(bc);

  char *res = read_all(out);
  ASSERT_STREQ(res, "");
  free(res);
  fclose(out);
}

static void test_bufch_write2v_uses_writev(void) {
  PartialByteChannelImpl *impl = NULL;
  ByteChannel *ch = partial_bytechannel_create(NULL, 0, 0, 3, &impl);
  BufChannel *bc = bufch_create(ch);
  ASSERT_TRUE(bc != NULL);
  ASSERT_TRUE(impl != NULL);

  const char *h = "head";
  const char *p = "body";
  ASSERT_TRUE(bufch_write2v(bc, h, 4, p, 4) == OK);
  ASSERT_TRUE(impl->writev_calls > 1);
  ASSERT_TRUE(impl->write_calls == 0);
  ASSERT_TRUE(impl->wlen == 8);
  ASSERT_TRUE(memcmp(impl->wbuf, "headbody", 8) == 0);

  bufch_destroy(bc);
}

int main(void) {
  test_bufch_peek_find_and_read();
  test_bufch_partial_reads();
  test_bufch_partial_writes();
  test_bufch_zero_len();
  test_bufch_write2v_uses_writev();

  fprintf(stderr, "OK: test_bufio\n");
  return 0;
}
