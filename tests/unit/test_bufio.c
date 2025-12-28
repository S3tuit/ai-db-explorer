#include <stdio.h>
#include <string.h>

#include "bufio.h"
#include "stdio_byte_channel.h"
#include "test.h"

static void test_bufreader_peek_find_and_read(void) {
  FILE *in = MEMFILE_IN("hello world");
  ByteChannel *ch = stdio_bytechannel_create(in, NULL, 0);
  BufReader *br = bufreader_create(ch);
  ASSERT_TRUE(br != NULL);

  ASSERT_TRUE(bufreader_ensure(br, 11) == YES);

  size_t avail = 0;
  const uint8_t *peek = bufreader_peek(br, &avail);
  ASSERT_TRUE(avail == 11);
  ASSERT_TRUE(peek != NULL);
  ASSERT_TRUE(memcmp(peek, "hello world", 11) == 0);

  ASSERT_TRUE(bufreader_find(br, "world", 5) == 6);

  char buf[8];
  ASSERT_TRUE(bufreader_read_n(br, buf, 6) == OK);
  buf[6] = '\0';
  ASSERT_TRUE(strcmp(buf, "hello ") == 0);

  ASSERT_TRUE(bufreader_read_n(br, buf, 5) == OK);
  buf[5] = '\0';
  ASSERT_TRUE(strcmp(buf, "world") == 0);

  ASSERT_TRUE(bufreader_ensure(br, 1) == NO);

  bufreader_destroy(br);
  fclose(in);
}

static void test_bufwriter_write_all(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(NULL, out, 0);
  BufWriter *bw = bufwriter_create(ch);
  ASSERT_TRUE(bw != NULL);

  ASSERT_TRUE(bufwriter_write_all(bw, "abc", 3) == OK);

  bufwriter_destroy(bw);

  char *res = read_all(out);
  ASSERT_STREQ(res, "abc");
  free(res);
  fclose(out);
}

static void test_bufwriter_zero_len(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(NULL, out, 0);
  BufWriter *bw = bufwriter_create(ch);
  ASSERT_TRUE(bw != NULL);

  ASSERT_TRUE(bufwriter_write_all(bw, "ignored", 0) == OK);

  bufwriter_destroy(bw);

  char *res = read_all(out);
  ASSERT_STREQ(res, "");
  free(res);
  fclose(out);
}

int main(void) {
  test_bufreader_peek_find_and_read();
  test_bufwriter_write_all();
  test_bufwriter_zero_len();

  fprintf(stderr, "OK: test_bufio\n");
  return 0;
}
