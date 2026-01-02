#include <stdio.h>
#include <string.h>

#include "byte_channel.h"
#include "stdio_byte_channel.h"
#include "test.h"

static void test_stdio_read_some(void) {
  FILE *in = MEMFILE_IN("abc");
  ByteChannel *ch = stdio_bytechannel_create(fileno(in), -1, 0);
  ASSERT_TRUE(ch != NULL);

  char buf[4] = {0};
  ssize_t n1 = bytech_read_some(ch, buf, 2);
  ASSERT_TRUE(n1 == 2);
  ASSERT_TRUE(memcmp(buf, "ab", 2) == 0);

  ssize_t n2 = bytech_read_some(ch, buf, 2);
  ASSERT_TRUE(n2 == 1);
  ASSERT_TRUE(buf[0] == 'c');

  ssize_t n3 = bytech_read_some(ch, buf, 2);
  ASSERT_TRUE(n3 == 0);

  bytech_destroy(ch);
  fclose(in);
}

static void test_stdio_write_and_flush(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(-1, fileno(out), 0);
  ASSERT_TRUE(ch != NULL);

  ssize_t n1 = bytech_write_some(ch, "hi", 2);
  ASSERT_TRUE(n1 == 2);
  ASSERT_TRUE(bytech_flush(ch) == OK);

  bytech_destroy(ch);

  char *res = read_all(out);
  ASSERT_STREQ(res, "hi");
  free(res);
  fclose(out);
}

static void test_stdio_shutdown_write_blocks(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(-1, fileno(out), 0);
  ASSERT_TRUE(ch != NULL);

  ssize_t n1 = bytech_write_some(ch, "x", 1);
  ASSERT_TRUE(n1 == 1);
  ASSERT_TRUE(bytech_shutdown_write(ch) == OK);

  ssize_t n2 = bytech_write_some(ch, "y", 1);
  ASSERT_TRUE(n2 == ERR);

  bytech_destroy(ch);

  char *res = read_all(out);
  ASSERT_STREQ(res, "x");
  free(res);
  fclose(out);
}

static void test_close_on_destroy_false_keeps_stream_open(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(-1, fileno(out), 0);
  ASSERT_TRUE(ch != NULL);

  bytech_destroy(ch);

  int rc = fputc('z', out);
  ASSERT_TRUE(rc != EOF);
  fflush(out);

  char *res = read_all(out);
  ASSERT_STREQ(res, "z");
  free(res);
  fclose(out);
}

int main(void) {
  test_stdio_read_some();
  test_stdio_write_and_flush();
  test_stdio_shutdown_write_blocks();
  test_close_on_destroy_false_keeps_stream_open();

  fprintf(stderr, "OK: test_stdio_byte_channel\n");
  return 0;
}
