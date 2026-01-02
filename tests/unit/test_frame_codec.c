#include <string.h>

#include "bufio.h"
#include "frame_codec.h"
#include "stdio_byte_channel.h"
#include "test.h"

static void write_be_u32(unsigned char *dst, uint32_t n) {
  dst[0] = (unsigned char)((n >> 24) & 0xFF);
  dst[1] = (unsigned char)((n >> 16) & 0xFF);
  dst[2] = (unsigned char)((n >> 8) & 0xFF);
  dst[3] = (unsigned char)(n & 0xFF);
}

static void test_frame_write_len(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(-1, fileno(out), 0);
  BufWriter *bw = bufwriter_create(ch);
  ASSERT_TRUE(bw != NULL);

  const char *payload = "hello";
  ASSERT_TRUE(frame_write_len(bw, payload, 5) == OK);
  bufwriter_destroy(bw);

  char *raw = read_all(out);
  ASSERT_TRUE(raw != NULL);
  ASSERT_TRUE((unsigned char)raw[0] == 0);
  ASSERT_TRUE((unsigned char)raw[1] == 0);
  ASSERT_TRUE((unsigned char)raw[2] == 0);
  ASSERT_TRUE((unsigned char)raw[3] == 5);
  ASSERT_TRUE(memcmp(raw + 4, "hello", 5) == 0);

  free(raw);
  fclose(out);
}

static void test_frame_read_len(void) {
  unsigned char buf[4 + 5];
  write_be_u32(buf, 5);
  memcpy(buf + 4, "hello", 5);

  FILE *in = MEMFILE_OUT();
  fwrite(buf, 1, sizeof(buf), in);
  fflush(in);
  fseek(in, 0, SEEK_SET);

  ByteChannel *ch = stdio_bytechannel_create(fileno(in), -1, 0);
  BufReader *br = bufreader_create(ch);
  ASSERT_TRUE(br != NULL);

  StrBuf payload = {0};
  ASSERT_TRUE(frame_read_len(br, &payload) == OK);
  ASSERT_TRUE(payload.len == 5);
  ASSERT_TRUE(memcmp(payload.data, "hello", 5) == 0);

  sb_clean(&payload);
  bufreader_destroy(br);
  fclose(in);
}

static void test_frame_read_len_too_large(void) {
  unsigned char buf[4];
  write_be_u32(buf, 9);

  FILE *in = MEMFILE_OUT();
  fwrite(buf, 1, sizeof(buf), in);
  fflush(in);
  fseek(in, 0, SEEK_SET);

  ByteChannel *ch = stdio_bytechannel_create(fileno(in), -1, 0);
  BufReader *br = bufreader_create(ch);
  ASSERT_TRUE(br != NULL);

  StrBuf payload = {0};
  ASSERT_TRUE(frame_read_len(br, &payload) == ERR);

  sb_clean(&payload);
  bufreader_destroy(br);
  fclose(in);
}

static void test_frame_write_cl(void) {
  FILE *out = MEMFILE_OUT();
  ByteChannel *ch = stdio_bytechannel_create(-1, fileno(out), 0);
  BufWriter *bw = bufwriter_create(ch);
  ASSERT_TRUE(bw != NULL);

  const char *payload = "abc";
  ASSERT_TRUE(frame_write_cl(bw, payload, 3) == OK);
  bufwriter_destroy(bw);

  char *res = read_all(out);
  ASSERT_STREQ(res, "Content-Length: 3\r\n\r\nabc");

  free(res);
  fclose(out);
}

int main(void) {
  test_frame_write_len();
  test_frame_read_len();
  test_frame_read_len_too_large();
  test_frame_write_cl();

  fprintf(stderr, "OK: test_frame_codec\n");
  return 0;
}
