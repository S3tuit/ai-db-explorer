#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test.h"
#include "transport_writer.h"

static void test_write_basic_frame(void) {
  char *out_buf = NULL;
  size_t out_len = 0;

  FILE *out = open_memstream(&out_buf, &out_len);
  ASSERT_TRUE(out != NULL);

  TransportWriter w;
  transport_w_init(&w, out);

  const char *payload = "{\"x\":1}";
  size_t n = strlen(payload);

  int rc = transport_w_write(&w, payload, n);
  ASSERT_TRUE(rc == 1);

  fclose(out);

  char expected_header[128];
  int hl = snprintf(expected_header, sizeof(expected_header),
                    "Content-Length: %zu\r\n\r\n", n);
  ASSERT_TRUE(hl > 0);

  size_t expected_len = (size_t)hl + n;
  ASSERT_TRUE(out_len == expected_len);

  // Compare header prefix
  ASSERT_TRUE(memcmp(out_buf, expected_header, (size_t)hl) == 0);
  // Compare payload suffix
  ASSERT_TRUE(memcmp(out_buf + (size_t)hl, payload, n) == 0);

  free(out_buf);
}

static void test_write_zero_bytes_writes_nothing(void) {
  char *out_buf = NULL;
  size_t out_len = 0;

  FILE *out = open_memstream(&out_buf, &out_len);
  ASSERT_TRUE(out != NULL);

  TransportWriter w;
  transport_w_init(&w, out);

  int rc = transport_w_write(&w, "IGNORED", 0);
  ASSERT_TRUE(rc == 0);

  fclose(out);

  ASSERT_TRUE(out_len == 0);
  ASSERT_TRUE(out_buf != NULL); // open_memstream allocates a buffer (may be "")
  ASSERT_TRUE(out_buf[0] == '\0');

  free(out_buf);
}

static void test_write_null_bytes_with_nonzero_len_is_error(void) {
  char *out_buf = NULL;
  size_t out_len = 0;

  FILE *out = open_memstream(&out_buf, &out_len);
  ASSERT_TRUE(out != NULL);

  TransportWriter w;
  transport_w_init(&w, out);

  int rc = transport_w_write(&w, NULL, 5);
  ASSERT_TRUE(rc == -1);

  fclose(out);

  // should not have written anything
  ASSERT_TRUE(out_len == 0);

  free(out_buf);
}

static void test_write_allows_null_bytes_if_len_zero(void) {
  char *out_buf = NULL;
  size_t out_len = 0;

  FILE *out = open_memstream(&out_buf, &out_len);
  ASSERT_TRUE(out != NULL);

  TransportWriter w;
  transport_w_init(&w, out);

  int rc = transport_w_write(&w, NULL, 0);
  ASSERT_TRUE(rc == 0);

  fclose(out);
  ASSERT_TRUE(out_len == 0);

  free(out_buf);
}

int main(void) {
  test_write_basic_frame();
  test_write_zero_bytes_writes_nothing();
  test_write_null_bytes_with_nonzero_len_is_error();
  test_write_allows_null_bytes_if_len_zero();

  fprintf(stderr, "OK: test_transport_writer\n");
  return 0;
}
