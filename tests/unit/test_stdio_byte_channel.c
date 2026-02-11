#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "byte_channel.h"
#include "string_op.h"
#include "stdio_byte_channel.h"
#include "test.h"

static void test_stdio_read_some(void) {
  FILE *in = MEMFILE_IN("abc");
  ByteChannel *ch = stdio_bytechannel_wrap_fd(fileno(in), -1);
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
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
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
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
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
  ByteChannel *ch = stdio_bytechannel_wrap_fd(-1, fileno(out));
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

/* Creates a temporary file path and returns it in caller-owned memory.
 * Ownership: returns heap path owned by caller; caller must unlink/free.
 * Side effects: creates an empty file under /tmp and closes it.
 * Error semantics: asserts on setup failure and returns non-NULL path.
 */
static char *make_tmp_path(void) {
  char tmpl[] = "/tmp/test_stdio_bch_XXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_TRUE(fd >= 0);
  ASSERT_TRUE(close(fd) == 0);
  return dup_or_null(tmpl);
}

/* Writes exact bytes to an existing path, truncating file first.
 * Ownership: borrows 'path' and 's'; no allocations.
 * Side effects: filesystem I/O on path.
 * Error semantics: asserts on write errors.
 */
static void write_file_exact(const char *path, const char *s) {
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(s != NULL);
  int fd = open(path, O_WRONLY | O_TRUNC);
  ASSERT_TRUE(fd >= 0);
  size_t len = strlen(s);
  ASSERT_TRUE(write(fd, s, len) == (ssize_t)len);
  ASSERT_TRUE(close(fd) == 0);
}

/* Reads all bytes from an existing path into caller-owned output buffer.
 * Ownership: borrows 'path'; writes NUL-terminated data into caller-owned 'out'
 * with cap 'out_cap'.
 * Side effects: filesystem I/O on path.
 * Error semantics: asserts on invalid input, open/read failure, or truncation.
 */
static void read_file_all(const char *path, char *out, size_t out_cap) {
  ASSERT_TRUE(path != NULL);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(out_cap >= 2);
  int fd = open(path, O_RDONLY);
  ASSERT_TRUE(fd >= 0);
  ssize_t n = read(fd, out, out_cap - 1);
  ASSERT_TRUE(n >= 0);
  out[n] = '\0';
  ASSERT_TRUE(close(fd) == 0);
}

/* Verifies open_path handles same path with a single O_RDWR descriptor. */
static void test_open_path_same_file(void) {
  char *path = make_tmp_path();
  write_file_exact(path, "ab");

  ByteChannel *ch = stdio_bytechannel_open_path(path, path);
  ASSERT_TRUE(ch != NULL);

  char buf[8] = {0};
  ASSERT_TRUE(bytech_read_some(ch, buf, 2) == 2);
  ASSERT_TRUE(memcmp(buf, "ab", 2) == 0);
  ASSERT_TRUE(bytech_write_some(ch, "cd", 2) == 2);
  bytech_destroy(ch);

  char out[16] = {0};
  read_file_all(path, out, sizeof(out));
  ASSERT_TRUE(strcmp(out, "abcd") == 0);

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
}

/* Verifies open_path supports read-only mode and blocks writes. */
static void test_open_path_read_only(void) {
  char *path = make_tmp_path();
  write_file_exact(path, "xyz");

  ByteChannel *ch = stdio_bytechannel_open_path(path, NULL);
  ASSERT_TRUE(ch != NULL);

  char buf[8] = {0};
  ASSERT_TRUE(bytech_read_some(ch, buf, 3) == 3);
  ASSERT_TRUE(memcmp(buf, "xyz", 3) == 0);
  ASSERT_TRUE(bytech_write_some(ch, "q", 1) == ERR);
  bytech_destroy(ch);

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
}

/* Verifies open_path supports write-only mode and blocks reads. */
static void test_open_path_write_only(void) {
  char *path = make_tmp_path();
  write_file_exact(path, "");

  ByteChannel *ch = stdio_bytechannel_open_path(NULL, path);
  ASSERT_TRUE(ch != NULL);
  ASSERT_TRUE(bytech_write_some(ch, "ok", 2) == 2);
  ASSERT_TRUE(bytech_read_some(ch, (char[1]){0}, 1) == ERR);
  bytech_destroy(ch);

  char out[16] = {0};
  read_file_all(path, out, sizeof(out));
  ASSERT_TRUE(strcmp(out, "ok") == 0);

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
}

/* Verifies invalid open_path input is rejected. */
static void test_open_path_invalid_input(void) {
  ASSERT_TRUE(stdio_bytechannel_open_path(NULL, NULL) == NULL);
  ASSERT_TRUE(stdio_bytechannel_open_path("", NULL) == NULL);
  ASSERT_TRUE(stdio_bytechannel_open_path(NULL, "") == NULL);
}

int main(void) {
  test_stdio_read_some();
  test_stdio_write_and_flush();
  test_stdio_shutdown_write_blocks();
  test_close_on_destroy_false_keeps_stream_open();
  test_open_path_same_file();
  test_open_path_read_only();
  test_open_path_write_only();
  test_open_path_invalid_input();

  fprintf(stderr, "OK: test_stdio_byte_channel\n");
  return 0;
}
