#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "file_io.h"
#include "test.h"

/* Creates a temp file with 'n' bytes from 'bytes' and returns a heap path.
 * Ownership: caller owns returned path and must free/unlink it.
 * Side effects: creates and writes a file under /tmp.
 * Error semantics: asserts on setup failures and returns non-NULL path on
 * success. */
static char *write_tmp_file(const uint8_t *bytes, size_t n) {
  char tmpl[] = "/tmp/test_fileio_XXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_TRUE(fd >= 0);

  size_t off = 0;
  while (off < n) {
    ssize_t wr = write(fd, bytes + off, n - off);
    ASSERT_TRUE(wr > 0);
    off += (size_t)wr;
  }
  close(fd);
  return strdup(tmpl);
}

/* Verifies reading a small file into StrBuf succeeds. */
static void test_read_small_ok(void) {
  const uint8_t payload[] = {'h', 'e', 'l', 'l', 'o'};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(fileio_read_all_limit(path, 64, &out) == OK);
  ASSERT_TRUE(out.len == sizeof(payload));
  ASSERT_TRUE(memcmp(out.data, payload, sizeof(payload)) == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies reading exactly at max_bytes succeeds. */
static void test_read_exact_limit_ok(void) {
  const uint8_t payload[] = {1, 2, 3, 4};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(fileio_read_all_limit(path, sizeof(payload), &out) == OK);
  ASSERT_TRUE(out.len == sizeof(payload));
  ASSERT_TRUE(memcmp(out.data, payload, sizeof(payload)) == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies fileio_read_all_limit fails when content exceeds max_bytes. */
static void test_read_over_limit_fails(void) {
  const uint8_t payload[] = {1, 2, 3, 4, 5, 6};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(sb_append_bytes(&out, "old", 3) == OK);
  ASSERT_TRUE(fileio_read_all_limit(path, 5, &out) == ERR);
  ASSERT_TRUE(out.len == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies missing file returns ERR and keeps output empty. */
static void test_read_missing_file_fails(void) {
  StrBuf out = {0};
  ASSERT_TRUE(fileio_read_all_limit("/tmp/does_not_exist_file_io", 16, &out) ==
              ERR);
  ASSERT_TRUE(out.len == 0);
  sb_clean(&out);
}

/* Verifies each read resets the output buffer instead of appending. */
static void test_read_resets_output(void) {
  const uint8_t first[] = {'a', 'b', 'c'};
  const uint8_t second[] = {'z', 'y'};
  char *p1 = write_tmp_file(first, sizeof(first));
  char *p2 = write_tmp_file(second, sizeof(second));

  StrBuf out = {0};
  ASSERT_TRUE(fileio_read_all_limit(p1, 16, &out) == OK);
  ASSERT_TRUE(out.len == sizeof(first));
  ASSERT_TRUE(memcmp(out.data, first, sizeof(first)) == 0);

  ASSERT_TRUE(fileio_read_all_limit(p2, 16, &out) == OK);
  ASSERT_TRUE(out.len == sizeof(second));
  ASSERT_TRUE(memcmp(out.data, second, sizeof(second)) == 0);

  sb_clean(&out);
  unlink(p1);
  unlink(p2);
  free(p1);
  free(p2);
}

int main(void) {
  test_read_small_ok();
  test_read_exact_limit_ok();
  test_read_over_limit_fails();
  test_read_missing_file_fails();
  test_read_resets_output();

  fprintf(stderr, "OK: test_file_io\n");
  return 0;
}
