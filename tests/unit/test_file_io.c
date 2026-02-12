#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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

/* Creates a unique temp path and ensures it does not exist yet.
 * Ownership: caller owns returned path and must free/unlink it.
 * Side effects: creates and unlinks a temporary file under /tmp.
 * Error semantics: asserts on setup failures and returns non-NULL path on
 * success. */
static char *make_tmp_path(void) {
  char tmpl[] = "/tmp/test_fileio_path_XXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_TRUE(fd >= 0);
  close(fd);
  ASSERT_TRUE(unlink(tmpl) == 0);
  return strdup(tmpl);
}

/* Verifies reading a small file into StrBuf succeeds. */
static void test_read_small_ok(void) {
  const uint8_t payload[] = {'h', 'e', 'l', 'l', 'o'};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(fileio_sb_read_limit(path, 64, &out) == OK);
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
  ASSERT_TRUE(fileio_sb_read_limit(path, sizeof(payload), &out) == OK);
  ASSERT_TRUE(out.len == sizeof(payload));
  ASSERT_TRUE(memcmp(out.data, payload, sizeof(payload)) == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies fileio_sb_read_limit fails when content exceeds max_bytes. */
static void test_read_over_limit_fails(void) {
  const uint8_t payload[] = {1, 2, 3, 4, 5, 6};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(sb_append_bytes(&out, "old", 3) == OK);
  ASSERT_TRUE(fileio_sb_read_limit(path, 5, &out) == ERR);
  ASSERT_TRUE(out.len == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies missing file returns ERR and keeps output empty. */
static void test_read_missing_file_fails(void) {
  StrBuf out = {0};
  ASSERT_TRUE(fileio_sb_read_limit("/tmp/does_not_exist_file_io", 16, &out) ==
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
  ASSERT_TRUE(fileio_sb_read_limit(p1, 16, &out) == OK);
  ASSERT_TRUE(out.len == sizeof(first));
  ASSERT_TRUE(memcmp(out.data, first, sizeof(first)) == 0);

  ASSERT_TRUE(fileio_sb_read_limit(p2, 16, &out) == OK);
  ASSERT_TRUE(out.len == sizeof(second));
  ASSERT_TRUE(memcmp(out.data, second, sizeof(second)) == 0);

  sb_clean(&out);
  unlink(p1);
  unlink(p2);
  free(p1);
  free(p2);
}

/* Verifies fileio_sb_read_up_to rejects invalid input and leaves output empty. */
static void test_sb_read_up_to_bad_input(void) {
  StrBuf out = {0};
  ASSERT_TRUE(fileio_sb_read_up_to(NULL, 16, &out) == -1);
  ASSERT_TRUE(fileio_sb_read_up_to("/tmp/does_not_exist_file_io",
                                   STRBUF_MAX_BYTES + 1, &out) == -1);
  ASSERT_TRUE(
      fileio_sb_read_up_to("/tmp/does_not_exist_file_io", 16, NULL) == -1);
  ASSERT_TRUE(out.len == 0);
  sb_clean(&out);
}

/* Verifies fileio_sb_read_up_to reads full content when file is shorter than
 * cap.
 */
static void test_sb_read_up_to_shorter_than_cap(void) {
  const uint8_t payload[] = {'q', 'w', 'e', 'r'};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(fileio_sb_read_up_to(path, 64, &out) == (ssize_t)sizeof(payload));
  ASSERT_TRUE(out.len == sizeof(payload));
  ASSERT_TRUE(memcmp(out.data, payload, sizeof(payload)) == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies fileio_sb_read_up_to truncates at max_bytes without returning
 * error.
 */
static void test_sb_read_up_to_longer_than_cap(void) {
  const uint8_t payload[] = {'a', 'b', 'c', 'd', 'e', 'f'};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out = {0};
  ASSERT_TRUE(fileio_sb_read_up_to(path, 3, &out) == 3);
  ASSERT_TRUE(out.len == 3);
  ASSERT_TRUE(memcmp(out.data, payload, 3) == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies fileio_read_limit reads all bytes and reports output size. */
static void test_read_limit_raw_ok(void) {
  const uint8_t payload[] = {'r', 'a', 'w', '1'};
  char *path = write_tmp_file(payload, sizeof(payload));

  uint8_t out[16] = {0};
  size_t nread = 999;
  ASSERT_TRUE(fileio_read_limit(path, 16, out, &nread) == OK);
  ASSERT_TRUE(nread == sizeof(payload));
  ASSERT_TRUE(memcmp(out, payload, sizeof(payload)) == 0);

  unlink(path);
  free(path);
}

/* Verifies fileio_read_limit rejects files larger than max_bytes. */
static void test_read_limit_raw_over_limit_fails(void) {
  const uint8_t payload[] = {'a', 'b', 'c', 'd', 'e', 'f'};
  char *path = write_tmp_file(payload, sizeof(payload));

  uint8_t out[8] = {0};
  size_t nread = 777;
  ASSERT_TRUE(fileio_read_limit(path, 5, out, &nread) == ERR);
  ASSERT_TRUE(nread == 0);

  unlink(path);
  free(path);
}

/* Verifies fileio_read_up_to (raw) rejects invalid input. */
static void test_read_up_to_raw_bad_input(void) {
  uint8_t out[8] = {0};
  ASSERT_TRUE(fileio_read_up_to(NULL, 8, out) == -1);
  ASSERT_TRUE(fileio_read_up_to("/tmp/does_not_exist_file_io", 8, out) == -1);
  ASSERT_TRUE(fileio_read_up_to("/tmp/does_not_exist_file_io", 8, NULL) == -1);
}

/* Verifies fileio_read_up_to (raw) returns full content when under cap. */
static void test_read_up_to_raw_shorter_than_cap(void) {
  const uint8_t payload[] = {'x', 'y', 'z'};
  char *path = write_tmp_file(payload, sizeof(payload));

  uint8_t out[8] = {0};
  ASSERT_TRUE(fileio_read_up_to(path, 8, out) == (ssize_t)sizeof(payload));
  ASSERT_TRUE(memcmp(out, payload, sizeof(payload)) == 0);

  unlink(path);
  free(path);
}

/* Verifies fileio_read_up_to (raw) truncates to cap without error. */
static void test_read_up_to_raw_longer_than_cap(void) {
  const uint8_t payload[] = {'1', '2', '3', '4', '5'};
  char *path = write_tmp_file(payload, sizeof(payload));

  uint8_t out[3] = {0};
  ASSERT_TRUE(fileio_read_up_to(path, 3, out) == 3);
  ASSERT_TRUE(memcmp(out, payload, 3) == 0);

  unlink(path);
  free(path);
}

/* Verifies fileio_write_exact rejects invalid arguments. */
static void test_write_exact_bad_input(void) {
  const uint8_t payload[] = {'w', 'r', 'i', 't', 'e'};
  char *path = make_tmp_path();

  ASSERT_TRUE(fileio_write_exact(NULL, payload, sizeof(payload), 0600) == ERR);
  ASSERT_TRUE(fileio_write_exact(path, NULL, 1, 0600) == ERR);
  ASSERT_TRUE(fileio_write_exact(path, payload, sizeof(payload), 0100000) ==
              ERR);

  free(path);
}

/* Verifies fileio_write_exact writes bytes exactly and applies mode. */
static void test_write_exact_ok(void) {
  const uint8_t payload[] = {'o', 'k', '1', '2'};
  char *path = make_tmp_path();

  ASSERT_TRUE(fileio_write_exact(path, payload, sizeof(payload), 0600) == OK);

  struct stat st;
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);
  ASSERT_TRUE(st.st_size == (off_t)sizeof(payload));

  uint8_t out[16] = {0};
  size_t nread = 0;
  ASSERT_TRUE(fileio_read_limit(path, sizeof(out), out, &nread) == OK);
  ASSERT_TRUE(nread == sizeof(payload));
  ASSERT_TRUE(memcmp(out, payload, sizeof(payload)) == 0);

  unlink(path);
  free(path);
}

/* Verifies fileio_write_exact truncates existing files to the new payload
 * size. */
static void test_write_exact_truncates_existing(void) {
  const uint8_t old_payload[] = {'a', 'b', 'c', 'd', 'e', 'f'};
  const uint8_t new_payload[] = {'z', 'y'};
  char *path = write_tmp_file(old_payload, sizeof(old_payload));

  ASSERT_TRUE(fileio_write_exact(path, new_payload, sizeof(new_payload), 0600) ==
              OK);

  struct stat st;
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(st.st_size == (off_t)sizeof(new_payload));

  uint8_t out[8] = {0};
  size_t nread = 0;
  ASSERT_TRUE(fileio_read_limit(path, sizeof(out), out, &nread) == OK);
  ASSERT_TRUE(nread == sizeof(new_payload));
  ASSERT_TRUE(memcmp(out, new_payload, sizeof(new_payload)) == 0);

  unlink(path);
  free(path);
}

/* Verifies zero-byte writes allow NULL source and create an empty file. */
static void test_write_exact_zero_size_ok(void) {
  char *path = make_tmp_path();

  ASSERT_TRUE(fileio_write_exact(path, NULL, 0, 0600) == OK);

  struct stat st;
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(S_ISREG(st.st_mode));
  ASSERT_TRUE((st.st_mode & 0777) == 0600);
  ASSERT_TRUE(st.st_size == 0);

  unlink(path);
  free(path);
}

int main(void) {
  test_read_small_ok();
  test_read_exact_limit_ok();
  test_read_over_limit_fails();
  test_read_missing_file_fails();
  test_read_resets_output();
  test_sb_read_up_to_bad_input();
  test_sb_read_up_to_shorter_than_cap();
  test_sb_read_up_to_longer_than_cap();
  test_read_limit_raw_ok();
  test_read_limit_raw_over_limit_fails();
  test_read_up_to_raw_bad_input();
  test_read_up_to_raw_shorter_than_cap();
  test_read_up_to_raw_longer_than_cap();
  test_write_exact_bad_input();
  test_write_exact_ok();
  test_write_exact_truncates_existing();
  test_write_exact_zero_size_ok();

  fprintf(stderr, "OK: test_file_io\n");
  return 0;
}
