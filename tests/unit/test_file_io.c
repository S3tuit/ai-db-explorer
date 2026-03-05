#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "file_io.h"
#include "test.h"

#ifdef _WIN32
#define TEST_PATH_SEP '\\'
#else
#define TEST_PATH_SEP '/'
#endif

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

/* Creates a unique temporary directory and returns a heap copy of its path.
 * Ownership: caller owns returned path and must remove/free it.
 * Side effects: creates one directory under /tmp.
 * Error semantics: asserts on setup failure and returns non-NULL on success.
 */
static char *make_tmp_dir(void) {
  char tmpl[] = "/tmp/test_fileio_dir_XXXXXX";
  char *dir = mkdtemp(tmpl);
  ASSERT_TRUE(dir != NULL);
  char *copy = strdup(dir);
  ASSERT_TRUE(copy != NULL);
  return copy;
}

/* Opens one directory path and returns its fd.
 * Ownership: borrows 'dir_path'; caller owns returned fd and must close it.
 * Side effects: opens one directory descriptor.
 * Error semantics: asserts on failure and returns fd >= 0 on success.
 */
static int open_dir_fd(const char *dir_path) {
  int flags = O_RDONLY;
#ifdef O_DIRECTORY
  flags |= O_DIRECTORY;
#endif
  int fd = open(dir_path, flags);
  ASSERT_TRUE(fd >= 0);
  return fd;
}

#define CHILD_EXIT_NO 20
#define CHILD_EXIT_YES 21
#define CHILD_EXIT_ERR 22

/* Waits one child and converts its exit code back to AdbxTriStatus.
 * Ownership: borrows 'pid'; no allocations.
 * Side effects: waits child process termination.
 * Error semantics: asserts for abnormal termination; returns YES/NO/ERR status.
 */
static AdbxTriStatus wait_child_tristatus(pid_t pid) {
  int status = 0;
  ASSERT_TRUE(waitpid(pid, &status, 0) == pid);
  ASSERT_TRUE(WIFEXITED(status));
  int code = WEXITSTATUS(status);
  if (code == CHILD_EXIT_YES)
    return YES;
  if (code == CHILD_EXIT_NO)
    return NO;
  return ERR;
}

/* Asserts directory content is exactly 'expected_count' entries and, when
 * expected_count==1, the single entry equals 'expected_name'.
 * Ownership: borrows inputs; no allocations.
 * Side effects: reads directory entries.
 * Error semantics: asserts on any mismatch.
 */
static void assert_dir_layout(const char *dir_path, const char *expected_name,
                              size_t expected_count) {
  DIR *d = opendir(dir_path);
  ASSERT_TRUE(d != NULL);

  size_t count = 0;
  char first_name[256] = {0};
  for (;;) {
    struct dirent *ent = readdir(d);
    if (!ent)
      break;
    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
      continue;

    count++;
    if (count == 1) {
      strncpy(first_name, ent->d_name, sizeof(first_name) - 1);
      first_name[sizeof(first_name) - 1] = '\0';
    }
    ASSERT_TRUE(strstr(ent->d_name, ".lock") == NULL);
  }
  closedir(d);

  ASSERT_TRUE(count == expected_count);
  if (expected_count == 1) {
    ASSERT_TRUE(expected_name != NULL);
    ASSERT_TRUE(strcmp(first_name, expected_name) == 0);
  }
}

/* Asserts file at 'path' has exactly 'len' bytes and every byte equals
 * 'expected'.
 * Ownership: borrows 'path'; no heap allocations.
 * Side effects: reads file content through fileio_read_limit.
 * Error semantics: asserts on read failure or content mismatch.
 */
static void assert_file_uniform_byte(const char *path, uint8_t expected,
                                     size_t len) {
  uint8_t out[256] = {0};
  ASSERT_TRUE(len <= sizeof(out));

  size_t nread = 0;
  ASSERT_TRUE(fileio_read_limit(path, sizeof(out), out, &nread) == OK);
  ASSERT_TRUE(nread == len);
  for (size_t i = 0; i < len; i++) {
    ASSERT_TRUE(out[i] == expected);
  }
}

/* Verifies reading a small file into StrBuf succeeds. */
static void test_read_small_ok(void) {
  const uint8_t payload[] = {'h', 'e', 'l', 'l', 'o'};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out;
  sb_init(&out);
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

  StrBuf out;
  sb_init(&out);
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

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(sb_append_bytes(&out, "old", 3) == OK);
  ASSERT_TRUE(fileio_sb_read_limit(path, 5, &out) == ERR);
  ASSERT_TRUE(out.len == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies missing file returns ERR and keeps output empty. */
static void test_read_missing_file_fails(void) {
  StrBuf out;
  sb_init(&out);
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

  StrBuf out;
  sb_init(&out);
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

/* Verifies fileio_sb_read_up_to rejects invalid input and leaves output empty.
 */
static void test_sb_read_up_to_bad_input(void) {
  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(fileio_sb_read_up_to(NULL, 16, &out) == -1);
  ASSERT_TRUE(fileio_sb_read_up_to("/tmp/does_not_exist_file_io",
                                   STRBUF_MAX_BYTES + 1, &out) == -1);
  ASSERT_TRUE(fileio_sb_read_up_to("/tmp/does_not_exist_file_io", 16, NULL) ==
              -1);
  ASSERT_TRUE(out.len == 0);
  sb_clean(&out);
}

/* Verifies fileio_sb_read_up_to reads full content when file is shorter than
 * cap.
 */
static void test_sb_read_up_to_shorter_than_cap(void) {
  const uint8_t payload[] = {'q', 'w', 'e', 'r'};
  char *path = write_tmp_file(payload, sizeof(payload));

  StrBuf out;
  sb_init(&out);
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

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(fileio_sb_read_up_to(path, 3, &out) == 3);
  ASSERT_TRUE(out.len == 3);
  ASSERT_TRUE(memcmp(out.data, payload, 3) == 0);

  sb_clean(&out);
  unlink(path);
  free(path);
}

/* Verifies fileio_sb_read_limit_fd reads full content and keeps fd open. */
static void test_sb_read_limit_fd_ok(void) {
  const uint8_t payload[] = {'f', 'd', 'o', 'k'};
  char *path = write_tmp_file(payload, sizeof(payload));
  int fd = open(path, O_RDONLY);
  ASSERT_TRUE(fd >= 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(fileio_sb_read_limit_fd(fd, 64, &out) == OK);
  ASSERT_TRUE(out.len == sizeof(payload));
  ASSERT_TRUE(memcmp(out.data, payload, sizeof(payload)) == 0);
  ASSERT_TRUE(fcntl(fd, F_GETFD) != -1);

  sb_clean(&out);
  close(fd);
  unlink(path);
  free(path);
}

/* Verifies fileio_sb_read_limit_fd enforces max_bytes and clears output on
 * error. */
static void test_sb_read_limit_fd_over_limit_fails(void) {
  const uint8_t payload[] = {'1', '2', '3', '4', '5', '6'};
  char *path = write_tmp_file(payload, sizeof(payload));
  int fd = open(path, O_RDONLY);
  ASSERT_TRUE(fd >= 0);

  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(sb_append_bytes(&out, "old", 3) == OK);
  ASSERT_TRUE(fileio_sb_read_limit_fd(fd, 5, &out) == ERR);
  ASSERT_TRUE(out.len == 0);

  sb_clean(&out);
  close(fd);
  unlink(path);
  free(path);
}

/* Verifies fileio_sb_read_limit_fd rejects invalid arguments. */
static void test_sb_read_limit_fd_bad_input(void) {
  StrBuf out;
  sb_init(&out);
  ASSERT_TRUE(fileio_sb_read_limit_fd(-1, 16, &out) == ERR);
  ASSERT_TRUE(fileio_sb_read_limit_fd(-1, 16, NULL) == ERR);
  sb_clean(&out);
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

  ASSERT_TRUE(
      fileio_write_exact(path, new_payload, sizeof(new_payload), 0600) == OK);

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

/* Verifies fileio_write_exact_fd writes bytes to an open fd and keeps it open.
 */
static void test_write_exact_fd_ok(void) {
  const uint8_t payload[] = {'f', 'd', 'w', 'r'};
  char *path = make_tmp_path();
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  ASSERT_TRUE(fd >= 0);

  ASSERT_TRUE(fileio_write_exact_fd(fd, payload, sizeof(payload)) == OK);
  ASSERT_TRUE(fcntl(fd, F_GETFD) != -1);
  ASSERT_TRUE(lseek(fd, 0, SEEK_SET) == 0);

  uint8_t out[8] = {0};
  ASSERT_TRUE(read(fd, out, sizeof(payload)) == (ssize_t)sizeof(payload));
  ASSERT_TRUE(memcmp(out, payload, sizeof(payload)) == 0);

  close(fd);
  unlink(path);
  free(path);
}

/* Verifies fileio_write_exact_fd rejects invalid arguments. */
static void test_write_exact_fd_bad_input(void) {
  const uint8_t payload[] = {'x'};
  ASSERT_TRUE(fileio_write_exact_fd(-1, payload, sizeof(payload)) == ERR);

  char *path = make_tmp_path();
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  ASSERT_TRUE(fd >= 0);

  ASSERT_TRUE(fileio_write_exact_fd(fd, NULL, 1) == ERR);
  ASSERT_TRUE(fileio_write_exact_fd(fd, NULL, 0) == OK);

  close(fd);
  unlink(path);
  free(path);
}

/* Verifies write_atomic rejects invalid arguments. */
static void test_write_atomic_bad_input(void) {
  const uint8_t payload[] = {'x'};
  ASSERT_TRUE(write_atomic(-1, "cfg", payload, sizeof(payload), NULL) == ERR);

  char *dir = make_tmp_dir();
  int dir_fd = open_dir_fd(dir);

  ASSERT_TRUE(write_atomic(dir_fd, NULL, payload, sizeof(payload), NULL) ==
              ERR);
  ASSERT_TRUE(write_atomic(dir_fd, "cfg", NULL, 1, NULL) == ERR);

  close(dir_fd);
  ASSERT_TRUE(rmdir(dir) == 0);
  free(dir);
}

/* Verifies write_atomic supports zero-length payloads and creates empty file.
 */
static void test_write_atomic_zero_len_ok(void) {
  char *dir = make_tmp_dir();
  int dir_fd = open_dir_fd(dir);

  ASSERT_TRUE(write_atomic(dir_fd, "cfg", NULL, 0, NULL) == YES);
  assert_dir_layout(dir, "cfg", 1);

  char *path = path_join(dir, "cfg");
  ASSERT_TRUE(path != NULL);
  struct stat st = {0};
  ASSERT_TRUE(stat(path, &st) == 0);
  ASSERT_TRUE(st.st_size == 0);

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
  close(dir_fd);
  ASSERT_TRUE(rmdir(dir) == 0);
  free(dir);
}

/* Verifies write_atomic overwrites target atomically with full new payload. */
static void test_write_atomic_overwrite_ok(void) {
  char *dir = make_tmp_dir();
  int dir_fd = open_dir_fd(dir);

  uint8_t a[100];
  uint8_t b[100];
  memset(a, 'A', sizeof(a));
  memset(b, 'B', sizeof(b));

  ASSERT_TRUE(write_atomic(dir_fd, "cfg", a, sizeof(a), NULL) == YES);
  ASSERT_TRUE(write_atomic(dir_fd, "cfg", b, sizeof(b), NULL) == YES);
  assert_dir_layout(dir, "cfg", 1);

  char *path = path_join(dir, "cfg");
  ASSERT_TRUE(path != NULL);
  assert_file_uniform_byte(path, 'B', sizeof(b));

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
  close(dir_fd);
  ASSERT_TRUE(rmdir(dir) == 0);
  free(dir);
}

/* Verifies write_atomic returns NO when another process holds the lock file. */
static void test_write_atomic_lock_contention_returns_no(void) {
  char *dir = make_tmp_dir();
  int dir_fd = open_dir_fd(dir);

  const char *file_name = "cfg";
  size_t lname_n = strlen(file_name) + strlen(".lock") + 1;
  char *lock_name = (char *)xmalloc(lname_n);
  snprintf(lock_name, lname_n, "%s.lock", file_name);

  int lfd = openat(dir_fd, lock_name, O_CREAT | O_WRONLY, 0600);
  ASSERT_TRUE(lfd >= 0);
  struct flock fl = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
  ASSERT_TRUE(fcntl(lfd, F_SETLK, &fl) == 0);

  uint8_t payload[100];
  memset(payload, 'X', sizeof(payload));

  pid_t pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    AdbxTriStatus rc =
        write_atomic(dir_fd, file_name, payload, sizeof(payload), NULL);
    if (rc == YES)
      _exit(CHILD_EXIT_YES);
    if (rc == NO)
      _exit(CHILD_EXIT_NO);
    _exit(CHILD_EXIT_ERR);
  }

  ASSERT_TRUE(wait_child_tristatus(pid) == NO);

  ASSERT_TRUE(close(lfd) == 0);
  ASSERT_TRUE(unlinkat(dir_fd, lock_name, 0) == 0);
  free(lock_name);

  assert_dir_layout(dir, NULL, 0);

  close(dir_fd);
  ASSERT_TRUE(rmdir(dir) == 0);
  free(dir);
}

/* Verifies concurrent write_atomic writers never produce partial/corrupt
 * content and leave no lock/temp artifacts. */
static void test_write_atomic_concurrent_writers(void) {
  char *dir = make_tmp_dir();
  int dir_fd = open_dir_fd(dir);

  const char *file_name = "cfg";
  enum { WORKERS = 24, PAYLOAD_LEN = 100 };
  uint8_t worker_bytes[WORKERS];
  for (size_t i = 0; i < WORKERS; i++) {
    worker_bytes[i] = (uint8_t)(8 + i);
  }

  pid_t pids[WORKERS];
  for (size_t i = 0; i < WORKERS; i++) {
    pids[i] = fork();
    ASSERT_TRUE(pids[i] >= 0);
    if (pids[i] == 0) {
      uint8_t payload[PAYLOAD_LEN];
      memset(payload, worker_bytes[i], sizeof(payload));
      AdbxTriStatus rc =
          write_atomic(dir_fd, file_name, payload, sizeof(payload), NULL);
      if (rc == YES)
        _exit(CHILD_EXIT_YES);
      if (rc == NO)
        _exit(CHILD_EXIT_NO);
      _exit(CHILD_EXIT_ERR);
    }
  }

  size_t yes_count = 0;
  size_t no_count = 0;
  size_t err_count = 0;
  for (size_t i = 0; i < WORKERS; i++) {
    AdbxTriStatus rc = wait_child_tristatus(pids[i]);
    if (rc == YES)
      yes_count++;
    else if (rc == NO)
      no_count++;
    else
      err_count++;
  }

  ASSERT_TRUE(yes_count > 0);
  ASSERT_TRUE(no_count + yes_count == WORKERS);
  ASSERT_TRUE(err_count == 0);

  char *path = path_join(dir, file_name);
  ASSERT_TRUE(path != NULL);

  uint8_t out[PAYLOAD_LEN + 1];
  size_t nread = 0;
  ASSERT_TRUE(fileio_read_limit(path, sizeof(out), out, &nread) == OK);
  ASSERT_TRUE(nread == PAYLOAD_LEN);
  for (size_t i = 1; i < nread; i++) {
    ASSERT_TRUE(out[i] == out[0]);
  }

  AdbxTriStatus winner_ok = NO;
  for (size_t i = 0; i < WORKERS; i++) {
    if (out[0] == worker_bytes[i]) {
      winner_ok = YES;
      break;
    }
  }
  ASSERT_TRUE(winner_ok == YES);

  assert_dir_layout(dir, file_name, 1);

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
  close(dir_fd);
  ASSERT_TRUE(rmdir(dir) == 0);
  free(dir);
}

/* Returns nanosecond component from one stat mtime field.
 */
static long stat_mtime_nsec(const struct stat *st) {
  if (!st)
    return 0;
#if defined(__APPLE__)
  return st->st_mtimespec.tv_nsec;
#else
  return st->st_mtim.tv_nsec;
#endif
}

/* Verifies write_atomic populates out_meta with the metadata of the committed
 * file when provided.
 */
static void test_write_atomic_populates_out_meta(void) {
  char *dir = make_tmp_dir();
  int dir_fd = open_dir_fd(dir);

  uint8_t payload[100];
  memset(payload, 'M', sizeof(payload));

  FileMeta got = {0};
  ASSERT_TRUE(write_atomic(dir_fd, "cfg", payload, sizeof(payload), &got) ==
              YES);

  char *path = path_join(dir, "cfg");
  ASSERT_TRUE(path != NULL);

  struct stat st = {0};
  ASSERT_TRUE(stat(path, &st) == 0);

  FileMeta expected = {0};
  expected.exists = 1;
  expected.dev = st.st_dev;
  expected.ino = st.st_ino;
  expected.size = st.st_size;
  expected.mtime_sec = st.st_mtime;
  expected.mtime_nsec = stat_mtime_nsec(&st);

  ASSERT_TRUE(fileio_meta_equal(&got, &expected) == YES);
  ASSERT_TRUE(got.size == (off_t)sizeof(payload));

  ASSERT_TRUE(unlink(path) == 0);
  free(path);
  close(dir_fd);
  ASSERT_TRUE(rmdir(dir) == 0);
  free(dir);
}

/* Verifies core boundary separator behavior for path_join. */
static void test_path_join_core_behavior(void) {
  char expected[32];

  snprintf(expected, sizeof(expected), "a%cb", TEST_PATH_SEP);
  char *out = path_join("a", "b");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected) == 0);
  free(out);

  char dir_with_sep[32];
  snprintf(dir_with_sep, sizeof(dir_with_sep), "a%c", TEST_PATH_SEP);
  out = path_join(dir_with_sep, "b");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected) == 0);
  free(out);

  char file_with_sep[32];
  snprintf(file_with_sep, sizeof(file_with_sep), "%cb", TEST_PATH_SEP);
  out = path_join("a", file_with_sep);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected) == 0);
  free(out);

  out = path_join(dir_with_sep, file_with_sep);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected) == 0);
  free(out);
}

/* Verifies path_join behavior when one or both sides are empty strings. */
static void test_path_join_empty_inputs(void) {
  char *out = path_join("", "b");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, "b") == 0);
  free(out);

  char file_with_sep[32];
  snprintf(file_with_sep, sizeof(file_with_sep), "%cb", TEST_PATH_SEP);
  out = path_join("", file_with_sep);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, file_with_sep) == 0);
  free(out);

  out = path_join("a", "");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, "a") == 0);
  free(out);

  char dir_with_sep[32];
  snprintf(dir_with_sep, sizeof(dir_with_sep), "a%c", TEST_PATH_SEP);
  out = path_join(dir_with_sep, "");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, dir_with_sep) == 0);
  free(out);

  out = path_join("", "");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, "") == 0);
  free(out);
}

/* Verifies root-ish and leading-separator edge cases for path_join. */
static void test_path_join_root_and_leading_separator_cases(void) {
  char root[2] = {TEST_PATH_SEP, '\0'};
  char file_with_sep[32];
  snprintf(file_with_sep, sizeof(file_with_sep), "%cb", TEST_PATH_SEP);
  char expected_root_child[32];
  snprintf(expected_root_child, sizeof(expected_root_child), "%cb",
           TEST_PATH_SEP);

  char *out = path_join(root, "b");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected_root_child) == 0);
  free(out);

  out = path_join(root, file_with_sep);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected_root_child) == 0);
  free(out);

  out = path_join("a", root);
  char expected_a_root[32];
  snprintf(expected_a_root, sizeof(expected_a_root), "a%c", TEST_PATH_SEP);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected_a_root) == 0);
  free(out);
}

/* Verifies path_join only normalizes one separator at the join boundary. */
static void test_path_join_multiple_separators_preserved(void) {
  char dir_double_sep[32];
  snprintf(dir_double_sep, sizeof(dir_double_sep), "a%c%c", TEST_PATH_SEP,
           TEST_PATH_SEP);
  char expected1[32];
  snprintf(expected1, sizeof(expected1), "a%c%cb", TEST_PATH_SEP,
           TEST_PATH_SEP);

  char *out = path_join(dir_double_sep, "b");
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected1) == 0);
  free(out);

  char file_double_sep[32];
  snprintf(file_double_sep, sizeof(file_double_sep), "%c%cb", TEST_PATH_SEP,
           TEST_PATH_SEP);
  char expected2[32];
  snprintf(expected2, sizeof(expected2), "a%c%cb", TEST_PATH_SEP,
           TEST_PATH_SEP);

  out = path_join("a", file_double_sep);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strcmp(out, expected2) == 0);
  free(out);
}

/* Verifies path_join rejects NULL inputs. */
static void test_path_join_null_input(void) {
  ASSERT_TRUE(path_join(NULL, "b") == NULL);
  ASSERT_TRUE(path_join("a", NULL) == NULL);
}

/* Verifies path_join allocates enough space and preserves full payloads for
 * long inputs. */
static void test_path_join_long_inputs(void) {
  size_t dir_len = 10000;
  size_t file_len = 10000;

  char *dir = (char *)malloc(dir_len + 1);
  char *file = (char *)malloc(file_len + 1);
  ASSERT_TRUE(dir != NULL);
  ASSERT_TRUE(file != NULL);

  memset(dir, 'a', dir_len);
  dir[dir_len] = '\0';
  memset(file, 'b', file_len);
  file[file_len] = '\0';

  char *out = path_join(dir, file);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(strlen(out) == dir_len + 1 + file_len);
  ASSERT_TRUE(out[0] == 'a');
  ASSERT_TRUE(out[dir_len - 1] == 'a');
  ASSERT_TRUE(out[dir_len] == TEST_PATH_SEP);
  ASSERT_TRUE(out[dir_len + 1] == 'b');
  ASSERT_TRUE(out[dir_len + file_len] == 'b');

  free(out);
  free(file);
  free(dir);
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
  test_sb_read_limit_fd_ok();
  test_sb_read_limit_fd_over_limit_fails();
  test_sb_read_limit_fd_bad_input();
  test_read_limit_raw_ok();
  test_read_limit_raw_over_limit_fails();
  test_read_up_to_raw_bad_input();
  test_read_up_to_raw_shorter_than_cap();
  test_read_up_to_raw_longer_than_cap();
  test_write_exact_bad_input();
  test_write_exact_ok();
  test_write_exact_truncates_existing();
  test_write_exact_zero_size_ok();
  test_write_exact_fd_ok();
  test_write_exact_fd_bad_input();
  test_write_atomic_bad_input();
  test_write_atomic_zero_len_ok();
  test_write_atomic_overwrite_ok();
  test_write_atomic_lock_contention_returns_no();
  test_write_atomic_populates_out_meta();
  for (int i = 0; i < 10; i++) {
    test_write_atomic_concurrent_writers();
  }
  test_path_join_core_behavior();
  test_path_join_empty_inputs();
  test_path_join_root_and_leading_separator_cases();
  test_path_join_multiple_separators_preserved();
  test_path_join_null_input();
  test_path_join_long_inputs();

  fprintf(stderr, "OK: test_file_io\n");
  return 0;
}
