#include "file_io.h"

#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FILEIO_READ_CHUNK 4096u

/* Validates shared max-bytes constraints for bounded reads.
 * Error semantics: returns OK on valid input, ERR otherwise.
 */
static AdbxStatus fileio_validate_max_bytes(size_t max_bytes) {
  if (max_bytes > STRBUF_MAX_BYTES || max_bytes > (size_t)SSIZE_MAX)
    return ERR;
  return OK;
}

/* Validates shared input constraints for bounded file reads.
 * Ownership: borrows 'path'; does not allocate.
 * Side effects: none.
 * Error semantics: returns OK on valid input, ERR otherwise.
 */
static AdbxStatus fileio_validate_common(const char *path, size_t max_bytes) {
  if (!path)
    return ERR;
  return fileio_validate_max_bytes(max_bytes);
}

/* Reads from one open fd into StrBuf and fails when content exceeds max_bytes.
 * Ownership: borrows 'fd'; writes into caller-owned 'out' and 'out_nread'.
 * Side effects: performs read(2) calls and mutates StrBuf 'out'.
 * Error semantics: returns OK on success, ERR on invalid input, I/O failure,
 * or when file size exceeds 'max_bytes'.
 */
static AdbxStatus fileio_sb_read_fd_impl(int fd, size_t max_bytes, StrBuf *out,
                                         ssize_t *out_nread) {
  if (fd < 0 || !out || !out_nread ||
      fileio_validate_max_bytes(max_bytes) != OK)
    return ERR;

  sb_clean(out);
  *out_nread = -1;

  AdbxStatus rc = OK;
  size_t total = 0;
  uint8_t buf[FILEIO_READ_CHUNK];

  for (;;) {
    if (total == max_bytes) {
      uint8_t probe = 0;
      ssize_t pn = 0;
      for (;;) {
        pn = read(fd, &probe, 1);
        if (pn < 0 && errno == EINTR)
          continue;
        break;
      }
      if (pn < 0 || pn > 0) {
        rc = ERR;
        break;
      }
      break;
    }

    size_t chunk_cap = max_bytes - total;
    if (chunk_cap > sizeof(buf))
      chunk_cap = sizeof(buf);

    ssize_t n = read(fd, buf, chunk_cap);
    if (n == 0)
      break;
    if (n < 0) {
      if (errno == EINTR)
        continue;
      rc = ERR;
      break;
    }

    size_t un = (size_t)n;
    if (un > max_bytes - total || total > (size_t)SSIZE_MAX - un) {
      rc = ERR;
      break;
    }
    if (sb_append_bytes(out, buf, un) != OK) {
      rc = ERR;
      break;
    }
    total += un;
  }

  if (rc != OK) {
    sb_clean(out);
    return ERR;
  }

  *out_nread = (ssize_t)total;
  return OK;
}

/* Reads one file path into StrBuf and fails when content exceeds max_bytes.
 * Ownership: borrows 'path'; writes into caller-owned 'out' and 'out_nread'.
 * Side effects: opens/reads/closes file and mutates StrBuf 'out'.
 * Error semantics: returns OK on success, ERR on invalid input, I/O failure,
 * or when file size exceeds 'max_bytes'.
 */
static AdbxStatus fileio_sb_read_impl(const char *path, size_t max_bytes,
                                      StrBuf *out, ssize_t *out_nread) {
  if (!out_nread || fileio_validate_common(path, max_bytes) != OK)
    return ERR;

  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return ERR;

  AdbxStatus rc = fileio_sb_read_fd_impl(fd, max_bytes, out, out_nread);
  if (close(fd) != 0)
    rc = ERR;
  if (rc != OK) {
    if (out)
      sb_clean(out);
    *out_nread = -1;
    return ERR;
  }
  return OK;
}

AdbxStatus fileio_sb_read_limit(const char *path, size_t max_bytes,
                                StrBuf *out) {
  ssize_t nread = -1;
  if (fileio_sb_read_impl(path, max_bytes, out, &nread) != OK)
    return ERR;
  return OK;
}

AdbxStatus fileio_sb_read_limit_fd(int fd, size_t max_bytes, StrBuf *out) {
  ssize_t nread = -1;
  if (fileio_sb_read_fd_impl(fd, max_bytes, out, &nread) != OK)
    return ERR;
  return OK;
}

AdbxStatus fileio_read_exact(const char *path, size_t n_bytes, uint8_t *out) {
  if (fileio_validate_common(path, n_bytes) != OK)
    return ERR;
  if (!out && n_bytes != 0)
    return ERR;

  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return ERR;

  AdbxStatus rc = OK;
  size_t total = 0;

  while (total < n_bytes) {
    ssize_t n = read(fd, out + total, n_bytes - total);
    if (n == 0) {
      rc = ERR;
      break;
    }
    if (n < 0) {
      if (errno == EINTR)
        continue;
      rc = ERR;
      break;
    }

    size_t un = (size_t)n;
    if (un > n_bytes - total || total > (size_t)SSIZE_MAX - un) {
      rc = ERR;
      break;
    }
    total += un;
  }

  if (rc == OK) {
    uint8_t probe = 0;
    ssize_t pn = 0;
    for (;;) {
      pn = read(fd, &probe, 1);
      if (pn < 0 && errno == EINTR)
        continue;
      break;
    }
    if (pn < 0 || pn > 0)
      rc = ERR;
  }

  if (close(fd) != 0)
    rc = ERR;
  return rc;
}

/* --------------------------------- write --------------------------------- */

/* Returns nanosecond component from one stat mtime field.
 */
static long fileio_mtime_nsec(const struct stat *st) {
  if (!st)
    return 0;
#if defined(__APPLE__)
  return st->st_mtimespec.tv_nsec;
#else
  return st->st_mtim.tv_nsec;
#endif
}

AdbxStatus fileio_meta_from_stat(const struct stat *st, FileMeta *out) {
  if (!st || !out)
    return ERR;
  out->exists = 1;
  out->dev = st->st_dev;
  out->ino = st->st_ino;
  out->size = st->st_size;
  out->mtime_sec = st->st_mtime;
  out->mtime_nsec = fileio_mtime_nsec(st);
  return OK;
}

AdbxTriStatus fileio_meta_equal(const FileMeta *a, const FileMeta *b) {
  if (!a || !b)
    return ERR;

  if (a->exists != b->exists)
    return NO;
  if (!a->exists)
    return YES;

  if (a->dev != b->dev)
    return NO;
  if (a->ino != b->ino)
    return NO;
  if (a->size != b->size)
    return NO;
  if (a->mtime_sec != b->mtime_sec)
    return NO;
  if (a->mtime_nsec != b->mtime_nsec)
    return NO;

  return YES;
}

AdbxStatus fileio_write_exact(const char *path, const uint8_t *src, size_t size,
                              mode_t mode) {
  if (!path)
    return ERR;
  if ((mode & ~(mode_t)0777) != 0)
    return ERR;

  int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  int fd = open(path, flags, 0600);
  if (fd < 0)
    return ERR;

  if (fileio_write_exact_fd(fd, src, size) != OK) {
    (void)close(fd);
    (void)unlink(path);
    return ERR;
  }

  if (fchmod(fd, mode) != 0) {
    (void)close(fd);
    (void)unlink(path);
    return ERR;
  }

  if (close(fd) != 0) {
    (void)unlink(path);
    return ERR;
  }
  return OK;
}

AdbxStatus fileio_write_exact_fd(int fd, const uint8_t *src, size_t size) {
  if (fd < 0)
    return ERR;
  if (!src && size != 0)
    return ERR;

  size_t off = 0;
  while (off < size) {
    ssize_t n = write(fd, src + off, size - off);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return ERR;
    }
    if (n == 0)
      return ERR;
    off += (size_t)n;
  }
  return OK;
}

#ifdef _WIN32
#define PATH_JOIN_SEPARATOR '\\'
#define PATH_JOIN_SEPARATOR_STR "\\"
#else
#define PATH_JOIN_SEPARATOR '/'
#define PATH_JOIN_SEPARATOR_STR "/"
#endif

char *path_join(const char *dir, const char *file) {
  if (dir == NULL || file == NULL)
    return NULL;

  size_t dir_len = strlen(dir);
  size_t file_len = strlen(file);

  int dir_has_sep = (dir_len > 0 && dir[dir_len - 1] == PATH_JOIN_SEPARATOR);
  int file_has_sep = (file_len > 0 && file[0] == PATH_JOIN_SEPARATOR);

  // We want exactly one separator between dir and file (unless one side is
  // empty).
  size_t need_sep = 0;
  size_t skip_file = 0;

  if (dir_len > 0 && file_len > 0) {
    if (dir_has_sep && file_has_sep) {
      skip_file = 1; // avoid double separator
    } else if (!dir_has_sep && !file_has_sep) {
      need_sep = 1; // add missing separator
    }
  }

  // total length = dir + (sep?) + (file minus skipped leading sep) + '\0'
  size_t file_part_len = file_len - skip_file;
  if (dir_len > SIZE_MAX - file_part_len)
    return NULL;
  size_t out_len = dir_len + file_part_len;
  if (need_sep && out_len == SIZE_MAX)
    return NULL;
  out_len += need_sep;

  char *buf = (char *)xmalloc(out_len + 1);

  // Copy dir
  if (dir_len > 0)
    memcpy(buf, dir, dir_len);

  size_t pos = dir_len;

  // Add separator if needed
  if (need_sep) {
    buf[pos++] = PATH_JOIN_SEPARATOR;
  }

  // Copy file (skipping leading separator if needed)
  if (file_len > skip_file) {
    memcpy(buf + pos, file + skip_file, file_len - skip_file);
    pos += (file_len - skip_file);
  }

  buf[pos] = '\0';
  return buf;
}

/* ---------------------------------- write atomic ------------------------- */
#define LOCK_SUFFIX ".lock"
// exclusive write lock
struct exwr_lock {
  int fd;
  int dir_fd;
  char *name;
};

/* Releases and cleans up the resources of 'lock'. */
static void release_lock(struct exwr_lock *lock) {
  if (!lock || lock->fd < 0 || lock->dir_fd < 0 || !lock->name)
    return;
  unlinkat(lock->dir_fd, lock->name, 0);
  close(lock->fd);
  free(lock->name);
}

/* Acquires a lock for writing to 'file_name' inside 'dir_fd'. Returns YES if
 * suceed and populates 'out_lfd'. Caller must call release_lock(out_lock) to
 * release lock. Returns NO if there's another process holding the lock, ERR if
 * system failure or invalid input. */
static AdbxTriStatus acquire_lock(int dir_fd, const char *file_name,
                                  struct exwr_lock *out_lock) {
  if (dir_fd < 0 || !file_name || !out_lock)
    return ERR;

  char *lname; // allocated string of the filename used to control locking
  size_t fname_len = strlen(file_name);
  // sizeof(LOCK_SUFFIX) includes the null terminator already
  lname = xmalloc(fname_len + sizeof(LOCK_SUFFIX));
  memcpy(lname, file_name, fname_len);
  memcpy(lname + fname_len, LOCK_SUFFIX, sizeof(LOCK_SUFFIX));

  int flags = O_CREAT | O_WRONLY;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif

  int rc = ERR;
  int l_fd = openat(dir_fd, lname, flags, 0600);
  if (l_fd < 0)
    goto err;

  struct flock exclusive_lock = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};

  if (fcntl(l_fd, F_SETLK, &exclusive_lock) < 0) {
    if (errno == EACCES || errno == EAGAIN) {
      rc = NO;
    }
    goto err_fd;
  }

  out_lock->fd = l_fd;
  out_lock->dir_fd = dir_fd;
  out_lock->name = lname;
  return YES;

err_fd:
  close(l_fd);
err:
  free(lname);
  return rc;
}

#define TMP_FILE_HEX_LEN_BYTES 32
#define TMP_FILE_OPEN_RETRIES 4
/* Executes an atomic write inside 'dir_fd' at 'file_name' with 'len' bytes from
 * 'data'. Returns YES on success, NO if there's another
 * process trying to concurrently writing 'file_name', ERR on any filesystem
 * failure.
 */
AdbxTriStatus write_atomic(int dir_fd, const char *file_name,
                           const uint8_t *data, size_t len,
                           FileMeta *out_meta) {
  if (dir_fd < 0 || !file_name || (!data && len != 0))
    return ERR;

  struct exwr_lock lock = {0};
  int rc = acquire_lock(dir_fd, file_name, &lock);
  if (rc != YES)
    return rc;

  // track cleanup state of the temporary file
  int tmp_exists = 0;
  int tmp_fd = -1;

  // tmp_name is a NUL-terminated filename generated from random hex chars.
  char tmp_name[TMP_FILE_HEX_LEN_BYTES + 1] = {0};
  int flags = O_CREAT | O_EXCL | O_WRONLY;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif

  for (int i = 0; i < TMP_FILE_OPEN_RETRIES; i++) {
    if (fill_random_hex(tmp_name, TMP_FILE_HEX_LEN_BYTES) != OK)
      goto err;
    tmp_name[TMP_FILE_HEX_LEN_BYTES] = '\0';

    tmp_fd = openat(dir_fd, tmp_name, flags, 0600);
    if (tmp_fd >= 0)
      break;
    if (errno != EEXIST)
      goto err;
  }
  if (tmp_fd < 0)
    goto err;

  tmp_exists = 1;
  if (fileio_write_exact_fd(tmp_fd, data, len) != OK) {
    goto err;
  }

  if (fsync(tmp_fd) != 0) {
    goto err;
  }

  // we can get the metadata of the tmp file because what changes on rename is
  // ctime and FileMeta doesn't track it
  if (out_meta) {
    struct stat st = {0};
    if (fstat(tmp_fd, &st) != 0)
      goto err;
    if (fileio_meta_from_stat(&st, out_meta) != OK)
      goto err;
  }

  rc = close(tmp_fd);
  tmp_fd = -1; // so we won't close in cleanup
  if (rc != 0) {
    goto err;
  }

  if (renameat(dir_fd, tmp_name, dir_fd, file_name) != 0) {
    goto err;
  }
  tmp_exists = 0;

  if (fsync(dir_fd) != 0) {
    goto err;
  }

  release_lock(&lock);
  return YES;

err:
  release_lock(&lock);
  if (tmp_fd >= 0)
    (void)close(tmp_fd);
  if (tmp_exists)
    (void)unlinkat(dir_fd, tmp_name, 0);
  return ERR;
}
