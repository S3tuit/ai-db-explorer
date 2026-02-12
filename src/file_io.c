#include "file_io.h"

#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#define FILEIO_READ_CHUNK 4096u

/* Validates shared input constraints for bounded file reads.
 * Ownership: borrows 'path'; does not allocate.
 * Side effects: none.
 * Error semantics: returns OK on valid input, ERR otherwise.
 */
static int fileio_validate_common(const char *path, size_t max_bytes) {
  if (!path)
    return ERR;
  if (max_bytes > STRBUF_MAX_BYTES || max_bytes > (size_t)SSIZE_MAX)
    return ERR;
  return OK;
}

/* Reads a file into a StrBuf with optional strict EOF enforcement.
 * Ownership: borrows 'path'; writes into caller-owned 'out' and 'out_nread'.
 * Side effects: performs filesystem I/O and mutates StrBuf 'out'.
 * Error semantics: returns OK on success, ERR on invalid input, I/O failure,
 * or strict-over-limit failure when 'require_eof' is YES.
 */
static int fileio_sb_read_impl(const char *path, size_t max_bytes, StrBuf *out,
                               int require_eof, ssize_t *out_nread) {
  if (!out || !out_nread || fileio_validate_common(path, max_bytes) != OK)
    return ERR;

  sb_clean(out);
  *out_nread = -1;

  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return ERR;

  int rc = OK;
  size_t total = 0;
  uint8_t buf[FILEIO_READ_CHUNK];

  for (;;) {
    if (total == max_bytes) {
      if (require_eof == YES) {
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

  if (close(fd) != 0)
    rc = ERR;
  if (rc != OK) {
    sb_clean(out);
    return ERR;
  }

  *out_nread = (ssize_t)total;
  return OK;
}

/* Reads a file into caller-owned byte buffer with optional strict EOF check.
 * Ownership: borrows 'path'; writes into caller-owned 'out' and 'out_nread'.
 * Side effects: performs filesystem I/O.
 * Error semantics: returns OK on success, ERR on invalid input, I/O failure,
 * or strict-over-limit failure when 'require_eof' is YES.
 */
static int fileio_raw_read_impl(const char *path, size_t max_bytes,
                                uint8_t *out, int require_eof,
                                size_t *out_nread) {
  if (!out_nread || fileio_validate_common(path, max_bytes) != OK)
    return ERR;
  if (!out && max_bytes != 0)
    return ERR;

  *out_nread = 0;

  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return ERR;

  int rc = OK;
  size_t total = 0;

  for (;;) {
    if (total == max_bytes) {
      if (require_eof == YES) {
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
      }
      break;
    }

    size_t chunk_cap = max_bytes - total;
    if (chunk_cap > FILEIO_READ_CHUNK)
      chunk_cap = FILEIO_READ_CHUNK;

    ssize_t n = read(fd, out + total, chunk_cap);
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
    total += un;
  }

  if (close(fd) != 0)
    rc = ERR;
  if (rc != OK)
    return ERR;

  *out_nread = total;
  return OK;
}

int fileio_sb_read_limit(const char *path, size_t max_bytes, StrBuf *out) {
  ssize_t nread = -1;
  if (fileio_sb_read_impl(path, max_bytes, out, YES, &nread) != OK)
    return ERR;
  return OK;
}

ssize_t fileio_sb_read_up_to(const char *path, size_t max_bytes, StrBuf *out) {
  ssize_t nread = -1;
  if (fileio_sb_read_impl(path, max_bytes, out, NO, &nread) != OK)
    return -1;
  return nread;
}

int fileio_read_limit(const char *path, size_t max_bytes, uint8_t *out,
                      size_t *out_nread) {
  return fileio_raw_read_impl(path, max_bytes, out, YES, out_nread);
}

ssize_t fileio_read_up_to(const char *path, size_t max_bytes, uint8_t *out) {
  size_t nread = 0;
  if (fileio_raw_read_impl(path, max_bytes, out, NO, &nread) != OK)
    return -1;
  return (ssize_t)nread;
}

int fileio_write_exact(const char *path, const uint8_t *src, size_t size,
                       mode_t mode) {
  if (!path)
    return ERR;
  if (!src && size != 0)
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

  size_t off = 0;
  while (off < size) {
    ssize_t n = write(fd, src + off, size - off);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      (void)close(fd);
      (void)unlink(path);
      return ERR;
    }
    if (n == 0) {
      (void)close(fd);
      (void)unlink(path);
      return ERR;
    }
    off += (size_t)n;
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
