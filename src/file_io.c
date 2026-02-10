#include "file_io.h"

#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#define FILEIO_READ_CHUNK 4096u

/* Reads the full file from 'path' into 'out' and enforces 'max_bytes'.
 * Ownership: borrows 'path'; 'out' is caller-owned and reset by this function.
 * Side effects: performs filesystem I/O and mutates StrBuf 'out'.
 * Error semantics: returns OK on success, ERR on invalid input, I/O error, or
 * if file size exceeds 'max_bytes'. */
int fileio_read_all_limit(const char *path, size_t max_bytes, StrBuf *out) {
  if (!path || !out)
    return ERR;
  if (max_bytes > STRBUF_MAX_BYTES)
    return ERR;

  sb_clean(out);

  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return ERR;

  size_t total = 0;
  uint8_t buf[FILEIO_READ_CHUNK];
  for (;;) {
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n == 0)
      break;
    if (n < 0) {
      if (errno == EINTR)
        continue;
      close(fd);
      sb_clean(out);
      return ERR;
    }

    size_t un = (size_t)n;
    // Prevent overflow and enforce hard cap before appending bytes.
    if (un > max_bytes || total > max_bytes - un) {
      close(fd);
      sb_clean(out);
      return ERR;
    }

    if (sb_append_bytes(out, buf, un) != OK) {
      close(fd);
      sb_clean(out);
      return ERR;
    }
    total += un;
  }

  if (close(fd) != 0) {
    sb_clean(out);
    return ERR;
  }
  return OK;
}
