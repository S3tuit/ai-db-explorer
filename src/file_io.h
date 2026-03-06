#ifndef FILE_IO_H
#define FILE_IO_H

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "string_op.h"
#include "utils.h"

/* Fast, one-shot read. Reads the full file from 'path' into 'out' and enforces
 * 'max_bytes'. Ownership: borrows 'path'; 'out' is caller-owned and reset by
 * this function. Returns OK on success, ERR on invalid input, I/O
 * error, or if file size exceeds 'max_bytes'. */
AdbxStatus fileio_sb_read_limit(const char *path, size_t max_bytes,
                                StrBuf *out);

/* Same as 'fileio_sb_read_limit' but reads from an already opened 'fd'. */
AdbxStatus fileio_sb_read_limit_fd(int fd, size_t max_bytes, StrBuf *out);

/* Fast, one-shot read. Reads exactly 'n_bytes' from 'path' into 'out' and
 * fails unless file size is exactly 'n_bytes'.
 * Ownership: borrows 'path'; writes into caller-owned 'out'.
 * Side effects: performs filesystem I/O.
 * Error semantics: returns OK on success, ERR on invalid input, I/O error, or
 * when file size differs from 'n_bytes'. */
AdbxStatus fileio_read_exact(const char *path, size_t n_bytes, uint8_t *out);

/* ----------------------------------- write ------------------------------- */

// holds the metadata to understand whether a file is changed from the last
// snapshot
typedef struct {
  int exists;
  dev_t dev;
  ino_t ino;
  off_t size;
  time_t mtime_sec;
  long mtime_nsec;
} FileMeta;

/* Stores file identity metadata from one stat struct.
 * It borrows 'st' and writes to caller-owned 'out'.
 * Error semantics: returns OK on success, ERR on invalid input.
 */
AdbxStatus fileio_meta_from_stat(const struct stat *st, FileMeta *out);

/* Compares 2 file metadata snapshots.
 * It borrows both inputs and performs no allocations.
 * Error semantics: returns YES when equal, NO when different, ERR on invalid
 * input.
 */
AdbxTriStatus fileio_meta_equal(const FileMeta *a, const FileMeta *b);

/* Writes exactly 'size' bytes to 'path' from 'src' with strict 'mode' policy.
 * Ownership: borrows 'path' and 'src'; no allocations.
 * Side effects: opens/writes/chmods/closes file and may unlink on failure.
 * Error semantics: returns OK on success, ERR on invalid input or I/O failure.
 * 'src' may be NULL only when 'size' is 0. 'mode' must use permission bits
 * only (0..0777). Note: this may not work if writing to symlink.
 */
AdbxStatus fileio_write_exact(const char *path, const uint8_t *src, size_t size,
                              mode_t mode);

/* Same as 'fileio_write_exact' but writes to an already opened 'fd' */
AdbxStatus fileio_write_exact_fd(int fd, const uint8_t *src, size_t size);

/* Executes an atomic write inside 'dir_fd' at 'file_name' with 'len' bytes from
 * 'data'. Returns YES on success, NO if there's another process trying to
 * concurrently writing 'file_name', ERR on any filesystem failure. If
 * 'out_meta' is not NULL, it get populated with the metadata of the file
 * written.
 */
AdbxTriStatus write_atomic(int dir_fd, const char *file_name,
                           const uint8_t *data, size_t len, FileMeta *out_meta);

/* Joins 'dir' and 'path' into a caller owned string representing a path.
 * Returns a heap-allocated string of a valid path or NULL. */
char *path_join(const char *dir, const char *file);

#endif
