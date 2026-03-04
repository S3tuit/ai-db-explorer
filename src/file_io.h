#ifndef FILE_IO_H
#define FILE_IO_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "string_op.h"
#include "utils.h"

/* Fast, one-shot read. Reads the full file from 'path' into 'out' and enforces
 * 'max_bytes'. Ownership: borrows 'path'; 'out' is caller-owned and reset by
 * this function. Side effects: performs filesystem I/O and mutates StrBuf
 * 'out'. Error semantics: returns OK on success, ERR on invalid input, I/O
 * error, or if file size exceeds 'max_bytes'. */
AdbxStatus fileio_sb_read_limit(const char *path, size_t max_bytes,
                                StrBuf *out);

/* Same as 'fileio_sb_read_limit' but reads from an already opened 'fd'. */
AdbxStatus fileio_sb_read_limit_fd(int fd, size_t max_bytes, StrBuf *out);

/* Fast, one-shot read. Reads up to 'max_bytes' from 'path' into 'out'.
 * Ownership: borrows 'path'; 'out' is caller-owned and reset by this function.
 * Side effects: performs filesystem I/O and mutates StrBuf 'out'.
 * Error semantics: returns -1 on invalid input or I/O error; otherwise returns
 * number of bytes read in [0..max_bytes]. */
ssize_t fileio_sb_read_up_to(const char *path, size_t max_bytes, StrBuf *out);

/* Fast, one-shot read. Reads the full file from 'path' into 'out' with a hard
 * cap of 'max_bytes'. Ownership: borrows 'path'; writes into caller-owned 'out'
 * and 'out_nread'. Side effects: performs filesystem I/O.
 * Error semantics: returns OK on success, ERR on invalid input, I/O error, or
 * when file size exceeds 'max_bytes'. */
AdbxStatus fileio_read_limit(const char *path, size_t max_bytes, uint8_t *out,
                             size_t *out_nread);

/* Fast, one-shot read. Reads up to 'max_bytes' bytes from 'path' into 'out'.
 * Ownership: borrows 'path'; writes into caller-owned 'out'.
 * Side effects: performs filesystem I/O.
 * Error semantics: returns -1 on invalid input or I/O error; otherwise returns
 * number of bytes read in [0..max_bytes]. */
ssize_t fileio_read_up_to(const char *path, size_t max_bytes, uint8_t *out);

/* Writes exactly 'size' bytes to 'path' from 'src' with strict 'mode' policy.
 * Ownership: borrows 'path' and 'src'; no allocations.
 * Side effects: opens/writes/chmods/closes file and may unlink on failure.
 * Error semantics: returns OK on success, ERR on invalid input or I/O failure.
 * 'src' may be NULL only when 'size' is 0. 'mode' must use permission bits
 * only (0..0777). Note: this may not work if writing to symlink.
 */
AdbxStatus fileio_write_exact(const char *path, const uint8_t *src, size_t size,
                              mode_t mode);

/* Joins 'dir' and 'path' into a caller owned string representing a path.
 * Returns a heap-allocated string of a valid path or NULL. */
char *path_join(const char *dir, const char *file);

#endif
