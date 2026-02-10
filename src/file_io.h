#ifndef FILE_IO_H
#define FILE_IO_H

#include <stddef.h>

#include "string_op.h"

/* Reads the full file from 'path' into 'out' and enforces 'max_bytes'.
 * Ownership: borrows 'path'; 'out' is caller-owned and reset by this function.
 * Side effects: performs filesystem I/O and mutates StrBuf 'out'.
 * Error semantics: returns OK on success, ERR on invalid input, I/O error, or
 * if file size exceeds 'max_bytes'. */
int fileio_read_all_limit(const char *path, size_t max_bytes, StrBuf *out);

#endif
