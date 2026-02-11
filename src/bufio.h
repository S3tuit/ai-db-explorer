#ifndef BUFIO_H
#define BUFIO_H

#include <stddef.h>
#include <stdint.h>

#include "byte_channel.h"
#include "string_op.h"

/* Buffered I/O on top of a ByteChannel. This layer is protocol-agnostic.
 *
 * Ownership: BufChannel always owns its ByteChannel and will destroy it.
 * Use stdio_bytechannel_wrap_fd when you need a non-owning ByteChannel.
 *
 * Pattern:
 * - One BufChannel per full-duplex transport (read + write on same fd).
 * - Use separate BufChannels only when the underlying fds differ
 *   (e.g., stdin and stdout).
 */

typedef struct BufChannel {
  ByteChannel *ch; // owned; destroyed in bufch_destroy
  StrBuf buf;      // read buffer
  size_t rpos;     // read position within buf.data [0..buf.len]
  int eof;         // sticky EOF flag (seen 0 from read_some)
} BufChannel;

/* Initializes a BufChannel without allocating it. */
int bufch_init(BufChannel *bc, ByteChannel *ch);

BufChannel *bufch_create(ByteChannel *ch);

/* cleanup and close/destroy ByteChannel */
void bufch_clean(BufChannel *bc);

/* cleanup and close/destroy ByteChannel */
void bufch_destroy(BufChannel *bc);

// Ensures at least 'need' bytes are available to peek/consume from 'bc'.
// Returns YES/NO/ERR, and sets EOF state if peer closes.
int bufch_ensure(BufChannel *bc, size_t need);

// Returns a pointer to available data buffered but not read and stores its
// length inside 'out_val'. The returned pointer is valid until next
// ensure/read. Returns NULL if there's nothing to peek.
// NOTE: peek consider data already buffered, if this returns NULL it doesn't
// mean it's EOF, call ensure for that.
const uint8_t *bufch_peek(const BufChannel *bc, size_t *out_avail);

/* Copies exactly 'n' bytes into 'dst', consuming them.
 * Ownership: borrows 'bc'; writes into caller-owned 'dst'.
 * Side effects: may read from underlying channel and advances buffered read
 * position.
 * Error semantics: returns OK on success, ERR on invalid input or short read.
 */
int bufch_read_exact(BufChannel *bc, void *dst, size_t n);

/* Reads up to 'max_n' bytes into 'dst', stopping at EOF or once max is met.
 * Ownership: borrows 'bc'; writes into caller-owned 'dst'.
 * Side effects: may read from underlying channel and advances buffered read
 * position.
 * Error semantics: returns -1 on invalid input or read error; otherwise returns
 * number of bytes copied in [0..max_n].
 */
ssize_t bufch_read_until(BufChannel *bc, void *dst, size_t max_n);

// Finds a byte pattern in the buffered data. Returns index (offset from current
// read position) if found, else -1. This is not efficient for long patterns.
ssize_t bufch_find_buffered(const BufChannel *bc, const void *needle,
                            size_t needle_len);
// Finds a byte pattern within a bounded window and reads as needed to decide.
// Returns index if found, -1 if not found within the window or error.
ssize_t bufch_findn(BufChannel *bc, const void *needle, size_t needle_len,
                    size_t max_dist);

/* Writes all 'n' bytes from 'src' to 'bc'. Returns ok/err. */
int bufch_write_all(BufChannel *bc, const void *src, size_t n);

/* Writes header + payload with a vector fast path when available. */
int bufch_write2v(BufChannel *bc, const void *h, size_t hlen, const void *p,
                  size_t plen);

/*---------------------------------- Helpers --------------------------------*/

#include "stdio_byte_channel.h"

#define bufch_stdio_init(bufch, in_path, out_path)                             \
  bufch_init(bufch, stdio_bytechannel_open_path(in_path, out_path))

#endif
