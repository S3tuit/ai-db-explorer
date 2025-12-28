#ifndef BUFIO_H
#define BUFIO_H

#include <stddef.h>
#include <stdint.h>

#include "byte_channel.h"
#include "string_op.h"


/* Buffered I/O on top of a ByteChannel. This layer is protocol-agnostic. */

typedef struct BufReader {
    ByteChannel *ch;   // owned; destroyed in bufreader_destroy
    StrBuf buf;        // read buffer
    size_t rpos;       // read position within buf.data [0..buf.len]
    int eof;           // sticky EOF flag (seen 0 from read_some)
} BufReader;

typedef struct BufWriter {
    ByteChannel *ch;   // owned; destroyed in bufwriter_destroy
} BufWriter;


/* ------------------------------- Reader ---------------------------------- */
BufReader *bufreader_create(ByteChannel *ch);

/* cleanup and close/destroy ByteChannel */
void bufreader_destroy(BufReader *br);

// Ensures at least 'need' bytes are available to peek/consume from 'br'.
// Returns YES/NO/ERR, and sets EOF state if peer closes.
int bufreader_ensure(BufReader *br, size_t need);

// Returns pointer to contiguous available data and its length inside 'out_val'.
// The returned pointer is valid until next ensure/read. Returns NULL if there's
// nothing to peek.
const uint8_t *bufreader_peek(const BufReader *br, size_t *out_avail);

// Copies exactly 'n' bytes into 'dst', consuming them.
int bufreader_read_n(BufReader *br, void *dst, size_t n);

// Finds a byte pattern in the buffered data. Returns index (offset from current
// read position) if found, else -1. This is not efficient for long patterns.
ssize_t bufreader_find(const BufReader *br, const void *needle, size_t needle_len);


/* ------------------------------- Writer ---------------------------------- */
BufWriter *bufwriter_create(ByteChannel *ch);

/* cleanup and close/destroy ByteChannel */
void bufwriter_destroy(BufWriter *bw);

/* Writes all 'n' bytes from 'src' to 'bw'. Returns ok/err. */
int bufwriter_write_all(BufWriter *bw, const void *src, size_t n);

#endif
