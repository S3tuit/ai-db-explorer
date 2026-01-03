#ifndef BYTE_CHANNEL_H
#define BYTE_CHANNEL_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "utils.h"

/* A ByteChannel is a thin abstraction over a byte stream endpoint:
 * - Unix: file descriptor (socket, pipe, stdin/stdout, etc.)
 * - Windows: HANDLE (named pipe, file, etc.)
 *
 * It supports partial reads (read_some) and full writes (write_all loops).
 *
 * Ownership:
 * - ByteChannel implementations may own the underlying resource.
 * - bytech_destroy() always frees the ByteChannel; it will close the underlying
 *   resource only if the implementation owns it.
 */

typedef struct ByteChannel ByteChannel;

/* Cross-platform "pollable" handle.
 * - On Unix, this is typically an int fd cast to intptr_t.
 * - On Windows, this is typically a HANDLE cast to intptr_t.
 * The type is intentionally opaque; platform code knows what it means.
 */
typedef intptr_t BytePollable;

/* Portable vector write descriptor. */
typedef struct ByteChannelVec {
    const void *base;
    size_t len;
} ByteChannelVec;

typedef struct ByteChannelVTable {
    // Reads up to 'cap' bytes into 'buf'. Returns:
    //  >0 : bytes read
    //   0 : EOF (peer closed cleanly)
    //  -1 : error (errno/GetLastError-like; impl decides how it's exposed)
    ssize_t (*read_some)(ByteChannel *ch, void *buf, size_t cap);

    // Writes up to 'len' bytes from 'buf'. Returns the number of bytes read or
    // -1 if error.
    ssize_t (*write_some)(ByteChannel *ch, const void *buf, size_t len);

    // Optional: vector write. Returns bytes written or -1 on error.
    // When NULL, callers should fallback to write_some.
    ssize_t (*writev_some)(ByteChannel *ch, const ByteChannelVec *vecs, int vcnt);

    // Optional: flush buffered output if any. Returns ok/err.
    // For sockets/pipes implement to NULL, then bytech_flush helper returns ok.
    int (*flush)(ByteChannel *ch);
    
    // Half-close: indicates "no more writes". Returns ok/error.
    int (*shutdown_write)(ByteChannel *ch);

    // Returns an OS pollable handle for multiplexing. The handle should be the
    // one used to poll for readability (data available to read)
    BytePollable (*get_pollable)(const ByteChannel *ch);

    // Frees the ByteChannel allocation.
    void (*destroy)(ByteChannel *ch);
} ByteChannelVTable;

struct ByteChannel {
    const ByteChannelVTable *vt;
    void *impl; // channel-specific
};

// Add the helpers to DbBackend interface too
/* Small helpers */
static inline ssize_t bytech_read_some(ByteChannel *ch, void *buf, size_t cap) {
    return ch->vt->read_some(ch, buf, cap);
}
static inline ssize_t bytech_write_some(ByteChannel *ch, const void *buf, size_t len) {
    return ch->vt->write_some(ch, buf, len);
}
static inline ssize_t bytech_writev_some(ByteChannel *ch,
        const ByteChannelVec *vecs, int vcnt) {
    if (!ch->vt->writev_some) return -1;
    return ch->vt->writev_some(ch, vecs, vcnt);
}
static inline int bytech_flush(ByteChannel *ch){
    if (!ch->vt->flush) return OK;
    return ch->vt->flush(ch);
}
static inline int bytech_shutdown_write(ByteChannel *ch) {
    return ch->vt->shutdown_write ? ch->vt->shutdown_write(ch) : 0;
}
static inline BytePollable bytech_get_pollable(const ByteChannel *ch) {
    return ch->vt->get_pollable(ch);
}
static inline void bytech_destroy(ByteChannel *ch) {
    if (!ch || !ch->vt || !ch->vt->destroy) return;
    ch->vt->destroy(ch);
}

#endif
