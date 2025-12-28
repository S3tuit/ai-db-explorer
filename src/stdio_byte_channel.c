#include "stdio_byte_channel.h"
#include "utils.h"

#include <stdlib.h>
#include <errno.h>
#include <string.h>

#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>  // fileno
#endif

typedef struct StdioByteChannelImpl {
    FILE *in;
    FILE *out;
    int close_on_destroy;
    int is_closed;
} StdioByteChannelImpl;

static ssize_t stdio_read_some(ByteChannel *ch, void *buf, size_t cap) {
    StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

    if (!impl || impl->is_closed) return -1;
    if (!impl->in) return -1;
    if (cap == 0) return 0;

    size_t n = fread(buf, 1, cap, impl->in);

    if (n > 0) return (ssize_t)n;

    if (ferror(impl->in)) {
        // errno set by stdio
        return -1;
    }

    // fread returned 0, treat as EOF
    return 0;
}

static ssize_t stdio_write_some(ByteChannel *ch, const void *buf, size_t len) {
    StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

    if (!impl || impl->is_closed) return ERR;
    if (!impl->out) return ERR;

    return fwrite(buf, 1, len, impl->out);
}

static int stdio_flush(ByteChannel *ch) {
    StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

    if (!impl || impl->is_closed) return ERR;
    if (!impl->out) return ERR;
    if(fflush(impl->out) != 0) return ERR;
    return OK;
}

static int stdio_shutdown_write(ByteChannel *ch) {
    // stdio can't half-close like a socket; closest thing is to flush.
    StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

    if (!impl || impl->is_closed) return ERR;
    if (!impl->out) return OK;
    if (fflush(impl->out) != 0) {
        return ERR;
    }
    // no more writes
    impl->out = NULL;
    return OK;
}

static BytePollable stdio_get_pollable(const ByteChannel *ch) {
    const StdioByteChannelImpl *impl = (const StdioByteChannelImpl *)ch->impl;
    if (!impl || impl->is_closed) return (BytePollable)-1;

#if defined(__unix__) || defined(__APPLE__)
    if (impl->in) return (BytePollable)fileno(impl->in);
    return (BytePollable)-1;
#else
    // On non-POSIX platforms, there's no portable fileno/pollable handle
    (void)impl;
    return (BytePollable)-1;
#endif
}

/* Closes underlying resource/handle, but doesn not free the ByteChannel 
 * itself. */
static int stdio_close(ByteChannel *ch) {
    StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

    if (!impl) return ERR;
    if (impl->is_closed) return OK;

    int rc = OK;

    // If we don't own, just mark closed.
    if (!impl->close_on_destroy) {
        impl->is_closed = 1;
        return OK;
    }

    // Owns streams: close them carefully, once if in==out.
    if (impl->in && impl->out && impl->in == impl->out) {
        if (fclose(impl->in) != 0) rc = ERR;
        impl->in = NULL;
        impl->out = NULL;
    } else {
        if (impl->in) {
            if (fclose(impl->in) != 0) rc = ERR;
            impl->in = NULL;
        }
        if (impl->out) {
            if (fclose(impl->out) != 0) rc = ERR;
            impl->out = NULL;
        }
    }

    impl->is_closed = 1;
    return rc;
}

static void stdio_destroy(ByteChannel *ch) {
    if (!ch) return;
    // close underlying streams
    stdio_close(ch);

    // free impl + wrapper
    free(ch->impl);
    ch->impl = NULL;
    free(ch);
}

static const ByteChannelVTable STDIO_BYTE_CHANNEL_VT = {
    .read_some      = stdio_read_some,
    .write_some     = stdio_write_some,
    .flush          = stdio_flush,
    .shutdown_write = stdio_shutdown_write,
    .get_pollable   = stdio_get_pollable,
    .destroy        = stdio_destroy
};

ByteChannel *stdio_bytechannel_create(FILE *in, FILE *out, int close_on_destroy) {
    if (!in && !out) {
        return NULL;
    }

    ByteChannel *ch = (ByteChannel *)xmalloc(sizeof(ByteChannel));
    StdioByteChannelImpl *impl = (StdioByteChannelImpl *)xmalloc(sizeof(StdioByteChannelImpl));

    impl->in = in;
    impl->out = out;
    impl->close_on_destroy = close_on_destroy ? 1 : 0;
    impl->is_closed = 0;

    ch->vt = &STDIO_BYTE_CHANNEL_VT;
    ch->impl = impl;
    return ch;
}
