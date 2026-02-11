#include "stdio_byte_channel.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

// TODO: when adding support for Windows too, this should become fd_bytechannel
// while on Windows we'll have handle_byechannel and the public stdio will be
// the same

typedef struct StdioByteChannelImpl {
  int in_fd;
  int out_fd;
  int owns_fds;
  int is_closed;
} StdioByteChannelImpl;

static ssize_t stdio_read_some(ByteChannel *ch, void *buf, size_t cap) {
  StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

  if (!impl || impl->is_closed)
    return -1;
  if (impl->in_fd < 0)
    return -1;
  if (cap == 0)
    return 0;

  // it's better to use read because we don't want any buffering. We want to
  // read everything the user wrote when this functio is called
  for (;;) {
    ssize_t n = read(impl->in_fd, buf, cap);
    if (n >= 0) {
      return n;
    }
    // PTY slave returns EIO on hangup; treat it as EOF to match file/pipe
    if (errno == EIO)
      return 0;
    if (errno == EINTR)
      continue;
    return -1;
  }
}

static ssize_t stdio_write_some(ByteChannel *ch, const void *buf, size_t len) {
  StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

  if (!impl || impl->is_closed)
    return ERR;
  if (impl->out_fd < 0)
    return ERR;

  // same as read, we don't want buffering. We want to writer everything as
  // soon as this func in called
  return write(impl->out_fd, buf, len);
}

static ssize_t stdio_writev_some(ByteChannel *ch, const ByteChannelVec *vecs,
                                 int vcnt) {
  StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;
  if (!impl || impl->is_closed)
    return ERR;
  if (impl->out_fd < 0)
    return ERR;
  if (!vecs || vcnt <= 0)
    return ERR;

  struct iovec iov[vcnt];
  for (int i = 0; i < vcnt; i++) {
    iov[i].iov_base = (void *)vecs[i].base;
    iov[i].iov_len = vecs[i].len;
  }
  return writev(impl->out_fd, iov, vcnt);
}

static int stdio_flush(ByteChannel *ch) {
  StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

  if (!impl || impl->is_closed)
    return ERR;
  if (impl->out_fd < 0)
    return ERR;
  // No stdio buffering to flush; write(2) is synchronous.
  return OK;
}

static int stdio_shutdown_write(ByteChannel *ch) {
  // stdio can't half-close like a socket; closest thing is to flush.
  StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

  if (!impl || impl->is_closed)
    return ERR;
  if (impl->out_fd < 0)
    return OK;
  // no more writes
  impl->out_fd = -1;
  return OK;
}

static BytePollable stdio_get_pollable(const ByteChannel *ch) {
  const StdioByteChannelImpl *impl = (const StdioByteChannelImpl *)ch->impl;
  if (!impl || impl->is_closed)
    return (BytePollable)-1;

  if (impl->in_fd >= 0)
    return (BytePollable)impl->in_fd;
  return (BytePollable)-1;
}

/* Closes underlying resource/handle IF owned, but doesn not free the
 * ByteChannel itself. */
static int stdio_close(ByteChannel *ch) {
  StdioByteChannelImpl *impl = (StdioByteChannelImpl *)ch->impl;

  if (!impl)
    return ERR;
  if (impl->is_closed)
    return OK;

  int rc = OK;

  // If we don't own, just mark closed.
  if (!impl->owns_fds) {
    impl->is_closed = 1;
    return OK;
  }

  // Owns fds: close them carefully, once if in_fd==out_fd.
  if (impl->in_fd >= 0 && impl->out_fd >= 0 && impl->in_fd == impl->out_fd) {
    if (close(impl->in_fd) != 0)
      rc = ERR;
    impl->in_fd = -1;
    impl->out_fd = -1;
  } else {
    if (impl->in_fd >= 0) {
      if (close(impl->in_fd) != 0)
        rc = ERR;
      impl->in_fd = -1;
    }
    if (impl->out_fd >= 0) {
      if (close(impl->out_fd) != 0)
        rc = ERR;
      impl->out_fd = -1;
    }
  }

  impl->is_closed = 1;
  return rc;
}

static void stdio_destroy(ByteChannel *ch) {
  if (!ch)
    return;
  stdio_close(ch);

  // free impl + wrapper
  free(ch->impl);
  ch->impl = NULL;
  free(ch);
}

static const ByteChannelVTable STDIO_BYTE_CHANNEL_VT = {
    .read_some = stdio_read_some,
    .write_some = stdio_write_some,
    .writev_some = stdio_writev_some,
    .flush = stdio_flush,
    .shutdown_write = stdio_shutdown_write,
    .get_pollable = stdio_get_pollable,
    .destroy = stdio_destroy};

static ByteChannel *stdio_bytechannel_create_impl(int in_fd, int out_fd,
                                                  int owns_fds) {
  if (in_fd < 0 && out_fd < 0) {
    return NULL;
  }

  ByteChannel *ch = (ByteChannel *)xmalloc(sizeof(ByteChannel));
  StdioByteChannelImpl *impl =
      (StdioByteChannelImpl *)xmalloc(sizeof(StdioByteChannelImpl));

  impl->in_fd = in_fd;
  impl->out_fd = out_fd;
  impl->owns_fds = owns_fds ? 1 : 0;
  impl->is_closed = 0;

  ch->vt = &STDIO_BYTE_CHANNEL_VT;
  ch->impl = impl;
  return ch;
}

ByteChannel *stdio_bytechannel_open_fd(int in_fd, int out_fd) {
  return stdio_bytechannel_create_impl(in_fd, out_fd, 1);
}

ByteChannel *stdio_bytechannel_open_path(const char *in_path,
                                         const char *out_path) {
  const char *in = (in_path && in_path[0] != '\0') ? in_path : NULL;
  const char *out = (out_path && out_path[0] != '\0') ? out_path : NULL;
  if (!in && !out)
    return NULL;

  int in_fd = -1;
  int out_fd = -1;

  if (in && out && strcmp(in, out) == 0) {
    int flags = O_RDWR;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int fd = open(in, flags);
    if (fd < 0)
      return NULL;
    ByteChannel *ch = stdio_bytechannel_open_fd(fd, fd);
    if (!ch)
      (void)close(fd);
    return ch;
  }

  if (in) {
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    in_fd = open(in, flags);
    if (in_fd < 0)
      goto err;
  }
  if (out) {
    int flags = O_WRONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    out_fd = open(out, flags);
    if (out_fd < 0)
      goto err;
  }

  return stdio_bytechannel_open_fd(in_fd, out_fd);

err: {
  int saved = errno;
  if (in_fd >= 0)
    (void)close(in_fd);
  if (out_fd >= 0 && out_fd != in_fd)
    (void)close(out_fd);
  errno = saved;
  return NULL;
}
}

ByteChannel *stdio_bytechannel_wrap_fd(int in_fd, int out_fd) {
  return stdio_bytechannel_create_impl(in_fd, out_fd, 0);
}
