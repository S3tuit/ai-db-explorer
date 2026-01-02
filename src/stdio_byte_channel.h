#ifndef STDIO_BYTE_CHANNEL_H
#define STDIO_BYTE_CHANNEL_H

#include "byte_channel.h"

/* Creates a ByteChannel backed by file descriptors.
 *
 * - 'in_fd'  may be -1 if you only need write (e.g. stdout only).
 * - 'out_fd' may be -1 if you only need read.
 *
 * Ownership:
 * - If close_on_destroy != 0: destroy() will close() the fds.
 * - If close_on_destroy == 0: destroy() will NOT close() them.
 *
 * Note: If in_fd == out_fd, it will only close() once when owned.
 */
ByteChannel *stdio_bytechannel_create(int in_fd, int out_fd, int close_on_destroy);

#endif
