#ifndef STDIO_BYTE_CHANNEL_H
#define STDIO_BYTE_CHANNEL_H

#include "byte_channel.h"

/* Creates a ByteChannel backed by file descriptors.
 *
 * - 'in_fd'  may be -1 if you only need write (e.g. stdout only).
 * - 'out_fd' may be -1 if you only need read.
 *
 * Ownership:
 * - open_fd takes ownership and will close() on destroy.
 *
 * Note: If in_fd == out_fd, open_fd will only close() once when owned.
 */
ByteChannel *stdio_bytechannel_open_fd(int in_fd, int out_fd);

/* Same as stdio_bytechannel_open_fd but takes in input the paths of the files.
 *
 * - 'in_path'  may be NULL if you only need write (e.g. stdout only).
 * - 'out_path' may be NULL if you only need read.
 *
 * in_path is opened with O_RDONLY, out_path with O_WRONLY. If they're the same,
 * they're opened with O_RDWR.
 */
ByteChannel *stdio_bytechannel_open_path(const char *in_path,
                                         const char *out_path);

/* Same as *_open_fd but this does NOT take ownership and will NOT close() on
 * destroy.*/
ByteChannel *stdio_bytechannel_wrap_fd(int in_fd, int out_fd);

#endif
