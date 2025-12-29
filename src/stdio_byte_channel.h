#ifndef STDIO_BYTE_CHANNEL_H
#define STDIO_BYTE_CHANNEL_H

#include <stdio.h>
#include "byte_channel.h"

/* Creates a ByteChannel backed by stdio streams.
 *
 * - 'in'  may be NULL if you only need write (e.g. stdout only).
 * - 'out' may be NULL if you only need read.
 *
 * Ownership:
 * - If close_on_destroy != 0: destroy() will fclose() the streams.
 * - If close_on_destroy == 0: destroy() will NOT fclose() them.
 *
 * Note: If in == out, it will only fclose() once when owned.
 */
ByteChannel *stdio_bytechannel_create(FILE *in, FILE *out, int close_on_destroy);

#endif
