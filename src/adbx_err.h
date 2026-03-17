#ifndef ADBX_ERR_H
#define ADBX_ERR_H

#include <stdio.h>

/* Shared fixed-size message capacity for module-local typed error structs.
 * Modules that use ADBX_ERR_CLEAR/ADBX_ERR_SETF should define:
 *
 *   typedef struct {
 *     SomeTypedErrCode code;
 *     char msg[ADBX_ERRMSG_MAX];
 *   } SomeTypedErr;
 *
 * Keeping 'code' typed at the module level preserves -Wenum-conversion checks,
 * while the shared macros remove the repeated clear/format boilerplate.
 */
#define ADBX_ERRMSG_MAX 256u

/* Clears one typed error output struct.
 * The caller passes the enum value that represents "no error" for the module.
 * The macro is a no-op when 'out_err' is NULL.
 */
#define ADBX_ERR_CLEAR(out_err, none_value) \
  do {                                      \
    if ((out_err) != NULL) {                \
      (out_err)->code = (none_value);       \
      (out_err)->msg[0] = '\0';             \
    }                                       \
  } while (0)

/* Stores one typed error code plus one formatted message.
 * The destination must expose '.code' and '.msg', where '.msg' is a fixed-size
 * char array. Formatting is truncated like snprintf(). The macro is a no-op
 * when 'out_err' is NULL.
 */
#define ADBX_ERR_SETF(out_err, code_value, fmt, ...)                       \
  do {                                                                     \
    if ((out_err) != NULL) {                                               \
      (out_err)->code = (code_value);                                      \
      if ((fmt) != NULL) {                                                 \
        (void)snprintf((out_err)->msg, sizeof((out_err)->msg), (fmt),      \
                       ##__VA_ARGS__);                                     \
      } else {                                                             \
        (out_err)->msg[0] = '\0';                                          \
      }                                                                    \
    }                                                                      \
  } while (0)

#endif
