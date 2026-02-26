#ifndef UTILS_AI_DB_EXPL_H
#define UTILS_AI_DB_EXPL_H

#include <stdint.h>
#include <stdlib.h>

// Return types that must be used for "mutators"; functions that do a thing,
// for example, append, connect, exec.
typedef enum { OK = 0, STATUS_ERR = -1 } AdbxStatus;

// Return types that must be used for functions that answer a question, for
// example is_null, contains.
typedef enum { YES = 1, NO = 0, TRI_STATUS_ERR = -1 } AdbxTriStatus;

// ERR is a shorthand to signal an error in both cases - this works if
// -Wenum-conversion is enabled
#define ERR -1

#define ARRLEN(x) (sizeof(x) / sizeof((x)[0]))

/* True if (str1,len1) equals NUL-terminated literal/cstring str2 */
#define STREQ(str1, len1, str2)                                                \
  (((len1) == strlen(str2)) && (memcmp((str1), (str2), (len1)) == 0))

void *xmalloc(size_t size);
void *xcalloc(size_t n, size_t size);
void *xrealloc(void *ptr, size_t size);

/* Returns monotonic time in ms (for duration calculations). */
uint64_t now_ms_monotonic(void);

/* Fills 'len' bytes of 'buf' with cryptographically secure random bytes.
 * Returns OK/ERR. */
AdbxStatus fill_random(uint8_t *buf, size_t len);

#endif
