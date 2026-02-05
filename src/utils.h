#ifndef UTILS_AI_DB_EXPL_H
#define UTILS_AI_DB_EXPL_H

#include <stdint.h>
#include <stdlib.h>

// Return types that must be used for "mutators"; functions that do a thing,
// for example, append, connect, exec.
#define OK 0
#define ERR -1

// Return types that must be used for functions that answer a question, for
// example is_null, contains.
#define YES 1
#define NO 0
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

#endif
