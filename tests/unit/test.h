#ifndef TESTS_H
#define TESTS_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

/* -------------------------------- ASSERTIONS ----------------------------- */
#define ASSERT_TRUE_AT(cond, file, line) do { \
  if (!(cond)) { \
    fprintf(stderr, "ASSERT_TRUE failed: %s (%s:%d)\n", #cond, file, line); \
    exit(1); \
  } \
} while (0)

#define ASSERT_TRUE(cond) ASSERT_TRUE_AT(cond, __FILE__, __LINE__)

#define ASSERT_STREQ_AT(a,b,file,line) do { \
  if (((a)==NULL && (b)!=NULL) || ((a)!=NULL && (b)==NULL) || ((a)!=NULL && (b)!=NULL && strcmp((a),(b))!=0)) { \
    fprintf(stderr, "ASSERT_STREQ failed:\n  got: %s\n  exp: %s\n  (%s:%d)\n", (a)?(a):"(null)", (b)?(b):"(null)", file, line); \
    exit(1); \
  } \
} while (0)

#define ASSERT_STREQ(a,b) ASSERT_STREQ_AT(a, b, __FILE__, __LINE__)

/* ----------------------------- IN-MEMORY I/O ------------------------------- */

/* Creates a memfile with the 'input' content and asserts it's being created. */
FILE *memfile_impl(const char *input, const char *file, int line) {
    (void)file; (void)line;
#if defined(_GNU_SOURCE)
    FILE *f = fmemopen((void *)input, strlen(input), "r");
    ASSERT_TRUE_AT(f != NULL, file, line);
    return f;
#else
    /* portable fallback: tmpfile */
    FILE *f = tmpfile();
    if (!f) return NULL;
    fwrite(input, 1, strlen(input), f);
    fflush(f);
    fseek(f, 0, SEEK_SET);
    ASSERT_TRUE_AT(f != NULL, file, line);
    return f;
#endif
}
#define MEMFILE_IN(input) memfile_impl((input), __FILE__, __LINE__)


/* Output a memfile with write+read permission. */
FILE *memfile_out_impl(const char *file, int line) {
    FILE *f = tmpfile();
    ASSERT_TRUE_AT(f != NULL, file, line);
    return f;
}
#define MEMFILE_OUT() memfile_out_impl(__FILE__, __LINE__)

/* Returns a pointer to a buffer with all the bytes of 'f'. Caller should free
 * the returned pointer. */
char *read_all(FILE *f) {
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    ASSERT_TRUE(sz >= 0);

    char *buf = xmalloc((size_t)sz + 1);

    size_t n = fread(buf, 1, (size_t)sz, f);
    buf[n] = '\0';
    return buf;
}

#endif
