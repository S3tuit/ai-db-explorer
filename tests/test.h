#ifndef TESTS_H
#define TESTS_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

#endif
