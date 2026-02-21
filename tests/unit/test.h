#ifndef TESTS_H
#define TESTS_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "conn_catalog.h"
#include "postgres_backend.h"
#include "utils.h"
#include "validator.h"

/* -------------------------------- ASSERTIONS ----------------------------- */
#define ASSERT_TRUE_AT(cond, file, line)                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      fprintf(stderr, "ASSERT_TRUE failed: %s (%s:%d)\n", #cond, file, line);  \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

#define ASSERT_TRUE(cond) ASSERT_TRUE_AT(cond, __FILE__, __LINE__)

#define ASSERT_STREQ_AT(a, b, file, line)                                      \
  do {                                                                         \
    if (((a) == NULL && (b) != NULL) || ((a) != NULL && (b) == NULL) ||        \
        ((a) != NULL && (b) != NULL && strcmp((a), (b)) != 0)) {               \
      fprintf(stderr,                                                          \
              "ASSERT_STREQ failed:\n  got: %s\n  exp: %s\n  (%s:%d)\n",       \
              (a) ? (a) : "(null)", (b) ? (b) : "(null)", file, line);         \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

#define ASSERT_STREQ(a, b) ASSERT_STREQ_AT(a, b, __FILE__, __LINE__)

/* ----------------------------- IN-MEMORY I/O ----------------------------- */

/* Creates a memfile with the 'input' content and asserts it's being created. */
FILE *memfile_impl(const char *input, const char *file, int line);
#define MEMFILE_IN(input) memfile_impl((input), __FILE__, __LINE__)

/* Output a memfile with write+read permission. */
FILE *memfile_out_impl(const char *file, int line);
#define MEMFILE_OUT() memfile_out_impl(__FILE__, __LINE__)

/* Returns a pointer to a buffer with all the bytes of 'f'. Caller should free
 * the returned pointer. */
char *read_all(FILE *f);

/* --------------------------------- HELPERS ------------------------------- */

/* Writes JSON content to a temp file and returns its path.
 * Caller owns the returned path string and must unlink it. */
char *write_tmp_config(const char *json);

/* Builds a catalog with the test policy and returns it.
 * Ownership: caller owns the catalog and must destroy it. */
ConnCatalog *load_test_catalog(void);

/* Builds one minimal ConnProfile.
 * It returns a stack value that borrows 'connection_name'.
 * Side effects: none.
 * Error semantics: none (test helper).
 */
ConnProfile make_profile(const char *connection_name,
                         SafetyColumnStrategy mode);

/* Runs 'sql' against a pre-defined ConnCatalog and populates 'out'.
 * Propagates validate_query() return value to caller. Caller must call
 * vq_out_clean on 'out'. */
int get_validate_query_out(ValidateQueryOut *out, char *sql);

#endif
