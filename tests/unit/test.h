#ifndef TESTS_H
#define TESTS_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "conn_catalog.h"
#include "db_backend.h"
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

/* Loads the ConnCatalog from an input 'path'. */
ConnCatalog *catalog_load_from_file(const char *path, char **err_out);

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

/* Creates one temporary directory under /tmp and returns its owned path.
 * Side effects: filesystem mutation.
 * Error semantics: test helper (asserts on failure).
 */
char *make_tmp_dir(void);

/* ----------------------------- FAKE BACKEND ------------------------------ */

/* Builds one shared fake DbBackend for unit tests.
 *
 * This fake intentionally encodes a simple authentication rule:
 * the connection succeeds only when 'pwd' equals 'profile->connection_name'.
 * We use that rule so tests can control success/failure without networking or
 * backend-specific fixtures.
 *
 * Ownership: caller owns the returned backend and must destroy it with
 * db_destroy().
 * Side effects: allocates one backend instance.
 * Error semantics: test helper; aborts on allocation failure.
 */
DbBackend *fake_backend_create(DbKind kind);

/* Resets the shared fake-backend counters to zero.
 * It performs no allocations.
 * Side effects: mutates unit-test global state.
 * Error semantics: none.
 */
void fake_backend_reset_counters(void);

/* Returns how many times the shared fake backend accepted or rejected a
 * connection attempt since the last reset.
 */
int fake_backend_connect_calls(void);

/* Returns how many times the shared fake backend was disconnected since the
 * last reset.
 */
int fake_backend_disconnect_calls(void);

/* Returns how many fake backend instances were destroyed since the last reset.
 */
int fake_backend_destroy_calls(void);

/* ------------------------------- ENV --------------------------------------*/
typedef struct {
  char *xdg_old;
  int had_xdg;
  char *home_old;
  int had_home;
} EnvGuard;

/* Restores one environment variable to its previous state.
 * It borrows all inputs and performs no allocations.
 * Side effects: updates process environment.
 * Error semantics: test helper (asserts on failure).
 */
void restore_env(const char *name, const char *old_val, int had_old);

/* Captures HOME/XDG_CONFIG_HOME for later restoration.
 * It borrows 'g' and allocates owned string snapshots.
 * Side effects: reads environment and allocates memory.
 * Error semantics: test helper (asserts on invalid input).
 */
void env_guard_begin(EnvGuard *g);

/* Restores one environment snapshot created by env_guard_begin.
 * It consumes owned strings inside 'g'.
 * Side effects: updates process environment.
 * Error semantics: test helper (asserts on failure).
 */
void env_guard_end(EnvGuard *g);

#endif
