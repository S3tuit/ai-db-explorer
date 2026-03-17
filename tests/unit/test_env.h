#ifndef TEST_ENV_H
#define TEST_ENV_H

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

/* Creates one temporary directory under /tmp and returns its owned path.
 * Side effects: filesystem mutation.
 * Error semantics: test helper (asserts on failure).
 */
char *make_tmp_dir(void);

#endif
