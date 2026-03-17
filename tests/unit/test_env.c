#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "string_op.h"
#include "test.h"
#include "test_env.h"

void restore_env(const char *name, const char *old_val, int had_old) {
  ASSERT_TRUE(name != NULL);
  if (!had_old) {
    ASSERT_TRUE(unsetenv(name) == 0);
    return;
  }
  ASSERT_TRUE(old_val != NULL);
  ASSERT_TRUE(setenv(name, old_val, 1) == 0);
}

void env_guard_begin(EnvGuard *g) {
  ASSERT_TRUE(g != NULL);
  memset(g, 0, sizeof(*g));

  const char *xdg = getenv("XDG_CONFIG_HOME");
  const char *home = getenv("HOME");

  g->had_xdg = (xdg != NULL);
  g->had_home = (home != NULL);
  g->xdg_old = xdg ? dup_or_null(xdg) : NULL;
  g->home_old = home ? dup_or_null(home) : NULL;
}

void env_guard_end(EnvGuard *g) {
  ASSERT_TRUE(g != NULL);
  restore_env("XDG_CONFIG_HOME", g->xdg_old, g->had_xdg);
  restore_env("HOME", g->home_old, g->had_home);
  free(g->xdg_old);
  free(g->home_old);
  memset(g, 0, sizeof(*g));
}

char *make_tmp_dir(void) {
  char templ[] = "/tmp/adbxcred_XXXXXX";
  char *out = dup_or_null(templ);
  ASSERT_TRUE(out != NULL);
  ASSERT_TRUE(mkdtemp(out) != NULL);
  return out;
}
