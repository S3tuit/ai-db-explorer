#include <stdlib.h>

/* Some toolchain/libc mixes emit references to glibc C23 entry points
 * (__isoc23_strtol/__isoc23_strtoul) that are unavailable at link time in
 * our integration container. Provide tiny compatibility wrappers so static
 * third-party objects can link reliably in that environment.
 *
 * Ownership/side effects: none.
 * Error semantics: identical to strtol/strtoul.
 */
long __isoc23_strtol(const char *nptr, char **endptr, int base) {
  return strtol(nptr, endptr, base);
}

unsigned long __isoc23_strtoul(const char *nptr, char **endptr, int base) {
  return strtoul(nptr, endptr, base);
}

