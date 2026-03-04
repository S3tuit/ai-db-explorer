#include <errno.h>
#include <stdio.h>
#include <time.h>

#ifdef __linux__
#include <sys/random.h>
#endif

#include "utils.h"

/* Succeed or die. If malloc fails to allocate memory, this terminates the
 * program.*/
void *xmalloc(size_t size) {
  void *ptr = malloc(size);
  if (!ptr) {
    fprintf(stderr, "xmalloc. virtual memory exhausted\n");
    exit(1);
  }
  return ptr;
}

void *xcalloc(size_t n, size_t size) {
  void *ptr = calloc(n, size);
  if (!ptr) {
    fprintf(stderr, "xcalloc. virtual memory exhausted\n");
    exit(1);
  }
  return ptr;
}

void *xrealloc(void *ptr, size_t size) {
  if (!ptr)
    return xmalloc(size);
  void *new = realloc(ptr, size);
  if (!new) {
    fprintf(stderr, "xrealloc. virtual memory exhausted\n");
    exit(1);
  }
  return new;
}

/* Returns monotonic time in ms (for duration calculations). */
uint64_t now_ms_monotonic(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

AdbxStatus fill_random(uint8_t *buf, size_t len) {
#ifdef __linux__
  size_t done = 0;
  while (done < len) {
    ssize_t n = getrandom(buf + done, len - done, 0);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return ERR;
    }
    done += (size_t)n;
  }
  return OK;
#else
  arc4random_buf(buf, len);
  return OK;
#endif
}

AdbxStatus fill_random_hex(char *buf, size_t len) {
  if (!buf && len > 0)
    return ERR;
  if (len == 0)
    return OK;

  static const char HEX[] = "0123456789abcdef";
  uint8_t rnd[128];
  size_t out_i = 0;

  while (out_i < len) {
    size_t rem_hex = len - out_i;
    size_t need_rnd = (rem_hex + 1) / 2;
    if (need_rnd > sizeof(rnd))
      need_rnd = sizeof(rnd);
    if (fill_random(rnd, need_rnd) != OK)
      return ERR;

    for (size_t i = 0; i < need_rnd && out_i < len; i++) {
      buf[out_i++] = HEX[rnd[i] >> 4];
      if (out_i < len)
        buf[out_i++] = HEX[rnd[i] & 0x0F];
    }
  }
  return OK;
}
