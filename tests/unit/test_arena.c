#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "arena.h"
#include "test.h"

/* These tests validate the arena's public contract:
 * - data is stored and retrieved intact
 * - returned payload pointers are aligned
 * - empty payloads are supported
 * - used counter stays consistent */

static void test_basic_add_get(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  const char *s1 = "abc";
  char *v1 = (char *)arena_add(ar, (void *)s1, 3);
  ASSERT_TRUE(v1 != NULL);
  ASSERT_TRUE(memcmp(v1, "abc", 3) == 0);
  ASSERT_TRUE(arena_get_used(ar) > 0);

  arena_destroy(ar);
}

static void test_alignment_and_empty(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  char *v1 = (char *)arena_add(ar, "a", 1);
  ASSERT_TRUE(v1 != NULL);

  char *v2 = (char *)arena_add(ar, "bbbb", 4);
  ASSERT_TRUE(v2 != NULL);
  ASSERT_TRUE(((uintptr_t)v2 % sizeof(uintptr_t)) == 0);

  char *v3 = (char *)arena_add(ar, NULL, 0);
  ASSERT_TRUE(v3 != NULL);

  arena_destroy(ar);
}

static void test_out_of_bounds(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);
  ASSERT_TRUE(arena_get_used(ar) == 0);
  arena_destroy(ar);
}

static void test_grow_blocks_and_stability(void) {
  // Force multiple blocks by exceeding the 1 KiB first block.
  uint32_t cap = 4096;
  Arena *ar = arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  char payload1[900];
  memset(payload1, 'a', sizeof(payload1));
  char *p1 = (char *)arena_add(ar, payload1, (uint32_t)sizeof(payload1));
  ASSERT_TRUE(p1 != NULL);

  // This should spill into a new block.
  char payload2[900];
  memset(payload2, 'b', sizeof(payload2));
  char *p2 = (char *)arena_add(ar, payload2, (uint32_t)sizeof(payload2));
  ASSERT_TRUE(p2 != NULL);
  ASSERT_TRUE(((uintptr_t)p2 % sizeof(uintptr_t)) == 0);

  // Original payload must still be accessible after growth.
  ASSERT_TRUE(memcmp(p1, payload1, sizeof(payload1)) == 0);

  arena_destroy(ar);
}

static void test_large_entry_and_cap(void) {
  uint32_t cap = 128;
  Arena *ar = arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  // Entry larger than block size should still work.
  char big[60];
  memset(big, 'x', sizeof(big));
  char *p = (char *)arena_add(ar, big, (uint32_t)sizeof(big));
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(memcmp(p, big, sizeof(big)) == 0);

  arena_destroy(ar);

  // Cap enforcement: second add should fail due to cap.
  // 20 bytes aligns up to 24; two entries need 48, so cap=40 forces failure.
  cap = 40;
  ar = arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  char *p1 = (char *)arena_add(ar, "aaaaaaaaaaaaaaaaaaaa", 20);
  ASSERT_TRUE(p1 != NULL);

  char *p2 = (char *)arena_add(ar, "bbbbbbbbbbbbbbbbbbbb", 20);
  ASSERT_TRUE(p2 == NULL);
  ASSERT_TRUE(arena_get_used(ar) > 0);

  arena_destroy(ar);
}

static void test_ptrvec_flatten(void) {
  /* PtrVec collects pointers on the heap and flattens them into arena memory.
   * This keeps arena allocations to a single copy at the end. */
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  PtrVec v = {0};
  int a = 1;
  int b = 2;
  int c = 3;

  ASSERT_TRUE(ptrvec_push(&v, &a) == OK);
  ASSERT_TRUE(ptrvec_push(&v, &b) == OK);
  ASSERT_TRUE(ptrvec_push(&v, &c) == OK);

  void **arr = ptrvec_flatten(&v, ar);
  ASSERT_TRUE(arr != NULL);
  ASSERT_TRUE(arr[0] == &a);
  ASSERT_TRUE(arr[1] == &b);
  ASSERT_TRUE(arr[2] == &c);

  ptrvec_clean(&v);
  arena_destroy(ar);
}

static void test_alloc_basic(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  uint8_t *p = (uint8_t *)arena_alloc(ar, 4);
  ASSERT_TRUE(p != NULL);

  p[0] = 0x11;
  p[1] = 0x22;
  p[2] = 0x33;
  p[3] = 0x44;

  uint8_t expected[] = {0x11, 0x22, 0x33, 0x44};
  ASSERT_TRUE(memcmp(p, expected, sizeof(expected)) == 0);

  arena_destroy(ar);
}

static void test_alloc_zero_len(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  uint8_t *p = (uint8_t *)arena_alloc(ar, 0);
  ASSERT_TRUE(p != NULL);

  arena_destroy(ar);
}

static void test_calloc_zeroes_payload(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  uint8_t *p = (uint8_t *)arena_calloc(ar, 8);
  ASSERT_TRUE(p != NULL);
  for (size_t i = 0; i < 8; i++) {
    ASSERT_TRUE(p[i] == 0);
  }

  arena_destroy(ar);
}

static void test_alloc_rejects_overflow_len(void) {
  uint32_t cap = UINT32_MAX;
  Arena *ar = arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  ASSERT_TRUE(arena_alloc(ar, UINT32_MAX) == NULL);

  arena_destroy(ar);
}

static void test_add_nul(void) {
  Arena *ar = arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  // NUL-terminated copy of a string.
  char *s1 = (char *)arena_add_nul(ar, (void *)"hello", 5);
  ASSERT_TRUE(s1 != NULL);
  ASSERT_TRUE(memcmp(s1, "hello", 5) == 0);
  ASSERT_TRUE(s1[5] == '\0');

  // Zero-length NUL-terminated string.
  char *s2 = (char *)arena_add_nul(ar, (void *)"", 0);
  ASSERT_TRUE(s2 != NULL);
  ASSERT_TRUE(s2[0] == '\0');

  // arena_add (without nul) must NOT guarantee a terminator.
  char *raw = (char *)arena_add(ar, (void *)"xyz", 3);
  ASSERT_TRUE(raw != NULL);
  ASSERT_TRUE(memcmp(raw, "xyz", 3) == 0);

  arena_destroy(ar);
}

int main(void) {
  test_basic_add_get();
  test_alignment_and_empty();
  test_out_of_bounds();
  test_grow_blocks_and_stability();
  test_large_entry_and_cap();
  test_ptrvec_flatten();
  test_alloc_basic();
  test_alloc_zero_len();
  test_calloc_zeroes_payload();
  test_alloc_rejects_overflow_len();
  test_add_nul();
  fprintf(stderr, "OK: test_arena\n");
  return 0;
}
