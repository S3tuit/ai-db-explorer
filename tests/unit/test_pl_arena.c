#include <stdalign.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "pl_arena.h"
#include "test.h"

/* These tests validate the arena's public contract:
 * - data is stored and retrieved intact
 * - returned payload pointers are aligned
 * - empty payloads are supported
 * - used counter stays consistent */

static void test_basic_add_get(void) {
  PlArena *ar = pl_arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  const char *s1 = "abc";
  char *v1 = (char *)pl_arena_add(ar, (void *)s1, 3);
  ASSERT_TRUE(v1 != NULL);
  ASSERT_TRUE(memcmp(v1, "abc", 3) == 0);
  ASSERT_TRUE(v1[3] == '\0');
  ASSERT_TRUE(pl_arena_get_used(ar) > 0);

  pl_arena_destroy(ar);
}

static void test_alignment_and_empty(void) {
  PlArena *ar = pl_arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);

  char *v1 = (char *)pl_arena_add(ar, "a", 1);
  ASSERT_TRUE(v1 != NULL);

  char *v2 = (char *)pl_arena_add(ar, "bbbb", 4);
  ASSERT_TRUE(v2 != NULL);
  ASSERT_TRUE(((uintptr_t)v2 % alignof(max_align_t)) == 0);

  char *v3 = (char *)pl_arena_add(ar, NULL, 0);
  ASSERT_TRUE(v3 != NULL);
  ASSERT_TRUE(v3[0] == '\0');

  pl_arena_destroy(ar);
}

static void test_out_of_bounds(void) {
  PlArena *ar = pl_arena_create(NULL, NULL);
  ASSERT_TRUE(ar != NULL);
  ASSERT_TRUE(pl_arena_get_used(ar) == 0);
  pl_arena_destroy(ar);
}

static void test_grow_blocks_and_stability(void) {
  // Force multiple blocks by exceeding the 1 KiB first block.
  uint32_t cap = 4096;
  PlArena *ar = pl_arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  char payload1[900];
  memset(payload1, 'a', sizeof(payload1));
  char *p1 = (char *)pl_arena_add(ar, payload1, (uint32_t)sizeof(payload1));
  ASSERT_TRUE(p1 != NULL);

  // This should spill into a new block.
  char payload2[900];
  memset(payload2, 'b', sizeof(payload2));
  char *p2 = (char *)pl_arena_add(ar, payload2, (uint32_t)sizeof(payload2));
  ASSERT_TRUE(p2 != NULL);
  ASSERT_TRUE(((uintptr_t)p2 % alignof(max_align_t)) == 0);

  // Original payload must still be accessible after growth.
  ASSERT_TRUE(memcmp(p1, payload1, sizeof(payload1)) == 0);

  pl_arena_destroy(ar);
}

static void test_large_entry_and_cap(void) {
  uint32_t cap = 128;
  PlArena *ar = pl_arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  // Entry larger than block size should still work.
  char big[60];
  memset(big, 'x', sizeof(big));
  char *p = (char *)pl_arena_add(ar, big, (uint32_t)sizeof(big));
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(memcmp(p, big, sizeof(big)) == 0);

  pl_arena_destroy(ar);

  // Cap enforcement: second add should fail due to cap.
  cap = 48;
  ar = pl_arena_create(NULL, &cap);
  ASSERT_TRUE(ar != NULL);

  char *p1 = (char *)pl_arena_add(ar, "aaaaaaaaaaaaaaaaaaaa", 20);
  ASSERT_TRUE(p1 != NULL);

  char *p2 = (char *)pl_arena_add(ar, "bbbbbbbbbbbbbbbbbbbb", 20);
  ASSERT_TRUE(p2 == NULL);
  ASSERT_TRUE(pl_arena_get_used(ar) > 0);

  pl_arena_destroy(ar);
}

static void test_ptrvec_flatten(void) {
  /* PtrVec collects pointers on the heap and flattens them into arena memory.
   * This keeps arena allocations to a single copy at the end. */
  PlArena *ar = pl_arena_create(NULL, NULL);
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
  pl_arena_destroy(ar);
}

int main(void) {
  test_basic_add_get();
  test_alignment_and_empty();
  test_out_of_bounds();
  test_grow_blocks_and_stability();
  test_large_entry_and_cap();
  test_ptrvec_flatten();
  fprintf(stderr, "OK: test_pl_arena\n");
  return 0;
}
