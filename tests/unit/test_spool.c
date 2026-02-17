#include <stdio.h>
#include <string.h>

#include "spool.h"
#include "test.h"

static void test_basic_dedup(void) {
  StringPool *sp = spool_create();
  ASSERT_TRUE(sp != NULL);

  const char *p1 = spool_add(sp, "alpha");
  const char *p2 = spool_add(sp, "alpha");
  const char *p3 = spool_add(sp, "beta");

  ASSERT_TRUE(p1 != NULL);
  ASSERT_TRUE(p2 != NULL);
  ASSERT_TRUE(p3 != NULL);
  ASSERT_TRUE(p1 == p2);
  ASSERT_TRUE(p1 != p3);
  ASSERT_TRUE(strcmp(p1, "alpha") == 0);
  ASSERT_TRUE(strcmp(p3, "beta") == 0);

  spool_destroy(sp);
}

static void test_addn_and_add_interop(void) {
  StringPool *sp = spool_create();
  ASSERT_TRUE(sp != NULL);

  const char src[] = {'a', 'b', 'c'};
  const char *p1 = spool_addn(sp, src, 3);
  const char *p2 = spool_add(sp, "abc");
  ASSERT_TRUE(p1 != NULL);
  ASSERT_TRUE(p2 != NULL);
  ASSERT_TRUE(p1 == p2);
  ASSERT_TRUE(strcmp(p1, "abc") == 0);

  const char *p3 = spool_addn(sp, "alphabet", 5);
  const char *p4 = spool_add(sp, "alpha");
  ASSERT_TRUE(p3 != NULL);
  ASSERT_TRUE(p4 != NULL);
  ASSERT_TRUE(p3 == p4);
  ASSERT_TRUE(strcmp(p3, "alpha") == 0);

  spool_destroy(sp);
}

static void test_empty_string(void) {
  StringPool *sp = spool_create();
  ASSERT_TRUE(sp != NULL);

  const char *p1 = spool_add(sp, "");
  const char *p2 = spool_addn(sp, "x", 0);
  ASSERT_TRUE(p1 != NULL);
  ASSERT_TRUE(p2 != NULL);
  ASSERT_TRUE(p1 == p2);
  ASSERT_TRUE(p1[0] == '\0');

  spool_destroy(sp);
}

static void test_clean_and_reinit(void) {
  StringPool sp = {0};
  ASSERT_TRUE(spool_init(&sp) == OK);
  ASSERT_TRUE(spool_add(&sp, "alpha") != NULL);

  spool_clean(&sp);
  ASSERT_TRUE(spool_add(&sp, "alpha") == NULL);

  ASSERT_TRUE(spool_init(&sp) == OK);
  ASSERT_TRUE(spool_add(&sp, "alpha") != NULL);
  spool_clean(&sp);
}

static void test_many_entries(void) {
  StringPool *sp = spool_create();
  ASSERT_TRUE(sp != NULL);

  char keys[256][32];
  const char *ptrs[256];
  for (int i = 0; i < 256; i++) {
    snprintf(keys[i], sizeof(keys[i]), "k-%d", i);
    ptrs[i] = spool_add(sp, keys[i]);
    ASSERT_TRUE(ptrs[i] != NULL);
  }

  for (int i = 0; i < 256; i++) {
    const char *again = spool_add(sp, keys[i]);
    ASSERT_TRUE(again == ptrs[i]);
  }

  spool_destroy(sp);
}

static void test_invalid_inputs(void) {
  ASSERT_TRUE(spool_init(NULL) == ERR);
  ASSERT_TRUE(spool_add(NULL, "x") == NULL);
  ASSERT_TRUE(spool_addn(NULL, "x", 1) == NULL);

  StringPool sp = {0};
  ASSERT_TRUE(spool_add(&sp, "x") == NULL);
  ASSERT_TRUE(spool_addn(&sp, "x", 1) == NULL);
  ASSERT_TRUE(spool_init(&sp) == OK);
  ASSERT_TRUE(spool_add(&sp, NULL) == NULL);
  ASSERT_TRUE(spool_addn(&sp, NULL, 0) == NULL);
  spool_clean(&sp);
}

int main(void) {
  test_basic_dedup();
  test_addn_and_add_interop();
  test_empty_string();
  test_clean_and_reinit();
  test_many_entries();
  test_invalid_inputs();

  fprintf(stderr, "OK: test_spool\n");
  return 0;
}
