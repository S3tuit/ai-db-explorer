#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "string_op.h"
#include "test.h"

static void test_dup_functions_basic(void) {
  const char *s = "hello";

  char *full = dup_or_null(s);
  ASSERT_STREQ(full, "hello");
  free(full);

  char *capped = dupn_or_null(s, 3);
  ASSERT_STREQ(capped, "he");
  free(capped);

  char *fits = dupn_or_null(s, 6);
  ASSERT_STREQ(fits, "hello");
  free(fits);

  ASSERT_TRUE(dupn_or_null(NULL, 4) == NULL);
  ASSERT_TRUE(dup_or_null(NULL) == NULL);
}

static void test_dup_pretty(void) {
  const char *s = "abcdef";

  char *pretty = dupn_or_null_pretty(s, 6);
  ASSERT_STREQ(pretty, "ab...");
  free(pretty);

  char *pretty_small = dupn_or_null_pretty(s, 4);
  ASSERT_STREQ(pretty_small, "...");
  free(pretty_small);

  char *pretty_tiny = dupn_or_null_pretty(s, 3);
  ASSERT_STREQ(pretty_tiny, "ab");
  free(pretty_tiny);
}

static void test_sb_append_bytes(void) {
  StrBuf sb = {0};

  ASSERT_TRUE(sb_append_bytes(&sb, "abc", 3) == OK);
  ASSERT_TRUE(sb.len == 3);
  ASSERT_TRUE(memcmp(sb.data, "abc", 3) == 0);

  ASSERT_TRUE(sb_append_bytes(&sb, "def", 3) == OK);
  ASSERT_TRUE(sb.len == 6);
  ASSERT_TRUE(memcmp(sb.data, "abcdef", 6) == 0);

  ASSERT_TRUE(sb_append_bytes(&sb, NULL, 0) == OK);
  ASSERT_TRUE(sb.len == 6);

  sb_clean(&sb);
  ASSERT_TRUE(sb.data == NULL);
  ASSERT_TRUE(sb.len == 0);
  ASSERT_TRUE(sb.cap == 0);
}

static void test_sb_prepare_for_write(void) {
  StrBuf sb = {0};
  char *dst = NULL;

  ASSERT_TRUE(sb_prepare_for_write(&sb, 3, &dst) == OK);
  ASSERT_TRUE(dst != NULL);
  memcpy(dst, "abc", 3);
  ASSERT_TRUE(sb.len == 3);
  ASSERT_TRUE(memcmp(sb.data, "abc", 3) == 0);

  dst = (char *)0x1;
  ASSERT_TRUE(sb_prepare_for_write(&sb, 0, &dst) == OK);
  ASSERT_TRUE(dst == NULL);
  ASSERT_TRUE(sb.len == 3);

  ASSERT_TRUE(sb_prepare_for_write(&sb, 2, &dst) == OK);
  ASSERT_TRUE(dst != NULL);
  memcpy(dst, "de", 2);
  ASSERT_TRUE(sb.len == 5);
  ASSERT_TRUE(memcmp(sb.data, "abcde", 5) == 0);

  sb_clean(&sb);
}

static void test_sb_append_hard_limit(void) {
  StrBuf sb = {0};
  char one = 'x';

  ASSERT_TRUE(sb_append_bytes(&sb, &one, STRBUF_MAX_BYTES + 1) == ERR);

  sb.data = (char *)xmalloc(1);
  sb.len = STRBUF_MAX_BYTES;
  sb.cap = STRBUF_MAX_BYTES;
  ASSERT_TRUE(sb_append_bytes(&sb, &one, 1) == ERR);

  sb_clean(&sb);
}

static void test_sb_to_cstr_basic(void) {
  StrBuf sb = {0};
  ASSERT_TRUE(sb_append_bytes(&sb, "abc", 3) == OK);

  const char *s = sb_to_cstr(&sb);
  ASSERT_TRUE(s != NULL);
  ASSERT_STREQ(s, "abc");
  ASSERT_TRUE(sb.len == 3);

  sb_clean(&sb);
}

static void test_sb_to_cstr_null_inputs(void) {
  ASSERT_STREQ(sb_to_cstr(NULL), "");

  StrBuf sb = {0};
  ASSERT_STREQ(sb_to_cstr(&sb), "");
}

static void test_sb_to_cstr_len_eq_cap_grow(void) {
  StrBuf sb = {0};
  ASSERT_TRUE(sb_append_bytes(&sb, "abc", 3) == OK);

  /* Force len==cap so sb_to_cstr must reserve one more byte. */
  sb.cap = sb.len;
  size_t old_len = sb.len;
  size_t old_cap = sb.cap;

  const char *s = sb_to_cstr(&sb);
  ASSERT_STREQ(s, "abc");
  ASSERT_TRUE(sb.len == old_len);
  ASSERT_TRUE(sb.cap > old_cap);

  sb_clean(&sb);
}

static void test_sb_to_cstr_hard_limit_cannot_grow(void) {
  StrBuf sb = {0};

  sb.data = (char *)malloc(1);
  ASSERT_TRUE(sb.data != NULL);
  sb.len = STRBUF_MAX_BYTES;
  sb.cap = STRBUF_MAX_BYTES;

  ASSERT_STREQ(sb_to_cstr(&sb), "");
  ASSERT_TRUE(sb.len == STRBUF_MAX_BYTES);
  ASSERT_TRUE(sb.cap == STRBUF_MAX_BYTES);

  sb_clean(&sb);
}

int main(void) {
  test_dup_functions_basic();
  test_dup_pretty();
  test_sb_append_bytes();
  test_sb_prepare_for_write();
  test_sb_append_hard_limit();
  test_sb_to_cstr_basic();
  test_sb_to_cstr_null_inputs();
  test_sb_to_cstr_len_eq_cap_grow();
  test_sb_to_cstr_hard_limit_cannot_grow();

  fprintf(stderr, "OK: test_string_op\n");
  return 0;
}
