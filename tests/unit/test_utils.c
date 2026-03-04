#include <stdio.h>
#include <string.h>

#include "test.h"
#include "utils.h"

/* Returns YES when 'c' is one lowercase hex digit [0-9a-f]. */
static AdbxTriStatus is_hex_digit(char c) {
  if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
    return YES;
  return NO;
}

/* Verifies fill_random_hex handles NULL/zero-length inputs safely. */
static void test_fill_random_hex_input_validation(void) {
  char out[1] = {'X'};

  ASSERT_TRUE(fill_random_hex(NULL, 1) == ERR);
  ASSERT_TRUE(fill_random_hex(NULL, 0) == OK);
  ASSERT_TRUE(fill_random_hex(out, 0) == OK);
  ASSERT_TRUE(out[0] == 'X');
}

/* Verifies fill_random_hex fills requested length with lowercase hex digits. */
static void test_fill_random_hex_output_format(void) {
  char out[513];
  memset(out, 'Z', sizeof(out));

  ASSERT_TRUE(fill_random_hex(out, 512) == OK);
  for (size_t i = 0; i < 512; i++) {
    ASSERT_TRUE(is_hex_digit(out[i]) == YES);
  }

  /* Ensure byte after requested range is untouched (no implicit terminator). */
  ASSERT_TRUE(out[512] == 'Z');
}

/* Verifies fill_random writes bytes and accepts zero-length requests. */
static void test_fill_random_basic(void) {
  uint8_t out[32];
  memset(out, 0, sizeof(out));

  ASSERT_TRUE(fill_random(out, sizeof(out)) == OK);
  ASSERT_TRUE(fill_random(out, 0) == OK);
}

int main(void) {
  test_fill_random_hex_input_validation();
  test_fill_random_hex_output_format();
  test_fill_random_basic();

  fprintf(stderr, "OK: test_utils\n");
  return 0;
}
