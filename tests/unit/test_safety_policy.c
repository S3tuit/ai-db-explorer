#include <stdio.h>
#include <stdint.h>

#include "test.h"
#include "safety_policy.h"

static void test_init_defaults_and_overrides(void) {
  SafetyPolicy p;

  int read_only = 0;
  uint32_t max_rows = 123;
  uint32_t max_cell_bytes = 2048;
  uint32_t statement_timeout_ms = 777;

  int rc = safety_policy_init(&p, &read_only, &max_rows,
                                       &max_cell_bytes, &statement_timeout_ms);
  ASSERT_TRUE(rc == 1);
  ASSERT_TRUE(p.read_only == 0);
  ASSERT_TRUE(p.max_rows == 123);
  ASSERT_TRUE(p.max_cell_bytes == 2048);
  ASSERT_TRUE(p.statement_timeout_ms == 777);

  rc = safety_policy_init(&p, NULL, NULL, NULL, NULL);
  ASSERT_TRUE(rc == 1);
  ASSERT_TRUE(p.read_only == 1);
  ASSERT_TRUE(p.max_rows == 200);
  ASSERT_TRUE(p.max_cell_bytes == 65536);
  ASSERT_TRUE(p.statement_timeout_ms == 5000);

  // safe minimun for max_cell_bytes
  uint32_t too_low = 2;
  rc = safety_policy_init(&p, NULL, NULL, &too_low, NULL);
  ASSERT_TRUE(p.max_cell_bytes == 56);
}

int main(void) {
  test_init_defaults_and_overrides();
  fprintf(stderr, "OK: test_safety_policy\n");
  return 0;
}
