#include <string.h>

#include "packed_array.h"
#include "sensitive_tok.h"
#include "test.h"

static void test_parse_view_ok(void) {
  char token[] = "tok_pgmain_7_42";
  ParsedTokView v = {0};
  ASSERT_TRUE(stok_parse_view_inplace(token, &v) == OK);
  ASSERT_STREQ(v.connection_name, "pgmain");
  ASSERT_TRUE(v.generation == 7u);
  ASSERT_TRUE(v.index == 42u);
}

static void test_parse_view_connection_underscore_ok(void) {
  char token[] = "tok_my_conn_name_12_3";
  ParsedTokView v = {0};
  ASSERT_TRUE(stok_parse_view_inplace(token, &v) == OK);
  ASSERT_STREQ(v.connection_name, "my_conn_name");
  ASSERT_TRUE(v.generation == 12u);
  ASSERT_TRUE(v.index == 3u);
}

static void test_parse_view_bad_input(void) {
  ParsedTokView v = {0};
  ASSERT_TRUE(stok_parse_view_inplace(NULL, &v) == ERR);

  char bad_prefix[] = "ttk_pgmain_1_2";
  ASSERT_TRUE(stok_parse_view_inplace(bad_prefix, &v) == ERR);

  char missing_parts[] = "tok_pgmain_1";
  ASSERT_TRUE(stok_parse_view_inplace(missing_parts, &v) == ERR);

  char not_number[] = "tok_pgmain_x_2";
  ASSERT_TRUE(stok_parse_view_inplace(not_number, &v) == ERR);

  char overflow[] = "tok_pgmain_1_4294967296";
  ASSERT_TRUE(stok_parse_view_inplace(overflow, &v) == ERR);
}

static void test_store_lazy_init(void) {
  PackedArray *stores = stok_store_array_create();
  ASSERT_TRUE(stores != NULL);
  ASSERT_TRUE(parr_len(stores) == 0);

  ConnProfile cp1 = {0};
  cp1.connection_name = "MyPostgres";
  cp1.safe_policy.column_strategy = SAFETY_COLSTRAT_DETERMINISTIC;

  DbTokenStore *s1 = NULL;
  ASSERT_TRUE(stok_store_get_or_init(stores, &cp1, &s1) == OK);
  ASSERT_TRUE(s1 != NULL);
  ASSERT_STREQ(s1->connection_name, "MyPostgres");
  ASSERT_TRUE(s1->mode == SAFETY_COLSTRAT_DETERMINISTIC);
  ASSERT_TRUE(parr_len(stores) == 1);

  DbTokenStore *s1_again = NULL;
  ASSERT_TRUE(stok_store_get_or_init(stores, &cp1, &s1_again) == OK);
  ASSERT_TRUE(s1_again == s1);
  ASSERT_TRUE(parr_len(stores) == 1);

  ConnProfile cp2 = {0};
  cp2.connection_name = "Analytics";
  cp2.safe_policy.column_strategy = SAFETY_COLSTRAT_RANDOMIZED;

  DbTokenStore *s2 = NULL;
  ASSERT_TRUE(stok_store_get_or_init(stores, &cp2, &s2) == OK);
  ASSERT_TRUE(s2 != NULL);
  ASSERT_STREQ(s2->connection_name, "Analytics");
  ASSERT_TRUE(s2->mode == SAFETY_COLSTRAT_RANDOMIZED);
  ASSERT_TRUE(parr_len(stores) == 2);

  parr_destroy(stores);
}

static void test_store_lazy_init_bad_input(void) {
  PackedArray *stores = stok_store_array_create();
  ASSERT_TRUE(stores != NULL);

  ConnProfile cp = {0};
  cp.connection_name = "MyPostgres";
  cp.safe_policy.column_strategy = SAFETY_COLSTRAT_DETERMINISTIC;

  DbTokenStore *out = NULL;
  ASSERT_TRUE(stok_store_get_or_init(NULL, &cp, &out) == ERR);
  ASSERT_TRUE(stok_store_get_or_init(stores, NULL, &out) == ERR);
  ASSERT_TRUE(stok_store_get_or_init(stores, &cp, NULL) == ERR);

  ConnProfile bad = {0};
  ASSERT_TRUE(stok_store_get_or_init(stores, &bad, &out) == ERR);

  parr_destroy(stores);
}

int main(void) {
  test_parse_view_ok();
  test_parse_view_connection_underscore_ok();
  test_parse_view_bad_input();
  test_store_lazy_init();
  test_store_lazy_init_bad_input();
  fprintf(stderr, "OK: test_sensitive_tok\n");
  return 0;
}
