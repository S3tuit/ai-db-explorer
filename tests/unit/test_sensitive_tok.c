#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "pl_arena.h"
#include "sensitive_tok.h"
#include "test.h"

/* Builds one minimal ConnProfile for token-store tests.
 * It returns a stack value that borrows 'connection_name'.
 * Side effects: none.
 * Error semantics: none (test helper).
 */
static ConnProfile make_profile(const char *connection_name,
                                SafetyColumnStrategy mode) {
  ConnProfile cp = {0};
  cp.connection_name = connection_name;
  cp.safe_policy.column_strategy = mode;
  return cp;
}

/* Initializes a bounded arena used by token-store tests.
 * It borrows 'arena' and mutates it in-place.
 * Side effects: allocates first arena block.
 * Error semantics: asserts on failure.
 */
static void init_test_arena(PlArena *arena) {
  uint32_t cap = 4u * 1024u * 1024u;
  ASSERT_TRUE(pl_arena_init(arena, NULL, &cap) == OK);
}

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

/* Validates parse behavior at uint32 boundaries for generation and index. */
static void test_parse_view_u32_bounds(void) {
  ParsedTokView v = {0};

  char max_ok[] = "tok_pgmain_4294967295_4294967295";
  ASSERT_TRUE(stok_parse_view_inplace(max_ok, &v) == OK);
  ASSERT_TRUE(v.generation == UINT32_MAX);
  ASSERT_TRUE(v.index == UINT32_MAX);

  char gen_overflow[] = "tok_pgmain_4294967296_1";
  ASSERT_TRUE(stok_parse_view_inplace(gen_overflow, &v) == ERR);

  char idx_not_number[] = "tok_pgmain_1_2x";
  ASSERT_TRUE(stok_parse_view_inplace(idx_not_number, &v) == ERR);
}

/* Verifies failed parse does not mutate the caller-owned token buffer. */
static void test_parse_view_failure_does_not_mutate_input(void) {
  char token[] = "tok_pgmain_1_x";
  const char *expected = "tok_pgmain_1_x";
  ParsedTokView v = {0};

  ASSERT_TRUE(stok_parse_view_inplace(token, &v) == ERR);
  ASSERT_TRUE(strcmp(token, expected) == 0);
}

static void test_store_init_and_clean(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);

  ASSERT_TRUE(store != NULL);
  ASSERT_TRUE(stok_store_len(store) == 0);

  stok_store_destroy(store);
  stok_store_destroy(NULL);

  pl_arena_clean(&arena);
}

static void test_store_init_bad_input(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  ASSERT_TRUE(stok_store_create(NULL, &arena) == NULL);
  ASSERT_TRUE(stok_store_create(&cp, NULL) == NULL);

  ConnProfile bad = {0};
  ASSERT_TRUE(stok_store_create(&bad, &arena) == NULL);

  ConnProfile bad_mode = make_profile("pgmain", (SafetyColumnStrategy)9999);
  ASSERT_TRUE(stok_store_create(&bad_mode, &arena) == NULL);

  pl_arena_clean(&arena);
}

static void test_store_connection_match_helpers(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile a_cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  ConnProfile b_cp = make_profile("pgmain", SAFETY_COLSTRAT_RANDOMIZED);
  ConnProfile c_cp = make_profile("analytics", SAFETY_COLSTRAT_RANDOMIZED);
  DbTokenStore *a = stok_store_create(&a_cp, &arena);
  DbTokenStore *b = stok_store_create(&b_cp, &arena);
  DbTokenStore *c = stok_store_create(&c_cp, &arena);
  ASSERT_TRUE(a != NULL);
  ASSERT_TRUE(b != NULL);
  ASSERT_TRUE(c != NULL);

  ASSERT_TRUE(stok_store_same_connection(a, b) == YES);
  ASSERT_TRUE(stok_store_same_connection(a, c) == NO);
  ASSERT_TRUE(stok_store_same_connection(NULL, b) == NO);
  ASSERT_TRUE(stok_store_same_connection(NULL, NULL) == NO);

  ASSERT_TRUE(stok_store_matches_conn_name(a, "pgmain") == YES);
  ASSERT_TRUE(stok_store_matches_conn_name(a, "analytics") == NO);
  ASSERT_TRUE(stok_store_matches_conn_name(NULL, "pgmain") == ERR);
  ASSERT_TRUE(stok_store_matches_conn_name(a, NULL) == ERR);

  stok_store_destroy(a);
  stok_store_destroy(b);
  stok_store_destroy(c);
  pl_arena_clean(&arena);
}

/* Verifies accessor edge cases for NULL and out-of-range indexes. */
static void test_store_get_len_edge_cases(void) {
  ASSERT_TRUE(stok_store_len(NULL) == 0);
  ASSERT_TRUE(stok_store_get(NULL, 0) == NULL);

  PlArena arena = {0};
  init_test_arena(&arena);
  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  ASSERT_TRUE(stok_store_len(store) == 0);
  ASSERT_TRUE(stok_store_get(store, 0) == NULL);

  SensitiveTokIn in = {
      .value = "abc",
      .value_len = 3u,
      .col_ref = "public.users.ssn",
      .col_ref_len = (uint32_t)strlen("public.users.ssn"),
      .pg_oid = 23u,
  };
  char tok[SENSITIVE_TOK_BUFSZ] = {0};
  ASSERT_TRUE(stok_store_create_token(store, 1u, &in, tok) > 0);
  ASSERT_TRUE(stok_store_len(store) == 1);
  ASSERT_TRUE(stok_store_get(store, 0) != NULL);
  ASSERT_TRUE(stok_store_get(store, 1) == NULL);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

/* Verifies deterministic mode compares by bytes, not pointer identity. */
static void test_create_token_deterministic_pointer_independence(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  char v1[] = {'a', 'b', 'c'};
  char v2[] = {'a', 'b', 'c'};
  char c1[] = "public.users.ssn";
  char c1_copy[] = "public.users.ssn";
  SensitiveTokIn in1 = {
      .value = v1,
      .value_len = 3u,
      .col_ref = c1,
      .col_ref_len = (uint32_t)strlen(c1),
      .pg_oid = 23u,
  };
  SensitiveTokIn in1_same = {
      .value = v2,
      .value_len = 3u,
      .col_ref = c1_copy,
      .col_ref_len = (uint32_t)strlen(c1_copy),
      .pg_oid = 23u,
  };
  SensitiveTokIn in2 = {
      .value = v1,
      .value_len = 3u,
      .col_ref = "public.users.email",
      .col_ref_len = (uint32_t)strlen("public.users.email"),
      .pg_oid = 25u,
  };
  SensitiveTokIn in3 = {
      .value = "abd",
      .value_len = 3u,
      .col_ref = c1,
      .col_ref_len = (uint32_t)strlen(c1),
      .pg_oid = 23u,
  };
  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  char tok3[SENSITIVE_TOK_BUFSZ] = {0};
  char tok4[SENSITIVE_TOK_BUFSZ] = {0};

  ASSERT_TRUE(stok_store_create_token(store, 9u, &in1, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, 9u, &in1_same, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  ASSERT_TRUE(stok_store_create_token(store, 9u, &in2, tok3) > 0);
  ASSERT_TRUE(strcmp(tok3, tok1) != 0);
  ASSERT_TRUE(stok_store_len(store) == 2);

  ASSERT_TRUE(stok_store_create_token(store, 9u, &in3, tok4) > 0);
  ASSERT_TRUE(strcmp(tok4, tok1) != 0);
  ASSERT_TRUE(stok_store_len(store) == 3);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

/* Verifies SQL NULL payloads are supported and deduplicated in deterministic
 * mode for identical column keys.
 */
static void test_create_token_null_value_deterministic(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  SensitiveTokIn in = {
      .value = NULL,
      .value_len = 0u,
      .col_ref = "public.users.ssn",
      .col_ref_len = (uint32_t)strlen("public.users.ssn"),
      .pg_oid = 23u,
  };
  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};

  ASSERT_TRUE(stok_store_create_token(store, 3u, &in, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, 3u, &in, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  const SensitiveTok *e0 = stok_store_get(store, 0);
  ASSERT_TRUE(e0 != NULL);
  ASSERT_TRUE(e0->value == NULL);
  ASSERT_TRUE(e0->value_len == 0);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

/* Verifies token creation rejects oversized connection names (>CONN_NAME_MAX_LEN). */
static void test_create_token_connection_name_too_long(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  char long_name[CONN_NAME_MAX_LEN + 2];
  memset(long_name, 'a', sizeof(long_name));
  long_name[sizeof(long_name) - 1] = '\0';

  ConnProfile cp = make_profile(long_name, SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  SensitiveTokIn in = {
      .value = "abc",
      .value_len = 3u,
      .col_ref = "public.users.ssn",
      .col_ref_len = (uint32_t)strlen("public.users.ssn"),
      .pg_oid = 23u,
  };
  char tok[SENSITIVE_TOK_BUFSZ] = {0};
  ASSERT_TRUE(stok_store_create_token(store, 1u, &in, tok) < 0);
  ASSERT_TRUE(stok_store_len(store) == 0);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

static void test_create_token_deterministic_reuse(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  const char val[] = "alice";
  SensitiveTokIn in = {
      .value = val,
      .value_len = 5u,
      .col_ref = "public.users.ssn",
      .col_ref_len = (uint32_t)strlen("public.users.ssn"),
      .pg_oid = 23u,
  };

  int n1 = stok_store_create_token(store, 7u, &in, tok1);
  ASSERT_TRUE(n1 > 0);
  ASSERT_TRUE(strcmp(tok1, "tok_pgmain_7_0") == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  int n2 = stok_store_create_token(store, 7u, &in, tok2);
  ASSERT_TRUE(n2 == n1);
  ASSERT_TRUE(strcmp(tok2, tok1) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  const SensitiveTok *e0 = stok_store_get(store, 0);
  ASSERT_TRUE(e0 != NULL);
  ASSERT_TRUE(e0->value != NULL);
  ASSERT_TRUE(e0->value_len == 5u);
  ASSERT_TRUE(memcmp(e0->value, val, 5u) == 0);
  ASSERT_TRUE(e0->value != val);
  ASSERT_TRUE(e0->col_ref_len == in.col_ref_len);
  ASSERT_TRUE(memcmp(e0->col_ref, in.col_ref, in.col_ref_len) == 0);

  const SensitiveTok *nul = stok_store_get(store, 1);
  ASSERT_TRUE(nul == NULL);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

static void test_create_token_randomized_appends(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("analytics", SAFETY_COLSTRAT_RANDOMIZED);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  const char val_bytes[3] = {'a', '\0', 'b'};
  SensitiveTokIn in = {
      .value = val_bytes,
      .value_len = 3u,
      .col_ref = "private.events.payload",
      .col_ref_len = (uint32_t)strlen("private.events.payload"),
      .pg_oid = 25u,
  };

  int n1 = stok_store_create_token(store, 3u, &in, tok1);
  ASSERT_TRUE(n1 > 0);
  ASSERT_TRUE(strcmp(tok1, "tok_analytics_3_0") == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  int n2 = stok_store_create_token(store, 3u, &in, tok2);
  ASSERT_TRUE(n2 > 0);
  ASSERT_TRUE(strcmp(tok2, "tok_analytics_3_1") == 0);
  ASSERT_TRUE(stok_store_len(store) == 2);

  const SensitiveTok *e0 = stok_store_get(store, 0);
  const SensitiveTok *e1 = stok_store_get(store, 1);
  ASSERT_TRUE(e0 != NULL);
  ASSERT_TRUE(e1 != NULL);
  ASSERT_TRUE(e0->value_len == 3u);
  ASSERT_TRUE(e1->value_len == 3u);
  ASSERT_TRUE(memcmp(e0->value, val_bytes, 3u) == 0);
  ASSERT_TRUE(memcmp(e1->value, val_bytes, 3u) == 0);
  ASSERT_TRUE(e0->col_ref_len == in.col_ref_len);
  ASSERT_TRUE(e1->col_ref_len == in.col_ref_len);
  ASSERT_TRUE(memcmp(e0->col_ref, in.col_ref, in.col_ref_len) == 0);
  ASSERT_TRUE(memcmp(e1->col_ref, in.col_ref, in.col_ref_len) == 0);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

static void test_create_token_input_validation(void) {
  PlArena arena = {0};
  init_test_arena(&arena);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store = stok_store_create(&cp, &arena);
  ASSERT_TRUE(store != NULL);

  char tok[SENSITIVE_TOK_BUFSZ] = {0};
  SensitiveTokIn in = {
      .value = "abc",
      .value_len = 3u,
      .col_ref = "public.users.ssn",
      .col_ref_len = (uint32_t)strlen("public.users.ssn"),
      .pg_oid = 23u,
  };

  ASSERT_TRUE(stok_store_create_token(NULL, 1u, &in, tok) < 0);
  ASSERT_TRUE(stok_store_create_token(store, 1u, NULL, tok) < 0);
  ASSERT_TRUE(stok_store_create_token(store, 1u, &in, NULL) < 0);

  SensitiveTokIn bad = in;
  bad.col_ref = NULL;
  ASSERT_TRUE(stok_store_create_token(store, 1u, &bad, tok) < 0);
  bad = in;
  bad.col_ref_len = 0;
  ASSERT_TRUE(stok_store_create_token(store, 1u, &bad, tok) < 0);
  bad = in;
  bad.value = NULL;
  bad.value_len = 1u;
  ASSERT_TRUE(stok_store_create_token(store, 1u, &bad, tok) < 0);

  ASSERT_TRUE(stok_store_create_token(store, 1u, &in, tok) > 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  stok_store_destroy(store);
  pl_arena_clean(&arena);
}

int main(void) {
  test_parse_view_ok();
  test_parse_view_connection_underscore_ok();
  test_parse_view_bad_input();
  test_parse_view_u32_bounds();
  test_parse_view_failure_does_not_mutate_input();
  test_store_init_and_clean();
  test_store_init_bad_input();
  test_store_connection_match_helpers();
  test_store_get_len_edge_cases();
  test_create_token_deterministic_pointer_independence();
  test_create_token_null_value_deterministic();
  test_create_token_connection_name_too_long();
  test_create_token_deterministic_reuse();
  test_create_token_randomized_appends();
  test_create_token_input_validation();
  fprintf(stderr, "OK: test_sensitive_tok\n");
  return 0;
}
