#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "sensitive_tok.h"
#include "test.h"

static SensitiveTokSession *create_test_session(uint32_t cap) {
  SensitiveTokSession *sess = stok_session_create(cap);
  ASSERT_TRUE(sess != NULL);
  ASSERT_TRUE(stok_session_is_ok(sess) == YES);
  return sess;
}

static DbTokenStore *create_test_store(SensitiveTokSession *sess,
                                       const char *conn_name,
                                       SafetyColumnStrategy mode) {
  ConnProfile cp = make_profile(conn_name, mode);
  DbTokenStore *store = stok_session_get_or_create_store(sess, &cp);
  ASSERT_TRUE(store != NULL);
  return store;
}

static DbTokenStore *create_det_store(SensitiveTokSession *sess,
                                      const char *conn_name) {
  return create_test_store(sess, conn_name, SAFETY_COLSTRAT_DETERMINISTIC);
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
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");

  ASSERT_TRUE(store != NULL);
  ASSERT_TRUE(stok_store_len(store) == 0);

  stok_store_destroy(NULL);
  stok_session_destroy(sess);
}

static void test_store_init_bad_input(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);

  ConnProfile cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  ASSERT_TRUE(stok_session_create(0) == NULL);
  ASSERT_TRUE(stok_session_get_or_create_store(NULL, &cp) == NULL);
  ASSERT_TRUE(stok_session_get_or_create_store(sess, NULL) == NULL);

  ConnProfile bad = {0};
  ASSERT_TRUE(stok_session_get_or_create_store(sess, &bad) == NULL);

  ConnProfile bad_mode = make_profile("pgmain", (SafetyColumnStrategy)9999);
  ASSERT_TRUE(stok_session_get_or_create_store(sess, &bad_mode) == NULL);

  stok_session_destroy(sess);
}

static void test_store_connection_match_helpers(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);

  ConnProfile a_cp = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  ConnProfile c_cp = make_profile("analytics", SAFETY_COLSTRAT_RANDOMIZED);
  DbTokenStore *a = stok_session_get_or_create_store(sess, &a_cp);
  DbTokenStore *b = stok_session_get_or_create_store(sess, &a_cp);
  DbTokenStore *c = stok_session_get_or_create_store(sess, &c_cp);
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

  stok_session_destroy(sess);
}

/* Verifies accessor edge cases for NULL and out-of-range indexes. */
static void test_store_get_len_edge_cases(void) {
  ASSERT_TRUE(stok_store_len(NULL) == 0);
  ASSERT_TRUE(stok_store_get(NULL, 0) == NULL);

  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  ASSERT_TRUE(stok_store_len(store) == 0);
  ASSERT_TRUE(stok_store_get(store, 0) == NULL);

  SensitiveTokIn in = {
      .value = "abc",
      .value_len = 3u,
      .domain = "ssn",
      .domain_len = (uint32_t)strlen("ssn"),
      .pg_oid = 23u,
  };
  char tok[SENSITIVE_TOK_BUFSZ] = {0};
  ASSERT_TRUE(stok_store_create_token(store, &in, tok) > 0);
  ASSERT_TRUE(stok_store_len(store) == 1);
  ASSERT_TRUE(stok_store_get(store, 0) != NULL);
  ASSERT_TRUE(stok_store_get(store, 1) == NULL);

  stok_session_destroy(sess);
}

/* Verifies deterministic mode compares domains by bytes, not pointer identity.
 */
static void test_create_token_deterministic_pointer_independence(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  char v1[] = {'a', 'b', 'c'};
  char v2[] = {'a', 'b', 'c'};
  char d1[] = "ssn";
  char d1_copy[] = "ssn";
  SensitiveTokIn in1 = {
      .value = v1,
      .value_len = 3u,
      .domain = d1,
      .domain_len = (uint32_t)strlen(d1),
      .pg_oid = 23u,
  };
  SensitiveTokIn in1_same = {
      .value = v2,
      .value_len = 3u,
      .domain = d1_copy,
      .domain_len = (uint32_t)strlen(d1_copy),
      .pg_oid = 23u,
  };
  SensitiveTokIn in2 = {
      .value = v1,
      .value_len = 3u,
      .domain = "email",
      .domain_len = (uint32_t)strlen("email"),
      .pg_oid = 25u,
  };
  SensitiveTokIn in3 = {
      .value = "abd",
      .value_len = 3u,
      .domain = d1,
      .domain_len = (uint32_t)strlen(d1),
      .pg_oid = 23u,
  };
  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  char tok3[SENSITIVE_TOK_BUFSZ] = {0};
  char tok4[SENSITIVE_TOK_BUFSZ] = {0};

  ASSERT_TRUE(stok_store_create_token(store, &in1, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, &in1_same, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  ASSERT_TRUE(stok_store_create_token(store, &in2, tok3) > 0);
  ASSERT_TRUE(strcmp(tok3, tok1) != 0);
  ASSERT_TRUE(stok_store_len(store) == 2);

  ASSERT_TRUE(stok_store_create_token(store, &in3, tok4) > 0);
  ASSERT_TRUE(strcmp(tok4, tok1) != 0);
  ASSERT_TRUE(stok_store_len(store) == 3);

  stok_session_destroy(sess);
}

/* Verifies SQL NULL payloads are supported and deduplicated in deterministic
 * mode for identical domain keys.
 */
static void test_create_token_null_value_deterministic(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  SensitiveTokIn in = {
      .value = NULL,
      .value_len = 0u,
      .domain = "ssn",
      .domain_len = (uint32_t)strlen("ssn"),
      .pg_oid = 23u,
  };
  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};

  ASSERT_TRUE(stok_store_create_token(store, &in, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, &in, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  const SensitiveTok *e0 = stok_store_get(store, 0);
  ASSERT_TRUE(e0 != NULL);
  ASSERT_TRUE(e0->value == NULL);
  ASSERT_TRUE(e0->value_len == 0);

  stok_session_destroy(sess);
}

/* Verifies token creation rejects oversized connection names
 * (>CONN_NAME_MAX_LEN). */
static void test_create_token_connection_name_too_long(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);

  char long_name[CONN_NAME_MAX_LEN + 2];
  memset(long_name, 'a', sizeof(long_name));
  long_name[sizeof(long_name) - 1] = '\0';

  DbTokenStore *store = create_det_store(sess, long_name);

  SensitiveTokIn in = {
      .value = "abc",
      .value_len = 3u,
      .domain = "ssn",
      .domain_len = (uint32_t)strlen("ssn"),
      .pg_oid = 23u,
  };
  char tok[SENSITIVE_TOK_BUFSZ] = {0};
  ASSERT_TRUE(stok_store_create_token(store, &in, tok) < 0);
  ASSERT_TRUE(stok_store_len(store) == 0);

  stok_session_destroy(sess);
}

static void test_create_token_deterministic_reuse(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  const char val[] = "alice";
  SensitiveTokIn in = {
      .value = val,
      .value_len = 5u,
      .domain = "ssn",
      .domain_len = (uint32_t)strlen("ssn"),
      .pg_oid = 23u,
  };

  int n1 = stok_store_create_token(store, &in, tok1);
  ASSERT_TRUE(n1 > 0);
  ASSERT_TRUE(strcmp(tok1, "tok_pgmain_0_0") == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  int n2 = stok_store_create_token(store, &in, tok2);
  ASSERT_TRUE(n2 == n1);
  ASSERT_TRUE(strcmp(tok2, tok1) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  const SensitiveTok *e0 = stok_store_get(store, 0);
  ASSERT_TRUE(e0 != NULL);
  ASSERT_TRUE(e0->value != NULL);
  ASSERT_TRUE(e0->value_len == 5u);
  ASSERT_TRUE(memcmp(e0->value, val, 5u) == 0);
  ASSERT_TRUE(e0->value != val);
  ASSERT_TRUE(e0->domain_len == in.domain_len);
  ASSERT_TRUE(memcmp(e0->domain, in.domain, in.domain_len) == 0);

  const SensitiveTok *nul = stok_store_get(store, 1);
  ASSERT_TRUE(nul == NULL);

  stok_session_destroy(sess);
}

/* Verifies deterministic mode reuses one token for the same (domain, value)
 * pair even across separate caller-owned input buffers.
 */
static void test_create_token_deterministic_same_domain_same_value(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  char v1[] = "alice";
  char v2[] = "alice";
  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  SensitiveTokIn in1 = {
      .value = v1,
      .value_len = 5u,
      .domain = "fiscal_code",
      .domain_len = (uint32_t)strlen("fiscal_code"),
      .pg_oid = 23u,
  };
  SensitiveTokIn in2 = {
      .value = v2,
      .value_len = 5u,
      .domain = "fiscal_code",
      .domain_len = (uint32_t)strlen("fiscal_code"),
      .pg_oid = 23u,
  };

  ASSERT_TRUE(stok_store_create_token(store, &in1, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, &in2, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  stok_session_destroy(sess);
}

/* Verifies deterministic mode separates equal plaintext values across
 * different sensitive domains.
 */
static void test_create_token_deterministic_different_domain_same_value(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  SensitiveTokIn in1 = {
      .value = "alice",
      .value_len = 5u,
      .domain = "fiscal_code",
      .domain_len = (uint32_t)strlen("fiscal_code"),
      .pg_oid = 23u,
  };
  SensitiveTokIn in2 = {
      .value = "alice",
      .value_len = 5u,
      .domain = "email",
      .domain_len = (uint32_t)strlen("email"),
      .pg_oid = 25u,
  };

  ASSERT_TRUE(stok_store_create_token(store, &in1, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, &in2, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) != 0);
  ASSERT_TRUE(stok_store_len(store) == 2);

  stok_session_destroy(sess);
}

/* Verifies deterministic mode separates different plaintext values inside the
 * same sensitive domain.
 */
static void test_create_token_deterministic_same_domain_different_value(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  SensitiveTokIn in1 = {
      .value = "alice",
      .value_len = 5u,
      .domain = "fiscal_code",
      .domain_len = (uint32_t)strlen("fiscal_code"),
      .pg_oid = 23u,
  };
  SensitiveTokIn in2 = {
      .value = "bob",
      .value_len = 3u,
      .domain = "fiscal_code",
      .domain_len = (uint32_t)strlen("fiscal_code"),
      .pg_oid = 23u,
  };

  ASSERT_TRUE(stok_store_create_token(store, &in1, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, &in2, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) != 0);
  ASSERT_TRUE(stok_store_len(store) == 2);

  stok_session_destroy(sess);
}

/* Verifies deterministic token identity remains isolated per connection even
 * when the same domain and plaintext value are reused.
 */
static void
test_create_token_deterministic_same_domain_same_value_diff_connection(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);

  ConnProfile cp_a = make_profile("pgmain", SAFETY_COLSTRAT_DETERMINISTIC);
  ConnProfile cp_b = make_profile("analytics", SAFETY_COLSTRAT_DETERMINISTIC);
  DbTokenStore *store_a = stok_session_get_or_create_store(sess, &cp_a);
  DbTokenStore *store_b = stok_session_get_or_create_store(sess, &cp_b);
  ASSERT_TRUE(store_a != NULL);
  ASSERT_TRUE(store_b != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  SensitiveTokIn in = {
      .value = "alice",
      .value_len = 5u,
      .domain = "fiscal_code",
      .domain_len = (uint32_t)strlen("fiscal_code"),
      .pg_oid = 23u,
  };

  ASSERT_TRUE(stok_store_create_token(store_a, &in, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store_b, &in, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, tok2) != 0);
  ASSERT_TRUE(stok_store_len(store_a) == 1);
  ASSERT_TRUE(stok_store_len(store_b) == 1);

  stok_session_destroy(sess);
}

/* Verifies randomized mode never deduplicates equal (domain, value) inputs. */
static void test_create_token_randomized_appends(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store =
      create_test_store(sess, "analytics", SAFETY_COLSTRAT_RANDOMIZED);
  ASSERT_TRUE(store != NULL);

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  const char val_bytes[3] = {'a', '\0', 'b'};
  SensitiveTokIn in = {
      .value = val_bytes,
      .value_len = 3u,
      .domain = "payload",
      .domain_len = (uint32_t)strlen("payload"),
      .pg_oid = 25u,
  };

  int n1 = stok_store_create_token(store, &in, tok1);
  ASSERT_TRUE(n1 > 0);
  ASSERT_TRUE(strcmp(tok1, "tok_analytics_0_0") == 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  int n2 = stok_store_create_token(store, &in, tok2);
  ASSERT_TRUE(n2 > 0);
  ASSERT_TRUE(strcmp(tok2, "tok_analytics_0_1") == 0);
  ASSERT_TRUE(stok_store_len(store) == 2);

  const SensitiveTok *e0 = stok_store_get(store, 0);
  const SensitiveTok *e1 = stok_store_get(store, 1);
  ASSERT_TRUE(e0 != NULL);
  ASSERT_TRUE(e1 != NULL);
  ASSERT_TRUE(e0->value_len == 3u);
  ASSERT_TRUE(e1->value_len == 3u);
  ASSERT_TRUE(memcmp(e0->value, val_bytes, 3u) == 0);
  ASSERT_TRUE(memcmp(e1->value, val_bytes, 3u) == 0);
  ASSERT_TRUE(e0->domain_len == in.domain_len);
  ASSERT_TRUE(e1->domain_len == in.domain_len);
  ASSERT_TRUE(memcmp(e0->domain, in.domain, in.domain_len) == 0);
  ASSERT_TRUE(memcmp(e1->domain, in.domain, in.domain_len) == 0);

  stok_session_destroy(sess);
}

/* Verifies token creation resets the session token state when the bounded
 * arena would be exceeded, bumps generation, and leaves old tokens stale.
 */
static void test_create_token_resets_session_on_arena_cap(void) {
  SensitiveTokSession *sess = create_test_session(96u);
  DbTokenStore *store =
      create_test_store(sess, "analytics", SAFETY_COLSTRAT_RANDOMIZED);

  char v1[40];
  char v2[40];
  char v3[40];
  memset(v1, 'a', sizeof(v1));
  memset(v2, 'b', sizeof(v2));
  memset(v3, 'c', sizeof(v3));

  SensitiveTokIn in1 = {
      .value = v1,
      .value_len = (uint32_t)sizeof(v1),
      .domain = "payload",
      .domain_len = (uint32_t)strlen("payload"),
      .pg_oid = 25u,
  };
  SensitiveTokIn in2 = in1;
  in2.value = v2;
  SensitiveTokIn in3 = in1;
  in3.value = v3;

  char tok1[SENSITIVE_TOK_BUFSZ] = {0};
  char tok2[SENSITIVE_TOK_BUFSZ] = {0};
  char tok3[SENSITIVE_TOK_BUFSZ] = {0};
  ASSERT_TRUE(stok_store_create_token(store, &in1, tok1) > 0);
  ASSERT_TRUE(stok_store_create_token(store, &in2, tok2) > 0);
  ASSERT_TRUE(strcmp(tok1, "tok_analytics_0_0") == 0);
  ASSERT_TRUE(strcmp(tok2, "tok_analytics_0_1") == 0);
  ASSERT_TRUE(stok_session_generation(sess) == 0u);
  ASSERT_TRUE(stok_store_len(store) == 2);

  ASSERT_TRUE(stok_store_create_token(store, &in3, tok3) > 0);
  ASSERT_TRUE(strcmp(tok3, "tok_analytics_1_0") == 0);
  ASSERT_TRUE(stok_session_generation(sess) == 1u);
  ASSERT_TRUE(stok_store_len(store) == 1);
  ASSERT_TRUE(stok_session_arena_used(sess) <= 48u);

  char stale_copy[SENSITIVE_TOK_BUFSZ] = {0};
  char fresh_copy[SENSITIVE_TOK_BUFSZ] = {0};
  ASSERT_TRUE(strlen(tok1) < sizeof(stale_copy));
  ASSERT_TRUE(strlen(tok3) < sizeof(fresh_copy));
  strcpy(stale_copy, tok1);
  strcpy(fresh_copy, tok3);

  const SensitiveTok *resolved = NULL;
  ASSERT_TRUE(stok_store_resolve_token(store, stale_copy, &resolved) ==
              STOK_RESOLVE_ERR_STALE);
  ASSERT_TRUE(resolved == NULL);
  ASSERT_TRUE(stok_store_resolve_token(store, fresh_copy, &resolved) ==
              STOK_RESOLVE_OK);
  ASSERT_TRUE(resolved != NULL);
  ASSERT_TRUE(resolved->value_len == sizeof(v3));
  ASSERT_TRUE(memcmp(resolved->value, v3, sizeof(v3)) == 0);

  stok_session_destroy(sess);
}

static void test_create_token_input_validation(void) {
  SensitiveTokSession *sess = create_test_session(4u * 1024u * 1024u);
  DbTokenStore *store = create_det_store(sess, "pgmain");
  ASSERT_TRUE(store != NULL);

  char tok[SENSITIVE_TOK_BUFSZ] = {0};
  SensitiveTokIn in = {
      .value = "abc",
      .value_len = 3u,
      .domain = "ssn",
      .domain_len = (uint32_t)strlen("ssn"),
      .pg_oid = 23u,
  };

  ASSERT_TRUE(stok_store_create_token(NULL, &in, tok) < 0);
  ASSERT_TRUE(stok_store_create_token(store, NULL, tok) < 0);
  ASSERT_TRUE(stok_store_create_token(store, &in, NULL) < 0);

  SensitiveTokIn bad = in;
  bad.domain = NULL;
  ASSERT_TRUE(stok_store_create_token(store, &bad, tok) < 0);
  bad = in;
  bad.domain_len = 0;
  ASSERT_TRUE(stok_store_create_token(store, &bad, tok) < 0);
  bad = in;
  bad.domain = "";
  bad.domain_len = 0u;
  ASSERT_TRUE(stok_store_create_token(store, &bad, tok) < 0);
  bad = in;
  bad.value = NULL;
  bad.value_len = 1u;
  ASSERT_TRUE(stok_store_create_token(store, &bad, tok) < 0);

  ASSERT_TRUE(stok_store_create_token(store, &in, tok) > 0);
  ASSERT_TRUE(stok_store_len(store) == 1);

  stok_session_destroy(sess);
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
  test_create_token_deterministic_same_domain_same_value();
  test_create_token_deterministic_different_domain_same_value();
  test_create_token_deterministic_same_domain_different_value();
  test_create_token_deterministic_same_domain_same_value_diff_connection();
  test_create_token_randomized_appends();
  test_create_token_resets_session_on_arena_cap();
  test_create_token_input_validation();
  fprintf(stderr, "OK: test_sensitive_tok\n");
  return 0;
}
