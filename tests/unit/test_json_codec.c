#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "json_codec.h"
#include "test.h"

/* -------------------------------- helpers -------------------------------- */

/* 'got' must have the same 'got_len' bytes as 'expected'. */
static void assert_bytes_eq(const char *got, size_t got_len,
                            const char *expected, const char *file, int line) {

  size_t exp_len = strlen(expected);

  ASSERT_TRUE_AT(got != NULL, file, line);
  ASSERT_TRUE_AT(got_len == exp_len, file, line);
  ASSERT_TRUE_AT(memcmp(got, expected, exp_len) == 0, file, line);
}

static void test_json_builder_object(void) {
  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_obj_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_str(&sb, "a", "x") == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "b", 2) == OK);
  ASSERT_TRUE(json_kv_l(&sb, "c", -3) == OK);
  ASSERT_TRUE(json_kv_bool(&sb, "d", 1) == OK);
  ASSERT_TRUE(json_kv_bool(&sb, "e", 0) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);

  const char *expected =
      "{\"a\":\"x\",\"b\":2,\"c\":-3,\"d\":true,\"e\":false}";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);

  ASSERT_TRUE(json_rpc_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "id", 1) == OK);
  const char *exp2 = "{\"jsonrpc\":\"2.0\",\"id\":1";
  assert_bytes_eq(sb.data, sb.len, exp2, __FILE__, __LINE__);
  sb_clean(&sb);
}

static void test_json_builder_array(void) {
  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_arr_begin(&sb) == OK);
  ASSERT_TRUE(json_arr_elem_str(&sb, "x") == OK);
  ASSERT_TRUE(json_arr_elem_u64(&sb, 2) == OK);
  ASSERT_TRUE(json_arr_elem_l(&sb, -3) == OK);
  ASSERT_TRUE(json_arr_elem_bool(&sb, 1) == OK);
  ASSERT_TRUE(json_arr_elem_bool(&sb, 0) == OK);
  ASSERT_TRUE(json_arr_end(&sb) == OK);

  const char *expected = "[\"x\",2,-3,true,false]";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);
}

/* Verifies json_arr_elem_raw_json appends trusted raw JSON values into an
 * array without escaping or extra wrapping.
 */
static void test_json_builder_array_raw_json(void) {
  StrBuf sb;
  sb_init(&sb);

  ASSERT_TRUE(json_arr_begin(&sb) == OK);
  ASSERT_TRUE(json_arr_elem_raw_json(&sb, "{\"a\":1}") == OK);
  ASSERT_TRUE(json_arr_elem_raw_json(&sb, "true") == OK);
  ASSERT_TRUE(json_arr_elem_raw_json(&sb, "[\"x\",null]") == OK);
  ASSERT_TRUE(json_arr_end(&sb) == OK);

  const char *expected = "[{\"a\":1},true,[\"x\",null]]";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);
}

/* Verifies json_arr_elem_raw_json fails closed on invalid input pointers.
 */
static void test_json_arr_elem_raw_json_invalid_input(void) {
  StrBuf sb;
  sb_init(&sb);

  ASSERT_TRUE(json_arr_elem_raw_json(NULL, "{\"a\":1}") == ERR);
  ASSERT_TRUE(json_arr_elem_raw_json(&sb, NULL) == ERR);

  sb_clean(&sb);
}

static void test_json_builder_nested(void) {
  StrBuf sb;
  sb_init(&sb);
  ASSERT_TRUE(json_obj_begin(&sb) == OK);
  ASSERT_TRUE(json_kv_obj_begin(&sb, "a") == OK);
  ASSERT_TRUE(json_kv_u64(&sb, "b", 1) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);
  ASSERT_TRUE(json_kv_arr_begin(&sb, "c") == OK);
  ASSERT_TRUE(json_arr_elem_bool(&sb, 1) == OK);
  ASSERT_TRUE(json_arr_end(&sb) == OK);
  ASSERT_TRUE(json_obj_end(&sb) == OK);

  const char *expected = "{\"a\":{\"b\":1},\"c\":[true]}";
  assert_bytes_eq(sb.data, sb.len, expected, __FILE__, __LINE__);
  sb_clean(&sb);
}

static void test_jsget_simple_rpc_validation(void) {
  const char *ok =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\",\"params\":{}}";
  const char *ok_str =
      "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"exec\"}";
  const char *no_id = "{\"jsonrpc\":\"2.0\",\"method\":\"exec\"}";
  const char *bad_ver = "{\"jsonrpc\":\"2.1\",\"id\":1,\"method\":\"exec\"}";
  const char *bad_json = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"exec\"";

  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  ASSERT_TRUE(jsget_init(&jg, ok, strlen(ok), &tok_buf) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == YES);

  ASSERT_TRUE(jsget_init(&jg, ok_str, strlen(ok_str), &tok_buf) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == YES);

  ASSERT_TRUE(jsget_init(&jg, no_id, strlen(no_id), &tok_buf) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == NO);

  ASSERT_TRUE(jsget_init(&jg, bad_ver, strlen(bad_ver), &tok_buf) == OK);
  ASSERT_TRUE(jsget_simple_rpc_validation(&jg) == NO);

  ASSERT_TRUE(jsget_init(&jg, bad_json, strlen(bad_json), &tok_buf) == ERR);
}

static void test_jsget_create_and_destroy(void) {
  const char *json = "{\"a\":{\"b\":{\"c\":\"z\"}},\"arr\":[1,2,3]}";
  JsonGetter jg = {0};
  JsonGetter sub = {0};
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_create(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_string_span(&jg, "a.b.c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');

  ASSERT_TRUE(jsget_object(&jg, "a.b", &sub) == YES);
  ASSERT_TRUE(jsget_string_span(&sub, "c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');

  // Child views never own tokens; destroy must be a no-op.
  jsget_destroy(&sub);
  jsget_destroy(&jg);
}

static void test_jsget_create_inplace_object_view_keeps_ownership(void) {
  const char *json = "{\"a\":{\"b\":{\"c\":\"z\"}},\"x\":2}";
  JsonGetter jg = {0};
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_create(&jg, json, strlen(json)) == OK);
  ASSERT_TRUE(jsget_object(&jg, "a.b", &jg) == YES);
  ASSERT_TRUE(jsget_string_span(&jg, "c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');
  jsget_destroy(&jg);
}

static void test_jsget_paths(void) {
  const char *json =
      "{\"a\":\"x\",\"b\":{\"c\":\"hello\",\"d\":{\"e\":\"z\"}}}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);

  char *s1 = NULL;
  char *s2 = NULL;
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_string_span(&jg, "a", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'x');

  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "b.c", &s1) == YES);
  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "b.d.e", &s2) == YES);
  ASSERT_STREQ(s1, "hello");
  ASSERT_STREQ(s2, "z");

  free(s1);
  free(s2);
}

static void test_jsget_object_view(void) {
  const char *json = "{\"a\":{\"b\":{\"c\":\"z\",\"num\":77},\"n\":1},\"x\":2}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonGetter sub;
  JsonStrSpan sp = {0};
  uint32_t num = 0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_object(&jg, "a.b", &sub) == YES);
  ASSERT_TRUE(jsget_string_span(&sub, "c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');
  ASSERT_TRUE(jsget_u32(&sub, "num", &num) == YES);
  ASSERT_TRUE(num == 77);

  ASSERT_TRUE(jsget_object(&jg, "a.n", &sub) == ERR);
  ASSERT_TRUE(jsget_object(&jg, "a.missing", &sub) == NO);
}

static void test_jsget_object_inplace(void) {
  const char *json = "{\"a\":{\"b\":{\"c\":\"z\"}},\"x\":2}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_object(&jg, "a.b", &jg) == YES);
  ASSERT_TRUE(jsget_string_span(&jg, "c", &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'z');
}

static void test_jsget_null_and_overflow(void) {
  const char *json = "{\"a\":null,\"b\":4294967296}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);

  uint32_t v = 0;
  ASSERT_TRUE(jsget_u32(&jg, "a", &v) == NO);
  ASSERT_TRUE(jsget_u32(&jg, "b", &v) == ERR);
}

static void test_jsget_u32_and_bool(void) {
  const char *json = "{\"id\":7,\"ok\":true,\"err\":false}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  uint32_t id = 0;
  int ok = 0;
  int err = 10;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_u32(&jg, "id", &id) == YES);
  ASSERT_TRUE(id == 7);
  ASSERT_TRUE(jsget_bool01(&jg, "ok", &ok) == YES);
  ASSERT_TRUE(ok == 1);
  ASSERT_TRUE(jsget_bool01(&jg, "err", &err) == YES);
  ASSERT_TRUE(err == 0);
}

static void test_jsget_f64(void) {
  const char *json = "{\"pi\":3.1415,\"i\":2,\"bad\":\"x\"}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  double v = 0.0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_f64(&jg, "pi", &v) == YES);
  ASSERT_TRUE(v > 3.14 && v < 3.15);
  ASSERT_TRUE(jsget_f64(&jg, "i", &v) == YES);
  ASSERT_TRUE(v == 2.0);
  ASSERT_TRUE(jsget_f64(&jg, "bad", &v) == ERR);
  ASSERT_TRUE(jsget_f64(&jg, "missing", &v) == NO);
}

static void test_jsget_i64(void) {
  const char *json = "{\"n\":-12,\"z\":0,\"bad\":3.1}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  int64_t v = 0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_i64(&jg, "n", &v) == YES);
  ASSERT_TRUE(v == -12);
  ASSERT_TRUE(jsget_i64(&jg, "z", &v) == YES);
  ASSERT_TRUE(v == 0);
  ASSERT_TRUE(jsget_i64(&jg, "bad", &v) == ERR);
  ASSERT_TRUE(jsget_i64(&jg, "missing", &v) == NO);
}

static void test_jsget_string_span_and_decode(void) {
  const char *json = "{\"raw\":\"a\\\\n\\\"b\\\"\"}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonStrSpan sp = {0};
  char *decoded = NULL;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_string_span(&jg, "raw", &sp) == YES);
  ASSERT_TRUE(sp.len == strlen("a\\\\n\\\"b\\\""));
  ASSERT_TRUE(jsget_string_decode_alloc(&jg, "raw", &decoded) == YES);
  ASSERT_STREQ(decoded, "a\\n\"b\"");
  free(decoded);
}

static void test_jsget_array_strings(void) {
  const char *json = "{\"arr\":[\"x\",\"y\"]}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonArrIter it;
  JsonStrSpan sp = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_array_strings_begin(&jg, "arr", &it) == YES);

  ASSERT_TRUE(jsget_array_strings_next(&jg, &it, &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'x');

  ASSERT_TRUE(jsget_array_strings_next(&jg, &it, &sp) == YES);
  ASSERT_TRUE(sp.len == 1);
  ASSERT_TRUE(sp.ptr[0] == 'y');

  ASSERT_TRUE(jsget_array_strings_next(&jg, &it, &sp) == NO);
}

static void test_jsget_array_objects(void) {
  const char *json = "{\"arr\":[{\"a\":1},{\"b\":2}]}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonArrIter it;
  JsonGetter obj = {0};
  uint32_t v = 0;

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_array_objects_begin(&jg, "arr", &it) == YES);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  ASSERT_TRUE(jsget_u32(&obj, "a", &v) == YES);
  ASSERT_TRUE(v == 1);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == YES);
  ASSERT_TRUE(jsget_u32(&obj, "b", &v) == YES);
  ASSERT_TRUE(v == 2);

  ASSERT_TRUE(jsget_array_objects_next(&jg, &it, &obj) == NO);
}

static void test_jsget_object_members(void) {
  const char *json =
      "{\"domains\":{\"email\":[\"a\",\"b\"],\"phone\":[\"c\"]}}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonObjIter oit;
  JsonArrIter ait;
  JsonGetter val = {0};
  JsonStrSpan key = {0};
  JsonStrSpan elem = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_object_members_begin(&jg, "domains", &oit) == YES);

  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == YES);
  ASSERT_TRUE(key.len == 5);
  ASSERT_TRUE(memcmp(key.ptr, "email", key.len) == 0);
  ASSERT_TRUE(jsget_array_strings_begin(&val, NULL, &ait) == YES);
  ASSERT_TRUE(jsget_array_strings_next(&val, &ait, &elem) == YES);
  ASSERT_TRUE(elem.len == 1);
  ASSERT_TRUE(elem.ptr[0] == 'a');
  ASSERT_TRUE(jsget_array_strings_next(&val, &ait, &elem) == YES);
  ASSERT_TRUE(elem.len == 1);
  ASSERT_TRUE(elem.ptr[0] == 'b');
  ASSERT_TRUE(jsget_array_strings_next(&val, &ait, &elem) == NO);

  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == YES);
  ASSERT_TRUE(key.len == 5);
  ASSERT_TRUE(memcmp(key.ptr, "phone", key.len) == 0);
  ASSERT_TRUE(jsget_array_strings_begin(&val, NULL, &ait) == YES);
  ASSERT_TRUE(jsget_array_strings_next(&val, &ait, &elem) == YES);
  ASSERT_TRUE(elem.len == 1);
  ASSERT_TRUE(elem.ptr[0] == 'c');
  ASSERT_TRUE(jsget_array_strings_next(&val, &ait, &elem) == NO);

  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == NO);
}

static void test_jsget_object_members_validation(void) {
  const char *json = "{\"domains\":null,\"array\":[1],\"obj\":{\"x\":1}}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonObjIter oit;
  JsonGetter val = {0};
  JsonStrSpan key = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_object_members_begin(&jg, "missing", &oit) == NO);
  ASSERT_TRUE(jsget_object_members_begin(&jg, "domains", &oit) == NO);
  ASSERT_TRUE(jsget_object_members_begin(&jg, "array", &oit) == ERR);
  ASSERT_TRUE(jsget_object_members_begin(&jg, "obj", &oit) == YES);
  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == YES);
  ASSERT_TRUE(jsget_object_members_begin(&val, NULL, &oit) == ERR);
}

static void test_jsget_object_members_begin_null_key(void) {
  const char *json = "{\"obj\":{\"x\":1},\"nil\":null,\"arr\":[1]}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonObjIter oit;
  JsonStrSpan key = {0};
  JsonGetter val = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_object_members_begin(&jg, NULL, &oit) == YES);
  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == YES);
  ASSERT_TRUE(key.len == 3);
  ASSERT_TRUE(memcmp(key.ptr, "obj", key.len) == 0);
  ASSERT_TRUE(jsget_object_members_begin(&val, NULL, &oit) == YES);

  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == YES);
  ASSERT_TRUE(key.len == 3);
  ASSERT_TRUE(memcmp(key.ptr, "nil", key.len) == 0);
  ASSERT_TRUE(jsget_object_members_begin(&val, NULL, &oit) == NO);

  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, &val) == YES);
  ASSERT_TRUE(key.len == 3);
  ASSERT_TRUE(memcmp(key.ptr, "arr", key.len) == 0);
  ASSERT_TRUE(jsget_object_members_begin(&val, NULL, &oit) == ERR);
}

static void test_jsget_object_members_input_validation(void) {
  const char *json = "{\"obj\":{\"x\":1}}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  JsonObjIter oit = {0};
  JsonStrSpan key = {0};
  JsonGetter val = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_object_members_begin(NULL, "obj", &oit) == ERR);
  ASSERT_TRUE(jsget_object_members_begin(&jg, "obj", NULL) == ERR);

  ASSERT_TRUE(jsget_object_members_begin(&jg, "obj", &oit) == YES);
  ASSERT_TRUE(jsget_object_members_next(NULL, &oit, &key, &val) == ERR);
  ASSERT_TRUE(jsget_object_members_next(&jg, NULL, &key, &val) == ERR);
  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, NULL, &val) == ERR);
  ASSERT_TRUE(jsget_object_members_next(&jg, &oit, &key, NULL) == ERR);
}

static void test_jsget_top_level_validation(void) {
  const char *json = "{\"a\":1,\"b\":2}";
  const char *json_extra = "{\"a\":1,\"b\":2,\"c\":3}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  const char *allowed[] = {"a", "b"};
  JsonStrSpan unknown = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_top_level_validation(&jg, NULL, allowed, 2, &unknown) ==
              YES);
  ASSERT_TRUE(unknown.ptr == NULL);
  ASSERT_TRUE(unknown.len == 0);

  ASSERT_TRUE(jsget_init(&jg, json_extra, strlen(json_extra), &tok_buf) == OK);
  ASSERT_TRUE(jsget_top_level_validation(&jg, NULL, allowed, 2, &unknown) ==
              NO);
  ASSERT_TRUE(unknown.ptr != NULL);
  ASSERT_TRUE(unknown.len == 1);
  ASSERT_TRUE(strncmp(unknown.ptr, "c", 1) == 0);

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(
      jsget_top_level_validation(&jg, "missing", allowed, 2, &unknown) == NO);
  ASSERT_TRUE(unknown.ptr == NULL);
  ASSERT_TRUE(unknown.len == 0);
}

static void test_jsget_exists_nonnull(void) {
  const char *json = "{\"a\":null,\"b\":1,\"c\":\"x\"}";
  const char *json_str = "{\"a\":\"null\"}";
  JsonGetter jg;
  JsonTokBuf tok_buf = {0};

  ASSERT_TRUE(jsget_init(&jg, json, strlen(json), &tok_buf) == OK);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "a") == NO);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "b") == YES);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "c") == YES);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "missing") == NO);

  ASSERT_TRUE(jsget_init(&jg, json_str, strlen(json_str), &tok_buf) == OK);
  ASSERT_TRUE(jsget_exists_nonnull(&jg, "a") == YES);
}

int main(void) {
  test_json_builder_object();
  test_json_builder_array();
  test_json_builder_array_raw_json();
  test_json_arr_elem_raw_json_invalid_input();
  test_json_builder_nested();
  test_jsget_simple_rpc_validation();
  test_jsget_create_and_destroy();
  test_jsget_create_inplace_object_view_keeps_ownership();
  test_jsget_paths();
  test_jsget_object_view();
  test_jsget_object_inplace();
  test_jsget_null_and_overflow();
  test_jsget_u32_and_bool();
  test_jsget_f64();
  test_jsget_i64();
  test_jsget_string_span_and_decode();
  test_jsget_array_strings();
  test_jsget_array_objects();
  test_jsget_object_members();
  test_jsget_object_members_validation();
  test_jsget_object_members_begin_null_key();
  test_jsget_object_members_input_validation();
  test_jsget_top_level_validation();
  test_jsget_exists_nonnull();

  fprintf(stderr, "OK: test_json\n");
  return (0);
}
