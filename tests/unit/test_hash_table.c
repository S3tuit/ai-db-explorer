#include <stdio.h>
#include <string.h>

#include "hash_table.h"
#include "test.h"

static int ht_put_cstr(HashTable *ht, const char *key, const void *value) {
  if (!key)
    return ERR;
  size_t len = strlen(key);
  if (len > UINT32_MAX)
    return ERR;
  return ht_put(ht, key, (uint32_t)len, value);
}

static const void *ht_get_cstr(const HashTable *ht, const char *key) {
  if (!key)
    return NULL;
  size_t len = strlen(key);
  if (len > UINT32_MAX)
    return NULL;
  return ht_get(ht, key, (uint32_t)len);
}

typedef struct TestCustomKey {
  const char *col_ref;
  const char *value;
  uint32_t value_len;
} TestCustomKey;

/* Hashes TestCustomKey by (col_ref,value_bytes).
 * It borrows 'key_v' and does not allocate.
 * Side effects: none.
 * Error semantics: returns non-zero hash for valid inputs, 0 on invalid input.
 */
static uint64_t test_custom_hash(const void *key_v, void *ctx) {
  (void)ctx;
  const TestCustomKey *key = (const TestCustomKey *)key_v;
  if (!key || !key->col_ref || (!key->value && key->value_len != 0))
    return 0;

  size_t col_len = strlen(key->col_ref);
  uint64_t h_col = ht_hash_bytes((const void *)key->col_ref, col_len);
  uint64_t h_val = ht_hash_bytes((const void *)key->value, key->value_len);
  uint64_t mix =
      h_col ^ (h_val + 0x9e3779b97f4a7c15ull + (h_col << 6) + (h_col >> 2));
  return (mix == 0) ? 1 : mix;
}

/* Deliberately returns zero to verify custom-key invalid-hash rejection.
 * It borrows input and does not allocate.
 * Side effects: none.
 * Error semantics: always returns 0.
 */
static uint64_t test_custom_hash_zero(const void *key_v, void *ctx) {
  (void)key_v;
  (void)ctx;
  return 0;
}

/* Compares TestCustomKey by (col_ref,value_bytes).
 * It borrows 'a_v' and 'b_v' and does not allocate.
 * Side effects: none.
 * Error semantics: returns YES when keys are equal, NO otherwise.
 */
static int test_custom_eq(const void *a_v, const void *b_v, void *ctx) {
  (void)ctx;
  const TestCustomKey *a = (const TestCustomKey *)a_v;
  const TestCustomKey *b = (const TestCustomKey *)b_v;
  if (!a || !b || !a->col_ref || !b->col_ref)
    return NO;
  if ((!a->value && a->value_len != 0) || (!b->value && b->value_len != 0))
    return NO;
  if (strcmp(a->col_ref, b->col_ref) != 0)
    return NO;
  if (a->value_len != b->value_len)
    return NO;
  if (a->value_len == 0)
    return YES;
  return (memcmp(a->value, b->value, a->value_len) == 0) ? YES : NO;
}

static void test_hash_bytes_edge_cases(void) {
  const char *s = "alpha";
  uint64_t h1 = ht_hash_bytes(s, 5);
  uint64_t h2 = ht_hash_bytes(s, 5);
  ASSERT_TRUE(h1 != 0);
  ASSERT_TRUE(h1 == h2);

  uint64_t h_empty_1 = ht_hash_bytes("", 0);
  uint64_t h_empty_2 = ht_hash_bytes(NULL, 0);
  ASSERT_TRUE(h_empty_1 != 0);
  ASSERT_TRUE(h_empty_2 != 0);
  ASSERT_TRUE(h_empty_1 == h_empty_2);

  const uint8_t bin[] = {0x00, 0x11, 0x00, 0x22};
  uint64_t h_bin_1 = ht_hash_bytes(bin, sizeof(bin));
  uint64_t h_bin_2 = ht_hash_bytes(bin, sizeof(bin));
  ASSERT_TRUE(h_bin_1 != 0);
  ASSERT_TRUE(h_bin_1 == h_bin_2);
}

static void test_hash_bytes_with_seed_edge_cases(void) {
  const char *s = "alpha";
  uint64_t h1 = ht_hash_bytes_withSeed(s, 5, 123u);
  uint64_t h2 = ht_hash_bytes_withSeed(s, 5, 123u);
  uint64_t h3 = ht_hash_bytes_withSeed(s, 5, 124u);
  ASSERT_TRUE(h1 != 0);
  ASSERT_TRUE(h1 == h2);
  ASSERT_TRUE(h1 != h3);

  uint64_t h_empty_1 = ht_hash_bytes_withSeed("", 0, 77u);
  uint64_t h_empty_2 = ht_hash_bytes_withSeed(NULL, 0, 77u);
  ASSERT_TRUE(h_empty_1 != 0);
  ASSERT_TRUE(h_empty_1 == h_empty_2);
}

static void test_basic_put_get(void) {
  HashTable *ht = ht_create();
  ASSERT_TRUE(ht != NULL);

  int v1 = 11;
  int v2 = 22;
  ASSERT_TRUE(ht_put_cstr(ht, "alpha", &v1) == OK);
  ASSERT_TRUE(ht_put_cstr(ht, "beta", &v2) == OK);
  ASSERT_TRUE(ht_len(ht) == 2);

  ASSERT_TRUE(ht_get_cstr(ht, "alpha") == &v1);
  ASSERT_TRUE(ht_get_cstr(ht, "beta") == &v2);
  ASSERT_TRUE(ht_get_cstr(ht, "missing") == NULL);

  ht_destroy(ht);
}

static void test_byte_keys_binary_and_empty(void) {
  HashTable *ht = ht_create();
  ASSERT_TRUE(ht != NULL);

  int v_empty = 7;
  int v_bin = 9;
  const char *empty_key = "";
  const uint8_t bin_key1[] = {0x61, 0x00, 0x62};
  const uint8_t bin_key2[] = {0x61, 0x00, 0x62};
  const uint8_t bin_miss[] = {0x61, 0x00, 0x63};

  ASSERT_TRUE(ht_put(ht, empty_key, 0, &v_empty) == OK);
  ASSERT_TRUE(ht_get(ht, empty_key, 0) == &v_empty);

  ASSERT_TRUE(ht_put(ht, (const char *)bin_key1, 3, &v_bin) == OK);
  ASSERT_TRUE(ht_get(ht, (const char *)bin_key2, 3) == &v_bin);
  ASSERT_TRUE(ht_get(ht, (const char *)bin_miss, 3) == NULL);

  ht_destroy(ht);
}

static void test_update_existing_key(void) {
  HashTable *ht = ht_create();
  ASSERT_TRUE(ht != NULL);

  int v1 = 10;
  int v2 = 99;
  ASSERT_TRUE(ht_put_cstr(ht, "same", &v1) == OK);
  ASSERT_TRUE(ht_len(ht) == 1);
  ASSERT_TRUE(ht_put_cstr(ht, "same", &v2) == OK);
  ASSERT_TRUE(ht_len(ht) == 1);
  ASSERT_TRUE(ht_get_cstr(ht, "same") == &v2);

  ht_destroy(ht);
}

static void test_rehash_keeps_entries(void) {
  HashTable *ht = ht_create_with_capacity(8);
  ASSERT_TRUE(ht != NULL);

  char keys[128][16];
  int vals[128];
  for (int i = 0; i < 128; i++) {
    snprintf(keys[i], sizeof(keys[i]), "k%d", i);
    vals[i] = i * 3;
    ASSERT_TRUE(ht_put_cstr(ht, keys[i], &vals[i]) == OK);
  }
  ASSERT_TRUE(ht_len(ht) == 128);

  for (int i = 0; i < 128; i++) {
    ASSERT_TRUE(ht_get_cstr(ht, keys[i]) == &vals[i]);
  }

  ht_destroy(ht);
}

static void test_linear_probe_collision(void) {
  HashTable *ht = ht_create_with_capacity(8);
  ASSERT_TRUE(ht != NULL);

  // Find two short different keys that collide on the first probe slot.
  char k1[16] = {0};
  char k2[16] = {0};
  size_t cap = 8;
  int found = NO;
  for (int i = 0; i < 1000 && found == NO; i++) {
    snprintf(k1, sizeof(k1), "c%d", i);
    uint64_t h1 = ht_hash_bytes(k1, strlen(k1));
    size_t idx1 = (size_t)h1 & (cap - 1);
    for (int j = i + 1; j < 1000; j++) {
      snprintf(k2, sizeof(k2), "c%d", j);
      uint64_t h2 = ht_hash_bytes(k2, strlen(k2));
      size_t idx2 = (size_t)h2 & (cap - 1);
      if (idx1 == idx2) {
        found = YES;
        break;
      }
    }
  }
  ASSERT_TRUE(found == YES);
  ASSERT_TRUE(strcmp(k1, k2) != 0);

  int v1 = 1;
  int v2 = 2;
  ASSERT_TRUE(ht_put_cstr(ht, k1, &v1) == OK);
  ASSERT_TRUE(ht_put_cstr(ht, k2, &v2) == OK);
  ASSERT_TRUE(ht_get_cstr(ht, k1) == &v1);
  ASSERT_TRUE(ht_get_cstr(ht, k2) == &v2);

  ht_destroy(ht);
}

static void test_clean_and_reinit(void) {
  HashTable *ht = ht_create();
  ASSERT_TRUE(ht != NULL);

  int v1 = 7;
  ASSERT_TRUE(ht_put_cstr(ht, "x", &v1) == OK);
  ASSERT_TRUE(ht_len(ht) == 1);

  ht_clean(ht);
  ASSERT_TRUE(ht_len(ht) == 0);
  ASSERT_TRUE(ht_put_cstr(ht, "x", &v1) == ERR);

  ASSERT_TRUE(ht_init_with_capacity(ht, 4) == OK);
  ASSERT_TRUE(ht_put_cstr(ht, "x", &v1) == OK);
  ASSERT_TRUE(ht_get_cstr(ht, "x") == &v1);
  ht_destroy(ht);
}

static void test_custom_basic_lookup(void) {
  HashTable *ht = ht_create_custom(test_custom_hash, test_custom_eq, NULL);
  ASSERT_TRUE(ht != NULL);

  int v1 = 101;
  TestCustomKey a1 = {
      .col_ref = "public.users.ssn", .value = "alice", .value_len = 5};
  ASSERT_TRUE(ht_put_custom(ht, &a1, &v1) == OK);
  ASSERT_TRUE(ht_len(ht) == 1);

  // Same logical key with different value pointer must still match.
  char val_copy[] = {'a', 'l', 'i', 'c', 'e'};
  TestCustomKey a1_same = {
      .col_ref = "public.users.ssn", .value = val_copy, .value_len = 5};
  ASSERT_TRUE(ht_get_custom(ht, &a1_same) == &v1);

  TestCustomKey miss = {
      .col_ref = "public.users.email", .value = "alice", .value_len = 5};
  ASSERT_TRUE(ht_get_custom(ht, &miss) == NULL);

  ht_destroy(ht);
}

static void test_custom_update_existing(void) {
  HashTable *ht =
      ht_create_custom_with_capacity(4, test_custom_hash, test_custom_eq, NULL);
  ASSERT_TRUE(ht != NULL);

  int v1 = 1;
  int v2 = 2;
  TestCustomKey key_a = {.col_ref = "c", .value = "v", .value_len = 1};
  ASSERT_TRUE(ht_put_custom(ht, &key_a, &v1) == OK);
  ASSERT_TRUE(ht_len(ht) == 1);

  char v_copy[] = {'v'};
  TestCustomKey key_same = {.col_ref = "c", .value = v_copy, .value_len = 1};
  ASSERT_TRUE(ht_put_custom(ht, &key_same, &v2) == OK);
  ASSERT_TRUE(ht_len(ht) == 1);
  ASSERT_TRUE(ht_get_custom(ht, &key_a) == &v2);

  ht_destroy(ht);
}

static void test_custom_zero_hash_callback(void) {
  HashTable *ht = ht_create_custom(test_custom_hash_zero, test_custom_eq, NULL);
  ASSERT_TRUE(ht != NULL);

  int v1 = 10;
  int v2 = 20;
  TestCustomKey k1 = {
      .col_ref = "public.users.ssn", .value = "alice", .value_len = 5};
  TestCustomKey k2 = {
      .col_ref = "public.users.email", .value = "alice", .value_len = 5};
  TestCustomKey k1_same = {
      .col_ref = "public.users.ssn", .value = "alice", .value_len = 5};

  ASSERT_TRUE(ht_put_custom(ht, &k1, &v1) == ERR);
  ASSERT_TRUE(ht_put_custom(ht, &k2, &v2) == ERR);
  ASSERT_TRUE(ht_len(ht) == 0);
  ASSERT_TRUE(ht_get_custom(ht, &k1_same) == NULL);
  ASSERT_TRUE(ht_get_custom(ht, &k2) == NULL);

  ht_destroy(ht);
}

static void test_invalid_inputs(void) {
  HashTable *tmp = ht_create_with_capacity(0);
  ASSERT_TRUE(tmp != NULL);
  ht_destroy(tmp);

  ASSERT_TRUE(ht_create_custom(NULL, test_custom_eq, NULL) == NULL);
  ASSERT_TRUE(ht_create_custom(test_custom_hash, NULL, NULL) == NULL);

  HashTable *ht = ht_create();
  ASSERT_TRUE(ht != NULL);
  ASSERT_TRUE(ht_put_cstr(ht, NULL, NULL) == ERR);
  ASSERT_TRUE(ht_put_cstr(ht, "nonnull", NULL) == ERR);
  ASSERT_TRUE(ht_get_cstr(ht, NULL) == NULL);
  ASSERT_TRUE(ht_put(ht, NULL, 0, NULL) == ERR);
  ASSERT_TRUE(ht_get(ht, NULL, 0) == NULL);
  ht_destroy(ht);

  HashTable *hc = ht_create_custom(test_custom_hash, test_custom_eq, NULL);
  ASSERT_TRUE(hc != NULL);
  int v = 3;
  TestCustomKey k = {.col_ref = "x", .value = "y", .value_len = 1};
  ASSERT_TRUE(ht_put_custom(hc, NULL, &v) == ERR);
  ASSERT_TRUE(ht_put_custom(hc, &k, NULL) == ERR);
  ASSERT_TRUE(ht_get_custom(hc, NULL) == NULL);
  ASSERT_TRUE(ht_put(hc, "abc", 3, &v) == ERR);
  ASSERT_TRUE(ht_get(hc, "abc", 3) == NULL);
  ht_destroy(hc);

  HashTable *hb = ht_create();
  ASSERT_TRUE(hb != NULL);
  ASSERT_TRUE(ht_put_custom(hb, &k, &v) == ERR);
  ASSERT_TRUE(ht_get_custom(hb, &k) == NULL);
  ht_destroy(hb);
}

int main(void) {
  test_hash_bytes_edge_cases();
  test_hash_bytes_with_seed_edge_cases();
  test_basic_put_get();
  test_byte_keys_binary_and_empty();
  test_update_existing_key();
  test_rehash_keeps_entries();
  test_linear_probe_collision();
  test_clean_and_reinit();
  test_custom_basic_lookup();
  test_custom_update_existing();
  test_custom_zero_hash_callback();
  test_invalid_inputs();

  fprintf(stderr, "OK: test_hash_table\n");
  return 0;
}
