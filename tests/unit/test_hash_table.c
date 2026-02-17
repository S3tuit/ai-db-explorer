#include <stdio.h>
#include <string.h>

#include "hash_table.h"
#include "rapidhash.h"
#include "test.h"

/* Mirrors current hash_table.c hash so we can deterministically find probing
 * collisions for a given power-of-two capacity.
 */
static uint64_t test_hash_bytes(const char *key, uint32_t key_len) {
  uint64_t h = rapidhash((const void *)key, (size_t)key_len);
  return (h == 0) ? 1 : h;
}

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
    uint64_t h1 = test_hash_bytes(k1, (uint32_t)strlen(k1));
    size_t idx1 = (size_t)h1 & (cap - 1);
    for (int j = i + 1; j < 1000; j++) {
      snprintf(k2, sizeof(k2), "c%d", j);
      uint64_t h2 = test_hash_bytes(k2, (uint32_t)strlen(k2));
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

static void test_invalid_inputs(void) {
  HashTable *tmp = ht_create_with_capacity(0);
  ASSERT_TRUE(tmp != NULL);
  ht_destroy(tmp);

  HashTable *ht = ht_create();
  ASSERT_TRUE(ht != NULL);
  ASSERT_TRUE(ht_put_cstr(ht, NULL, NULL) == ERR);
  ASSERT_TRUE(ht_put_cstr(ht, "nonnull", NULL) == ERR);
  ASSERT_TRUE(ht_get_cstr(ht, NULL) == NULL);
  ASSERT_TRUE(ht_put(ht, NULL, 0, NULL) == ERR);
  ASSERT_TRUE(ht_get(ht, NULL, 0) == NULL);
  ht_destroy(ht);
}

int main(void) {
  test_basic_put_get();
  test_update_existing_key();
  test_rehash_keeps_entries();
  test_linear_probe_collision();
  test_clean_and_reinit();
  test_invalid_inputs();

  fprintf(stderr, "OK: test_hash_table\n");
  return 0;
}
