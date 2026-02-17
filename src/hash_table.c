#include "hash_table.h"

#include "rapidhash.h"
#include "utils.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct HashSlot {
  const char *key;   // borrowed
  uint32_t key_len;  // key length in bytes
  uint64_t hash;     // hash of key bytes
  const void *value; // borrowed payload pointer
  uint8_t used;      // 1 when occupied, 0 when empty
} HashSlot;

struct HashTable {
  HashSlot *slots; // owned
  size_t cap;      // number of slots, power of two
  size_t len;      // occupied slots
  size_t grow_at;  // resize threshold
};

#define HT_MIN_CAP 16u
#define HT_LOAD_NUM 7u
#define HT_LOAD_DEN 10u

/* Hashes key bytes using rapidhash.
 * It borrows 'key' and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns a deterministic non-zero hash for valid input.
 */
static uint64_t ht_hash_bytes(const char *key, uint32_t key_len) {
  uint64_t h = rapidhash((const void *)key, (size_t)key_len);
  // Keep 0 reserved as an impossible hash for easier defensive checks.
  return (h == 0) ? 1 : h;
}

/* Computes next power-of-two capacity at least min_slots and >= HT_MIN_CAP.
 * It borrows inputs and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns 0 on overflow/invalid, otherwise power-of-two cap.
 */
static size_t ht_next_cap(size_t min_slots) {
  size_t cap = HT_MIN_CAP;
  if (min_slots > cap) {
    while (cap < min_slots) {
      if (cap > SIZE_MAX / 2)
        return 0;
      cap *= 2;
    }
  }
  return cap;
}

/* Calculates load-factor growth threshold for a given capacity.
 * It borrows inputs and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns 0 only on invalid cap.
 */
static size_t ht_calc_grow_at(size_t cap) {
  if (cap == 0)
    return 0;
  size_t grow = (cap * HT_LOAD_NUM) / HT_LOAD_DEN;
  if (grow == 0)
    grow = 1;
  if (grow >= cap)
    grow = cap - 1;
  return grow;
}

/* Resets table metadata to the empty/uninitialized state.
 * It borrows 'ht' and does not allocate memory.
 * Side effects: zeroes metadata pointers/counters.
 * Error semantics: none (safe on NULL).
 */
static void ht_zero(HashTable *ht) {
  if (!ht)
    return;
  ht->slots = NULL;
  ht->cap = 0;
  ht->len = 0;
  ht->grow_at = 0;
}

/* Compares a slot key with (key,key_len) using hash+length+bytes.
 * It borrows all pointers and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns YES on match, NO on mismatch.
 */
static int ht_slot_key_eq(const HashSlot *slot, uint64_t hash, const char *key,
                          uint32_t key_len) {
  if (!slot || !key || !slot->used)
    return NO;
  if (slot->hash != hash || slot->key_len != key_len)
    return NO;
  if (key_len == 0)
    return YES;
  return (memcmp(slot->key, key, key_len) == 0) ? YES : NO;
}

/* Inserts one slot into a table that is guaranteed to have free capacity.
 * It borrows table memory and does not allocate.
 * Side effects: mutates one slot and increments len.
 * Returns OK on success, ERR on invalid input.
 */
static int ht_insert_no_grow(HashTable *ht, const char *key, uint32_t key_len,
                             uint64_t hash, const void *value) {
  if (!ht || !ht->slots || ht->cap == 0 || !key || hash == 0)
    return ERR;

  size_t mask = ht->cap - 1;
  size_t idx = (size_t)hash & mask;
  for (size_t steps = 0; steps < ht->cap; steps++) {
    HashSlot *slot = &ht->slots[idx];
    if (!slot->used) {
      slot->used = 1;
      slot->key = key;
      slot->key_len = key_len;
      slot->hash = hash;
      slot->value = value;
      ht->len++;
      return OK;
    }
    idx = (idx + 1) & mask;
  }

  return ERR;
}

/* Rehashes all existing entries into a new capacity.
 * It borrows 'ht' and replaces its owned slot buffer.
 * Side effects: allocates new slot storage and frees old slot storage.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
static int ht_rehash(HashTable *ht, size_t new_cap) {
  if (!ht || !ht->slots || ht->cap == 0)
    return ERR;
  if (new_cap < HT_MIN_CAP || (new_cap & (new_cap - 1)) != 0)
    return ERR;

  HashSlot *old_slots = ht->slots;
  size_t old_cap = ht->cap;
  size_t old_len = ht->len;

  HashSlot *new_slots = (HashSlot *)xcalloc(new_cap, sizeof(*new_slots));

  ht->slots = new_slots;
  ht->cap = new_cap;
  ht->len = 0;
  ht->grow_at = ht_calc_grow_at(new_cap);

  for (size_t i = 0; i < old_cap; i++) {
    HashSlot *slot = &old_slots[i];
    if (!slot->used)
      continue;
    if (ht_insert_no_grow(ht, slot->key, slot->key_len, slot->hash,
                          slot->value) != OK) {
      // This should not happen with a correctly-sized table.
      free(old_slots);
      return ERR;
    }
  }
  free(old_slots);

  if (ht->len != old_len)
    return ERR;
  return OK;
}

/* Grows table when load factor reached.
 * It borrows 'ht' and may rehash storage.
 * Side effects: may allocate and move slots.
 * Returns OK on success, ERR on invalid input or allocation/overflow failure.
 */
static int ht_ensure_room(HashTable *ht) {
  if (!ht || !ht->slots || ht->cap == 0)
    return ERR;
  if (ht->len < ht->grow_at)
    return OK;
  if (ht->cap > SIZE_MAX / 2)
    return ERR;
  return ht_rehash(ht, ht->cap * 2);
}

int ht_init_with_capacity(HashTable *ht, size_t min_slots) {
  if (!ht)
    return ERR;

  if (ht->slots) {
    free(ht->slots);
  }
  ht_zero(ht);

  size_t cap = ht_next_cap(min_slots);
  if (cap == 0)
    return ERR;

  HashSlot *slots = (HashSlot *)calloc(cap, sizeof(*slots));
  if (!slots)
    return ERR;

  ht->slots = slots;
  ht->cap = cap;
  ht->len = 0;
  ht->grow_at = ht_calc_grow_at(cap);
  return OK;
}

int ht_init(HashTable *ht) { return ht_init_with_capacity(ht, HT_MIN_CAP); }

HashTable *ht_create_with_capacity(size_t min_slots) {
  HashTable *ht = (HashTable *)malloc(sizeof(*ht));
  if (!ht)
    return NULL;
  ht_zero(ht);
  if (ht_init_with_capacity(ht, min_slots) != OK) {
    free(ht);
    return NULL;
  }
  return ht;
}

HashTable *ht_create(void) { return ht_create_with_capacity(HT_MIN_CAP); }

void ht_clean(HashTable *ht) {
  if (!ht)
    return;
  free(ht->slots);
  ht_zero(ht);
}

void ht_destroy(HashTable *ht) {
  if (!ht)
    return;
  ht_clean(ht);
  free(ht);
}

size_t ht_len(const HashTable *ht) {
  if (!ht)
    return 0;
  return ht->len;
}

int ht_put(HashTable *ht, const char *key, uint32_t key_len,
           const void *value) {
  if (!ht || !ht->slots || ht->cap == 0 || !key || !value)
    return ERR;

  if (ht_ensure_room(ht) != OK)
    return ERR;

  uint64_t hash = ht_hash_bytes(key, key_len);
  size_t mask = ht->cap - 1;
  size_t idx = (size_t)hash & mask;
  for (size_t steps = 0; steps < ht->cap; steps++) {
    HashSlot *slot = &ht->slots[idx];
    if (!slot->used) {
      slot->used = 1;
      slot->key = key;
      slot->key_len = key_len;
      slot->hash = hash;
      slot->value = value;
      ht->len++;
      return OK;
    }
    if (ht_slot_key_eq(slot, hash, key, key_len) == YES) {
      slot->value = value;
      return OK;
    }
    idx = (idx + 1) & mask;
  }

  return ERR;
}

const void *ht_get(const HashTable *ht, const char *key, uint32_t key_len) {
  if (!ht || !ht->slots || ht->cap == 0 || !key)
    return NULL;

  uint64_t hash = ht_hash_bytes(key, key_len);
  size_t mask = ht->cap - 1;
  size_t idx = (size_t)hash & mask;
  for (size_t steps = 0; steps < ht->cap; steps++) {
    const HashSlot *slot = &ht->slots[idx];
    if (!slot->used)
      return NULL;
    if (ht_slot_key_eq(slot, hash, key, key_len) == YES)
      return slot->value;
    idx = (idx + 1) & mask;
  }

  return NULL;
}
