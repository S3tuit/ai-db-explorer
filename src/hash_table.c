#include "hash_table.h"

#include "rapidhash.h"
#include "utils.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct HashSlot {
  const void *key;   // borrowed
  uint32_t key_len;  // used by byte-key mode
  uint64_t hash;     // hash of key
  const void *value; // borrowed payload pointer
  uint8_t used;      // 1 when occupied, 0 when empty
} HashSlot;

typedef enum HtMode {
  HT_MODE_NONE = 0,
  HT_MODE_BYTES = 1,
  HT_MODE_CUSTOM = 2,
} HtMode;

struct HashTable {
  HashSlot *slots; // owned
  size_t cap;      // number of slots, power of two
  size_t len;      // occupied slots
  size_t grow_at;  // resize threshold
  HtMode mode;     // key semantics for put/get
  HtHashFn hash_fn;
  HtEqFn eq_fn;
  void *ops_ctx;
};

#define HT_MIN_CAP 16u
#define HT_LOAD_NUM 7u
#define HT_LOAD_DEN 10u

inline uint64_t ht_hash_bytes(const void *data, size_t len) {
  return rapidhash(data, len);
}

inline uint64_t ht_hash_bytes_withSeed(const void *key, size_t len,
                                       uint64_t seed) {
  return rapidhash_withSeed(key, len, seed);
}

/* Hashes a custom key using caller-provided callback.
 * It borrows 'ht' and 'key' and does not allocate memory.
 * Side effects: invokes caller callback.
 * Error semantics: returns callback hash as-is; 0 means invalid key.
 */
static inline uint64_t ht_hash_custom(const HashTable *ht, const void *key) {
  assert(ht);
  assert(key);
  assert(ht->mode == HT_MODE_CUSTOM);
  assert(ht->hash_fn);
  return ht->hash_fn(key, ht->ops_ctx);
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
 * Side effects: zeroes metadata pointers/counters and key-mode callbacks.
 * Error semantics: none (safe on NULL).
 */
static void ht_zero(HashTable *ht) {
  if (!ht)
    return;
  ht->slots = NULL;
  ht->cap = 0;
  ht->len = 0;
  ht->grow_at = 0;
  ht->mode = HT_MODE_NONE;
  ht->hash_fn = NULL;
  ht->eq_fn = NULL;
  ht->ops_ctx = NULL;
}

/* Compares a slot key with (key,key_len) using hash+length+bytes.
 * It borrows all pointers and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns YES on match, NO on mismatch.
 */
static int ht_slot_key_eq_bytes(const HashSlot *slot, uint64_t hash,
                                const char *key, uint32_t key_len) {
  if (!slot || !key || !slot->used)
    return NO;
  if (slot->hash != hash || slot->key_len != key_len)
    return NO;
  if (key_len == 0)
    return YES;
  return (memcmp(slot->key, key, key_len) == 0) ? YES : NO;
}

/* Compares a slot key with a custom lookup key using callback semantics.
 * It borrows 'ht', 'slot', and 'key' and does not allocate memory.
 * Side effects: invokes caller eq callback.
 * Error semantics: returns YES on match, NO on mismatch.
 */
static int ht_slot_key_eq_custom(const HashTable *ht, const HashSlot *slot,
                                 uint64_t hash, const void *key) {
  assert(ht);
  assert(ht->eq_fn);
  assert(slot->key);
  assert(slot);
  assert(slot->used);
  assert(key);
  if (slot->hash != hash)
    return NO;

  return ht->eq_fn(slot->key, key, ht->ops_ctx);
}

/* Inserts one slot into a table that is guaranteed to have free capacity.
 * It borrows table memory and does not allocate.
 * Side effects: mutates one slot and increments len.
 * Returns OK on success, ERR on invalid input.
 */
static int ht_insert_no_grow(HashTable *ht, const void *key, uint32_t key_len,
                             uint64_t hash, const void *value) {
  assert(ht);
  assert(ht->slots);
  assert(value);
  assert(key);
  if (ht->cap == 0)
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
  assert(ht);
  assert(ht->slots);
  if (ht->cap == 0)
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
  assert(ht->grow_at != 0);

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
  assert(ht);
  assert(ht->slots);
  assert(ht->cap != 0);
  if (ht->len < ht->grow_at)
    return OK;
  if (ht->cap > SIZE_MAX / 2)
    return ERR;
  return ht_rehash(ht, ht->cap * 2);
}

/* Initializes hash table internals with requested key mode/callbacks.
 * It borrows 'ht' and mutates it in-place.
 * Side effects: allocates internal slot storage and resets callbacks/mode.
 * Error semantics: returns OK on success, ERR on invalid input/allocation
 * failure.
 */
static int ht_init_common(HashTable *ht, size_t min_slots, HtMode mode,
                          HtHashFn hash_fn, HtEqFn eq_fn, void *ctx) {
  if (!ht)
    return ERR;
  if (mode != HT_MODE_BYTES && mode != HT_MODE_CUSTOM)
    return ERR;
  if (mode == HT_MODE_CUSTOM && (!hash_fn || !eq_fn))
    return ERR;

  if (ht->slots)
    free(ht->slots);
  ht_zero(ht);

  size_t cap = ht_next_cap(min_slots);
  if (cap == 0)
    return ERR;

  HashSlot *slots = (HashSlot *)xcalloc(cap, sizeof(*slots));
  if (!slots)
    return ERR;

  ht->slots = slots;
  ht->cap = cap;
  ht->len = 0;
  ht->grow_at = ht_calc_grow_at(cap);
  ht->mode = mode;
  if (mode == HT_MODE_CUSTOM) {
    ht->hash_fn = hash_fn;
    ht->eq_fn = eq_fn;
    ht->ops_ctx = ctx;
  }
  return OK;
}

int ht_init_with_capacity(HashTable *ht, size_t min_slots) {
  return ht_init_common(ht, min_slots, HT_MODE_BYTES, NULL, NULL, NULL);
}

int ht_init(HashTable *ht) { return ht_init_with_capacity(ht, HT_MIN_CAP); }

int ht_init_custom_with_capacity(HashTable *ht, size_t min_slots,
                                 HtHashFn hash_fn, HtEqFn eq_fn, void *ctx) {
  return ht_init_common(ht, min_slots, HT_MODE_CUSTOM, hash_fn, eq_fn, ctx);
}

int ht_init_custom(HashTable *ht, HtHashFn hash_fn, HtEqFn eq_fn, void *ctx) {
  return ht_init_custom_with_capacity(ht, HT_MIN_CAP, hash_fn, eq_fn, ctx);
}

/* Allocates a hash-table object and initializes it for the selected key mode.
 * Ownership: caller owns returned table and must call ht_destroy().
 * Side effects: heap allocation.
 * Error semantics: returns initialized table on success, NULL on failure.
 */
static HashTable *ht_create_common(size_t min_slots, HtMode mode,
                                   HtHashFn hash_fn, HtEqFn eq_fn, void *ctx) {
  HashTable *ht = (HashTable *)xmalloc(sizeof(*ht));
  ht_zero(ht);

  int rc =
      (mode == HT_MODE_CUSTOM)
          ? ht_init_custom_with_capacity(ht, min_slots, hash_fn, eq_fn, ctx)
          : ht_init_with_capacity(ht, min_slots);
  if (rc != OK) {
    free(ht);
    return NULL;
  }
  return ht;
}

HashTable *ht_create_with_capacity(size_t min_slots) {
  return ht_create_common(min_slots, HT_MODE_BYTES, NULL, NULL, NULL);
}

HashTable *ht_create(void) { return ht_create_with_capacity(HT_MIN_CAP); }

HashTable *ht_create_custom_with_capacity(size_t min_slots, HtHashFn hash_fn,
                                          HtEqFn eq_fn, void *ctx) {
  return ht_create_common(min_slots, HT_MODE_CUSTOM, hash_fn, eq_fn, ctx);
}

HashTable *ht_create_custom(HtHashFn hash_fn, HtEqFn eq_fn, void *ctx) {
  return ht_create_custom_with_capacity(HT_MIN_CAP, hash_fn, eq_fn, ctx);
}

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
  if (!ht || ht->mode != HT_MODE_BYTES || !key || !value)
    return ERR;
  assert(ht->slots);
  assert(ht->cap != 0);
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
    if (ht_slot_key_eq_bytes(slot, hash, key, key_len) == YES) {
      slot->value = value;
      return OK;
    }
    idx = (idx + 1) & mask;
  }

  return ERR;
}

const void *ht_get(const HashTable *ht, const char *key, uint32_t key_len) {
  if (!ht || ht->mode != HT_MODE_BYTES || !key)
    return NULL;
  assert(ht->slots);
  assert(ht->cap != 0);

  uint64_t hash = ht_hash_bytes(key, key_len);
  size_t mask = ht->cap - 1;
  size_t idx = (size_t)hash & mask;
  for (size_t steps = 0; steps < ht->cap; steps++) {
    const HashSlot *slot = &ht->slots[idx];
    if (!slot->used)
      return NULL;
    if (ht_slot_key_eq_bytes(slot, hash, key, key_len) == YES)
      return slot->value;
    idx = (idx + 1) & mask;
  }

  return NULL;
}

int ht_put_custom(HashTable *ht, const void *key, const void *value) {
  if (!ht || ht->mode != HT_MODE_CUSTOM || !key || !value)
    return ERR;
  assert(ht->slots);
  assert(ht->cap != 0);
  if (ht_ensure_room(ht) != OK)
    return ERR;

  uint64_t hash = ht_hash_custom(ht, key);
  if (hash == 0)
    return ERR;

  size_t mask = ht->cap - 1;
  size_t idx = (size_t)hash & mask;
  for (size_t steps = 0; steps < ht->cap; steps++) {
    HashSlot *slot = &ht->slots[idx];
    if (!slot->used) {
      slot->used = 1;
      slot->key = key;
      slot->key_len = 0;
      slot->hash = hash;
      slot->value = value;
      ht->len++;
      return OK;
    }
    if (ht_slot_key_eq_custom(ht, slot, hash, key) == YES) {
      slot->value = value;
      return OK;
    }
    idx = (idx + 1) & mask;
  }

  return ERR;
}

const void *ht_get_custom(const HashTable *ht, const void *key) {
  if (!ht || ht->mode != HT_MODE_CUSTOM || !key)
    return NULL;
  assert(ht->slots);
  assert(ht->cap != 0);

  uint64_t hash = ht_hash_custom(ht, key);
  if (hash == 0)
    return NULL;

  size_t mask = ht->cap - 1;
  size_t idx = (size_t)hash & mask;
  for (size_t steps = 0; steps < ht->cap; steps++) {
    const HashSlot *slot = &ht->slots[idx];
    if (!slot->used)
      return NULL;
    if (ht_slot_key_eq_custom(ht, slot, hash, key) == YES)
      return slot->value;
    idx = (idx + 1) & mask;
  }

  return NULL;
}
