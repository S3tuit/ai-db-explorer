#include "sensitive_tok.h"

#include <assert.h>
#include <limits.h>
#include <stddef.h>
#include <string.h>

#include "hash_table.h"
#include "utils.h"

/* Finds one existing store by exact connection name match. This is O(n) since
 * we expect few entries (~10).
 * It borrows 'stores' and 'connection_name' and does not allocate.
 * Side effects: none.
 * Error semantics: returns mutable store pointer when found, NULL otherwise.
 */
static DbTokenStore *stok_store_find(PackedArray *stores,
                                     const char *connection_name) {
  assert(stores != NULL);
  assert(connection_name != NULL);

  size_t n = parr_len(stores);
  for (size_t i = 0; i < n; i++) {
    DbTokenStore *store = (DbTokenStore *)parr_at(stores, i);
    if (!store || !store->connection_name)
      continue;
    if (strcmp(store->connection_name, connection_name) == 0)
      return store;
  }
  return NULL;
}

/* Cleans one packed-array slot containing a DbTokenStore.
 * It borrows slot memory provided by PackedArray.
 * Side effects: releases store-owned arrays/hash tables/string pools.
 * Error semantics: none (cleanup callback, safe on partially initialized
 * entries).
 */
static void stok_store_cleanup(void *obj, void *ctx) {
  (void)ctx;
  DbTokenStore *store = (DbTokenStore *)obj;
  if (!store)
    return;

  parr_destroy(store->tokens);
  store->tokens = NULL;

  ht_destroy(store->det_index);
  store->det_index = NULL;

  spool_clean(&store->col_ref_pool);
  store->connection_name = NULL;
  store->mode = 0;
}

/* Initializes one DbTokenStore from a connection profile.
 * It borrows 'profile' and writes owned internals into caller-owned '*store'.
 * Side effects: allocates token array, string pool, and optional deterministic
 * hash index.
 * Error semantics: returns OK on success, ERR on invalid input/allocation
 * failure.
 */
static int stok_store_init(DbTokenStore *store, const ConnProfile *profile) {
  assert(store != NULL);
  assert(profile != NULL);
  if (!profile->connection_name)
    return ERR;

  memset(store, 0, sizeof(*store));
  store->connection_name = profile->connection_name;
  store->mode = profile->safe_policy.column_strategy;

  store->tokens = parr_create(sizeof(SensitiveTok));
  if (!store->tokens) {
    stok_store_cleanup(store, NULL);
    return ERR;
  }

  if (spool_init(&store->col_ref_pool) != OK) {
    stok_store_cleanup(store, NULL);
    return ERR;
  }

  if (store->mode == SAFETY_COLSTRAT_DETERMINISTIC) {
    store->det_index = ht_create();
    if (!store->det_index) {
      stok_store_cleanup(store, NULL);
      return ERR;
    }
  }

  return OK;
}

PackedArray *stok_store_array_create(void) {
  PackedArray *stores = parr_create(sizeof(DbTokenStore));
  if (!stores)
    return NULL;
  parr_set_cleanup(stores, stok_store_cleanup, NULL);
  return stores;
}

int stok_store_get_or_init(PackedArray *stores, const ConnProfile *profile,
                           DbTokenStore **out_store) {
  if (out_store)
    *out_store = NULL;
  if (!stores || !profile || !profile->connection_name || !out_store)
    return ERR;

  DbTokenStore *found = stok_store_find(stores, profile->connection_name);
  if (found) {
    *out_store = found;
    return OK;
  }

  DbTokenStore *slot = NULL;
  size_t idx = parr_emplace(stores, (void **)&slot);
  if (idx == SIZE_MAX || !slot)
    return ERR;

  if (stok_store_init(slot, profile) != OK) {
    parr_drop_swap(stores, idx);
    return ERR;
  }

  *out_store = slot;
  return OK;
}

/* Parses one unsigned base-10 integer from [start, end) into '*out_u32'.
 * It borrows input pointers and does not allocate.
 * Side effects: writes to '*out_u32' on success.
 * Error semantics: returns OK on valid uint32 text, ERR otherwise.
 */
static int parse_u32_span(const char *start, const char *end,
                          uint32_t *out_u32) {
  assert(start != NULL);
  assert(end != NULL);
  assert(out_u32 != NULL);
  if (start >= end)
    return ERR;

  uint64_t acc = 0;
  for (const char *p = start; p < end; p++) {
    if (*p < '0' || *p > '9')
      return ERR;
    acc = (acc * 10u) + (uint64_t)(*p - '0');
    if (acc > UINT32_MAX)
      return ERR;
  }
  *out_u32 = (uint32_t)acc;
  return OK;
}

/* Finds the last underscore in the inclusive character range [start, end].
 * It borrows all pointers and does not allocate.
 * Side effects: none.
 * Error semantics: returns pointer to '_' when found, NULL otherwise.
 */
static char *find_last_underscore(char *start, char *end) {
  assert(start != NULL);
  assert(end != NULL);
  if (start > end)
    return NULL;

  for (char *p = end;; p--) {
    if (*p == '_')
      return p;
    if (p == start)
      break;
  }
  return NULL;
}

int stok_parse_view_inplace(char *token, ParsedTokView *out) {
  if (!token || !out)
    return ERR;
  size_t prefix_len = strlen(SENSITIVE_TOK_PREFIX);
  if (strncmp(token, SENSITIVE_TOK_PREFIX, prefix_len) != 0)
    return ERR;

  size_t tok_len = strlen(token);
  if (tok_len <= prefix_len + 3u)
    return ERR;

  char *conn_start = token + prefix_len;
  char *str_end = token + tok_len - 1;

  // Parse from right to left to allow underscores inside connection_name.
  char *last_us = find_last_underscore(conn_start, str_end);
  if (!last_us || last_us == str_end)
    return ERR;

  char *prev_end = last_us - 1;
  if (prev_end < conn_start)
    return ERR;
  char *mid_us = find_last_underscore(conn_start, prev_end);
  if (!mid_us)
    return ERR;
  if (mid_us == conn_start)
    return ERR;

  uint32_t generation = 0;
  uint32_t index = 0;
  if (parse_u32_span(mid_us + 1, last_us, &generation) != OK)
    return ERR;
  if (parse_u32_span(last_us + 1, str_end + 1, &index) != OK)
    return ERR;

  *mid_us = '\0';
  *last_us = '\0';

  out->connection_name = conn_start;
  out->generation = generation;
  out->index = index;
  return OK;
}
