#include "sensitive_tok.h"

#include <assert.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash_table.h"
#include "packed_array.h"
#include "pl_arena.h"
#include "spool.h"
#include "utils.h"

struct DbTokenStore {
  const char *connection_name;
  uint32_t connection_name_len;
  SafetyColumnStrategy mode;
  PlArena *arena;      // borrowed from BrokerMcpSession
  PackedArray *tokens; // entries are SensitiveTok
  StringPool col_ref_pool;
  HashTable *det_index; // used only for deterministic mode
};

/* Encodes a token index into a non-NULL pointer payload for HashTable values.
 * Why: HashTable uses NULL as "missing key", so index 0 cannot be stored as a
 * raw pointer value; +1 keeps 0 representable while preserving NULL sentinel.
 * It borrows no inputs and does not allocate memory.
 */
static inline const void *stok_index_encode(uint32_t idx) {
  return (const void *)((uintptr_t)idx + (uintptr_t)1u);
}

/* Decodes a HashTable value payload back to token index.
 * This reverses stok_index_encode() by subtracting the +1 offset.
 * It borrows 'p' and writes to caller-owned 'out_idx'.
 * Side effects: writes decoded value to '*out_idx'.
 * Error semantics: returns OK on valid payload, ERR on invalid input/range.
 */
static inline int stok_index_decode(const void *p, uint32_t *out_idx) {
  if (!p || !out_idx)
    return ERR;
  uintptr_t raw = (uintptr_t)p;
  if (raw == 0)
    return ERR;
  raw -= (uintptr_t)1u;
  if (raw > UINT32_MAX)
    return ERR;
  *out_idx = (uint32_t)raw;
  return OK;
}

/* Struct that borrows all the values that identify an unique SesnsitiveTok. */
typedef struct SensitiveTokKey {
  const char *col_ref;
  uint32_t col_ref_len;
  const char *value;
  uint32_t value_len;
} SensitiveTokKey;

/* Hashes one deterministic token key by (col_ref,value bytes).
 * It borrows 'key' and does not allocate memory.
 * Error semantics: returns non-zero hash for valid key, 0 for invalid input.
 */
static uint64_t tok_hash(const void *key, void *ctx) {
  (void)ctx;
  const SensitiveTokKey *k = (const SensitiveTokKey *)key;
  assert(k);
  // column ref cannot be 0/NULL
  if (!k->col_ref || k->col_ref_len == 0)
    return 0;
  if (!k->value && k->value_len != 0)
    return 0;

  uint64_t seed = ht_hash_bytes(k->col_ref, k->col_ref_len);
  return ht_hash_bytes_withSeed(k->value, k->value_len, seed);
}

/* Compares two deterministic token keys by exact byte content.
 * It borrows all inputs and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns YES when equal, NO otherwise.
 */
static int tok_eq(const void *a, const void *b, void *ctx) {
  (void)ctx;
  const SensitiveTokKey *ka = (const SensitiveTokKey *)a;
  const SensitiveTokKey *kb = (const SensitiveTokKey *)b;
  assert(ka);
  assert(kb);

  if (!ka->col_ref || !kb->col_ref)
    return NO;
  if (ka->col_ref_len != kb->col_ref_len || ka->value_len != kb->value_len)
    return NO;
  if (memcmp(ka->col_ref, kb->col_ref, ka->col_ref_len) != 0)
    return NO;
  if (ka->value_len == 0)
    return YES;
  return (memcmp(ka->value, kb->value, ka->value_len) == 0) ? YES : NO;
}

/* Releases internals of one in-place store without freeing the struct itself.
 * It borrows 'store' and does not allocate memory.
 * Side effects: frees owned arrays/index/pool and clears borrowed pointers.
 * Error semantics: none (safe on NULL/partially initialized input).
 */
static void stok_store_clean_inplace(DbTokenStore *store) {
  if (!store)
    return;

  parr_destroy(store->tokens);
  store->tokens = NULL;

  ht_destroy(store->det_index);
  store->det_index = NULL;

  spool_clean(&store->col_ref_pool);

  store->connection_name = NULL;
  store->connection_name_len = 0;
  store->mode = 0;
  store->arena = NULL;
}

DbTokenStore *stok_store_create(const ConnProfile *profile, PlArena *arena) {
  if (!profile || !profile->connection_name || !arena)
    return NULL;

  DbTokenStore *store = xcalloc(1, sizeof(*store));

  size_t conn_len = strlen(profile->connection_name);
  if (conn_len == 0 || conn_len > UINT32_MAX) {
    free(store);
    return NULL;
  }
  store->connection_name = profile->connection_name;
  store->connection_name_len = (uint32_t)conn_len;
  store->mode = profile->safe_policy.column_strategy;
  store->arena = arena;

  if (store->mode != SAFETY_COLSTRAT_RANDOMIZED &&
      store->mode != SAFETY_COLSTRAT_DETERMINISTIC) {
    stok_store_clean_inplace(store);
    free(store);
    return NULL;
  }

  store->tokens = parr_create(sizeof(SensitiveTok));
  if (!store->tokens) {
    stok_store_clean_inplace(store);
    free(store);
    return NULL;
  }

  if (spool_init(&store->col_ref_pool) != OK) {
    stok_store_clean_inplace(store);
    free(store);
    return NULL;
  }

  if (store->mode == SAFETY_COLSTRAT_DETERMINISTIC) {
    store->det_index = ht_create_custom(tok_hash, tok_eq, NULL);
    if (!store->det_index) {
      stok_store_clean_inplace(store);
      free(store);
      return NULL;
    }
  }

  return store;
}

void stok_store_destroy(DbTokenStore *store) {
  if (!store)
    return;
  stok_store_clean_inplace(store);
  free(store);
}

int stok_store_same_connection(const DbTokenStore *a, const DbTokenStore *b) {
  if (!a || !b || !a->connection_name || !b->connection_name)
    return NO;
  if (a->connection_name_len != b->connection_name_len)
    return NO;
  if (a->connection_name_len == 0)
    return NO;
  return (memcmp(a->connection_name, b->connection_name,
                 a->connection_name_len) == 0)
             ? YES
             : NO;
}

int stok_store_matches_conn_name(const DbTokenStore *store,
                                 const char *connection_name) {
  if (!store || !connection_name || !store->connection_name)
    return ERR;
  size_t input_len = strlen(connection_name);
  if (input_len > UINT32_MAX)
    return ERR;
  if (store->connection_name_len != (uint32_t)input_len)
    return NO;
  if (store->connection_name_len == 0)
    return ERR;
  return (memcmp(store->connection_name, connection_name,
                 store->connection_name_len) == 0)
             ? YES
             : NO;
}

size_t stok_store_len(const DbTokenStore *store) {
  if (!store || !store->tokens)
    return 0;
  return parr_len(store->tokens);
}

const SensitiveTok *stok_store_get(const DbTokenStore *store, uint32_t idx) {
  if (!store || !store->tokens)
    return NULL;
  return (const SensitiveTok *)parr_cat(store->tokens, idx);
}

/* Formats token text into caller-owned buffer.
 * It borrows all inputs and does not allocate.
 * Side effects: writes bytes to out_tok.
 * Error semantics: returns token length (without NUL) on success, -1 on bad
 * input, truncation, or formatting failure.
 */
static int stok_format_token(char out_tok[SENSITIVE_TOK_BUFSZ],
                             const char *connection_name, uint32_t generation,
                             uint32_t index) {
  assert(out_tok);
  assert(connection_name);

  int n = snprintf(out_tok, SENSITIVE_TOK_BUFSZ, "%s%s_%u_%u",
                   SENSITIVE_TOK_PREFIX, connection_name, generation, index);
  if (n <= 0)
    return -1;
  if ((size_t)n >= SENSITIVE_TOK_BUFSZ)
    return -1;
  return n;
}

/* Appends one SensitiveTok entry to store->tokens (from borrowed input view),
 * stores 'in->value' inside the 'stores''s borrowed PlArena, and stores the
 * column reference of the token inside 'store''s owned StringPool.
 * Populates 'out' with the exact SensitiveTok added to store->tokens.
 * Error semantics: returns appended index on success, UINT32_MAX on invalid
 * input or allocation failure.
 */
static uint32_t stok_append_entry(DbTokenStore *store, const SensitiveTokIn *in,
                                  const SensitiveTok **out) {
  assert(store != NULL);
  assert(store->arena != NULL);
  assert(in != NULL);
  assert(in->col_ref != NULL);

  const char *intern_col_ref =
      spool_addn(&store->col_ref_pool, in->col_ref, in->col_ref_len);
  if (!intern_col_ref)
    return UINT32_MAX;

  const char *arena_value = NULL;
  if (in->value) {
    arena_value = (const char *)pl_arena_add(store->arena, (void *)in->value,
                                             in->value_len);
    if (!arena_value)
      return UINT32_MAX;
  }

  SensitiveTok *slot = NULL;
  uint32_t idx = parr_emplace(store->tokens, (void **)&slot);
  if (idx == UINT32_MAX || !slot)
    return UINT32_MAX;

  slot->value = arena_value;
  slot->value_len = in->value_len;
  slot->col_ref = intern_col_ref;
  slot->col_ref_len = in->col_ref_len;
  slot->pg_oid = in->pg_oid;
  if (out) {
    *out = slot;
  }
  return idx;
}

int stok_store_create_token(DbTokenStore *store, uint32_t generation,
                            const SensitiveTokIn *in,
                            char out_tok[SENSITIVE_TOK_BUFSZ]) {
  if (!store || !in || !out_tok)
    return -1;
  if (!in->col_ref || in->col_ref_len == 0)
    return -1;
  if (!in->value && in->value_len != 0)
    return -1;
  if (store->connection_name_len == 0 ||
      store->connection_name_len > CONN_NAME_MAX_LEN)
    return -1;

  assert(store->arena && store->tokens);
  assert(store->connection_name);

  if (store->mode == SAFETY_COLSTRAT_DETERMINISTIC) {
    assert(store->det_index);

    SensitiveTokKey lookup = {
        .col_ref = in->col_ref,
        .col_ref_len = in->col_ref_len,
        .value = in->value,
        .value_len = in->value_len,
    };
    const void *found_idx_ptr = ht_get_custom(store->det_index, &lookup);
    if (found_idx_ptr) {
      uint32_t found_idx = 0;
      if (stok_index_decode(found_idx_ptr, &found_idx) != OK)
        return -1;
      return stok_format_token(out_tok, store->connection_name, generation,
                               found_idx);
    }

    const SensitiveTok *added_tok = NULL;
    uint32_t added_idx = stok_append_entry(store, in, &added_tok);
    if (added_idx == UINT32_MAX || !added_tok)
      return -1;

    int added_len = stok_format_token(out_tok, store->connection_name,
                                      generation, added_idx);
    if (added_len < 0)
      return -1;

    // we have to persist the key used by the HashTable since it must be valid
    // for the whole HashTable's lifetime
    SensitiveTokKey *owned_key =
        (SensitiveTokKey *)pl_arena_alloc(store->arena, sizeof(*owned_key));
    if (!owned_key) {
      parr_drop_swap(store->tokens, added_idx);
      return -1;
    }
    owned_key->col_ref = added_tok->col_ref;
    owned_key->col_ref_len = added_tok->col_ref_len;
    owned_key->value = added_tok->value;
    owned_key->value_len = added_tok->value_len;

    if (ht_put_custom(store->det_index, owned_key,
                      stok_index_encode(added_idx)) != OK) {
      parr_drop_swap(store->tokens, added_idx);
      return -1;
    }
    return added_len;
  }

  if (store->mode != SAFETY_COLSTRAT_RANDOMIZED)
    return -1;

  uint32_t added_idx = stok_append_entry(store, in, NULL);
  if (added_idx == UINT32_MAX)
    return -1;

  int tok_len =
      stok_format_token(out_tok, store->connection_name, generation, added_idx);
  if (tok_len < 0) {
    parr_drop_swap(store->tokens, added_idx);
    return -1;
  }
  return tok_len;
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
