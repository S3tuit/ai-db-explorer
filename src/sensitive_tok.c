#include "sensitive_tok.h"

#include <assert.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arena.h"
#include "hash_table.h"
#include "packed_array.h"
#include "utils.h"

struct SensitiveTokSession {
  Arena arena;
  PackedArray *stores; // entries are DbTokenStore*
  uint32_t generation;
  uint32_t arena_cap;
};

struct DbTokenStore {
  SensitiveTokSession *owner;
  const char *connection_name;
  uint32_t connection_name_len;
  SafetyColumnStrategy mode;
  PackedArray *tokens;  // entries are SensitiveTok
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
static inline AdbxStatus stok_index_decode(const void *p, uint32_t *out_idx) {
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
  const char *domain;
  uint32_t domain_len;
  const char *value;
  uint32_t value_len;
} SensitiveTokKey;

/* Hashes one deterministic token key by (domain,value bytes).
 * It borrows 'key' and does not allocate memory.
 * Error semantics: returns non-zero hash for valid key, 0 for invalid input.
 */
static uint64_t tok_hash(const void *key, void *ctx) {
  (void)ctx;
  const SensitiveTokKey *k = (const SensitiveTokKey *)key;
  assert(k);
  if (!k->domain || k->domain_len == 0)
    return 0;
  if (!k->value && k->value_len != 0)
    return 0;

  uint64_t seed = ht_hash_bytes(k->domain, k->domain_len);
  return ht_hash_bytes_withSeed(k->value, k->value_len, seed);
}

/* Compares two deterministic token keys by exact byte content.
 * It borrows all inputs and does not allocate memory.
 * Side effects: none.
 * Error semantics: returns YES when equal, NO otherwise.
 */
static AdbxTriStatus tok_eq(const void *a, const void *b, void *ctx) {
  (void)ctx;
  const SensitiveTokKey *ka = (const SensitiveTokKey *)a;
  const SensitiveTokKey *kb = (const SensitiveTokKey *)b;
  assert(ka);
  assert(kb);

  if (!ka->domain || !kb->domain)
    return NO;
  if (ka->domain_len != kb->domain_len || ka->value_len != kb->value_len)
    return NO;
  if (memcmp(ka->domain, kb->domain, ka->domain_len) != 0)
    return NO;
  if (ka->value_len == 0)
    return YES;
  return (memcmp(ka->value, kb->value, ka->value_len) == 0) ? YES : NO;
}

/* Releases internals of one in-place store without freeing the struct itself.
 * It borrows 'store' and does not allocate memory.
 * Side effects: frees owned arrays/index and clears borrowed pointers.
 * Error semantics: none (safe on NULL/partially initialized input).
 */
static void stok_store_clean_inplace(DbTokenStore *store) {
  if (!store)
    return;

  parr_destroy(store->tokens);
  store->tokens = NULL;

  ht_destroy(store->det_index);
  store->det_index = NULL;

  store->connection_name = NULL;
  store->connection_name_len = 0;
  store->mode = 0;
  store->owner = NULL;
}

static AdbxStatus stok_store_init_inplace(DbTokenStore *store,
                                          SensitiveTokSession *owner,
                                          const char *connection_name,
                                          SafetyColumnStrategy mode) {
  if (!store || !owner || !connection_name)
    return ERR;

  size_t conn_len = strlen(connection_name);
  if (conn_len == 0 || conn_len > UINT32_MAX) {
    return ERR;
  }
  store->owner = owner;
  store->connection_name = connection_name;
  store->connection_name_len = (uint32_t)conn_len;
  store->mode = mode;

  if (store->mode != SAFETY_COLSTRAT_RANDOMIZED &&
      store->mode != SAFETY_COLSTRAT_DETERMINISTIC) {
    stok_store_clean_inplace(store);
    return ERR;
  }

  store->tokens = parr_create(sizeof(SensitiveTok));
  if (!store->tokens) {
    stok_store_clean_inplace(store);
    return ERR;
  }

  if (store->mode == SAFETY_COLSTRAT_DETERMINISTIC) {
    store->det_index = ht_create_custom(tok_hash, tok_eq, NULL);
    if (!store->det_index) {
      stok_store_clean_inplace(store);
      return ERR;
    }
  }

  return OK;
}

static DbTokenStore *stok_store_create_owned(SensitiveTokSession *owner,
                                             const ConnProfile *profile) {
  if (!owner || !profile || !profile->connection_name)
    return NULL;

  DbTokenStore *store = xcalloc(1, sizeof(*store));
  if (stok_store_init_inplace(store, owner, profile->connection_name,
                              profile->safe_policy.column_strategy) != OK) {
    stok_store_clean_inplace(store);
    free(store);
    return NULL;
  }
  return store;
}

void stok_store_destroy(DbTokenStore *store) {
  if (!store)
    return;
  stok_store_clean_inplace(store);
  free(store);
}

/* Cleanup function used by PackedArray to clean DbTokenStore. */
static void stok_session_store_cleanup(void *obj, void *ctx) {
  (void)ctx;
  DbTokenStore **slot = (DbTokenStore **)obj;
  if (!slot)
    return;
  stok_store_destroy(*slot);
  *slot = NULL;
}

/* Returns a PackedArray for DbTokenStore with cleanup function set. Returns
 * NULL on internal error. */
static PackedArray *stok_session_create_store_array(void) {
  PackedArray *stores = parr_create(sizeof(DbTokenStore *));
  if (!stores)
    return NULL;
  parr_set_cleanup(stores, stok_session_store_cleanup, NULL);
  return stores;
}

SensitiveTokSession *stok_session_create(uint32_t arena_cap) {
  if (arena_cap == 0)
    return NULL;

  SensitiveTokSession *sess = xcalloc(1, sizeof(*sess));
  sess->arena_cap = arena_cap;
  if (arena_init(&sess->arena, NULL, &sess->arena_cap) != OK) {
    free(sess);
    return NULL;
  }

  sess->stores = stok_session_create_store_array();
  if (!sess->stores) {
    arena_clean(&sess->arena);
    free(sess);
    return NULL;
  }
  sess->generation = 0;
  return sess;
}

void stok_session_destroy(SensitiveTokSession *sess) {
  if (!sess)
    return;
  parr_destroy(sess->stores);
  sess->stores = NULL;
  arena_zero_mem(&sess->arena);
  arena_clean(&sess->arena);
  free(sess);
}

AdbxTriStatus stok_session_is_ok(const SensitiveTokSession *sess) {
  if (!sess)
    return ERR;
  if (!sess->stores || arena_is_ok(&sess->arena) != YES || sess->arena_cap == 0)
    return NO;
  return YES;
}

uint32_t stok_session_generation(const SensitiveTokSession *sess) {
  return sess ? sess->generation : 0u;
}

uint32_t stok_session_arena_used(SensitiveTokSession *sess) {
  if (!sess)
    return 0u;
  return arena_get_used(&sess->arena);
}

DbTokenStore *stok_session_get_or_create_store(SensitiveTokSession *sess,
                                               const ConnProfile *profile) {
  if (!sess || !profile || !profile->connection_name || !sess->stores)
    return NULL;

  size_t n = parr_len(sess->stores);
  for (size_t i = 0; i < n; i++) {
    DbTokenStore **slot = (DbTokenStore **)parr_at(sess->stores, (uint32_t)i);
    if (!slot || !*slot)
      continue;
    AdbxTriStatus eq =
        stok_store_matches_conn_name(*slot, profile->connection_name);
    if (eq == YES)
      return *slot;
  }

  DbTokenStore **slot = NULL;
  uint32_t idx = parr_emplace(sess->stores, (void **)&slot);
  if (idx == UINT32_MAX || !slot)
    return NULL;
  *slot = NULL;

  DbTokenStore *store = stok_store_create_owned(sess, profile);
  if (!store) {
    parr_drop_swap(sess->stores, idx);
    return NULL;
  }
  *slot = store;
  return store;
}

/* Resets token state after arena cap exhaustion while keeping the triggering
 * store object alive. The token entries, deterministic index, arena contents,
 * and sibling stores are discarded; callers must reacquire any other borrowed
 * stores after a successful reset.
 *
 * Keeping 'preserve' as the same object matters because callers such as
 * QueryResultBuilder may hold a borrowed store pointer while producing many
 * tokens.
 */
static AdbxStatus stok_session_reset_preserve(SensitiveTokSession *sess,
                                              DbTokenStore *preserve) {
  if (!sess || !preserve || preserve->owner != sess || !sess->stores)
    return ERR;
  if (sess->generation == UINT32_MAX)
    return ERR;

  const char *connection_name = preserve->connection_name;
  SafetyColumnStrategy mode = preserve->mode;
  if (!connection_name)
    return ERR;

  PackedArray *old_stores = sess->stores;
  int found = 0;
  size_t old_len = parr_len(old_stores);
  for (size_t i = 0; i < old_len; i++) {
    DbTokenStore **slot = (DbTokenStore **)parr_at(old_stores, (uint32_t)i);
    if (slot && *slot == preserve) {
      found = 1;
      break;
    }
  }
  if (!found)
    return ERR;

  PackedArray *new_stores = stok_session_create_store_array();
  if (!new_stores)
    return ERR;

  DbTokenStore **new_slot = NULL;
  uint32_t new_idx = parr_emplace(new_stores, (void **)&new_slot);
  if (new_idx == UINT32_MAX || !new_slot) {
    parr_destroy(new_stores);
    return ERR;
  }
  *new_slot = NULL;

  /* Destroy all other stores explicitly. The old registry cleanup is disabled
   * below so it cannot free the store whose pointer must remain valid.
   */
  for (size_t i = 0; i < old_len; i++) {
    DbTokenStore **slot = (DbTokenStore **)parr_at(old_stores, (uint32_t)i);
    if (!slot || !*slot || *slot == preserve)
      continue;
    stok_store_destroy(*slot);
    *slot = NULL;
  }

  parr_set_cleanup(old_stores, NULL, NULL);
  parr_destroy(old_stores);
  sess->stores = NULL;

  stok_store_clean_inplace(preserve);

  arena_zero_mem(&sess->arena);
  arena_clean(&sess->arena);
  if (arena_init(&sess->arena, NULL, &sess->arena_cap) != OK) {
    parr_destroy(new_stores);
    return ERR;
  }

  if (stok_store_init_inplace(preserve, sess, connection_name, mode) != OK) {
    parr_destroy(new_stores);
    arena_zero_mem(&sess->arena);
    arena_clean(&sess->arena);
    return ERR;
  }

  *new_slot = preserve;
  sess->stores = new_stores;
  sess->generation++;
  return OK;
}

AdbxTriStatus stok_store_same_connection(const DbTokenStore *a,
                                         const DbTokenStore *b) {
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

AdbxTriStatus stok_store_matches_conn_name(const DbTokenStore *store,
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

/* Appends one SensitiveTok entry to store->tokens from one borrowed input
 * view. It copies 'in->value' into the store's borrowed Arena and borrows
 * 'in->domain' directly, assuming the caller guarantees that domain pointer
 * outlives the store.
 * Populates 'out_idx' and 'out' with the exact SensitiveTok added to
 * store->tokens.
 * Error semantics: returns OK on success, AS_CAP when the token arena cap is
 * reached, or ERR on invalid input/allocation failure.
 */
static AdbxStatus stok_append_entry(DbTokenStore *store,
                                    const SensitiveTokIn *in, uint32_t *out_idx,
                                    const SensitiveTok **out) {
  assert(store != NULL);
  assert(store->owner != NULL);
  assert(in != NULL);
  assert(in->domain != NULL);
  assert(out_idx != NULL);

  *out_idx = UINT32_MAX;
  if (out)
    *out = NULL;

  Arena *arena = &store->owner->arena;
  const char *arena_value = NULL;
  if (in->value) {
    AdbxStatus rc = arena_add_nul(arena, (void *)in->value, in->value_len,
                                  (void **)&arena_value);
    if (rc != OK)
      return rc;
  }

  SensitiveTok *slot = NULL;
  uint32_t idx = parr_emplace(store->tokens, (void **)&slot);
  if (idx == UINT32_MAX || !slot)
    return ERR;

  slot->value = arena_value;
  slot->value_len = in->value_len;
  slot->domain = in->domain;
  slot->domain_len = in->domain_len;
  slot->pg_oid = in->pg_oid;
  if (out) {
    *out = slot;
  }
  *out_idx = idx;
  return OK;
}

static AdbxStatus
stok_store_create_token_input_ok(DbTokenStore *store, const SensitiveTokIn *in,
                                 char out_tok[SENSITIVE_TOK_BUFSZ]) {
  if (!store || !in || !out_tok)
    return ERR;
  if (!store->owner || stok_session_is_ok(store->owner) != YES)
    return ERR;
  if (!in->domain || in->domain_len == 0)
    return ERR;
  if (!in->value && in->value_len != 0)
    return ERR;
  if (store->connection_name_len == 0 ||
      store->connection_name_len > CONN_NAME_MAX_LEN)
    return ERR;
  if (!store->tokens || !store->connection_name)
    return ERR;
  if (store->mode == SAFETY_COLSTRAT_DETERMINISTIC && !store->det_index)
    return ERR;
  if (store->mode != SAFETY_COLSTRAT_DETERMINISTIC &&
      store->mode != SAFETY_COLSTRAT_RANDOMIZED)
    return ERR;
  return OK;
}

static AdbxStatus
stok_store_create_token_once(DbTokenStore *store, const SensitiveTokIn *in,
                             char out_tok[SENSITIVE_TOK_BUFSZ],
                             int *out_tok_len) {
  assert(store != NULL);
  assert(store->owner != NULL);
  assert(in != NULL);
  assert(out_tok != NULL);
  assert(out_tok_len != NULL);
  *out_tok_len = -1;

  if (store->mode == SAFETY_COLSTRAT_DETERMINISTIC) {
    assert(store->det_index);

    SensitiveTokKey lookup = {
        .domain = in->domain,
        .domain_len = in->domain_len,
        .value = in->value,
        .value_len = in->value_len,
    };
    const void *found_idx_ptr = ht_get_custom(store->det_index, &lookup);
    if (found_idx_ptr) {
      uint32_t found_idx = 0;
      if (stok_index_decode(found_idx_ptr, &found_idx) != OK)
        return ERR;
      int n = stok_format_token(out_tok, store->connection_name,
                                store->owner->generation, found_idx);
      if (n < 0)
        return ERR;
      *out_tok_len = n;
      return OK;
    }

    const SensitiveTok *added_tok = NULL;
    uint32_t added_idx = UINT32_MAX;
    AdbxStatus rc = stok_append_entry(store, in, &added_idx, &added_tok);
    if (rc != OK)
      return rc;
    if (added_idx == UINT32_MAX || !added_tok)
      return ERR;

    int added_len = stok_format_token(out_tok, store->connection_name,
                                      store->owner->generation, added_idx);
    if (added_len < 0)
      return ERR;

    // TODO: same-domain deterministic reuse currently assumes all columns in
    // the same domain are bind-compatible and can therefore reuse the stored
    // pg_oid of the first token created for that (domain, value) pair.
    // We have to persist the key used by the HashTable since it must be valid
    // for the whole HashTable's lifetime.
    SensitiveTokKey *owned_key = NULL;
    rc = arena_calloc(&store->owner->arena, (uint32_t)sizeof(*owned_key),
                      (void **)&owned_key);
    if (rc != OK) {
      parr_drop_swap(store->tokens, added_idx);
      return rc;
    }
    owned_key->domain = added_tok->domain;
    owned_key->domain_len = added_tok->domain_len;
    owned_key->value = added_tok->value;
    owned_key->value_len = added_tok->value_len;

    if (ht_put_custom(store->det_index, owned_key,
                      stok_index_encode(added_idx)) != OK) {
      parr_drop_swap(store->tokens, added_idx);
      return ERR;
    }
    *out_tok_len = added_len;
    return OK;
  }

  assert(store->mode == SAFETY_COLSTRAT_RANDOMIZED);

  uint32_t added_idx = UINT32_MAX;
  AdbxStatus rc = stok_append_entry(store, in, &added_idx, NULL);
  if (rc != OK)
    return rc;
  if (added_idx == UINT32_MAX)
    return ERR;

  int tok_len = stok_format_token(out_tok, store->connection_name,
                                  store->owner->generation, added_idx);
  if (tok_len < 0) {
    parr_drop_swap(store->tokens, added_idx);
    return ERR;
  }
  *out_tok_len = tok_len;
  return OK;
}

int stok_store_create_token(DbTokenStore *store, const SensitiveTokIn *in,
                            char out_tok[SENSITIVE_TOK_BUFSZ]) {
  if (stok_store_create_token_input_ok(store, in, out_tok) != OK)
    return -1;

  int n = -1;
  AdbxStatus rc = stok_store_create_token_once(store, in, out_tok, &n);
  if (rc == OK)
    return n;
  if (rc != AS_CAP)
    return -1;

  SensitiveTokSession *sess = store->owner;
  if (stok_session_reset_preserve(sess, store) != OK)
    return -1;

  n = -1;
  rc = stok_store_create_token_once(store, in, out_tok, &n);
  return (rc == OK) ? n : -1;
}

StokResolveStatus stok_store_resolve_token(DbTokenStore *store, char *token,
                                           const SensitiveTok **out_tok) {
  if (out_tok)
    *out_tok = NULL;
  if (!store || !token || !out_tok || !store->owner)
    return STOK_RESOLVE_ERR_INPUT;
  if (stok_session_is_ok(store->owner) != YES)
    return STOK_RESOLVE_ERR_INPUT;

  ParsedTokView parsed = {0};
  if (stok_parse_view_inplace(token, &parsed) != OK)
    return STOK_RESOLVE_ERR_FORMAT;

  AdbxTriStatus conn_match =
      stok_store_matches_conn_name(store, parsed.connection_name);
  if (conn_match == NO)
    return STOK_RESOLVE_ERR_CONNECTION;
  if (conn_match != YES)
    return STOK_RESOLVE_ERR_INPUT;

  if (parsed.generation != store->owner->generation)
    return STOK_RESOLVE_ERR_STALE;

  const SensitiveTok *resolved = stok_store_get(store, parsed.index);
  if (!resolved)
    return STOK_RESOLVE_ERR_UNKNOWN;

  *out_tok = resolved;
  return STOK_RESOLVE_OK;
}

/* Parses one unsigned base-10 integer from [start, end) into '*out_u32'.
 * It borrows input pointers and does not allocate.
 * Side effects: writes to '*out_u32' on success.
 * Error semantics: returns OK on valid uint32 text, ERR otherwise.
 */
static AdbxStatus parse_u32_span(const char *start, const char *end,
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

AdbxStatus stok_parse_view_inplace(char *token, ParsedTokView *out) {
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
