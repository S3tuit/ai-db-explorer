#include <ctype.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "string_op.h"
#include "test.h"

#define FUZZ_CONFIG_CASES 96u
#define FUZZ_LOOKUPS_MIN 20u
#define FUZZ_LOOKUPS_SPAN 81u

typedef struct {
  uint64_t state;
} FuzzRng;

typedef struct {
  const char *domain; // borrowed from FuzzModel.domains
  char *schema;       // owned by the rule; NULL when unqualified
  char *table;        // owned by the rule; NULL when column-only
  char *column_pat;   // owned by the rule; normalized lowercase pattern
  uint16_t star_count;
  uint16_t literal_len;
} FuzzRule;

typedef struct {
  char **domains; // owned array of owned lowercase domain names
  size_t n_domains;
  FuzzRule *rules; // owned flat rule array
  size_t n_rules;
} FuzzModel;

typedef struct {
  char *schema; // owned; may be NULL or ""
  char *table;  // owned; may be NULL
  char *column; // owned and never NULL on success
} FuzzLookup;

typedef enum {
  FUZZ_CONFLICT_EXACT_EQUIV = 0,
  FUZZ_CONFLICT_GLOB_EQUIV,
  FUZZ_CONFLICT_ESCAPED_EQUIV,
} FuzzConflictKind;

static uint64_t g_suite_seed = 0;

static const char *const g_schema_pool[] = {"public", "private", "crm", "sales",
                                            "registry"};
static const char *const g_table_pool[] = {"users", "accounts", "contacts",
                                           "anagrafica", "registry"};
static const char *const g_column_pool[] = {
    "email",   "telefono",  "codicefiscale", "iban", "vatcode", "nome",
    "cognome", "indirizzo", "cap",           "city", "balance", "note"};

/* Returns one pseudo-random 64-bit value and updates the caller-owned RNG
 * state. This uses xorshift64* for deterministic unit-test reproducibility.
 * Side effects: mutates '*rng' and abort process on invalid input.
 */
static uint64_t rng_next_u64(FuzzRng *rng) {
  ASSERT_TRUE(rng);

  if (rng->state == 0)
    rng->state = UINT64_C(0x9e3779b97f4a7c15);

  uint64_t x = rng->state;
  x ^= x >> 12;
  x ^= x << 25;
  x ^= x >> 27;
  rng->state = x;
  return x * UINT64_C(2685821657736338717);
}

/* Returns one value in [0, upper_bound) from the caller-owned RNG.
 * Aborts process on invalid input.
 */
static size_t rng_range(FuzzRng *rng, size_t upper_bound) {
  ASSERT_TRUE(rng);
  ASSERT_TRUE(upper_bound > 0);

  return (size_t)(rng_next_u64(rng) % upper_bound);
}

/* Returns one random boolean from the caller-owned RNG.
 * Aborts process on invalid input.
 */
static int rng_bool(FuzzRng *rng) {
  ASSERT_TRUE(rng);

  return (int)(rng_next_u64(rng) & 1u);
}

/* Appends one NUL-terminated string to 'sb'. It borrows both inputs and only
 * grows StrBuf storage as needed. Aborts process on invalid input.
 */
static void sb_append_cstr(StrBuf *sb, const char *s) {
  ASSERT_TRUE(sb);
  ASSERT_TRUE(s);

  AdbxStatus rc = sb_append_bytes(sb, s, strlen(s));
  ASSERT_TRUE(rc == OK);
}

/* Appends one ASCII character to 'sb'. It borrows 'sb' and performs no other
 * allocations beyond StrBuf growth. Aborts on allocation failure.
 */
static void sb_append_char(StrBuf *sb, char c) {
  AdbxStatus rc = sb_append_bytes(sb, &c, 1);
  ASSERT_TRUE(rc == OK);
}

/* Returns one owned lowercase domain name for a deterministic domain index.
 * Side effects: allocates one heap string. Aborts on allocation failure.
 */
static char *make_domain_name(size_t idx) {
  char buf[64];
  int n = snprintf(buf, sizeof(buf), "domain_%zu", idx);
  ASSERT_TRUE(n > 0);
  ASSERT_TRUE((size_t)n < sizeof(buf));
  char *rc = dup_or_null(buf);
  ASSERT_TRUE(rc);
  return rc;
}

/* Returns one owned lowercase identifier chosen from the static pool.
 * Side effects: allocates one heap string. Aborts on invalid input or
 * allocation failure.
 */
static char *make_identifier_name(FuzzRng *rng) {
  ASSERT_TRUE(rng);

  const char *base = g_column_pool[rng_range(rng, ARRLEN(g_column_pool))];
  if (rng_range(rng, 5) != 0) {
    char *rc = dup_or_null(base);
    ASSERT_TRUE(rc);
    return rc;
  }

  char buf[96];
  int n = snprintf(buf, sizeof(buf), "%s_%zu", base, rng_range(rng, 16));
  ASSERT_TRUE(n > 0);
  ASSERT_TRUE((size_t)n < sizeof(buf));
  char *rc = dup_or_null(buf);
  ASSERT_TRUE(rc);
  return rc;
}

/* Appends one JSON string escape for an ASCII byte.
 * It borrows 'sb' and does not allocate beyond StrBuf growth. Example: if 'c'
 * is E, it appends \u0045 that should be the same thing for JSON.
 * Aborts on invalid input or append failure.
 */
static void sb_append_json_hex_escape(StrBuf *sb, unsigned char c) {
  ASSERT_TRUE(sb);

  char buf[7];
  int n = snprintf(buf, sizeof(buf), "\\u%04X", (unsigned)c);
  ASSERT_TRUE(n == 6);

  AdbxStatus rc = sb_append_bytes(sb, buf, 6);
  ASSERT_TRUE(rc == OK);
}

/* Appends one JSON string literal that preserves the logical input string
 * while only perturbing alphabetic case. It borrows all inputs and grows 'sb'
 * as needed. Aborts on invalid input or append failure.
 */
static void sb_append_case_noisy_json_string(StrBuf *sb, const char *input,
                                             FuzzRng *rng) {
  ASSERT_TRUE(sb);
  ASSERT_TRUE(input);
  ASSERT_TRUE(rng);

  sb_append_char(sb, '"');
  for (size_t i = 0; input[i] != '\0'; i++) {
    char c = input[i];
    if (c >= 'a' && c <= 'z' && rng_bool(rng))
      c = (char)toupper((unsigned char)c);
    if ((unsigned char)c < 0x20 || c == '"' || c == '\\') {
      sb_append_json_hex_escape(sb, (unsigned char)c);
      continue;
    }
    sb_append_char(sb, c);
  }
  sb_append_char(sb, '"');
}

/* Appends one JSON string literal that decodes to the same logical value as
 * 'input' while guaranteeing that at least one alphanumeric byte is written as
 * a \uXXXX escape. It borrows all inputs and grows 'sb' as needed. Aborts on
 * invalid input or append failure.
 */
static void sb_append_forced_escaped_json_string(StrBuf *sb, const char *input,
                                                 FuzzRng *rng) {
  ASSERT_TRUE(sb);
  ASSERT_TRUE(input);
  ASSERT_TRUE(rng);

  int forced = 0;
  sb_append_char(sb, '"');
  for (size_t i = 0; input[i] != '\0'; i++) {
    char c = input[i];
    if (c >= 'a' && c <= 'z' && rng_bool(rng))
      c = (char)toupper((unsigned char)c);

    if (!forced && isalnum((unsigned char)c)) {
      sb_append_json_hex_escape(sb, (unsigned char)c);
      forced = 1;
      continue;
    }
    if ((unsigned char)c < 0x20 || c == '"' || c == '\\') {
      sb_append_json_hex_escape(sb, (unsigned char)c);
      continue;
    }
    sb_append_char(sb, c);
  }
  ASSERT_TRUE(forced == 1);
  sb_append_char(sb, '"');
}

/* Appends one noisy JSON string literal that still decodes to the intended
 * logical content after lowercasing and '*' normalization in conn_catalog.
 * The input is borrowed; the output JSON bytes are appended into 'sb'.
 * Side effects: mutates '*rng' and may grow 'sb'. Aborts on invalid input or
 * append failure.
 */
static void sb_append_noisy_json_string(StrBuf *sb, const char *input,
                                        FuzzRng *rng, int allow_star_dup) {
  ASSERT_TRUE(sb);
  ASSERT_TRUE(input);
  ASSERT_TRUE(rng);

  sb_append_char(sb, '"');

  for (size_t i = 0; input[i] != '\0'; i++) {
    char logical = input[i];
    char noisy = logical;
    if (logical >= 'a' && logical <= 'z' && rng_bool(rng))
      noisy = (char)toupper((unsigned char)logical);

    size_t repeat = 1;
    if (allow_star_dup && logical == '*' && rng_range(rng, 4) == 0)
      repeat = 2 + rng_range(rng, 3);

    for (size_t j = 0; j < repeat; j++) {
      if ((unsigned char)noisy < 0x20 || noisy == '"' || noisy == '\\') {
        sb_append_json_hex_escape(sb, (unsigned char)noisy);
        continue;
      }

      if ((isalnum((unsigned char)noisy) || noisy == '_') &&
          rng_range(rng, 5) == 0) {
        sb_append_json_hex_escape(sb, (unsigned char)noisy);
        continue;
      }

      sb_append_char(sb, noisy);
    }
  }

  sb_append_char(sb, '"');
}

/* Computes normalized glob statistics for one caller-borrowed pattern.
 * It performs no allocations and aborts on invalid input.
 */
static void compute_pattern_stats(const char *pattern, uint16_t *out_star_count,
                                  uint16_t *out_literal_len) {
  ASSERT_TRUE(pattern);
  ASSERT_TRUE(out_star_count);
  ASSERT_TRUE(out_literal_len);

  size_t n = strlen(pattern);
  ASSERT_TRUE(n > 0);

  uint16_t star_count = 0;
  uint16_t literal_len = 0;
  for (size_t i = 0; i < n; i++) {
    if (pattern[i] == '*')
      star_count++;
    else
      literal_len++;
  }

  *out_star_count = star_count;
  *out_literal_len = literal_len;
}

/* Returns one owned normalized copy of 'input' with repeated '*' collapsed.
 * It borrows 'input' and allocates the result for the caller. Aborts on
 * invalid input or allocation failure.
 */
static char *normalize_pattern_copy(const char *input) {
  ASSERT_TRUE(input);
  ASSERT_TRUE(input[0] != '\0');

  size_t n = strlen(input);
  char *out = (char *)xmalloc(n + 1);
  size_t w = 0;
  int prev_star = 0;
  for (size_t r = 0; r < n; r++) {
    char c = input[r];
    if (c == '*') {
      if (prev_star)
        continue;
      prev_star = 1;
    } else {
      prev_star = 0;
    }
    out[w++] = c;
  }
  out[w] = '\0';
  return out;
}

/* Returns one owned glob pattern derived from the exact identifier 'exact'.
 * It borrows 'exact', injects 1-2 '*' wildcards, and normalizes adjacent
 * stars so the result matches conn_catalog's normalization rules. Aborts on
 * invalid input or allocation failure.
 */
static char *make_glob_pattern(const char *exact, FuzzRng *rng) {
  ASSERT_TRUE(exact);
  ASSERT_TRUE(rng);

  size_t n = strlen(exact);
  if (n < 2) {
    char *rc = dup_or_null(exact);
    ASSERT_TRUE(rc);
    return rc;
  }

  char *tmp = dup_or_null(exact);
  ASSERT_TRUE(tmp != NULL);

  size_t repl = 1 + rng_range(rng, 2);
  for (size_t i = 0; i < repl; i++) {
    tmp[rng_range(rng, n)] = '*';
  }

  char *out = normalize_pattern_copy(tmp);
  free(tmp);
  ASSERT_TRUE(out != NULL);
  return out;
}

/* Returns one owned fully-qualified pattern string for the caller-borrowed
 * rule using [schema.][table.]column_pat shape. Aborts on invalid input or
 * allocation failure.
 */
static char *fuzz_rule_full_pattern_copy(const FuzzRule *rule) {
  ASSERT_TRUE(rule);
  ASSERT_TRUE(rule->column_pat);

  StrBuf sb;
  sb_init(&sb);
  if (rule->schema) {
    sb_append_cstr(&sb, rule->schema);
    sb_append_char(&sb, '.');
  }
  if (rule->table) {
    sb_append_cstr(&sb, rule->table);
    sb_append_char(&sb, '.');
  }
  sb_append_cstr(&sb, rule->column_pat);

  char *out = dup_or_null(sb_to_cstr(&sb));
  ASSERT_TRUE(out != NULL);
  sb_clean(&sb);
  return out;
}

/* Returns one owned variant of the normalized glob 'pattern' where one '*'
 * has been duplicated, so conn_catalog normalization should collapse it back to
 * the original pattern. Aborts on invalid input or allocation failure.
 */
static char *make_glob_conflict_pattern(const char *pattern, FuzzRng *rng) {
  ASSERT_TRUE(pattern);
  ASSERT_TRUE(rng);

  size_t n = strlen(pattern);
  ASSERT_TRUE(n > 0);

  size_t star_count = 0;
  for (size_t i = 0; i < n; i++) {
    if (pattern[i] == '*')
      star_count++;
  }
  ASSERT_TRUE(star_count > 0);

  size_t chosen = rng_range(rng, star_count);
  char *out = (char *)xmalloc(n + 2);
  size_t w = 0;
  size_t seen = 0;
  for (size_t i = 0; i < n; i++) {
    out[w++] = pattern[i];
    if (pattern[i] == '*') {
      if (seen == chosen)
        out[w++] = '*';
      seen++;
    }
  }
  out[w] = '\0';
  return out;
}

/* Compares two optional strings lexicographically with NULL ordered first.
 * It borrows both inputs and performs no allocations.
 * Returns <0, 0, >0 like strcmp().
 */
static int nullable_cmp(const char *a, const char *b) {
  if (!a && !b)
    return 0;
  if (!a)
    return -1;
  if (!b)
    return 1;
  return strcmp(a, b);
}

/* Returns the reference precedence rank for one rule shape.
 * It borrows 'rule' and performs no allocations.
 * Aborts on invalid rule shape and otherwise returns one rank in [0, 5].
 */
static int fuzz_rule_rank(const FuzzRule *rule) {
  ASSERT_TRUE(rule);
  ASSERT_TRUE(rule->column_pat);
  ASSERT_TRUE(!rule->schema || rule->table);

  if (rule->schema)
    return (rule->star_count == 0) ? 0 : 1;
  if (rule->table)
    return (rule->star_count == 0) ? 2 : 3;
  return (rule->star_count == 0) ? 4 : 5;
}

/* Orders rules in the same deterministic precedence used by the reference
 * matcher. It borrows both inputs and performs no allocations.
 * Returns <0, 0, >0 for qsort-style ordering.
 */
static int fuzz_rule_cmp(const void *a, const void *b) {
  const FuzzRule *ra = (const FuzzRule *)a;
  const FuzzRule *rb = (const FuzzRule *)b;

  int rka = fuzz_rule_rank(ra);
  int rkb = fuzz_rule_rank(rb);
  if (rka != rkb)
    return (rka < rkb) ? -1 : 1;

  int sc = nullable_cmp(ra->schema, rb->schema);
  if (sc != 0)
    return sc;

  int tc = nullable_cmp(ra->table, rb->table);
  if (tc != 0)
    return tc;

  if (ra->star_count != 0 || rb->star_count != 0) {
    if (ra->star_count != rb->star_count)
      return (ra->star_count < rb->star_count) ? -1 : 1;
    if (ra->literal_len != rb->literal_len)
      return (ra->literal_len > rb->literal_len) ? -1 : 1;
  }

  int pc = strcmp(ra->column_pat, rb->column_pat);
  if (pc != 0)
    return pc;
  return strcmp(ra->domain, rb->domain);
}

/* Checks whether two rules refer to the same normalized schema/table/pattern
 * triple, ignoring domain. It borrows both inputs and performs no allocations.
 * Aborts on invalid input. Returns YES when the match pattern is identical, NO
 * when different.
 */
static AdbxTriStatus fuzz_rule_same_pattern(const FuzzRule *a,
                                            const FuzzRule *b) {
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  if (nullable_cmp(a->schema, b->schema) != 0)
    return NO;
  if (nullable_cmp(a->table, b->table) != 0)
    return NO;
  if (strcmp(a->column_pat, b->column_pat) != 0)
    return NO;
  return YES;
}

/* Releases all heap allocations held by one caller-owned FuzzModel.
 * Side effects: frees nested strings and arrays, then zeroes '*model'.
 * Error semantics: none; NULL input is ignored.
 */
static void fuzz_model_clean(FuzzModel *model) {
  if (!model)
    return;

  for (size_t i = 0; i < model->n_domains; i++)
    free(model->domains[i]);
  free(model->domains);

  for (size_t i = 0; i < model->n_rules; i++) {
    free(model->rules[i].schema);
    free(model->rules[i].table);
    free(model->rules[i].column_pat);
  }
  free(model->rules);
  memset(model, 0, sizeof(*model));
}

/* Appends one fully-owned rule to 'model'. It takes ownership of 'schema',
 * 'table', and 'column_pat' regardless of success.
 * Side effects: grows the rule array on success. Aborts on invalid input or
 * allocation failure.
 */
static void fuzz_model_add_rule(FuzzModel *model, const char *domain,
                                char *schema, char *table, char *column_pat) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(domain);
  ASSERT_TRUE(column_pat);
  uint16_t star_count = 0;
  uint16_t literal_len = 0;
  compute_pattern_stats(column_pat, &star_count, &literal_len);

  FuzzRule *nr = (FuzzRule *)xrealloc(model->rules, (model->n_rules + 1) *
                                                        sizeof(*model->rules));
  model->rules = nr;

  FuzzRule *rule = &model->rules[model->n_rules++];
  rule->domain = domain;
  rule->schema = schema;
  rule->table = table;
  rule->column_pat = column_pat;
  rule->star_count = star_count;
  rule->literal_len = literal_len;
}

/* Generates one valid canonical sensitive-domain model for the given case
 * seed. The model uses lowercase normalized rules with no conflicting patterns.
 * Side effects: allocates model-owned strings and arrays. Aborts on invalid
 * input or allocation failure.
 */
static void fuzz_model_generate(FuzzModel *model, uint64_t case_seed) {
  ASSERT_TRUE(model);
  memset(model, 0, sizeof(*model));

  FuzzRng rng = {.state = case_seed};
  model->n_domains = 1 + rng_range(&rng, 4);
  model->domains = (char **)xcalloc(model->n_domains, sizeof(*model->domains));

  for (size_t i = 0; i < model->n_domains; i++) {
    model->domains[i] = make_domain_name(i);
    ASSERT_TRUE(model->domains[i] != NULL);
  }

  for (size_t d = 0; d < model->n_domains; d++) {
    size_t n_rules = 1 + rng_range(&rng, 5);
    for (size_t i = 0; i < n_rules; i++) {
      int added = 0;
      for (size_t attempt = 0; attempt < 128; attempt++) {
        size_t depth = rng_range(&rng, 3);
        char *schema = NULL;
        char *table = NULL;

        if (depth == 2)
          schema = dup_or_null(
              g_schema_pool[rng_range(&rng, ARRLEN(g_schema_pool))]);
        if (depth >= 1)
          table =
              dup_or_null(g_table_pool[rng_range(&rng, ARRLEN(g_table_pool))]);

        char *column_pat = make_identifier_name(&rng);
        ASSERT_TRUE(column_pat != NULL);
        if (rng_range(&rng, 10) < 4) {
          char *glob = make_glob_pattern(column_pat, &rng);
          free(column_pat);
          column_pat = glob;
          ASSERT_TRUE(column_pat != NULL);
        }

        FuzzRule cand = {.domain = model->domains[d],
                         .schema = schema,
                         .table = table,
                         .column_pat = column_pat};
        compute_pattern_stats(cand.column_pat, &cand.star_count,
                              &cand.literal_len);

        int conflict = 0;
        for (size_t r = 0; r < model->n_rules; r++) {
          if (fuzz_rule_same_pattern(&cand, &model->rules[r]) == YES) {
            conflict = 1;
            break;
          }
        }
        if (conflict) {
          free(schema);
          free(table);
          free(column_pat);
          continue;
        }

        fuzz_model_add_rule(model, model->domains[d], schema, table,
                            column_pat);
        added = 1;
        break;
      }

      ASSERT_TRUE(added == 1);
    }
  }

  qsort(model->rules, model->n_rules, sizeof(*model->rules), fuzz_rule_cmp);
}

/* Returns one random rule index matching the requested star shape.
 * 'want_star' must be -1 for any rule, 0 for exact rules, or 1 for glob
 * rules. It borrows 'model', uses '*rng', and performs no allocations.
 * Returns SIZE_MAX when no matching rule exists.
 */
static size_t fuzz_pick_rule_idx(const FuzzModel *model, int want_star,
                                 FuzzRng *rng) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(rng);
  ASSERT_TRUE(want_star >= -1 && want_star <= 1);

  size_t count = 0;
  for (size_t i = 0; i < model->n_rules; i++) {
    int has_star = (model->rules[i].star_count > 0) ? 1 : 0;
    if (want_star != -1 && has_star != want_star)
      continue;
    count++;
  }
  if (count == 0)
    return SIZE_MAX;

  size_t want = rng_range(rng, count);
  for (size_t i = 0; i < model->n_rules; i++) {
    int has_star = (model->rules[i].star_count > 0) ? 1 : 0;
    if (want_star != -1 && has_star != want_star)
      continue;
    if (want == 0)
      return i;
    want--;
  }

  return SIZE_MAX;
}

/* Shuffles one caller-owned index array in place using Fisher-Yates.
 * Side effects: mutates 'values' and '*rng'. Aborts on invalid input.
 */
static void shuffle_size_t(size_t *values, size_t n, FuzzRng *rng) {
  ASSERT_TRUE(values || n == 0);
  ASSERT_TRUE(rng);
  if (n <= 1)
    return;

  for (size_t i = n - 1; i > 0; i--) {
    size_t j = rng_range(rng, i + 1);
    size_t tmp = values[i];
    values[i] = values[j];
    values[j] = tmp;
  }
}

/* Emits one noisy JSON config for the canonical model.
 * The emitted JSON preserves logical equality after conn_catalog decoding,
 * lowercasing, and '*' normalization, but randomizes lexical encoding to
 * stress the parser. Aborts on invalid input or allocation failure and returns
 * one caller-owned JSON string.
 */
static char *fuzz_model_emit_noisy_json(const FuzzModel *model,
                                        uint64_t case_seed) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(model->domains);
  ASSERT_TRUE(model->n_domains > 0);

  FuzzRng rng = {.state = case_seed ^ UINT64_C(0x7261776a736f6e31)};
  StrBuf sb;
  sb_init(&sb);

  sb_append_cstr(&sb, "{");
  sb_append_cstr(&sb, "\"version\":\"1.1\",");
  sb_append_cstr(&sb, "\"configNamespace\":\"TestNamespace\",");
  sb_append_cstr(&sb, "\"safetyPolicy\":{},");
  sb_append_cstr(&sb, "\"databases\":[{");
  sb_append_cstr(&sb, "\"type\":\"postgres\",");
  sb_append_cstr(&sb, "\"connectionName\":\"FuzzSensitiveDomains\",");
  sb_append_cstr(&sb, "\"host\":\"127.0.0.1\",");
  sb_append_cstr(&sb, "\"port\":5432,");
  sb_append_cstr(&sb, "\"username\":\"user\",");
  sb_append_cstr(&sb, "\"database\":\"db\",");
  sb_append_cstr(&sb, "\"sensitiveDomains\":{");

  size_t *domain_order =
      (size_t *)xmalloc(model->n_domains * sizeof(*domain_order));
  for (size_t i = 0; i < model->n_domains; i++)
    domain_order[i] = i;
  shuffle_size_t(domain_order, model->n_domains, &rng);

  for (size_t di = 0; di < model->n_domains; di++) {
    size_t dom_ix = domain_order[di];
    if (di != 0)
      sb_append_char(&sb, ',');

    sb_append_noisy_json_string(&sb, model->domains[dom_ix], &rng, 0);
    sb_append_char(&sb, ':');
    sb_append_char(&sb, '[');

    size_t count = 0;
    for (size_t r = 0; r < model->n_rules; r++) {
      if (strcmp(model->rules[r].domain, model->domains[dom_ix]) == 0)
        count++;
    }

    size_t *rule_ixs = (size_t *)xmalloc(count * sizeof(*rule_ixs));
    size_t w = 0;
    for (size_t r = 0; r < model->n_rules; r++) {
      if (strcmp(model->rules[r].domain, model->domains[dom_ix]) == 0)
        rule_ixs[w++] = r;
    }
    ASSERT_TRUE(w == count);
    shuffle_size_t(rule_ixs, count, &rng);

    int first_elem = 1;
    for (size_t i = 0; i < count; i++) {
      const FuzzRule *rule = &model->rules[rule_ixs[i]];
      StrBuf pat_sb;
      sb_init(&pat_sb);
      if (rule->schema) {
        sb_append_cstr(&pat_sb, rule->schema);
        sb_append_char(&pat_sb, '.');
      }
      if (rule->table) {
        sb_append_cstr(&pat_sb, rule->table);
        sb_append_char(&pat_sb, '.');
      }
      sb_append_cstr(&pat_sb, rule->column_pat);
      char *full_pat = sb_to_cstr(&pat_sb);
      ASSERT_TRUE(full_pat != NULL);

      size_t dup_count = 1;
      if (rng_range(&rng, 4) == 0)
        dup_count += 1 + rng_range(&rng, 2);

      for (size_t dup = 0; dup < dup_count; dup++) {
        if (!first_elem)
          sb_append_char(&sb, ',');
        first_elem = 0;
        sb_append_noisy_json_string(&sb, full_pat, &rng, 1);
      }

      sb_clean(&pat_sb);
    }

    free(rule_ixs);
    sb_append_char(&sb, ']');
  }

  free(domain_order);
  sb_append_cstr(&sb, "}}]}");
  char *json = dup_or_null(sb_to_cstr(&sb));
  ASSERT_TRUE(json != NULL);
  sb_clean(&sb);
  return json;
}

/* Emits one invalid JSON config derived from a valid model by injecting one
 * extra domain entry whose pattern normalizes to the same rule as an existing
 * domain. It borrows 'model', chooses one conflict shape using '*rng', and
 * returns one caller-owned JSON string plus the chosen kind.
 * Aborts on invalid input or allocation failure.
 */
static char *fuzz_model_emit_noisy_json_with_conflict(
    const FuzzModel *model, uint64_t case_seed, FuzzConflictKind *out_kind) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(model->n_rules > 0);
  ASSERT_TRUE(out_kind);

  FuzzRng rng = {.state = case_seed ^ UINT64_C(0x434f4e464c494354)};
  FuzzConflictKind kinds[3];
  size_t nkinds = 0;

  if (fuzz_pick_rule_idx(model, 0, &rng) != SIZE_MAX)
    kinds[nkinds++] = FUZZ_CONFLICT_EXACT_EQUIV;
  if (fuzz_pick_rule_idx(model, 1, &rng) != SIZE_MAX)
    kinds[nkinds++] = FUZZ_CONFLICT_GLOB_EQUIV;
  kinds[nkinds++] = FUZZ_CONFLICT_ESCAPED_EQUIV;
  ASSERT_TRUE(nkinds > 0);

  FuzzConflictKind kind = kinds[rng_range(&rng, nkinds)];
  *out_kind = kind;

  size_t rule_ix = SIZE_MAX;
  if (kind == FUZZ_CONFLICT_EXACT_EQUIV)
    rule_ix = fuzz_pick_rule_idx(model, 0, &rng);
  else if (kind == FUZZ_CONFLICT_GLOB_EQUIV)
    rule_ix = fuzz_pick_rule_idx(model, 1, &rng);
  else
    rule_ix = fuzz_pick_rule_idx(model, -1, &rng);
  ASSERT_TRUE(rule_ix != SIZE_MAX);

  char *full_pat = fuzz_rule_full_pattern_copy(&model->rules[rule_ix]);
  ASSERT_TRUE(full_pat != NULL);

  char *conflict_pat = NULL;
  if (kind == FUZZ_CONFLICT_GLOB_EQUIV) {
    conflict_pat = make_glob_conflict_pattern(full_pat, &rng);
  } else {
    conflict_pat = dup_or_null(full_pat);
  }
  ASSERT_TRUE(conflict_pat != NULL);

  StrBuf pat_sb;
  sb_init(&pat_sb);
  if (kind == FUZZ_CONFLICT_ESCAPED_EQUIV) {
    sb_append_forced_escaped_json_string(&pat_sb, conflict_pat, &rng);
  } else {
    sb_append_case_noisy_json_string(&pat_sb, conflict_pat, &rng);
  }

  char *json = fuzz_model_emit_noisy_json(model, case_seed);
  ASSERT_TRUE(json != NULL);
  size_t json_len = strlen(json);
  ASSERT_TRUE(json_len >= 4);
  ASSERT_TRUE(memcmp(json + json_len - 4, "}}]}", 4) == 0);

  const char *frag_mid = ",\"conflict_domain\":[";
  const char *frag_tail = "]";
  size_t pat_len = strlen(sb_to_cstr(&pat_sb));
  size_t out_len = (json_len - 4) + strlen(frag_mid) + pat_len +
                   strlen(frag_tail) + 4;
  char *out = (char *)xmalloc(out_len + 1);

  size_t w = 0;
  memcpy(out + w, json, json_len - 4);
  w += json_len - 4;
  memcpy(out + w, frag_mid, strlen(frag_mid));
  w += strlen(frag_mid);
  memcpy(out + w, sb_to_cstr(&pat_sb), pat_len);
  w += pat_len;
  memcpy(out + w, frag_tail, strlen(frag_tail));
  w += strlen(frag_tail);
  memcpy(out + w, "}}]}", 4);
  w += 4;
  out[w] = '\0';

  sb_clean(&pat_sb);
  free(conflict_pat);
  free(full_pat);
  free(json);
  return out;
}

/* Matches one normalized wildcard pattern against one normalized candidate
 * value using a simple recursive '*' matcher independent from production code.
 * It borrows both inputs and performs no allocations.
 * Aborts on invalid input. Returns YES when the pattern matches, NO when it
 * does not.
 */
static AdbxTriStatus ref_glob_match(const char *pattern, const char *value) {
  ASSERT_TRUE(pattern);
  ASSERT_TRUE(value);

  if (*pattern == '\0')
    return (*value == '\0') ? YES : NO;
  if (*pattern == '*') {
    if (ref_glob_match(pattern + 1, value) == YES)
      return YES;
    if (*value != '\0' && ref_glob_match(pattern, value + 1) == YES)
      return YES;
    return NO;
  }
  if (*value == '\0' || *pattern != *value)
    return NO;
  return ref_glob_match(pattern + 1, value + 1);
}

/* Checks whether one reference rule matches the caller-supplied lookup. It
 * borrows all inputs and performs no allocations.
 * Error semantics: returns YES on match, NO when the rule does not apply.
 */
static AdbxTriStatus fuzz_rule_matches(const FuzzRule *rule, const char *schema,
                                       const char *table, const char *column,
                                       int ignore_schema) {
  ASSERT_TRUE(rule);
  ASSERT_TRUE(table);
  ASSERT_TRUE(column);

  if (!ignore_schema && rule->schema) {
    if (!schema || strcmp(rule->schema, schema) != 0)
      return NO;
  }
  if (rule->table && strcmp(rule->table, table) != 0)
    return NO;

  if (rule->star_count == 0)
    return (strcmp(rule->column_pat, column) == 0) ? YES : NO;
  return ref_glob_match(rule->column_pat, column);
}

/* Finds the first matching domain within one rank range of the sorted
 * reference-rule array. It borrows all inputs and writes one borrowed domain
 * through 'out_domain' on success.
 * Error semantics: returns YES on first match, NO when nothing matches.
 */
static AdbxTriStatus
fuzz_model_find_first_match(const FuzzModel *model, int rank_lo, int rank_hi,
                            const char *schema, const char *table,
                            const char *column, int ignore_schema,
                            const char **out_domain) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(table);
  ASSERT_TRUE(column);
  ASSERT_TRUE(out_domain);

  for (size_t i = 0; i < model->n_rules; i++) {
    const FuzzRule *rule = &model->rules[i];
    int rank = fuzz_rule_rank(rule);
    if (rank < rank_lo)
      continue;
    if (rank > rank_hi)
      break;

    if (fuzz_rule_matches(rule, schema, table, column, ignore_schema) == YES) {
      *out_domain = rule->domain;
      return YES;
    }
  }
  return NO;
}

/* Collects one unique matching schema-qualified domain across all schemas for
 * the caller-supplied table/column pair. It borrows all inputs and writes the
 * borrowed unique domain through 'inout_domain' when present.
 * Error semantics: returns YES when one or more schema-qualified rules match
 * without conflict, NO when none match, ERR on conflicting matched domains.
 */
static AdbxTriStatus
fuzz_model_collect_unique_stc_domain(const FuzzModel *model, const char *table,
                                     const char *column,
                                     const char **inout_domain) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(table);
  ASSERT_TRUE(column);
  ASSERT_TRUE(inout_domain);

  int found = 0;
  for (size_t i = 0; i < model->n_rules; i++) {
    const FuzzRule *rule = &model->rules[i];
    int rank = fuzz_rule_rank(rule);
    if (rank > 1)
      break;
    if (fuzz_rule_matches(rule, NULL, table, column, 1) != YES)
      continue;

    if (!*inout_domain) {
      *inout_domain = rule->domain;
    } else if (strcmp(*inout_domain, rule->domain) != 0) {
      return ERR;
    }
    found = 1;
  }

  return found ? YES : NO;
}

/* Finds the expected sensitive-domain result for one lookup according to the
 * canonical reference model. It borrows all inputs and performs no
 * allocations. Column-only lookups are invalid and therefore return ERR.
 * On YES, it writes one borrowed expected domain through 'out_domain'.
 */
static AdbxTriStatus fuzz_model_find_domain(const FuzzModel *model,
                                            const char *schema,
                                            const char *table,
                                            const char *column,
                                            const char **out_domain) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(out_domain);

  *out_domain = NULL;
  if (!table || !column || table[0] == '\0' || column[0] == '\0')
    return ERR;

  const char *schema_norm = (schema && schema[0] != '\0') ? schema : NULL;
  if (schema_norm) {
    return fuzz_model_find_first_match(model, 0, 5, schema_norm, table, column,
                                       0, out_domain);
  }

  const char *tc_domain = NULL;
  AdbxTriStatus rc = fuzz_model_find_first_match(model, 2, 5, NULL, table,
                                                 column, 0, &tc_domain);

  const char *stc_domain = NULL;
  AdbxTriStatus src =
      fuzz_model_collect_unique_stc_domain(model, table, column, &stc_domain);
  if (src == ERR)
    return ERR;
  if (src == NO) {
    *out_domain = tc_domain;
    return rc;
  }
  if (rc == NO) {
    *out_domain = stc_domain;
    return YES;
  }
  if (strcmp(stc_domain, tc_domain) != 0)
    return ERR;
  *out_domain = tc_domain;
  return YES;
}

/* Returns one owned concrete identifier that matches the caller-borrowed glob
 * pattern. It performs deterministic random expansion for '*' segments and
 * aborts on invalid input or allocation failure.
 */
static char *materialize_glob_pattern(const char *pattern, FuzzRng *rng) {
  ASSERT_TRUE(pattern);
  ASSERT_TRUE(rng);

  StrBuf sb;
  sb_init(&sb);
  for (size_t i = 0; pattern[i] != '\0'; i++) {
    if (pattern[i] == '*') {
      size_t n = rng_range(rng, 3);
      for (size_t j = 0; j < n; j++) {
        char c = (char)('a' + rng_range(rng, 26));
        sb_append_char(&sb, c);
      }
      continue;
    }
    sb_append_char(&sb, pattern[i]);
  }

  char *out = dup_or_null(sb_to_cstr(&sb));
  ASSERT_TRUE(out != NULL);
  sb_clean(&sb);
  return out;
}

/* Releases all owned strings held by one caller-owned FuzzLookup and zeroes
 * the structure.
 * Side effects: frees memory.
 * Error semantics: none; NULL input is ignored.
 */
static void fuzz_lookup_clean(FuzzLookup *lookup) {
  if (!lookup)
    return;
  free(lookup->schema);
  free(lookup->table);
  free(lookup->column);
  memset(lookup, 0, sizeof(*lookup));
}

/* Generates one random lookup, either targeted to an existing rule or fully
 * synthetic. The lookup strings are owned by '*out'.
 * Side effects: allocates strings and mutates '*rng'. Aborts on invalid input
 * or allocation failure.
 */
static void fuzz_lookup_generate(const FuzzModel *model, FuzzLookup *out,
                                 FuzzRng *rng) {
  ASSERT_TRUE(model);
  ASSERT_TRUE(out);
  ASSERT_TRUE(rng);
  memset(out, 0, sizeof(*out));

  if (model->n_rules > 0 && rng_range(rng, 10) < 6) {
    const FuzzRule *rule = &model->rules[rng_range(rng, model->n_rules)];
    out->column = (rule->star_count == 0)
                      ? dup_or_null(rule->column_pat)
                      : materialize_glob_pattern(rule->column_pat, rng);
    ASSERT_TRUE(out->column != NULL);

    if (rule->table)
      out->table = dup_or_null(rule->table);
    else if (rng_range(rng, 3) == 0)
      out->table =
          dup_or_null(g_table_pool[rng_range(rng, ARRLEN(g_table_pool))]);

    if (rule->schema) {
      if (rng_bool(rng))
        out->schema = dup_or_null(rule->schema);
      else
        out->schema = dup_or_null("");
    } else if (rng_range(rng, 3) == 0) {
      out->schema =
          dup_or_null(g_schema_pool[rng_range(rng, ARRLEN(g_schema_pool))]);
    } else if (rng_bool(rng)) {
      out->schema = dup_or_null("");
    }
    return;
  }

  size_t depth = rng_range(rng, 3);
  out->column = make_identifier_name(rng);
  ASSERT_TRUE(out->column != NULL);

  if (depth >= 1)
    out->table =
        dup_or_null(g_table_pool[rng_range(rng, ARRLEN(g_table_pool))]);
  if (depth == 2)
    out->schema =
        dup_or_null(g_schema_pool[rng_range(rng, ARRLEN(g_schema_pool))]);
  else if (rng_bool(rng))
    out->schema = dup_or_null("");
}

/* Prints one detailed fuzz failure and terminates the process so the failing
 * seed remains visible in CI logs. It borrows all inputs.
 * Side effects: writes to stderr and exits(1).
 * Error semantics: none; this function does not return.
 */
static void fail_fuzz_case(uint64_t case_seed, size_t lookup_idx,
                           const char *json, const FuzzLookup *lookup,
                           AdbxTriStatus exp_rc, const char *exp_domain,
                           AdbxTriStatus got_rc, const char *got_domain,
                           const char *err_msg) {
  fprintf(stderr,
          "fuzz conn_catalog failure\n"
          "  suite_seed: 0x%016" PRIx64 "\n"
          "  case_seed: 0x%016" PRIx64 "\n"
          "  lookup_idx: %zu\n"
          "  schema: %s\n"
          "  table: %s\n"
          "  column: %s\n"
          "  exp_rc: %d\n"
          "  expected: %s\n"
          "  got_rc: %d\n"
          "  got_domain: %s\n",
          g_suite_seed, case_seed, lookup_idx,
          (lookup && lookup->schema) ? lookup->schema : "(null)",
          (lookup && lookup->table) ? lookup->table : "(null)",
          (lookup && lookup->column) ? lookup->column : "(null)",
          (int)exp_rc, exp_domain ? exp_domain : "(none)", (int)got_rc,
          got_domain ? got_domain : "(null)");
  if (err_msg)
    fprintf(stderr, "  error: %s\n", err_msg);
  if (json)
    fprintf(stderr, "  json: %s\n", json);
  exit(1);
}

/* Prints one invalid-config fuzz failure and terminates the process so the
 * failing seed and injected conflict kind remain visible in CI logs.
 * It borrows all inputs and does not return.
 */
static void fail_invalid_fuzz_case(uint64_t case_seed, FuzzConflictKind kind,
                                   const char *json, const char *err) {
  const char *kind_name = "unknown";
  switch (kind) {
  case FUZZ_CONFLICT_EXACT_EQUIV:
    kind_name = "exact_equivalent";
    break;
  case FUZZ_CONFLICT_GLOB_EQUIV:
    kind_name = "glob_equivalent";
    break;
  case FUZZ_CONFLICT_ESCAPED_EQUIV:
    kind_name = "escaped_equivalent";
    break;
  }

  fprintf(stderr,
          "fuzz conn_catalog invalid-config failure\n"
          "  suite_seed: 0x%016" PRIx64 "\n"
          "  case_seed: 0x%016" PRIx64 "\n"
          "  conflict_kind: %s\n",
          g_suite_seed, case_seed, kind_name);
  if (err)
    fprintf(stderr, "  error: %s\n", err);
  if (json)
    fprintf(stderr, "  json: %s\n", json);
  exit(1);
}

/* Initializes the fuzz-suite seed. When ADBX_FUZZ_SUITE_SEED is set, it uses
 * that value (base 0, so hex is accepted). Otherwise it mixes current wall
 * time with process id to produce a different seed on each run.
 * Side effects: reads one environment variable and writes the global
 * 'g_suite_seed'.
 * Error semantics: aborts on invalid env-var syntax or clock failure.
 */
static void init_suite_seed(void) {
  const char *env = getenv("ADBX_FUZZ_SUITE_SEED");
  if (env && env[0] != '\0') {
    char *end = NULL;
    unsigned long long val = strtoull(env, &end, 0);
    ASSERT_TRUE(end != NULL);
    ASSERT_TRUE(*end == '\0');
    g_suite_seed = (uint64_t)val;
  } else {
    struct timespec ts = {0};
    ASSERT_TRUE(clock_gettime(CLOCK_REALTIME, &ts) == 0);
    g_suite_seed = ((uint64_t)ts.tv_sec << 32) ^ ((uint64_t)ts.tv_nsec << 1) ^
                   (uint64_t)getpid();
  }

  if (g_suite_seed == 0)
    g_suite_seed = UINT64_C(0x42f09d18b0f53c27);
}

/* Generates many valid random version 1.1 catalogs, emits noisy JSON for each,
 * parses them through the real catalog loader, and compares random lookups
 * against the independent reference matcher.
 * Aborts the test process on the first mismatch.
 */
static void test_valid_random_configs_match_reference(void) {
  FuzzRng suite_rng = {.state = g_suite_seed};

  for (size_t case_ix = 0; case_ix < FUZZ_CONFIG_CASES; case_ix++) {
    uint64_t case_seed = rng_next_u64(&suite_rng);
    FuzzModel model = {0};
    fuzz_model_generate(&model, case_seed);

    char *json = fuzz_model_emit_noisy_json(&model, case_seed);
    ASSERT_TRUE(json != NULL);

    char *path = write_tmp_config(json);
    ASSERT_TRUE(path != NULL);

    char *err = NULL;
    ConnCatalog *cat = catalog_load_from_file(path, &err);
    unlink(path);
    free(path);
    if (!cat)
      fail_fuzz_case(case_seed, 0, json, NULL, ERR, NULL, ERR, NULL, err);

    ConnProfile *cp = NULL;
    ASSERT_TRUE(catalog_list(cat, &cp, 1) == 1);
    ASSERT_TRUE(cp != NULL);

    FuzzRng lookup_rng = {.state = case_seed ^ UINT64_C(0x1f3d5b79a4c2e807)};
    size_t n_lookups =
        FUZZ_LOOKUPS_MIN + rng_range(&lookup_rng, FUZZ_LOOKUPS_SPAN);
    for (size_t lookup_ix = 0; lookup_ix < n_lookups; lookup_ix++) {
      FuzzLookup lookup = {0};
      fuzz_lookup_generate(&model, &lookup, &lookup_rng);

      const char *exp_domain = NULL;
      AdbxTriStatus exp = fuzz_model_find_domain(
          &model, lookup.schema, lookup.table, lookup.column, &exp_domain);
      SensDomainOut out = {0};
      AdbxTriStatus got = connp_get_sensitive_domain(
          cp, lookup.schema, lookup.table, lookup.column, &out);

      if (got != exp ||
          (exp == YES &&
           (!out.domain || strcmp(exp_domain, out.domain) != 0))) {
        fail_fuzz_case(case_seed, lookup_ix, json, &lookup, exp, exp_domain,
                       got, out.domain, out.err.msg[0] != '\0' ? out.err.msg
                                                               : NULL);
      }

      fuzz_lookup_clean(&lookup);
    }

    catalog_destroy(cat);
    free(err);
    free(json);
    fuzz_model_clean(&model);
  }
}

/* Generates valid canonical models, injects one random cross-domain pattern
 * conflict after normalization, and asserts that catalog parsing fails with
 * the expected sensitiveDomains duplicate-pattern error.
 */
static void test_invalid_random_conflicting_patterns_reject(void) {
  FuzzRng suite_rng = {.state = g_suite_seed ^ UINT64_C(0x17c1f0bada55eedd)};

  for (size_t case_ix = 0; case_ix < FUZZ_CONFIG_CASES; case_ix++) {
    uint64_t case_seed = rng_next_u64(&suite_rng);
    FuzzModel model = {0};
    fuzz_model_generate(&model, case_seed);

    FuzzConflictKind kind = FUZZ_CONFLICT_EXACT_EQUIV;
    char *json = fuzz_model_emit_noisy_json_with_conflict(&model, case_seed,
                                                          &kind);
    ASSERT_TRUE(json != NULL);

    char *path = write_tmp_config(json);
    ASSERT_TRUE(path != NULL);

    char *err = NULL;
    ConnCatalog *cat = catalog_load_from_file(path, &err);
    unlink(path);
    free(path);

    if (cat != NULL || err == NULL ||
        strstr(err, "same normalized pattern resolves to different domains") ==
            NULL ||
        strstr(err, "sensitiveDomains") == NULL) {
      if (cat)
        catalog_destroy(cat);
      fail_invalid_fuzz_case(case_seed, kind, json, err);
    }

    free(err);
    free(json);
    fuzz_model_clean(&model);
  }
}

int main(void) {
  init_suite_seed();
  fprintf(stderr, "test_fuzz_conn_catalog suite_seed=0x%016" PRIx64 "\n",
          g_suite_seed);
  test_valid_random_configs_match_reference();
  test_invalid_random_conflicting_patterns_reject();
  fprintf(stderr, "OK: test_fuzz_conn_catalog\n");
  return 0;
}
