#include "conn_catalog.h"
#include "file_io.h"
#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>

#define CONFIG_MAX_BYTES (8u * 1024u * 1024u)
#define CONFIG_MAX_CONNECTIONS 50u

/* Lowercases an ASCII string in-place. */
static inline void str_lower_inplace(char *s) {
  if (!s)
    return;
  for (; *s; s++) {
    *s = (char)tolower((unsigned char)*s);
  }
}

/* Sets an allocated error message once.
 * Ownership: allocates *err_out once; caller must free it.
 * Side effects: heap allocation for the formatted message.
 * Error semantics: no return value; if allocation fails, *err_out remains
 * unchanged.
 */
static void set_parse_err(char **err_out, const char *fmt, ...) {
  assert(err_out);
  assert(fmt);

  if (*err_out)
    return;

  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  if (n < 0)
    return;

  size_t len = (size_t)n;
  if (len >= sizeof(buf))
    len = sizeof(buf) - 1;

  char *msg = (char *)malloc(len + 1);
  if (!msg)
    return;
  memcpy(msg, buf, len);
  msg[len] = '\0';
  *err_out = msg;
}

/* Sets an unknown-key parse error using the first offending key when present.
 * Borrows all inputs, allocates '*err_out'.
 */
static void set_parse_unknown_key_err(char **err_out, const char *path_prefix,
                                      const JsonStrSpan *unknown_key,
                                      const char *scope_suffix) {
  assert(path_prefix);
  assert(scope_suffix);

  char *decoded = NULL;
  if (unknown_key && unknown_key->ptr && unknown_key->len > 0 &&
      json_span_decode_alloc(unknown_key, &decoded) == YES && decoded &&
      decoded[0] != '\0') {
    set_parse_err(err_out, "%s: unknown key \"%s\" %s.", path_prefix, decoded,
                  scope_suffix);
    free(decoded);
    return;
  }

  free(decoded);
  set_parse_err(err_out, "%s: unknown key %s.", path_prefix, scope_suffix);
}

/* Splits a decoded sensitive-domain pattern into optional schema/table and a
 * required final column pattern. Ownership: caller owns 's' and output
 * pointers borrow slices inside it. Side effects: mutates 's' by inserting NUL
 * terminators. Returns OK on valid [schema.][table.]column input, ERR on empty
 * parts or too many qualifiers.
 */
static AdbxStatus split_sensitive_path(char *s, char **out_schema,
                                       char **out_table, char **out_col) {
  if (!s || !out_schema || !out_table || !out_col)
    return ERR;

  *out_schema = NULL;
  *out_table = NULL;
  *out_col = NULL;

  char *first = strchr(s, '.');
  if (!first) {
    if (s[0] == '\0')
      return ERR;
    *out_col = s;
    return OK;
  }

  char *second = strchr(first + 1, '.');
  if (second && strchr(second + 1, '.'))
    return ERR;

  *first = '\0';
  if (!second) {
    *out_table = s;
    *out_col = first + 1;
  } else {
    *second = '\0';
    *out_schema = s;
    *out_table = first + 1;
    *out_col = second + 1;
  }

  if ((*out_table && (*out_table)[0] == '\0') || !*out_col ||
      (*out_col)[0] == '\0')
    return ERR;
  if (*out_schema && (*out_schema)[0] == '\0')
    return ERR;
  return OK;
}

/* Normalizes one caller-owned column pattern in place. It collapse repeated '*'
 * so logically equivalent globs compare equal. Returns OK on success, ERR when
 * schema/table wildcards leak here or inputs are invalid. On success,
 * '*out_star_count' and '*out_literal_len' describe the normalized pattern.
 */
static AdbxStatus normalize_column_pattern(char *column_pat,
                                           uint16_t *out_star_count,
                                           uint16_t *out_literal_len) {
  if (!column_pat || !out_star_count || !out_literal_len)
    return ERR;

  uint16_t star_count = 0;
  uint16_t literal_len = 0;
  size_t w = 0;
  int prev_star = 0;

  for (size_t r = 0; column_pat[r] != '\0'; r++) {
    char c = column_pat[r];
    if (c == '.') {
      return ERR;
    }
    if (c == '*') {
      if (prev_star)
        continue;
      column_pat[w++] = c;
      star_count++;
      prev_star = 1;
      continue;
    }

    column_pat[w++] = c;
    literal_len++;
    prev_star = 0;
  }

  if (w == 0)
    return ERR;

  column_pat[w] = '\0';
  *out_star_count = star_count;
  *out_literal_len = literal_len;
  return OK;
}

/* Maps one sensitive rule to its precedence bucket.
 * Returns a stable rank in precedence order, or -1 on invalid rule shape.
 * The order is:
 *  0. exact schema.table.column
 *  1. glob schema.table.col_pattern
 *  2. exact table.column
 *  3. glob table.col_pattern
 *  4. exact column
 *  5. glob col_pattern
 */
static int sensitive_rule_rank(const SensitiveRule *rule) {
  if (!rule || !rule->column_pat)
    return -1;
  if (rule->schema && !rule->table)
    return -1;

  if (rule->schema)
    return (rule->star_count == 0) ? 0 : 1;
  if (rule->table)
    return (rule->star_count == 0) ? 2 : 3;
  return (rule->star_count == 0) ? 4 : 5;
}

/* Compares optional strings lexicographically with NULL ordered first.
 * Returns <0 / 0 / >0 like strcmp().
 */
static int nullable_str_cmp(const char *a, const char *b) {
  if (!a && !b)
    return 0;
  if (!a)
    return -1;
  if (!b)
    return 1;
  return strcmp(a, b);
}

/* Compares sensitive rules in deterministic match precedence order.
 * Returns <0 / 0 / >0 for qsort-style ordering.
 */
static int sensitive_rule_cmp(const void *a, const void *b) {
  const SensitiveRule *ra = (const SensitiveRule *)a;
  const SensitiveRule *rb = (const SensitiveRule *)b;

  int rka = sensitive_rule_rank(ra);
  int rkb = sensitive_rule_rank(rb);
  if (rka != rkb) {
    assert(rka >= 0);
    assert(rkb >= 0);

    return (rka < rkb) ? -1 : 1;
  }

  int sc = nullable_str_cmp(ra->schema, rb->schema);
  if (sc != 0)
    return sc;
  int tc = nullable_str_cmp(ra->table, rb->table);
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

/* Checks whether two sensitive rules refer to the same normalized match
 * pattern, ignoring domain.
 * Returns YES when the two rules would match the same identifiers, NO when
 * different, ERR on invalid input.
 */
static AdbxTriStatus sensitive_rule_same_pattern(const SensitiveRule *a,
                                                 const SensitiveRule *b) {
  if (!a || !b)
    return ERR;
  if (sensitive_rule_rank(a) != sensitive_rule_rank(b))
    return NO;
  if (nullable_str_cmp(a->schema, b->schema) != 0)
    return NO;
  if (nullable_str_cmp(a->table, b->table) != 0)
    return NO;
  if (strcmp(a->column_pat, b->column_pat) != 0)
    return NO;
  return YES;
}

/* Assigns one contiguous rank slice to the corresponding policy bucket.
 * Ownership: bucket borrows from 'pol->storage'. Side effects: mutates bucket
 * metadata in '*pol'. Returns OK on success, ERR on invalid rank/input.
 */
static AdbxStatus sensitive_policy_set_bucket(SensitiveDomainPolicy *pol,
                                              int rank, size_t start,
                                              size_t count) {
  if (!pol)
    return ERR;

  SensitiveRuleBucket *bucket = NULL;
  switch (rank) {
  case 0:
    bucket = &pol->exact_stc;
    break;
  case 1:
    bucket = &pol->glob_stc;
    break;
  case 2:
    bucket = &pol->exact_tc;
    break;
  case 3:
    bucket = &pol->glob_tc;
    break;
  case 4:
    bucket = &pol->exact_c;
    break;
  case 5:
    bucket = &pol->glob_c;
    break;
  default:
    return ERR;
  }

  bucket->rules = (count == 0 || !pol->storage) ? NULL : &pol->storage[start];
  bucket->n_rules = count;
  return OK;
}

/* Sorts, deduplicates, and slices one sensitive-domain policy in place.
 * Mutates caller-owned '*pol' only. Returns OK on success, ERR on invalid
 * state or conflicting normalized patterns that resolve to different domains.
 */
static AdbxStatus sensitive_policy_finalize(SensitiveDomainPolicy *pol,
                                            const char *path_prefix,
                                            char **err_out) {
  if (!pol || (!pol->storage && pol->n_storage != 0))
    return ERR;

  pol->exact_stc = (SensitiveRuleBucket){0};
  pol->glob_stc = (SensitiveRuleBucket){0};
  pol->exact_tc = (SensitiveRuleBucket){0};
  pol->glob_tc = (SensitiveRuleBucket){0};
  pol->exact_c = (SensitiveRuleBucket){0};
  pol->glob_c = (SensitiveRuleBucket){0};

  if (pol->n_storage == 0)
    return OK;

  qsort(pol->storage, pol->n_storage, sizeof(*pol->storage),
        sensitive_rule_cmp);

  // dedup
  size_t out_ix = 0;
  for (size_t i = 0; i < pol->n_storage;) {
    size_t j = i + 1;
    while (j < pol->n_storage &&
           sensitive_rule_same_pattern(&pol->storage[i], &pol->storage[j]) ==
               YES) {
      if (strcmp(pol->storage[i].domain, pol->storage[j].domain) != 0) {
        // TODO: log the wrong normalized pattern
        set_parse_err(
            err_out,
            "%s.sensitiveDomains: the same normalized pattern resolves to "
            "different domains.",
            path_prefix);
        return ERR;
      }
      j++;
    }
    pol->storage[out_ix++] = pol->storage[i];
    i = j;
  }
  pol->n_storage = out_ix;

  for (size_t i = 0; i < pol->n_storage;) {
    int rank = sensitive_rule_rank(&pol->storage[i]);
    if (rank < 0)
      return ERR;
    size_t j = i + 1;
    while (j < pol->n_storage &&
           sensitive_rule_rank(&pol->storage[j]) == rank) {
      j++;
    }
    if (sensitive_policy_set_bucket(pol, rank, i, j - i) != OK)
      return ERR;
    i = j;
  }
  return OK;
}

typedef struct {
  char *schema; // NULL for global rules
  char *name;
} SafeFuncRuleTmp;

/* Compares SafeFuncRuleTmp entries by name, schema (NULL first). */
static int saferuletmp_cmp(const void *a, const void *b) {
  const SafeFuncRuleTmp *ra = (const SafeFuncRuleTmp *)a;
  const SafeFuncRuleTmp *rb = (const SafeFuncRuleTmp *)b;

  int nc = strcmp(ra->name, rb->name);
  if (nc != 0)
    return nc;

  if (!ra->schema && !rb->schema)
    return 0;
  if (!ra->schema)
    return -1;
  if (!rb->schema)
    return 1;
  return strcmp(ra->schema, rb->schema);
}

static AdbxStatus split_func_path(char *input, char **out_schema,
                                  char **out_name) {
  if (!input || !out_schema || !out_name)
    return ERR;
  *out_schema = NULL;
  *out_name = NULL;

  char *dot = strchr(input, '.');
  if (!dot) {
    if (input[0] == '\0')
      return ERR;
    *out_name = input;
    return OK;
  }

  if (dot == input)
    return ERR;
  *dot = '\0';
  char *schema = input;
  char *name = dot + 1;
  if (schema[0] == '\0' || name[0] == '\0')
    return ERR;
  if (strchr(name, '.'))
    return ERR;
  *out_schema = schema;
  *out_name = name;
  return OK;
}

/* Parses sensitiveDomains into one sorted SensitiveDomainPolicy.
 * Ownership: stores all strings and rule storage in out->sens_policy.arena.
 * Returns OK on success, ERR on malformed patterns, conflicting normalized
 * rules, or allocation failure.
 */
static AdbxStatus parse_sensitive_domains(const JsonGetter *jg,
                                          ConnProfile *out,
                                          const char *path_prefix,
                                          char **err_out) {
  if (!jg || !out || !path_prefix)
    return ERR;

  JsonGetter domains_obj = {0};
  AdbxTriStatus orc = jsget_object(jg, "sensitiveDomains", &domains_obj);
  if (orc == NO)
    return OK;
  if (orc != YES) {
    set_parse_err(err_out, "%s.sensitiveDomains: expected object.",
                  path_prefix);
    return ERR;
  }

  size_t n_rules = 0;
  JsonObjIter oit = {0};
  if (jsget_object_members_begin(&domains_obj, NULL, &oit) != YES)
    return OK;

  // we first allocate the whole array of SensitiveRule at the start of the
  // arena, then assign values to each of the rule.

  // count how many SensitiveRule to allocate
  for (;;) {
    JsonStrSpan domain_key = {0};
    JsonGetter arr_view = {0};
    AdbxTriStatus rc =
        jsget_object_members_next(&domains_obj, &oit, &domain_key, &arr_view);
    if (rc == NO)
      break;
    if (rc != YES) {
      set_parse_err(err_out, "%s.sensitiveDomains: malformed object member.",
                    path_prefix);
      return ERR;
    }

    JsonArrIter ait = {0};
    AdbxTriStatus arc = jsget_array_strings_begin(&arr_view, NULL, &ait);
    if (arc != YES) {
      set_parse_err(err_out,
                    "%s.sensitiveDomains: each domain value must be an array "
                    "of strings.",
                    path_prefix);
      return ERR;
    }
    if ((size_t)ait.count > (SIZE_MAX - n_rules)) {
      set_parse_err(err_out, "%s.sensitiveDomains: too many entries.",
                    path_prefix);
      return ERR;
    }
    n_rules += (size_t)ait.count;
  }

  if (n_rules == 0)
    return OK;
  if (n_rules > (SIZE_MAX / sizeof(SensitiveRule)) ||
      n_rules > (size_t)(UINT32_MAX / sizeof(SensitiveRule))) {
    set_parse_err(err_out, "%s.sensitiveDomains: too many entries.",
                  path_prefix);
    return ERR;
  }

  if (arena_init(&out->sens_policy.arena, NULL, NULL) != OK) {
    set_parse_err(err_out, "%s.sensitiveDomains: internal allocation error.",
                  path_prefix);
    return ERR;
  }

  out->sens_policy.storage = (SensitiveRule *)arena_alloc(
      &out->sens_policy.arena, (uint32_t)(n_rules * sizeof(SensitiveRule)));
  if (!out->sens_policy.storage) {
    set_parse_err(err_out, "%s.sensitiveDomains: internal allocation error.",
                  path_prefix);
    goto error;
  }
  out->sens_policy.n_storage = n_rules;

  if (jsget_object_members_begin(&domains_obj, NULL, &oit) != YES) {
    set_parse_err(err_out, "%s.sensitiveDomains: internal iteration error.",
                  path_prefix);
    goto error;
  }

  // Loop over each object inside sensitiveDomains, allocate its string
  // identifier inside the arena, then loop over each array element of that
  // object and assign the values to the SensitiveRule
  size_t rule_ix = 0;
  for (;;) {
    JsonStrSpan domain_key = {0};
    JsonGetter arr_view = {0};
    AdbxTriStatus rc =
        jsget_object_members_next(&domains_obj, &oit, &domain_key, &arr_view);
    if (rc == NO)
      break;
    if (rc != YES) {
      set_parse_err(err_out, "%s.sensitiveDomains: malformed object member.",
                    path_prefix);
      goto error;
    }

    char *decoded_domain = NULL;
    if (json_span_decode_alloc(&domain_key, &decoded_domain) != YES ||
        !decoded_domain) {
      set_parse_err(err_out,
                    "%s.sensitiveDomains: failed to decode domain key.",
                    path_prefix);
      free(decoded_domain);
      goto error;
    }
    str_lower_inplace(decoded_domain);
    if (decoded_domain[0] == '\0') {
      set_parse_err(err_out,
                    "%s.sensitiveDomains: domain names must not be empty.",
                    path_prefix);
      free(decoded_domain);
      goto error;
    }

    char *domain_ar =
        (char *)arena_add_nul(&out->sens_policy.arena, decoded_domain,
                              (uint32_t)strlen(decoded_domain));
    free(decoded_domain);
    if (!domain_ar) {
      set_parse_err(err_out, "%s.sensitiveDomains: internal allocation error.",
                    path_prefix);
      goto error;
    }

    JsonArrIter ait = {0};
    AdbxTriStatus arc = jsget_array_strings_begin(&arr_view, NULL, &ait);
    if (arc != YES) {
      set_parse_err(err_out,
                    "%s.sensitiveDomains: each domain value must be an array "
                    "of strings.",
                    path_prefix);
      goto error;
    }

    for (;;) {
      JsonStrSpan sp = {0};
      arc = jsget_array_strings_next(&arr_view, &ait, &sp);
      if (arc == NO)
        break;
      if (arc != YES) {
        set_parse_err(
            err_out,
            "%s.sensitiveDomains: each domain value must contain only strings.",
            path_prefix);
        goto error;
      }

      char *decoded_pat = NULL;
      if (json_span_decode_alloc(&sp, &decoded_pat) != YES || !decoded_pat) {
        set_parse_err(err_out,
                      "%s.sensitiveDomains: failed to decode string entry.",
                      path_prefix);
        free(decoded_pat);
        goto error;
      }

      str_lower_inplace(decoded_pat);

      // we first add the string to the arena, then mutate it with split_*
      char *pat_ar = (char *)arena_add_nul(&out->sens_policy.arena, decoded_pat,
                                           (uint32_t)strlen(decoded_pat));
      free(decoded_pat);
      if (!pat_ar) {
        set_parse_err(err_out,
                      "%s.sensitiveDomains: internal allocation error.",
                      path_prefix);
        goto error;
      }

      char *schema = NULL;
      char *table = NULL;
      char *column_pat = NULL;
      if (split_sensitive_path(pat_ar, &schema, &table, &column_pat) != OK) {
        set_parse_err(err_out,
                      "%s.sensitiveDomains[]: expected [schema.][table.]"
                      "column with '*' only in the column segment.",
                      path_prefix);
        goto error;
      }
      if ((schema && strchr(schema, '*')) || (table && strchr(table, '*'))) {
        set_parse_err(err_out,
                      "%s.sensitiveDomains[]: '*' is allowed only in the "
                      "column segment.",
                      path_prefix);
        goto error;
      }

      uint16_t star_count = 0;
      uint16_t literal_len = 0;
      if (normalize_column_pattern(column_pat, &star_count, &literal_len) !=
          OK) {
        set_parse_err(err_out, "%s.sensitiveDomains[]: invalid column pattern.",
                      path_prefix);
        free(decoded_pat);
        goto error;
      }

      SensitiveRule *rule = &out->sens_policy.storage[rule_ix++];
      rule->schema = schema;
      rule->table = table;
      rule->column_pat = column_pat;
      rule->domain =
          domain_ar; // we allocate just one string per sensitiveDomains object
      rule->star_count = star_count;
      rule->literal_len = literal_len;
    }
  }

  if (rule_ix != n_rules) {
    set_parse_err(err_out, "%s.sensitiveDomains: internal counting mismatch.",
                  path_prefix);
    goto error;
  }

  out->sens_policy.n_storage = rule_ix;
  if (sensitive_policy_finalize(&out->sens_policy, path_prefix, err_out) != OK)
    goto error;
  return OK;

error:
  arena_clean(&out->sens_policy.arena);
  memset(&out->sens_policy, 0, sizeof(out->sens_policy));
  return ERR;
}

/* Parses safeFunctions into SafeFunctionRule list.
 * Ownership: stores all strings and arrays in out->safe_funcs.arena.
 * Side effects: allocates temporary heap lists and arena-backed rule storage.
 * Error semantics: returns OK on success, ERR on malformed entries, allocation
 * failures, or invalid input.
 */
static AdbxStatus parse_safe_functions(const JsonGetter *jg, ConnProfile *out,
                                       const char *path_prefix,
                                       char **err_out) {
  if (!jg || !out || !path_prefix)
    return ERR;

  JsonArrIter it;
  AdbxTriStatus rc = jsget_array_strings_begin(jg, "safeFunctions", &it);
  if (rc == NO)
    return OK;
  if (rc != YES) {
    set_parse_err(err_out, "%s.safeFunctions: expected an array of strings.",
                  path_prefix);
    return ERR;
  }

  if (arena_init(&out->safe_funcs.arena, NULL, NULL) != OK) {
    set_parse_err(err_out, "%s.safeFunctions: internal allocation error.",
                  path_prefix);
    return ERR;
  }

  SafeFuncRuleTmp *tmp = NULL;
  size_t tmp_len = 0;
  size_t tmp_cap = 0;

  // same logic as the loop we use to parse sensitive columns
  for (;;) {
    JsonStrSpan sp = {0};
    rc = jsget_array_strings_next(jg, &it, &sp);
    if (rc == NO)
      break;
    if (rc != YES) {
      set_parse_err(err_out, "%s.safeFunctions: expected string entries.",
                    path_prefix);
      goto error;
    }

    char *decoded = NULL;
    if (json_span_decode_alloc(&sp, &decoded) != YES) {
      set_parse_err(err_out, "%s.safeFunctions: failed to decode string entry.",
                    path_prefix);
      goto error;
    }

    char *schema = NULL;
    char *name = NULL;
    if (split_func_path(decoded, &schema, &name) != OK) {
      free(decoded);
      set_parse_err(err_out, "%s.safeFunctions[]: expected [schema.]function.",
                    path_prefix);
      goto error;
    }

    str_lower_inplace(schema);
    str_lower_inplace(name);

    if (tmp_len == tmp_cap) {
      size_t nc = (tmp_cap == 0) ? 8 : tmp_cap * 2;
      SafeFuncRuleTmp *nt = (SafeFuncRuleTmp *)xrealloc(tmp, nc * sizeof(*tmp));
      tmp = nt;
      tmp_cap = nc;
    }
    tmp[tmp_len].schema = schema ? strdup(schema) : NULL;
    tmp[tmp_len].name = strdup(name);
    if (!tmp[tmp_len].name || (schema && !tmp[tmp_len].schema)) {
      free(decoded);
      set_parse_err(err_out, "%s.safeFunctions: internal allocation error.",
                    path_prefix);
      goto error;
    }
    tmp_len++;

    free(decoded);
  }

  if (tmp_len == 0)
    return OK;

  qsort(tmp, tmp_len, sizeof(*tmp), saferuletmp_cmp);

  // find unique function names
  size_t uniq = 0;
  for (size_t i = 0; i < tmp_len; i++) {
    if (i == 0 || strcmp(tmp[i].name, tmp[i - 1].name) != 0)
      uniq++;
  }

  out->safe_funcs.rules = (SafeFunctionRule *)arena_calloc(
      &out->safe_funcs.arena, (uint32_t)(uniq * sizeof(SafeFunctionRule)));
  if (!out->safe_funcs.rules)
    goto error;
  out->safe_funcs.n_rules = uniq;

  size_t ri = 0;
  size_t i = 0;
  while (i < tmp_len) {
    char *name = tmp[i].name;

    size_t j = i;
    size_t scount = 0;
    int is_global = 0;
    const char *last_schema = NULL;
    while (j < tmp_len && strcmp(tmp[j].name, name) == 0) {
      if (!tmp[j].schema) {
        is_global = 1;
      } else if (!last_schema || strcmp(tmp[j].schema, last_schema) != 0) {
        scount++;
        last_schema = tmp[j].schema;
      }
      j++;
    }

    SafeFunctionRule *r = &out->safe_funcs.rules[ri++];
    r->name = (const char *)arena_add_nul(&out->safe_funcs.arena, name,
                                          (uint32_t)strlen(name));
    if (!r->name)
      goto error;
    r->is_global = is_global;
    r->n_schemas = (uint32_t)scount;
    if (scount == 0) {
      r->schemas = NULL;
    } else {
      r->schemas = (const char **)arena_calloc(
          &out->safe_funcs.arena, (uint32_t)(scount * sizeof(char *)));
      if (!r->schemas)
        goto error;
      size_t k = 0;
      const char *prev = NULL;
      for (size_t t = i; t < j; t++) {
        if (!tmp[t].schema)
          continue;
        if (prev && strcmp(prev, tmp[t].schema) == 0)
          continue;
        r->schemas[k++] =
            (const char *)arena_add_nul(&out->safe_funcs.arena, tmp[t].schema,
                                        (uint32_t)strlen(tmp[t].schema));
        if (!r->schemas[k - 1])
          goto error;
        prev = tmp[t].schema;
      }
    }

    i = j;
  }

  for (size_t t = 0; t < tmp_len; t++) {
    free(tmp[t].schema);
    free(tmp[t].name);
  }
  free(tmp);
  return OK;

error:
  set_parse_err(err_out, "%s.safeFunctions: invalid entry.", path_prefix);
  if (tmp) {
    for (size_t t = 0; t < tmp_len; t++) {
      free(tmp[t].schema);
      free(tmp[t].name);
    }
  }
  free(tmp);
  arena_clean(&out->safe_funcs.arena);
  out->safe_funcs.rules = NULL;
  out->safe_funcs.n_rules = 0;
  return ERR;
}

/* Parses one safetyPolicy object and merges parsed values into '*out'.
 * Ownership: borrows all inputs and mutates caller-owned '*out'.
 * Side effects: none beyond writing '*out' and optional allocated error string.
 * Error semantics: returns OK on valid policy object, ERR on malformed values
 * or unknown keys. On ERR, it sets a descriptive allocated message in
 * '*err_out' when provided.
 */
static AdbxStatus parse_policy(const JsonGetter *jg, SafetyPolicy *out,
                               const char *path_prefix, char **err_out) {
  if (!jg || !out || !path_prefix)
    return ERR;

  const char *const keys[] = {"readOnly", "statementTimeoutMs",
                              "maxRowReturned", "maxPayloadKiloBytes",
                              "columnPolicy"};
  JsonStrSpan unknown = {0};
  if (jsget_top_level_validation(jg, NULL, keys, ARRLEN(keys), &unknown) !=
      YES) {
    set_parse_unknown_key_err(err_out, path_prefix, &unknown, "in object");
    return ERR;
  }

  JsonStrSpan ro = {0};
  AdbxTriStatus rrc = jsget_string_span(jg, "readOnly", &ro);
  if (rrc == ERR) {
    set_parse_err(err_out, "%s.readOnly: expected string.", path_prefix);
    return ERR;
  }
  if (rrc == YES) {
    if (ro.len == 3 && strncasecmp(ro.ptr, "yes", 3) == 0) {
      out->read_only = 1;
    } else if (ro.len == 9 && strncasecmp(ro.ptr, "no unsafe", 9) == 0) {
      out->read_only = 0;
    } else {
      set_parse_err(err_out, "%s.readOnly: expected \"yes\" or \"no unsafe\".",
                    path_prefix);
      return ERR;
    }
  }

  uint32_t timeout_ms = 0;
  AdbxTriStatus trc = jsget_u32(jg, "statementTimeoutMs", &timeout_ms);
  if (trc == ERR) {
    set_parse_err(err_out, "%s.statementTimeoutMs: expected uint32.",
                  path_prefix);
    return ERR;
  }
  if (trc == YES)
    out->statement_timeout_ms = timeout_ms;

  uint32_t max_rows = 0;
  AdbxTriStatus mrc = jsget_u32(jg, "maxRowReturned", &max_rows);
  if (mrc == ERR) {
    set_parse_err(err_out, "%s.maxRowReturned: expected uint32.", path_prefix);
    return ERR;
  }
  if (mrc == YES)
    out->max_rows = max_rows;

  uint32_t max_payload_kb = 0;
  AdbxTriStatus qrc = jsget_u32(jg, "maxPayloadKiloBytes", &max_payload_kb);
  if (qrc == ERR) {
    set_parse_err(err_out, "%s.maxPayloadKiloBytes: expected uint32.",
                  path_prefix);
    return ERR;
  }
  if (qrc == YES) {
    if (max_payload_kb > (UINT32_MAX / 1024u)) {
      set_parse_err(err_out, "%s.maxPayloadKiloBytes: value too large.",
                    path_prefix);
      return ERR;
    }
    out->max_payload_bytes = max_payload_kb * 1024u;
  }

  JsonGetter col = {0};
  AdbxTriStatus crc = jsget_object(jg, "columnPolicy", &col);
  if (crc == ERR) {
    set_parse_err(err_out, "%s.columnPolicy: expected object.", path_prefix);
    return ERR;
  }
  if (crc == YES) {
    const char *const ckeys[] = {"mode", "strategy"};
    JsonStrSpan c_unknown = {0};
    if (jsget_top_level_validation(&col, NULL, ckeys, ARRLEN(ckeys),
                                   &c_unknown) != YES) {
      char cpol_path[96];
      snprintf(cpol_path, sizeof(cpol_path), "%s.columnPolicy", path_prefix);
      set_parse_unknown_key_err(err_out, cpol_path, &c_unknown, "in object");
      return ERR;
    }

    JsonStrSpan mode = {0};
    AdbxTriStatus mrc2 = jsget_string_span(&col, "mode", &mode);
    if (mrc2 != YES) {
      set_parse_err(err_out, "%s.columnPolicy.mode: expected \"pseudonymize\".",
                    path_prefix);
      return ERR;
    }
    if (!(mode.len == strlen("pseudonymize") &&
          strncasecmp(mode.ptr, "pseudonymize", mode.len) == 0)) {
      set_parse_err(err_out, "%s.columnPolicy.mode: expected \"pseudonymize\".",
                    path_prefix);
      return ERR;
    }
    out->column_mode = SAFETY_COLMODE_PSEUDONYMIZE;

    JsonStrSpan strat = {0};
    AdbxTriStatus src = jsget_string_span(&col, "strategy", &strat);
    if (src != YES) {
      set_parse_err(err_out,
                    "%s.columnPolicy.strategy: expected \"deterministic\" or "
                    "\"randomized\".",
                    path_prefix);
      return ERR;
    }

    if (strat.len == strlen("deterministic") &&
        strncasecmp(strat.ptr, "deterministic", strat.len) == 0) {
      out->column_strategy = SAFETY_COLSTRAT_DETERMINISTIC;
    } else if (strat.len == strlen("randomized") &&
               strncasecmp(strat.ptr, "randomized", strat.len) == 0) {
      out->column_strategy = SAFETY_COLSTRAT_RANDOMIZED;
    } else {
      set_parse_err(err_out,
                    "%s.columnPolicy.strategy: expected \"deterministic\" or "
                    "\"randomized\".",
                    path_prefix);
      return ERR;
    }
  }

  return OK;
}

static void profile_clean(ConnProfile *p) {
  if (!p)
    return;
  free((char *)p->connection_name);
  free((char *)p->host);
  free((char *)p->db_name);
  free((char *)p->user);
  free((char *)p->options);
  p->secret_ref.cred_namespace = NULL;
  p->secret_ref.connection_name = NULL;
  arena_clean(&p->sens_policy.arena);
  arena_clean(&p->safe_funcs.arena);
}

/* Parses the required top-level credential namespace string.
 * It borrows 'jg' and allocates one owned string for 'cat'.
 * Side effects: allocates heap memory for 'cat->credential_namespace'.
 * Error semantics: returns OK on success, ERR on missing, empty, or malformed
 * input.
 */
static AdbxStatus parse_credential_namespace(const JsonGetter *jg,
                                             ConnCatalog *cat, char **err_out) {
  if (!jg || !cat)
    return ERR;

  char *ns = NULL;
  if (jsget_string_decode_alloc(jg, "configNamespace", &ns) != YES) {
    set_parse_err(err_out, "$.configNamespace: expected string.");
    return ERR;
  }
  if (ns[0] == '\0') {
    free(ns);
    set_parse_err(err_out, "$.configNamespace: must not be empty.");
    return ERR;
  }

  if (strlen(ns) > NAMESPACE_MAX_LEN) {
    free(ns);
    set_parse_err(err_out, "$.configNamespace: must be at most %d bytes.",
                  NAMESPACE_MAX_LEN);
    return ERR;
  }

  cat->credential_namespace = ns;
  return OK;
}

/* Parses one databases[i] object into 'out'. This will clean 'out' if something
 * goes wrong. Ownership: writes owned strings and policy arenas into
 * caller-owned 'out'. Side effects: heap and arena allocations. Error
 * semantics: returns OK on valid entry, ERR on malformed fields or allocation
 * failures.
 */
static AdbxStatus parse_db_entry(ConnCatalog *cat, const JsonGetter *jg,
                                 ConnProfile *out, size_t db_index,
                                 char **err_out) {
  if (!cat || !jg || !out)
    return ERR;

  char db_path[64];
  snprintf(db_path, sizeof(db_path), "$.databases[%zu]", db_index);

  const char *const keys[] = {
      "type",          "connectionName", "host",    "port",
      "username",      "database",       "options", "sensitiveDomains",
      "safeFunctions", "safetyPolicy"};
  JsonStrSpan unknown = {0};
  AdbxTriStatus vrc = jsget_top_level_validation(jg, NULL, keys, ARRLEN(keys),
                                                 &unknown);
  if (vrc != YES) {
    set_parse_unknown_key_err(err_out, db_path, &unknown, "in database entry");
    return ERR;
  }

  char *type = NULL;
  char *conn_name = NULL;
  char *host = NULL;
  char *user = NULL;
  char *db_name = NULL;
  char *options = NULL;

  if (jsget_string_decode_alloc(jg, "type", &type) != YES) {
    set_parse_err(err_out, "%s.type: expected string.", db_path);
    goto error;
  }
  if (strcmp(type, "postgres") != 0) {
    set_parse_err(err_out, "%s.type: only \"postgres\" is supported.", db_path);
    goto error;
  }

  if (jsget_string_decode_alloc(jg, "connectionName", &conn_name) != YES) {
    set_parse_err(err_out, "%s.connectionName: expected string.", db_path);
    goto error;
  }
  if (strlen(conn_name) > CONN_NAME_MAX_LEN) {
    set_parse_err(err_out, "%s.connectionName: must be at most %d bytes.",
                  db_path, CONN_NAME_MAX_LEN);
    goto error;
  }
  if (jsget_string_decode_alloc(jg, "host", &host) != YES) {
    set_parse_err(err_out, "%s.host: expected string.", db_path);
    goto error;
  }

  uint32_t port = 0;
  if (jsget_u32(jg, "port", &port) != YES || port > UINT16_MAX) {
    set_parse_err(err_out, "%s.port: expected uint16.", db_path);
    goto error;
  }

  if (jsget_string_decode_alloc(jg, "username", &user) != YES) {
    set_parse_err(err_out, "%s.username: expected string.", db_path);
    goto error;
  }
  if (jsget_string_decode_alloc(jg, "database", &db_name) != YES) {
    set_parse_err(err_out, "%s.database: expected string.", db_path);
    goto error;
  }

  AdbxTriStatus orc = jsget_string_decode_alloc(jg, "options", &options);
  if (orc == ERR) {
    set_parse_err(err_out, "%s.options: expected string when present.",
                  db_path);
    goto error;
  }
  if (orc == NO)
    options = NULL;

  free(type);
  type = NULL;

  out->connection_name = conn_name;
  out->secret_ref.cred_namespace = cat->credential_namespace;
  out->secret_ref.connection_name = conn_name;
  out->kind = DB_KIND_POSTGRES;
  out->host = host;
  out->port = (uint16_t)port;
  out->db_name = db_name;
  out->user = user;
  out->options = options;

  out->safe_policy = cat->policy;
  JsonGetter db_pol = {0};
  AdbxTriStatus prc = jsget_object(jg, "safetyPolicy", &db_pol);
  if (prc == ERR) {
    set_parse_err(err_out, "%s.safetyPolicy: expected object.", db_path);
    goto error;
  }
  if (prc == YES) {
    char sp_path[96];
    snprintf(sp_path, sizeof(sp_path), "%s.safetyPolicy", db_path);
    if (parse_policy(&db_pol, &out->safe_policy, sp_path, err_out) != OK)
      goto error;
  }

  if (parse_sensitive_domains(jg, out, db_path, err_out) != OK)
    goto error;
  if (parse_safe_functions(jg, out, db_path, err_out) != OK)
    goto error;
  return OK;

error:
  profile_clean(out);

  // make it safe for caller to call free if ERR
  out->connection_name = NULL;
  out->secret_ref.cred_namespace = NULL;
  out->secret_ref.connection_name = NULL;
  out->db_name = NULL;
  out->host = NULL;
  out->options = NULL;
  out->user = NULL;
  memset(&out->sens_policy, 0, sizeof(out->sens_policy));
  memset(&out->safe_funcs, 0, sizeof(out->safe_funcs));
  return ERR;
}

/* Parses the "databases" array and allocates ConnProfile entries inside 'cat'.
 * Ownership: writes owned profile array into '*cat'.
 * Side effects: allocates heap memory for profiles and nested arenas.
 * Error semantics: returns OK on valid non-empty array, ERR otherwise.
 */
static AdbxStatus parse_databases(const JsonGetter *jg, ConnCatalog *cat,
                                  char **err_out) {
  if (!jg || !cat)
    return ERR;

  JsonArrIter it;
  AdbxTriStatus rc = jsget_array_objects_begin(jg, "databases", &it);
  if (rc != YES) {
    set_parse_err(err_out, "$.databases: expected array of objects.");
    return ERR;
  }

  if (it.count <= 0) {
    set_parse_err(err_out, "$.databases: at least one entry is required.");
    return ERR;
  }

  if (it.count < 0 || (size_t)it.count > CONFIG_MAX_CONNECTIONS) {
    set_parse_err(
        err_out,
        "$.databases: too many entries (exceeds configured connection cap).");
    return ERR;
  }

  size_t n = (size_t)it.count;
  ConnProfile *profiles = NULL;
  if (n > 0) {
    // zero-init so cleanup can safely free partially parsed entries
    profiles = (ConnProfile *)xcalloc(n, sizeof(*profiles));
  }

  size_t idx = 0;
  for (;;) {
    JsonGetter entry = {0};
    rc = jsget_array_objects_next(jg, &it, &entry);
    if (rc == NO)
      break;
    if (rc != YES)
      goto error;

    if (parse_db_entry(cat, &entry, &profiles[idx], idx, err_out) != OK)
      goto error;

    // connectionName must be unique
    for (size_t j = 0; j < idx; j++) {
      if (strcasecmp(profiles[idx].connection_name,
                     profiles[j].connection_name) == 0) {
        set_parse_err(
            err_out,
            "$.databases: duplicate connectionName (case-insensitive).");
        goto error;
      }
    }

    idx++;
  }

  cat->profiles = profiles;
  cat->n_profiles = idx;
  return OK;

error:
  for (; n > 0; n--) {
    profile_clean(&profiles[n - 1]);
  }
  free(profiles);
  return ERR;
}

/* Validates the top-level catalog version string.
 * Ownership: borrows 'jg' and allocates one temporary decoded string.
 * Error semantics: returns YES on the supported version, NO on missing or
 * unsupported value, ERR on malformed input or invalid arguments.
 */
static AdbxTriStatus parse_version(const JsonGetter *jg) {
  if (!jg)
    return ERR;

  char *ver = NULL;
  AdbxTriStatus rc = jsget_string_decode_alloc(jg, "version", &ver);
  if (rc == NO)
    return NO;
  if (rc != YES)
    return ERR;

  AdbxTriStatus ok =
      (strcmp(ver, CURR_CONN_CAT_VERSION) == 0) ? YES : NO;
  free(ver);
  return ok;
}

/* Parses one full config document already loaded in memory.
 * It borrows 'data' and allocates one catalog owned by caller.
 * Side effects: heap and arena allocations while building the catalog.
 * Error semantics: returns a populated catalog on success, NULL on parse or
 * allocation failure and sets '*err_out' when provided.
 */
static ConnCatalog *catalog_parse_config_bytes(const char *data, size_t len,
                                               char **err_out) {
  char *err_msg = NULL;
  ConnCatalog *cat = NULL;

  if (err_out)
    *err_out = NULL;

  JsonGetter jg;
  JsonTokBuf tok_buf = {0};
  if (jsget_init(&jg, data, len, &tok_buf) != OK) {
    set_parse_err(&err_msg, "$: invalid JSON.");
    goto error;
  }

  // make sure these 2 objects are present in the config file
  const char *const root_keys[] = {"version", "configNamespace", "safetyPolicy",
                                   "databases"};
  JsonStrSpan root_unknown = {0};
  if (jsget_top_level_validation(&jg, NULL, root_keys, ARRLEN(root_keys),
                                 &root_unknown) != YES) {
    set_parse_unknown_key_err(&err_msg, "$", &root_unknown, "at top level");
    goto error;
  }

  AdbxTriStatus vrc = parse_version(&jg);
  if (vrc == NO) {
    set_parse_err(&err_msg, "$.version: missing or unsupported value.");
    goto error;
  }
  if (vrc != YES) {
    set_parse_err(&err_msg, "$.version: invalid string.");
    goto error;
  }

  cat = xcalloc(1, sizeof(*cat));
  if (safety_policy_init(&cat->policy, NULL, NULL, NULL, NULL) != OK) {
    set_parse_err(&err_msg, "$.safetyPolicy: failed to initialize defaults.");
    goto error;
  }

  if (parse_credential_namespace(&jg, cat, &err_msg) != OK)
    goto error;

  JsonGetter policy_obj = {0};
  if (jsget_object(&jg, "safetyPolicy", &policy_obj) != YES) {
    set_parse_err(&err_msg, "$.safetyPolicy: expected object.");
    goto error;
  }
  if (parse_policy(&policy_obj, &cat->policy, "$.safetyPolicy", &err_msg) !=
      OK) {
    goto error;
  }

  if (parse_databases(&jg, cat, &err_msg) != OK) {
    goto error;
  }

  return cat;

error:
  catalog_destroy(cat);
  if (!err_msg)
    set_parse_err(&err_msg, "$: invalid configuration.");
  if (err_out) {
    *err_out = err_msg;
  } else {
    free(err_msg);
  }
  return NULL;
}

ConnCatalog *catalog_load_from_fd(int fd, char **err_out) {
  if (err_out)
    *err_out = NULL;

  if (fd < 0) {
    char *err_msg = NULL;
    set_parse_err(&err_msg, "$: config fd is invalid.");
    if (err_out) {
      *err_out = err_msg;
    } else {
      free(err_msg);
    }
    return NULL;
  }

  if (lseek(fd, 0, SEEK_SET) < 0) {
    char *err_msg = NULL;
    set_parse_err(&err_msg, "$: failed to rewind config file.");
    if (err_out) {
      *err_out = err_msg;
    } else {
      free(err_msg);
    }
    return NULL;
  }

  StrBuf sb;
  sb_init(&sb);
  if (fileio_sb_read_limit_fd(fd, CONFIG_MAX_BYTES, &sb) != OK) {
    char *err_msg = NULL;
    set_parse_err(&err_msg,
                  "$: failed to read config file (check path and size limit).");
    if (err_out) {
      *err_out = err_msg;
    } else {
      free(err_msg);
    }
    return NULL;
  }

  ConnCatalog *cat = catalog_parse_config_bytes(sb.data, sb.len, err_out);
  sb_clean(&sb);
  return cat;
}

ConnCatalog *catalog_create_empty(const char *cred_namespace) {
  if (!cred_namespace || cred_namespace[0] == '\0')
    return NULL;

  ConnCatalog *cat = (ConnCatalog *)xcalloc(1, sizeof(*cat));

  cat->credential_namespace = dup_or_null(cred_namespace);
  if (!cat->credential_namespace) {
    free(cat);
    return NULL;
  }

  return cat;
}

void catalog_destroy(ConnCatalog *cat) {
  if (!cat)
    return;
  if (cat->profiles) {
    for (size_t i = 0; i < cat->n_profiles; i++) {
      profile_clean(&cat->profiles[i]);
    }
    free(cat->profiles);
    cat->profiles = NULL;
  }
  free((char *)cat->credential_namespace);
  cat->credential_namespace = NULL;
  free(cat);
}

size_t catalog_count(const ConnCatalog *cat) {
  if (!cat)
    return 0;
  return cat->n_profiles;
}

size_t catalog_list(ConnCatalog *cat, ConnProfile **out, size_t cap_count) {
  if (!cat)
    return 0;
  if (!out || cap_count == 0)
    return cat->n_profiles;

  size_t n = (cat->n_profiles < cap_count) ? cat->n_profiles : cap_count;
  for (size_t i = 0; i < n; i++) {
    out[i] = &cat->profiles[i];
  }
  return n;
}

/* Comparator for SafeFunctionRule array lookup by name. */
static int saferule_cmp(const void *a, const void *b) {
  const SafeFunctionRule *ra = (const SafeFunctionRule *)a;
  const SafeFunctionRule *rb = (const SafeFunctionRule *)b;
  return strcmp(ra->name, rb->name);
}

/* Matches one normalized '*' column glob against one normalized identifier.
 * Returns YES when the pattern matches, NO when it does not.
 */
static AdbxTriStatus glob_match_column_pattern(const char *pattern,
                                               const char *value) {
  assert(pattern);
  assert(value);

  const char *p = pattern;
  const char *v = value;
  const char *star = NULL;
  const char *match = NULL;

  while (*v) {
    if (*p == '*') {
      star = p;
      p++;
      match = v;
    } else if (*p == *v) {
      p++;
      v++;
    } else if (star) {
      p = star + 1;
      match++;
      v = match;
    } else {
      return NO;
    }
  }

  while (*p == '*')
    p++;

  return (*p == '\0') ? YES : NO;
}

/* Searches one sensitive-rule bucket and writes '*out_domain', if not NULL, to
 * a caller-borrowed string identifying the sensitive domain. Returns YES on
 * first matching rule, NO when no rule matches, ERR on invalid input or
 * malformed rule state.
 */
static AdbxTriStatus
sensitive_bucket_find_domain(const SensitiveRuleBucket *bucket,
                             const char *schema, const char *table,
                             const char *column, const char **out_domain) {
  assert(bucket);
  assert(column);

  for (size_t i = 0; i < bucket->n_rules; i++) {
    const SensitiveRule *rule = &bucket->rules[i];
    assert(rule->column_pat);
    assert(rule->domain);

    if (rule->schema) {
      if (!schema || strcmp(rule->schema, schema) != 0)
        continue;
    }
    if (rule->table) {
      if (!table || strcmp(rule->table, table) != 0)
        continue;
    }

    if (rule->star_count == 0) {
      if (strcmp(rule->column_pat, column) != 0)
        continue;
      if (out_domain) {
        *out_domain = rule->domain;
      }
      return YES;
    } else {
      AdbxTriStatus mrc = glob_match_column_pattern(rule->column_pat, column);
      if (mrc == YES) {
        if (out_domain) {
          *out_domain = rule->domain;
        }
        return YES;
      }
    }
  }
  return NO;
}

/* Clears one optional sensitive-domain lookup output before use.
 * It borrows 'out' and does not allocate memory.
 * Side effects: resets the caller-owned output struct to the empty state.
 * Error semantics: none; NULL input is ignored.
 */
static void sensitive_lookup_out_clear(SensDomainOut *out) {
  if (!out)
    return;
  out->domain = NULL;
  ADBX_ERR_CLEAR(&out->err, CONNCAT_ERR_NONE);
}

/* Stores one typed sensitive-domain lookup error in an optional output.
 * It borrows 'out' and allocates nothing.
 * Side effects: clears any previously stored domain and formats one message.
 * Error semantics: none; NULL output is ignored.
 */
static void sensitive_lookup_out_set_err(SensDomainOut *out,
                                         ConnCatalogErrCode code,
                                         const char *fmt, ...) {
  if (!out)
    return;

  out->domain = NULL;
  out->err.code = code;
  if (!fmt) {
    out->err.msg[0] = '\0';
    return;
  }

  va_list ap;
  va_start(ap, fmt);
  (void)vsnprintf(out->err.msg, sizeof(out->err.msg), fmt, ap);
  va_end(ap);
}

/* Scans one schema-qualified bucket while ignoring schema, and collects one
 * unique matching domain across all schemas for the caller-supplied
 * table/column pair. It borrows all inputs and writes the borrowed unique
 * domain through 'inout_domain' when provided. When it detects a conflicting
 * second domain, it writes that borrowed domain through
 * 'out_conflict_domain' when provided.
 * Side effects: mutates '*inout_domain' when it discovers the first match.
 * Error semantics: returns YES when at least one rule matches without domain
 * conflict, NO when nothing matches, ERR on invalid input, malformed rule
 * state, or conflicting matched domains.
 */
static AdbxTriStatus
sensitive_bucket_collect_unique_domain(const SensitiveRuleBucket *bucket,
                                       const char *table, const char *column,
                                       const char **inout_domain,
                                       const char **out_conflict_domain) {
  if (!bucket || !table || !column || !inout_domain)
    return ERR;
  if (out_conflict_domain)
    *out_conflict_domain = NULL;

  int found = 0;
  for (size_t i = 0; i < bucket->n_rules; i++) {
    const SensitiveRule *rule = &bucket->rules[i];
    if (!rule->schema || !rule->table || !rule->column_pat || !rule->domain)
      return ERR;
    if (strcmp(rule->table, table) != 0)
      continue;

    AdbxTriStatus mrc = NO;
    if (rule->star_count == 0) {
      mrc = (strcmp(rule->column_pat, column) == 0) ? YES : NO;
    } else {
      mrc = glob_match_column_pattern(rule->column_pat, column);
    }
    if (mrc == ERR)
      return ERR;
    if (mrc == NO)
      continue;

    if (!*inout_domain) {
      *inout_domain = rule->domain;
    } else if (strcmp(*inout_domain, rule->domain) != 0) {
      if (out_conflict_domain)
        *out_conflict_domain = rule->domain;
      return ERR;
    }
    found = 1;
  }

  return found ? YES : NO;
}

AdbxTriStatus connp_get_sensitive_domain(const ConnProfile *cp,
                                         const char *schema, const char *table,
                                         const char *column,
                                         SensDomainOut *out) {
  sensitive_lookup_out_clear(out);

  if (!cp || !table || !column || table[0] == '\0' || column[0] == '\0') {
    sensitive_lookup_out_set_err(
        out, CONNCAT_ERR_INVALID_INPUT,
        "sensitive domain lookup requires non-empty table and column names.");
    return ERR;
  }

  const SensitiveDomainPolicy *pol = &cp->sens_policy;
  // TODO: consider adding an out_domain_len parameter to avoid repeated
  // strlen() calls in callers that need the borrowed domain bytes.
  if (!pol->storage || pol->n_storage == 0)
    return NO;

  const char *schema_norm = (schema && schema[0] != '\0') ? schema : NULL;
  const char *domain = NULL;
  AdbxTriStatus rc = NO;
  if (schema_norm) {
    rc = sensitive_bucket_find_domain(&pol->exact_stc, schema_norm, table,
                                      column, &domain);
    if (rc != NO)
      goto done;
    rc = sensitive_bucket_find_domain(&pol->glob_stc, schema_norm, table,
                                      column, &domain);
    if (rc != NO)
      goto done;
    rc = sensitive_bucket_find_domain(&pol->exact_tc, NULL, table, column,
                                      &domain);
    if (rc != NO)
      goto done;
    rc = sensitive_bucket_find_domain(&pol->glob_tc, NULL, table, column,
                                      &domain);
    if (rc != NO)
      goto done;
    rc = sensitive_bucket_find_domain(&pol->exact_c, NULL, table, column,
                                      &domain);
    if (rc != NO)
      goto done;
    rc = sensitive_bucket_find_domain(&pol->glob_c, NULL, table, column,
                                      &domain);
    if (rc == YES && out)
      out->domain = domain;
    return rc;
  }

  const char *tc_domain = NULL;
  rc = sensitive_bucket_find_domain(&pol->exact_tc, NULL, table, column,
                                    &tc_domain);
  if (rc == NO) {
    rc = sensitive_bucket_find_domain(&pol->glob_tc, NULL, table, column,
                                      &tc_domain);
  }
  if (rc == NO) {
    rc = sensitive_bucket_find_domain(&pol->exact_c, NULL, table, column,
                                      &tc_domain);
  }
  if (rc == NO) {
    rc = sensitive_bucket_find_domain(&pol->glob_c, NULL, table, column,
                                      &tc_domain);
  }
  if (rc == ERR) {
    sensitive_lookup_out_set_err(
        out, CONNCAT_ERR_INTERNAL,
        "invalid sensitive-domain policy state while matching '%s.%s'.", table,
        column);
    return ERR;
  }

  const char *stc_domain = NULL;
  const char *conflict_domain = NULL;
  AdbxTriStatus src = sensitive_bucket_collect_unique_domain(
      &pol->exact_stc, table, column, &stc_domain, &conflict_domain);
  if (src == YES || src == NO) {
    AdbxTriStatus grc = sensitive_bucket_collect_unique_domain(
        &pol->glob_stc, table, column, &stc_domain, &conflict_domain);
    if (grc == ERR) {
      if (conflict_domain) {
        sensitive_lookup_out_set_err(
            out, CONNCAT_ERR_AMBIGUOUS_DOMAIN,
            "ambiguous sensitive domain for lookup '%s.%s': conflicting "
            "domains '%s' and '%s'.",
            table, column, stc_domain ? stc_domain : "<unknown>",
            conflict_domain);
      } else {
        sensitive_lookup_out_set_err(
            out, CONNCAT_ERR_INTERNAL,
            "invalid sensitive-domain policy state while matching '%s.%s'.",
            table, column);
      }
      return ERR;
    }
    if (grc == YES)
      src = YES;
  } else {
    if (conflict_domain) {
      sensitive_lookup_out_set_err(
          out, CONNCAT_ERR_AMBIGUOUS_DOMAIN,
          "ambiguous sensitive domain for lookup '%s.%s': conflicting "
          "domains '%s' and '%s'.",
          table, column, stc_domain ? stc_domain : "<unknown>",
          conflict_domain);
    } else {
      sensitive_lookup_out_set_err(
          out, CONNCAT_ERR_INTERNAL,
          "invalid sensitive-domain policy state while matching '%s.%s'.",
          table, column);
    }
    return ERR;
  }

  if (src == NO) {
    if (rc == YES && out)
      out->domain = tc_domain;
    return rc;
  }

  if (rc == NO) {
    if (out)
      out->domain = stc_domain;
    return YES;
  }

  if (strcmp(stc_domain, tc_domain) != 0) {
    sensitive_lookup_out_set_err(
        out, CONNCAT_ERR_AMBIGUOUS_DOMAIN,
        "ambiguous sensitive domain for lookup '%s.%s': conflicting domains "
        "'%s' and '%s'.",
        table, column, tc_domain, stc_domain);
    return ERR;
  }

  domain = tc_domain;
done:
  if (rc == ERR) {
    sensitive_lookup_out_set_err(
        out, CONNCAT_ERR_INTERNAL,
        "invalid sensitive-domain policy state while matching '%s.%s'.", table,
        column);
    return ERR;
  }
  if (rc == YES && out)
    out->domain = domain;
  return rc;
}

AdbxTriStatus connp_is_func_safe(const ConnProfile *cp, const char *schema,
                                 const char *name) {
  if (!cp || !name)
    return ERR;

  SafeFunctionPolicy *pol = (SafeFunctionPolicy *)&cp->safe_funcs;
  if (pol->n_rules == 0)
    return NO;

  SafeFunctionRule key = {0};
  key.name = name;
  SafeFunctionRule *r = (SafeFunctionRule *)bsearch(
      &key, pol->rules, pol->n_rules, sizeof(*pol->rules), saferule_cmp);
  if (!r)
    return NO;

  if (r->is_global)
    return YES;

  const char *schema_norm = (schema && schema[0] != '\0') ? schema : NULL;
  if (!schema_norm) {
    // Unqualified functions only match global rules; schema-scoped rules are
    // not considered.
    return NO;
  }

  // Schemas are few, so a linear scan is simpler and fast.
  for (uint32_t i = 0; i < r->n_schemas; i++) {
    if (strcmp(schema_norm, r->schemas[i]) == 0)
      return YES;
  }
  return NO;
}
