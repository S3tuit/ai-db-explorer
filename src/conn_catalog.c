#include "conn_catalog.h"
#include "file_io.h"
#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>

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
 * Error semantics: returns ERR in all cases; if allocation fails, *err_out
 * remains unchanged.
 */
static int set_parse_err(char **err_out, const char *fmt, ...) {
  if (!err_out || !fmt)
    return ERR;
  if (*err_out)
    return ERR;

  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  if (n < 0)
    return ERR;

  size_t len = (size_t)n;
  if (len >= sizeof(buf))
    len = sizeof(buf) - 1;

  char *msg = (char *)malloc(len + 1);
  if (!msg)
    return ERR;
  memcpy(msg, buf, len);
  msg[len] = '\0';
  *err_out = msg;
  return ERR;
}

/* Splits a decoded column path into schema/table/column components.
 * Ownership: caller owns 's' and any output pointers are into 's'.
 * Side effects: mutates 's' by inserting NUL terminators.
 * Returns OK/ERR. */
static int split_column_path(char *s, char **out_schema, char **out_table,
                             char **out_col) {
  if (!s || !out_table || !out_col)
    return ERR;

  *out_schema = NULL;
  *out_table = NULL;
  *out_col = NULL;

  char *first = strchr(s, '.');
  if (!first)
    return ERR;
  char *second = strchr(first + 1, '.');

  if (second && strchr(second + 1, '.'))
    return ERR; // too many parts

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

  if (!*out_table || !*out_col)
    return ERR;
  if ((*out_table)[0] == '\0' || (*out_col)[0] == '\0')
    return ERR;
  if (*out_schema && (*out_schema)[0] == '\0')
    return ERR;

  return OK;
}

// Temporary rule list on heap: lets us parse, normalize, sort, and dedupe
// before moving data into the arena in a compact form.
typedef struct {
  char *table;
  char *col;
  char *schema; // NULL for global rules
} ColumnRuleTmp;

typedef struct {
  char *schema; // NULL for global rules
  char *name;
} SafeFuncRuleTmp;

/* Compares ColumnRuleTmp entries by table, col, schema (NULL first). */
static int colruletmp_cmp(const void *a, const void *b) {
  const ColumnRuleTmp *ra = (const ColumnRuleTmp *)a;
  const ColumnRuleTmp *rb = (const ColumnRuleTmp *)b;

  int tc = strcmp(ra->table, rb->table);
  if (tc != 0)
    return tc;
  int cc = strcmp(ra->col, rb->col);
  if (cc != 0)
    return cc;

  if (!ra->schema && !rb->schema)
    return 0;
  if (!ra->schema)
    return -1;
  if (!rb->schema)
    return 1;
  return strcmp(ra->schema, rb->schema);
}

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

static int split_func_path(char *input, char **out_schema, char **out_name) {
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

/* Parses sensitiveColumns into ColumnRules.
 * Business logic is documented in connp_is_col_sensitive().
 * Ownership: stores all strings and arrays in out->col_policy.arena.
 * Side effects: allocates temporary heap lists and arena-backed rule storage.
 * Error semantics: returns OK on success, ERR on malformed entries, allocation
 * failures, or invalid input.
 */
static int parse_sensitive_columns(const JsonGetter *jg, ConnProfile *out,
                                   const char *path_prefix, char **err_out) {
  if (!jg || !out || !path_prefix)
    return ERR;

  JsonArrIter it;
  int rc = jsget_array_strings_begin(jg, "sensitiveColumns", &it);
  if (rc == NO)
    return OK;
  if (rc != YES) {
    return set_parse_err(err_out,
                         "%s.sensitiveColumns: expected an array of strings.",
                         path_prefix);
  }

  ColumnRuleTmp *tmp = NULL;
  size_t tmp_len = 0;
  size_t tmp_cap = 0;

  for (;;) {
    JsonStrSpan sp = {0};
    rc = jsget_array_strings_next(jg, &it, &sp);
    if (rc == NO)
      break;
    if (rc != YES) {
      set_parse_err(err_out, "%s.sensitiveColumns: expected string entries.",
                    path_prefix);
      goto error;
    }

    char *decoded = NULL;
    if (json_span_decode_alloc(&sp, &decoded) != YES) {
      set_parse_err(err_out,
                    "%s.sensitiveColumns: failed to decode string entry.",
                    path_prefix);
      goto error;
    }

    char *schema = NULL;
    char *table = NULL;
    char *colname = NULL;
    if (split_column_path(decoded, &schema, &table, &colname) != OK) {
      free(decoded);
      set_parse_err(err_out,
                    "%s.sensitiveColumns[]: expected [schema.]table.column.",
                    path_prefix);
      goto error;
    }

    str_lower_inplace(schema);
    str_lower_inplace(table);
    str_lower_inplace(colname);

    if (tmp_len == tmp_cap) {
      size_t nc = (tmp_cap == 0) ? 8 : tmp_cap * 2;
      ColumnRuleTmp *nt = (ColumnRuleTmp *)xrealloc(tmp, nc * sizeof(*tmp));
      tmp = nt;
      tmp_cap = nc;
    }

    tmp[tmp_len].schema = schema ? strdup(schema) : NULL;
    tmp[tmp_len].table = strdup(table);
    tmp[tmp_len].col = strdup(colname);
    if (!tmp[tmp_len].table || !tmp[tmp_len].col ||
        (schema && !tmp[tmp_len].schema)) {
      free(decoded);
      set_parse_err(err_out, "%s.sensitiveColumns: internal allocation error.",
                    path_prefix);
      goto error;
    }
    tmp_len++;

    free(decoded);
  }

  if (tmp_len == 0)
    return OK;

  qsort(tmp, tmp_len, sizeof(*tmp), colruletmp_cmp);

  if (pl_arena_init(&out->col_policy.arena, NULL, NULL) != OK) {
    set_parse_err(err_out, "%s.sensitiveColumns: internal allocation error.",
                  path_prefix);
    goto error;
  }

  // this loop finds unique elements
  size_t n_rules = 0;
  for (size_t i = 0; i < tmp_len;) {
    size_t j = i + 1;
    // works because the array is sorted
    while (j < tmp_len && strcmp(tmp[i].table, tmp[j].table) == 0 &&
           strcmp(tmp[i].col, tmp[j].col) == 0) {
      j++;
    }
    n_rules++;
    i = j;
  }

  // At this point we have a sorted tmp array (not malloc'd) of ColumnRuleTmp.
  // Now we have to malloc it in order to persist it.
  ColumnRule *rules = (ColumnRule *)pl_arena_alloc(
      &out->col_policy.arena, (uint32_t)(n_rules * sizeof(*rules)));
  if (!rules) {
    set_parse_err(err_out, "%s.sensitiveColumns: internal allocation error.",
                  path_prefix);
    goto error;
  }

  // since we may have duplication, this loop is used for deduplication
  size_t rix = 0;
  for (size_t i = 0; i < tmp_len;) {
    size_t j = i;
    int is_global = 0;
    size_t n_schema = 0;
    const char *last_schema = NULL;

    // j skips duplicated elements
    while (j < tmp_len && strcmp(tmp[i].table, tmp[j].table) == 0 &&
           strcmp(tmp[i].col, tmp[j].col) == 0) {
      // no schema-qualified -> global rule
      if (!tmp[j].schema) {
        is_global = 1;

        // since the temp list is sorted, ignoring consecutive and equal schemas
        // means deduplicating
      } else if (!last_schema || strcmp(tmp[j].schema, last_schema) != 0) {
        n_schema++;
        last_schema = tmp[j].schema;
      }
      j++;
    }

    ColumnRule *r = &rules[rix++];
    r->table = (const char *)pl_arena_add(&out->col_policy.arena, tmp[i].table,
                                          (uint32_t)strlen(tmp[i].table));
    r->col = (const char *)pl_arena_add(&out->col_policy.arena, tmp[i].col,
                                        (uint32_t)strlen(tmp[i].col));
    r->is_global = is_global;
    r->n_schemas = (uint32_t)n_schema;
    r->schemas = NULL;

    if (n_schema > 0) {
      const char **schemas = (const char **)pl_arena_alloc(
          &out->col_policy.arena, (uint32_t)(n_schema * sizeof(*schemas)));
      if (!schemas) {
        set_parse_err(err_out,
                      "%s.sensitiveColumns: internal allocation error.",
                      path_prefix);
        goto error;
      }

      size_t k = 0;
      last_schema = NULL;
      for (size_t t = i; t < j; t++) {
        if (!tmp[t].schema)
          continue;
        if (last_schema && strcmp(tmp[t].schema, last_schema) == 0)
          continue;
        schemas[k++] =
            (const char *)pl_arena_add(&out->col_policy.arena, tmp[t].schema,
                                       (uint32_t)strlen(tmp[t].schema));
        last_schema = tmp[t].schema;
      }
      r->schemas = schemas;
    }

    i = j;
  }

  out->col_policy.rules = rules;
  out->col_policy.n_rules = n_rules;

  for (size_t i = 0; i < tmp_len; i++) {
    free(tmp[i].schema);
    free(tmp[i].table);
    free(tmp[i].col);
  }
  free(tmp);
  return OK;

error:
  set_parse_err(err_out, "%s.sensitiveColumns: invalid entry.", path_prefix);
  if (tmp) {
    for (size_t i = 0; i < tmp_len; i++) {
      free(tmp[i].schema);
      free(tmp[i].table);
      free(tmp[i].col);
    }
    free(tmp);
  }
  return ERR;
}

/* Parses safeFunctions into SafeFunctionRule list.
 * Ownership: stores all strings and arrays in out->safe_funcs.arena.
 * Side effects: allocates temporary heap lists and arena-backed rule storage.
 * Error semantics: returns OK on success, ERR on malformed entries, allocation
 * failures, or invalid input.
 */
static int parse_safe_functions(const JsonGetter *jg, ConnProfile *out,
                                const char *path_prefix, char **err_out) {
  if (!jg || !out || !path_prefix)
    return ERR;

  JsonArrIter it;
  int rc = jsget_array_strings_begin(jg, "safeFunctions", &it);
  if (rc == NO)
    return OK;
  if (rc != YES) {
    return set_parse_err(err_out,
                         "%s.safeFunctions: expected an array of strings.",
                         path_prefix);
  }

  if (pl_arena_init(&out->safe_funcs.arena, NULL, NULL) != OK) {
    return set_parse_err(
        err_out, "%s.safeFunctions: internal allocation error.", path_prefix);
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

  out->safe_funcs.rules = (SafeFunctionRule *)pl_arena_alloc(
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
    r->name = (const char *)pl_arena_add(&out->safe_funcs.arena, name,
                                         (uint32_t)strlen(name));
    if (!r->name)
      goto error;
    r->is_global = is_global;
    r->n_schemas = (uint32_t)scount;
    if (scount == 0) {
      r->schemas = NULL;
    } else {
      r->schemas = (const char **)pl_arena_alloc(
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
            (const char *)pl_arena_add(&out->safe_funcs.arena, tmp[t].schema,
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
  pl_arena_clean(&out->safe_funcs.arena);
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
static int parse_policy(const JsonGetter *jg, SafetyPolicy *out,
                        const char *path_prefix, char **err_out) {
  if (!jg || !out || !path_prefix)
    return ERR;

  const char *const keys[] = {"readOnly", "statementTimeoutMs",
                              "maxRowReturned", "maxPayloadKiloBytes",
                              "columnPolicy"};
  if (jsget_top_level_validation(jg, NULL, keys, ARRLEN(keys)) != YES) {
    return set_parse_err(err_out, "%s: unknown key in object.", path_prefix);
  }

  JsonStrSpan ro = {0};
  int rrc = jsget_string_span(jg, "readOnly", &ro);
  if (rrc == ERR) {
    return set_parse_err(err_out, "%s.readOnly: expected string.", path_prefix);
  }
  if (rrc == YES) {
    if (ro.len == 3 && strncasecmp(ro.ptr, "yes", 3) == 0) {
      out->read_only = 1;
    } else if (ro.len == 9 && strncasecmp(ro.ptr, "no unsafe", 9) == 0) {
      out->read_only = 0;
    } else {
      return set_parse_err(err_out,
                           "%s.readOnly: expected \"yes\" or \"no unsafe\".",
                           path_prefix);
    }
  }

  uint32_t timeout_ms = 0;
  int trc = jsget_u32(jg, "statementTimeoutMs", &timeout_ms);
  if (trc == ERR)
    return set_parse_err(err_out, "%s.statementTimeoutMs: expected uint32.",
                         path_prefix);
  if (trc == YES)
    out->statement_timeout_ms = timeout_ms;

  uint32_t max_rows = 0;
  int mrc = jsget_u32(jg, "maxRowReturned", &max_rows);
  if (mrc == ERR)
    return set_parse_err(err_out, "%s.maxRowReturned: expected uint32.",
                         path_prefix);
  if (mrc == YES)
    out->max_rows = max_rows;

  uint32_t max_payload_kb = 0;
  int qrc = jsget_u32(jg, "maxPayloadKiloBytes", &max_payload_kb);
  if (qrc == ERR)
    return set_parse_err(err_out, "%s.maxPayloadKiloBytes: expected uint32.",
                         path_prefix);
  if (qrc == YES) {
    if (max_payload_kb > (UINT32_MAX / 1024u)) {
      return set_parse_err(err_out, "%s.maxPayloadKiloBytes: value too large.",
                           path_prefix);
    }
    out->max_payload_bytes = max_payload_kb * 1024u;
  }

  JsonGetter col = {0};
  int crc = jsget_object(jg, "columnPolicy", &col);
  if (crc == ERR)
    return set_parse_err(err_out, "%s.columnPolicy: expected object.",
                         path_prefix);
  if (crc == YES) {
    const char *const ckeys[] = {"mode", "strategy"};
    if (jsget_top_level_validation(&col, NULL, ckeys, ARRLEN(ckeys)) != YES) {
      return set_parse_err(err_out, "%s.columnPolicy: unknown key in object.",
                           path_prefix);
    }

    JsonStrSpan mode = {0};
    int mrc2 = jsget_string_span(&col, "mode", &mode);
    if (mrc2 != YES) {
      return set_parse_err(err_out,
                           "%s.columnPolicy.mode: expected \"pseudonymize\".",
                           path_prefix);
    }
    if (!(mode.len == strlen("pseudonymize") &&
          strncasecmp(mode.ptr, "pseudonymize", mode.len) == 0)) {
      return set_parse_err(err_out,
                           "%s.columnPolicy.mode: expected \"pseudonymize\".",
                           path_prefix);
    }
    out->column_mode = SAFETY_COLMODE_PSEUDONYMIZE;

    JsonStrSpan strat = {0};
    int src = jsget_string_span(&col, "strategy", &strat);
    if (src != YES) {
      return set_parse_err(
          err_out,
          "%s.columnPolicy.strategy: expected \"deterministic\" or "
          "\"randomized\".",
          path_prefix);
    }

    if (strat.len == strlen("deterministic") &&
        strncasecmp(strat.ptr, "deterministic", strat.len) == 0) {
      out->column_strategy = SAFETY_COLSTRAT_DETERMINISTIC;
    } else if (strat.len == strlen("randomized") &&
               strncasecmp(strat.ptr, "randomized", strat.len) == 0) {
      out->column_strategy = SAFETY_COLSTRAT_RANDOMIZED;
    } else {
      return set_parse_err(
          err_out,
          "%s.columnPolicy.strategy: expected \"deterministic\" or "
          "\"randomized\".",
          path_prefix);
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
  pl_arena_clean(&p->col_policy.arena);
  pl_arena_clean(&p->safe_funcs.arena);
}

/* Parses one databases[i] object into 'out'. This will clean 'out' if something
 * goes wrong. Ownership: writes owned strings and policy arenas into
 * caller-owned 'out'. Side effects: heap and arena allocations. Error
 * semantics: returns OK on valid entry, ERR on malformed fields or allocation
 * failures.
 */
static int parse_db_entry(ConnCatalog *cat, const JsonGetter *jg,
                          ConnProfile *out, size_t db_index, char **err_out) {
  if (!cat || !jg || !out)
    return ERR;

  char db_path[64];
  snprintf(db_path, sizeof(db_path), "$.databases[%zu]", db_index);

  const char *const keys[] = {
      "type",          "connectionName", "host",    "port",
      "username",      "database",       "options", "sensitiveColumns",
      "safeFunctions", "safetyPolicy"};
  int vrc = jsget_top_level_validation(jg, NULL, keys, ARRLEN(keys));
  if (vrc != YES) {
    return set_parse_err(err_out, "%s: unknown key in database entry.",
                         db_path);
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

  int orc = jsget_string_decode_alloc(jg, "options", &options);
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
  out->kind = DB_KIND_POSTGRES;
  out->host = host;
  out->port = (uint16_t)port;
  out->db_name = db_name;
  out->user = user;
  out->options = options;

  out->safe_policy = cat->policy;
  JsonGetter db_pol = {0};
  int prc = jsget_object(jg, "safetyPolicy", &db_pol);
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

  if (parse_sensitive_columns(jg, out, db_path, err_out) != OK)
    goto error;
  if (parse_safe_functions(jg, out, db_path, err_out) != OK)
    goto error;
  return OK;

error:
  profile_clean(out);

  // make it safe for caller to call free if ERR
  out->connection_name = NULL;
  out->db_name = NULL;
  out->host = NULL;
  out->options = NULL;
  out->user = NULL;
  memset(&out->col_policy, 0, sizeof(out->col_policy));
  memset(&out->safe_funcs, 0, sizeof(out->safe_funcs));
  return ERR;
}

/* Parses the "databases" array and allocates ConnProfile entries inside 'cat'.
 * Ownership: writes owned profile array into '*cat'.
 * Side effects: allocates heap memory for profiles and nested arenas.
 * Error semantics: returns OK on valid non-empty array, ERR otherwise.
 */
static int parse_databases(const JsonGetter *jg, ConnCatalog *cat,
                           char **err_out) {
  if (!jg || !cat)
    return ERR;

  JsonArrIter it;
  int rc = jsget_array_objects_begin(jg, "databases", &it);
  if (rc != YES) {
    return set_parse_err(err_out, "$.databases: expected array of objects.");
  }

  if (it.count <= 0) {
    return set_parse_err(err_out,
                         "$.databases: at least one entry is required.");
  }

  if (it.count < 0 || (size_t)it.count > CONFIG_MAX_CONNECTIONS) {
    return set_parse_err(
        err_out,
        "$.databases: too many entries (exceeds configured connection cap).");
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

/* Validates that the top-level "version" key exists and matches the current
 * catalog schema version. Returns:
 * - YES: version exists and is supported.
 * - NO: version missing or unsupported.
 * - ERR: malformed version value or internal error.
 */
static int parse_version(const JsonGetter *jg) {
  if (!jg)
    return ERR;

  char *ver = NULL;
  int rc = jsget_string_decode_alloc(jg, "version", &ver);
  if (rc != YES)
    return NO;

  int ok = (strcmp(ver, CURR_CONN_CAT_VERSION) == 0) ? YES : NO;
  free(ver);
  return ok;
}

ConnCatalog *catalog_load_from_file(const char *path, char **err_out) {
  char *err_msg = NULL;
  StrBuf sb;
  sb_init(&sb);
  ConnCatalog *cat = NULL;

  if (err_out)
    *err_out = NULL;

  if (!path) {
    set_parse_err(&err_msg, "$: config path is NULL.");
    goto error;
  }

  if (fileio_sb_read_limit(path, CONFIG_MAX_BYTES, &sb) != OK) {
    set_parse_err(&err_msg,
                  "$: failed to read config file (check path and size limit).");
    goto error;
  }

  JsonGetter jg;
  if (jsget_init(&jg, sb.data, sb.len) != OK) {
    set_parse_err(&err_msg, "$: invalid JSON.");
    goto error;
  }

  // make sure these 2 objects are present in the config file
  const char *const root_keys[] = {"version", "safetyPolicy", "databases"};
  if (jsget_top_level_validation(&jg, NULL, root_keys, ARRLEN(root_keys)) !=
      YES) {
    set_parse_err(&err_msg, "$: unknown key at top level.");
    goto error;
  }

  int vrc = parse_version(&jg);
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

  sb_clean(&sb);
  return cat;

error:
  sb_clean(&sb);
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

/* Comparator for ColumnRule array sorting and lookup. */
static int colrule_cmp(const void *a, const void *b) {
  const ColumnRule *ra = (const ColumnRule *)a;
  const ColumnRule *rb = (const ColumnRule *)b;
  int tc = strcmp(ra->table, rb->table);
  if (tc != 0)
    return tc;
  return strcmp(ra->col, rb->col);
}

/* Comparator for SafeFunctionRule array lookup by name. */
static int saferule_cmp(const void *a, const void *b) {
  const SafeFunctionRule *ra = (const SafeFunctionRule *)a;
  const SafeFunctionRule *rb = (const SafeFunctionRule *)b;
  return strcmp(ra->name, rb->name);
}

int connp_is_col_sensitive(const ConnProfile *cp, const char *schema,
                           const char *table, const char *column) {
  if (!cp || !table || !column)
    return ERR;

  const ColumnPolicy *pol = &cp->col_policy;
  if (!pol->rules || pol->n_rules == 0)
    return NO;

  ColumnRule key = {0};
  key.table = table;
  key.col = column;

  ColumnRule *r = (ColumnRule *)bsearch(&key, pol->rules, pol->n_rules,
                                        sizeof(*pol->rules), colrule_cmp);
  if (!r)
    return NO;

  if (r->is_global)
    return YES;

  const char *schema_norm = (schema && schema[0] != '\0') ? schema : NULL;
  if (!schema_norm) {
    // Unqualified SQL matches any schema-scoped rule (we do not resolve
    // search_path).
    return YES;
  }

  if (!r->schemas || r->n_schemas == 0)
    return NO;

  // Schemas are few (usually <=10), so a linear scan is simpler and fast.
  for (uint32_t i = 0; i < r->n_schemas; i++) {
    if (strcmp(schema_norm, r->schemas[i]) == 0)
      return YES;
  }
  return NO;
}

int connp_is_func_safe(const ConnProfile *cp, const char *schema,
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
