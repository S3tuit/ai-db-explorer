#include "conn_catalog.h"
#include "file_io.h"
#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <ctype.h>
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

/* Parses columnPolicy.pseudonymize.{deterministic,randomized} into ColumnRules.
 * Business logic is documented in connp_is_col_sensitive().
 * Ownership: stores all strings and arrays in out->col_policy.arena. */
static int parse_column_policy(const JsonGetter *jg, ConnProfile *out) {
  if (!jg || !out)
    return ERR;

  JsonGetter col = {0};
  int crc = jsget_object(jg, "columnPolicy", &col);
  if (crc == NO)
    return OK;
  if (crc != YES)
    return ERR;

  const char *const col_keys[] = {"pseudonymize"};
  if (jsget_top_level_validation(&col, NULL, col_keys, ARRLEN(col_keys)) !=
      YES) {
    return ERR;
  }

  JsonGetter pseud = {0};
  int prc = jsget_object(&col, "pseudonymize", &pseud);
  if (prc == NO)
    return OK;
  if (prc != YES)
    return ERR;

  const char *const pseu_keys[] = {"deterministic", "randomized"};
  if (jsget_top_level_validation(&pseud, NULL, pseu_keys, ARRLEN(pseu_keys)) !=
      YES) {
    return ERR;
  }

  // v1 only supports deterministic; reject randomized to avoid silent footguns.
  JsonStrSpan span = {0};
  int rrc = jsget_string_span(&pseud, "randomized", &span);
  if (rrc == YES)
    return ERR;
  if (rrc == ERR)
    return ERR;

  // init a temporary heap list
  ColumnRuleTmp *tmp = NULL;
  size_t tmp_len = 0;
  size_t tmp_cap = 0;

  const char *const lists[] = {"deterministic"};
  for (size_t li = 0; li < ARRLEN(lists); li++) {
    JsonArrIter it;
    int rc = jsget_array_strings_begin(&pseud, lists[li], &it);
    if (rc == NO)
      continue;
    if (rc != YES)
      goto error;

    for (;;) {
      JsonStrSpan sp = {0};
      rc = jsget_array_strings_next(&pseud, &it, &sp);
      if (rc == NO)
        break;
      if (rc != YES)
        goto error;

      char *decoded = NULL;
      if (json_span_decode_alloc(&sp, &decoded) != YES)
        goto error;

      char *schema = NULL;
      char *table = NULL;
      char *colname = NULL;
      if (split_column_path(decoded, &schema, &table, &colname) != OK) {
        free(decoded);
        goto error;
      }

      str_lower_inplace(schema);
      str_lower_inplace(table);
      str_lower_inplace(colname);

      // For v1 we only store "deterministic" rules (lists[0]) while still
      // validating "randomized" entries to reject malformed configs.
      // li == 0 means only "deterministic"
      if (li == 0) {

        // grow the temporary list
        if (tmp_len == tmp_cap) {
          size_t nc = (tmp_cap == 0) ? 8 : tmp_cap * 2;
          ColumnRuleTmp *nt = (ColumnRuleTmp *)xrealloc(tmp, nc * sizeof(*tmp));
          tmp = nt;
          tmp_cap = nc;
        }

        // add elements to the temporary list
        tmp[tmp_len].schema = schema ? strdup(schema) : NULL;
        tmp[tmp_len].table = strdup(table);
        tmp[tmp_len].col = strdup(colname);
        tmp_len++;
      }

      free(decoded);
    }
  }

  if (tmp_len == 0)
    return OK;

  // sort the temporary list
  qsort(tmp, tmp_len, sizeof(*tmp), colruletmp_cmp);

  if (pl_arena_init(&out->col_policy.arena, NULL, NULL) != OK)
    goto error;

  // find unique elemets
  size_t n_rules = 0;
  for (size_t i = 0; i < tmp_len;) {
    size_t j = i + 1;
    while (j < tmp_len && strcmp(tmp[i].table, tmp[j].table) == 0 &&
           strcmp(tmp[i].col, tmp[j].col) == 0) {
      j++;
    }
    n_rules++;
    i = j;
  }

  ColumnRule *rules = (ColumnRule *)pl_arena_alloc(
      &out->col_policy.arena, (uint32_t)(n_rules * sizeof(*rules)));
  if (!rules)
    goto error;

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
      if (!schemas)
        goto error;

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
 * Business logic is documented in connp_is_func_safe().
 * Ownership: stores all strings and arrays in out->safe_funcs.arena. */
static int parse_safe_functions(const JsonGetter *jg, ConnProfile *out) {
  if (!jg || !out)
    return ERR;

  JsonArrIter it;
  int rc = jsget_array_strings_begin(jg, "safeFunctions", &it);
  if (rc == NO)
    return OK;
  if (rc != YES)
    return ERR;

  SafeFuncRuleTmp *tmp = NULL;
  size_t tmp_len = 0;
  size_t tmp_cap = 0;

  for (;;) {
    JsonStrSpan sp = {0};
    rc = jsget_array_strings_next(jg, &it, &sp);
    if (rc == NO)
      break;
    if (rc != YES)
      goto error;

    char *decoded = NULL;
    if (json_span_decode_alloc(&sp, &decoded) != YES)
      goto error;

    char *schema = NULL;
    char *name = NULL;
    if (split_func_path(decoded, &schema, &name) != OK) {
      free(decoded);
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
    tmp_len++;

    free(decoded);
  }

  if (tmp_len == 0)
    return OK;

  qsort(tmp, tmp_len, sizeof(*tmp), saferuletmp_cmp);

  if (pl_arena_init(&out->safe_funcs.arena, NULL, NULL) != OK)
    goto error;

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

/* Parses the "safetyPolicy" object and stores the resulting SafetyPolicy into
 * '*out'. */
static int parse_policy(const JsonGetter *jg, SafetyPolicy *out) {
  if (!jg || !out)
    return ERR;

  // makes sure policy if correctly formatted
  const char *const keys[] = {"readOnly", "statementTimeoutMs",
                              "maxRowReturned", "maxQueryKiloBytes"};
  int vrc = jsget_top_level_validation(jg, "safetyPolicy", keys, ARRLEN(keys));
  if (vrc != YES)
    return ERR;

  int read_only_flag = 0;
  int *read_only_ptr = NULL;
  JsonStrSpan span = {0};
  int rrc = jsget_string_span(jg, "safetyPolicy.readOnly", &span);
  if (rrc == ERR)
    return ERR;
  if (rrc == YES) {
    if (strncasecmp(span.ptr, "yes", span.len) == 0) {
      read_only_flag = 1;
    } else if (strncasecmp(span.ptr, "no unsafe", span.len) == 0) {
      read_only_flag = 0;
    } else {
      return ERR;
    }
    read_only_ptr = &read_only_flag;
  }

  uint32_t timeout_ms = 0;
  uint32_t *timeout_ptr = NULL;
  int trc = jsget_u32(jg, "safetyPolicy.statementTimeoutMs", &timeout_ms);
  if (trc == ERR)
    return ERR;
  if (trc == YES)
    timeout_ptr = &timeout_ms;

  uint32_t max_rows = 0;
  uint32_t *max_rows_ptr = NULL;
  int mrc = jsget_u32(jg, "safetyPolicy.maxRowReturned", &max_rows);
  if (mrc == ERR)
    return ERR;
  if (mrc == YES)
    max_rows_ptr = &max_rows;

  uint32_t max_query_kb = 0;
  uint32_t *max_query_ptr = NULL;
  int qrc = jsget_u32(jg, "safetyPolicy.maxQueryKiloBytes", &max_query_kb);
  if (qrc == ERR)
    return ERR;
  if (qrc == YES) {
    if (max_query_kb > (UINT32_MAX / 1024u))
      return ERR;
    max_query_kb *= 1024u;
    max_query_ptr = &max_query_kb;
  }

  // Use the standard defaults for any unset knobs.
  if (safety_policy_init(out, read_only_ptr, max_rows_ptr, max_query_ptr,
                         timeout_ptr) != OK) {
    return ERR;
  }
  return OK;
}

/* Parses one database entry object into 'out'. */
static int parse_db_entry(ConnCatalog *cat, const JsonGetter *jg,
                          ConnProfile *out) {
  if (!cat || !jg || !out)
    return ERR;

  const char *const keys[] = {"type",    "connectionName", "host",
                              "port",    "username",       "database",
                              "options", "columnPolicy",   "safeFunctions"};
  int vrc = jsget_top_level_validation(jg, NULL, keys, ARRLEN(keys));
  if (vrc != YES)
    return ERR;

  char *type = NULL;
  char *conn_name = NULL;
  char *host = NULL;
  char *user = NULL;
  char *db_name = NULL;
  char *options = NULL;

  if (jsget_string_decode_alloc(jg, "type", &type) != YES)
    goto error;
  if (strcmp(type, "postgres") != 0)
    goto error;

  if (jsget_string_decode_alloc(jg, "connectionName", &conn_name) != YES)
    goto error;
  if (jsget_string_decode_alloc(jg, "host", &host) != YES)
    goto error;

  uint32_t port = 0;
  if (jsget_u32(jg, "port", &port) != YES || port > UINT16_MAX)
    goto error;

  if (jsget_string_decode_alloc(jg, "username", &user) != YES)
    goto error;
  if (jsget_string_decode_alloc(jg, "database", &db_name) != YES)
    goto error;

  int orc = jsget_string_decode_alloc(jg, "options", &options);
  if (orc == ERR)
    goto error;
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

  // TODO: Right now, we allow just for one global SafetyPolicy, allow a
  // SafetyPolicy per connection that, if present, overwrites the global
  // SafetyPolicy just for that connection
  out->safe_policy = cat->policy;
  if (parse_column_policy(jg, out) != OK)
    goto error;
  if (parse_safe_functions(jg, out) != OK)
    goto error;
  return OK;

error:
  free(type);
  free(conn_name);
  free(host);
  free(user);
  free(db_name);
  free(options);
  out->connection_name = NULL;
  out->host = NULL;
  out->db_name = NULL;
  out->user = NULL;
  out->options = NULL;
  pl_arena_clean(&out->col_policy.arena);
  out->col_policy.rules = NULL;
  out->col_policy.n_rules = 0;
  pl_arena_clean(&out->safe_funcs.arena);
  out->safe_funcs.rules = NULL;
  out->safe_funcs.n_rules = 0;
  return ERR;
}

static void profile_free(ConnProfile *p) {
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

/* Parses the "databases" array and allocates ConnProfile entries inside 'cat'.
 */
static int parse_databases(const JsonGetter *jg, ConnCatalog *cat) {
  if (!jg || !cat)
    return ERR;

  JsonArrIter it;
  int rc = jsget_array_objects_begin(jg, "databases", &it);
  if (rc != YES)
    return ERR;

  if (it.count < 0 || (size_t)it.count > CONFIG_MAX_CONNECTIONS)
    return ERR;

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

    if (parse_db_entry(cat, &entry, &profiles[idx]) != OK)
      goto error;

    // connectionName must be unique
    for (size_t j = 0; j < idx; j++) {
      if (strcmp(profiles[idx].connection_name, profiles[j].connection_name) ==
          0) {
        goto error;
      }
    }

    idx++;
  }

  cat->profiles = profiles;
  cat->n_profiles = idx;
  return OK;

error:
  if (profiles) {
    // parse_db_entry can partially initialize profiles[idx] before failing.
    // Since profiles are zero-initialized, it's safe to clean all entries.
    for (size_t i = 0; i < n; i++) {
      profile_free(&profiles[i]);
    }
    free(profiles);
  }
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
  StrBuf sb = {0};
  ConnCatalog *cat = NULL;

  if (!path) {
    err_msg = "ConnCatalog: can't read from a NULL path.";
    goto error;
  }

  if (fileio_read_all_limit(path, CONFIG_MAX_BYTES, &sb) != OK) {
    err_msg = "ConnCatalog: failed to read config file. Check path and ensure "
              "file size respects configured limits.";
    goto error;
  }

  JsonGetter jg;
  if (jsget_init(&jg, sb.data, sb.len) != OK) {
    err_msg = "ConnCatalog: invalid JSON.";
    goto error;
  }

  // make sure these 2 objects are present in the config file
  const char *const root_keys[] = {"version", "safetyPolicy", "databases"};
  if (jsget_top_level_validation(&jg, NULL, root_keys, ARRLEN(root_keys)) !=
      YES) {
    err_msg = "ConnCatalog: unknown key at top level.";
    goto error;
  }

  int vrc = parse_version(&jg);
  if (vrc == NO) {
    err_msg = "ConnCatalog: unsupported or missing \"version\".";
    goto error;
  }
  if (vrc != YES) {
    err_msg = "ConnCatalog: invalid \"version\".";
    goto error;
  }

  // TODO: make errors of parse_policy and parse_databases more user-friendly

  cat = xcalloc(1, sizeof(*cat));
  // safetyPolicy is optional; when absent we fall back to defaults.
  const char *const policy_keys[] = {"readOnly", "statementTimeoutMs",
                                     "maxRowReturned", "maxQueryKiloBytes"};
  int prc = jsget_top_level_validation(&jg, "safetyPolicy", policy_keys,
                                       ARRLEN(policy_keys));
  if (prc == YES) {
    if (parse_policy(&jg, &cat->policy) != OK) {
      err_msg = "ConnCatalog: invalid safetyPolicy.";
      goto error;
    }
  } else if (prc == NO) {
    // Distinguish between a missing object (allowed) and a malformed one.
    JsonStrSpan span = {0};
    int has = jsget_string_span(&jg, "safetyPolicy", &span);
    if (has == NO) {
      if (safety_policy_init(&cat->policy, NULL, NULL, NULL, NULL) != OK) {
        err_msg = "ConnCatalog: invalid safetyPolicy.";
        goto error;
      }
    } else {
      err_msg = "ConnCatalog: invalid safetyPolicy.";
      goto error;
    }
  } else {
    err_msg = "ConnCatalog: invalid safetyPolicy.";
    goto error;
  }

  if (parse_databases(&jg, cat) != OK) {
    err_msg = "ConnCatalog: invalid \"databases\".";
    goto error;
  }

  sb_clean(&sb);
  return cat;

error:
  sb_clean(&sb);
  catalog_destroy(cat);
  if (err_out && err_msg)
    *err_out = err_msg;
  return NULL;
}

void catalog_destroy(ConnCatalog *cat) {
  if (!cat)
    return;
  if (cat->profiles) {
    for (size_t i = 0; i < cat->n_profiles; i++) {
      profile_free(&cat->profiles[i]);
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
