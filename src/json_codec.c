#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -------------------------------- builder ------------------------------- */

/*
 * Escapes 's' into JSON string content (WITHOUT surrounding quotes) and
 * appends it to 'sb'. This function takes 'len' and it'll limit the bytes
 * append to at most len if a '\0' is not seen before.
 *
 * Escape:
 *  - backslash, quote
 *  - control chars 0x00..0x1F as \u00XX
 * For bytes >= 0x20, pass through as-is.
 *
 * Return OK on success, ERR on error/bad input.
 */
static AdbxStatus sb_append_json_escaped(StrBuf *sb, const char *s,
                                         size_t len) {
  if (!sb || !s)
    return ERR;

  // we loop until a '\0' is encountered or until we append 'len' bytes
  for (size_t i = 0; i < len && s[i]; i++) {
    unsigned char c = (unsigned char)s[i];
    // append a \ before the char
    switch (c) {
    case '\"':
      if (sb_append_bytes(sb, "\\\"", 2) != OK)
        return ERR;
      break;
    case '\\':
      if (sb_append_bytes(sb, "\\\\", 2) != OK)
        return ERR;
      break;
    case '\b':
      if (sb_append_bytes(sb, "\\b", 2) != OK)
        return ERR;
      break;
    case '\f':
      if (sb_append_bytes(sb, "\\f", 2) != OK)
        return ERR;
      break;
    case '\n':
      if (sb_append_bytes(sb, "\\n", 2) != OK)
        return ERR;
      break;
    case '\r':
      if (sb_append_bytes(sb, "\\r", 2) != OK)
        return ERR;
      break;
    case '\t':
      if (sb_append_bytes(sb, "\\t", 2) != OK)
        return ERR;
      break;
    default:
      if (c < 0x20) {
        // \u00XX + '\0' = 7 chars len
        // example: 0x1F become \u001F (+ '\0')
        char tmp[7];
        int n = snprintf(tmp, sizeof(tmp), "\\u%04X", (unsigned)c);
        // without counting the '\0' there should be 6 bytes
        if (n != 6)
          return ERR;
        if (sb_append_bytes(sb, tmp, (size_t)n) != OK)
          return ERR;
      } else {
        if (sb_append_bytes(sb, &c, 1) != OK)
          return ERR;
      }
      break;
    }
  }
  return OK;
}

/* This appends values of different types to 'sb' based on 'fmt'. All the values
 * appended other than 'fmt' are json-escaped. 'fmt' uses the same logic as
 * printf, the only flags supported are:
 *  - %c appends a char
 *  - %s appends a string
 *  - %u appends uint32_t
 *  - %U appends uint64_t
 *  - %b appends bool (0 -> false, non-zero -> true)
 *  - %d appends int
 *  - %l appends long
 */
static AdbxStatus json_append(StrBuf *sb, const char *fmt, ...) {
  if (!sb || !fmt)
    return ERR;

  va_list ap;
  va_start(ap, fmt);

  size_t prev = 0;

  for (size_t i = 0; fmt[i] != '\0'; i++) {
    if (fmt[i] != '%')
      continue;

    // append literal chunk before %
    if (i > prev) {
      if (sb_append_bytes(sb, fmt + prev, i - prev) != OK)
        goto fail;
    }

    // parse specifier
    char spec = fmt[++i];
    if (spec == '\0')
      goto fail; // dangling '%'

    switch (spec) {
    case '%': {
      if (sb_append_bytes(sb, "%", 1) != OK)
        goto fail;
      break;
    }
    case 'c': {
      char c = (char)va_arg(ap, int);
      if (sb_append_json_escaped(sb, &c, 1) != OK)
        goto fail;
      break;
    }
    case 's': {
      const char *s = va_arg(ap, const char *);
      if (!s)
        goto fail;
      if (sb_append_json_escaped(sb, s, SIZE_MAX) != OK)
        goto fail;
      break;
    }
    case 'u': {
      uint32_t v = va_arg(ap, uint32_t);
      char tmp[32];
      int n = snprintf(tmp, sizeof(tmp), "%" PRIu32, v);
      if (n < 0)
        goto fail;
      if (sb_append_bytes(sb, tmp, (size_t)n) != OK)
        goto fail;
      break;
    }
    case 'U': {
      uint64_t v = va_arg(ap, uint64_t);
      char tmp[64];
      int n = snprintf(tmp, sizeof(tmp), "%" PRIu64, v);
      if (n < 0)
        goto fail;
      if (sb_append_bytes(sb, tmp, (size_t)n) != OK)
        goto fail;
      break;
    }
    case 'b': {
      int v = va_arg(ap, int);
      const char *lit = v ? "true" : "false";
      if (sb_append_bytes(sb, lit, strlen(lit)) != OK)
        goto fail;
      break;
    }
    case 'd': {
      int v = va_arg(ap, int);
      char tmp[32];
      int n = snprintf(tmp, sizeof(tmp), "%d", v);
      if (n < 0)
        goto fail;
      if (sb_append_bytes(sb, tmp, (size_t)n) != OK)
        goto fail;
      break;
    }
    case 'l': {
      long v = va_arg(ap, long);
      char tmp[32];
      int n = snprintf(tmp, sizeof(tmp), "%ld", v);
      if (n < 0)
        goto fail;
      if (sb_append_bytes(sb, tmp, (size_t)n) != OK)
        goto fail;
      break;
    }
    default:
      goto fail;
    }
    // next literal chunk starts after the specifier
    prev = i + 1;
  }

  // append remainder
  size_t tail = strlen(fmt + prev);
  if (tail > 0) {
    if (sb_append_bytes(sb, fmt + prev, tail) != OK)
      goto fail;
  }

  va_end(ap);
  return OK;

fail:
  va_end(ap);
  return ERR;
}

/* Adds a comma between elements when needed. This is a minimal state tracker
 * based on the last emitted non-whitespace byte. */
static AdbxStatus json_maybe_comma(StrBuf *sb) {
  if (!sb)
    return ERR;
  for (size_t i = sb->len; i > 0; i--) {
    unsigned char c = (unsigned char)sb->data[i - 1];
    if (isspace(c))
      continue;
    if (c == '{' || c == '[' || c == ':')
      return OK;
    return sb_append_bytes(sb, ",", 1);
  }
  return OK;
}

AdbxStatus json_obj_begin(StrBuf *sb) {
  if (!sb)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return sb_append_bytes(sb, "{", 1);
}

AdbxStatus json_obj_end(StrBuf *sb) {
  if (!sb)
    return ERR;
  return sb_append_bytes(sb, "}", 1);
}

AdbxStatus json_arr_begin(StrBuf *sb) {
  if (!sb)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return sb_append_bytes(sb, "[", 1);
}

AdbxStatus json_arr_end(StrBuf *sb) {
  if (!sb)
    return ERR;
  return sb_append_bytes(sb, "]", 1);
}

AdbxStatus json_kv_obj_begin(StrBuf *sb, const char *key) {
  if (!sb || !key)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  if (json_append(sb, "\"%s\":", key) != OK)
    return ERR;
  return sb_append_bytes(sb, "{", 1);
}

AdbxStatus json_kv_arr_begin(StrBuf *sb, const char *key) {
  if (!sb || !key)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  if (json_append(sb, "\"%s\":", key) != OK)
    return ERR;
  return sb_append_bytes(sb, "[", 1);
}

AdbxStatus json_kv_str(StrBuf *sb, const char *key, const char *val) {
  if (!sb || !key || !val)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "\"%s\":\"%s\"", key, val);
}

AdbxStatus json_kv_u64(StrBuf *sb, const char *key, uint64_t val) {
  if (!sb || !key)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "\"%s\":%U", key, val);
}

AdbxStatus json_kv_l(StrBuf *sb, const char *key, long val) {
  if (!sb || !key)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "\"%s\":%l", key, val);
}

AdbxStatus json_kv_bool(StrBuf *sb, const char *key, int val) {
  if (!sb || !key)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "\"%s\":%b", key, val);
}

AdbxStatus json_kv_null(StrBuf *sb, const char *key) {
  if (!sb || !key)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "\"%s\":null", key);
}

AdbxStatus json_arr_elem_str(StrBuf *sb, const char *val) {
  if (!sb || !val)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "\"%s\"", val);
}

AdbxStatus json_arr_elem_u64(StrBuf *sb, uint64_t val) {
  if (!sb)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "%U", val);
}

AdbxStatus json_arr_elem_l(StrBuf *sb, long val) {
  if (!sb)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "%l", val);
}

AdbxStatus json_arr_elem_bool(StrBuf *sb, int val) {
  if (!sb)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "%b", val);
}

AdbxStatus json_arr_elem_null(StrBuf *sb) {
  if (!sb)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return json_append(sb, "null");
}

AdbxStatus json_arr_elem_raw_json(StrBuf *sb, const char *raw_json) {
  if (!sb || !raw_json)
    return ERR;
  if (json_maybe_comma(sb) != OK)
    return ERR;
  return sb_append_bytes(sb, raw_json, strlen(raw_json));
}

AdbxStatus json_rpc_begin(StrBuf *sb) {
  if (json_obj_begin(sb) != OK)
    return ERR;
  return json_kv_str(sb, "jsonrpc", "2.0");
}

/* --------------------------------- decode new -------------------------------
 */
// JsonArrIter and JsonGetter are defined in json_codec.h.

/*
 * Decodes JSON string content (WITHOUT surrounding quotes) into a newly
 * allocated NUL-terminated C string.
 *
 * Unescape:
 *  - \" \\ \/ \b \f \n \r \t
 *  - \uXXXX (UTF-16 code units), including surrogate pairs, converted to UTF-8
 *
 * Notes:
 *  - 's' is NOT required to be NUL-terminated; use 'len'.
 *  - Output is allocated and must be freed by caller.
 *  - This does not validate UTF-8 for non-escaped bytes; it passes bytes >=
 * 0x20 through.
 *
 * Return OK on success, ERR on error/bad input.
 */
static AdbxStatus json_string_unescape_alloc(const char *s, size_t len,
                                             char **out) {
  if (!s || !out)
    return ERR;

  // Worst-case decoded output is <= input len (escape sequences shrink).
  // Surrogate pairs (\uXXXX\uYYYY = 12 bytes) decode to <= 4 UTF-8 bytes.
  // So len + 1 is safe for NUL.
  char *dst = (char *)xmalloc(len + 1);
  if (!dst)
    return ERR;

  size_t j = 0;

  for (size_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)s[i];

    if (c != '\\') {
      // JSON does not allow unescaped control chars, but if present we treat as
      // error.
      if (c < 0x20) {
        free(dst);
        return ERR;
      }
      dst[j++] = (char)c;
      continue;
    }

    // We saw '\', must have at least one more char
    if (i + 1 >= len) {
      free(dst);
      return ERR;
    }
    char e = s[++i];

    switch (e) {
    case '\"':
      dst[j++] = '\"';
      break;
    case '\\':
      dst[j++] = '\\';
      break;
    case '/':
      dst[j++] = '/';
      break;
    case 'b':
      dst[j++] = '\b';
      break;
    case 'f':
      dst[j++] = '\f';
      break;
    case 'n':
      dst[j++] = '\n';
      break;
    case 'r':
      dst[j++] = '\r';
      break;
    case 't':
      dst[j++] = '\t';
      break;

    case 'u': {
      // Expect 4 hex digits after \u
      if (i + 4 >= len) {
        free(dst);
        return ERR;
      }

      // decode first code unit
      uint16_t cu1 = 0;
      {
        uint16_t v = 0;
        for (int k = 0; k < 4; k++) {
          char h = s[i + 1 + (size_t)k];
          uint16_t d;
          if (h >= '0' && h <= '9')
            d = (uint16_t)(h - '0');
          else if (h >= 'a' && h <= 'f')
            d = (uint16_t)(10 + (h - 'a'));
          else if (h >= 'A' && h <= 'F')
            d = (uint16_t)(10 + (h - 'A'));
          else {
            free(dst);
            return ERR;
          }
          v = (uint16_t)((v << 4) | d);
        }
        cu1 = v;
      }

      // advance i over the 4 hex digits we just consumed
      i += 4;

      uint32_t codepoint = cu1;

      // Handle UTF-16 surrogate pairs:
      // High surrogate: 0xD800..0xDBFF
      // Low surrogate : 0xDC00..0xDFFF
      if (cu1 >= 0xD800 && cu1 <= 0xDBFF) {
        // must be followed by \uXXXX for low surrogate
        if (i + 6 >= len) {
          free(dst);
          return ERR;
        }
        if (s[i + 1] != '\\' || s[i + 2] != 'u') {
          free(dst);
          return ERR;
        }

        uint16_t cu2 = 0;
        {
          uint16_t v = 0;
          for (int k = 0; k < 4; k++) {
            char h = s[i + 3 + (size_t)k];
            uint16_t d;
            if (h >= '0' && h <= '9')
              d = (uint16_t)(h - '0');
            else if (h >= 'a' && h <= 'f')
              d = (uint16_t)(10 + (h - 'a'));
            else if (h >= 'A' && h <= 'F')
              d = (uint16_t)(10 + (h - 'A'));
            else {
              free(dst);
              return ERR;
            }
            v = (uint16_t)((v << 4) | d);
          }
          cu2 = v;
        }

        if (cu2 < 0xDC00 || cu2 > 0xDFFF) {
          free(dst);
          return ERR;
        }

        // Combine surrogate pair into code point
        // codepoint = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
        codepoint = 0x10000u + (((uint32_t)cu1 - 0xD800u) << 10) +
                    ((uint32_t)cu2 - 0xDC00u);

        // advance i over "\uXXXX" (6 chars: backslash, u, 4 hex)
        i += 6;
      } else if (cu1 >= 0xDC00 && cu1 <= 0xDFFF) {
        // Low surrogate without preceding high surrogate is invalid
        free(dst);
        return ERR;
      }

      // Encode codepoint as UTF-8
      if (codepoint <= 0x7F) {
        dst[j++] = (char)codepoint;
      } else if (codepoint <= 0x7FF) {
        dst[j++] = (char)(0xC0 | (codepoint >> 6));
        dst[j++] = (char)(0x80 | (codepoint & 0x3F));
      } else if (codepoint <= 0xFFFF) {
        dst[j++] = (char)(0xE0 | (codepoint >> 12));
        dst[j++] = (char)(0x80 | ((codepoint >> 6) & 0x3F));
        dst[j++] = (char)(0x80 | (codepoint & 0x3F));
      } else if (codepoint <= 0x10FFFF) {
        dst[j++] = (char)(0xF0 | (codepoint >> 18));
        dst[j++] = (char)(0x80 | ((codepoint >> 12) & 0x3F));
        dst[j++] = (char)(0x80 | ((codepoint >> 6) & 0x3F));
        dst[j++] = (char)(0x80 | (codepoint & 0x3F));
      } else {
        // Unicode max is 0x10FFFF
        free(dst);
        return ERR;
      }
      break;
    }

    default:
      // Invalid escape sequence
      free(dst);
      return ERR;
    }
  }

  dst[j] = '\0';
  *out = dst;
  return OK;
}

AdbxStatus json_decode_string_alloc(const char *s, size_t len, char **out) {
  return json_string_unescape_alloc(s, len, out);
}

/*
 * Returns 1 if token 't' is a JSON primitive representing null, 0 otherwise.
 * Only valid for JSMN_PRIMITIVE tokens.
 */
static AdbxTriStatus tok_is_null(const char *json, const jsmntok_t *t) {
  if (!json || !t)
    return 0;
  size_t n = (size_t)(t->end - t->start);
  return (t->type == JSMN_PRIMITIVE && n == 4 &&
          memcmp(json + t->start, "null", 4) == 0);
}

/*
 * Compares JSON token string content (WITHOUT quotes) with a string segment.
 * Return 1 if equal, 0 otherwise.
 */
static AdbxTriStatus tok_streq_n(const char *json, const jsmntok_t *t,
                                 const char *key, size_t key_len) {
  if (!json || !t || !key)
    return 0;
  if (t->type != JSMN_STRING)
    return 0;

  size_t n = (size_t)(t->end - t->start);
  return (n == key_len && memcmp(json + t->start, key, n) == 0);
}

/*
 * Skips token at index 'i' and returns the index of the next token after it.
 * This walks the token tree based on sizes and types and does not allocate.
 *
 * Return next index on success, -1 on error.
 */
static int skip_token(const jsmntok_t *toks, int ntok, int i) {
  if (!toks || ntok <= 0 || i < 0 || i >= ntok)
    return -1;

  int j = i + 1;
  const jsmntok_t *t = &toks[i];

  if (t->type == JSMN_PRIMITIVE || t->type == JSMN_STRING) {
    return j;
  }

  if (t->type == JSMN_ARRAY) {
    // For arrays, size is number of elements
    for (int k = 0; k < t->size; k++) {
      j = skip_token(toks, ntok, j);
      if (j < 0)
        return -1;
    }
    return j;
  }

  if (t->type == JSMN_OBJECT) {
    // For objects, size is number of key/value pairs
    for (int k = 0; k < t->size; k++) {
      // key
      j = skip_token(toks, ntok, j);
      if (j < 0)
        return -1;
      // value
      j = skip_token(toks, ntok, j);
      if (j < 0)
        return -1;
    }
    return j;
  }

  return -1;
}

/*
 * Finds the token index of a top-level object value by key.
 * Root token must be an object at index 0.
 *
 * Return:
 *  >=0 token index of the VALUE token on success
 *  -1 on not found
 *  -2 on error (invalid token stream)
 */
/*
 * Finds the token index of an object value by key, starting from obj_idx.
 *
 * Return:
 *  >=0 token index of the VALUE token on success
 *  -1 on not found
 *  -2 on error (invalid token stream)
 */
static int find_object_value_tok(const JsonGetter *jg, int obj_idx,
                                 const char *key, size_t key_len) {
  if (!jg || !jg->json || !key)
    return -2;
  if (obj_idx < 0 || obj_idx >= jg->ntok)
    return -2;
  if (jg->toks[obj_idx].type != JSMN_OBJECT)
    return -2;

  int i = obj_idx + 1;
  for (int pair = 0; pair < jg->toks[obj_idx].size; pair++) {
    if (i < 0 || i >= jg->ntok)
      return -2;

    const jsmntok_t *tkey = &jg->toks[i];
    if (tkey->type != JSMN_STRING)
      return -2;

    int val_i = i + 1;
    if (val_i < 0 || val_i >= jg->ntok)
      return -2;

    if (tok_streq_n(jg->json, tkey, key, key_len)) {
      return val_i;
    }

    int next = skip_token(jg->toks, jg->ntok, val_i);
    if (next < 0)
      return -2;
    i = next;
  }

  return -1;
}

/*
 * Finds a value token by a dot-delimited path (e.g., "params.raw.sql").
 *
 * Return:
 *  >=0 token index of the VALUE token on success
 *  -1 on not found
 *  -2 on error (invalid token stream or mismatched types)
 */
static int find_value_tok_path(const JsonGetter *jg, const char *path) {
  if (!jg || !path)
    return -2;

  int obj_idx = jg->root;
  const char *seg = path;

  while (*seg) {
    const char *dot = strchr(seg, '.');
    size_t len = dot ? (size_t)(dot - seg) : strlen(seg);
    if (len == 0)
      return -2;

    int val_i = find_object_value_tok(jg, obj_idx, seg, len);
    if (val_i < 0)
      return val_i;

    if (!dot)
      return val_i;

    if (jg->toks[val_i].type != JSMN_OBJECT)
      return -2;
    obj_idx = val_i;
    seg = dot + 1;
  }

  return -1;
}

/* Resolves a key path relative to 'jg' or, when 'key' is NULL, returns the
 * current root token. It borrows 'jg'.
 * Returns a token index on success, -1 when the keyed value is missing, -2 on
 * invalid input or malformed token navigation.
 */
static int resolve_value_tok_or_root(const JsonGetter *jg, const char *key) {
  if (!jg)
    return -2;
  if (!key) {
    if (jg->root < 0 || jg->root >= jg->ntok)
      return -2;
    return jg->root;
  }
  return find_value_tok_path(jg, key);
}

/* Initializes 'out' as a borrowed JsonGetter view rooted at 'root_tok'. It
 * borrows the JSON buffer and token storage from 'jg'. When narrowing in
 * place, it preserves owned token storage so jsget_destroy() remains correct.
 * Returns OK on success, ERR on invalid input or token index.
 */
static AdbxStatus set_view_root(const JsonGetter *jg, int root_tok,
                                JsonGetter *out) {
  if (!jg || !out)
    return ERR;
  if (root_tok < 0 || root_tok >= jg->ntok)
    return ERR;

  jsmntok_t *owned_toks = (out == jg) ? out->owned_toks : NULL;
  out->json = jg->json;
  out->json_len = jg->json_len;
  out->toks = jg->toks;
  out->ntok = jg->ntok;
  out->root = root_tok;
  out->owned_toks = owned_toks;
  return OK;
}

/* Initializes an array iterator from the token index 'arr_tok'. It borrows
 * 'jg' and mutates caller-owned 'it'. Explicit JSON null is treated as absent.
 * Returns YES on success, NO on null, ERR on invalid input or non-array type.
 */
static AdbxTriStatus array_iter_begin_from_tok(const JsonGetter *jg, int arr_tok,
                                               JsonArrIter *it) {
  if (!jg || !it)
    return ERR;
  if (arr_tok < 0 || arr_tok >= jg->ntok)
    return ERR;

  memset(it, 0, sizeof(*it));

  const jsmntok_t *tv = &jg->toks[arr_tok];
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;
  if (tv->type != JSMN_ARRAY)
    return ERR;

  it->arr_tok = arr_tok;
  it->idx = 0;
  it->count = tv->size;
  it->next_tok = arr_tok + 1;
  return YES;
}

/* Initializes an object-member iterator from token index 'obj_tok'. It borrows
 * 'jg' and mutates caller-owned 'it'. Explicit JSON null is treated as
 * absent. Returns YES on success, NO on null, ERR on invalid input or
 * non-object type.
 */
static AdbxTriStatus object_iter_begin_from_tok(const JsonGetter *jg, int obj_tok,
                                                JsonObjIter *it) {
  if (!jg || !it)
    return ERR;
  if (obj_tok < 0 || obj_tok >= jg->ntok)
    return ERR;

  memset(it, 0, sizeof(*it));

  const jsmntok_t *tv = &jg->toks[obj_tok];
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;
  if (tv->type != JSMN_OBJECT)
    return ERR;

  it->obj_tok = obj_tok;
  it->idx = 0;
  it->count = tv->size;
  it->next_tok = obj_tok + 1;
  return YES;
}

/*
 * Parses an unsigned 32-bit integer from a JSMN_PRIMITIVE token.
 * Return OK on success, ERR on error/bad input.
 */
static AdbxStatus tok_parse_u32(const char *json, const jsmntok_t *t,
                                uint32_t *out_u32) {
  if (!json || !t || !out_u32)
    return ERR;
  if (t->type != JSMN_PRIMITIVE)
    return ERR;

  const char *p = json + t->start;
  size_t n = (size_t)(t->end - t->start);
  if (n == 0)
    return ERR;

  // reject leading '-' for unsigned
  if (p[0] == '-')
    return ERR;

  // parse digit-by-digit to avoid needing NUL-termination
  uint64_t v = 0;
  for (size_t i = 0; i < n; i++) {
    char c = p[i];
    if (c < '0' || c > '9')
      return ERR;
    v = v * 10u + (uint64_t)(c - '0');
    if (v > 0xFFFFFFFFu)
      return ERR;
  }

  *out_u32 = (uint32_t)v;
  return OK;
}

/*
 * Parses a boolean from a JSMN_PRIMITIVE token ("true" or "false").
 * Return OK on success, ERR on error/bad input.
 */
static AdbxStatus tok_parse_bool01(const char *json, const jsmntok_t *t,
                                   int *out01) {
  if (!json || !t || !out01)
    return ERR;
  if (t->type != JSMN_PRIMITIVE)
    return ERR;

  const char *p = json + t->start;
  size_t n = (size_t)(t->end - t->start);

  if (n == 4 && memcmp(p, "true", 4) == 0) {
    *out01 = 1;
    return OK;
  }
  if (n == 5 && memcmp(p, "false", 5) == 0) {
    *out01 = 0;
    return OK;
  }

  return ERR;
}

/*
 * Parses a double from a JSMN_PRIMITIVE token.
 * Return OK on success, ERR on error/bad input.
 */
static AdbxStatus tok_parse_double(const char *json, const jsmntok_t *t,
                                   double *out_double) {
  if (!json || !t || !out_double)
    return ERR;
  if (t->type != JSMN_PRIMITIVE)
    return ERR;

  const char *p = json + t->start;
  size_t n = (size_t)(t->end - t->start);
  if (n == 0)
    return ERR;

  // Copy into a NUL-terminated buffer for strtod.
  char *tmp = (char *)xmalloc(n + 1);
  if (!tmp)
    return ERR;
  memcpy(tmp, p, n);
  tmp[n] = '\0';

  errno = 0;
  char *endp = NULL;
  double v = strtod(tmp, &endp);
  if (errno != 0 || endp != tmp + n) {
    free(tmp);
    return ERR;
  }

  *out_double = v;
  free(tmp);
  return OK;
}

/*
 * Parses a signed 64-bit integer from a JSMN_PRIMITIVE token.
 * Return OK on success, ERR on error/bad input.
 */
static AdbxStatus tok_parse_long(const char *json, const jsmntok_t *t,
                                 int64_t *out_long) {
  if (!json || !t || !out_long)
    return ERR;
  if (t->type != JSMN_PRIMITIVE)
    return ERR;

  const char *p = json + t->start;
  size_t n = (size_t)(t->end - t->start);
  if (n == 0)
    return ERR;

  // Copy into a NUL-terminated buffer for strtoll.
  char *tmp = (char *)xmalloc(n + 1);
  if (!tmp)
    return ERR;
  memcpy(tmp, p, n);
  tmp[n] = '\0';

  errno = 0;
  char *endp = NULL;
  long long v = strtoll(tmp, &endp, 10);
  if (errno != 0 || endp != tmp + n) {
    free(tmp);
    return ERR;
  }

  *out_long = (int64_t)v;
  free(tmp);
  return OK;
}

/* Parses one JSON document into the provided 'tokens' buffer and initializes
 * the root JsonGetter view. It borrows jg/json and mutates jg in place. Side
 * effects: writes token metadata into 'tokens'. Error semantics: returns OK on
 * success, ERR on parse/type failures.
 */
static AdbxStatus jsget_parse_into_tokens(JsonGetter *jg, jsmntok_t *tokens,
                                          uint32_t token_cap) {
  assert(jg);
  assert(jg->json);
  assert(jg->json_len > 0);
  assert(tokens);
  assert(token_cap);

  jsmn_parser p;
  jsmn_init(&p);

  int ntok = jsmn_parse(&p, jg->json, jg->json_len, tokens, token_cap);
  if (ntok <= 0 || (uint32_t)ntok > token_cap)
    return ERR;

  // Root must be an object for all current JsonGetter callers.
  if (tokens[0].type != JSMN_OBJECT)
    return ERR;

  jg->toks = tokens;
  jg->ntok = ntok;
  jg->root = 0;
  return OK;
}

AdbxStatus jsget_init(JsonGetter *jg, const char *json, size_t json_len,
                      JsonTokBuf *tok_buf) {
  if (!jg || !json || json_len == 0 || !tok_buf)
    return ERR;

  memset(jg, 0, sizeof(*jg));
  jg->json = json;
  jg->json_len = json_len;
  jg->owned_toks = NULL;
  return jsget_parse_into_tokens(jg, tok_buf->toks,
                                 (uint32_t)ARRLEN(tok_buf->toks));
}

AdbxStatus jsget_create(JsonGetter *jg, const char *json, size_t json_len) {
  if (!jg || !json || json_len == 0)
    return ERR;

  memset(jg, 0, sizeof(*jg));
  jg->json = json;
  jg->json_len = json_len;

  jsmn_parser p;
  jsmn_init(&p);

  // First pass: count required tokens without storing them.
  int needed = jsmn_parse(&p, json, json_len, NULL, 0);
  if (needed <= 0)
    return ERR;
  if ((uint32_t)needed > JSON_GETTER_MAX_ALLOC_TOKENS)
    return ERR;

  jg->owned_toks = (jsmntok_t *)xmalloc((size_t)needed * sizeof(jsmntok_t));
  if (jsget_parse_into_tokens(jg, jg->owned_toks, (uint32_t)needed) != OK) {
    jsget_destroy(jg);
    return ERR;
  }
  return OK;
}

void jsget_destroy(JsonGetter *jg) {
  if (!jg)
    return;
  free(jg->owned_toks);
  memset(jg, 0, sizeof(*jg));
}

AdbxTriStatus jsget_u32(const JsonGetter *jg, const char *key,
                        uint32_t *out_u32) {
  if (!jg || !key || !out_u32)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];

  // treat explicit null as "missing"
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;

  if (tok_parse_u32(jg->json, tv, out_u32) != OK)
    return ERR;
  return YES;
}

AdbxTriStatus jsget_bool01(const JsonGetter *jg, const char *key, int *out01) {
  if (!jg || !key || !out01)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];

  // treat explicit null as "missing"
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;

  if (tok_parse_bool01(jg->json, tv, out01) != OK)
    return ERR;
  return YES;
}

AdbxTriStatus jsget_f64(const JsonGetter *jg, const char *key,
                        double *out_double) {
  if (!jg || !key || !out_double)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];

  // treat explicit null as "missing"
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;

  if (tok_parse_double(jg->json, tv, out_double) != OK)
    return ERR;
  return YES;
}

AdbxTriStatus jsget_i64(const JsonGetter *jg, const char *key,
                        int64_t *out_long) {
  if (!jg || !key || !out_long)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];

  // treat explicit null as "missing"
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;

  if (tok_parse_long(jg->json, tv, out_long) != OK)
    return ERR;
  return YES;
}

/*
 * Checks whether a key path exists and is not JSON null.
 * Return yes/no/err.
 */
AdbxTriStatus jsget_exists_nonnull(const JsonGetter *jg, const char *key) {
  if (!jg || !key)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;
  return YES;
}

AdbxTriStatus jsget_string_span(const JsonGetter *jg, const char *key,
                                JsonStrSpan *out) {
  if (!jg || !key || !out)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];

  // treat explicit null as "missing"
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;

  if (tv->type != JSMN_STRING)
    return ERR;

  out->ptr = jg->json + tv->start;
  out->len = (size_t)(tv->end - tv->start);
  return YES;
}

AdbxTriStatus jsget_string_decode_alloc(const JsonGetter *jg, const char *key,
                                        char **out_nul) {
  if (!jg || !key || !out_nul)
    return ERR;

  JsonStrSpan sp = {0};
  int rc = jsget_string_span(jg, key, &sp);
  if (rc != YES)
    return rc;

  if (json_string_unescape_alloc(sp.ptr, sp.len, out_nul) != OK)
    return ERR;
  return YES;
}

AdbxTriStatus json_span_decode_alloc(const JsonStrSpan *sp, char **out_nul) {
  if (!sp || !out_nul)
    return ERR;
  if (json_string_unescape_alloc(sp->ptr, sp->len, out_nul) != OK)
    return ERR;
  return YES;
}

AdbxTriStatus jsget_object(const JsonGetter *jg, const char *key,
                           JsonGetter *out) {
  if (!jg || !key || !out)
    return ERR;

  int val_i = find_value_tok_path(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;

  const jsmntok_t *tv = &jg->toks[val_i];

  // treat explicit null as "missing"
  if (tv->type == JSMN_PRIMITIVE && tok_is_null(jg->json, tv))
    return NO;
  if (tv->type != JSMN_OBJECT)
    return ERR;

  if (set_view_root(jg, val_i, out) != OK)
    return ERR;
  return YES;
}

AdbxTriStatus jsget_array_strings_begin(const JsonGetter *jg, const char *key,
                                        JsonArrIter *it) {
  if (!jg || !it)
    return ERR;

  int val_i = resolve_value_tok_or_root(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;
  return array_iter_begin_from_tok(jg, val_i, it);
}

AdbxTriStatus jsget_array_strings_next(const JsonGetter *jg, JsonArrIter *it,
                                       JsonStrSpan *out_elem) {
  if (!jg || !it || !out_elem)
    return ERR;

  if (it->idx >= it->count)
    return NO;

  int i = it->next_tok;
  if (i < 0 || i >= jg->ntok)
    return ERR;

  const jsmntok_t *te = &jg->toks[i];
  if (te->type != JSMN_STRING)
    return ERR;

  out_elem->ptr = jg->json + te->start;
  out_elem->len = (size_t)(te->end - te->start);

  // Move cursor to next element token by skipping this element.
  int next = skip_token(jg->toks, jg->ntok, i);
  if (next < 0)
    return ERR;

  it->next_tok = next;
  it->idx++;
  return YES;
}

AdbxTriStatus jsget_array_objects_begin(const JsonGetter *jg, const char *key,
                                        JsonArrIter *it) {
  if (!jg || !it)
    return ERR;

  int val_i = resolve_value_tok_or_root(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;
  return array_iter_begin_from_tok(jg, val_i, it);
}

AdbxTriStatus jsget_array_objects_next(const JsonGetter *jg, JsonArrIter *it,
                                       JsonGetter *out_obj) {
  if (!jg || !it || !out_obj)
    return ERR;
  if (it->idx >= it->count)
    return NO;

  int i = it->next_tok;
  if (i < 0 || i >= jg->ntok)
    return ERR;

  const jsmntok_t *te = &jg->toks[i];
  if (te->type != JSMN_OBJECT)
    return ERR;

  if (set_view_root(jg, i, out_obj) != OK)
    return ERR;

  int next = skip_token(jg->toks, jg->ntok, i);
  if (next < 0)
    return ERR;

  it->next_tok = next;
  it->idx++;
  return YES;
}

AdbxTriStatus jsget_object_members_begin(const JsonGetter *jg,
                                         const char *key, JsonObjIter *it) {
  if (!jg || !it)
    return ERR;

  int val_i = resolve_value_tok_or_root(jg, key);
  if (val_i == -1)
    return NO;
  if (val_i == -2)
    return ERR;
  return object_iter_begin_from_tok(jg, val_i, it);
}

AdbxTriStatus jsget_object_members_next(const JsonGetter *jg, JsonObjIter *it,
                                        JsonStrSpan *out_key,
                                        JsonGetter *out_val) {
  if (!jg || !it || !out_key || !out_val)
    return ERR;
  if (it->idx >= it->count)
    return NO;

  int key_i = it->next_tok;
  if (key_i < 0 || key_i >= jg->ntok)
    return ERR;

  const jsmntok_t *tkey = &jg->toks[key_i];
  if (tkey->type != JSMN_STRING)
    return ERR;

  int val_i = key_i + 1;
  if (val_i < 0 || val_i >= jg->ntok)
    return ERR;

  out_key->ptr = jg->json + tkey->start;
  out_key->len = (size_t)(tkey->end - tkey->start);
  if (set_view_root(jg, val_i, out_val) != OK)
    return ERR;

  int next = skip_token(jg->toks, jg->ntok, val_i);
  if (next < 0)
    return ERR;

  it->next_tok = next;
  it->idx++;
  return YES;
}

AdbxTriStatus jsget_top_level_validation(const JsonGetter *jg,
                                         const char *obj_key,
                                         const char *const *allowed,
                                         size_t n_allowed,
                                         JsonStrSpan *out_unknown_key) {
  if (!jg || !allowed)
    return ERR;
  if (out_unknown_key) {
    out_unknown_key->ptr = NULL;
    out_unknown_key->len = 0;
  }

  int obj_idx = jg->root;
  if (obj_key) {
    obj_idx = find_value_tok_path(jg, obj_key);
    if (obj_idx == -1)
      return NO;
    if (obj_idx == -2)
      return ERR;
  }

  if (obj_idx < 0 || obj_idx >= jg->ntok)
    return ERR;
  if (jg->toks[obj_idx].type != JSMN_OBJECT)
    return ERR;

  int i = obj_idx + 1;
  for (int pair = 0; pair < jg->toks[obj_idx].size; pair++) {
    if (i < 0 || i >= jg->ntok)
      return ERR;
    const jsmntok_t *tkey = &jg->toks[i];
    if (tkey->type != JSMN_STRING)
      return ERR;

    (void)(tkey->end - tkey->start);
    int ok = 0;
    for (size_t k = 0; k < n_allowed; k++) {
      size_t alen = strlen(allowed[k]);
      if (tok_streq_n(jg->json, tkey, allowed[k], alen)) {
        ok = 1;
        break;
      }
    }
    if (!ok) {
      if (out_unknown_key) {
        out_unknown_key->ptr = jg->json + tkey->start;
        out_unknown_key->len = (size_t)(tkey->end - tkey->start);
      }
      return NO;
    }

    int val_i = i + 1;
    int next = skip_token(jg->toks, jg->ntok, val_i);
    if (next < 0)
      return ERR;
    i = next;
  }
  return YES;
}

AdbxTriStatus jsget_simple_rpc_validation(JsonGetter *jg) {
  if (!jg || !jg->json)
    return ERR;

  const jsmntok_t *root = &jg->toks[jg->root];
  if (root->type != JSMN_OBJECT)
    return NO;
  if ((size_t)root->end > jg->json_len)
    return ERR;

  // root object should end with } and whitespaces is any
  for (size_t i = (size_t)root->end; i < jg->json_len; i++) {
    if (!isspace((unsigned char)jg->json[i]))
      return ERR;
  }

  JsonStrSpan jsonrpc = {0};
  JsonStrSpan method = {0};

  if (jsget_string_span(jg, "jsonrpc", &jsonrpc) != YES)
    return NO;
  if (jsget_string_span(jg, "method", &method) != YES)
    return NO;

  int id_i = find_value_tok_path(jg, "id");
  if (id_i < 0)
    return NO;
  const jsmntok_t *tid = &jg->toks[id_i];
  if (tid->type == JSMN_STRING) {
    /* ok */
  } else if (tid->type == JSMN_PRIMITIVE) {
    if (tok_is_null(jg->json, tid))
      return NO;
    uint32_t id = 0;
    if (tok_parse_u32(jg->json, tid, &id) != OK)
      return NO;
  } else {
    return NO;
  }

  if (jsonrpc.len != 3 || memcmp(jsonrpc.ptr, "2.0", 3) != 0)
    return NO;
  if (method.len == 0)
    return NO;

  return YES;
}
