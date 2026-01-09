#include "json_codec.h"
#include "utils.h"
#include "string_op.h"

#include <ctype.h>
#include <limits.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

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
static int sb_append_json_escaped(StrBuf *sb, const char *s, size_t len) {
    if (!sb || !s) return ERR;

    // we loop until a '\0' is encountered or until we append 'len' bytes
    for (size_t i = 0; i < len && s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        // append a \ before the char 
        switch (c) {
            case '\"':
                if (sb_append_bytes(sb, "\\\"", 2) != OK) return ERR;
                break;
            case '\\':
                if (sb_append_bytes(sb, "\\\\", 2) != OK) return ERR;
                break;
            case '\b':
                if (sb_append_bytes(sb, "\\b", 2) != OK) return ERR;
                break;
            case '\f':
                if (sb_append_bytes(sb, "\\f", 2) != OK) return ERR;
                break;
            case '\n':
                if (sb_append_bytes(sb, "\\n", 2) != OK) return ERR;
                break;
            case '\r':
                if (sb_append_bytes(sb, "\\r", 2) != OK) return ERR;
                break;
            case '\t':
                if (sb_append_bytes(sb, "\\t", 2) != OK) return ERR;
                break;
            default:
                if (c < 0x20) {
                    // \u00XX + '\0' = 7 chars len
                    // example: 0x1F become \u001F (+ '\0')
                    char tmp[7];
                    int n = snprintf(tmp, sizeof(tmp), "\\u%04X", (unsigned)c);
                    // without counting the '\0' there should be 6 bytes
                    if (n != 6) return ERR;
                    if (sb_append_bytes(sb, tmp, (size_t)n) != OK) return ERR;
                } else {
                    if (sb_append_bytes(sb, &c, 1) != OK) return ERR;
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
static int json_append(StrBuf *sb, const char *fmt, ...) {
    if (!sb || !fmt) return ERR;

    va_list ap;
    va_start(ap, fmt);

    size_t prev = 0;

    for (size_t i = 0; fmt[i] != '\0'; i++) {
        if (fmt[i] != '%') continue;

        // append literal chunk before %
        if (i > prev) {
            if (sb_append_bytes(sb, fmt + prev, i - prev) != OK) goto fail;
        }

        // parse specifier
        char spec = fmt[++i];
        if (spec == '\0') goto fail; // dangling '%'

        switch (spec) {
            case '%': {
                if (sb_append_bytes(sb, "%", 1) != OK) goto fail;
                break;
            }
            case 'c': {
                char c = (char)va_arg(ap, int);
                if (sb_append_json_escaped(sb, &c, 1) != OK) goto fail;
                break;
            }
            case 's': {
                const char *s = va_arg(ap, const char *);
                if (!s) goto fail;
                if (sb_append_json_escaped(sb, s, SIZE_MAX) != OK) goto fail;
                break;
            }
            case 'u': { 
                uint32_t v = va_arg(ap, uint32_t);
                char tmp[32];
                int n = snprintf(tmp, sizeof(tmp), "%" PRIu32, v);
                if (n < 0) goto fail;
                if (sb_append_bytes(sb, tmp, (size_t)n) != OK) goto fail;
                break;
            }
            case 'U': { 
                uint64_t v = va_arg(ap, uint64_t);
                char tmp[64];
                int n = snprintf(tmp, sizeof(tmp), "%" PRIu64, v);
                if (n < 0) goto fail;
                if (sb_append_bytes(sb, tmp, (size_t)n) != OK) goto fail;
                break;
            }
            case 'b': {
                int v = va_arg(ap, int);
                const char *lit = v ? "true" : "false";
                if (sb_append_bytes(sb, lit, strlen(lit)) != OK) goto fail;
                break;
            }
            case 'd': {
                int v = va_arg(ap, int);
                char tmp[32];
                int n = snprintf(tmp, sizeof(tmp), "%d", v);
                if (n < 0) goto fail;
                if (sb_append_bytes(sb, tmp, (size_t)n) != OK) goto fail;
                break;
            }
            case 'l': {
                long v = va_arg(ap, long);
                char tmp[32];
                int n = snprintf(tmp, sizeof(tmp), "%ld", v);
                if (n < 0) goto fail;
                if (sb_append_bytes(sb, tmp, (size_t)n) != OK) goto fail;
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
        if (sb_append_bytes(sb, fmt + prev, tail) != OK) goto fail;
    }

    va_end(ap);
    return OK;

fail:
    va_end(ap);
    return ERR;
}

/* Stores 'a' * 'b' into 'out' handling overflow of uint32_t. */
static inline void safe_mul_u32(uint32_t a, uint32_t b, uint64_t *out) {
    if (!out) return;
    *out = (uint64_t)a * (uint64_t)b;
}

/* Adds a comma between elements when needed. This is a minimal state tracker
 * based on the last emitted non-whitespace byte. */
static int json_maybe_comma(StrBuf *sb) {
    if (!sb) return ERR;
    for (size_t i = sb->len; i > 0; i--) {
        unsigned char c = (unsigned char)sb->data[i - 1];
        if (isspace(c)) continue;
        if (c == '{' || c == '[' || c == ':') return OK;
        return sb_append_bytes(sb, ",", 1);
    }
    return OK;
}

int json_obj_begin(StrBuf *sb) {
    if (!sb) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return sb_append_bytes(sb, "{", 1);
}

int json_obj_end(StrBuf *sb) {
    if (!sb) return ERR;
    return sb_append_bytes(sb, "}", 1);
}

int json_arr_begin(StrBuf *sb) {
    if (!sb) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return sb_append_bytes(sb, "[", 1);
}

int json_arr_end(StrBuf *sb) {
    if (!sb) return ERR;
    return sb_append_bytes(sb, "]", 1);
}

int json_kv_obj_begin(StrBuf *sb, const char *key) {
    if (!sb || !key) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    if (json_append(sb, "\"%s\":", key) != OK) return ERR;
    return sb_append_bytes(sb, "{", 1);
}

int json_kv_arr_begin(StrBuf *sb, const char *key) {
    if (!sb || !key) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    if (json_append(sb, "\"%s\":", key) != OK) return ERR;
    return sb_append_bytes(sb, "[", 1);
}

int json_kv_str(StrBuf *sb, const char *key, const char *val) {
    if (!sb || !key || !val) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "\"%s\":\"%s\"", key, val);
}

int json_kv_u64(StrBuf *sb, const char *key, uint64_t val) {
    if (!sb || !key) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "\"%s\":%U", key, val);
}

int json_kv_l(StrBuf *sb, const char *key, long val) {
    if (!sb || !key) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "\"%s\":%l", key, val);
}

int json_kv_bool(StrBuf *sb, const char *key, int val) {
    if (!sb || !key) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "\"%s\":%b", key, val);
}

int json_arr_elem_str(StrBuf *sb, const char *val) {
    if (!sb || !val) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "\"%s\"", val);
}

int json_arr_elem_u64(StrBuf *sb, uint64_t val) {
    if (!sb) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "%U", val);
}

int json_arr_elem_l(StrBuf *sb, long val) {
    if (!sb) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "%l", val);
}

int json_arr_elem_bool(StrBuf *sb, int val) {
    if (!sb) return ERR;
    if (json_maybe_comma(sb) != OK) return ERR;
    return json_append(sb, "%b", val);
}

int json_rpc_begin(StrBuf *sb) {
    if (json_obj_begin(sb) != OK) return ERR;
    return json_kv_str(sb, "jsonrpc", "2.0");
}

/* --------------------------- encoding objects --------------------------- */

static int json_qr_ok(StrBuf *sb, const QueryResult *qr) {
    // nrows*ncols should fit in addressable range of cells
    uint64_t cell_count_u64 = 0;
    safe_mul_u32(qr->nrows, qr->ncols, &cell_count_u64);
    if (cell_count_u64 > SIZE_MAX) return ERR;

    // if there are cols/rows, require corresponding pointers
    if (qr->ncols > 0 && !qr->cols) return ERR;
    if (cell_count_u64 > 0 && !qr->cells) return ERR;

    if (json_obj_begin(sb) != OK) return ERR;
    if (json_kv_u64(sb, "exec_ms", qr->exec_ms) != OK) return ERR;

    // columns
    if (json_maybe_comma(sb) != OK) return ERR;
    if (json_append(sb, "\"columns\":") != OK) return ERR;
    if (json_arr_begin(sb) != OK) return ERR;
    for (uint32_t c = 0; c < qr->ncols; ++c) {
        const QRColumn *col = qr_get_col((QueryResult *)qr, c);
        // defensive, in case internals are partially set or missing entirely
        const char *name = "";
        const char *type = "";
        if (col) {
            name = col->name ? col->name : "";
            type = col->type ? col->type : "";
        }

        if (json_obj_begin(sb) != OK) return ERR;
        if (json_kv_str(sb, "name", name) != OK) return ERR;
        if (json_kv_str(sb, "type", type) != OK) return ERR;
        if (json_obj_end(sb) != OK) return ERR;
    }
    if (json_arr_end(sb) != OK) return ERR;

    // rows
    if (json_maybe_comma(sb) != OK) return ERR;
    if (json_append(sb, "\"rows\":") != OK) return ERR;
    if (json_arr_begin(sb) != OK) return ERR;

    for (uint32_t r = 0; r < qr->nrows; ++r) {
        if (json_arr_begin(sb) != OK) return ERR;

        for (uint32_t c = 0; c < qr->ncols; ++c) {
            const char *cell = qr_get_cell(qr, r, c);
            if (!cell) {
                if (json_maybe_comma(sb) != OK) return ERR;
                if (json_append(sb, "null") != OK) return ERR;
            } else {
                /* quote + escaped string content + quote */
                if (json_arr_elem_str(sb, cell) != OK) return ERR;
            }
        }

        if (json_arr_end(sb) != OK) return ERR;
    }
    if (json_arr_end(sb) != OK) return ERR;

    // rowcount + truncated
    if (json_kv_u64(sb, "rowcount", qr->nrows) != OK) return ERR;
    if (json_kv_bool(sb, "truncated", qr->truncated ? 1 : 0) != OK) return ERR;
    if (json_obj_end(sb) != OK) return ERR;

    return OK;
}

static int json_qr_err(StrBuf *sb, const QueryResult *qr) {
    const char *msg = qr->err_msg ? qr->err_msg : "";
    if (json_obj_begin(sb) != OK) return ERR;
    if (json_kv_u64(sb, "exec_ms", qr->exec_ms) != OK) return ERR;
    if (json_kv_str(sb, "message", msg) != OK) return ERR;
    if (json_obj_end(sb) != OK) return ERR;
    return OK;
}

int qr_to_jsonrpc(const QueryResult *qr, char **out_json, size_t *out_len) {
    if (!out_json || !out_len) return ERR;
    *out_json = NULL;
    *out_len  = 0;

    if (!qr) return ERR;

    StrBuf sb = {0};

    // begin JSON-RPC envelope
    if (json_obj_begin(&sb) != OK) goto err;
    if (json_kv_str(&sb, "jsonrpc", "2.0") != OK) goto err;
    if (json_kv_u64(&sb, "id", qr->id) != OK) goto err;

    if (qr->status == QR_ERROR) {
        if (json_maybe_comma(&sb) != OK) goto err;
        if (json_append(&sb, "\"error\":") != OK) goto err;
        if (json_qr_err(&sb, qr) != OK) goto err;
    } else {
        if (json_maybe_comma(&sb) != OK) goto err;
        if (json_append(&sb, "\"result\":") != OK) goto err;
        if (json_qr_ok(&sb, qr) != OK) goto err;
    }
    if (json_obj_end(&sb) != OK) goto err;

    // materialize output (exact size; not NUL-terminated)
    char *out = xmalloc(sb.len);
    memcpy(out, sb.data, sb.len);
    *out_json = out;
    *out_len  = sb.len;

    sb_clean(&sb);
    return OK;

err:
    sb_clean(&sb);
    *out_json = NULL;
    *out_len  = 0;
    return ERR;
}


/* --------------------------------- decode ------------------------------- */

static const char *json_skip_ws(const char *p) {
    if (!p) return NULL;
    while (*p && isspace((unsigned char)*p)) p++;
    return p;
}

/* Parses a JSON string. If 'out' is non-NULL, appends decoded bytes to it.
 * Returns pointer to the first byte after the closing quote, or NULL on error. */
static const char *json_parse_string(const char *p, StrBuf *out) {
    if (!p || *p != '"') return NULL;
    p++;
    while (*p) {
        unsigned char c = (unsigned char)*p;
        if (c == '"') return p + 1;
        if (c == '\\') {
            p++;
            if (!*p) return NULL;
            switch (*p) {
                case '"':  c = '"';  break;
                case '\\': c = '\\'; break;
                case '/':  c = '/';  break;
                case 'b':  c = '\b'; break;
                case 'f':  c = '\f'; break;
                case 'n':  c = '\n'; break;
                case 'r':  c = '\r'; break;
                case 't':  c = '\t'; break;
                default:
                    return NULL;
            }
            if (out && sb_append_bytes(out, &c, 1) != OK) return NULL;
            p++;
            continue;
        }
        if (out && sb_append_bytes(out, &c, 1) != OK) return NULL;
        p++;
    }
    return NULL;
}

static const char *json_skip_value(const char *p);

static const char *json_skip_object(const char *p) {
    if (!p || *p != '{') return NULL;
    p = json_skip_ws(p + 1);
    if (!p) return NULL;
    if (*p == '}') return p + 1;

    for (;;) {
        p = json_skip_ws(p);
        p = json_parse_string(p, NULL);
        if (!p) return NULL;
        p = json_skip_ws(p);
        if (!p || *p != ':') return NULL;
        p = json_skip_ws(p + 1);
        p = json_skip_value(p);
        if (!p) return NULL;
        p = json_skip_ws(p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == '}') return p + 1;
        return NULL;
    }
}

static const char *json_skip_array(const char *p) {
    if (!p || *p != '[') return NULL;
    p = json_skip_ws(p + 1);
    if (!p) return NULL;
    if (*p == ']') return p + 1;

    for (;;) {
        p = json_skip_value(p);
        if (!p) return NULL;
        p = json_skip_ws(p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') return p + 1;
        return NULL;
    }
}

static const char *json_skip_number(const char *p) {
    if (!p) return NULL;
    if (*p == '-') p++;
    if (!isdigit((unsigned char)*p)) return NULL;
    while (isdigit((unsigned char)*p)) p++;
    if (*p == '.' || *p == 'e' || *p == 'E') {
        p++;
        if (*p == '-' || *p == '+') p++;
        if (!isdigit((unsigned char)*p)) return NULL;
        while (isdigit((unsigned char)*p)) p++;
    }
    return p;
}

static const char *json_skip_value(const char *p) {
    p = json_skip_ws(p);
    if (!p || !*p) return NULL;
    if (*p == '"') return json_parse_string(p, NULL);
    if (*p == '{') return json_skip_object(p);
    if (*p == '[') return json_skip_array(p);
    if (*p == 't') return (strncmp(p, "true", 4) == 0) ? p + 4 : NULL;
    if (*p == 'f') return (strncmp(p, "false", 5) == 0) ? p + 5 : NULL;
    if (*p == 'n') return (strncmp(p, "null", 4) == 0) ? p + 4 : NULL;
    if (*p == '-' || isdigit((unsigned char)*p)) return json_skip_number(p);
    return NULL;
}

static int json_key_matches(const char *p, const char *key, const char **out_next) {
    StrBuf sb = {0};
    const char *next = json_parse_string(p, &sb);
    if (!next) {
        sb_clean(&sb);
        return ERR;
    }
    if (sb_append_bytes(&sb, "\0", 1) != OK) {
        sb_clean(&sb);
        return ERR;
    }
    int match = (strcmp(sb.data ? sb.data : "", key) == 0);
    sb_clean(&sb);
    if (out_next) *out_next = next;
    return match ? YES : NO;
}

static int json_find_in_object(const char *p, const char **parts, size_t depth,
                               const char **out_val) {
    if (!p || !parts || depth == 0) return ERR;
    p = json_skip_ws(p);
    if (!p || *p != '{') return ERR;
    p = json_skip_ws(p + 1);
    if (!p) return ERR;
    if (*p == '}') return NO;

    for (;;) {
        p = json_skip_ws(p);
        if (!p || *p != '"') return ERR;
        int match = json_key_matches(p, parts[0], &p);
        if (match == ERR) return ERR;
        p = json_skip_ws(p);
        if (!p || *p != ':') return ERR;
        p = json_skip_ws(p + 1);
        if (match == YES) {
            if (depth == 1) {
                if (out_val) *out_val = p;
                return YES;
            }
            if (*p != '{') return ERR;
            return json_find_in_object(p, parts + 1, depth - 1, out_val);
        }
        p = json_skip_value(p);
        if (!p) return ERR;
        p = json_skip_ws(p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == '}') return NO;
        return ERR;
    }
}

static int json_find_path_value(const char *json, char **parts, size_t depth,
                                const char **out_val) {
    if (!json || !parts || depth == 0) return ERR;
    const char *p = json;
    const char *val = NULL;

    for (size_t i = 0; i < depth; i++) {
        const char *single[1] = { parts[i] };
        int rc = json_find_in_object(p, single, 1, &val);
        if (rc != YES) return rc;
        if (i + 1 == depth) {
            if (out_val) *out_val = val;
            return YES;
        }
        val = json_skip_ws(val);
        if (!val || *val != '{') return ERR;
        p = val;
    }
    return ERR;
}
static int json_split_path(const char *path, char **parts, size_t *out_n) {
    if (!path || !parts || !out_n) return ERR;
    size_t n = 0;
    const char *start = path;
    for (const char *p = path; ; p++) {
        if (*p == '.' || *p == '\0') {
            if (n >= 3 || p == start) {
                for (size_t i = 0; i < n; i++) free(parts[i]);
                return ERR;
            }
            size_t len = (size_t)(p - start);
            char *s = xmalloc(len + 1);
            memcpy(s, start, len);
            s[len] = '\0';
            parts[n++] = s;
            if (*p == '\0') break;
            start = p + 1;
        }
    }
    *out_n = n;
    return OK;
}

static int json_parse_uint(const char *p, uint64_t *out, const char **out_end) {
    if (!p || !out) return ERR;
    p = json_skip_ws(p);
    if (!p) return ERR;
    if (strncmp(p, "null", 4) == 0) return NO;
    if (*p == '-') return ERR;
    if (!isdigit((unsigned char)*p)) return ERR;

    uint64_t v = 0;
    while (isdigit((unsigned char)*p)) {
        uint64_t d = (uint64_t)(*p - '0');
        if (v > (UINT64_MAX - d) / 10) return ERR;
        v = v * 10 + d;
        p++;
    }
    *out = v;
    if (out_end) *out_end = p;
    return YES;
}

static int json_decode_string_value(const char *p, char **out, size_t *out_len,
                                    const char **out_end) {
    if (!p || !out) return ERR;
    p = json_skip_ws(p);
    if (!p) return ERR;
    if (strncmp(p, "null", 4) == 0) return NO;
    StrBuf sb = {0};
    const char *next = json_parse_string(p, &sb);
    if (!next) {
        sb_clean(&sb);
        return ERR;
    }
    size_t len = sb.len;
    char *s = xmalloc(len + 1);
    if (len > 0) memcpy(s, sb.data, len);
    s[len] = '\0';
    sb_clean(&sb);
    *out = s;
    if (out_len) *out_len = len;
    if (out_end) *out_end = next;
    return YES;
}

int json_get_value(const char *json, size_t json_len, const char *fmt, ...) {
    if (!json || !fmt) return ERR;
    if (json_len == SIZE_MAX) return ERR;

    char *json_buf = (char *)xmalloc(json_len + 1);
    if (json_len > 0) memcpy(json_buf, json, json_len);
    json_buf[json_len] = '\0';

    va_list ap;
    va_start(ap, fmt);

    for (size_t i = 0; fmt[i] != '\0'; i++) {
        if (fmt[i] != '%') continue;
        char spec = fmt[++i];
        if (spec == '\0') {
            va_end(ap);
            return ERR;
        }

        const char *key = va_arg(ap, const char *);
        if (!key) {
            va_end(ap);
            return ERR;
        }

        char *parts[3] = {0};
        size_t depth = 0;
        if (json_split_path(key, parts, &depth) != OK) {
            va_end(ap);
            return ERR;
        }

        const char *val = NULL;
        int rc = json_find_path_value(json_buf, parts, depth, &val);
        if (rc == NO) {
            for (size_t j = 0; j < depth; j++) free(parts[j]);
            va_end(ap);
            free(json_buf);
            return NO;
        }
        if (rc != YES || !val) {
            for (size_t j = 0; j < depth; j++) free(parts[j]);
            va_end(ap);
            free(json_buf);
            return ERR;
        }

        if (spec == 's') {
            char **out = va_arg(ap, char **);
            if (!out) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return ERR;
            }
            rc = json_decode_string_value(val, out, NULL, NULL);
            if (rc == NO) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return NO;
            }
            if (rc != YES) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return ERR;
            }
            for (size_t j = 0; j < depth; j++) free(parts[j]);
            continue;
        }

        if (spec == 'c') {
            char *out = va_arg(ap, char *);
            if (!out) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return ERR;
            }
            char *s = NULL;
            size_t len = 0;
            rc = json_decode_string_value(val, &s, &len, NULL);
            if (rc == NO) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return NO;
            }
            if (rc != YES || !s || len != 1) {
                free(s);
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return ERR;
            }
            *out = s[0];
            free(s);
            for (size_t j = 0; j < depth; j++) free(parts[j]);
            continue;
        }

        if (spec == 'u' || spec == 'U') {
            uint64_t v = 0;
            rc = json_parse_uint(val, &v, NULL);
            if (rc == NO) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return NO;
            }
            if (rc != YES) {
                for (size_t j = 0; j < depth; j++) free(parts[j]);
                va_end(ap);
                free(json_buf);
                return ERR;
            }
            if (spec == 'u') {
                uint32_t *out = va_arg(ap, uint32_t *);
                if (!out) {
                    for (size_t j = 0; j < depth; j++) free(parts[j]);
                    va_end(ap);
                    free(json_buf);
                    return ERR;
                }
                if (v > UINT32_MAX) {
                    for (size_t j = 0; j < depth; j++) free(parts[j]);
                    va_end(ap);
                    free(json_buf);
                    return ERR;
                }
                *out = (uint32_t)v;
            } else {
                uint64_t *out = va_arg(ap, uint64_t *);
                if (!out) {
                    for (size_t j = 0; j < depth; j++) free(parts[j]);
                    va_end(ap);
                    free(json_buf);
                    return ERR;
                }
                *out = v;
            }
            for (size_t j = 0; j < depth; j++) free(parts[j]);
            continue;
        }

        for (size_t j = 0; j < depth; j++) free(parts[j]);
        va_end(ap);
        free(json_buf);
        return ERR;
    }

    va_end(ap);
    free(json_buf);
    return YES;
}

/* Minimal validation for JSON-RPC inputs (NUL-terminated). */
int json_simple_validation(const char *json) {
    if (!json) return ERR;

    const char *p = json_skip_ws(json);
    if (!p || *p == '\0') return ERR;
    if (*p != '{') return NO;
    const char *end = json_skip_value(p);
    if (!end) return ERR;
    end = json_skip_ws(end);
    if (*end != '\0') return ERR;

    size_t len = strlen(json);
    char *jsonrpc = NULL;
    char *method = NULL;
    uint64_t id = 0;

    int rc = json_get_value(json, len, "%s%U%s",
            "jsonrpc", &jsonrpc,
            "id", &id,
            "method", &method);
    if (rc != YES) {
        free(jsonrpc);
        free(method);
        return rc;
    }

    int ok = YES;
    if (strcmp(jsonrpc, "2.0") != 0) ok = NO;
    if (!method || method[0] == '\0') ok = NO;
    if (id > UINT32_MAX) ok = NO;

    free(jsonrpc);
    free(method);
    return ok;
}
