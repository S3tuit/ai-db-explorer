#include "serializer.h"
#include "utils.h"

#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/* ------------------------- small growable buffer ------------------------- */

typedef struct StrBuf {
    char  *data;
    size_t len;
    size_t cap;
} StrBuf;

/* Clean the internal allocation of 'sb'. */
static void sb_clean(StrBuf *sb) {
    if (!sb) return;
    free(sb->data);
    sb->data = NULL;
    sb->len = 0;
    sb->cap = 0;
}

// TODO: think about the max size of the output and don't forget that an sql
// query for libqp must be lower than 8k chars.

/* Makes sure 'sb' has enough space for 'add' more bytes. Returns 1 on success,
 * -1 on bad input or overflow, 0 if add is 0. */
static int sb_reserve(StrBuf *sb, size_t add) {
    if (!sb) return -1;
    // overflow
    if (add > SIZE_MAX - sb->len) return -1;

    // state unchanged
    if (add == 0) return 0;

    size_t needed = sb->len + add;
    if (needed <= sb->cap) return 1;

    size_t newcap = sb->cap ? sb->cap : 256;
    while (newcap < needed) {
        if (newcap > SIZE_MAX / 2) {
            newcap = needed;
            break;
        }
        newcap *= 2;
    }

    char *p = (char *)xrealloc(sb->data, newcap);

    sb->data = p;
    sb->cap  = newcap;
    return 1;
}

/* Adds 'n' bytes starting from 'src' to sb. */
static int sb_append_bytes(StrBuf *sb, const void *src, size_t n) {
    if (!sb || (!src && n != 0)) return -1;
    if (n == 0) return 0;
    if (sb_reserve(sb, n) != 1) return -1;
    memcpy(sb->data + sb->len, src, n);
    sb->len += n;
    return 1;
}

/* ------------------------------- serializer ------------------------------ */

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
 * Return 1 on success, -1 on error/bad input.
 */
static int sb_append_json_escaped(StrBuf *sb, const char *s, size_t len) {
    if (!sb || !s) return -1;

    // we loop until a '\0' is encountered or until we append 'len' bytes
    for (size_t i = 0; i < len && s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        // append a \ before the char 
        switch (c) {
            case '\"':
                if (sb_append_bytes(sb, "\\\"", 2) != 1) return -1;
                break;
            case '\\':
                if (sb_append_bytes(sb, "\\\\", 2) != 1) return -1;
                break;
            case '\b':
                if (sb_append_bytes(sb, "\\b", 2) != 1) return -1;
                break;
            case '\f':
                if (sb_append_bytes(sb, "\\f", 2) != 1) return -1;
                break;
            case '\n':
                if (sb_append_bytes(sb, "\\n", 2) != 1) return -1;
                break;
            case '\r':
                if (sb_append_bytes(sb, "\\r", 2) != 1) return -1;
                break;
            case '\t':
                if (sb_append_bytes(sb, "\\t", 2) != 1) return -1;
                break;
            default:
                if (c < 0x20) {
                    // \u00XX + '\0' = 7 chars len
                    // example: 0x1F become \u001F (+ '\0')
                    char tmp[7];
                    int n = snprintf(tmp, sizeof(tmp), "\\u%04X", (unsigned)c);
                    // without counting the '\0' there should be 6 bytes
                    if (n != 6) return -1;
                    if (sb_append_bytes(sb, tmp, (size_t)n) != 1) return -1;
                } else {
                    if (sb_append_bytes(sb, &c, 1) != 1) return -1;
                }
                break;
        }
    }
    return 1;
}

/* This appends values of different types to 'sb' based on 'fmt'. All the values
 * appended other than 'fmt' are json-escaped. 'fmt' uses the same logic as
 * printf, the only flags supported are:
 *  - %c appends a char
 *  - %s appends a string
 *  - %u appends uint32_t
 *  - %U appends uint64_t
 */
static int serializer_append(StrBuf *sb, const char *fmt, ...) {
    if (!sb || !fmt) return -1;

    va_list ap;
    va_start(ap, fmt);

    size_t prev = 0;

    for (size_t i = 0; fmt[i] != '\0'; i++) {
        if (fmt[i] != '%') continue;

        // append literal chunk before %
        if (i > prev) {
            if (sb_append_bytes(sb, fmt + prev, i - prev) != 1) goto fail;
        }

        // parse specifier
        char spec = fmt[++i];
        if (spec == '\0') goto fail; // dangling '%'

        switch (spec) {
            case '%': {
                if (sb_append_bytes(sb, "%", 1) != 1) goto fail;
                break;
            }
            case 'c': {
                char c = (char)va_arg(ap, int);
                if (sb_append_json_escaped(sb, &c, 1) != 1) goto fail;
                break;
            }
            case 's': {
                const char *s = va_arg(ap, const char *);
                if (!s) goto fail;
                if (sb_append_json_escaped(sb, s, SIZE_MAX) != 1) goto fail;
                break;
            }
            case 'u': { 
                uint32_t v = va_arg(ap, uint32_t);
                char tmp[32];
                int n = snprintf(tmp, sizeof(tmp), "%" PRIu32, v);
                if (n < 0) goto fail;
                if (sb_append_bytes(sb, tmp, (size_t)n) != 1) goto fail;
                break;
            }
            case 'U': { 
                uint64_t v = va_arg(ap, uint64_t);
                char tmp[64];
                int n = snprintf(tmp, sizeof(tmp), "%" PRIu64, v);
                if (n < 0) goto fail;
                if (sb_append_bytes(sb, tmp, (size_t)n) != 1) goto fail;
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
        if (sb_append_bytes(sb, fmt + prev, tail) != 1) goto fail;
    }

    va_end(ap);
    return 1;

fail:
    va_end(ap);
    return -1;
}

/* Stores 'a' * 'b' into 'out' handling overflow of uint32_t. */
static inline void safe_mul_u32(uint32_t a, uint32_t b, uint64_t *out) {
    if (!out) return;
    *out = (uint64_t)a * (uint64_t)b;
}

int serializer_qr_to_jsonrpc(const QueryResult *qr, char **out_json,
                            size_t *out_len) {
    if (!out_json || !out_len) return 0;
    *out_json = NULL;
    *out_len  = 0;

    if (!qr) return 0;

    // nrows*ncols should fit in addressable range of cells
    uint64_t cell_count_u64 = 0;
    safe_mul_u32(qr->nrows, qr->ncols, &cell_count_u64);
    if (cell_count_u64 > SIZE_MAX) return -1;

    // if there are cols/rows, require corresponding pointers
    if (qr->ncols > 0 && !qr->cols) return -1;
    if (cell_count_u64 > 0 && !qr->cells) return -1;

    StrBuf sb = {0};

    // begin JSON-RPC envelope + exec_ms
    if (serializer_append(&sb,
            "{\"jsonrpc\":\"2.0\",\"id\":%u,\"result\":{"
            "\"exec_ms\":%U",
            qr->id, qr->exec_ms) != 1) goto err;

    // columns
    if (serializer_append(&sb, ",\"columns\":[") != 1) goto err;
    for (uint32_t c = 0; c < qr->ncols; ++c) {
        if (c > 0) {
            if (serializer_append(&sb, ",") != 1) goto err;
        }

        const QRColumn *col = qr_get_col((QueryResult *)qr, c);
        // defensive, in case internals are partially set or missing entirely
        const char *name = "";
        const char *type = "";
        if (col) {
            name = col->name ? col->name : "";
            type = col->type ? col->type : "";
        }

        if (serializer_append(&sb,
                "{\"name\":\"%s\",\"type\":\"%s\"}",
                name, type) != 1) goto err;
    }
    if (serializer_append(&sb, "]") != 1) goto err;

    // rows
    if (serializer_append(&sb, ",\"rows\":[") != 1) goto err;

    for (uint32_t r = 0; r < qr->nrows; ++r) {
        if (r > 0) {
            if (serializer_append(&sb, ",") != 1) goto err;
        }
        if (serializer_append(&sb, "[") != 1) goto err;

        for (uint32_t c = 0; c < qr->ncols; ++c) {
            if (c > 0) {
                if (serializer_append(&sb, ",") != 1) goto err;
            }

            const char *cell = qr_get_cell(qr, r, c);
            if (!cell) {
                if (serializer_append(&sb, "null") != 1) goto err;
            } else {
                /* quote + escaped string content + quote */
                if (serializer_append(&sb, "\"%s\"", cell) != 1) goto err;
            }
        }

        if (serializer_append(&sb, "]") != 1) goto err;
    }

    if (serializer_append(&sb, "]") != 1) goto err;

    // rowcount + truncated
    if (serializer_append(&sb,
            ",\"rowcount\":%u,\"truncated\":%s"
            "}}",
            qr->nrows, qr->truncated ? "true" : "false") != 1) goto err;

    // materialize output (exact size; not NUL-terminated)
    char *out = (char *)malloc(sb.len);
    if (!out) goto err;
    memcpy(out, sb.data, sb.len);
    *out_json = out;
    *out_len  = sb.len;

    sb_clean(&sb);
    return 1;

err:
    sb_clean(&sb);
    *out_json = NULL;
    *out_len  = 0;
    return -1;
}
