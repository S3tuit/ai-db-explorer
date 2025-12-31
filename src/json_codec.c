#include "json_codec.h"
#include "utils.h"
#include "string_op.h"

#include <limits.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/* -------------------------------- encoding ------------------------------- */

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

static int json_qr_ok(StrBuf *sb, const QueryResult *qr) {
    // nrows*ncols should fit in addressable range of cells
    uint64_t cell_count_u64 = 0;
    safe_mul_u32(qr->nrows, qr->ncols, &cell_count_u64);
    if (cell_count_u64 > SIZE_MAX) return ERR;

    // if there are cols/rows, require corresponding pointers
    if (qr->ncols > 0 && !qr->cols) return ERR;
    if (cell_count_u64 > 0 && !qr->cells) return ERR;

    if (json_append(sb,
            "\"result\":{\"exec_ms\":%U", qr->exec_ms) != OK) return ERR;

    // columns
    if (json_append(sb, ",\"columns\":[") != OK) return ERR;
    for (uint32_t c = 0; c < qr->ncols; ++c) {
        if (c > 0) {
            if (json_append(sb, ",") != OK) return ERR;
        }

        const QRColumn *col = qr_get_col((QueryResult *)qr, c);
        // defensive, in case internals are partially set or missing entirely
        const char *name = "";
        const char *type = "";
        if (col) {
            name = col->name ? col->name : "";
            type = col->type ? col->type : "";
        }

        if (json_append(sb,
                "{\"name\":\"%s\",\"type\":\"%s\"}",
                name, type) != OK) return ERR;
    }
    if (json_append(sb, "]") != OK) return ERR;

    // rows
    if (json_append(sb, ",\"rows\":[") != OK) return ERR;

    for (uint32_t r = 0; r < qr->nrows; ++r) {
        if (r > 0) {
            if (json_append(sb, ",") != OK) return ERR;
        }
        if (json_append(sb, "[") != OK) return ERR;

        for (uint32_t c = 0; c < qr->ncols; ++c) {
            if (c > 0) {
                if (json_append(sb, ",") != OK) return ERR;
            }

            const char *cell = qr_get_cell(qr, r, c);
            if (!cell) {
                if (json_append(sb, "null") != OK) return ERR;
            } else {
                /* quote + escaped string content + quote */
                if (json_append(sb, "\"%s\"", cell) != OK) return ERR;
            }
        }

        if (json_append(sb, "]") != OK) return ERR;
    }

    if (json_append(sb, "]") != OK) return ERR;

    // rowcount + truncated
    if (json_append(sb,
            ",\"rowcount\":%u,\"truncated\":%s"
            "}}",
            qr->nrows, qr->truncated ? "true" : "false") != OK) return ERR;

    return OK;
}

static int json_qr_err(StrBuf *sb, const QueryResult *qr) {
    const char *msg = qr->err_msg ? qr->err_msg : "";
    if (json_append(sb,
                "\"error\":{\"message\":\"%s\"}}", msg) != OK) return ERR;
    return OK;
}

int qr_to_jsonrpc(const QueryResult *qr, char **out_json,
                                size_t *out_len) {
    if (!out_json || !out_len) return ERR;
    *out_json = NULL;
    *out_len  = 0;

    if (!qr) return ERR;

    StrBuf sb = {0};

    // begin JSON-RPC envelope
    if (json_append(&sb,
            "{\"jsonrpc\":\"2.0\",\"id\":%u,",
            qr->id) != OK) goto err;

    if (qr->status == QR_ERROR) {
        if (json_qr_err(&sb, qr) != OK) goto err;
    } else {
        if (json_qr_ok(&sb, qr) != OK) goto err;
    }

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

int command_to_jsonrpc(const Command *cmd, uint32_t id,
        char **out_json, size_t *out_len) {

    if (!out_json || !out_len || !cmd) return ERR;
    *out_json = NULL;
    *out_len  = 0;

    StrBuf sb = {0};

    if (cmd->type == CMD_SQL) {
        if (!cmd->raw_sql) goto err;
        if (json_append(&sb,
                "{\"jsonrpc\":\"2.0\",\"id\":%u,\"method\":\"exec\",\"params\":{"
                "\"sql\":\"%s\"}}",
                id, cmd->raw_sql) != OK) goto err;
    } else if (cmd->type == CMD_META) {
        if (!cmd->cmd) goto err;
        if (cmd->args && cmd->args[0] != '\0') {
            if (json_append(&sb,
                    "{\"jsonrpc\":\"2.0\",\"id\":%u,\"method\":\"%s\",\"params\":{"
                    "\"raw\":\"%s\"}}",
                    id, cmd->cmd, cmd->args) != OK) goto err;
        } else {
            if (json_append(&sb,
                    "{\"jsonrpc\":\"2.0\",\"id\":%u,\"method\":\"%s\"}",
                    id, cmd->cmd) != OK) goto err;
        }
    } else {
        goto err;
    }

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
