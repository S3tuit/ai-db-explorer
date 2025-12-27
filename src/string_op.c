#include "string_op.h"
#include "utils.h"

#include <string.h>
#include <inttypes.h>
#include <limits.h>

static char *dup_n_or_null_impl(const char *s, size_t cap, int pretty) {
    if (!s) return NULL;

    size_t n = strnlen(s, cap);

    if (n == cap) { // not terminated within cap
        char *p = xmalloc(cap);
        if (pretty && cap >= 4) {
            memcpy(p, s, cap - 4);
            p[cap - 2] = p[cap - 3] = p[cap - 4] = '.';
        } else {
            memcpy(p, s, cap - 1);
        }
        p[cap - 1] = '\0';
        return p;
    }

    // fully fits (including '\0')
    char *p = xmalloc(n + 1);
    memcpy(p, s, n + 1); // includes '\0'
    return p;
}

char *dupn_or_null(const char *s, size_t cap) {
    return dup_n_or_null_impl(s, cap, 0);
}

char *dupn_or_null_pretty(const char *s, size_t cap) {
    return dup_n_or_null_impl(s, cap, 1);
}

char *dup_or_null(const char *s) {
    // using SIZE_MAX triggers compilation warning since it's huge
    return dup_n_or_null_impl(s, INT_MAX, 0);
}

/* ------------------------- small growable buffer ------------------------- */

void sb_clean(StrBuf *sb) {
    if (!sb) return;
    free(sb->data);
    sb->data = NULL;
    sb->len = 0;
    sb->cap = 0;
}

// TODO: think about the max size of the output of a QueryResult. Right now it 
// may have 1k columns all of 10k char. And don't forget that an sql
// query for libqp must be lower than 8k chars.

/* Makes sure 'sb' has enough space for 'add' more bytes. Returns OK on success,
 * ERR on bad input or overflow. */
int sb_reserve(StrBuf *sb, size_t add) {
    if (!sb) return ERR;
    // overflow
    if (add > SIZE_MAX - sb->len) return ERR;

    // state unchanged
    if (add == 0) return OK;

    size_t needed = sb->len + add;
    if (needed <= sb->cap) return OK;

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
    return OK;
}

int sb_append_bytes(StrBuf *sb, const void *src, size_t n) {
    if (!sb || (!src && n != 0)) return ERR;
    if (n == 0) return OK;
    if (sb_reserve(sb, n) != OK) return ERR;
    memcpy(sb->data + sb->len, src, n);
    sb->len += n;
    return OK;
}

