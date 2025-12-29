#include "query_reader.h"
#include "utils.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Discard the first 'n' bytes from the stash buffer.
static void stash_consume_prefix(QueryReader *r, size_t n) {
    if (n == 0 || !r) return;
    if (n >= r->stash.len) {
        sb_clean(&r->stash);
        return;
    }
    memmove(r->stash.data, r->stash.data + n, r->stash.len - n);
    r->stash.len -= n;
}

QueryReader *query_reader_create(ByteChannel *ch) {
    if (!ch) return NULL;
    BufReader *br = bufreader_create(ch);
    if (!br) {
        bytech_destroy(ch);
        return NULL;
    }
    QueryReader *r = (QueryReader *)xmalloc(sizeof(*r));
    r->br = br;
    r->stash.data = NULL;
    r->stash.len = 0;
    r->stash.cap = 0;
    return r;
}

void query_reader_destroy(QueryReader *r) {
    if (!r) return;

    sb_clean(&r->stash);
    if (r->br) {
        bufreader_destroy(r->br);
        r->br = NULL;
    }
    free(r);
}

int query_reader_read_sql(QueryReader *r, char **out_sql) {
    if (!r || !out_sql) return ERR;
    *out_sql = NULL;
    
    // where we'll store the bytes read
    StrBuf acc = {0};
    // 0 if we're outside any quotes, "\'" if inside single quotes, "\"" if
    // inside double quotes
    char quote_inside = 0;
    ssize_t terminator_semicolon_idx = -1;

    while (1) {
        const unsigned char *src = NULL;
        size_t src_len = 0;
        unsigned char *tmp = NULL;

        // we always put the line we read inside stash.
        // then, we remove the bytes up until the terminator and keep the
        // others to be consumed in the next iteration
        if (r->stash.len > 0) {
            src = (const unsigned char *)r->stash.data;
            src_len = r->stash.len;
        } else {
            size_t avail = 0;
            const uint8_t *peek = bufreader_peek(r->br, &avail);
            
            // no more bytes buffered, see if it's EOF or there are more bytes
            if (peek == NULL || avail == 0) {
                int rc = bufreader_ensure(r->br, 1);
                if (rc == ERR) {
                    sb_clean(&acc);
                    return ERR;
                }
                if (rc == NO) {
                    if (acc.len == 0) return NO;
                    sb_clean(&acc);
                    return ERR;
                }
                continue;
            }

            tmp = (unsigned char *)xmalloc(avail);
            if (bufreader_read_n(r->br, tmp, avail) != OK) {
                free(tmp);
                sb_clean(&acc);
                return ERR;
            }
            src = tmp;
            src_len = avail;
        }

        size_t i = 0;
        while (i < src_len) {
            if (terminator_semicolon_idx > -1) {
                // consume spaces after ;
                while (i < src_len && isspace((unsigned char)src[i])) i++;
                break;
            }

            unsigned char c = src[i];

            if (quote_inside) {
                if (c == (unsigned char)quote_inside) {
                    // a double quote, inside the same quote, it treated as
                    // escaped. e.g. " "" " is still a single statement 
                    if (i + 1 < src_len && src[i + 1] == (unsigned char)quote_inside) {
                        if (sb_append_bytes(&acc, src + i, 2) != OK) {
                            free(tmp);
                            sb_clean(&acc);
                            return ERR;
                        }
                        i += 2;
                        continue;
                    }
                    quote_inside = 0;
                }
                if (sb_append_bytes(&acc, &c, 1) != OK) {
                    free(tmp);
                    sb_clean(&acc);
                    return ERR;
                }
                i++;
                continue;
            }

            if (c == '\'' || c == '\"') {
                quote_inside = (char)c;
                if (sb_append_bytes(&acc, &c, 1) != OK) {
                    free(tmp);
                    sb_clean(&acc);
                    return ERR;
                }
                i++;
                continue;
            }

            // terminator detected outside quotes
            if (c == ';') {
                if (sb_append_bytes(&acc, &c, 1) != OK) {
                    free(tmp);
                    sb_clean(&acc);
                    return ERR;
                }
                terminator_semicolon_idx = (ssize_t)acc.len - 1;
                i++;
                continue;
            }

            // normal char
            if (sb_append_bytes(&acc, &c, 1) != OK) {
                free(tmp);
                sb_clean(&acc);
                return ERR;
            }
            i++;
        }

        if (r->stash.len > 0) {
            // remove up until the terminator
            stash_consume_prefix(r, i);
        } else {
            if (i < src_len) {
                // we have unconsumed bytes
                if (sb_append_bytes(&r->stash, src + i, src_len - i) != OK) {
                    free(tmp);
                    sb_clean(&acc);
                    return ERR;
                }
            }
        }

        free(tmp);

        if (terminator_semicolon_idx >= 0) {
            size_t sql_len = terminator_semicolon_idx + 1;
            // defensive check
            if (sql_len > acc.len) return ERR;

            char *out = xmalloc(sql_len + 1);
            memcpy(out, acc.data, sql_len);
            out[sql_len] = '\0';

            *out_sql = out;
            sb_clean(&acc);
            return YES;
        }
    }
}
