#include "transport_reader.h"

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Appends 'n' bytes starting from 'src' into the 'len'th byte of 'buf' onward.
 * If the current 'cap' of the buffer is too small, it gets reallocated. 
 *
 * Returns:
 *  1: success, more than one byte appended.
 *  0: 0 bytes appended. 
 * -1: realloc error. */
static int buf_append(char **buf, size_t *len, size_t *cap, const char *src,
        size_t n) {
    if (n == 0) return 0;
    if (*cap < *len + n + 1) {
        size_t newcap = (*cap == 0) ? 1024 : *cap;
        while (newcap < *len + n + 1) newcap *= 2;
        char *p = (char *)realloc(*buf, newcap);
        if (!p) return -1;
        *buf = p;
        *cap = newcap;
    }
    memcpy(*buf + *len, src, n);
    *len += n;
    (*buf)[*len] = '\0';
    return 1;
}

/* Deletes 'n' byte from the stash of 'r'. */
static void stash_consume_prefix(TransportReader *r, size_t n) {
    if (n == 0) return;
    if (n >= r->stash_len) {
        if (r->stash) free(r->stash);
        r->stash = NULL;
        r->stash_len = 0;
        return;
    }
    memmove(r->stash, r->stash + n, r->stash_len - n);
    r->stash_len -= n;
    r->stash[r->stash_len] = '\0';
    // TODO: shrick allocation of stash
}

void transport_r_init(TransportReader *r, FILE *in) {
    r->in = in;
    r->stash = NULL;
    r->stash_len = 0;
}

void transport_r_clean(TransportReader *r) {
    if (!r) {
        fprintf(stderr, "WARN: Calling transport_r_clean on NULL TransportReader.");
        return;
    }
    if (r->stash) {
        free(r->stash);
        r->stash = NULL;
    }
    r->stash_len = 0;
    r->in = NULL;
}

/* Trim trailing newlines/spaces after we cut at ';' (but keep the ';'). */
static void strip_after_semicolon(char *sql) {
    size_t n = strlen(sql);
    while (n > 0 && (sql[n - 1] == '\n' || sql[n - 1] == '\r')) {
        sql[--n] = '\0';
    }
}

int transport_r_read_sql(TransportReader *r, char **out_sql) {
    *out_sql = NULL;

    // accumulator of bytes, where we'll store the chars
    char *acc = NULL;
    size_t acc_len = 0, acc_cap = 0;

    // which quote are we inside? '\'' or '\"'
    char quote_inside = 0;
    ssize_t terminator_semicolon_idx = -1;

    while (1) {
        const char *src = NULL;
        size_t src_len = 0;

        // we always put the line we read inside stash.
        // then, we remove the bytes up until terminator from stash and remain
        // the other to be consumed in the next iteration
        if (r->stash_len > 0) {
            src = r->stash;
            src_len = r->stash_len;
        } else {
            // read a full line
            char *line = NULL;
            size_t cap = 0;
            ssize_t got = getline(&line, &cap, r->in);
            // EOF
            if (got < 0) {
                free(line); // malloc'd by getline
                if (acc_len == 0) {
                    if (acc) free(acc);
                    return 0;
                }
                // only return if we saw terminator before EOF
                if (terminator_semicolon_idx > -1) {
                    strip_after_semicolon(acc);
                    *out_sql = acc;
                    return 1;
                }
                // incomplete statement at EOF
                free(acc);
                // TODO: custom error codes for portability
                return -1;
            }
            // move line into stash so we use one codepath
            r->stash = line;
            r->stash_len = (size_t)got;
            continue;
        }

        size_t i = 0;
        while (i < src_len) {
            if (terminator_semicolon_idx > -1) {
                // consumes spaces
                while (i < src_len && isspace(src[i])) i++;
                // next statement starts on same line; keep it in stash
                break;
            }

            char c = src[i];

            if (quote_inside) {
                if (c == quote_inside) {
                    // double quote is treated as escaped
                    if (i + 1 < src_len && src[i + 1] == quote_inside) {
                        if (!buf_append(&acc, &acc_len, &acc_cap, src + i, 2)) {
                            free(acc);
                            return -1;
                        }
                        i += 2;
                        continue;
                    }
                    quote_inside = 0;
                }
                if (!buf_append(&acc, &acc_len, &acc_cap, &c, 1)) {
                    free(acc);
                    return -1;
                }
                i++;
                continue;
            }

            // outside quotes, entering quotes?
            if (c == '\'' || c == '\"') {
                quote_inside = c;
                if (!buf_append(&acc, &acc_len, &acc_cap, &c, 1)) {
                    free(acc);
                    return -1;
                }
                i++;
                continue;
            }

            // terminator detected outside any quotes
            if (c == ';') {
                if (!buf_append(&acc, &acc_len, &acc_cap, &c, 1)) {
                    free(acc);
                    return -1;
                }
                terminator_semicolon_idx = acc_len - 1;
                i++;
                continue;
            }

            // normal char
            if (!buf_append(&acc, &acc_len, &acc_cap, &c, 1)) {
                free(acc);
                return -1;
            }
            i++;
        }

        // src always cames from stash, remove up until, and including,
        // the last byte of the line containing the terminator
        stash_consume_prefix(r, i);

        if (terminator_semicolon_idx >= 0) {
            // now acc contains everything up to and including newline after
            // ';'. Returns only up to and including semicolon.
            if ((size_t)terminator_semicolon_idx > acc_len) {
                fprintf(stderr, "Semicolon terminator out-of-bouds.");
                free(acc);
                return -1;
            }

            // put '\0' after the terminator ';' so we ignore everything after
            // it

            // we may be at the end of the accumulator
            if ((size_t)terminator_semicolon_idx + 1 == acc_len) {
                if (!buf_append(&acc, &acc_len, &acc_cap, "\0", 1)) {
                    free(acc);
                    return -1;
                }
            } else {
                acc[terminator_semicolon_idx + 1] = '\0';
            }

            strip_after_semicolon(acc);
            *out_sql = acc;
            return 1;
        }

        // continue the loop and read more input
    }
}
