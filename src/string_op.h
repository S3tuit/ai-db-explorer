#ifndef STRING_OP_H
#define STRING_OP_H

#include <stddef.h>

/* Returns a pointer to a duplicated value of 's' of at most 'cap' bytes or
 * NULL if 's' is NULL. If the duplicated string gets truncated, this appends
 * "\0" without going past the 'cap'. Caller should free the result. */
char *dupn_or_null(const char *s, size_t cap);

/* Same as dupn_or_null but appends "..." and a nul-term in case the duplicated
 * string gets truncated. If cap < 4, it falls back to normal truncation. */
char *dupn_or_null_pretty(const char *s, size_t cap);

/* Returns a pointer to a duplicated value of 's'. Caller should free the
 * result. */
char *dup_or_null(const char *s);


/* ------------------------- small growable buffer ------------------------- */
typedef struct StrBuf {
    char  *data;
    size_t len;
    size_t cap;
} StrBuf;

/* Adds 'n' bytes starting from 'src' to sb. */
int sb_append_bytes(StrBuf *sb, const void *src, size_t n);

/* Clean the internal allocation of 'sb'. */
void sb_clean(StrBuf *sb);

#endif
