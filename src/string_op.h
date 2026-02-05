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
  char *data;
  size_t len;
  size_t cap;
} StrBuf;

// TODO: make this configurable via compile-time flags.
#define STRBUF_MAX_BYTES (1u << 30)

/* Adds 'n' bytes starting from 'src' to sb. Returns OK/ERR. */
int sb_append_bytes(StrBuf *sb, const void *src, size_t n);

/* Reserves and exposes a writable span of length 'n'. The returned pointer
 * points to the new region at the end of sb and sb->len is advanced. */
int sb_prepare_for_write(StrBuf *sb, size_t n, char **out_dst);

/* Returns a NUL-terminated C-string view of sb.
 * Best-effort: returns "" if sb is NULL, uninitialized, or growth fails.
 * Does not change sb->len. May increase sb->cap. */
const char *sb_to_cstr(StrBuf *sb);

/* Zeroes any allocated bytes of 'sb' and frees them.
 * Side effects: overwrites memory to clear sensitive data. */
void sb_zero_clean(StrBuf *sb);

/* Clean the internal allocation of 'sb'. */
void sb_clean(StrBuf *sb);

/* Best effort to reset the internal metadata, without freeing anything, so
 * next time bytes will be appended as if 'sb' was clean. */
void sb_reset(StrBuf *sb);

#endif
