// Dumps the libpg_query JSON AST for an input SQL query from stdin.
// Ownership: input buffer is owned and freed by this function.
// Error semantics: exits non-zero on parse error or empty input.
#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Reads stdin into a NUL-terminated buffer.
 * Ownership: caller owns returned buffer and must free it.
 * Error semantics: returns NULL on allocation failure. */
static char *read_all_stdin(size_t *out_len) {
  size_t cap = 4096;
  size_t len = 0;
  char *buf = (char *)malloc(cap + 1);
  if (!buf) return NULL;

  while (!feof(stdin)) {
    size_t n = fread(buf + len, 1, cap - len, stdin);
    if (n == 0) break;
    len += n;
    if (len == cap) {
      size_t new_cap = cap * 2;
      char *tmp = (char *)realloc(buf, new_cap + 1);
      if (!tmp) {
        free(buf);
        return NULL;
      }
      buf = tmp;
      cap = new_cap;
    }
  }

  buf[len] = '\0';
  if (out_len) *out_len = len;
  return buf;
}

/* Entry point: parse SQL from stdin and print the JSON parse tree.
 * Side effects: reads stdin, writes stdout/stderr, exits non-zero on error. */
int main(void) {
  size_t len = 0;
  char *sql = read_all_stdin(&len);
  if (!sql) {
    fprintf(stderr, "error: out of memory\n");
    return 2;
  }
  if (len == 0) {
    fprintf(stderr, "error: empty input\n");
    free(sql);
    return 2;
  }

  PgQueryParseResult result = pg_query_parse(sql);
  if (result.error) {
    fprintf(stderr, "error: %s at %d\n", result.error->message, result.error->cursorpos);
    pg_query_free_parse_result(result);
    free(sql);
    return 1;
  }

  if (result.parse_tree) {
    fputs(result.parse_tree, stdout);
    fputc('\n', stdout);
  }

  pg_query_free_parse_result(result);
  free(sql);
  pg_query_exit();
  return 0;
}
