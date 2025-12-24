#ifndef UTILS_AI_DB_EXPL_H
#define UTILS_AI_DB_EXPL_H

#include <stdlib.h>

// Return types that must be used for "mutators"; functions that do a thing,
// for example, append, connect, exec.
#define OK 0
#define ERR -1

// Return types that must be used for functions that answer a question, for
// example is_null, contains.
#define YES 1
#define NO 0
#define ERR -1

void *xmalloc(size_t size);
void *xcalloc(size_t n, size_t size);
void *xrealloc(void *ptr, size_t size);

#endif
