#include <stdio.h>

#include "utils.h"

/* Succeed or die. If malloc fails to allocate memory, this terminates the
 * program.*/
void *xmalloc(size_t size) {
    void *ptr = malloc(size);
    if (!ptr) {
        fprintf(stderr, "xmalloc. virtual memory exhausted\n");
        exit(1);
    }
    return ptr;
}

void *xcalloc(size_t n, size_t size) {
    void *ptr = calloc(n, size);
    if (!ptr) {
        fprintf(stderr, "xcalloc. virtual memory exhausted\n");
        exit(1);
    }
    return ptr;
}

void *xrealloc(void *ptr, size_t size) {
    if (!ptr) return xmalloc(size);
    void *new = realloc(ptr, size);
    if (!new) {
        fprintf(stderr, "xrealloc. virtual memory exhausted\n");
        exit(1);
    }
    return new;
}
