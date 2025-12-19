#include <stdio.h>

#include "utils.h"

/* Succeed or die. If malloc fails to allocate memory, this terminates the
 * program.*/
void *xmalloc(size_t size) {
    void *ptr = malloc(size);
    if (!ptr) {
        fprintf(stderr, "virtual memory exhausted\n");
        exit(1);
    }
    return ptr;
}
