#include "test.h"

FILE *memfile_impl(const char *input, const char *file, int line) {
    (void)file; (void)line;
#if defined(_GNU_SOURCE)
    return fmemopen((void *)input, strlen(input), "r");
#else
    /* portable fallback: tmpfile */
    FILE *f = tmpfile();
    if (!f) return NULL;
    fwrite(input, 1, strlen(input), f);
    fflush(f);
    fseek(f, 0, SEEK_SET);
    return f;
#endif
}

