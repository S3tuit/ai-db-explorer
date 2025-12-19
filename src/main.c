#include "transport_reader.h"

#include <stdlib.h>

int main(void) {
    TransportReader r;
    transport_r_init(&r, stdin);
    
    while (1) {
        char *sql = NULL;
        int rc = transport_r_read_sql(&r, &sql);
        if (rc == 0) break;
        if (rc < 0) {
            perror("read");
            break;
        }

        fprintf(stderr, "Got SQL: %s\n", sql);
        free(sql);
    }

    transport_r_clean(&r);
    return 0;
}
