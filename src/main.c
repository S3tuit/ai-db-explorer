#include "transport_reader.h"
#include "transport_writer.h"

#include <stdlib.h>
#include <string.h>

int main(void) {
    TransportReader r;
    transport_r_init(&r, stdin);
    
    TransportWriter w;
    transport_w_init(&w, stdout);

    while (1) {
        char *sql = NULL;
        int rc = transport_r_read_sql(&r, &sql);
        if (rc == 0) break;
        if (rc < 0) {
            perror("read");
            break;
        }

        transport_w_write(&w, sql, strlen(sql));
        free(sql);
    }

    transport_r_clean(&r);
    transport_w_clean(&w);
    return 0;
}
