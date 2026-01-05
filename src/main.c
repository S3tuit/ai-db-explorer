#include "client.h"
#include "broker.h"
#include "utils.h"

#include <stdio.h>
#include <string.h>

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s [-client|-broker] [-sock <path>]\n", prog);
}

int main(int argc, char **argv) {
    const char *sock_path = SOCK_PATH;
    int run_client = 1;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-client") == 0) {
            run_client = 1;
        } else if (strcmp(argv[i], "-broker") == 0) {
            run_client = 0;
        } else if (strcmp(argv[i], "-sock") == 0) {
            if (i + 1 >= argc) {
                print_usage(argv[0]);
                return 1;
            }
            sock_path = argv[++i];
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (run_client) {
        Client c;
        if (client_init(&c, stdin, stdout, sock_path) != OK) {
            fprintf(stderr, "ERROR: client init failed\n");
            return 1;
        }
        
        // keep init logs inside stderr
        fprintf(stderr, "LOG: client init success\n");
        int rc = client_run(&c);
        if (rc != OK) fprintf(stderr, "ERROR: %s\n", client_last_error(&c));
        client_clean(&c);
        return (rc == OK) ? 0 : 1;
    }

    Broker *b = broker_create(sock_path);
    if (!b) {
        fprintf(stderr, "ERROR: broker init failed\n");
        return 1;
    }

    fprintf(stderr, "LOG: broker init success\n");
    int rc = broker_run(b);
    if (rc != OK) fprintf(stderr, "ERROR: broker run failed\n");
    broker_destroy(b);
    return (rc == OK) ? 0 : 1;
}
