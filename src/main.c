#include "mcp_server.h"
#include "broker.h"
#include "utils.h"
#include "connection_catalog.h"
#include "secret_store.h"

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
        McpServer s;
        if (mcpser_init(&s, stdin, stdout, sock_path) != OK) {
            fprintf(stderr, "ERROR: server init failed\n");
            return 1;
        }
        
        // keep init logs inside stderr
        fprintf(stderr, "LOG: server init success\n");
        int rc = mcpser_run(&s);
        if (rc != OK) fprintf(stderr, "ERROR: %s\n", mcpser_last_error(&s));
        mcpser_clean(&s);
        return (rc == OK) ? 0 : 1;
    }

    ConnectionCatalog *cat = NULL;
    SecretStore *secrets = NULL;
    ConnManager *cm = NULL;

    if (catalog_load_from_file("dummy", &cat) != OK) {
        fprintf(stderr, "ERROR: catalog init failed\n");
        return 1;
    }
    secrets = secret_store_create();
    if (!secrets) {
        catalog_destroy(cat);
        fprintf(stderr, "ERROR: secret store init failed\n");
        return 1;
    }
    cm = connm_create(cat, secrets);
    if (!cm) {
        catalog_destroy(cat);
        secret_store_destroy(secrets);
        fprintf(stderr, "ERROR: conn manager init failed\n");
        return 1;
    }

    Broker *b = broker_create(sock_path, cm);
    if (!b) {
        connm_destroy(cm);
        fprintf(stderr, "ERROR: broker init failed\n");
        return 1;
    }

    fprintf(stderr, "LOG: broker init success\n");
    int rc = broker_run(b);
    if (rc != OK) fprintf(stderr, "ERROR: broker run failed\n");
    broker_destroy(b);
    return (rc == OK) ? 0 : 1;
}
