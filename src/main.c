#include "broker.h"
#include "conn_catalog.h"
#include "log.h"
#include "mcp_server.h"
#include "private_dir.h"
#include "secret_store.h"
#include "utils.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>

static void print_usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s [-client|-broker] [-privdir <path>] [-config <path>]\n",
          prog);
}

int main(int argc, char **argv) {
  // Test-only marker to confirm the ADBX_TEST_MODE build is running.
  TLOG("INFO - startup with ADBX_TEST_MODE enabled");
  // Ignore SIGPIPE so write failures return -1 instead of terminating the
  // process. This makes test failures observable via error handling/logging.
  (void)signal(SIGPIPE, SIG_IGN);
  const char *privdir_base = NULL;
  const char *config_path = "template-config.json";
  int run_client = 1;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-client") == 0) {
      run_client = 1;
    } else if (strcmp(argv[i], "-broker") == 0) {
      run_client = 0;
    } else if (strcmp(argv[i], "-privdir") == 0) {
      if (i + 1 >= argc) {
        print_usage(argv[0]);
        return 1;
      }
      privdir_base = argv[++i];
    } else if (strcmp(argv[i], "-config") == 0) {
      if (i + 1 >= argc) {
        print_usage(argv[0]);
        return 1;
      }
      config_path = argv[++i];
    } else {
      print_usage(argv[0]);
      return 1;
    }
  }

  if (run_client) {
    PrivDir *pd = privdir_resolve(privdir_base);
    if (!pd) {
      fprintf(stderr, "ERROR: failed to resolve private directory\n");
      return 1;
    }

    McpServer s;
    if (mcpser_init(&s, stdin, stdout, pd->sock_path) != OK) {
      fprintf(stderr, "ERROR: server init failed\n");
      privdir_free(pd);
      return 1;
    }

    // keep init logs inside stderr
    fprintf(stderr, "LOG: server init success\n");
    int rc = mcpser_run(&s);
    if (rc != OK)
      fprintf(stderr, "ERROR: %s\n", mcpser_last_error(&s));
    mcpser_clean(&s);
    privdir_free(pd);
    return (rc == OK) ? 0 : 1;
  }

  PrivDir *pd = privdir_resolve(privdir_base);
  if (!pd) {
    fprintf(stderr, "ERROR: failed to resolve private directory\n");
    return 1;
  }
  if (privdir_create_layout(pd) != OK) {
    fprintf(stderr, "ERROR: failed to create private directory layout\n");
    privdir_free(pd);
    return 1;
  }
  if (privdir_generate_token(pd) != OK) {
    fprintf(stderr, "ERROR: failed to generate token\n");
    privdir_cleanup(pd);
    privdir_free(pd);
    return 1;
  }
  uint8_t secret_token[SECRET_TOKEN_LEN] = {0};
  if (privdir_read_token(pd, secret_token) != OK) {
    fprintf(stderr, "ERROR: failed to read generated token\n");
    privdir_cleanup(pd);
    privdir_free(pd);
    return 1;
  }

  char *cat_err = NULL;
  ConnCatalog *cat = catalog_load_from_file(config_path, &cat_err);
  if (!cat) {
    fprintf(stderr, "ERROR: catalog init failed: %s\n",
            cat_err ? cat_err : "unknown error");
    privdir_cleanup(pd);
    privdir_free(pd);
    return 1;
  }

  SecretStore *secrets = NULL;
  secrets = secret_store_create();
  if (!secrets) {
    catalog_destroy(cat);
    fprintf(stderr, "ERROR: secret store init failed\n");
    privdir_cleanup(pd);
    privdir_free(pd);
    return 1;
  }

  ConnManager *cm = NULL;
  cm = connm_create(cat, secrets);
  if (!cm) {
    catalog_destroy(cat);
    secret_store_destroy(secrets);
    fprintf(stderr, "ERROR: conn manager init failed\n");
    privdir_cleanup(pd);
    privdir_free(pd);
    return 1;
  }

  Broker *b = broker_create(pd->sock_path, cm, secret_token);
  if (!b) {
    connm_destroy(cm);
    fprintf(stderr, "ERROR: broker init failed\n");
    privdir_cleanup(pd);
    privdir_free(pd);
    return 1;
  }

  fprintf(stderr, "LOG: broker init success\n");
  int rc = broker_run(b);
  if (rc != OK)
    fprintf(stderr, "ERROR: broker run failed\n");
  broker_destroy(b);
  privdir_cleanup(pd);
  privdir_free(pd);
  return (rc == OK) ? 0 : 1;
}
