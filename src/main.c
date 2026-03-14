#include "broker.h"
#include "config_dir.h"
#include "conn_catalog.h"
#include "log.h"
#include "mcp_server.h"
#include "private_dir.h"
#include "secret_store.h"
#include "utils.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// TODO: maybe rename -privdir to -rundir for user clarity
static void print_usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s [-client|-broker] [-privdir <path>] [-config <path>]\n",
          prog);
}

/* Prints one broker-init error tailored for the most common private-dir
 * misconfiguration.
 * It borrows 'pd' and reads process errno and filesystem existence state.
 * Side effects: writes one diagnostic line to stderr.
 * Error semantics: none.
 */
static void print_broker_init_error(const PrivDir *pd) {
  if (!pd || !pd->base) {
    fprintf(stderr, "ERROR: broker init failed: %s\n", strerror(errno));
    return;
  }

  if (errno == ENOENT && access(pd->base, F_OK) != 0) {
    fprintf(stderr,
            "ERROR: broker init failed: private-dir base '%s' does not exist. "
            "-privdir expects an existing parent directory; the broker will "
            "create '%s/' under it.\n",
            pd->base, PRIVDIR_APP_DIRNAME);
    return;
  }

  fprintf(stderr, "ERROR: broker init failed: %s\n", strerror(errno));
}

int main(int argc, char **argv) {
  // Test-only marker to confirm the ADBX_TEST_MODE build is running.
  TLOG("INFO - startup with ADBX_TEST_MODE enabled");
  // Ignore SIGPIPE so write failures return -1 instead of terminating the
  // process. This makes test failures observable via error handling/logging.
  (void)signal(SIGPIPE, SIG_IGN);
  const char *privdir_base = NULL;
  const char *config_input = NULL;
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
      config_input = argv[++i];
    } else {
      print_usage(argv[0]);
      return 1;
    }
  }

  if (run_client) {
    char *privdir_err = NULL;
    PrivDir *pd = privdir_resolve(privdir_base, &privdir_err);
    if (!pd) {
      fprintf(stderr, "ERROR: failed to resolve private directory: %s\n",
              privdir_err ? privdir_err : "unknown error");
      free(privdir_err);
      return 1;
    }

    McpServer s;
    McpServerInit init = {
        .in = stdin,
        .out = stdout,
        .privd = pd,
    };
    if (mcpser_init(&s, &init) != OK) {
      fprintf(stderr, "ERROR: server init failed\n");
      privdir_clean(pd);
      return 1;
    }

    // keep init logs inside stderr
    fprintf(stderr, "LOG: server init success\n");
    int rc = mcpser_run(&s);
    if (rc != OK)
      fprintf(stderr, "ERROR: %s\n", mcpser_last_error(&s));
    mcpser_clean(&s);
    privdir_clean(pd);
    free(privdir_err);
    return (rc == OK) ? 0 : 1;
  }

  char *privdir_err = NULL;
  PrivDir *pd = privdir_resolve(privdir_base, &privdir_err);
  if (!pd) {
    fprintf(stderr, "ERROR: failed to resolve private directory: %s\n",
            privdir_err ? privdir_err : "unknown error");
    free(privdir_err);
    return 1;
  }
  free(privdir_err);

  ConfFile config = {.fd = -1, .path = NULL};
  char *config_path_err = NULL;
  if (confdir_open(config_input, &config, &config_path_err) != OK) {
    fprintf(stderr, "ERROR: config path setup failed: %s\n",
            config_path_err ? config_path_err : "unknown error");
    free(config_path_err);
    privdir_clean(pd);
    return 1;
  }

  char *cat_err = NULL;
  ConnCatalog *cat = catalog_load_from_fd(config.fd, &cat_err);
  conffile_clean(&config);
  if (!cat) {
    fprintf(stderr, "ERROR: catalog init failed: %s\n",
            cat_err ? cat_err : "unknown error");
    free(cat_err);
    privdir_clean(pd);
    return 1;
  }

  SecretStore *secrets = NULL;
  char *ss_err = NULL;
  secrets = secret_store_create(&ss_err);
  if (!secrets) {
    catalog_destroy(cat);
    fprintf(stderr, "ERROR: secret store init failed: %s\n",
            ss_err ? ss_err : "unknown error");
    free(ss_err);
    privdir_clean(pd);
    return 1;
  }
  free(ss_err);

  ConnManager *cm = NULL;
  cm = connm_create(cat, secrets);
  if (!cm) {
    catalog_destroy(cat);
    secret_store_destroy(secrets);
    fprintf(stderr, "ERROR: conn manager init failed\n");
    privdir_clean(pd);
    return 1;
  }

  Broker *b = broker_create(pd, cm);
  if (!b) {
    connm_destroy(cm);
    print_broker_init_error(pd);
    privdir_clean(pd);
    return 1;
  }

  fprintf(stderr, "LOG: broker init success\n");
  int rc = broker_run(b);
  if (rc != OK)
    fprintf(stderr, "ERROR: broker run failed\n");
  broker_destroy(b);
  privdir_clean(pd);
  return (rc == OK) ? 0 : 1;
}
