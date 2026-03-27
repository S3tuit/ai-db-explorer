#include "app_dir.h"
#include "broker.h"
#include "cli_args.h"
#include "config_dir.h"
#include "conn_catalog.h"
#include "cred_manager.h"
#include "log.h"
#include "mcp_server.h"
#include "secret_store.h"
#include "utils.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Prints the CLI usage block for this program.
 * It borrows 'prog' and allocates no memory.
 * Side effects: writes usage text to stderr.
 * Error semantics: none.
 */
static void print_usage(const char *prog) {
  fprintf(stderr,
          "Usage:\n"
          "  %s [-client|-broker|-which-config] [-appdir <path>] "
          "[-config <path>]\n"
          "  %s -cred (--sync|-s [connection] | --test|-t [connection] |\n"
          "           --reset|-r (<namespace> | --everything))\n"
           "           [-config <path>]\n",
          prog, prog);
}

/* Runs one credential-manager command selected from the CLI and prints any
 * error. Returns 0 on success, 1 on failure or invalid input.
 */
static int run_cred_mode(const CredManagerReq *req, const char *config_input) {
  char *err = NULL;
  if (cred_manager_execute(req, config_input, &err) != OK) {
    fprintf(stderr, "ERROR: %s\n", err ? err : "credential command failed");
    free(err);
    return 1;
  }

  free(err);
  return 0;
}

/* Prints one startup error derived from 'err'. Falls back to strerror(errno)
 * when no typed message is available.
 */
static void print_appdir_error(const char *prefix, const AppDirErr *err) {
  const char *msg = strerror(errno);
  if (err && err->msg[0] != '\0')
    msg = err->msg;
  fprintf(stderr, "ERROR: %s: %s\n", prefix ? prefix : "startup failed", msg);
}

/* Resolves and prints the runtime/config paths implied by one CLI selection.
 * It borrows 'cli' and allocates transient path objects that are released
 * before returning.
 * Side effects: reads env/config path policy and writes the resolved paths to
 * stdout.
 * Returns 0 on success, 1 on invalid input or path-resolution failure.
 */
static int run_which_config_mode(const CliArgs *cli) {
  if (!cli)
    return 1;

  AppDirErr appdir_err;
  AppDir *appd = appdir_resolve(cli->app_dir_input, &appdir_err);
  if (!appd) {
    print_appdir_error("failed to resolve app directory", &appdir_err);
    return 1;
  }

  char *internal_dir = NULL;
  char *internal_dir_err = NULL;
  if (confdir_default_resolve(&internal_dir, NULL, &internal_dir_err) != OK) {
    fprintf(stderr, "ERROR: failed to resolve internal config directory: %s\n",
            internal_dir_err ? internal_dir_err : "unknown error");
    free(internal_dir_err);
    appdir_clean(appd);
    return 1;
  }

  char *config_path = NULL;
  char *config_err = NULL;
  if (confdir_resolve_config_path(cli->config_input, &config_path, &config_err) !=
      OK) {
    fprintf(stderr, "ERROR: failed to resolve configuration file: %s\n",
            config_err ? config_err : "unknown error");
    free(config_err);
    free(internal_dir);
    appdir_clean(appd);
    return 1;
  }

  printf("Runtime shared directory: %s\n", appd->run_dir);
  printf("Runtime secret directory: %s\n", appd->secret_dir);
  printf("Internal state/config directory: %s\n", internal_dir);
  printf("Configuration file: %s\n", config_path);

  free(config_path);
  free(config_err);
  free(internal_dir);
  free(internal_dir_err);
  appdir_clean(appd);
  return 0;
}

/* Parses the process CLI into one structured selection used by main().
 * It borrows 'argv', writes borrowed string views into 'out', and writes a
 * human-readable parse error into caller-owned 'err_buf' on failure.
 * Side effects: mutates 'out' and 'err_buf' only.
 * Returns OK on success, ERR on invalid CLI shape or invalid input.
 */
int main(int argc, char **argv) {
  // Test-only marker to confirm the ADBX_TEST_MODE build is running.
  TLOG("INFO - startup with ADBX_TEST_MODE enabled");
  // Ignore SIGPIPE so write failures return -1 instead of terminating the
  // process. This makes test failures observable via error handling/logging.
  (void)signal(SIGPIPE, SIG_IGN);

  // cli args parsing
  CliArgs cli = {0};
  char *cli_err = malloc(512);
  if (!cli_err) {
    fprintf(stderr, "ERROR: unable to allocate CLI error buffer\n");
    return 1;
  }
  if (cli_parse_args(argc, argv, &cli, cli_err, 512) != OK) {
    fprintf(stderr, "ERROR: %s\n",
            cli_err[0] != '\0' ? cli_err : "invalid command line");
    print_usage(argv[0]);
    free(cli_err);
    return 1;
  }
  free(cli_err);

  if (cli.mode == APP_MODE_CRED)
    return run_cred_mode(&cli.cred_req, cli.config_input);

  if (cli.mode == APP_MODE_WHICH_CONFIG)
    return run_which_config_mode(&cli);

  if (cli.mode == APP_MODE_CLIENT) {
    AppDirErr appdir_err;
    AppDir *appd = appdir_resolve(cli.app_dir_input, &appdir_err);
    if (!appd) {
      print_appdir_error("failed to resolve app directory", &appdir_err);
      return 1;
    }

    McpServer s;
    McpServerInit init = {
        .in = stdin,
        .out = stdout,
        .appd = appd,
    };
    if (mcpser_init(&s, &init) != OK) {
      fprintf(stderr, "ERROR: server init failed\n");
      appdir_clean(appd);
      return 1;
    }

    // keep init logs inside stderr
    fprintf(stderr, "LOG: server init success\n");
    int rc = mcpser_run(&s);
    if (rc != OK)
      fprintf(stderr, "ERROR: %s\n", mcpser_last_error(&s));
    mcpser_clean(&s);
    appdir_clean(appd);
    return (rc == OK) ? 0 : 1;
  }

  AppDirErr appdir_err;
  AppDir *appd = appdir_resolve(cli.app_dir_input, &appdir_err);
  if (!appd) {
    print_appdir_error("failed to resolve app directory", &appdir_err);
    return 1;
  }

  ConfFile config = {.fd = -1, .path = NULL};
  char *config_path_err = NULL;
  if (confdir_open(cli.config_input, &config, &config_path_err) != OK) {
    fprintf(stderr, "ERROR: config path setup failed: %s\n",
            config_path_err ? config_path_err : "unknown error");
    free(config_path_err);
    appdir_clean(appd);
    return 1;
  }

  char *cat_err = NULL;
  ConnCatalog *cat = catalog_load_from_fd(config.fd, &cat_err);
  conffile_clean(&config);
  if (!cat) {
    fprintf(stderr, "ERROR: catalog init failed: %s\n",
            cat_err ? cat_err : "unknown error");
    free(cat_err);
    appdir_clean(appd);
    return 1;
  }

  SecretStore *secrets = NULL;
  SecretStoreErr ss_err;
  secrets = secret_store_create(&ss_err);
  if (!secrets) {
    catalog_destroy(cat);
    fprintf(stderr, "ERROR: secret store init failed: %s\n",
            ss_err.msg[0] != '\0' ? ss_err.msg : "unknown error");
    appdir_clean(appd);
    return 1;
  }

  ConnManager *cm = NULL;
  cm = connm_create(cat, secrets);
  if (!cm) {
    catalog_destroy(cat);
    secret_store_destroy(secrets);
    fprintf(stderr, "ERROR: conn manager init failed\n");
    appdir_clean(appd);
    return 1;
  }

  AppDirErr broker_err;
  Broker *b = broker_create(appd, cm, &broker_err);
  if (!b) {
    connm_destroy(cm);
    print_appdir_error("broker init failed", &broker_err);
    appdir_clean(appd);
    return 1;
  }

  fprintf(stderr, "LOG: broker init success\n");
  int rc = broker_run(b);
  if (rc != OK)
    fprintf(stderr, "ERROR: broker run failed\n");
  broker_destroy(b);
  appdir_clean(appd);
  return (rc == OK) ? 0 : 1;
}
