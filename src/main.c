#include "broker.h"
#include "config_dir.h"
#include "conn_catalog.h"
#include "cred_manager.h"
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

typedef enum {
  APP_MODE_CLIENT = 0,
  APP_MODE_BROKER,
  APP_MODE_CRED,
} AppMode;

static void print_usage(const char *prog) {
  fprintf(stderr,
          "Usage:\n"
          "  %s [-client|-broker] [-privdir <path>] [-config <path>]\n"
          "  %s -cred (--sync|-s [connection] | --test|-t [connection] |\n"
          "           --reset|-r (<namespace> | --everything))\n"
          "           [-config <path>]\n",
          prog, prog);
}

/* Applies the top-level mode flag 'requested' to 'mode' while rejecting
 * conflicting combinations. It sets 'seen_mode' to 1 on success.
 * Returns OK on success, ERR when the caller attempts to combine different
 * exclusive modes.
 */
static AdbxStatus parse_mode_flag(AppMode *mode, int *seen_mode,
                                  AppMode requested) {
  if (!mode || !seen_mode)
    return ERR;

  if (*seen_mode && *mode != requested)
    return ERR;

  *mode = requested;
  *seen_mode = 1;
  return OK;
}

/* Applies one credential-manager subcommand flag 'cmd' to 'req' while rejecting
 * conflicting command mixes. It borrows 'req' and 'seen_cmd'. Sets 'seen_cmd'
 * to 1 on success. Returns OK on success, ERR when the caller combines
 * different cred subcommands or passes invalid parser state.
 */
static AdbxStatus parse_cred_cmd_flag(CredManagerReq *req, int *seen_cmd,
                                      CredManagerCommand cmd) {
  if (!req || !seen_cmd)
    return ERR;

  if (*seen_cmd && req->cmd != cmd)
    return ERR;

  req->cmd = cmd;
  *seen_cmd = 1;
  return OK;
}

/* Finalizes the parsed credential-manager request after argv decoding.
 * It borrows all inputs and writes the validated CLI selection into 'req'.
 * Returns OK on success, ERR when the CLI shape is invalid for cred mode.
 */
static AdbxStatus finalize_cred_req(CredManagerReq *req, int seen_cmd,
                                    int use_everything,
                                    const char *cred_operand) {
  if (!req || !seen_cmd)
    return ERR;

  if (!seen_cmd)
    return ERR;

  switch (req->cmd) {
  case CRED_MAN_SYNC:
  case CRED_MAN_TEST:
    if (use_everything)
      return ERR;
    req->connection_name = cred_operand;
    return OK;
  case CRED_MAN_RESET:
    if (use_everything && cred_operand)
      return ERR;
    if (use_everything) {
      req->reset_scope = CRED_MAN_RESET_SCOPE_ALL;
      req->cred_namespace = NULL;
      return OK;
    }
    if (!cred_operand || cred_operand[0] == '\0')
      return ERR;
    req->reset_scope = CRED_MAN_RESET_SCOPE_NAMESPACE;
    req->cred_namespace = cred_operand;
    return OK;
  default:
    return ERR;
  }
}

/* Runs one credential-manager command selected from the CLI.
 * It borrows 'req' and 'config_input'. The cred manager owns any internal
 * allocations; this wrapper frees only the returned heap error string.
 * Side effects: may prompt on the terminal, read config, and update secret
 * store / credential state depending on the selected command.
 * Returns 0 on success, 1 on failure or invalid input.
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

/* Prints one broker-init error tailored for the most common private-dir
 * misconfiguration.
 * It borrows 'pd' and reads process errno and filesystem existence state.
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
  const char *cred_operand = NULL;
  AppMode mode = APP_MODE_CLIENT;
  int seen_mode = 0;           // 1 if we already see -client/-broker/-cred
  int seen_cred_cmd = 0;       // 1 if we saw -cred
  int cred_use_everything = 0; // used for -cred --resert
  CredManagerReq cred_req = {
      .cmd = CRED_MAN_SYNC,
      .connection_name = NULL,
      .reset_scope = CRED_MAN_RESET_SCOPE_NAMESPACE,
  };

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-client") == 0) {
      if (parse_mode_flag(&mode, &seen_mode, APP_MODE_CLIENT) != OK) {
        print_usage(argv[0]);
        return 1;
      }
    } else if (strcmp(argv[i], "-broker") == 0) {
      if (parse_mode_flag(&mode, &seen_mode, APP_MODE_BROKER) != OK) {
        print_usage(argv[0]);
        return 1;
      }
    } else if (strcmp(argv[i], "-cred") == 0) {
      if (parse_mode_flag(&mode, &seen_mode, APP_MODE_CRED) != OK) {
        print_usage(argv[0]);
        return 1;
      }
    } else if (strcmp(argv[i], "--sync") == 0 || strcmp(argv[i], "-s") == 0) {
      if (parse_cred_cmd_flag(&cred_req, &seen_cred_cmd, CRED_MAN_SYNC) != OK) {
        print_usage(argv[0]);
        return 1;
      }
    } else if (strcmp(argv[i], "--test") == 0 || strcmp(argv[i], "-t") == 0) {
      if (parse_cred_cmd_flag(&cred_req, &seen_cred_cmd, CRED_MAN_TEST) != OK) {
        print_usage(argv[0]);
        return 1;
      }
    } else if (strcmp(argv[i], "--reset") == 0 || strcmp(argv[i], "-r") == 0) {
      if (parse_cred_cmd_flag(&cred_req, &seen_cred_cmd, CRED_MAN_RESET) !=
          OK) {
        print_usage(argv[0]);
        return 1;
      }
    } else if (strcmp(argv[i], "--everything") == 0) {
      cred_use_everything = 1;
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
    } else if (argv[i][0] != '-') {
      if (cred_operand) {
        print_usage(argv[0]);
        return 1;
      }
      cred_operand = argv[i];
    } else {
      print_usage(argv[0]);
      return 1;
    }
  }

  if (mode == APP_MODE_CRED) {
    if (privdir_base ||
        finalize_cred_req(&cred_req, seen_cred_cmd, cred_use_everything,
                          cred_operand) != OK) {
      print_usage(argv[0]);
      return 1;
    }
    return run_cred_mode(&cred_req, config_input);
  }

  // invalid cred flags when -cred is not passed
  if (seen_cred_cmd || cred_use_everything || cred_operand) {
    print_usage(argv[0]);
    return 1;
  }

  if (mode == APP_MODE_CLIENT) {
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
  SecretStoreErr ss_err;
  secrets = secret_store_create(&ss_err);
  if (!secrets) {
    catalog_destroy(cat);
    fprintf(stderr, "ERROR: secret store init failed: %s\n",
            ss_err.msg[0] != '\0' ? ss_err.msg : "unknown error");
    privdir_clean(pd);
    return 1;
  }

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
