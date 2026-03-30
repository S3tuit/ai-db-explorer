#include "cli_args.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* Writes one formatted CLI parse error into caller-owned storage.
 * It borrows 'err_buf' and 'fmt' and truncates the message when needed.
 * Side effects: mutates 'err_buf'.
 * Returns OK on success, ERR on invalid input.
 */
static AdbxStatus cli_set_errf(char *err_buf, size_t err_cap, const char *fmt,
                               ...) {
  if (!err_buf || err_cap == 0 || !fmt)
    return ERR;

  va_list args;
  va_start(args, fmt);
  int written = vsnprintf(err_buf, err_cap, fmt, args);
  va_end(args);
  if (written < 0) {
    err_buf[0] = '\0';
    return ERR;
  }
  return OK;
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

/* Applies one credential-manager subcommand flag 'cmd' to 'req' while
 * rejecting conflicting command mixes. It borrows 'req' and 'seen_cmd'. Sets
 * 'seen_cmd' to 1 on success. Returns OK on success, ERR when the caller
 * combines different cred subcommands or passes invalid parser state.
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

AdbxStatus cli_parse_args(int argc, char **argv, CliArgs *out, char *err_buf,
                          size_t err_cap) {
  const char *cred_operand = NULL;
  int seen_mode = 0;           // 1 if we already saw -client/-broker/-cred
  int seen_cred_cmd = 0;       // 1 if we saw a credential subcommand
  int cred_use_everything = 0; // used for -cred --reset

  if (argc < 1 || !argv || !out || !err_buf || err_cap == 0)
    return ERR;

  memset(out, 0, sizeof(*out));
  err_buf[0] = '\0';
  out->mode = APP_MODE_CLIENT;
  out->cred_req.cmd = CRED_MAN_SYNC;
  out->cred_req.reset_scope = CRED_MAN_RESET_SCOPE_NAMESPACE;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-client") == 0) {
      if (parse_mode_flag(&out->mode, &seen_mode, APP_MODE_CLIENT) != OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting top-level modes: '-client', '-broker', "
                     "'-which-config', '-cred', '-h/--help', and "
                     "'--version' are mutually exclusive");
        return ERR;
      }
    } else if (strcmp(argv[i], "-broker") == 0) {
      if (parse_mode_flag(&out->mode, &seen_mode, APP_MODE_BROKER) != OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting top-level modes: '-client', '-broker', "
                     "'-which-config', '-cred', '-h/--help', and "
                     "'--version' are mutually exclusive");
        return ERR;
      }
    } else if (strcmp(argv[i], "-which-config") == 0) {
      if (parse_mode_flag(&out->mode, &seen_mode, APP_MODE_WHICH_CONFIG) !=
          OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting top-level modes: '-client', '-broker', "
                     "'-which-config', '-cred', '-h/--help', and "
                     "'--version' are mutually exclusive");
        return ERR;
      }
    } else if (strcmp(argv[i], "-cred") == 0) {
      if (parse_mode_flag(&out->mode, &seen_mode, APP_MODE_CRED) != OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting top-level modes: '-client', '-broker', "
                     "'-which-config', '-cred', '-h/--help', and "
                     "'--version' are mutually exclusive");
        return ERR;
      }
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      if (parse_mode_flag(&out->mode, &seen_mode, APP_MODE_HELP) != OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting top-level modes: '-client', '-broker', "
                     "'-which-config', '-cred', '-h/--help', and "
                     "'--version' are mutually exclusive");
        return ERR;
      }
    } else if (strcmp(argv[i], "--version") == 0) {
      if (parse_mode_flag(&out->mode, &seen_mode, APP_MODE_VERSION) != OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting top-level modes: '-client', '-broker', "
                     "'-which-config', '-cred', '-h/--help', and "
                     "'--version' are mutually exclusive");
        return ERR;
      }
    } else if (strcmp(argv[i], "--sync") == 0 || strcmp(argv[i], "-s") == 0) {
      if (parse_cred_cmd_flag(&out->cred_req, &seen_cred_cmd, CRED_MAN_SYNC) !=
          OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting credential commands: choose only one of "
                     "'--sync', '--test', or '--reset'");
        return ERR;
      }
    } else if (strcmp(argv[i], "--test") == 0 || strcmp(argv[i], "-t") == 0) {
      if (parse_cred_cmd_flag(&out->cred_req, &seen_cred_cmd, CRED_MAN_TEST) !=
          OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting credential commands: choose only one of "
                     "'--sync', '--test', or '--reset'");
        return ERR;
      }
    } else if (strcmp(argv[i], "--reset") == 0 || strcmp(argv[i], "-r") == 0) {
      if (parse_cred_cmd_flag(&out->cred_req, &seen_cred_cmd,
                              CRED_MAN_RESET) != OK) {
        cli_set_errf(err_buf, err_cap,
                     "conflicting credential commands: choose only one of "
                     "'--sync', '--test', or '--reset'");
        return ERR;
      }
    } else if (strcmp(argv[i], "--everything") == 0) {
      cred_use_everything = 1;
    } else if (strcmp(argv[i], "-appdir") == 0) {
      if (i + 1 >= argc) {
        cli_set_errf(err_buf, err_cap, "missing path after '-appdir'");
        return ERR;
      }
      out->app_dir_input = argv[++i];
    } else if (strcmp(argv[i], "-config") == 0) {
      if (i + 1 >= argc) {
        cli_set_errf(err_buf, err_cap, "missing path after '-config'");
        return ERR;
      }
      out->config_input = argv[++i];
    } else if (argv[i][0] != '-') {
      if (cred_operand) {
        cli_set_errf(err_buf, err_cap,
                     "unexpected extra positional argument '%s'", argv[i]);
        return ERR;
      }
      cred_operand = argv[i];
    } else {
      cli_set_errf(err_buf, err_cap, "unknown flag '%s'", argv[i]);
      return ERR;
    }
  }

  if (out->mode == APP_MODE_HELP || out->mode == APP_MODE_VERSION) {
    if (out->app_dir_input || out->config_input || seen_cred_cmd ||
        cred_use_everything || cred_operand) {
      cli_set_errf(err_buf, err_cap,
                   "help/version flags do not accept other options");
      return ERR;
    }
    return OK;
  }

  if (out->mode == APP_MODE_CRED) {
    if (out->app_dir_input) {
      cli_set_errf(err_buf, err_cap,
                   "'-appdir' is not supported in '-cred' mode");
      return ERR;
    }
    if (finalize_cred_req(&out->cred_req, seen_cred_cmd, cred_use_everything,
                          cred_operand) != OK) {
      cli_set_errf(err_buf, err_cap,
                   "invalid '-cred' invocation: choose one credential command "
                   "and provide the required operand shape");
      return ERR;
    }
    return OK;
  }

  if (seen_cred_cmd || cred_use_everything || cred_operand) {
    cli_set_errf(err_buf, err_cap,
                 "credential-only flags require '-cred' mode");
    return ERR;
  }

  return OK;
}
