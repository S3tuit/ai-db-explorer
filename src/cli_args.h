#ifndef CLI_ARGS_H
#define CLI_ARGS_H

#include <stddef.h>

#include "cred_manager.h"
#include "utils.h"

typedef enum {
  APP_MODE_CLIENT = 0,
  APP_MODE_BROKER,
  APP_MODE_WHICH_CONFIG,
  APP_MODE_CRED,
  APP_MODE_HELP,
  APP_MODE_VERSION,
} AppMode;

/* Parsed command-line selection consumed by main().
 * All string fields are borrowed directly from argv storage; this struct does
 * not own heap memory.
 */
typedef struct {
  AppMode mode;
  const char *app_dir_input;
  const char *config_input;
  CredManagerReq cred_req;
} CliArgs;

/* Parses argv into one structured CLI selection.
 * It borrows 'argv', writes borrowed string views into caller-owned 'out', and
 * writes a human-readable parse error into caller-owned 'err_buf' on failure.
 * Returns OK on success, ERR on invalid CLI shape or invalid input.
 */
AdbxStatus cli_parse_args(int argc, char **argv, CliArgs *out, char *err_buf,
                          size_t err_cap);

#endif
