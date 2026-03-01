#ifndef CONFIG_DIR_H
#define CONFIG_DIR_H

#include "utils.h"

/* Resolves the runtime config file path and ensures both parent directory and
 * file exist.
 *
 * Behavior:
 * - If 'input_path' is non-NULL and non-empty, that path is used.
 * - Otherwise the default path is used:
 *   - $XDG_CONFIG_HOME/adbxplorer/config.json when XDG_CONFIG_HOME is set.
 *   - Linux fallback: $HOME/.config/adbxplorer/config.json
 *   - macOS fallback: $HOME/Library/Application
 * Support/adbxplorer/config.json
 *
 * If the file does not exist, a built-in default config is created.
 *
 * Ownership:
 * - On OK, '*out_path' is heap-allocated and must be freed by caller.
 * - On ERR, '*out_err' may contain an allocated error string that caller must
 *   free.
 */
AdbxStatus confdir_resolve(const char *input_path, char **out_path,
                           char **out_err);

#endif
