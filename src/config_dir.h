#ifndef CONFIG_DIR_H
#define CONFIG_DIR_H

#include "utils.h"

#define CONFDIR_APP_DIRNAME "adbxplorer"

/* Resolves the default configuration base directory for the current platform.
 *
 * Behavior:
 * - Linux:
 *   - $XDG_CONFIG_HOME when XDG_CONFIG_HOME is set to an absolute path.
 *   - $HOME/.config otherwise.
 * - macOS:
 *   - $HOME/Library/Application Support
 *
 * Ownership:
 * - On OK, 'out_fd' is opened and must be closed by caller. If 'out_path' is
 * not NULL, a caller-owned string will be assigned to it.
 * - On ERR, '*out_err' may contain an allocated error string that caller must
 *   free.
 */
AdbxStatus confdir_get_default_base_dir(int *out_fd, char **out_path,
                                        char **out_err);

/* Resolves the runtime config file path under the current policy.
 *
 * Behavior:
 * - If 'input_path' is non-NULL and non-empty, it must be an absolute path to
 *   an existing regular file; no directories or files are created.
 * - Otherwise the default path is used:
 *   - Linux default: $XDG_CONFIG_HOME/adbxplorer/config.json when
 *     XDG_CONFIG_HOME is set to an absolute path.
 *   - Linux fallback: $HOME/.config/adbxplorer/config.json
 *   - macOS fallback: $HOME/Library/Application Support/adbxplorer/config.json
 *
 * Default-path behavior:
 * - The base config directory must already exist, except Linux's HOME/.config
 *   fallback, which is created when missing and XDG_CONFIG_HOME is unset.
 * - The app-owned 'adbxplorer' directory is created or chmod'd to 0700.
 * - The default config file is created when missing and chmod'd to 0600.
 *
 * Ownership:
 * - On OK, '*out_path' is heap-allocated and must be freed by caller.
 * - On ERR, '*out_err' may contain an allocated error string that caller must
 *   free.
 */
AdbxStatus confdir_resolve(const char *input_path, char **out_path,
                           char **out_err);

#endif
