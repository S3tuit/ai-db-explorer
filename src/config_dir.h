#ifndef CONFIG_DIR_H
#define CONFIG_DIR_H

#include "utils.h"

#define CONFDIR_APP_DIRNAME "adbxplorer"

typedef struct {
  int fd;
  char *path; // display/log path owned by caller
} ConfDir;

typedef struct {
  int fd;
  char *path; // display/log path owned by caller
} ConfFile;

typedef enum {
  CONFDIR_ERR_NONE = 0,
  CONFDIR_ERR_ENV,
  CONFDIR_ERR_DIR,
} ConfDirErrCode;

/* Opens the default application config directory for the current platform.
 * 'out_code' and 'out_err' may be NULL if caller doesn't care about them.
 *
 * Behavior:
 * - Linux:
 *   - $XDG_CONFIG_HOME/adbxplorer when XDG_CONFIG_HOME is set to an absolute
 *     path.
 *   - $HOME/.config/adbxplorer otherwise.
 * - macOS:
 *   - $HOME/Library/Application Support/adbxplorer
 *
 * Default-path behavior:
 * - The base config directory must already exist, except Linux's HOME/.config
 *   fallback, which is created when missing and XDG_CONFIG_HOME is unset.
 * - The app-owned 'adbxplorer' directory is created or chmod'd to 0700.
 *
 * Ownership:
 * - On OK, 'out->fd' is opened and 'out->path' is heap-allocated; caller must
 *   later call confdir_clean().
 * - On ERR, '*out_code' reports whether the failure came from invalid env
 *   inputs or from base/app directory open/create/validation.
 * - On ERR, '*out_err' may contain an allocated error string that caller must
 *   free.
 */
AdbxStatus confdir_default_open(ConfDir *out, ConfDirErrCode *out_code,
                                char **out_err);

/* Opens the runtime config file under the current policy.
 *
 * Behavior:
 * - If 'input_path' is non-NULL and non-empty, it must be an absolute path to
 *   an existing regular file; final symlinks are allowed and no directories or
 *   files are created.
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
 * - On OK, 'out->fd' is opened and 'out->path' is heap-allocated; caller must
 *   later call conffile_clean().
 * - On ERR, '*out_err' may contain an allocated error string that caller must
 *   free.
 */
AdbxStatus confdir_open(const char *input_path, ConfFile *out, char **out_err);

/* Releases one caller-owned default-config directory handle.
 * It consumes 'cd'.
 */
void confdir_clean(ConfDir *cd);

/* Releases one caller-owned opened config file handle.
 * It consumes 'cf'.
 */
void conffile_clean(ConfFile *cf);

#endif
