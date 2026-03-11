#include "cred_manager.h"

#include "config_dir.h"
#include "conn_catalog.h"
#include "file_io.h"
#include "json_codec.h"
#include "rapidhash.h"
#include "secret_store.h"
#include "string_op.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#define CREDM_STATE_MAX_BYTES (8u * 1024u * 1024u)

typedef enum {
  CREDM_SYNC_ACT_UNKNOWN = 0, // init
  CREDM_SYNC_ACT_KEEP,        // do nothing
  CREDM_SYNC_ACT_PROMPT,      // prompt user to enter the password
  CREDM_SYNC_ACT_RENAME,      // rename a connection
} CredmSyncActionKind;

/* What should we do to sync all the connections from config file to state file
 */
typedef struct {
  CredmSyncActionKind kind;
  size_t state_idx; // SIZE_MAX means "no matching state entry"
} CredmSyncAction;

typedef struct {
  int tty_fd;
  struct termios saved_tio;
  int termios_saved;
  struct sigaction old_int;
  struct sigaction old_term;
  int handlers_installed;
} CredmPromptSession;

static volatile sig_atomic_t g_credm_signal = 0;

/* Writes one formatted user-facing error into '*out_err' once.
 * It borrows all inputs and allocates one heap string owned by caller.
 * Side effects: may allocate memory.
 * Error semantics: best effort; leaves '*out_err' unchanged on allocation
 * failure or when it already contains a message.
 */
static void credm_set_err(char **out_err, const char *fmt, ...) {
  if (!out_err || !fmt || *out_err)
    return;

  char tmp[256];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
  va_end(ap);
  if (n < 0)
    return;

  size_t len = (size_t)n;
  if (len >= sizeof(tmp))
    len = sizeof(tmp) - 1;

  char *msg = (char *)xmalloc(len + 1);

  memcpy(msg, tmp, len);
  msg[len] = '\0';
  *out_err = msg;
}

/* Formats one backend failure using the SecretStore diagnostic snapshot.
 * It borrows 'store' and writes one heap message into '*out_err'.
 * Error semantics: best effort; always returns ERR so callers can tail-return.
 */
static AdbxStatus credm_set_store_err(char **out_err, SecretStore *store,
                                      const char *op_name) {
  credm_set_err(out_err, "credential %s failed: %s", op_name,
                secret_store_last_error(store));
  return ERR;
}

/* Returns YES when two nullable strings are equal, else NO.
 * It borrows both inputs and performs no allocations.
 * Error semantics: returns YES on equality, NO otherwise.
 */
static AdbxTriStatus credm_str_eq_nullable(const char *a, const char *b) {
  if (a == b)
    return YES;
  if (!a || !b)
    return NO;
  return (strcmp(a, b) == 0) ? YES : NO;
}

/* Compares one config profile against one state snapshot entry for sync
 * decisions. It borrows both inputs and performs no allocations.
 * Error semantics: returns YES when all credential-relevant fields are equal,
 * NO when any differ, ERR on invalid input.
 */
static AdbxTriStatus credm_profile_same_sync_fields(const ConnProfile *a,
                                                    const ConnProfile *b) {
  if (!a || !b)
    return ERR;

  if (a->kind != b->kind || a->port != b->port)
    return NO;
  if (strcmp(a->host, b->host) != 0 || strcmp(a->user, b->user) != 0 ||
      strcmp(a->db_name, b->db_name) != 0) {
    return NO;
  }

  return credm_str_eq_nullable(a->options, b->options);
}

/* Compares one config profile against one state snapshot entry for rename
 * detection. It borrows both inputs and performs no allocations.
 * Error semantics: returns YES when the stable identity tuple matches, NO when
 * it differs, ERR on invalid input.
 */
static AdbxTriStatus credm_profile_same_tuple(const ConnProfile *a,
                                              const ConnProfile *b) {
  if (!a || !b)
    return ERR;

  if (a->kind != b->kind || a->port != b->port)
    return NO;
  if (strcmp(a->host, b->host) != 0 || strcmp(a->user, b->user) != 0 ||
      strcmp(a->db_name, b->db_name) != 0) {
    return NO;
  }

  return YES;
}

// "state." (6) + 16 hex digits + ".json" (5) + null terminator = 28 bytes.
#define CREDM_FSTATE_NAME 28
/* Builds one state-file leaf name from the credential namespace hash.
 * It borrows 'cred_namespace' and writes into caller-owned 'out_name'.
 * Error semantics: returns OK on success, ERR on invalid input or truncation.
 */
static AdbxStatus credm_state_file_name(const char *cred_namespace,
                                        char out_name[CREDM_FSTATE_NAME]) {
  if (!cred_namespace || cred_namespace[0] == '\0' || !out_name) {
    return ERR;
  }

  uint64_t ns_hash = rapidhash(cred_namespace, strlen(cred_namespace));
  int n = snprintf(out_name, CREDM_FSTATE_NAME, "state.%016" PRIx64 ".json",
                   ns_hash);
  if (n < 0 || (size_t)n >= CREDM_FSTATE_NAME)
    return ERR;
  return OK;
}

/* Opens one existing app-owned state file relative to 'app_fd' and validates
 * it as a 0600 regular file. It borrows 'state_name' and returns one owned fd
 * in '*out_fd'.
 * Side effects: opens one fd and may chmod the file once to repair mode.
 * Error semantics: returns YES on success, NO when the file does not exist,
 * ERR on invalid input, symlink/policy mismatch, or I/O failure.
 */
static AdbxTriStatus credm_open_state_file(int app_fd, const char *state_name,
                                           int *out_fd) {
  if (app_fd < 0 || !state_name || !out_fd)
    return ERR;
  *out_fd = -1;

  int flags = O_RDONLY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif

  int fd = openat(app_fd, state_name, flags);
  if (fd < 0)
    return (errno == ENOENT) ? NO : ERR;

  if (validate_uown_file(fd, 0600) != OK) {
    (void)close(fd);
    return ERR;
  }

  *out_fd = fd;
  return YES;
}

/* Loads one state snapshot catalog from the default app dir or creates an
 * empty in-memory catalog when the state file is missing. It borrows 'app' and
 * 'cred_namespace'; on success it returns a caller-owned catalog in '*out_cat'.
 * Side effects: may open/read one state file and allocate catalog memory.
 * Error semantics: returns OK on success, else, ERR.
 */
static AdbxStatus credm_load_state_catalog(const ConfDir *app,
                                           const char *cred_namespace,
                                           ConnCatalog **out_cat,
                                           char **out_err) {
  if (!app || app->fd < 0 || !cred_namespace || !out_cat)
    return ERR;
  *out_cat = NULL;

  char state_name[CREDM_FSTATE_NAME];
  if (credm_state_file_name(cred_namespace, state_name) != OK) {
    credm_set_err(out_err,
                  "failed to derive the credential state file name from the "
                  "config namespace.");
    return ERR;
  }

  int state_fd = -1;
  AdbxTriStatus src = credm_open_state_file(app->fd, state_name, &state_fd);
  if (src == NO) {
    *out_cat = catalog_create_empty(cred_namespace);
    if (!*out_cat) {
      credm_set_err(out_err,
                    "failed to allocate an empty credential state snapshot.");
      return ERR;
    }
    return OK;
  }
  if (src == ERR) {
    char *state_path = path_join(app->path, state_name);
    credm_set_err(out_err, "failed to open the credential state file '%s'.",
                  state_path ? state_path : state_name);
    free(state_path);
    return ERR;
  }

  char *load_err = NULL;
  ConnCatalog *state_cat = catalog_load_from_fd(state_fd, &load_err);
  (void)close(state_fd);
  if (!state_cat) {
    credm_set_err(out_err, "failed to parse the credential state file: %s",
                  load_err ? load_err : "unknown error");
    free(load_err);
    return ERR;
  }

  if (strcmp(state_cat->credential_namespace, cred_namespace) != 0) {
    credm_set_err(out_err,
                  "credential state namespace mismatch: expected '%s', found "
                  "'%s'.",
                  cred_namespace, state_cat->credential_namespace);
    catalog_destroy(state_cat);
    return ERR;
  }

  *out_cat = state_cat;
  return OK;
}

/* Writes all bytes from 'buf' to one tty fd. It borrows all inputs and
 * retries short writes.
 * Error semantics: returns OK on success, ERR on invalid input, I/O failure,
 * or interruption.
 */
static AdbxStatus credm_tty_write_all(int tty_fd, const char *buf, size_t len) {
  if (tty_fd < 0 || (!buf && len > 0))
    return ERR;

  size_t off = 0;
  while (off < len) {
    ssize_t wr = write(tty_fd, buf + off, len - off);
    if (wr < 0) {
      if (errno == EINTR && g_credm_signal != 0)
        return ERR;
      if (errno == EINTR)
        continue;
      return ERR;
    }
    off += (size_t)wr;
  }

  return OK;
}

/* Records one caught termination signal so the interactive loop can unwind
 * safely after restoring termios state. It borrows no pointers.
 */
static void credm_on_signal(int signo) { g_credm_signal = signo; }

/* Opens /dev/tty and installs temporary SIGINT/SIGTERM handlers for password
 * prompts. It writes one caller-owned session into 'out'.
 * Side effects: opens one terminal fd and updates process signal handlers.
 * Error semantics: returns OK on success, ERR on invalid input, missing tty,
 * or signal-handler installation failure.
 */
static AdbxStatus credm_prompt_session_begin(CredmPromptSession *out,
                                             char **out_err) {
  if (!out)
    return ERR;

  memset(out, 0, sizeof(*out));
  out->tty_fd = -1;
  g_credm_signal = 0;

  int flags = O_RDWR;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
  out->tty_fd = open("/dev/tty", flags);
  if (out->tty_fd < 0) {
    credm_set_err(out_err,
                  "credential sync needs an interactive terminal to prompt "
                  "for passwords.");
    return ERR;
  }

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = credm_on_signal;
  sigemptyset(&sa.sa_mask);

  if (sigaction(SIGINT, &sa, &out->old_int) != 0) {
    credm_set_err(out_err, "failed to install the SIGINT handler.");
    (void)close(out->tty_fd);
    out->tty_fd = -1;
    return ERR;
  }
  if (sigaction(SIGTERM, &sa, &out->old_term) != 0) {
    (void)sigaction(SIGINT, &out->old_int, NULL);
    credm_set_err(out_err, "failed to install the SIGTERM handler.");
    (void)close(out->tty_fd);
    out->tty_fd = -1;
    return ERR;
  }

  out->handlers_installed = 1;
  return OK;
}

/* Release owned resources of one interactive prompt session and tries to
 * restore tty settings when possible. It consumes 'sess'. */
static void credm_prompt_session_end(CredmPromptSession *sess) {
  if (!sess)
    return;

  if (sess->tty_fd >= 0 && sess->termios_saved) {
    (void)tcsetattr(sess->tty_fd, TCSAFLUSH, &sess->saved_tio);
  }
  if (sess->handlers_installed) {
    (void)sigaction(SIGINT, &sess->old_int, NULL);
    (void)sigaction(SIGTERM, &sess->old_term, NULL);
  }
  if (sess->tty_fd >= 0)
    (void)close(sess->tty_fd);

  memset(sess, 0, sizeof(*sess));
  sess->tty_fd = -1;
  g_credm_signal = 0;
}

/* Save the current tty configuration inside 'sess'->saved_tio. Returns OK on
 * success, else, ERR and modifies 'out_err'. */
static AdbxStatus credm_save_tty_cfg(CredmPromptSession *sess, char **out_err) {
  if (!sess || sess->tty_fd < 0) {
    credm_set_err(out_err,
                  "bad system state detected while trying to save tty config, "
                  "aborted. This is likely a bug, please, report it.");
    return ERR;
  }

  if (tcgetattr(sess->tty_fd, &sess->saved_tio) != 0) {
    credm_set_err(out_err, "failed to read terminal settings.");
    return ERR;
  }
  sess->termios_saved = 1;
  return OK;
}

/* Restores the tty configuration of 'sess'->tty_fd. Returns OK on
 * success, else, ERR and modifies 'out_err'. */
static AdbxStatus credm_restore_tty_cfg(CredmPromptSession *sess,
                                        char **out_err) {
  if (!sess || sess->tty_fd < 0 || !sess->termios_saved) {
    credm_set_err(
        out_err,
        "bad system state detected while trying to restore tty config, "
        "aborted. This is likely a bug, please, report it.");
    return ERR;
  }

  if (tcsetattr(sess->tty_fd, TCSAFLUSH, &sess->saved_tio) != 0) {
    credm_set_err(out_err, "failed to restore terminal settings.");
    return ERR;
  }
  sess->termios_saved = 0;
  return OK;
}

/* Reads one password from /dev/tty with echo disabled and writes the plaintext
 * into caller-owned 'out_secret'. It borrows 'sess' and 'connection_name'.
 * It always restore tty settings to the state right before entering the method
 * unless system crashes.
 */
static AdbxStatus credm_prompt_password(CredmPromptSession *sess,
                                        const char *connection_name,
                                        StrBuf *out_secret, char **out_err) {
  if (!sess || sess->tty_fd < 0 || !connection_name || !out_secret)
    return ERR;

  sb_reset(out_secret);

  char prompt[128];
  int pn =
      snprintf(prompt, sizeof(prompt), "Password for %s: ", connection_name);
  if (pn < 0 || (size_t)pn >= sizeof(prompt)) {
    credm_set_err(out_err, "failed to format the password prompt for '%s'.",
                  connection_name);
    return ERR;
  }
  if (credm_tty_write_all(sess->tty_fd, prompt, (size_t)pn) != OK) {
    credm_set_err(out_err, "failed to write the password prompt.");
    return ERR;
  }

  if (credm_save_tty_cfg(sess, out_err) != OK) {
    return ERR;
  }

  struct termios hidden = sess->saved_tio;
  hidden.c_lflag &= ~(ECHO);
  if (tcsetattr(sess->tty_fd, TCSAFLUSH, &hidden) != 0) {
    credm_set_err(out_err, "failed to disable terminal echo.");
    goto restore_n_ret;
  }

  for (;;) {
    char ch = '\0';
    ssize_t nread = read(sess->tty_fd, &ch, 1);
    if (nread == 0) {
      credm_set_err(out_err, "interactive terminal closed while reading a "
                             "password.");
      goto restore_n_ret;
    }
    if (nread < 0) {
      if (errno == EINTR && g_credm_signal != 0) {
        credm_set_err(out_err, "credential sync interrupted.");
        goto restore_n_ret;
      }
      if (errno == EINTR)
        continue;
      credm_set_err(out_err, "failed while reading a password from the "
                             "terminal.");
      goto restore_n_ret;
    }

    if (ch == '\n')
      break;
    if (ch == '\r')
      continue;
    if (sb_append_bytes(out_secret, &ch, 1) != OK) {
      credm_set_err(out_err, "password is too large to keep in memory.");
      goto restore_n_ret;
    }
  }

  if (credm_tty_write_all(sess->tty_fd, "\n", 1) != OK) {
    credm_set_err(out_err, "failed to write the prompt terminator.");
    goto restore_n_ret;
  }

  if (!sb_to_cstr(out_secret)) {
    credm_set_err(out_err, "failed to terminate the typed password.");
    goto restore_n_ret;
  }

  if (credm_restore_tty_cfg(sess, out_err) != OK)
    return ERR;
  return OK;

restore_n_ret:
  credm_restore_tty_cfg(sess, NULL);
  return ERR;
}

/* Finds one state profile by exact connection name among all snapshot entries.
 * Error semantics: returns YES and writes '*out_idx' on a single match, NO
 * when not found, ERR on invalid input.
 * Note this runs in O(n) because we expect <20 connections per file.
 */
static AdbxTriStatus credm_find_state_name(const ConnCatalog *state_cat,
                                           const char *connection_name,
                                           size_t *out_idx) {
  if (!state_cat || !connection_name || !out_idx)
    return ERR;

  for (size_t i = 0; i < state_cat->n_profiles; i++) {
    if (strcmp(state_cat->profiles[i].connection_name, connection_name) == 0) {
      *out_idx = i;
      return YES;
    }
  }

  return NO;
}

/* Returns the JSON string used for one database backend kind inside config and
 * state snapshots. It borrows no pointers and performs no allocations.
 * Error semantics: returns a stable string on success, NULL for unsupported
 * kinds.
 */
static const char *credm_db_kind_name(DbKind kind) {
  switch (kind) {
  case DB_KIND_POSTGRES:
    return "postgres";
  default:
    return NULL;
  }
}

/* Appends one minimal database entry to a state snapshot JSON builder. It
 * borrows 'profile' and writes JSON into caller-owned 'sb'.
 * Side effects: appends bytes to 'sb'.
 * Error semantics: returns OK on success, ERR on invalid input, unsupported
 * backend kind, or buffer growth failure.
 */
static AdbxStatus credm_append_state_db_json(StrBuf *sb,
                                             const ConnProfile *profile) {
  if (!sb || !profile)
    return ERR;

  const char *kind_name = credm_db_kind_name(profile->kind);
  if (!kind_name)
    return ERR;

  if (json_obj_begin(sb) != OK)
    return ERR;
  if (json_kv_str(sb, "type", kind_name) != OK)
    return ERR;
  if (json_kv_str(sb, "connectionName", profile->connection_name) != OK)
    return ERR;
  if (json_kv_str(sb, "host", profile->host) != OK)
    return ERR;
  if (json_kv_l(sb, "port", (long)profile->port) != OK)
    return ERR;
  if (json_kv_str(sb, "username", profile->user) != OK)
    return ERR;
  if (json_kv_str(sb, "database", profile->db_name) != OK)
    return ERR;
  if (profile->options && json_kv_str(sb, "options", profile->options) != OK)
    return ERR;
  if (json_obj_end(sb) != OK)
    return ERR;

  return OK;
}

/* Builds one minimal state snapshot JSON blob for the given profile list. It
 * borrows 'cred_namespace' and all profile pointers, and writes bytes into
 * caller-owned 'out_json'.
 * Side effects: appends JSON into 'out_json'.
 * Error semantics: returns OK on success, ERR on invalid input, unsupported
 * backend kind, or allocation failure while growing the buffer.
 */
static AdbxStatus credm_build_state_json(StrBuf *out_json,
                                         const char *cred_namespace,
                                         const ConnProfile *const *profiles,
                                         size_t n_profiles) {
  if (!out_json || !cred_namespace || (!profiles && n_profiles > 0))
    return ERR;

  sb_reset(out_json);

  if (json_obj_begin(out_json) != OK)
    return ERR;
  if (json_kv_str(out_json, "version", CURR_CONN_CAT_VERSION) != OK)
    return ERR;
  if (json_kv_str(out_json, "credentialNamespace", cred_namespace) != OK)
    return ERR;
  if (json_kv_obj_begin(out_json, "safetyPolicy") != OK)
    return ERR;
  if (json_obj_end(out_json) != OK)
    return ERR;
  if (json_kv_arr_begin(out_json, "databases") != OK)
    return ERR;
  for (size_t i = 0; i < n_profiles; i++) {
    if (credm_append_state_db_json(out_json, profiles[i]) != OK)
      return ERR;
  }
  if (json_arr_end(out_json) != OK)
    return ERR;
  if (json_obj_end(out_json) != OK)
    return ERR;

  return OK;
}

/* Persists one minimal state snapshot from a profile pointer list. It borrows
 * all inputs except temporary JSON storage owned locally.
 * Side effects: builds JSON in memory and atomically writes one app-owned
 * state file.
 * Error semantics: returns OK on success, ERR on invalid input, snapshot-build
 * failure, filename derivation failure, or atomic-write failure.
 */
static AdbxStatus credm_save_state_profiles(const ConfDir *app,
                                            const char *cred_namespace,
                                            const ConnProfile *const *profiles,
                                            size_t n_profiles, char **out_err) {
  if (!app || app->fd < 0 || !cred_namespace || (!profiles && n_profiles > 0))
    return ERR;

  char state_name[CREDM_FSTATE_NAME];
  if (credm_state_file_name(cred_namespace, state_name) != OK) {
    credm_set_err(out_err,
                  "failed to derive the credential state file name from the "
                  "config namespace.");
    return ERR;
  }

  StrBuf json;
  sb_init(&json);
  if (credm_build_state_json(&json, cred_namespace, profiles, n_profiles) !=
      OK) {
    sb_clean(&json);
    credm_set_err(out_err,
                  "failed to build the updated credential state snapshot.");
    return ERR;
  }

  AdbxTriStatus wrc = write_atomic(app->fd, state_name,
                                   (const uint8_t *)json.data, json.len, NULL);
  sb_clean(&json);
  if (wrc == YES)
    return OK;
  if (wrc == NO) {
    credm_set_err(out_err,
                  "failed to save the credential state because another "
                  "process is writing it concurrently.");
    return ERR;
  }

  credm_set_err(out_err, "failed to save the credential state file.");
  return ERR;
}

/* Finds one named connection inside one catalog. It borrows all inputs and
 * performs no allocations.
 * Error semantics: returns YES and writes '*out_idx' on a match, NO when not
 * found, ERR on invalid input.
 */
static AdbxTriStatus credm_find_catalog_name(const ConnCatalog *cat,
                                             const char *connection_name,
                                             size_t *out_idx) {
  if (!cat)
    return ERR;
  return credm_find_state_name(cat, connection_name, out_idx);
}

/* Decides whether one targeted sync action must rewrite the saved state file.
 * It borrows all inputs and performs only catalog comparisons.
 * Error semantics: returns YES when targeted sync must patch or append state,
 * NO when the current saved state is already correct, ERR on invalid input or
 * inconsistent action data.
 */
static AdbxTriStatus
credm_sync_one_needs_state_write(const ConnCatalog *conf_cat, size_t conf_idx,
                                 const ConnCatalog *state_cat,
                                 const CredmSyncAction *act) {
  if (!conf_cat || conf_idx >= conf_cat->n_profiles || !state_cat || !act)
    return ERR;

  switch (act->kind) {
  case CREDM_SYNC_ACT_KEEP:
    return NO;
  case CREDM_SYNC_ACT_RENAME:
    return YES;
  case CREDM_SYNC_ACT_PROMPT:
    if (act->state_idx == SIZE_MAX)
      return YES;
    if (act->state_idx >= state_cat->n_profiles)
      return ERR;
    {
      AdbxTriStatus same = credm_profile_same_sync_fields(
          &conf_cat->profiles[conf_idx], &state_cat->profiles[act->state_idx]);
      if (same == ERR)
        return ERR;
      return (same == YES) ? NO : YES;
    }
  case CREDM_SYNC_ACT_UNKNOWN:
  default:
    return ERR;
  }
}

/* Plans one targeted sync for a single config connection by reconciling it
 * against the saved state and active secret store. It borrows all inputs and
 * writes one small caller-owned action into 'out_act'.
 *
 * Business logic — three-phase decision for one connection:
 *
 * 1. Name match: look up connection_name in the saved state.
 *    - Found, fields identical, secret present → KEEP  (nothing to do)
 *    - Found, fields identical, secret missing → PROMPT (re-ask password)
 *    - Found, fields differ                   → PROMPT (config changed)
 *
 * 2. Rename detection (no name match): scan ALL state entries by tuple
 *    (host/port/user/db). Also count how many config entries share the same
 *    tuple. Unlike plan_sync_all which uses sequential claiming via
 *    state_seen[], targeted sync has no ordering context, so it checks both
 *    sides to avoid ambiguity:
 *    - Exactly 1 state match AND exactly 1 config match → unique rename
 *      detected. If the old secret exists → RENAME, else → PROMPT.
 *    - Multiple matches on either side → ambiguous, fall through to PROMPT.
 *
 * 3. Fallback: no name match and no unique rename → PROMPT.
 *
 * Side effects: may read the secret store to decide whether prompting is
 * needed.
 * Error semantics: returns OK on success, ERR on invalid input, missing target
 * connection, or secret-store lookup failure.
 */
static AdbxStatus credm_plan_sync_one(const ConnCatalog *conf_cat,
                                      const ConnCatalog *state_cat,
                                      const char *connection_name,
                                      SecretStore *store,
                                      CredmSyncAction *out_act,
                                      size_t *out_conf_idx, char **out_err) {
  if (!conf_cat || !state_cat || !connection_name || !store || !out_act ||
      !out_conf_idx)
    return ERR;

  memset(out_act, 0, sizeof(*out_act));
  out_act->kind = CREDM_SYNC_ACT_UNKNOWN;
  out_act->state_idx = SIZE_MAX;

  size_t conf_idx = 0;
  AdbxTriStatus crc =
      credm_find_catalog_name(conf_cat, connection_name, &conf_idx);
  if (crc == ERR) {
    credm_set_err(out_err,
                  "credential sync hit an internal bug while resolving the "
                  "target connection.");
    return ERR;
  }
  if (crc == NO) {
    credm_set_err(out_err,
                  "connection '%s' was not found in the current config.",
                  connection_name);
    return ERR;
  }

  *out_conf_idx = conf_idx;
  const ConnProfile *conf_p = &conf_cat->profiles[conf_idx];

  // Phase 1: try to match by connection name in the saved state.
  size_t state_idx = SIZE_MAX;
  AdbxTriStatus src =
      credm_find_catalog_name(state_cat, connection_name, &state_idx);
  if (src == ERR) {
    credm_set_err(out_err,
                  "credential sync hit an internal bug while resolving the "
                  "saved state entry.");
    return ERR;
  }

  if (src == YES) {
    out_act->state_idx = state_idx;

    // name matched — check whether the connection fields changed
    AdbxTriStatus same =
        credm_profile_same_sync_fields(conf_p, &state_cat->profiles[state_idx]);
    if (same == ERR) {
      credm_set_err(out_err,
                    "credential sync hit an internal bug while comparing the "
                    "current config against saved state.");
      return ERR;
    }
    if (same == NO) {
      // fields changed: the old password is no longer valid
      out_act->kind = CREDM_SYNC_ACT_PROMPT;
      return OK;
    }

    // fields unchanged — only KEEP if the secret actually exists in the store
    StrBuf secret;
    sb_init(&secret);
    AdbxTriStatus grc = secret_store_get(store, &conf_p->secret_ref, &secret);
    sb_zero_clean(&secret);
    if (grc == ERR)
      return credm_set_store_err(out_err, store, "lookup");

    out_act->kind = (grc == YES) ? CREDM_SYNC_ACT_KEEP : CREDM_SYNC_ACT_PROMPT;
    return OK;
  }

  // Phase 2: no name match — try rename detection.
  // Count how many state entries share the same tuple (host/port/user/db).
  size_t tuple_match_idx = 0;
  size_t tuple_match_count = 0;
  for (size_t i = 0; i < state_cat->n_profiles; i++) {
    AdbxTriStatus same =
        credm_profile_same_tuple(conf_p, &state_cat->profiles[i]);
    if (same == ERR) {
      credm_set_err(out_err,
                    "credential sync hit an internal bug while comparing the "
                    "target connection against saved state.");
      return ERR;
    }
    if (same == YES) {
      tuple_match_idx = i;
      tuple_match_count++;
    }
  }

  // Count how many config entries share the same tuple as the target.
  // If >1 config entries have the same tuple, we can't tell which one is the
  // "real" rename, so we must fall through to PROMPT.
  size_t conf_tuple_match_count = 0;
  for (size_t i = 0; i < conf_cat->n_profiles; i++) {
    AdbxTriStatus same =
        credm_profile_same_tuple(conf_p, &conf_cat->profiles[i]);
    if (same == ERR) {
      credm_set_err(out_err,
                    "credential sync hit an internal bug while comparing the "
                    "target connection against the current config.");
      return ERR;
    }
    if (same == YES)
      conf_tuple_match_count++;
  }

  // Unique 1:1 rename: exactly one state entry and one config entry share the
  // tuple. Reuse the old secret if it still exists, otherwise prompt.
  if (tuple_match_count == 1 && conf_tuple_match_count == 1) {
    out_act->state_idx = tuple_match_idx;

    StrBuf secret;
    sb_init(&secret);
    AdbxTriStatus grc = secret_store_get(
        store, &state_cat->profiles[tuple_match_idx].secret_ref, &secret);
    sb_zero_clean(&secret);
    if (grc == ERR)
      return credm_set_store_err(out_err, store, "lookup");

    out_act->kind =
        (grc == YES) ? CREDM_SYNC_ACT_RENAME : CREDM_SYNC_ACT_PROMPT;
    return OK;
  }

  // Phase 3: ambiguous or no tuple match — must prompt for a fresh password.
  out_act->kind = CREDM_SYNC_ACT_PROMPT;
  out_act->state_idx = SIZE_MAX;
  return OK;
}

/* Applies one sync action for a single connection against the secret store.
 * It borrows all inputs. The caller must provide a valid 'sess' when
 * act->kind == CREDM_SYNC_ACT_PROMPT; it may be NULL otherwise.
 * Side effects: may prompt on /dev/tty (via 'sess') and mutate the store.
 * Error semantics: returns OK on success, ERR on failure.
 */
static AdbxStatus credm_apply_action(const ConnProfile *conf_p,
                                     const ConnCatalog *state_cat,
                                     const CredmSyncAction *act,
                                     SecretStore *store,
                                     CredmPromptSession *sess,
                                     char **out_err) {
  if (!conf_p || !state_cat || !act || !store)
    return ERR;

  if (act->kind == CREDM_SYNC_ACT_KEEP)
    return OK;

  if (act->kind == CREDM_SYNC_ACT_RENAME) {
    const ConnProfile *old_p = &state_cat->profiles[act->state_idx];
    StrBuf secret;
    sb_init(&secret);
    AdbxTriStatus grc = secret_store_get(store, &old_p->secret_ref, &secret);
    if (grc == ERR) {
      sb_zero_clean(&secret);
      return credm_set_store_err(out_err, store,
                                 "lookup failure during sync. Please, retry.");
    }
    if (grc == NO) {
      sb_zero_clean(&secret);
      credm_set_err(out_err,
                    "stored credential for renamed connection '%s' "
                    "disappeared during sync; rerun the command.",
                    old_p->connection_name);
      return ERR;
    }

    fprintf(stderr,
            "LOG: reusing stored credential for renamed connection '%s' -> "
            "'%s'\n",
            old_p->connection_name, conf_p->connection_name);
    if (secret_store_set(store, &conf_p->secret_ref, secret.data) != OK) {
      sb_zero_clean(&secret);
      return credm_set_store_err(
          out_err, store, "secret write failure during sync. Please, retry.");
    }
    if (secret_store_delete(store, &old_p->secret_ref) != OK) {
      sb_zero_clean(&secret);
      return credm_set_store_err(
          out_err, store, "secret delete failure during sync. Please, retry.");
    }
    sb_zero_clean(&secret);
    return OK;
  }

  if (act->kind == CREDM_SYNC_ACT_PROMPT) {
    if (!sess)
      return ERR;
    StrBuf secret;
    sb_init(&secret);
    AdbxStatus rc = OK;
    if (credm_prompt_password(sess, conf_p->connection_name, &secret,
                              out_err) != OK) {
      rc = ERR;
    } else if (secret_store_set(store, &conf_p->secret_ref, secret.data) !=
               OK) {
      rc = credm_set_store_err(out_err, store, "write");
    }
    sb_zero_clean(&secret);
    return rc;
  }

  credm_set_err(out_err, "credential sync hit an internal reconciliation bug.");
  return ERR;
}

/* Applies one targeted sync action to the active secret store. It borrows all
 * inputs and manages its own prompt session when the action requires one.
 * Side effects: may prompt on /dev/tty and mutate the secret store.
 * Error semantics: returns OK on success, ERR on failure.
 */
static AdbxStatus credm_apply_sync_one(const ConnCatalog *conf_cat,
                                       size_t conf_idx,
                                       const ConnCatalog *state_cat,
                                       const CredmSyncAction *act,
                                       SecretStore *store, char **out_err) {
  if (!conf_cat || conf_idx >= conf_cat->n_profiles || !state_cat || !act ||
      !store)
    return ERR;

  if (act->kind == CREDM_SYNC_ACT_KEEP)
    return OK;

  CredmPromptSession sess;
  memset(&sess, 0, sizeof(sess));
  sess.tty_fd = -1;
  int need_prompt = (act->kind == CREDM_SYNC_ACT_PROMPT);
  if (need_prompt && credm_prompt_session_begin(&sess, out_err) != OK)
    return ERR;

  AdbxStatus rc = credm_apply_action(&conf_cat->profiles[conf_idx], state_cat,
                                     act, store, need_prompt ? &sess : NULL,
                                     out_err);

  if (need_prompt)
    credm_prompt_session_end(&sess);
  return rc;
}

/* Persists one targeted sync state patch while keeping unrelated state entries
 * untouched. It borrows all inputs and builds one temporary pointer list.
 * Side effects: serializes and atomically writes one updated state snapshot.
 * Error semantics: returns OK on success, ERR on invalid input, allocation
 * failure, or snapshot write failure.
 */
static AdbxStatus
credm_save_state_sync_one(const ConfDir *app, const ConnCatalog *state_cat,
                          const ConnCatalog *conf_cat, size_t conf_idx,
                          const ConnProfile *target, const CredmSyncAction *act,
                          char **out_err) {
  if (!app || !state_cat || !conf_cat || conf_idx >= conf_cat->n_profiles ||
      !target || !act)
    return ERR;

  AdbxTriStatus wrc =
      credm_sync_one_needs_state_write(conf_cat, conf_idx, state_cat, act);
  if (wrc == ERR) {
    credm_set_err(out_err,
                  "credential sync hit an internal reconciliation bug while "
                  "updating saved state.");
    return ERR;
  }
  if (wrc == NO)
    return OK;

  size_t n_out = state_cat->n_profiles + (act->state_idx == SIZE_MAX ? 1u : 0u);
  const ConnProfile **profiles =
      (const ConnProfile **)xcalloc(n_out ? n_out : 1, sizeof(*profiles));

  if (act->state_idx != SIZE_MAX) {
    if (act->state_idx >= state_cat->n_profiles) {
      free(profiles);
      credm_set_err(out_err,
                    "credential sync hit an internal reconciliation bug while "
                    "patching saved state.");
      return ERR;
    }
    for (size_t i = 0; i < state_cat->n_profiles; i++) {
      profiles[i] = (i == act->state_idx) ? target : &state_cat->profiles[i];
    }
  } else {
    for (size_t i = 0; i < state_cat->n_profiles; i++) {
      profiles[i] = &state_cat->profiles[i];
    }
    profiles[state_cat->n_profiles] = target;
  }

  AdbxStatus rc = credm_save_state_profiles(
      app, target->secret_ref.cred_namespace, profiles, n_out, out_err);
  free(profiles);
  return rc;
}

/* Plans one full sync by reconciling config entries against the saved state and
 * current secret store. It allocates 'actions' and 'state_seen' for caller.
 *
 * Business logic — two-phase decision for all connections:
 *
 * Phase 1 — name matching: for each config entry, look up its connection name
 * in the saved state.
 *   - Found, fields identical, secret present → KEEP  (nothing to do)
 *   - Found, fields identical, secret missing → PROMPT (re-ask password)
 *   - Found, fields differ                   → PROMPT (config changed)
 *   - Not found                              → leave as UNKNOWN for phase 2
 *   Each matched state entry is marked in state_seen[] so it cannot be
 *   claimed twice. After this phase, state_seen[] also tells the apply step
 *   which state entries were removed (unseen → delete their secrets).
 *
 * Phase 2 — rename detection: for each still-UNKNOWN config entry, scan only
 * the UNSEEN state entries by tuple (host/port/user/db).
 *   - Exactly 1 unseen state match → unique rename detected. Claim it (mark
 *     seen). If the old secret exists → RENAME, else → PROMPT.
 *   - 0 or >1 matches → ambiguous or brand new, → PROMPT.
 *   Unlike plan_sync_one which checks both sides (state AND config tuple
 *   counts), plan_sync_all handles ambiguity via sequential claiming: the
 *   first config entry that uniquely matches a state entry claims it, so
 *   later entries with the same tuple see it as already-seen and fall
 *   through to PROMPT.
 *
 * Error semantics: returns OK on success, else, ERR and modifies 'out_err'.
 */
static AdbxStatus
credm_plan_sync_all(const ConnCatalog *conf_cat, const ConnCatalog *state_cat,
                    SecretStore *store, CredmSyncAction **out_actions,
                    unsigned char **out_state_seen, char **out_err) {
  if (!conf_cat || !state_cat || !store || !out_actions || !out_state_seen) {
    return ERR;
  }

  *out_actions = NULL;
  *out_state_seen = NULL;

  size_t conf_n = conf_cat->n_profiles;
  size_t state_n = state_cat->n_profiles;
  CredmSyncAction *actions =
      (CredmSyncAction *)xcalloc(conf_n ? conf_n : 1, sizeof(*actions));
  unsigned char *state_seen =
      (unsigned char *)xcalloc(state_n ? state_n : 1, sizeof(*state_seen));

  for (size_t i = 0; i < conf_n; i++) {
    actions[i].kind = CREDM_SYNC_ACT_UNKNOWN;
    actions[i].state_idx = 0;
  }

  // Phase 1: match each config entry by connection name in the saved state.
  for (size_t i = 0; i < conf_n; i++) {
    const ConnProfile *conf_p = &conf_cat->profiles[i];
    size_t state_idx = 0;
    AdbxTriStatus mrc =
        credm_find_state_name(state_cat, conf_p->connection_name, &state_idx);
    if (mrc == ERR) {
      free(actions);
      free(state_seen);
      return credm_set_store_err(out_err, store,
                                 "invalid values during sync. This is probably "
                                 "a bug, please, report it.");
    }
    if (mrc == NO)
      continue; // no name match — leave UNKNOWN for phase 2

    // name matched — claim the state entry so phase 2 won't reuse it
    state_seen[state_idx] = 1;
    actions[i].state_idx = state_idx;

    // check whether the connection fields changed
    const ConnProfile *state_p = &state_cat->profiles[state_idx];
    AdbxTriStatus same = credm_profile_same_sync_fields(conf_p, state_p);
    if (same == ERR) {
      free(actions);
      free(state_seen);
      return credm_set_store_err(out_err, store,
                                 "invalid values during sync. This is probably "
                                 "a bug, please, report it.");
    }
    if (same == NO) {
      // fields changed: the old password is no longer valid
      actions[i].kind = CREDM_SYNC_ACT_PROMPT;
      continue;
    }

    // fields unchanged — only KEEP if the secret actually exists in the store
    StrBuf secret;
    sb_init(&secret);
    AdbxTriStatus src = secret_store_get(store, &conf_p->secret_ref, &secret);
    sb_zero_clean(&secret);
    if (src == ERR) {
      free(actions);
      free(state_seen);
      return credm_set_store_err(out_err, store,
                                 "invalid state during lookup. This is "
                                 "probably a bug, please, report it.");
    }
    actions[i].kind =
        (src == YES) ? CREDM_SYNC_ACT_KEEP : CREDM_SYNC_ACT_PROMPT;
  }

  // Phase 2: rename detection for entries that had no name match.
  // Scan only UNSEEN state entries by tuple. The first config entry that
  // uniquely matches claims the state entry, preventing later duplicates.
  for (size_t i = 0; i < conf_n; i++) {
    if (actions[i].kind != CREDM_SYNC_ACT_UNKNOWN)
      continue;

    size_t match_idx = 0;
    size_t match_count = 0;
    for (size_t j = 0; j < state_n; j++) {
      if (state_seen[j])
        continue; // already claimed by phase 1 or an earlier rename
      AdbxTriStatus same = credm_profile_same_tuple(&conf_cat->profiles[i],
                                                    &state_cat->profiles[j]);
      if (same == ERR) {
        free(actions);
        free(state_seen);
        return credm_set_store_err(
            out_err, store,
            "invalid values during sync. This is probably "
            "a bug, please, report it.");
      }
      if (same == YES) {
        match_idx = j;
        match_count++;
      }
    }

    if (match_count == 1) {
      // unique rename — claim the state entry
      state_seen[match_idx] = 1;
      actions[i].state_idx = match_idx;

      // we can only reuse the credential if the old secret still exists
      StrBuf secret;
      sb_init(&secret);
      AdbxTriStatus src = secret_store_get(
          store, &state_cat->profiles[match_idx].secret_ref, &secret);
      sb_zero_clean(&secret);
      if (src == ERR) {
        free(actions);
        free(state_seen);
        return credm_set_store_err(out_err, store, "lookup");
      }
      if (src == YES) {
        actions[i].kind = CREDM_SYNC_ACT_RENAME;
      } else {
        actions[i].kind = CREDM_SYNC_ACT_PROMPT;
      }
      continue;
    }

    // 0 or >1 matches — ambiguous or brand new connection
    actions[i].kind = CREDM_SYNC_ACT_PROMPT;
  }

  *out_actions = actions;
  *out_state_seen = state_seen;
  return OK;
}

/* Applies one full sync plan against the active secret store. It borrows all
 * inputs. Opens one prompt session shared across all entries that need it,
 * then deletes secrets for state entries no longer present in the config.
 * Side effects: may prompt on /dev/tty, mutate the secret store, and log
 * automatic rename reuse messages to stderr.
 * Error semantics: returns OK on success, ERR on invalid input, terminal
 * failures, interruptions, or secret-store mutation failures.
 */
static AdbxStatus credm_apply_sync_all(const ConnCatalog *conf_cat,
                                       const ConnCatalog *state_cat,
                                       const CredmSyncAction *actions,
                                       const unsigned char *state_seen,
                                       SecretStore *store, char **out_err) {
  if (!conf_cat || !state_cat || !actions || !state_seen || !store)
    return ERR;

  CredmPromptSession sess;
  memset(&sess, 0, sizeof(sess));
  sess.tty_fd = -1;

  int need_prompt = 0;
  for (size_t i = 0; i < conf_cat->n_profiles; i++) {
    if (actions[i].kind == CREDM_SYNC_ACT_PROMPT)
      need_prompt = 1;
  }

  if (need_prompt && credm_prompt_session_begin(&sess, out_err) != OK)
    return ERR;

  AdbxStatus rc = OK;
  for (size_t i = 0; i < conf_cat->n_profiles; i++) {
    rc = credm_apply_action(&conf_cat->profiles[i], state_cat, &actions[i],
                            store, need_prompt ? &sess : NULL, out_err);
    if (rc != OK)
      break;
  }

  // delete secrets for state entries that are no longer in the config
  if (rc == OK) {
    for (size_t i = 0; i < state_cat->n_profiles; i++) {
      if (state_seen[i])
        continue;
      if (secret_store_delete(store, &state_cat->profiles[i].secret_ref) !=
          OK) {
        rc = credm_set_store_err(out_err, store, "delete");
        break;
      }
    }
  }

  if (need_prompt)
    credm_prompt_session_end(&sess);
  return rc;
}

/* Persists the current config bytes as the new credential state snapshot.
 * It borrows the opened config file and app dir; callers retain ownership.
 * Side effects: rewinds/reads the config fd and atomically writes one app-owned
 * state file.
 * Error semantics: returns OK on success, ERR on invalid input, config read
 * failure, filename derivation failure, or atomic-write failure.
 */
static AdbxStatus credm_save_state_snapshot(const ConfDir *app,
                                            const char *cred_namespace,
                                            int config_fd, char **out_err) {
  if (!app || app->fd < 0 || !cred_namespace || config_fd < 0)
    return ERR;

  char state_name[CREDM_FSTATE_NAME];
  if (credm_state_file_name(cred_namespace, state_name) != OK) {
    credm_set_err(out_err,
                  "failed to derive the credential state file name from the "
                  "config namespace.");
    return ERR;
  }

  if (lseek(config_fd, 0, SEEK_SET) < 0) {
    credm_set_err(out_err, "failed to rewind the config file before saving "
                           "credential state.");
    return ERR;
  }

  StrBuf raw;
  sb_init(&raw);
  if (fileio_sb_read_limit_fd(config_fd, CREDM_STATE_MAX_BYTES, &raw) != OK) {
    sb_clean(&raw);
    credm_set_err(out_err, "failed to read the config file before saving the "
                           "credential state.");
    return ERR;
  }

  AdbxTriStatus wrc = write_atomic(app->fd, state_name,
                                   (const uint8_t *)raw.data, raw.len, NULL);
  sb_clean(&raw);
  if (wrc == YES)
    return OK;

  if (wrc == NO) {
    credm_set_err(out_err,
                  "failed to save the credential state because another "
                  "process is writing it concurrently.");
    return ERR;
  }

  credm_set_err(out_err, "failed to save the credential state file.");
  return ERR;
}

/* Executes the namespace-wide sync path by reconciling the current config,
 * saved state, and active secret store. It borrows 'config_input' and returns
 * user-facing errors through '*out_err'.
 * Side effects: opens config/app-dir handles, reads/writes the secret store,
 * may prompt on the terminal, and writes the state snapshot.
 * Error semantics: returns OK on success, ERR on invalid input, config/state
 * failures, secret-store failures, prompt failures, or snapshot write
 * failures.
 */
static AdbxStatus credm_execute_sync_all(const char *config_input,
                                         char **out_err) {
  ConfFile cfg = {.fd = -1, .path = NULL};
  ConfDir app = {.fd = -1, .path = NULL};
  ConnCatalog *conf_cat = NULL;  // the config.json catalog
  ConnCatalog *state_cat = NULL; // the state.json catalog
  SecretStore *store = NULL;
  CredmSyncAction *actions = NULL;
  unsigned char *state_seen = NULL;
  AdbxStatus rc = ERR;

  char *cfg_err = NULL;
  if (confdir_open(config_input, &cfg, &cfg_err) != OK) {
    credm_set_err(out_err, "failed to open the config file: %s",
                  cfg_err ? cfg_err : "unknown error");
    free(cfg_err);
    goto cleanup;
  }

  char *cat_err = NULL;
  conf_cat = catalog_load_from_fd(cfg.fd, &cat_err);
  if (!conf_cat) {
    credm_set_err(out_err, "failed to parse the config file: %s",
                  cat_err ? cat_err : "unknown error");
    free(cat_err);
    goto cleanup;
  }

  char *app_err = NULL;
  if (confdir_default_open(&app, NULL, &app_err) != OK) {
    credm_set_err(out_err, "failed to open the credential state directory: %s",
                  app_err ? app_err : "unknown error");
    free(app_err);
    goto cleanup;
  }

  if (credm_load_state_catalog(&app, conf_cat->credential_namespace, &state_cat,
                               out_err) != OK) {
    goto cleanup;
  }

  store = secret_store_create();
  if (!store) {
    credm_set_err(out_err, "failed to initialize the configured secret store.");
    goto cleanup;
  }

  if (credm_plan_sync_all(conf_cat, state_cat, store, &actions, &state_seen,
                          out_err) != OK) {
    goto cleanup;
  }

  if (credm_apply_sync_all(conf_cat, state_cat, actions, state_seen, store,
                           out_err) != OK) {
    goto cleanup;
  }

  if (credm_save_state_snapshot(&app, conf_cat->credential_namespace, cfg.fd,
                                out_err) != OK) {
    goto cleanup;
  }

  rc = OK;

cleanup:
  free(actions);
  free(state_seen);
  secret_store_destroy(store);
  catalog_destroy(state_cat);
  catalog_destroy(conf_cat);
  confdir_clean(&app);
  conffile_clean(&cfg);
  return rc;
}

/* Executes targeted sync for one named connection by reconciling only that
 * config entry against the saved state and active secret store. It borrows
 * 'config_input' and 'connection_name' and returns user-facing errors through
 * '*out_err'.
 * Side effects: opens config/app-dir handles, may prompt on the terminal,
 * mutates the secret store, and writes a patched state snapshot when needed.
 * Error semantics: returns OK on success, ERR on invalid input, config/state
 * failures, prompt failures, secret-store failures, or state-write failures.
 */
static AdbxStatus credm_execute_sync_one(const char *config_input,
                                         const char *connection_name,
                                         char **out_err) {
  if (!connection_name || connection_name[0] == '\0') {
    credm_set_err(out_err,
                  "targeted credential sync requires a non-empty connection "
                  "name.");
    return ERR;
  }

  ConfFile cfg = {.fd = -1, .path = NULL};
  ConfDir app = {.fd = -1, .path = NULL};
  ConnCatalog *conf_cat = NULL;
  ConnCatalog *state_cat = NULL;
  SecretStore *store = NULL;
  CredmSyncAction act;
  size_t conf_idx = 0;
  AdbxStatus rc = ERR;

  char *cfg_err = NULL;
  if (confdir_open(config_input, &cfg, &cfg_err) != OK) {
    credm_set_err(out_err, "failed to open the config file: %s",
                  cfg_err ? cfg_err : "unknown error");
    free(cfg_err);
    goto cleanup;
  }

  char *cat_err = NULL;
  conf_cat = catalog_load_from_fd(cfg.fd, &cat_err);
  if (!conf_cat) {
    credm_set_err(out_err, "failed to parse the config file: %s",
                  cat_err ? cat_err : "unknown error");
    free(cat_err);
    goto cleanup;
  }

  char *app_err = NULL;
  if (confdir_default_open(&app, NULL, &app_err) != OK) {
    credm_set_err(out_err, "failed to open the credential state directory: %s",
                  app_err ? app_err : "unknown error");
    free(app_err);
    goto cleanup;
  }

  if (credm_load_state_catalog(&app, conf_cat->credential_namespace, &state_cat,
                               out_err) != OK) {
    goto cleanup;
  }

  store = secret_store_create();
  if (!store) {
    credm_set_err(out_err, "failed to initialize the configured secret store.");
    goto cleanup;
  }

  if (credm_plan_sync_one(conf_cat, state_cat, connection_name, store, &act,
                          &conf_idx, out_err) != OK) {
    goto cleanup;
  }

  if (credm_apply_sync_one(conf_cat, conf_idx, state_cat, &act, store,
                           out_err) != OK) {
    goto cleanup;
  }

  if (credm_save_state_sync_one(&app, state_cat, conf_cat, conf_idx,
                                &conf_cat->profiles[conf_idx], &act,
                                out_err) != OK) {
    goto cleanup;
  }

  rc = OK;

cleanup:
  secret_store_destroy(store);
  catalog_destroy(state_cat);
  catalog_destroy(conf_cat);
  confdir_clean(&app);
  conffile_clean(&cfg);
  return rc;
}

AdbxStatus cred_manager_execute(const CredManagerReq *req,
                                const char *config_input, char **out_err) {
  if (out_err)
    *out_err = NULL;

  if (!req) {
    credm_set_err(out_err, "credential command failed: invalid request.");
    return ERR;
  }

  switch (req->cmd) {
  case CRED_MAN_SYNC:
    if (req->connection_name)
      return credm_execute_sync_one(config_input, req->connection_name,
                                    out_err);
    return credm_execute_sync_all(config_input, out_err);
  case CRED_MAN_TEST:
    credm_set_err(out_err, "credential testing is not implemented yet.");
    return ERR;
  case CRED_MAN_PRUNE:
    credm_set_err(out_err, "credential pruning is not implemented yet.");
    return ERR;
  case CRED_MAN_RESET:
    credm_set_err(out_err, "credential reset is not implemented yet.");
    return ERR;
  case CRED_MAN_LIST:
    credm_set_err(out_err, "credential listing is not implemented yet.");
    return ERR;
  default:
    credm_set_err(out_err, "credential command failed: unknown command.");
    return ERR;
  }
}
