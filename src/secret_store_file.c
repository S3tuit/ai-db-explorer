#include "secret_store.h"

#include "arena.h"
#include "config_dir.h"
#include "file_io.h"
#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

// TODO: we should ALWAYS print a warning log to the user when SecretStoreFile
// is in use, users should prefer libsecret/keychain.

#define SS_CRED_FILE "credentials.json"
#define SS_FILE_VERSION "1"
#define SS_FILE_MAX_BYTES (16u * 1024u * 1024u)

typedef struct {
  const char *cred_namespace;
  const char *connection_name;
  const char *secret; // the password of the database (in plain text)
} SecretEntry;

typedef struct {
  SecretEntry *entries;
  size_t n_entries;
  Arena arena; // owns all the strings and arrays of SecretEntryList
} SecretEntryList;

typedef struct {
  SecretStore base;
  int dir_fd; // file descriptor of the configuration directory

  SecretEntryList cache;
  FileMeta cache_meta;
  int cache_loaded;
  SecretStoreErrCode last_err_code;
  char last_err_msg[256];
} FileSecretStore;

/* Clears one backend error snapshot.
 */
static void ss_clear_err(FileSecretStore *store) {
  if (!store)
    return;
  store->last_err_code = SSERR_NONE;
  store->last_err_msg[0] = '\0';
}

/* Stores one formatted backend error snapshot for diagnostics.
 * It borrows 'store' and 'fmt'; no allocations.
 * Side effects: updates store error code/message.
 * Error semantics: none.
 */
static void ss_set_err(FileSecretStore *store, SecretStoreErrCode code,
                       const char *fmt, ...) {
  if (!store)
    return;

  store->last_err_code = code;
  store->last_err_msg[0] = '\0';
  if (!fmt)
    return;

  va_list ap;
  va_start(ap, fmt);
  (void)vsnprintf(store->last_err_msg, sizeof(store->last_err_msg), fmt, ap);
  va_end(ap);
}

/* Zeroes and frees one heap-allocated secret string.
 * It consumes '*s'.
 */
static void ss_secret_free(char **s) {
  if (!s || !*s)
    return;
  size_t n = strlen(*s);
  memset(*s, 0, n);
  free(*s);
  *s = NULL;
}

/* Clears one cached entry list and zeroes stored values.
 * It consumes arena-owned memory in 'list'.
 */
static void ss_entries_clean(SecretEntryList *list) {
  if (!list)
    return;

  AdbxTriStatus zeroed = arena_is_zeroed(&list->arena);
  if (zeroed == NO) {
    arena_zero_mem(&list->arena);
    arena_clean(&list->arena);
  }

  list->entries = NULL;
  list->n_entries = 0;
}

/* Compares one secret reference tuple.
 * It borrows all inputs and performs no allocations.
 * Error semantics: returns strcmp-style ordering by namespace then
 * connection-name.
 */
static inline int ss_ref_cmp_parts(const char *a_namespace,
                                   const char *a_conn_name,
                                   const char *b_namespace,
                                   const char *b_conn_name) {
  int ns_cmp = strcmp(a_namespace, b_namespace);
  if (ns_cmp != 0)
    return ns_cmp;
  return strcmp(a_conn_name, b_conn_name);
}

/* Comparator for final entries by namespace then connection name.
 * It borrows both inputs and performs no allocations.
 * Error semantics: returns qsort/bsearch-compatible ordering.
 */
static int ss_entry_cmp(const void *a, const void *b) {
  const SecretEntry *ea = (const SecretEntry *)a;
  const SecretEntry *eb = (const SecretEntry *)b;
  return ss_ref_cmp_parts(ea->cred_namespace, ea->connection_name,
                          eb->cred_namespace, eb->connection_name);
}

/* Validates one typed secret reference for backend operations.
 * It borrows inputs and writes backend error state on failure.
 * Side effects: updates backend error state when invalid.
 * Error semantics: returns OK on valid namespace+connection name, ERR
 * otherwise.
 */
static AdbxStatus ss_validate_ref(FileSecretStore *store,
                                  const SecretRefInfo *ref,
                                  const char *op_name) {
  if (!store || !op_name)
    return ERR;

  if (!ref || !ref->cred_namespace || !ref->connection_name) {
    ss_set_err(store, SSERR_INPUT,
               "secret-store %s failed: invalid input pointers. This is "
               "probably a bug, please, report it.",
               op_name);
    return ERR;
  }

  if (ref->cred_namespace[0] == '\0' || ref->connection_name[0] == '\0') {
    ss_set_err(store, SSERR_INPUT,
               "secret-store %s failed: secret reference fields cannot be "
               "empty. This is probably a bug, please, report it.",
               op_name);
    return ERR;
  }

  return OK;
}

/* Duplicates one C-string into arena-owned NUL-terminated storage.
 * It borrows 's' and returns a pointer owned by 'arena'.
 * Side effects: allocates inside arena.
 * Error semantics: returns NULL on invalid input or allocation failure.
 */
static inline char *ss_arena_dup_cstr(Arena *arena, const char *s) {
  if (!arena || !s)
    return NULL;
  size_t n = strlen(s);
  if (n > UINT32_MAX)
    return NULL;
  return (char *)arena_add_nul(arena, (void *)s, (uint32_t)n);
}

/* Initializes one list with arena-backed entry array of size n.
 * It writes owned storage into 'out'.
 * Side effects: initializes arena and allocates entry array.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_init_with_n_entries(SecretEntryList *out, size_t n) {
  assert(out);

  out->entries = NULL;
  out->n_entries = 0;
  memset(&out->arena, 0, sizeof(out->arena));

  if (n == 0)
    return OK;

  if (n > ((size_t)UINT32_MAX / sizeof(*out->entries)))
    return ERR;

  if (arena_init(&out->arena, NULL, NULL) != OK)
    return ERR;

  SecretEntry *entries = (SecretEntry *)arena_calloc(
      &out->arena, (uint32_t)(n * sizeof(*entries)));
  if (!entries) {
    ss_entries_clean(out);
    return ERR;
  }

  out->entries = entries;
  out->n_entries = n;
  return OK;
}

/* Copies one typed secret ref/secret pair into one list entry.
 * It borrows all inputs and writes arena-owned strings in list[idx].
 * Side effects: allocates strings in list arena.
 * Error semantics: returns OK on success, ERR on allocation failure.
 */
static inline AdbxStatus ss_list_set_entry_copy(SecretEntryList *list,
                                                size_t idx,
                                                const SecretRefInfo *ref,
                                                const char *secret) {
  assert(list);
  assert(ref);
  assert(ref->cred_namespace);
  assert(ref->connection_name);
  assert(secret);
  assert(idx < list->n_entries);

  list->entries[idx].cred_namespace =
      ss_arena_dup_cstr(&list->arena, ref->cred_namespace);
  list->entries[idx].connection_name =
      ss_arena_dup_cstr(&list->arena, ref->connection_name);
  list->entries[idx].secret = ss_arena_dup_cstr(&list->arena, secret);
  if (!list->entries[idx].cred_namespace ||
      !list->entries[idx].connection_name || !list->entries[idx].secret)
    return ERR;

  return OK;
}

/* Finds one typed ref position in sorted list.
 * It borrows list/ref and writes one position to out_pos.
 * Side effects: none.
 * Error semantics: returns YES when found, NO when missing (out_pos is insert
 * point).
 */
static AdbxTriStatus ss_list_find_ref_pos(const SecretEntryList *list,
                                          const SecretRefInfo *ref,
                                          size_t *out_pos) {
  assert(list);
  assert(ref);
  assert(ref->cred_namespace);
  assert(ref->connection_name);
  assert(out_pos);

  size_t lo = 0;
  size_t hi = list->n_entries;
  while (lo < hi) {
    size_t mid = lo + ((hi - lo) / 2u);
    int cmp = ss_ref_cmp_parts(list->entries[mid].cred_namespace,
                               list->entries[mid].connection_name,
                               ref->cred_namespace, ref->connection_name);
    if (cmp < 0) {
      lo = mid + 1u;
    } else if (cmp > 0) {
      hi = mid;
    } else {
      *out_pos = mid;
      return YES;
    }
  }

  *out_pos = lo;
  return NO;
}

/* Builds 'out_next' list by upserting one typed ref/secret pair into a sorted
 * source list. It borrows all inputs and creates a new object that can outlive
 * 'src'.
 * Side effects: allocates output arena memory.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_build_with_upsert(const SecretEntryList *src,
                                            const SecretRefInfo *ref,
                                            const char *secret,
                                            SecretEntryList *out_next) {
  if (!src || !ref || !secret || !out_next)
    return ERR;

  size_t pos = 0;
  AdbxTriStatus fnd = ss_list_find_ref_pos(src, ref, &pos);
  if (fnd == ERR)
    return ERR;

  size_t out_n = src->n_entries + ((fnd == YES) ? 0u : 1u);
  if (ss_list_init_with_n_entries(out_next, out_n) != OK)
    return ERR;

  size_t src_i = 0;
  for (size_t dst_i = 0; dst_i < out_n; dst_i++) {
    // If not found, the new entry will be the first element of the array.
    // Else, this updates the list at the index of the element found.
    if (dst_i == pos) {
      if (ss_list_set_entry_copy(out_next, dst_i, ref, secret) != OK) {
        ss_entries_clean(out_next);
        return ERR;
      }
      if (fnd == YES)
        src_i++;
      continue;
    }

    if (src_i >= src->n_entries) {
      ss_entries_clean(out_next);
      return ERR;
    }
    SecretRefInfo src_ref = {
        .cred_namespace = src->entries[src_i].cred_namespace,
        .connection_name = src->entries[src_i].connection_name};
    if (ss_list_set_entry_copy(out_next, dst_i, &src_ref,
                               src->entries[src_i].secret) != OK) {
      ss_entries_clean(out_next);
      return ERR;
    }
    src_i++;
  }

  return OK;
}

/* Builds 'out_next' list by removing one entry index from sorted source list.
 * It borrows src and creates a new object that can outlive 'src'.
 * Side effects: allocates output arena memory.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_build_without_index(const SecretEntryList *src,
                                              size_t remove_idx,
                                              SecretEntryList *out_next) {
  if (!src || !out_next || remove_idx >= src->n_entries)
    return ERR;

  size_t out_n = src->n_entries - 1u;
  if (ss_list_init_with_n_entries(out_next, out_n) != OK)
    return ERR;

  size_t dst_i = 0;
  for (size_t src_i = 0; src_i < src->n_entries; src_i++) {
    if (src_i == remove_idx)
      continue;

    SecretRefInfo src_ref = {
        .cred_namespace = src->entries[src_i].cred_namespace,
        .connection_name = src->entries[src_i].connection_name};
    if (ss_list_set_entry_copy(out_next, dst_i, &src_ref,
                               src->entries[src_i].secret) != OK) {
      ss_entries_clean(out_next);
      return ERR;
    }
    dst_i++;
  }

  return OK;
}

/* Builds 'out_next' list by removing all entries in one namespace.
 * It borrows src/cred_namespace and creates a new object that can outlive
 * 'src'.
 * Side effects: allocates output arena memory.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_build_without_namespace(const SecretEntryList *src,
                                                  const char *cred_namespace,
                                                  SecretEntryList *out_next) {
  if (!src || !cred_namespace || !out_next)
    return ERR;

  size_t keep_n = 0;
  for (size_t i = 0; i < src->n_entries; i++) {
    if (strcmp(src->entries[i].cred_namespace, cred_namespace) != 0)
      keep_n++;
  }

  if (ss_list_init_with_n_entries(out_next, keep_n) != OK)
    return ERR;

  size_t dst_i = 0;
  for (size_t src_i = 0; src_i < src->n_entries; src_i++) {
    if (strcmp(src->entries[src_i].cred_namespace, cred_namespace) == 0)
      continue;

    SecretRefInfo src_ref = {
        .cred_namespace = src->entries[src_i].cred_namespace,
        .connection_name = src->entries[src_i].connection_name};
    if (ss_list_set_entry_copy(out_next, dst_i, &src_ref,
                               src->entries[src_i].secret) != OK) {
      ss_entries_clean(out_next);
      return ERR;
    }
    dst_i++;
  }

  return OK;
}

/* Copies cached refs into one heap-owned SecretRefList for callers.
 * It borrows 'src' and writes one owned list into 'out'.
 * Side effects: allocates heap strings and list storage.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_copy_refs(const SecretEntryList *src,
                                    SecretRefList *out) {
  if (!src || !out)
    return ERR;

  out->items = NULL;
  out->n_items = 0;
  if (src->n_entries == 0)
    return OK;

  SecretRefInfo *items =
      (SecretRefInfo *)calloc(src->n_entries, sizeof(*items));
  if (!items)
    return ERR;

  out->items = items;
  out->n_items = src->n_entries;
  for (size_t i = 0; i < src->n_entries; i++) {
    items[i].cred_namespace = dup_or_null(src->entries[i].cred_namespace);
    items[i].connection_name = dup_or_null(src->entries[i].connection_name);
    if (!items[i].cred_namespace || !items[i].connection_name) {
      secret_ref_list_clean(out);
      return ERR;
    }
  }

  return OK;
}

/* Opens the app directory used for file-backed credentials and stores its fd
 * inside 'out_fd'. It borrows 'store' and returns one owned fd to caller.
 * Side effects: may create the default app directory through config_dir and
 * updates backend error state on failure.
 * Error semantics: returns OK on success, ERR on invalid environment,
 * config-dir resolution failure, or input.
 */
static AdbxStatus ss_open_config_dir(FileSecretStore *store, int *out_fd) {
  if (!store || !out_fd)
    return ERR;
  *out_fd = -1;

  ConfDir app = {.fd = -1, .path = NULL};
  ConfDirErrCode cfg_code = CONFDIR_ERR_NONE;
  char *cfg_err = NULL;
  if (confdir_default_open(&app, &cfg_code, &cfg_err) != OK) {
    SecretStoreErrCode code =
        (cfg_code == CONFDIR_ERR_ENV) ? SSERR_ENV : SSERR_DIR;
    ss_set_err(store, code, "secret-store init failed: %s",
               cfg_err ? cfg_err : "unable to resolve config base path");
    free(cfg_err);
    confdir_clean(&app);
    return ERR;
  }
  free(cfg_err);

  *out_fd = app.fd;
  app.fd = -1;
  confdir_clean(&app);
  return OK;
}

/* Resolves a best-effort filesystem path for one open file descriptor.
 * It borrows 'fd' and writes into caller-owned 'buf' when successful.
 * Side effects: reads symlink metadata from procfs/devfs.
 * Error semantics: returns 'buf' on success, otherwise returns "<unknown>".
 */
static const char *ss_fd_path_or_unknown(int fd, char *buf, size_t cap) {
  if (fd < 0 || !buf || cap == 0)
    return "<unknown>";
  buf[0] = '\0';

  char link_path[64];

  int n = snprintf(link_path, sizeof(link_path), "/proc/self/fd/%d", fd);
  if (n > 0 && (size_t)n < sizeof(link_path)) {
    ssize_t got = readlink(link_path, buf, cap - 1);
    if (got >= 0) {
      buf[(size_t)got] = '\0';
      return buf;
    }
  }

  n = snprintf(link_path, sizeof(link_path), "/dev/fd/%d", fd);
  if (n > 0 && (size_t)n < sizeof(link_path)) {
    ssize_t got = readlink(link_path, buf, cap - 1);
    if (got >= 0) {
      buf[(size_t)got] = '\0';
      return buf;
    }
  }

  return "<unknown>";
}

/* Returns the file descriptor of the directory where FileSecretStore stores its
 * files. Returns -1 on error. Side effects: may create the directory. Error
 * semantics: returns OK on success, ERR on invalid environment or policy
 * mismatch.
 */
static inline int ss_get_dir_fd(FileSecretStore *store) {
  if (!store)
    return -1;

  int dir_fd = store->dir_fd;
  // dir_fd already opened
  if (dir_fd >= 0) {
    if (fcntl(dir_fd, F_GETFD) >= 0)
      return dir_fd;
  }

  if (ss_open_config_dir(store, &dir_fd) != OK || dir_fd < 0) {
    store->dir_fd = -1;
    return -1;
  }

  if (validate_uown_dir(dir_fd, 0700) != OK) {
    char dir_path[PATH_MAX];
    ss_set_err(store, SSERR_DIR,
               "secret-store init failed: config dir must be owned by current "
               "user and have mode 0700. Dir: %s.",
               ss_fd_path_or_unknown(dir_fd, dir_path, sizeof(dir_path)));
    store->dir_fd = -1;
    close(dir_fd);
    return -1;
  }

  store->dir_fd = dir_fd;
  return dir_fd;
}

/* Validates one opened credentials file descriptor and snapshots metadata.
 * It borrows 'file_fd' and writes into caller-owned 'out_meta'.
 * Side effects: reads file metadata.
 * Error semantics: returns OK when file is a valid user-owned regular file
 * with mode 0600, ERR otherwise.
 */
static AdbxStatus ss_validate_credfile_fd(FileSecretStore *store, int file_fd,
                                          FileMeta *out_meta) {
  if (!store || file_fd < 0 || !out_meta)
    return ERR;

  struct stat st = {0};
  if (fstat(file_fd, &st) != 0) {
    int saved_errno = errno;
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: cannot stat credentials file: "
               "<config-dir>/adbxplorer/credentials.json. %s.",
               strerror(saved_errno));
    return ERR;
  }

  if (!S_ISREG(st.st_mode)) {
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: credentials file must be a regular "
               "file: <config-dir>/adbxplorer/credentials.json.");
    return ERR;
  }
  if (st.st_uid != getuid()) {
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: credentials file owner mismatch: "
               "<config-dir>/adbxplorer/credentials.json.");
    return ERR;
  }
  if ((st.st_mode & 0777) != 0600) {
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: credentials file mode is %03o, "
               "expected 600. Fix with: chmod 600 "
               "<config-dir>/adbxplorer/credentials.json",
               (unsigned)(st.st_mode & 0777));
    return ERR;
  }

  if (fileio_meta_from_stat(&st, out_meta) != OK) {
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: unable to snapshot credentials file "
               "metadata: <config-dir>/adbxplorer/credentials.json.");
    return ERR;
  }
  return OK;
}

/* Opens credentials file in read mode and validates ownership/permissions.
 * It borrows 'store' and writes outputs to caller-owned 'out_fd/out_meta'.
 * Side effects: opens one fd and reads metadata; does not create files.
 * Error semantics: returns YES on success, NO when file is missing, ERR on
 * invalid input or filesystem/policy failures.
 */
static AdbxTriStatus ss_open_credfile_read(FileSecretStore *store, int *out_fd,
                                           FileMeta *out_meta) {
  if (!store || !out_fd || !out_meta) {
    if (store) {
      ss_set_err(store, SSERR_INPUT,
                 "secret-store internal error: invalid read-credentials input. "
                 "This is probably an internal bug, please, report it.");
    }
    return ERR;
  }
  *out_fd = -1;
  memset(out_meta, 0, sizeof(*out_meta));

  int dir_fd = ss_get_dir_fd(store);
  if (dir_fd < 0)
    return ERR;

  int flags = O_RDONLY;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  int file_fd = openat(dir_fd, SS_CRED_FILE, flags);
  if (file_fd < 0) {
    if (errno == ENOENT)
      return NO;
    int saved_errno = errno;
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: cannot open '%s': %s.", SS_CRED_FILE,
               strerror(saved_errno));
    return ERR;
  }

  if (ss_validate_credfile_fd(store, file_fd, out_meta) != OK) {
    close(file_fd);
    return ERR;
  }

  *out_fd = file_fd;
  return YES;
}

/* Serializes one entry list into JSON payload.
 * It borrows 'entries' and writes into caller-owned 'out_json'.
 * Side effects: mutates StrBuf content.
 * Error semantics: returns OK on success, ERR on allocation or encoding
 * failure.
 */
static AdbxStatus ss_serialize_entries(const SecretEntryList *entries,
                                       StrBuf *out_json) {
  if (!entries || !out_json)
    return ERR;

  sb_reset(out_json);

  if (json_obj_begin(out_json) != OK)
    goto error;
  if (json_kv_str(out_json, "version", SS_FILE_VERSION) != OK)
    goto error;
  if (json_kv_arr_begin(out_json, "entries") != OK)
    goto error;

  for (size_t i = 0; i < entries->n_entries; i++) {
    if (json_obj_begin(out_json) != OK)
      goto error;
    if (json_kv_str(out_json, "credentialNamespace",
                    entries->entries[i].cred_namespace) != OK)
      goto error;
    if (json_kv_str(out_json, "connectionName",
                    entries->entries[i].connection_name) != OK)
      goto error;
    if (json_kv_str(out_json, "secret", entries->entries[i].secret) != OK)
      goto error;
    if (json_obj_end(out_json) != OK)
      goto error;
  }

  if (json_arr_end(out_json) != OK)
    goto error;
  if (json_obj_end(out_json) != OK)
    goto error;

  return OK;

error:
  sb_reset(out_json);
  return ERR;
}

/* Parses JSON payload into one sorted SecretEntryList.
 * It borrows 'json' and allocates all output memory in out_list arena.
 * Side effects: allocates temporary decoded strings and output arena.
 * Error semantics: returns OK on success, ERR on malformed input,
 * duplicate namespace+connection pairs, or allocation failures.
 */
static AdbxStatus ss_parse_entries_json(FileSecretStore *store,
                                        const char *json, size_t json_len,
                                        SecretEntryList *out_list) {
  if (!store || !json || !out_list)
    return ERR;

  out_list->entries = NULL;
  out_list->n_entries = 0;
  memset(&out_list->arena, 0, sizeof(out_list->arena));

  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  if (jsget_init(&jg, json, json_len, &tok_buf) != OK) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file is malformed JSON (or token limit exceeded). "
               "Fix credentials.json format or delete it to reset.");
    return ERR;
  }

  const char *const top_keys[] = {"version", "entries"};
  JsonStrSpan top_unknown = {0};
  if (jsget_top_level_validation(&jg, NULL, top_keys, ARRLEN(top_keys),
                                 &top_unknown) != YES) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file schema invalid: expected top-level keys "
               "'version' and 'entries'. Fix credentials.json format or delete "
               "it to reset.");
    return ERR;
  }

  char *version = NULL;
  if (jsget_string_decode_alloc(&jg, "version", &version) != YES) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file schema invalid: missing string 'version'. Fix "
               "credentials.json format or delete it to reset.");
    return ERR;
  }
  int ver_ok = (strcmp(version, SS_FILE_VERSION) == 0);
  free(version);
  if (!ver_ok) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file version is unsupported; expected version "
               "'%s'. Fix credentials.json format or delete it to reset.",
               SS_FILE_VERSION);
    return ERR;
  }

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(&jg, "entries", &it) != YES) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file schema invalid: 'entries' must be an array of "
               "objects. Fix credentials.json format or delete it to reset.");
    return ERR;
  }

  // first, we allocate all the entries, unordered, at the beginning of the
  // arena
  if (it.count < 0) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file schema invalid: invalid 'entries' count. Fix "
               "credentials.json format or delete it to reset.");
    return ERR;
  }
  size_t n_entries = (size_t)it.count;
  if (ss_list_init_with_n_entries(out_list, n_entries) != OK) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file parse failed: memory allocation error. "
               "Please, retry.");
    return ERR;
  }

  size_t fill_i = 0;
  for (;;) {
    JsonGetter entry = {0};
    AdbxTriStatus nrc = jsget_array_objects_next(&jg, &it, &entry);
    if (nrc == NO)
      break;
    if (nrc != YES) {
      ss_set_err(store, SSERR_PARSE,
                 "credentials file schema invalid: entries must be objects. "
                 "Fix credentials.json format or delete it to reset.");
      goto parse_error;
    }

    const char *const entry_keys[] = {"credentialNamespace", "connectionName",
                                      "secret"};
    JsonStrSpan entry_unknown = {0};
    if (jsget_top_level_validation(&entry, NULL, entry_keys, ARRLEN(entry_keys),
                                   &entry_unknown) != YES) {
      ss_set_err(
          store, SSERR_PARSE,
          "credentials file schema invalid: each entry must contain only "
          "'credentialNamespace', 'connectionName', and 'secret'. Fix "
          "credentials.json format or delete it to reset.");
      goto parse_error;
    }

    char *cred_namespace = NULL;
    char *connection_name = NULL;
    char *secret = NULL;
    if (jsget_string_decode_alloc(&entry, "credentialNamespace",
                                  &cred_namespace) != YES) {
      ss_set_err(store, SSERR_PARSE,
                 "credentials file schema invalid: entry.credentialNamespace "
                 "must be a string. Fix credentials.json format or delete it "
                 "to reset.");
      goto parse_error;
    }
    if (jsget_string_decode_alloc(&entry, "connectionName", &connection_name) !=
        YES) {
      ss_set_err(store, SSERR_PARSE,
                 "credentials file schema invalid: entry.connectionName must "
                 "be a string. Fix credentials.json format or delete it to "
                 "reset.");
      free(cred_namespace);
      goto parse_error;
    }
    if (jsget_string_decode_alloc(&entry, "secret", &secret) != YES) {
      ss_set_err(store, SSERR_PARSE,
                 "credentials file schema invalid: entry.secret must be a "
                 "string. Fix credentials.json format or delete it to reset.");
      free(cred_namespace);
      free(connection_name);
      goto parse_error;
    }

    if (fill_i >= out_list->n_entries || cred_namespace[0] == '\0' ||
        connection_name[0] == '\0') {
      ss_set_err(
          store, SSERR_PARSE,
          "credentials file schema invalid: entry.credentialNamespace and "
          "entry.connectionName must be non-empty. Fix credentials.json "
          "format or delete it to reset.");
      free(cred_namespace);
      free(connection_name);
      ss_secret_free(&secret);
      goto parse_error;
    }

    // TODO: add a json_codec decode-into-arena helper to avoid this extra
    // heap-allocation+copy path for decoded strings.

    // transfer ownership of the strings to the arena and wire the new pointers
    // to the corresponding entry
    SecretRefInfo ref = {.cred_namespace = cred_namespace,
                         .connection_name = connection_name};
    if (ss_list_set_entry_copy(out_list, fill_i, &ref, secret) != OK) {
      ss_set_err(store, SSERR_PARSE,
                 "credentials file parse failed: memory allocation error. Fix "
                 "credentials.json format or delete it to reset.");
      free(cred_namespace);
      free(connection_name);
      ss_secret_free(&secret);
      goto parse_error;
    }
    fill_i++;
    free(cred_namespace);
    free(connection_name);
    ss_secret_free(&secret);
  }

  // defensive check, we should've filled exactly n_entries
  if (fill_i != out_list->n_entries) {
    ss_set_err(
        store, SSERR_PARSE,
        "credentials file schema invalid: entries array is inconsistent. Fix "
        "credentials.json format or delete it to reset.");
    goto parse_error;
  }

  if (out_list->n_entries > 1)
    qsort(out_list->entries, out_list->n_entries, sizeof(*out_list->entries),
          ss_entry_cmp);

  // makes sure there are no duplicates secret references, else fail close
  for (size_t i = 1; i < out_list->n_entries; i++) {
    if (ss_ref_cmp_parts(out_list->entries[i - 1].cred_namespace,
                         out_list->entries[i - 1].connection_name,
                         out_list->entries[i].cred_namespace,
                         out_list->entries[i].connection_name) == 0) {
      ss_set_err(
          store, SSERR_PARSE,
          "credentials file schema invalid: duplicate connection reference "
          "'%s/%s'. Fix credentials.json format or delete it to reset.",
          out_list->entries[i].cred_namespace,
          out_list->entries[i].connection_name);
      goto parse_error;
    }
  }

  return OK;

parse_error:
  ss_entries_clean(out_list);
  return ERR;
}

/* Loads credentials at the already opened 'file_fd' and stores them into list.
 * It borrows file_fd and writes an owned list to out_list.
 * Error semantics: returns OK on success, ERR on I/O/parsing failures.
 */
static AdbxStatus ss_load_list_from_fd(FileSecretStore *store, int file_fd,
                                       SecretEntryList *out_list) {
  if (!store || file_fd < 0 || !out_list)
    return ERR;

  StrBuf sb;
  sb_init(&sb);

  if (fileio_sb_read_limit_fd(file_fd, SS_FILE_MAX_BYTES, &sb) != OK) {
    ss_set_err(store, SSERR_CRED_FILE,
               "secret-store read failed: unable to read credentials file "
               "(size limit %u bytes).",
               (unsigned)SS_FILE_MAX_BYTES);
    sb_clean(&sb);
    return ERR;
  }

  if (sb.len == 0) {
    ss_set_err(store, SSERR_PARSE,
               "credentials file is empty. Fix credentials.json with valid "
               "JSON: {\"version\":\"1\",\"entries\":[]}.");
    sb_clean(&sb);
    return ERR;
  }

  AdbxStatus rc = ss_parse_entries_json(store, sb.data, sb.len, out_list);
  sb_zero_clean(&sb);
  return rc;
}

/* Refreshes secret entries if the file that contains them is changed from the
 * latest cached version by 'store'. Error semantics: returns OK on success, ERR
 * on invalid input, I/O or parse failures.
 */
static AdbxStatus ss_refresh_if_changed(FileSecretStore *store) {
  if (!store)
    return ERR;

  FileMeta new_meta = {0};
  int file_fd = -1;
  AdbxTriStatus frc = ss_open_credfile_read(store, &file_fd, &new_meta);
  if (frc == ERR)
    return ERR;
  if (frc == NO) {
    // the file is missing, so SecretFileStore will have all the attribute to 0
    // to represent an empty state
    if (store->cache_loaded == 1 &&
        fileio_meta_equal(&store->cache_meta, &new_meta) == YES) {
      return OK;
    }

    SecretEntryList old = store->cache;
    memset(&store->cache, 0, sizeof(store->cache));
    store->cache_meta = new_meta;
    store->cache_loaded = 1;
    ss_entries_clean(&old);
    return OK;
  }

  if (store->cache_loaded == 1 &&
      fileio_meta_equal(&store->cache_meta, &new_meta) == YES) {
    close(file_fd);
    return OK;
  }

  SecretEntryList next = {0};
  if (ss_load_list_from_fd(store, file_fd, &next) != OK) {
    close(file_fd);
    return ERR;
  }

  SecretEntryList old = store->cache;
  store->cache = next;
  store->cache_meta = new_meta;
  store->cache_loaded = 1;
  ss_entries_clean(&old);
  close(file_fd);

  return OK;
}

/* Persists new list 'next' at SS_CRED_FILE inside 'store'->dir_fd
 * and swaps it into 'store' cache. It borrows all the input. Side effects:
 * writes credential file, stats metadata, and replaces in-memory cache with
 * zeroized cleanup of old cache. Error semantics: returns OK on success, ERR on
 * write/stat failures.
 */
static AdbxStatus ss_persist_and_swap(FileSecretStore *store,
                                      SecretEntryList *next) {
  if (!store || !next)
    return ERR;

  StrBuf sb;
  sb_init(&sb);
  if (ss_serialize_entries(next, &sb) != OK) {
    ss_set_err(store, SSERR_WRITE,
               "secret-store write failed: unable to serialize credentials. "
               "Please, retry.");
    sb_zero_clean(&sb);
    return ERR;
  }

  FileMeta meta_new = {0};
  int dir_fd = ss_get_dir_fd(store);
  if (dir_fd < 0) {
    sb_zero_clean(&sb);
    return ERR;
  }
  AdbxTriStatus rc = write_atomic(dir_fd, SS_CRED_FILE,
                                  (const uint8_t *)sb.data, sb.len, &meta_new);
  sb_zero_clean(&sb);
  if (rc == NO) {
    ss_set_err(store, SSERR_WRITE,
               "secret-store write blocked: another process is writing '%s'. "
               "Please, retry.",
               SS_CRED_FILE);
    return ERR;
  }
  if (rc != YES) {
    int saved_errno = errno;
    ss_set_err(store, SSERR_WRITE,
               "secret-store write failed: cannot atomically persist '%s': %s.",
               SS_CRED_FILE,
               (saved_errno != 0) ? strerror(saved_errno) : "unknown error");
    return ERR;
  }

  SecretEntryList old = store->cache;
  store->cache = *next;
  // clear memory
  next->entries = NULL;
  next->n_entries = 0;
  memset(&next->arena, 0, sizeof(next->arena));

  store->cache_meta = meta_new;
  store->cache_loaded = 1;

  ss_entries_clean(&old);
  return OK;
}

/* Reads one secret by typed reference using read-through cache semantics.
 * It borrows 'base' and writes into caller-owned 'out'.
 * Side effects: may refresh cache from disk and mutates output buffer.
 * Error semantics: returns YES when found, NO when missing, ERR on invalid
 * input, policy, I/O, parse, or allocation failures.
 */
static AdbxTriStatus secret_store_file_get(SecretStore *base,
                                           const SecretRefInfo *ref,
                                           StrBuf *out) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_clear_err(store);

  if (!out)
    return ERR;

  if (ss_validate_ref(store, ref, "get") != OK)
    return ERR;

  sb_zero_clean(out);
  sb_init(out);

  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  SecretEntry key = {.cred_namespace = ref->cred_namespace,
                     .connection_name = ref->connection_name,
                     .secret = NULL};
  SecretEntryList *list = &store->cache;
  if (list->n_entries == 0 || !list->entries)
    return NO;
  const SecretEntry *e = bsearch(&key, list->entries, list->n_entries,
                                 sizeof(*list->entries), ss_entry_cmp);
  if (!e)
    return NO;

  size_t n = strlen(e->secret);
  if (sb_append_bytes(out, e->secret, n + 1) != OK) {
    ss_set_err(store, SSERR_INPUT,
               "secret-store get failed: unable to allocate output buffer. "
               "Please, retry.");
    return ERR;
  }
  return YES;
}

/* Stores or replaces one secret and updates cache atomically.
 * It borrows all inputs.
 * Side effects: refreshes cache, writes credential file, and swaps in-memory
 * cache on success.
 * Error semantics: returns OK on success, ERR on invalid input, policy,
 * I/O, parse, or allocation failures.
 */
static AdbxStatus secret_store_file_set(SecretStore *base,
                                        const SecretRefInfo *ref,
                                        const char *secret) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_clear_err(store);

  if (!secret) {
    ss_set_err(store, SSERR_INPUT,
               "secret-store set failed: invalid input pointers. This is "
               "probably a bug, please, report it.");
    return ERR;
  }

  if (ss_validate_ref(store, ref, "set") != OK)
    return ERR;

  if (ss_refresh_if_changed(store) != OK) {
    return ERR;
  }

  SecretEntryList next = {0};
  if (ss_list_build_with_upsert(&store->cache, ref, secret, &next) != OK) {
    ss_set_err(
        store, SSERR_WRITE,
        "secret-store set failed: memory allocation error. Please, retry.");
    return ERR;
  }

  AdbxStatus prc = ss_persist_and_swap(store, &next);
  if (prc != OK)
    ss_entries_clean(&next);
  return prc;
}

/* Deletes one secret by ref and updates cache atomically.
 * It borrows all inputs.
 * Side effects: refreshes cache, writes credential file, and swaps in-memory
 * cache on success.
 * Error semantics: returns OK on success, ERR on invalid input, policy,
 * I/O, parse, or allocation failures.
 */
static AdbxStatus secret_store_file_delete(SecretStore *base,
                                           const SecretRefInfo *ref) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_clear_err(store);

  if (ss_validate_ref(store, ref, "delete") != OK)
    return ERR;

  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  size_t pos = 0;
  AdbxTriStatus fnd = ss_list_find_ref_pos(&store->cache, ref, &pos);
  if (fnd == ERR)
    return ERR;
  if (fnd == NO)
    return OK;

  SecretEntryList next = {0};
  AdbxStatus rc = ss_list_build_without_index(&store->cache, pos, &next);
  if (rc != OK) {
    ss_set_err(
        store, SSERR_WRITE,
        "secret-store delete failed: memory allocation error. Please, retry.");
    return ERR;
  }

  rc = ss_persist_and_swap(store, &next);
  if (rc != OK)
    ss_entries_clean(&next);
  return rc;
}

/* Lists all stored secret references using read-through cache semantics.
 * It borrows 'base' and writes one owned list into 'out'.
 * Side effects: may refresh cache from disk and allocates heap memory for the
 * returned list.
 * Error semantics: returns OK on success, ERR on invalid input, policy, I/O,
 * parse, or allocation failures.
 */
static AdbxStatus secret_store_file_list_refs(SecretStore *base,
                                              SecretRefList *out) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_clear_err(store);

  if (!out) {
    ss_set_err(store, SSERR_INPUT,
               "secret-store list failed: invalid output pointer. This is "
               "probably a bug, please, report it.");
    return ERR;
  }

  out->items = NULL;
  out->n_items = 0;

  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  if (ss_list_copy_refs(&store->cache, out) != OK) {
    ss_set_err(store, SSERR_WRITE,
               "secret-store list failed: memory allocation error. Please, "
               "retry.");
    return ERR;
  }

  return OK;
}

/* Deletes all secrets in one namespace and updates cache atomically.
 * It borrows all inputs.
 * Side effects: refreshes cache, writes credential file, and swaps in-memory
 * cache on success.
 * Error semantics: returns OK on success, ERR on invalid input, policy, I/O,
 * parse, or allocation failures.
 */
static AdbxStatus secret_store_file_wipe_namespace(SecretStore *base,
                                                   const char *cred_namespace) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_clear_err(store);

  if (!cred_namespace || cred_namespace[0] == '\0') {
    ss_set_err(store, SSERR_INPUT,
               "secret-store namespace wipe failed: invalid namespace. This "
               "is probably a bug, please, report it.");
    return ERR;
  }

  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  SecretEntryList *list = &store->cache;
  size_t keep_n = 0;
  for (size_t i = 0; i < list->n_entries; i++) {
    if (strcmp(list->entries[i].cred_namespace, cred_namespace) != 0)
      keep_n++;
  }
  if (keep_n == list->n_entries)
    return OK;

  SecretEntryList next = {0};
  if (ss_list_build_without_namespace(list, cred_namespace, &next) != OK) {
    ss_set_err(store, SSERR_WRITE,
               "secret-store namespace wipe failed: memory allocation error. "
               "Please, retry.");
    return ERR;
  }

  AdbxStatus rc = ss_persist_and_swap(store, &next);
  if (rc != OK)
    ss_entries_clean(&next);
  return rc;
}

/* Deletes all secrets from the file-backed store.
 * It borrows 'base'.
 * Side effects: refreshes cache, and when entries exist, writes the emptied
 * credential file and swaps in-memory cache on success. Missing/empty store is
 * treated as a successful no-op.
 * Error semantics: returns OK on success, ERR on invalid input, policy,
 * I/O, or allocation failures.
 */
static AdbxStatus secret_store_file_wipe_all(SecretStore *base) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_clear_err(store);

  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  if (store->cache.n_entries == 0)
    return OK;

  SecretEntryList next = {0};
  AdbxStatus rc = ss_persist_and_swap(store, &next);
  if (rc != OK)
    ss_entries_clean(&next);
  return rc;
}

/* Destroys one file-backed secret store.
 * It consumes 'base'.
 * Side effects: zeroes in-memory cached secrets and releases owned resources.
 * Error semantics: none.
 */
static void secret_store_file_destroy(SecretStore *base) {
  if (!base)
    return;

  FileSecretStore *store = (FileSecretStore *)base;
  ss_entries_clean(&store->cache);

  if (store->dir_fd >= 0)
    close(store->dir_fd);
  free(store);
}

/* Returns backend-specific error details for the file-backed secret store.
 * It borrows 'base' and returns an internal pointer.
 * Side effects: none.
 * Error semantics: returns empty string on invalid input or when no error is
 * available.
 */
static const char *secret_store_file_last_error(SecretStore *base) {
  if (!base)
    return "";
  FileSecretStore *store = (FileSecretStore *)base;
  return (store->last_err_msg[0] != '\0') ? store->last_err_msg : "";
}

/* Returns backend-specific error category for the file-backed secret store.
 * It borrows 'base' and performs no allocations.
 * Side effects: none.
 * Error semantics: returns SSERR_NONE on invalid input or when no error is
 * available.
 */
static SecretStoreErrCode secret_store_file_last_error_code(SecretStore *base) {
  if (!base)
    return SSERR_NONE;
  FileSecretStore *store = (FileSecretStore *)base;
  return store->last_err_code;
}

static const SecretStoreVTable SECRET_STORE_FILE_VT = {
    .get = secret_store_file_get,
    .set = secret_store_file_set,
    .delete = secret_store_file_delete,
    .list_refs = secret_store_file_list_refs,
    .wipe_namespace = secret_store_file_wipe_namespace,
    .wipe_all = secret_store_file_wipe_all,
    .destroy = secret_store_file_destroy,
    .last_error = secret_store_file_last_error,
    .last_error_code = secret_store_file_last_error_code,
};

static SecretStore *secret_store_file_backend_create(void) {
  FileSecretStore *store = (FileSecretStore *)xmalloc(sizeof(*store));
  store->base.vt = &SECRET_STORE_FILE_VT;
  store->dir_fd = -1;
  store->cache_loaded = 0;
  memset(&store->cache_meta, 0, sizeof(store->cache_meta));
  memset(&store->cache, 0, sizeof(store->cache));
  ss_clear_err(store);

  // Fail close: backend creation succeeds only when storage directory is ready.
  if (ss_get_dir_fd(store) < 0) {
    secret_store_file_destroy((SecretStore *)store);
    return NULL;
  }

  return (SecretStore *)store;
}

AdbxTriStatus secret_store_file_backend_probe(SecretStore **out_store) {
  if (!out_store)
    return ERR;
  *out_store = NULL;

  *out_store = secret_store_file_backend_create();
  return *out_store ? YES : ERR;
}
