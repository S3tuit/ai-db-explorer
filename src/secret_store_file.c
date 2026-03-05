#include "secret_store.h"

#include "arena.h"
#include "file_io.h"
#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define SS_APPNAME "adbxplorer"
#define SS_CRED_FILE "credentials.json"
#define SS_FILE_VERSION "1"
#define SS_FILE_MAX_BYTES (16u * 1024u * 1024u)

typedef struct {
  const char *ref;    // the connectionName
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
} FileSecretStore;

/* Zeroes and frees one heap-allocated secret string.
 * It consumes '*s'.
 * Side effects: overwrites secret bytes before free.
 * Error semantics: none.
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
 * Side effects: overwrites arena payload and releases arena blocks.
 * Error semantics: none.
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

/* Comparator for final entries by ref.
 * It borrows both inputs and performs no allocations.
 * Side effects: none.
 * Error semantics: returns qsort/bsearch-compatible ordering.
 */
static int ss_entry_cmp(const void *a, const void *b) {
  const SecretEntry *ea = (const SecretEntry *)a;
  const SecretEntry *eb = (const SecretEntry *)b;
  return strcmp(ea->ref, eb->ref);
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

/* Copies one ref/secret pair into one list entry.
 * It borrows all inputs and writes arena-owned strings in list[idx].
 * Side effects: allocates strings in list arena.
 * Error semantics: returns OK on success, ERR on allocation failure.
 */
static inline AdbxStatus ss_list_set_entry_copy(SecretEntryList *list,
                                                size_t idx, const char *ref,
                                                const char *secret) {
  assert(list);
  assert(ref);
  assert(secret);
  assert(idx < list->n_entries);

  list->entries[idx].ref = ss_arena_dup_cstr(&list->arena, ref);
  list->entries[idx].secret = ss_arena_dup_cstr(&list->arena, secret);
  if (!list->entries[idx].ref || !list->entries[idx].secret)
    return ERR;

  return OK;
}

/* Finds one ref position in sorted list.
 * It borrows list/ref and writes one position to out_pos.
 * Side effects: none.
 * Error semantics: returns YES when found, NO when missing (out_pos is insert
 * point).
 */
static AdbxTriStatus ss_list_find_ref_pos(const SecretEntryList *list,
                                          const char *ref, size_t *out_pos) {
  assert(list);
  assert(ref);
  assert(out_pos);

  size_t lo = 0;
  size_t hi = list->n_entries;
  while (lo < hi) {
    size_t mid = lo + ((hi - lo) / 2u);
    int cmp = strcmp(list->entries[mid].ref, ref);
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

/* Builds 'out_next' list by upserting one ref/secret pair into a sorted source
 * list. It borrows all inputs and creates a new object that can outlive 'src'.
 * Side effects: allocates output arena memory.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_build_with_upsert(const SecretEntryList *src,
                                            const char *ref, const char *secret,
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

    if (src_i >= src->n_entries ||
        ss_list_set_entry_copy(out_next, dst_i, src->entries[src_i].ref,
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

    if (ss_list_set_entry_copy(out_next, dst_i, src->entries[src_i].ref,
                               src->entries[src_i].secret) != OK) {
      ss_entries_clean(out_next);
      return ERR;
    }
    dst_i++;
  }

  return OK;
}

/* Validates that the opened directory 'dir_fd' is user-owned 0700.
 * Side effects: reads filesystem metadata and may chmod once to repair mode.
 * Error semantics: returns OK when valid, else ERR.
 */
static AdbxStatus ss_validate_config_dir(const int dir_fd) {
  assert(dir_fd >= 0);
  if (dir_fd < 0)
    return ERR;

  struct stat st;
  if (fstat(dir_fd, &st) != 0)
    return ERR;

  if (st.st_uid != getuid())
    return ERR;

  // if the permissions are wrong, we try to adjust them once before failing
  for (int retries = 0; retries <= 1; retries++) {
    if ((st.st_mode & 0777) != 0700) {
      if (retries == 0 && fchmod(dir_fd, 0700) == 0) {
        // reload stat
        if (fstat(dir_fd, &st) != 0) {
          return ERR;
        }
        continue;
      }
      return ERR;
    }
    return OK;
  }

  return ERR;
}

/* Opens the app directory used for file-backed credentials and stores its fd
 * inside 'out_fd'. Side effects: opens a fd that caller must close and may
 * create SS_APPNAME dir. Error semantics: returns ERR on invalid environment or
 * input.
 */
static AdbxStatus ss_open_config_dir(int *out_fd) {
  if (!out_fd)
    return ERR;
  *out_fd = -1;

  // We use XDG_CONFIG_HOME directly when present; HOME is only a fallback.
  char *base = NULL;
  const char *xdg = getenv("XDG_CONFIG_HOME");
  if (xdg && xdg[0] == '/') {
    base = dup_or_null(xdg);
  } else {
    const char *home = getenv("HOME");
    if (!home || home[0] != '/')
      return ERR;
#ifdef __APPLE__
    base = path_join(home, "Library/Application Support");
#else
    base = path_join(home, ".config");
#endif
  }
  if (!base)
    return ERR;

  int flags = O_RDONLY | O_DIRECTORY;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  int base_fd = open(base, flags);
  free(base);
  if (base_fd < 0)
    return ERR;

  // create the app directory if it doesn't exist
  if (mkdirat(base_fd, SS_APPNAME, 0700) != 0 && errno != EEXIST) {
    close(base_fd);
    return ERR;
  }

  int app_flags = O_RDONLY | O_DIRECTORY;
#ifdef O_NOFOLLOW
  app_flags |= O_NOFOLLOW;
#endif
  int app_fd = openat(base_fd, SS_APPNAME, app_flags);
  close(base_fd);
  if (app_fd < 0)
    return ERR;

  *out_fd = app_fd;
  return OK;
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

  if (ss_open_config_dir(&dir_fd) != OK || dir_fd < 0) {
    store->dir_fd = -1;
    return -1;
  }

  if (ss_validate_config_dir(dir_fd) != OK) {
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
static AdbxStatus ss_validate_credfile_fd(int file_fd, FileMeta *out_meta) {
  if (file_fd < 0 || !out_meta)
    return ERR;

  struct stat st = {0};
  if (fstat(file_fd, &st) != 0)
    return ERR;

  if (!S_ISREG(st.st_mode))
    return ERR;
  if (st.st_uid != getuid())
    return ERR;
  if ((st.st_mode & 0777) != 0600)
    return ERR;

  return fileio_meta_from_stat(&st, out_meta);
}

/* Opens credentials file in read mode and validates ownership/permissions.
 * It borrows 'store' and writes outputs to caller-owned 'out_fd/out_meta'.
 * Side effects: opens one fd and reads metadata; does not create files.
 * Error semantics: returns YES on success, NO when file is missing, ERR on
 * invalid input or filesystem/policy failures.
 */
static AdbxTriStatus ss_open_credfile_read(FileSecretStore *store, int *out_fd,
                                           FileMeta *out_meta) {
  if (!store || !out_fd || !out_meta)
    return ERR;
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
    return ERR;
  }

  if (ss_validate_credfile_fd(file_fd, out_meta) != OK) {
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
    if (json_kv_str(out_json, "ref", entries->entries[i].ref) != OK)
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
 * duplicate refs, or allocation failures.
 */
static AdbxStatus ss_parse_entries_json(const char *json, size_t json_len,
                                        SecretEntryList *out_list) {
  if (!json || !out_list)
    return ERR;

  out_list->entries = NULL;
  out_list->n_entries = 0;
  memset(&out_list->arena, 0, sizeof(out_list->arena));

  JsonGetter jg = {0};
  JsonTokBuf tok_buf = {0};
  if (jsget_init(&jg, json, json_len, &tok_buf) != OK)
    return ERR;

  const char *const top_keys[] = {"version", "entries"};
  JsonStrSpan top_unknown = {0};
  if (jsget_top_level_validation(&jg, NULL, top_keys, ARRLEN(top_keys),
                                 &top_unknown) != YES) {
    return ERR;
  }

  char *version = NULL;
  if (jsget_string_decode_alloc(&jg, "version", &version) != YES)
    return ERR;
  int ver_ok = (strcmp(version, SS_FILE_VERSION) == 0);
  free(version);
  if (!ver_ok)
    return ERR;

  JsonArrIter it = {0};
  if (jsget_array_objects_begin(&jg, "entries", &it) != YES)
    return ERR;

  // first, we allocate all the entries, unordered, at the beginning of the
  // arena
  if (it.count < 0)
    return ERR;
  size_t n_entries = (size_t)it.count;
  if (ss_list_init_with_n_entries(out_list, n_entries) != OK)
    return ERR;

  size_t fill_i = 0;
  for (;;) {
    JsonGetter entry = {0};
    AdbxTriStatus nrc = jsget_array_objects_next(&jg, &it, &entry);
    if (nrc == NO)
      break;
    if (nrc != YES)
      goto parse_error;

    const char *const entry_keys[] = {"ref", "secret"};
    JsonStrSpan entry_unknown = {0};
    if (jsget_top_level_validation(&entry, NULL, entry_keys, ARRLEN(entry_keys),
                                   &entry_unknown) != YES) {
      goto parse_error;
    }

    char *ref = NULL;
    char *secret = NULL;
    if (jsget_string_decode_alloc(&entry, "ref", &ref) != YES)
      goto parse_error;
    if (jsget_string_decode_alloc(&entry, "secret", &secret) != YES) {
      free(ref);
      goto parse_error;
    }

    if (fill_i >= out_list->n_entries || ref[0] == '\0') {
      free(ref);
      ss_secret_free(&secret);
      goto parse_error;
    }

    // TODO: add a json_codec decode-into-arena helper to avoid this extra
    // heap-allocation+copy path for decoded strings.

    // transfer ownership of the strings to the arena and wire the new pointers
    // to the corresponding entry
    if (ss_list_set_entry_copy(out_list, fill_i, ref, secret) != OK) {
      free(ref);
      ss_secret_free(&secret);
      goto parse_error;
    }
    fill_i++;
    free(ref);
    ss_secret_free(&secret);
  }

  // defensive check, we should've filled exactly n_entries
  if (fill_i != out_list->n_entries)
    goto parse_error;

  if (out_list->n_entries > 1)
    qsort(out_list->entries, out_list->n_entries, sizeof(*out_list->entries),
          ss_entry_cmp);

  // makes sure there are no duplicates secret references, else fail close
  for (size_t i = 1; i < out_list->n_entries; i++) {
    if (strcmp(out_list->entries[i - 1].ref, out_list->entries[i].ref) == 0)
      goto parse_error;
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
static AdbxStatus ss_load_list_from_fd(int file_fd, SecretEntryList *out_list) {
  if (file_fd < 0 || !out_list)
    return ERR;

  StrBuf sb;
  sb_init(&sb);

  if (fileio_sb_read_limit_fd(file_fd, SS_FILE_MAX_BYTES, &sb) != OK) {
    sb_clean(&sb);
    return ERR;
  }

  if (sb.len == 0) {
    sb_clean(&sb);
    return ERR;
  }

  AdbxStatus rc = ss_parse_entries_json(sb.data, sb.len, out_list);
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
  if (ss_load_list_from_fd(file_fd, &next) != OK) {
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
    sb_zero_clean(&sb);
    return ERR;
  }

  FileMeta meta_new = {0};
  int dir_fd = ss_get_dir_fd(store);
  AdbxTriStatus rc = write_atomic(dir_fd, SS_CRED_FILE,
                                  (const uint8_t *)sb.data, sb.len, &meta_new);
  sb_zero_clean(&sb);
  // TODO: we treat lock blocked as ERR, maybe there's a better design
  if (rc != YES)
    return ERR;

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

/* Reads one secret by ref using read-through cache semantics.
 * It borrows 'base' and writes into caller-owned 'out'.
 * Side effects: may refresh cache from disk and mutates output buffer.
 * Error semantics: returns YES when found, NO when missing, ERR on invalid
 * input, policy, I/O, parse, or allocation failures.
 */
static AdbxTriStatus
secret_store_file_get(SecretStore *base, const char *secret_ref, StrBuf *out) {
  if (!base || !secret_ref || !out)
    return ERR;

  sb_zero_clean(out);
  sb_init(out);

  FileSecretStore *store = (FileSecretStore *)base;
  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  SecretEntry key = {0};
  key.ref = secret_ref;
  SecretEntryList *list = &store->cache;
  if (list->n_entries == 0 || !list->entries)
    return NO;
  const SecretEntry *e = bsearch(&key, list->entries, list->n_entries,
                                 sizeof(*list->entries), ss_entry_cmp);
  if (!e)
    return NO;

  size_t n = strlen(e->secret);
  if (sb_append_bytes(out, e->secret, n + 1) != OK)
    return ERR;
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
                                        const char *secret_ref,
                                        const char *secret) {
  if (!base || !secret_ref || secret_ref[0] == '\0' || !secret)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  if (ss_refresh_if_changed(store) != OK) {
    return ERR;
  }

  SecretEntryList next = {0};
  if (ss_list_build_with_upsert(&store->cache, secret_ref, secret, &next) != OK)
    return ERR;

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
                                           const char *secret_ref) {
  if (!base || !secret_ref || secret_ref[0] == '\0')
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  size_t pos = 0;
  AdbxTriStatus fnd = ss_list_find_ref_pos(&store->cache, secret_ref, &pos);
  if (fnd == ERR)
    return ERR;
  if (fnd == NO)
    return OK;

  SecretEntryList next = {0};
  AdbxStatus rc = ss_list_build_without_index(&store->cache, pos, &next);
  if (rc != OK)
    return ERR;

  rc = ss_persist_and_swap(store, &next);
  if (rc != OK)
    ss_entries_clean(&next);
  return rc;
}

/* Deletes all secrets and updates cache atomically.
 * It borrows 'base'.
 * Side effects: refreshes cache, writes credential file, and swaps in-memory
 * cache on success.
 * Error semantics: returns OK on success, ERR on invalid input, policy,
 * I/O, or allocation failures.
 */
static AdbxStatus secret_store_file_wipe_all(SecretStore *base) {
  if (!base)
    return ERR;

  FileSecretStore *store = (FileSecretStore *)base;
  if (ss_refresh_if_changed(store) != OK)
    return ERR;

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

static const SecretStoreVTable SECRET_STORE_FILE_VT = {
    .get = secret_store_file_get,
    .set = secret_store_file_set,
    .delete = secret_store_file_delete,
    .wipe_all = secret_store_file_wipe_all,
    .destroy = secret_store_file_destroy,
};

SecretStore *secret_store_file_backend_create(void) {
  FileSecretStore *store = (FileSecretStore *)xmalloc(sizeof(*store));
  store->base.vt = &SECRET_STORE_FILE_VT;
  store->dir_fd = -1;
  store->cache_loaded = 0;
  memset(&store->cache_meta, 0, sizeof(store->cache_meta));
  memset(&store->cache, 0, sizeof(store->cache));

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
