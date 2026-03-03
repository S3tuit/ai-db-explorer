#include "secret_store.h"

#include "arena.h"
#include "file_io.h"
#include "json_codec.h"
#include "string_op.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

// Temporary struct used to load all the entries in memory and then sort them.
// Aftert sorted, they become SecretEntry and gets added to the arena
typedef struct {
  char *ref;
  char *secret;
} SecretEntryTmp;

// holds the metadata to understand whether a file is changed from our last
// snapshot
typedef struct {
  int exists;
  dev_t dev;
  ino_t ino;
  off_t size;
  time_t mtime_sec;
  long mtime_nsec;
} SsFileMeta;

typedef struct {
  SecretStore base;
  char *dir_path;
  char *file_path;

  SecretEntryList cache;
  SsFileMeta cache_meta;
  int cache_loaded;
} FileSecretStore;

/* Returns nanosecond component from one stat mtime field.
 */
static long ss_mtime_nsec(const struct stat *st) {
  if (!st)
    return 0;
#if defined(__APPLE__)
  return st->st_mtimespec.tv_nsec;
#else
  return st->st_mtim.tv_nsec;
#endif
}

/* Stores file identity metadata from one stat struct.
 * It borrows 'st' and writes to caller-owned 'out'.
 * Side effects: mutates out metadata fields.
 * Error semantics: returns OK on success, ERR on invalid input.
 */
static AdbxStatus ss_meta_from_stat(const struct stat *st, SsFileMeta *out) {
  if (!st || !out)
    return ERR;
  out->exists = 1;
  out->dev = st->st_dev;
  out->ino = st->st_ino;
  out->size = st->st_size;
  out->mtime_sec = st->st_mtime;
  out->mtime_nsec = ss_mtime_nsec(st);
  return OK;
}

/* Compares 2 file metadata snapshots.
 * It borrows both inputs and performs no allocations.
 * Side effects: none.
 * Error semantics: returns YES when equal, NO when different, ERR on invalid
 * input.
 */
static AdbxTriStatus ss_meta_equal(const SsFileMeta *a, const SsFileMeta *b) {
  if (!a || !b)
    return ERR;

  if (a->exists != b->exists)
    return NO;
  if (!a->exists)
    return YES;

  if (a->dev != b->dev)
    return NO;
  if (a->ino != b->ino)
    return NO;
  if (a->size != b->size)
    return NO;
  if (a->mtime_sec != b->mtime_sec)
    return NO;
  if (a->mtime_nsec != b->mtime_nsec)
    return NO;

  return YES;
}

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

/* Cleans temporary parsed entries allocated on heap.
 * It consumes temporary arrays and strings.
 * Side effects: overwrites and frees secret strings.
 * Error semantics: none.
 */
static void ss_tmp_clean(SecretEntryTmp *tmp, size_t n) {
  if (!tmp)
    return;
  for (size_t i = 0; i < n; i++) {
    free(tmp[i].ref);
    tmp[i].ref = NULL;
    ss_secret_free(&tmp[i].secret);
  }
  free(tmp);
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
  if (!out)
    return ERR;

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
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_set_entry_copy(SecretEntryList *list, size_t idx,
                                         const char *ref, const char *secret) {
  if (!list || !ref || !secret || idx >= list->n_entries)
    return ERR;

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
 * point), ERR on invalid input.
 */
static AdbxTriStatus ss_list_find_ref_pos(const SecretEntryList *list,
                                          const char *ref, size_t *out_pos) {
  if (!list || !ref || !out_pos)
    return ERR;

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

/* Builds next list by upserting one ref/secret pair into a sorted source list.
 * It borrows all inputs and writes owned result in out_next.
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

/* Builds next list by removing one entry index from sorted source list.
 * It borrows src and writes owned result in out_next.
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

/* Appends one owned tmp entry into a dynamic temporary vector.
 * It consumes 'ref' and 'secret' on success.
 * Side effects: may grow heap storage for tmp vector.
 * Error semantics: returns OK on success, ERR on invalid input or overflow.
 */
static AdbxStatus ss_tmp_push_owned(SecretEntryTmp **tmp, size_t *n,
                                    size_t *cap, char *ref, char *secret) {
  if (!tmp || !n || !cap || !ref || !secret)
    return ERR;

  if (*n == *cap) {
    size_t new_cap = (*cap == 0) ? 8u : (*cap * 2u);
    if (new_cap < *cap)
      return ERR;
    *tmp = (SecretEntryTmp *)xrealloc(*tmp, new_cap * sizeof(**tmp));
    *cap = new_cap;
  }

  (*tmp)[*n].ref = ref;
  (*tmp)[*n].secret = secret;
  (*n)++;
  return OK;
}

/* Creates an arena-backed sorted entry list from a sorted temporary array.
 * It borrows 'tmp' and writes owned storage in 'out'.
 * Side effects: initializes and allocates one arena.
 * Error semantics: returns OK on success, ERR on invalid input or allocation
 * failure.
 */
static AdbxStatus ss_list_init_from_tmp_sorted(const SecretEntryTmp *tmp,
                                               size_t n, SecretEntryList *out) {
  if (!tmp && n != 0)
    return ERR;

  if (ss_list_init_with_n_entries(out, n) != OK)
    return ERR;
  for (size_t i = 0; i < n; i++) {
    if (ss_list_set_entry_copy(out, i, tmp[i].ref, tmp[i].secret) != OK) {
      ss_entries_clean(out);
      return ERR;
    }
  }
  return OK;
}

/* Returns YES when path is absolute.
 * It borrows input and performs no allocations.
 * Side effects: none.
 * Error semantics: returns NO for NULL/non-absolute input.
 */
static AdbxTriStatus ss_is_abs_path(const char *path) {
  if (!path || path[0] != '/')
    return NO;
  return YES;
}

/* Duplicates one [start,end) range into a heap NUL-terminated string.
 * It borrows input pointers and returns caller-owned memory.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *ss_dup_range(const char *start, const char *end) {
  if (!start || !end || end < start)
    return NULL;
  size_t len = (size_t)(end - start);
  char *out = xmalloc(len + 1);
  memcpy(out, start, len);
  out[len] = '\0';
  return out;
}

/* Joins path fragments as "a/b".
 * It borrows both inputs and returns caller-owned memory.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on invalid input.
 */
static char *ss_join_path2(const char *a, const char *b) {
  if (!a || !b)
    return NULL;
  size_t alen = strlen(a);
  size_t blen = strlen(b);
  int has_slash = (alen > 0 && a[alen - 1] == '/');
  size_t out_len = alen + (has_slash ? 0u : 1u) + blen;
  char *out = xmalloc(out_len + 1);
  if (has_slash) {
    snprintf(out, out_len + 1, "%s%s", a, b);
  } else {
    snprintf(out, out_len + 1, "%s/%s", a, b);
  }
  return out;
}

/* Computes parent directory path of one file path.
 * It borrows 'path' and returns caller-owned memory in out_dir.
 * Side effects: allocates memory.
 * Error semantics: returns OK on success, ERR on invalid input.
 */
static AdbxStatus ss_parent_dir(const char *path, char **out_dir) {
  if (!path || !out_dir)
    return ERR;
  *out_dir = NULL;

  size_t len = strlen(path);
  if (len == 0 || path[len - 1] == '/')
    return ERR;

  const char *last = strrchr(path, '/');
  if (!last) {
    *out_dir = dup_or_null(".");
    return *out_dir ? OK : ERR;
  }
  if (last == path) {
    *out_dir = dup_or_null("/");
    return *out_dir ? OK : ERR;
  }

  *out_dir = ss_dup_range(path, last);
  return *out_dir ? OK : ERR;
}

/* Ensures one directory exists with expected type.
 * It borrows 'path'.
 * Side effects: may create directory and chmod it.
 * Error semantics: returns OK on success, ERR on invalid input or syscall
 * failure.
 */
static AdbxStatus ss_ensure_one_dir(const char *path, mode_t create_mode) {
  if (!path || path[0] == '\0')
    return ERR;

  if (mkdir(path, create_mode) == 0) {
    if (chmod(path, create_mode) != 0)
      return ERR;
    return OK;
  }

  if (errno != EEXIST)
    return ERR;

  struct stat st = {0};
  if (lstat(path, &st) != 0)
    return ERR;
  return S_ISDIR(st.st_mode) ? OK : ERR;
}

/* Ensures all path components in dir_path exist.
 * It borrows input and uses one temporary mutable path copy.
 * Side effects: may create directory tree.
 * Error semantics: returns OK on success, ERR on invalid input or syscall
 * failure.
 */
static AdbxStatus ss_ensure_dir_tree(const char *dir_path) {
  if (!dir_path || dir_path[0] == '\0')
    return ERR;

  char *tmp = dup_or_null(dir_path);
  if (!tmp)
    return ERR;

  size_t len = strlen(tmp);
  for (size_t i = 1; i < len; i++) {
    if (tmp[i] != '/')
      continue;
    tmp[i] = '\0';
    if (tmp[0] != '\0' && ss_ensure_one_dir(tmp, 0700) != OK) {
      free(tmp);
      return ERR;
    }
    tmp[i] = '/';
  }

  AdbxStatus rc = ss_ensure_one_dir(tmp, 0700);
  free(tmp);
  return rc;
}

/* Validates one user-owned 0700 directory.
 * It borrows 'path' and performs no allocations.
 * Side effects: reads filesystem metadata.
 * Error semantics: returns YES when valid, NO when missing, ERR on mismatch or
 * syscall failure.
 */
static AdbxTriStatus ss_validate_user_dir_0700(const char *path) {
  if (!path)
    return ERR;

  struct stat st = {0};
  if (lstat(path, &st) != 0) {
    if (errno == ENOENT)
      return NO;
    return ERR;
  }

  if (!S_ISDIR(st.st_mode))
    return ERR;
  if (st.st_uid != getuid())
    return ERR;
  // TODO: maybe instead of failing we could try to set the right cred for the
  // dir
  if ((st.st_mode & 0777) != 0700)
    return ERR;

  return YES;
}

/* Resolves the app directory used for file-backed credentials.
 * It borrows environment variables and returns caller-owned absolute path.
 * Side effects: allocates memory.
 * Error semantics: returns NULL on missing/invalid environment inputs.
 */
static char *ss_resolve_app_dir(void) {
  const char *xdg = getenv("XDG_CONFIG_HOME");
  if (ss_is_abs_path(xdg) == YES && xdg[0] != '\0')
    return ss_join_path2(xdg, SS_APPNAME);

  const char *home = getenv("HOME");
  if (ss_is_abs_path(home) != YES || home[0] == '\0')
    return NULL;

#ifdef __APPLE__
  char *base = ss_join_path2(home, "Library/Application Support");
#else
  char *base = ss_join_path2(home, ".config");
#endif
  if (!base)
    return NULL;

  char *dir = ss_join_path2(base, SS_APPNAME);
  free(base);
  return dir;
}

/* Resolves and validates store paths and directory policy.
 * It returns owned strings in out_dir/out_file.
 * Side effects: may create parent directories.
 * Error semantics: returns OK on success, ERR on invalid environment or policy
 * mismatch.
 */
static AdbxStatus ss_prepare_paths(char **out_dir, char **out_file) {
  if (!out_dir || !out_file)
    return ERR;
  *out_dir = NULL;
  *out_file = NULL;

  char *dir = ss_resolve_app_dir();
  if (!dir)
    return ERR;

  char *parent = NULL;
  if (ss_parent_dir(dir, &parent) != OK) {
    free(dir);
    return ERR;
  }

  if (ss_ensure_dir_tree(parent) != OK) {
    free(parent);
    free(dir);
    return ERR;
  }
  free(parent);

  AdbxTriStatus vrc = ss_validate_user_dir_0700(dir);
  if (vrc == NO) {
    if (ss_ensure_one_dir(dir, 0700) != OK) {
      free(dir);
      return ERR;
    }
    vrc = ss_validate_user_dir_0700(dir);
  }
  if (vrc != YES) {
    free(dir);
    return ERR;
  }

  char *file = ss_join_path2(dir, SS_CRED_FILE);
  if (!file) {
    free(dir);
    return ERR;
  }

  *out_dir = dir;
  *out_file = file;
  return OK;
}

/* Stats one file path and validates strict file policy.
 * It borrows 'path' and writes metadata into out_meta.
 * Side effects: reads filesystem metadata.
 * Error semantics: returns YES when file exists and is valid, NO when missing,
 * ERR on mode/ownership/type mismatch or syscall failure.
 */
static AdbxTriStatus ss_stat_file_strict(const char *path,
                                         SsFileMeta *out_meta) {
  if (!path || !out_meta)
    return ERR;

  memset(out_meta, 0, sizeof(*out_meta));

  struct stat st = {0};
  if (lstat(path, &st) != 0) {
    if (errno == ENOENT)
      return NO;
    return ERR;
  }

  if (!S_ISREG(st.st_mode))
    return ERR;
  if (st.st_uid != getuid())
    return ERR;
  // TODO: instead of failing close we may try to se the correct credentials
  // once
  if ((st.st_mode & 0777) != 0600)
    return ERR;

  return ss_meta_from_stat(&st, out_meta) == OK ? YES : ERR;
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

/* Writes exactly len bytes to an open fd.
 * It borrows all inputs and performs no allocations.
 * Side effects: performs write syscalls.
 * Error semantics: returns OK on success, ERR on invalid input or write
 * failure.
 */
static AdbxStatus ss_write_all_fd(int fd, const uint8_t *src, size_t len) {
  if (fd < 0 || (!src && len != 0))
    return ERR;

  size_t off = 0;
  while (off < len) {
    ssize_t n = write(fd, src + off, len - off);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return ERR;
    }
    if (n == 0)
      return ERR;
    off += (size_t)n;
  }

  return OK;
}

/* Persists bytes through temp file + rename with durability fsyncs.
 * It borrows all inputs and does not retain pointers.
 * Side effects: creates temp file, writes bytes, fsyncs file and directory,
 * then renames into destination path.
 * Error semantics: returns OK on success, ERR on any filesystem failure.
 */
static AdbxStatus ss_write_atomic(const char *dir_path, const char *file_path,
                                  const uint8_t *data, size_t len) {
  if (!dir_path || !file_path || (!data && len != 0))
    return ERR;

  char *tpl = ss_join_path2(dir_path, SS_CRED_FILE ".tmp.XXXXXX");
  if (!tpl)
    return ERR;

  int tfd = mkstemp(tpl);
  if (tfd < 0) {
    free(tpl);
    return ERR;
  }

  if (fchmod(tfd, 0600) != 0) {
    (void)close(tfd);
    (void)unlink(tpl);
    free(tpl);
    return ERR;
  }

  if (ss_write_all_fd(tfd, data, len) != OK) {
    (void)close(tfd);
    (void)unlink(tpl);
    free(tpl);
    return ERR;
  }

  if (fsync(tfd) != 0) {
    (void)close(tfd);
    (void)unlink(tpl);
    free(tpl);
    return ERR;
  }

  if (close(tfd) != 0) {
    (void)unlink(tpl);
    free(tpl);
    return ERR;
  }

  if (rename(tpl, file_path) != 0) {
    (void)unlink(tpl);
    free(tpl);
    return ERR;
  }
  free(tpl);

  int dfd = open(dir_path, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
    return ERR;
  if (fsync(dfd) != 0) {
    (void)close(dfd);
    return ERR;
  }
  if (close(dfd) != 0)
    return ERR;

  return OK;
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

  SecretEntryTmp *tmp = NULL;
  size_t tmp_n = 0;
  size_t tmp_cap = 0;

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

    if (ref[0] == '\0') {
      free(ref);
      ss_secret_free(&secret);
      goto parse_error;
    }

    if (ss_tmp_push_owned(&tmp, &tmp_n, &tmp_cap, ref, secret) != OK) {
      free(ref);
      ss_secret_free(&secret);
      goto parse_error;
    }
  }

  if (tmp_n > 1)
    qsort(tmp, tmp_n, sizeof(*tmp), ss_entry_cmp);

  // makes sure there are no duplicates secret references, else fail close
  for (size_t i = 1; i < tmp_n; i++) {
    if (strcmp(tmp[i - 1].ref, tmp[i].ref) == 0)
      goto parse_error;
  }

  if (ss_list_init_from_tmp_sorted(tmp, tmp_n, out_list) != OK)
    goto parse_error;

  ss_tmp_clean(tmp, tmp_n);
  return OK;

parse_error:
  ss_tmp_clean(tmp, tmp_n);
  ss_entries_clean(out_list);
  return ERR;
}

/* Loads and parses one credentials file into list.
 * It borrows file_path and writes an owned list to out_list.
 * Side effects: performs file I/O and parsing allocations.
 * Error semantics: returns OK on success, ERR on I/O/parsing failures.
 */
static AdbxStatus ss_load_list_from_file(const char *file_path,
                                         SecretEntryList *out_list) {
  if (!file_path || !out_list)
    return ERR;

  StrBuf sb;
  sb_init(&sb);

  if (fileio_sb_read_limit(file_path, SS_FILE_MAX_BYTES, &sb) != OK) {
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

/* Writes one entry list to credentials file with strict policy checks.
 * It borrows inputs and does not retain pointers.
 * Side effects: validates directory policy and writes file atomically.
 * Error semantics: returns OK on success, ERR on policy or I/O failures.
 */
static AdbxStatus ss_store_list_to_file(FileSecretStore *store,
                                        const SecretEntryList *list) {
  if (!store || !list)
    return ERR;

  if (ss_validate_user_dir_0700(store->dir_path) != YES)
    return ERR;

  StrBuf sb;
  sb_init(&sb);
  if (ss_serialize_entries(list, &sb) != OK) {
    sb_zero_clean(&sb);
    return ERR;
  }

  AdbxStatus rc = ss_write_atomic(store->dir_path, store->file_path,
                                  (const uint8_t *)sb.data, sb.len);
  sb_zero_clean(&sb);
  return rc;
}

/* Refreshes in-memory cache when file metadata changed.
 * It borrows 'store' and updates cache state in place.
 * Side effects: validates filesystem policy, may read+parse credential file,
 * and zero/replace old cached secrets.
 * Error semantics: returns OK on success, ERR on policy, I/O, or parse
 * failures.
 */
static AdbxStatus ss_refresh_if_changed(FileSecretStore *store) {
  if (!store)
    return ERR;

  if (ss_validate_user_dir_0700(store->dir_path) != YES)
    return ERR;

  SsFileMeta meta_now = {0};
  AdbxTriStatus frc = ss_stat_file_strict(store->file_path, &meta_now);
  if (frc == ERR)
    return ERR;

  if (store->cache_loaded == 1 &&
      ss_meta_equal(&store->cache_meta, &meta_now) == YES)
    return OK;

  SecretEntryList next = {0};
  if (frc == YES) {
    if (ss_load_list_from_file(store->file_path, &next) != OK)
      return ERR;
  }

  SecretEntryList old = store->cache;
  store->cache = next;
  store->cache_meta = meta_now;
  store->cache_loaded = 1;
  ss_entries_clean(&old);
  return OK;
}

/* Persists new list and swaps it into store cache.
 * It borrows 'store' and consumes 'next' on success.
 * Side effects: writes credential file, stats metadata, and replaces in-memory
 * cache with zeroized cleanup of old cache.
 * Error semantics: returns OK on success, ERR on write/stat failures.
 */
static AdbxStatus ss_persist_and_swap(FileSecretStore *store,
                                      SecretEntryList *next) {
  if (!store || !next)
    return ERR;

  if (ss_store_list_to_file(store, next) != OK)
    return ERR;

  SsFileMeta meta_new = {0};
  if (ss_stat_file_strict(store->file_path, &meta_new) != YES)
    return ERR;

  SecretEntryList old = store->cache;
  store->cache = *next;
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
  if (ss_refresh_if_changed(store) != OK)
    return ERR;

  SecretEntryList next = {0};
  AdbxStatus rc =
      ss_list_build_with_upsert(&store->cache, secret_ref, secret, &next);
  if (rc != OK)
    return ERR;

  rc = ss_persist_and_swap(store, &next);
  if (rc != OK)
    ss_entries_clean(&next);
  return rc;
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
  free(store->file_path);
  free(store->dir_path);
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
  char *dir_path = NULL;
  char *file_path = NULL;
  if (ss_prepare_paths(&dir_path, &file_path) != OK) {
    free(dir_path);
    free(file_path);
    return NULL;
  }

  FileSecretStore *store = (FileSecretStore *)xcalloc(1, sizeof(*store));
  store->base.vt = &SECRET_STORE_FILE_VT;
  store->dir_path = dir_path;
  store->file_path = file_path;
  store->cache_loaded = 0;
  memset(&store->cache_meta, 0, sizeof(store->cache_meta));
  memset(&store->cache, 0, sizeof(store->cache));
  return (SecretStore *)store;
}
