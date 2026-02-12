#define _GNU_SOURCE

#include "proc_identity.h"

#include "file_io.h"
#include "utils.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define PROCID_STAT_MAX 2048u
#define PROCID_COMM_MAX                                                        \
  32u // allocate more than 16 bytes for defensive programming

typedef struct {
  pid_t ppid;
  uint64_t start_time_ticks;
  char comm[PROCID_COMM_MAX];
} StatFields;

/* Parses '/proc/<pid>/stat' content to extract parent pid and start time.
 * Ownership: borrows and mutates 'line' during tokenization.
 * Side effects: none.
 * Error semantics: returns OK on successful parse, ERR on malformed input.
 */
static int procid_parse_stat_line(char *line, StatFields *stats) {
  if (!line || !stats)
    return ERR;

  char *sparen = strchr(line, '(');
  char *rparen = strrchr(line, ')');
  if (!sparen || !rparen || rparen[1] != ' ')
    return ERR;

  // Tokenize from "state ppid ... starttime ...". We avoid sscanf-heavy
  // parsing because comm can contain spaces, so fields before ')' are special.
  char *save = NULL;
  char *tok = strtok_r(rparen + 2, " ", &save);
  int field_no = 3;
  long ppid_val = -1;
  unsigned long long start_val = 0;

  while (tok) {
    if (field_no == 4) {
      char *end = NULL;
      errno = 0;
      long v = strtol(tok, &end, 10);
      if (errno != 0 || end == tok || *end != '\0' || v <= 1)
        return ERR;
      ppid_val = v;
    } else if (field_no == 22) {
      char *end = NULL;
      errno = 0;
      unsigned long long v = strtoull(tok, &end, 10);
      if (errno != 0 || end == tok || *end != '\0' || v == 0)
        return ERR;
      start_val = v;
      break;
    }
    field_no++;
    tok = strtok_r(NULL, " ", &save);
  }

  if (ppid_val <= 1 || start_val == 0)
    return ERR;

  if (rparen <= sparen)
    return ERR;

  const char *comm_start = sparen + 1;
  size_t comm_len = (size_t)(rparen - comm_start);
  if (comm_len >= sizeof(stats->comm))
    return ERR;

  if (comm_len > 0)
    memcpy(stats->comm, comm_start, comm_len);
  stats->comm[comm_len] = '\0';
  stats->ppid = (pid_t)ppid_val;
  stats->start_time_ticks = (uint64_t)start_val;
  return OK;
}

/* Reads comm name, pid and start-time ticks for process 'pid' from procfs.
 * Ownership: borrows outputs; does not allocate.
 * Side effects: reads '/proc/<pid>/stat'.
 * Error semantics: returns OK on success, ERR on I/O or parse failure.
 */
static int procid_read_stat_fields(pid_t pid, StatFields *stats) {
  if (pid <= 1 || !stats)
    return ERR;

  char path[64];
  int n = snprintf(path, sizeof(path), "/proc/%ld/stat", (long)pid);
  if (n < 0 || (size_t)n >= sizeof(path))
    return ERR;

  char line[PROCID_STAT_MAX + 1];
  ssize_t nread = fileio_read_up_to(path, PROCID_STAT_MAX, (uint8_t *)line);
  if (nread < 0)
    return ERR;
  line[nread] = '\0';
  if (line[0] == '\0')
    return ERR;

  int rc = procid_parse_stat_line(line, stats);
  return rc;
}

/* Returns YES when 'name' is a known thin launcher wrapper process.
 * Ownership: borrows 'name'; does not allocate.
 * Side effects: none.
 * Error semantics: returns YES when wrapper-like, NO when not, ERR on invalid
 * input.
 */
static int procid_is_wrapper_name(const char *name) {
  if (!name || name[0] == '\0')
    return ERR;

  static const char *const wrappers[] = {
      "sh", "bash", "dash", "zsh", "fish", "env",
  };

  for (size_t i = 0; i < ARRLEN(wrappers); i++) {
    if (strcmp(name, wrappers[i]) == 0)
      return YES;
  }
  return NO;
}

int procid_parent_identity(ProcIdentity *out) {
  if (!out)
    return ERR;

#ifdef __linux__
  pid_t parent_pid = getppid();
  if (parent_pid <= 1)
    return ERR;

  StatFields parent_stats = {0};
  if (procid_read_stat_fields(parent_pid, &parent_stats) != OK)
    return ERR;

  ProcIdentity chosen = {
      .pid = parent_pid,
      .start_time_ticks = parent_stats.start_time_ticks,
  };

  if (procid_is_wrapper_name(parent_stats.comm) == YES &&
      parent_stats.ppid > 1) {
    // TODO: validate this wrapper heuristic with real MCP host launch chains.
    StatFields gparent_stats = {0}; // gradparent
    if (procid_read_stat_fields(parent_stats.ppid, &gparent_stats) == OK) {
      chosen.pid = parent_stats.ppid;
      chosen.start_time_ticks = gparent_stats.start_time_ticks;
    }
  }

  *out = chosen;
  return OK;
#else
  // TODO: at least, add support for mac
  return ERR;
#endif
}
