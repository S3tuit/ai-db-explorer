#include "spool.h"

#include "hash_table.h"
#include "utils.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define SPOOL_HT_INIT_SLOTS 64u

/* Stores one new interned string and indexes it in the hash table.
 * It borrows 'sp' and 's'. On success the returned pointer is arena-owned and
 * remains valid for pool lifetime.
 * Side effects: appends bytes into arena and inserts an index entry.
 * Error semantics: returns interned pointer on success, NULL on allocation or
 * hash-table insertion failure.
 */
static const char *spool_store(StringPool *sp, const char *s, uint32_t len) {
  assert(sp != NULL);
  assert(s != NULL);
  if (sp->arena.head == NULL || sp->arena.tail == NULL)
    return NULL;

  const char *owned = (const char *)pl_arena_add(&sp->arena, (void *)s, len);
  if (!owned)
    return NULL;
  if (ht_put(sp->index, owned, len, owned) != OK)
    return NULL;
  return owned;
}

StringPool *spool_create(void) {
  StringPool *sp = (StringPool *)xcalloc(1, sizeof(*sp));
  if (spool_init(sp) != OK) {
    free(sp);
    return NULL;
  }
  return sp;
}

int spool_init(StringPool *sp) {
  if (!sp)
    return ERR;

  memset(sp, 0, sizeof(*sp));
  if (pl_arena_init(&sp->arena, NULL, NULL) != OK)
    return ERR;

  sp->index = ht_create_with_capacity(SPOOL_HT_INIT_SLOTS);
  if (!sp->index) {
    pl_arena_clean(&sp->arena);
    return ERR;
  }
  return OK;
}

void spool_clean(StringPool *sp) {
  if (!sp)
    return;
  ht_destroy(sp->index);
  sp->index = NULL;
  pl_arena_clean(&sp->arena);
}

void spool_destroy(StringPool *sp) {
  if (!sp)
    return;
  spool_clean(sp);
  free(sp);
}

const char *spool_add(StringPool *sp, const char *s) {
  if (!sp || !s)
    return NULL;
  size_t len = strlen(s);
  return spool_addn(sp, s, len);
}

const char *spool_addn(StringPool *sp, const char *s, size_t len) {
  if (!sp || !s)
    return NULL;
  if (len > UINT32_MAX)
    return NULL;

  uint32_t n = (uint32_t)len;
  const char *found = ht_get(sp->index, s, len);
  if (found)
    return found;
  return spool_store(sp, s, n);
}
