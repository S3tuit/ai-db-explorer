#include "mcp_id.h"
#include "utils.h"

#include <string.h>

/* Initializes an integer id. Ownership: no allocations. */
void mcp_id_init_u32(McpId *id, uint32_t v) {
  if (!id)
    return;
  id->kind = MCP_ID_INT;
  id->u32 = v;
}

/* Initializes a string id by copying 's'. Ownership: allocates and must be
 * cleaned with mcp_id_clean(). Returns OK/ERR. */
int mcp_id_init_str_copy(McpId *id, const char *s) {
  if (!id || !s)
    return ERR;
  size_t len = strlen(s) + 1;
  char *dup = (char *)xmalloc(len);
  memcpy(dup, s, len);
  id->kind = MCP_ID_STR;
  id->str = dup;
  return OK;
}

/* Deep-copies 'src' into 'dst'. Ownership: allocates when src is string.
 * Returns OK/ERR. */
int mcp_id_copy(McpId *dst, const McpId *src) {
  if (!dst || !src)
    return ERR;
  if (src->kind == MCP_ID_INT) {
    mcp_id_init_u32(dst, src->u32);
    return OK;
  }
  if (!src->str)
    return ERR;
  return mcp_id_init_str_copy(dst, src->str);
}

/* Releases any owned memory inside 'id'. Safe to call multiple times. */
void mcp_id_clean(McpId *id) {
  if (!id)
    return;
  if (id->kind == MCP_ID_STR) {
    free(id->str);
  }
  id->kind = MCP_ID_INT;
  id->u32 = 0;
}
