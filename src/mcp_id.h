#ifndef MCP_ID_H
#define MCP_ID_H

#include <stdint.h>

typedef enum {
  MCP_ID_INT = 0,
  MCP_ID_STR = 1,
} McpIdKind;

/* MCP request id. If kind is MCP_ID_STR, the string is owned and must be
 * freed with mcp_id_clean(). */
typedef struct McpId {
  McpIdKind kind;
  union {
    uint32_t u32;
    char *str;
  };
} McpId;

/* Initializes an integer id. Ownership: no allocations. */
void mcp_id_init_u32(McpId *id, uint32_t v);

/* Initializes a string id by copying 's'. Ownership: allocates and must be
 * cleaned with mcp_id_clean(). Returns OK/ERR. */
int mcp_id_init_str_copy(McpId *id, const char *s);

/* Deep-copies 'src' into 'dst'. Ownership: allocates when src is string.
 * Returns OK/ERR. */
int mcp_id_copy(McpId *dst, const McpId *src);

/* Releases any owned memory inside 'id'. Safe to call multiple times. */
void mcp_id_clean(McpId *id);

#endif
