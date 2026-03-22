#ifndef BROKER_RESPONSE_H
#define BROKER_RESPONSE_H

#include <stddef.h>

#include "conn_catalog.h"
#include "db_backend.h"
#include "mcp_id.h"
#include "query_result.h"
#include "string_op.h"
#include "utils.h"

/* Entity that handles json serialization of the different entities the broker
 * has to emit. It takes ownership of request-scoped payloads (QueryResult,
 * DbRelationInfo); it creates an interal summary for shared views
 * (ConnProfile).*/
typedef struct BrokerResponse BrokerResponse;

typedef enum {
  BRESPERR_INTERNAL = -32603,
  BRESPERR_INPARAM = -32602,
  BRESPERR_INREQ = -32600,
  BRESPERR_INMETHOD = -32601,
  BRESPERR_PARSER = -32700,

  BRESPERR_RESOURCE = -30001,
} BrespErrorCode;

/* Creates one BrokerResponse that owns the successful QueryResult payload.
 * It borrows 'id' and takes ownership of 'qr' only on success. On failure,
 * caller retains ownership of 'qr'. It deep-copies 'id'.
 * Returns a caller-owned BrokerResponse on success, NULL on invalid input or
 * allocation failure.
 */
BrokerResponse *bresp_create_query_result(const McpId *id, QueryResult *qr);

/* Creates one BrokerResponse that owns copied list_database_connections
 * summaries derived from 'profiles'. It borrows 'id' and 'profiles'.
 * It deep-copies 'id', and a summary of the connection info needed for
 * serialization. Returns a caller-owned BrokerResponse on success, NULL on
 * invalid input, unsupported backend kinds, or allocation failure.
 */
BrokerResponse *bresp_create_conn_profiles(const McpId *id,
                                           const ConnProfile *profiles,
                                           size_t n_profiles);

/* Creates one BrokerResponse that owns one described relation payload.
 * It borrows 'id' and 'profile', and takes ownership of 'info' only on
 * success. On failure, caller retains ownership of 'info'. It deep-copies 'id'.
 * Returns a caller-owned BrokerResponse on success, NULL on invalid input,
 * malformed relation metadata, sensitivity-resolution failure, or allocation
 * failure.
 */
BrokerResponse *bresp_create_relation_info(const McpId *id,
                                           const ConnProfile *profile,
                                           DbRelationInfo *info);

/* Creates one BrokerResponse that serializes as a JSON-RPC error object.
 * It borrows 'id' and formatting inputs. It deep-copies 'id'.
 * Returns a caller-owned BrokerResponse on success, NULL on invalid input or
 * allocation failure.
 */
BrokerResponse *bresp_create_err(const McpId *id, BrespErrorCode code,
                                 const char *fmt, ...);

/* Creates one BrokerResponse that serializes as a CallToolResult with
 * isError=true. It borrows 'id' and formatting inputs. It deep-copies 'id'.
 * Returns a caller-owned BrokerResponse on success, NULL on invalid input or
 * allocation failure.
 */
BrokerResponse *bresp_create_tool_err(const McpId *id, const char *fmt, ...);

/* Frees all memory owned by 'bresp', including any copied ids, messages, and
 * payload entities.
 * Error semantics: none; NULL input is ignored.
 */
void bresp_destroy(BrokerResponse *bresp);

/* Serializes one BrokerResponse into a full JSON-RPC payload.
 * It borrows 'bresp' and caller-owned initialized 'out_json'.
 * Side effects: resets and grows 'out_json'.
 * Returns OK on success, ERR on invalid input or serialization failure. On ERR
 * 'out_json' is reset to empty.
 */
AdbxStatus bresp_to_jsonrpc(const BrokerResponse *bresp, StrBuf *out_json);

#endif
