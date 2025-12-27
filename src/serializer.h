#ifndef SERIALIZER_H
#define SERIALIZER_H

#include <stddef.h>
#include <stdint.h>

#include "query_result.h"

/*
 * Serializes 'qr' into a JSON-RPC 2.0 payload (no Content-Length frame).
 * Returns:
 *  OK: success. *out_json points to a malloc'd buffer containing exactly
 *  out_len bytes. It is NOT NUL-terminated (treat as raw bytes).
 *  ERR: error. *out_json is set to NULL, *out_len set to 0
 *
 * NOTE: Caller must free(*out_json).
 */
int serializer_qr_to_jsonrpc(const QueryResult *qr, char **out_json,
        size_t *out_len);


#endif /* SERIALIZER_H */
