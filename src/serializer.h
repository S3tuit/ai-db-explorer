#ifndef SERIALIZER_H
#define SERIALIZER_H

#include <stddef.h>
#include <stdint.h>

#include "query_result.h"

/* 
 * Everything is serialized into a JSON-RPC 2.0 payload (no Content-Length
 * frame).
 */

/*
 * Format:
 *  {"jsonrpc":"2.0","id":<u>,"result":{"exec_ms":<U>,columns:["name":<s>,
 *  "type":<s>...]},rows [[]...],"rowcount":<u>,"truncated":<s>}
 *
 * Returns:
 *  OK: success. *out_json points to a malloc'd buffer containing exactly
 *  out_len bytes. It is NOT NUL-terminated (treat as raw bytes).
 *  ERR: error. *out_json is set to NULL, *out_len set to 0
 *
 * NOTE: Caller must free(*out_json).
 */
int serializer_qr_to_jsonrpc(const QueryResult *qr, char **out_json,
        size_t *out_len);

/* Serializes 'method' and a variable number of [key, value] strings. The keys
 * and values must be exaclty 'key_no'.
 *
 * Format:
 *  {"jsonrpc":"2.0","id":<'id'>,"method":<'method'>,"params":{<key>:<value>}}
 * 
 * Returns the same as serializer_qr_to_jsonrpc.
 * */
int serializer_method_to_jsonrpc(const char *method, uint32_t id,
        char **out_json, size_t *out_len, uint32_t key_no, ...);

#endif /* SERIALIZER_H */
