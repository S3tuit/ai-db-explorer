#ifndef JSON_CODEC_H
#define JSON_CODEC_H

#include <stddef.h>
#include <stdint.h>

#include "command_reader.h"
#include "query_result.h"
#include "broker.h"

/*
 * Format:
 *  {"jsonrpc":"2.0","id":<u>,"result":{"exec_ms":<U>,columns:["name":<s>,
 *  "type":<s>...]},rows [[]...],"rowcount":<u>,"truncated":<s>}
 *  {"jsonrpc":"2.0","id":<u>,"error":{"exec_ms":<U>,"message":<s>}}
 *
 * Returns:
 *  OK: success. *out_json points to a malloc'd buffer containing exactly
 *  out_len bytes. It is NOT NUL-terminated (treat as raw bytes).
 *  ERR: error. *out_json is set to NULL, *out_len set to 0
 *
 * NOTE: Caller must free(*out_json).
 */
int qr_to_jsonrpc(const QueryResult *qr, char **out_json, size_t *out_len);

/* Serializes a Command into JSON-RPC.
 *
 * Format:
 *  CMD_SQL:
 *    {"jsonrpc":"2.0","id":<u>,"method":"exec","params":{"sql":<s>}}
 *  CMD_META (with args):
 *    {"jsonrpc":"2.0","id":<u>,"method":<s>,"params":{<k>:<v>...}}
 *  CMD_META (no args):
 *    {"jsonrpc":"2.0","id":<u>,"method":<s>}
 *
 * Best-effort parsing (client never fails on args):
 * - Values parse as positive integers only when all digits; otherwise string.
 * - Quotes allow spaces in values; quotes are stripped (no escaping).
 * - Tokens without '=' become {"token":""}.
 * - Empty keys/values are allowed; duplicate keys: last wins.
 * - Keys with no '=' are treated as empty (e.g., a b -> {"a":"", "b":""}).
 * - The first '=' is treated as key-value separator, extra '=' in a token
 *   become part of the value (e.g., a=1=b -> {"a":"1=b"}).
 *
 * Returns the same as qr_to_jsonrpc.
 * */
int command_to_jsonrpc(const Command *cmd, uint32_t id,
        char **out_json, size_t *out_len);

/* Extracts values from 'json' based on 'fmt' and key paths.
 * 'json' is not required to be NUL-terminated; use 'json_len'.
 *
 * Format specifiers:
 *  %c expects a JSON string of length 1 (stores into char *)
 *  %s expects a JSON string (allocates into char **)
 *  %u expects a JSON number into uint32_t *
 *  %U expects a JSON number into uint64_t *
 *
 * Variadic layout: for each specifier, pass (const char *key, out_ptr).
 * Keys are dot-delimited paths (max depth 3). Arrays are not supported.
 *
 * Returns:
 *  YES -> all keys found and values stored.
 *  NO  -> at least one key missing or value is null. The value of the outputs
 *          is UB.
 *  ERR -> parse/type error.
 *
 * Note: output strings are NUL-terminated.
 */
int json_get_value(const char *json, size_t json_len, const char *fmt, ...);

#endif
