#ifndef JSON_CODEC_H
#define JSON_CODEC_H

#include <stddef.h>
#include <stdint.h>

#include "query_result.h"
#include "string_op.h"

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

/* JSON helpers for building objects/arrays with automatic comma handling. */
int json_obj_begin(StrBuf *sb);
int json_obj_end(StrBuf *sb);
int json_arr_begin(StrBuf *sb);
int json_arr_end(StrBuf *sb);
int json_kv_obj_begin(StrBuf *sb, const char *key);
int json_kv_arr_begin(StrBuf *sb, const char *key);
int json_kv_str(StrBuf *sb, const char *key, const char *val);
int json_kv_u64(StrBuf *sb, const char *key, uint64_t val);
int json_kv_l(StrBuf *sb, const char *key, long val);
int json_kv_bool(StrBuf *sb, const char *key, int val);
int json_arr_elem_str(StrBuf *sb, const char *val);
int json_arr_elem_u64(StrBuf *sb, uint64_t val);
int json_arr_elem_l(StrBuf *sb, long val);
int json_arr_elem_bool(StrBuf *sb, int val);

// helper to init a json object and add "jsonrpc":"2.0"
int json_rpc_begin(StrBuf *sb);

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

/* Validates a JSON-RPC request. Expects a NUL-terminated JSON string.
 * Returns YES if the payload is valid and has jsonrpc/id/method, NO if it
 * doesn't match the schema, ERR on parse errors. */
int json_simple_rpc_validation(const char *json);

#endif
