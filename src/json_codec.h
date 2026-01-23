#ifndef JSON_CODEC_H
#define JSON_CODEC_H

#include <stddef.h>
#include <stdint.h>

#include "query_result.h"
#include "string_op.h"

// Avoid compiling jsmn implementation in every TU that includes this header.
#ifndef JSMN_HEADER
#define JSMN_HEADER
#endif
#include "jsmn.h"

/*
 * Format:
 *  {"jsonrpc":"2.0","id":<u|s>,"result":{"exec_ms":<U>,columns:["name":<s>,
 *  "type":<s>...]},rows [[]...],"rowcount":<u>,"resultTruncated":<s>}
 *  {"jsonrpc":"2.0","id":<u|s>,"error":{"exec_ms":<U>,"message":<s>}}
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
int json_kv_null(StrBuf *sb, const char *key);
int json_arr_elem_str(StrBuf *sb, const char *val);
int json_arr_elem_u64(StrBuf *sb, uint64_t val);
int json_arr_elem_l(StrBuf *sb, long val);
int json_arr_elem_bool(StrBuf *sb, int val);

// helper to init a json object and add "jsonrpc":"2.0"
int json_rpc_begin(StrBuf *sb);

#define JSON_GETTER_MAX_TOKENS 1024

typedef struct JsonArrIter {
    int arr_tok;   // token index of the array
    int idx;       // current element index [0..count)
    int count;     // number of elements in the array
    int next_tok;  // next token index to consume (internal cursor)
} JsonArrIter;

typedef struct JsonGetter {
    const char *json;
    size_t json_len;
    const jsmntok_t *toks;
    int ntok;
    int root; // token index of the root object for this view
    jsmntok_t tok_storage[JSON_GETTER_MAX_TOKENS];
} JsonGetter;

typedef struct { const char *ptr; size_t len; } JsonStrSpan;

/*
 * Initializes JsonGetter by tokenizing the entire JSON text.
 *
 * Requirements:
 *  - json is not required to be NUL-terminated; use json_len.
 *  - root must be a JSON object.
 *  - token buffer is capped to 1024 tokens.
 *
 * Return OK on success, ERR on error/bad input.
 */
int jsget_init(JsonGetter *jg, const char *json, size_t json_len);

/* Validates a JSON-RPC request and initializes JsonGetter.
 * Returns YES if the payload is valid and has jsonrpc/id/method, NO if it
 * doesn't match the schema, ERR on parse errors. */
int jsget_simple_rpc_validation(JsonGetter *jg);

// TODO: all these functions that returns a value give a key may take the idx
// of an object so we scan starting from that object not from root. This
// improves performace.

/*
 * Gets a key path as a uint32_t (supports dot-delimited paths).
 *
 * Return:
 *  YES -> found and parsed successfully.
 *  NO  -> key not found or value is null.
 *  ERR -> type/parse error.
 */
int jsget_u32(const JsonGetter *jg, const char *key, uint32_t *out_u32);

/*
 * Gets a key path as a boolean into *out01 (0=false, 1=true). Returns
 * yes/no/err.
 */
int jsget_bool01(const JsonGetter *jg, const char *key, int *out01);

/*
 * Gets a key path as a double. Returns yes/no/err.
 */
int jsget_f64(const JsonGetter *jg, const char *key, double *out_double);

/*
 * Gets a key path as a signed 64-bit integer. Returns yes/no/err.
 */
int jsget_i64(const JsonGetter *jg, const char *key, int64_t *out_long);

/*
 * Checks whether a key path exists and is not JSON null. Returns yes/no/err.
 */
int jsget_exists_nonnull(const JsonGetter *jg, const char *key);

/*
 * Gets a key path as a raw JSON string content span (WITHOUT quotes).
 * This does NOT unescape; it returns a view into the JSON buffer. Returns
 * yes/no/err.
 */
int jsget_string_span(const JsonGetter *jg, const char *key, JsonStrSpan *out);

/*
 * Gets a key path as a decoded (unescaped) NUL-terminated string.
 * Caller owns the returned string and must free it. Return yes/no/err.
 */
int jsget_string_decode_alloc(const JsonGetter *jg, const char *key, char **out_nul);

/*
 * Gets a key path as a JsonGetter view rooted at the object value.
 *
 * The returned JsonGetter shares the token array and JSON buffer with the
 * input; it stays valid as long as the parent JsonGetter is alive.
 *
 * Returns yes/no/err.
 */
int jsget_object(const JsonGetter *jg, const char *key, JsonGetter *out);

/*
 * Initializes an iterator over an array of JSON strings at key path `key`.
 * Returns yes/no/err.
 */
int jsget_array_strings_begin(const JsonGetter *jg, const char *key, JsonArrIter *it);

/*
 * Gets next element of the array iterator as a raw JSON string content span.
 *
 * Return:
 *  YES -> produced next element.
 *  NO  -> no more elements.
 *  ERR -> element type error / token stream error.
 */
int jsget_array_strings_next(const JsonGetter *jg, JsonArrIter *it, JsonStrSpan *out_elem);

/*
 * Initializes an iterator over an array of JSON objects at key path `key`.
 * Returns yes/no/err.
 */
int jsget_array_objects_begin(const JsonGetter *jg, const char *key, JsonArrIter *it);

/*
 * Gets next element of the object array iterator as a JsonGetter view.
 *
 * The returned JsonGetter shares the token array and JSON buffer with the
 * input; it stays valid as long as the parent JsonGetter is alive.
 *
 * Return:
 *  YES -> produced next element (view rooted at the object element).
 *  NO  -> no more elements.
 *  ERR -> element type error / token stream error.
 */
int jsget_array_objects_next(const JsonGetter *jg, JsonArrIter *it, JsonGetter *out_obj);

/* Makes sure the json object identified by 'obj_key' only contains the
 * 'allowed' top-level keys. If obj_key is NULL, the root object is used.
 * Returns YES/NO/ERR. */
int jsget_top_level_validation(const JsonGetter *jg, const char *obj_key,
                                const char *const *allowed, size_t n_allowed);

#endif
