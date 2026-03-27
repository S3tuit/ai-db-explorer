#include "secret_store.h"

// This implementation is tested using Mac VMs for now

#ifndef __APPLE__

AdbxTriStatus secret_store_keychain_backend_probe(SecretStore **out_store,
                                                  SecretStoreErr *out_err) {
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  if (!out_store)
    return ERR;
  *out_store = NULL;
  ADBX_ERR_SETF(out_err, SSERR_ENV,
                "keychain backend is unavailable on this platform.");
  return NO;
}

#else /* __APPLE__ */

#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef ADBX_TEST_MODE
#define KC_SERVICE_NAME "com.adbxplorer.secret-store.test.v1"
#define KC_LABEL_PREFIX "adbxplorer-test"
#else
#define KC_SERVICE_NAME "com.adbxplorer.secret-store.v1"
#define KC_LABEL_PREFIX "adbxplorer"
#endif

typedef struct {
  SecretStore base;
} KeychainStore;

/* Stores one formatted Keychain OSStatus inside 'out_err'.
 * It borrows 'context'; it may allocate one temporary CoreFoundation error
 * string internally and always releases it before returning.
 * Side effects: updates one caller-owned typed error snapshot.
 * Return semantics: none.
 */
static void kc_set_status_err(SecretStoreErr *out_err, SecretStoreErrCode code,
                              const char *context, OSStatus status) {
  if (!out_err)
    return;

  char status_msg[128] = {0};
  CFStringRef cf_msg = SecCopyErrorMessageString(status, NULL);
  if (cf_msg) {
    (void)CFStringGetCString(cf_msg, status_msg, sizeof(status_msg),
                             kCFStringEncodingUTF8);
    CFRelease(cf_msg);
  }

  if (status_msg[0] != '\0') {
    ADBX_ERR_SETF(out_err, code, "%s: %s (OSStatus=%ld)",
                  context ? context : "keychain operation failed", status_msg,
                  (long)status);
    return;
  }

  ADBX_ERR_SETF(out_err, code, "%s (OSStatus=%ld)",
                context ? context : "keychain operation failed", (long)status);
}

/* Validates one typed secret reference before a Keychain operation.
 * It borrows 'ref' and 'op_name'; it allocates no memory.
 * Side effects: writes one caller-owned typed error on invalid input.
 * Returns OK on valid namespace+connection-name pairs, ERR on invalid input.
 */
static AdbxStatus kc_validate_ref(const SecretRefInfo *ref, const char *op_name,
                                  SecretStoreErr *out_err) {
  if (!op_name || !ref || !ref->cred_namespace || !ref->connection_name) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain %s failed: invalid input pointers. This is "
                  "probably a bug, please, report it.",
                  op_name ? op_name : "operation");
    return ERR;
  }

  if (ref->cred_namespace[0] == '\0' || ref->connection_name[0] == '\0') {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain %s failed: secret reference fields cannot be "
                  "empty. This is probably a bug, please, report it.",
                  op_name);
    return ERR;
  }

  return OK;
}

/* Adds one UTF-8 C string value to a CoreFoundation dictionary.
 * It borrows 'dict', 'key', and 'value'; it allocates one temporary CFString
 * and releases it before returning.
 * Side effects: mutates the caller-owned dictionary on success.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
static AdbxStatus kc_dict_add_cstr(CFMutableDictionaryRef dict, const void *key,
                                   const char *value, const char *field_name,
                                   SecretStoreErr *out_err) {
  if (!dict || !key || !value || !field_name) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain query build failed: invalid input pointers. This "
                  "is probably a bug, please, report it.");
    return ERR;
  }

  CFStringRef cf_value =
      CFStringCreateWithCString(NULL, value, kCFStringEncodingUTF8);
  if (!cf_value) {
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain query build failed: unable to allocate %s. "
                  "Please, retry.",
                  field_name);
    return ERR;
  }

  CFDictionarySetValue(dict, key, cf_value);
  CFRelease(cf_value);
  return OK;
}

/* Adds one raw byte attribute to a CoreFoundation dictionary.
 * It borrows 'dict', 'key', and 'data'; it allocates one temporary CFData and
 * releases it before returning.
 * Side effects: mutates the caller-owned dictionary on success.
 * Returns OK on success, ERR on invalid input or allocation failure.
 */
static AdbxStatus kc_dict_add_bytes(CFMutableDictionaryRef dict,
                                    const void *key, const uint8_t *data,
                                    size_t len, const char *field_name,
                                    SecretStoreErr *out_err) {
  if (!dict || !key || (!data && len > 0) || !field_name) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain query build failed: invalid input pointers. This "
                  "is probably a bug, please, report it.");
    return ERR;
  }

  CFDataRef cf_value = CFDataCreate(NULL, data, (CFIndex)len);
  if (!cf_value) {
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain query build failed: unable to allocate %s. "
                  "Please, retry.",
                  field_name);
    return ERR;
  }

  CFDictionarySetValue(dict, key, cf_value);
  CFRelease(cf_value);
  return OK;
}

/* Creates one base Keychain query scoped to adbxplorer-owned generic-password
 * items.
 * It allocates and returns one caller-owned CoreFoundation dictionary.
 * Side effects: none beyond CoreFoundation allocations.
 * Returns the owned query on success, NULL on allocation failure.
 */
static CFMutableDictionaryRef kc_make_base_query(SecretStoreErr *out_err) {
  CFMutableDictionaryRef query =
      CFDictionaryCreateMutable(NULL, 0, &kCFTypeDictionaryKeyCallBacks,
                                &kCFTypeDictionaryValueCallBacks);
  if (!query) {
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain query build failed: unable to allocate query "
                  "dictionary. Please, retry.");
    return NULL;
  }

  CFDictionarySetValue(query, kSecClass, kSecClassGenericPassword);
  CFDictionarySetValue(query, kSecAttrService, CFSTR(KC_SERVICE_NAME));
  return query;
}

/* Encodes one secret reference into the Keychain account attribute.
 * It borrows 'ref' and returns one owned C string that the caller must free.
 * The namespace is hex-encoded so account uniqueness remains unambiguous even
 * when connection names contain separators.
 * Returns the owned account string on success, NULL on invalid input or
 * allocation failure.
 */
static char *kc_encode_account(const SecretRefInfo *ref) {
  if (!ref || !ref->cred_namespace || !ref->connection_name)
    return NULL;

  size_t ns_len = strlen(ref->cred_namespace);
  size_t conn_len = strlen(ref->connection_name);
  if (ns_len > (SIZE_MAX - conn_len - 2) / 2)
    return NULL;

  size_t out_len = ns_len * 2 + 1 + conn_len + 1;
  char *out = (char *)malloc(out_len);
  if (!out)
    return NULL;

  static const char HEX[] = "0123456789abcdef";
  for (size_t i = 0; i < ns_len; i++) {
    uint8_t ch = (uint8_t)ref->cred_namespace[i];
    out[i * 2] = HEX[ch >> 4];
    out[i * 2 + 1] = HEX[ch & 0x0F];
  }

  out[ns_len * 2] = ':';
  memcpy(out + ns_len * 2 + 1, ref->connection_name, conn_len + 1);
  return out;
}

/* Builds one exact-item query for the secret identified by 'ref'.
 * It borrows 'ref' and returns one caller-owned query dictionary.
 * Side effects: allocates one query dictionary and one temporary account
 * string. The dictionary scopes matches to adbxplorer-owned items only.
 * Returns the owned query on success, NULL on invalid input or allocation
 * failure.
 */
static CFMutableDictionaryRef kc_make_account_query(const SecretRefInfo *ref,
                                                    SecretStoreErr *out_err) {
  if (!ref) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain query build failed: invalid secret reference. "
                  "This is probably a bug, please, report it.");
    return NULL;
  }

  char *account = kc_encode_account(ref);
  if (!account) {
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain query build failed: unable to allocate account "
                  "key. Please, retry.");
    return NULL;
  }

  CFMutableDictionaryRef query = kc_make_base_query(out_err);
  if (!query) {
    free(account);
    return NULL;
  }

  if (kc_dict_add_cstr(query, kSecAttrAccount, account, "account attribute",
                       out_err) != OK) {
    CFRelease(query);
    free(account);
    return NULL;
  }

  free(account);
  return query;
}

/* Builds one namespace-scoped query for adbxplorer-owned Keychain items.
 * It borrows 'cred_namespace' and returns one caller-owned query dictionary.
 * Side effects: allocates one query dictionary and one temporary CFData.
 * Returns the owned query on success, NULL on invalid input or allocation
 * failure.
 */
static CFMutableDictionaryRef
kc_make_namespace_query(const char *cred_namespace, SecretStoreErr *out_err) {
  if (!cred_namespace || cred_namespace[0] == '\0') {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain namespace query build failed: invalid namespace. "
                  "This is probably a bug, please, report it.");
    return NULL;
  }

  CFMutableDictionaryRef query = kc_make_base_query(out_err);
  if (!query)
    return NULL;

  if (kc_dict_add_bytes(query, kSecAttrGeneric, (const uint8_t *)cred_namespace,
                        strlen(cred_namespace), "namespace attribute",
                        out_err) != OK) {
    CFRelease(query);
    return NULL;
  }

  return query;
}

/* Builds one human-readable Keychain label for 'ref'.
 * It borrows 'ref' and returns one owned C string that the caller must free.
 * Returns the owned label on success, NULL on invalid input or allocation
 * failure.
 */
static char *kc_make_label(const SecretRefInfo *ref) {
  if (!ref || !ref->cred_namespace || !ref->connection_name)
    return NULL;

  size_t ns_len = strlen(ref->cred_namespace);
  size_t conn_len = strlen(ref->connection_name);
  size_t prefix_len = strlen(KC_LABEL_PREFIX);
  if (prefix_len > SIZE_MAX - ns_len - conn_len - 4)
    return NULL;

  size_t out_len = prefix_len + 1 + ns_len + 1 + conn_len + 1;
  char *label = (char *)malloc(out_len);
  if (!label)
    return NULL;

  (void)snprintf(label, out_len, "%s %s/%s", KC_LABEL_PREFIX,
                 ref->cred_namespace, ref->connection_name);
  return label;
}

/* Appends one looked-up Keychain secret into 'out' using the SecretStore get
 * contract.
 * It borrows 'base' and 'ref'; 'out' remains caller-owned and is reset by this
 * function before use.
 * Returns YES when the secret exists, NO when it is missing, ERR on failure.
 */
static AdbxTriStatus kc_get(SecretStore *base, const SecretRefInfo *ref,
                            StrBuf *out, SecretStoreErr *out_err) {
  if (!base || !out) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain get failed: invalid input pointers. This is "
                  "probably a bug, please, report it.");
    return ERR;
  }

  if (kc_validate_ref(ref, "get", out_err) != OK)
    return ERR;

  sb_zero_clean(out);
  sb_init(out);

  CFMutableDictionaryRef query = kc_make_account_query(ref, out_err);
  if (!query)
    return ERR;

  CFDictionarySetValue(query, kSecMatchLimit, kSecMatchLimitOne);
  CFDictionarySetValue(query, kSecReturnData, kCFBooleanTrue);

  CFTypeRef result = NULL;
  OSStatus status = SecItemCopyMatching(query, &result);
  CFRelease(query);

  if (status == errSecItemNotFound)
    return NO;
  if (status != errSecSuccess) {
    if (result)
      CFRelease(result);
    kc_set_status_err(out_err, SSERR_ENV, "keychain get failed", status);
    return ERR;
  }

  if (!result || CFGetTypeID(result) != CFDataGetTypeID()) {
    if (result)
      CFRelease(result);
    ADBX_ERR_SETF(out_err, SSERR_ENV,
                  "keychain get failed: unexpected result type.");
    return ERR;
  }

  CFDataRef data = (CFDataRef)result;
  CFIndex data_len = CFDataGetLength(data);
  const uint8_t *bytes = CFDataGetBytePtr(data);
  if (data_len < 0 || (data_len > 0 && !bytes)) {
    CFRelease(result);
    ADBX_ERR_SETF(out_err, SSERR_ENV,
                  "keychain get failed: invalid secret payload.");
    return ERR;
  }

  if (sb_append_bytes(out, bytes, (size_t)data_len) != OK ||
      sb_append_bytes(out, "", 1) != OK) {
    CFRelease(result);
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain get failed: unable to allocate the output buffer. "
                  "Please, retry.");
    return ERR;
  }

  CFRelease(result);
  return YES;
}

/* Stores or replaces one secret in macOS Keychain.
 * It borrows 'base', 'ref', and 'secret'; temporary CoreFoundation objects are
 * released before returning.
 * Side effects: mutates one Keychain generic-password item scoped to
 * adbxplorer-owned attributes.
 * Returns OK on success, ERR on invalid input or Keychain failure.
 */
static AdbxStatus kc_set(SecretStore *base, const SecretRefInfo *ref,
                         const char *secret, SecretStoreErr *out_err) {
  if (!base) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain set failed: invalid input pointers. This is "
                  "probably a bug, please, report it.");
    return ERR;
  }

  if (kc_validate_ref(ref, "set", out_err) != OK)
    return ERR;
  if (!secret) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain set failed: NULL secret. This is probably a bug, "
                  "please, report it.");
    return ERR;
  }

  char *label = kc_make_label(ref);
  if (!label) {
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain set failed: unable to allocate item label. "
                  "Please, retry.");
    return ERR;
  }

  CFMutableDictionaryRef add_query = kc_make_account_query(ref, out_err);
  if (!add_query) {
    free(label);
    return ERR;
  }

  if (kc_dict_add_bytes(
          add_query, kSecAttrGeneric, (const uint8_t *)ref->cred_namespace,
          strlen(ref->cred_namespace), "namespace attribute", out_err) != OK ||
      kc_dict_add_cstr(add_query, kSecAttrLabel, label, "label attribute",
                       out_err) != OK ||
      kc_dict_add_bytes(add_query, kSecValueData, (const uint8_t *)secret,
                        strlen(secret), "secret payload", out_err) != OK) {
    CFRelease(add_query);
    free(label);
    return ERR;
  }

  OSStatus status = SecItemAdd(add_query, NULL);
  if (status == errSecSuccess) {
    CFRelease(add_query);
    free(label);
    return OK;
  }

  if (status != errSecDuplicateItem) {
    CFRelease(add_query);
    free(label);
    kc_set_status_err(out_err, SSERR_ENV, "keychain set failed", status);
    return ERR;
  }

  CFMutableDictionaryRef update_query = kc_make_account_query(ref, out_err);
  CFMutableDictionaryRef attrs =
      CFDictionaryCreateMutable(NULL, 0, &kCFTypeDictionaryKeyCallBacks,
                                &kCFTypeDictionaryValueCallBacks);
  if (!update_query || !attrs) {
    if (!attrs) {
      ADBX_ERR_SETF(out_err, SSERR_WRITE,
                    "keychain set failed: unable to allocate update "
                    "attributes. Please, retry.");
    }
    if (update_query)
      CFRelease(update_query);
    if (attrs)
      CFRelease(attrs);
    CFRelease(add_query);
    free(label);
    return ERR;
  }

  /* Update only the mutable fields so duplicate items are repaired in place
   * without deleting the stored secret first.
   */
  if (kc_dict_add_bytes(
          attrs, kSecAttrGeneric, (const uint8_t *)ref->cred_namespace,
          strlen(ref->cred_namespace), "namespace attribute", out_err) != OK ||
      kc_dict_add_cstr(attrs, kSecAttrLabel, label, "label attribute",
                       out_err) != OK ||
      kc_dict_add_bytes(attrs, kSecValueData, (const uint8_t *)secret,
                        strlen(secret), "secret payload", out_err) != OK) {
    CFRelease(update_query);
    CFRelease(attrs);
    CFRelease(add_query);
    free(label);
    return ERR;
  }

  status = SecItemUpdate(update_query, attrs);
  CFRelease(update_query);
  CFRelease(attrs);
  CFRelease(add_query);
  free(label);

  if (status != errSecSuccess) {
    kc_set_status_err(out_err, SSERR_ENV, "keychain set failed", status);
    return ERR;
  }

  return OK;
}

/* Deletes one Keychain secret identified by 'ref'.
 * It borrows 'base' and 'ref'; temporary CoreFoundation objects are released
 * before returning.
 * Side effects: removes one adbxplorer-owned Keychain item when present.
 * Returns OK on success, ERR on invalid input or Keychain failure.
 */
static AdbxStatus kc_delete(SecretStore *base, const SecretRefInfo *ref,
                            SecretStoreErr *out_err) {
  if (!base) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain delete failed: invalid input pointers. This is "
                  "probably a bug, please, report it.");
    return ERR;
  }

  if (kc_validate_ref(ref, "delete", out_err) != OK)
    return ERR;

  CFMutableDictionaryRef query = kc_make_account_query(ref, out_err);
  if (!query)
    return ERR;

  OSStatus status = SecItemDelete(query);
  CFRelease(query);

  if (status == errSecSuccess || status == errSecItemNotFound)
    return OK;

  kc_set_status_err(out_err, SSERR_ENV, "keychain delete failed", status);
  return ERR;
}

/* Deletes every adbxplorer-owned secret stored in the namespace
 * 'cred_namespace'.
 * It borrows 'base' and 'cred_namespace'; temporary CoreFoundation objects are
 * released before returning.
 * Side effects: removes all matching Keychain items inside one namespace.
 * Returns OK on success, ERR on invalid input or Keychain failure.
 */
static AdbxStatus kc_wipe_namespace(SecretStore *base,
                                    const char *cred_namespace,
                                    SecretStoreErr *out_err) {
  if (!base) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain namespace wipe failed: invalid input pointers. "
                  "This is probably a bug, please, report it.");
    return ERR;
  }

  CFMutableDictionaryRef query =
      kc_make_namespace_query(cred_namespace, out_err);
  if (!query)
    return ERR;

  OSStatus status = SecItemDelete(query);
  CFRelease(query);

  if (status == errSecSuccess || status == errSecItemNotFound)
    return OK;

  kc_set_status_err(out_err, SSERR_ENV, "keychain wipe_namespace failed",
                    status);
  return ERR;
}

/* Deletes every adbxplorer-owned secret across all namespaces.
 * It borrows 'base'; temporary CoreFoundation objects are released before
 * returning.
 * Side effects: removes all adbxplorer-owned Keychain items.
 * Returns OK on success, ERR on invalid input or Keychain failure.
 */
static AdbxStatus kc_wipe_all(SecretStore *base, SecretStoreErr *out_err) {
  if (!base) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain wipe_all failed: invalid input pointers. This is "
                  "probably a bug, please, report it.");
    return ERR;
  }

  CFMutableDictionaryRef query = kc_make_base_query(out_err);
  if (!query)
    return ERR;

  OSStatus status = SecItemDelete(query);
  CFRelease(query);

  if (status == errSecSuccess || status == errSecItemNotFound)
    return OK;

  kc_set_status_err(out_err, SSERR_ENV, "keychain wipe_all failed", status);
  return ERR;
}

/* Destroys one Keychain backend wrapper instance.
 * It consumes 'base', which was allocated by
 * secret_store_keychain_backend_probe().
 * Side effects: releases heap memory for the wrapper only; Keychain state is
 * untouched.
 * Return semantics: none.
 */
static void kc_destroy(SecretStore *base) { free(base); }

static const SecretStoreVTable KC_VTABLE = {
    .get = kc_get,
    .set = kc_set,
    .delete = kc_delete,
    .wipe_namespace = kc_wipe_namespace,
    .wipe_all = kc_wipe_all,
    .destroy = kc_destroy,
};

AdbxTriStatus secret_store_keychain_backend_probe(SecretStore **out_store,
                                                  SecretStoreErr *out_err) {
  ADBX_ERR_CLEAR(out_err, SSERR_NONE);
  if (!out_store) {
    ADBX_ERR_SETF(out_err, SSERR_INPUT,
                  "keychain backend probe failed: invalid output pointer. "
                  "This is probably a bug, please, report it.");
    return ERR;
  }
  *out_store = NULL;

  SecretRefInfo probe_ref = {
      .cred_namespace = "__adbx_probe__",
      .connection_name = "__adbx_probe__",
  };
  CFMutableDictionaryRef probe_query =
      kc_make_account_query(&probe_ref, out_err);
  if (!probe_query)
    return ERR;

  CFDictionarySetValue(probe_query, kSecMatchLimit, kSecMatchLimitOne);
  CFDictionarySetValue(probe_query, kSecReturnAttributes, kCFBooleanTrue);

  CFTypeRef result = NULL;
  OSStatus status = SecItemCopyMatching(probe_query, &result);
  if (result)
    CFRelease(result);
  CFRelease(probe_query);

  if (status != errSecSuccess && status != errSecItemNotFound) {
    kc_set_status_err(out_err, SSERR_ENV,
                      "keychain backend is unavailable in this environment",
                      status);
    return NO;
  }

  KeychainStore *store = (KeychainStore *)xcalloc(1, sizeof(*store));
  if (!store) {
    ADBX_ERR_SETF(out_err, SSERR_WRITE,
                  "keychain backend probe failed: memory allocation error. "
                  "Please, retry.");
    return ERR;
  }
  store->base.vt = &KC_VTABLE;
  *out_store = &store->base;
  return YES;
}

#endif /* __APPLE__ */
