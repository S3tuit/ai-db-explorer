# Vault Key Storage & Retrieval Spec (v1)

## 1. Goals

* Protect **vault contents at rest** (SQLCipher database storing tokens and DB secrets).
* Provide a **clear, user-driven unlock UX**:

  * broker never silently unlocks
  * user runs `aidbx unlock` when prompted
* Support:

  * **macOS Keychain** (preferred)
  * **Linux Secret Service** via GNOME Keyring / KWallet (preferred)
  * **fallback passphrase mode** when no keyring is available
* Avoid passing secrets via command line arguments or environment variables.

Non-goals:

* Defeating a same-UID malware attacker once the user session is unlocked.

---

## 2. Data model and files

### 2.1 Vault file

* Path: `~/.local/share/ai-db-explorer/vault.db` (or `$XDG_DATA_HOME/ai-db-explorer/vault.db`)
* Permissions: directory `0700`, file `0600`
* Format: SQLCipher database

### 2.2 Vault metadata (non-secret)

* Path: `~/.config/ai-db-explorer/vault.json` (or `$XDG_CONFIG_HOME/ai-db-explorer/vault.json`)
* Permissions: directory `0700`, file `0600`
* Purpose:

  * declare **unlock mode**
  * store KDF parameters (passphrase mode)
  * store keyring item identifiers (keyring mode)

Schema (v1):

```json
{
  "version": 1,
  "unlockMode": "keyring" | "passphrase",
  "keyring": {
    "provider": "macos_keychain" | "secret_service",
    "service": "ai-db-explorer",
    "account": "vault-key",
    "label": "ai-db-explorer vault key"
  },
  "kdf": {
    "alg": "argon2id",
    "saltB64": "<base64>",
    "memKiB": 65536,
    "iters": 3,
    "parallelism": 1,
    "keyLen": 32
  }
}
```

Notes:

* `keyring` object is present only when `unlockMode=keyring`.
* `kdf` object is present only when `unlockMode=passphrase`.
* `saltB64` and KDF params are **not secret**.

---

## 3. Unlock modes

### 3.1 Keyring mode (preferred)

* A random 256-bit vault key `K_vault` is generated at setup.
* `K_vault` is stored in OS key storage:

  * macOS: Keychain
  * Linux: Secret Service (GNOME Keyring / KWallet)
* SQLCipher vault is opened with `K_vault`.

### 3.2 Passphrase mode (fallback)

* User supplies a passphrase at unlock time.
* `K_vault` is derived using Argon2id:

  * `K_vault = Argon2id(passphrase, salt, mem, iters, parallelism, keyLen=32)`
* SQLCipher vault is opened with derived `K_vault`.
* Passphrase is never stored.

---

## 4. Setup procedure (`aidbx setup`)

### 4.1 Common steps

1. Create config/data directories with `0700`.
2. Create `vault.db` initialized as SQLCipher database.
3. Create `vault.json` with mode and required metadata.

### 4.2 Keyring setup

1. Generate `K_vault` (32 bytes) using OS CSPRNG (preferred) or best available RNG.
2. Store `K_vault` in keyring as an opaque secret.
3. Write `vault.json` with:

   * `unlockMode=keyring`
   * keyring provider + identifiers
4. Initialize SQLCipher vault using `K_vault`.

**Keyring availability test:**

* attempt a create/read/delete of a test item under the chosen provider.
* if it fails, fall back to passphrase setup.

### 4.3 Passphrase setup

1. Prompt user for passphrase twice (no echo).
2. Generate random salt (16+ bytes) using CSPRNG.
3. Derive `K_vault` using Argon2id with default parameters.
4. Write `vault.json` with:

   * `unlockMode=passphrase`
   * KDF params + salt
5. Initialize SQLCipher vault using derived `K_vault`.
6. Zero passphrase buffers after use.

**Defaults (v1):**

* memKiB: 65536 (64 MiB)
* iters: 3
* parallelism: 1
* keyLen: 32

(Parameters may be tuned per platform.)

---

## 5. Retrieval procedure (`aidbx unlock`)

### 5.1 CLI behavior

`aidbx unlock` must:

1. Load `vault.json`.
2. Acquire `K_vault` via the selected mode.
3. Perform a secure unlock handshake with the broker (see §6).
4. On success, broker marks vault as unlocked for the session TTL.
5. CLI prints a clear message:

   * success: “Vault unlocked.”
   * failure: “Vault unlock failed: <reason>”

**No secrets** printed.

### 5.2 Keyring retrieval

* macOS Keychain:

  * retrieve secret for (`service`, `account`)
  * if item requires user presence, OS will display prompt (Touch ID/password)
* Linux Secret Service:

  * retrieve secret for (`service`, `account`)
  * if keyring locked, retrieval fails with a “locked” error → CLI prints “Unlock your keyring and retry.”

### 5.3 Passphrase retrieval

* prompt user for passphrase (no echo)
* derive `K_vault` using Argon2id with params from `vault.json`
* do not store passphrase
* zero buffers after derive

---

## 6. Unlock handshake (CLI ↔ Broker)

### 6.1 Goals

* Avoid passing `K_vault` via argv/env.
* Prefer **proof of possession** instead of sending the raw key.
* Ensure only same-UID clients can attempt unlock.

### 6.2 Transport prerequisites

Broker exposes a Unix domain socket in a private runtime directory:

* Linux: `$XDG_RUNTIME_DIR/ai-db-explorer/broker.sock`
* macOS: `$TMPDIR/ai-db-explorer/<uid>/broker.sock` or similar
  Permissions:
* runtime dir: `0700`
* socket: `0600`

Broker validates peer identity:

* Linux: `SO_PEERCRED` UID check
* macOS: `getpeereid()` UID check

### 6.3 One-time challenge

Broker maintains an in-memory `unlock_nonce` per connected client session (or generates per unlock attempt).

Unlock protocol:

1. CLI requests nonce:

   * `UNLOCK_CHALLENGE` → broker responds `{nonce, session_id, request_id}`
2. CLI computes:

   * `proof = HMAC(K_vault, nonce || session_id || "unlock")`
3. CLI sends:

   * `UNLOCK_PROVE {request_id, proof}`
4. Broker verifies proof using its own `K_vault` source:

   * In v1 broker does NOT have `K_vault` until unlocked, so broker must:

     * accept proof and then attempt to open SQLCipher using `K_vault` provided by CLI OR
     * accept proof plus *wrapped key* (see §6.4)

Given broker cannot verify without `K_vault`, v1 chooses §6.4.

### 6.4 Minimal v1 handshake (practical)

CLI sends the raw key over the protected local socket:

* `UNLOCK_KEY {request_id, K_vault_bytes}`

Broker:

* checks peer UID
* immediately uses `K_vault` to open SQLCipher
* wipes `K_vault` buffer after opening
* replies success/failure

**Security note:** raw key crosses local IPC. This is acceptable for v1 given:

* socket is private + peer-verified
* threat model does not claim to defeat same-UID malware

### 6.5 v2 handshake upgrade (optional)

To avoid sending raw key:

* broker stores a “vault verifier” inside vault and uses OS keychain itself to retrieve `K_vault` (requires broker to have keychain integration)
* then proof-of-possession can be verified without sending key.

---

## 7. Broker unlock state and TTL

* Broker maintains:

  * `vault_state = LOCKED | UNLOCKED`
  * `vault_unlocked_until` timestamp
* On unlock:

  * open SQLCipher handle
  * derive `K_master` for HMAC/session pseudonyms if needed
* TTL default: 15 minutes (configurable)
* Broker provides:

  * `aidbx lock` to close SQLCipher and wipe keys
  * `aidbx status` to show locked/unlocked (no secrets)

---

## 8. Key rotation and reset (v1)

### 8.1 Reset

`aidbx vault reset`:

* closes broker vault if open
* deletes `vault.db`
* deletes `vault.json`
* deletes keyring entry if present
* requires explicit confirmation

### 8.2 Rotation (defer)

Key rotation can be v2 (SQLCipher supports rekey). Not required for v1.

---

## 9. Security hygiene requirements

* Never accept passphrase or key via CLI args.
* Zero sensitive buffers (`explicit_bzero` / `memset_s`) after use.
* Do not log:

  * `K_vault`
  * passphrases
  * token plaintext mappings
* Ensure files/dirs have correct permissions on creation (umask + chmod).

---

## 10. Error contract to MCP client

When broker requires vault but it is locked, return a structured error:

* code: `VAULT_LOCKED`
* message: “Unlock required. Run: aidbx unlock”
* fields:

  * `unlock_command`
  * `provider_hint` (keyring|passphrase)
  * `request_id`

---
