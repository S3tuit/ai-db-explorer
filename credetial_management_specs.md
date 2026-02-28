# Credential Management Specification — adbxplorer v1

## Overview

adbxplorer stores database connection metadata (host, port, database, username) in a user-editable config file but **never** stores passwords there. Passwords are managed separately through a credential subsystem that uses OS-native secret stores where available, with a secure file-based fallback for headless environments.

The rest of the application is **unaware** of which secret backend is in use. The only runtime consumer is `ConnManager`, which calls a uniform `SecretStore` interface to retrieve passwords lazily when creating database connections.

---

## Secret Store Abstraction

### Interface

```c
typedef struct SecretStore SecretStore;
typedef struct SecretStoreVTable SecretStoreVTable;

struct SecretStoreVTable {
    /// Writes a NUL-terminated secret into `out`. Returns OK or ERR.
    AdbxStatus (*get)(SecretStore *store, const char *ref, StrBuf *out);

    /// Stores a NUL-terminated secret. Returns OK or ERR.
    AdbxStatus (*set)(SecretStore *store, const char *ref, const char *secret);

    /// Deletes a stored secret. Returns OK or ERR.
    AdbxStatus (*delete)(SecretStore *store, const char *ref);

    /// Destroys the store and releases resources.
    void (*destroy)(SecretStore *store);
};

struct SecretStore {
    const SecretStoreVTable *vt;
};
```

### Key Format

Callers pass a plain `ref` string (the `connection_name` from `ConnProfile`). Each backend implementation **internally** prefixes it with `adbxplorer:` to namespace keys in the underlying store. Callers never construct or know about this prefix.

Example: caller passes `"prod-db"` → backend stores/retrieves under key `"adbxplorer:prod-db"`.

### Factory

```c
AdbxStatus secret_store_create(SecretStore **out);
```

This function probes backends in priority order and returns the first available one:

1. **macOS**: Keychain Services → `KeychainSecretStore`
2. **Linux**: Secret Service D-Bus API via `libsecret` → `LibsecretSecretStore`
3. **Fallback**: File-based store → `FileSecretStore`

The caller never chooses the backend. Detection is automatic.

---

## Backend Implementations

### KeychainSecretStore (macOS)

Uses the macOS Keychain Services C API. Reimplement the logic from [hrantzsch/keychain](https://github.com/hrantzsch/keychain/tree/master/src) in C.

Concrete struct:

```c
typedef struct {
    SecretStore base; // must be first member, for casting
} KeychainSecretStore;
```

Key mapping: the `adbxplorer:<ref>` string maps to the Keychain item's **account** attribute. Use a fixed **service** name such as `"adbxplorer"`.

### LibsecretSecretStore (Linux)

Uses `libsecret` to communicate with any Secret Service D-Bus provider (gnome-keyring, kwallet, etc.). Reimplement the logic from [hrantzsch/keychain](https://github.com/hrantzsch/keychain/tree/master/src) in C.

Concrete struct:

```c
typedef struct {
    SecretStore base;
} LibsecretSecretStore;
```

Key mapping: store secrets with attributes `{ "application": "adbxplorer", "ref": "<ref>" }`. Use an appropriate `SecretSchema`.

Detection: during `secret_store_create`, attempt to connect to the Secret Service D-Bus. If the service is unavailable or the call fails, fall through to the file fallback.

### FileSecretStore (Fallback)

Used when no OS keystore is available (typically headless Linux servers).

Concrete struct:

```c
typedef struct {
    SecretStore base;
    char *file_path; // e.g., ~/.adbxplorer/credentials
} FileSecretStore;
```

Storage details:

- **Location**: always in a user-owned directory resolved by a dedicated function (analogous to `confdir_resolve` but without an `input_path` parameter — it always resolves to the user's adbxplorer directory). The file is named `credentials.json`.
- **Location is independent of config file location.** Even if the config lives in a project directory or shared mount, the credentials file always resides in the user's home-based adbxplorer directory.
- **Format (v1)**: json key-value. The outer object has `pwd` as key and the object `"connectionName": "password"` as value. Passwords are the plain-text value.
- **Permissions**: file is created with mode `0600`. On every access (read or write), verify permissions have not drifted. If they have, fix back to `0600` and print a warning to stderr.
- **Warning**: every time any `cred` subcommand runs and the file backend is active, print to stderr:
  ```
  No supported keystore detected. Credentials stored in <path> (mode 0600).
  Consider enabling gnome-keyring or kwallet for stronger protection.
  ```

---

## State Tracking (Change Detection)

### Purpose

To detect when a connection's identity has changed between `cred` runs, we maintain a snapshot of the last-known connection identities.

### Connection Identity

A connection's identity is defined as the tuple: **(host, port, database, username)**. A change to any of these values means the connection has changed and a new password may be needed.

### State File

- **Location**: same directory as `credentials`.
- **Filename**: `state.json`
- **First line**: a comment (JSON doesn't support comments, so use a reserved key):
  ```json
  {
    "_warning": "DO NOT EDIT. Auto-generated by adbxplorer. If corrupted, run: adbxplorer cred --reset or adbxplorer cred --init --force",
      "databases":[
        {
          "type":"postgres",
          "connectionName":"AnotherPostgres",
          "host":"localhost",
          "port":5432,
          "username":"postgres",
          "database":"another"
        }
      ]
  }
  ```
- **Contains no secrets.** Only connection metadata.
- **Same structure as config.json** So we can reuse the code inside conn_catalog.

### Diff Logic (used by `--update`)

Load the current config via `catalog_load_from_file`. Load `state.json`. Compare:

| Condition | Action |
|---|---|
| Connection name exists in config but not in state | **New connection** — prompt for password |
| Connection name exists in both, but identity tuple differs | **Changed connection** — prompt for password (offer to keep existing by just clicking Enter with empty value) |
| Connection name exists in both, identity tuple matches | **Unchanged** — skip |
| Connection name exists in state but not in config | **Removed connection** — delete credential from store, notify user |

After all prompts and store operations, overwrite `state.json` with the current config's connection identities.

---

## CLI Command Surface

All commands are subcommands of `adbxplorer cred`. The config file path is determined by `confdir_resolve(config_input, ...)`.

### `adbxplorer cred --init` (`-i`)

**Purpose**: first-time credential setup.

**Flags**:
- `--force` / `-f`: required to reinitialize when credentials already exist.

**Behavior**:

1. Read and validate config via `catalog_load_from_file`.
2. Check if any credentials already exist in the store for the configured connections.
   - **If credentials exist and `--force` is NOT set**: print a message suggesting `--update` and exit with code 0.
     ```
     Credentials already exist. To sync with config changes, use: adbxplorer cred --update
     To wipe and reinitialize all credentials, use: adbxplorer cred --init --force
     ```
   - **If credentials exist and `--force` IS set**: warn and require confirmation:
     ```
     This will delete all stored credentials and re-prompt for every connection.
     Type 'reset' to confirm:
     ```
     On confirmation, delete all existing credentials from the store.
3. For each connection in config, prompt for password via TTY.
4. Store each password in the secret store.
5. Write `state.json` with current connection identities.

**Output on success**:
```
4 credentials stored.
```

### `adbxplorer cred --update` (`-u`)

**Purpose**: sync stored credentials with the current config.

**Optional positional argument**: `<connection-name>` to target a single connection.

**Behavior (no argument — full sync)**:

1. Read and validate config.
2. Load `state.json`. If missing, print error suggesting `--init` and exit nonzero.
3. Diff config against state (see Diff Logic above).
4. For **new** connections: prompt for password, store it.
5. For **changed** connections: display what changed, then prompt:
   ```
   Connection 'prod-db' has changed (host: prod-host → prod-host-2).
   Enter new password (or press Enter to keep existing):
   ```
   - If user enters a password → store the new password, print `✓ prod-db: password updated`.
   - If user presses Enter → keep existing, print `✓ prod-db: password retained`.
6. For **removed** connections: delete credential from store, print:
   ```
   Connection 'staging' removed from config. Credential deleted.
   ```
7. For **unchanged** connections: no action.
8. Update `state.json`.

**Output summary**:
```
Summary: 1 new, 1 updated, 1 removed, 2 unchanged.
```

**Behavior (with connection name)**:

1. Read and validate config. Verify the named connection exists; if not, error and exit.
2. Prompt for new password for that connection only.
3. Store password. Update `state.json` if identity changed.

### `adbxplorer cred --test` (`-t`)

**Purpose**: verify connectivity using stored credentials.

**Optional positional argument**: `<connection-name>` to test a single connection.

**Behavior**:

1. Read and validate config.
2. For each connection (or the named one), retrieve password from store, attempt a database connection.
3. Print results:
   ```
   ==SUCCESS==
   prod-db    (postgres@prod-host:5432/mydb)
   ==FAIL==
   staging    (postgres@stg-host:5432/mydb)        authentication failed
   dev        (postgres@localhost:5432/mydb)       connection refused
   ```
4. Exit code: 0 if all succeed, nonzero if any fail.

### `adbxplorer cred --reset` (`-r`)

**Purpose**: delete all stored credentials.

**Behavior**:

1. Require deliberate confirmation:
   ```
   This will delete ALL stored credentials for adbxplorer.
   Type 'reset' to confirm:
   ```
2. If confirmed: delete all adbxplorer credentials from the active store. Delete `state.json`.
3. Print:
   ```
   ✓ All credentials deleted.
   ```
4. If not confirmed or input doesn't match: print `Aborted.` and exit with code 0.

### `adbxplorer cred --list` (`-l`)

**Purpose**: show configured connections and credential status.

**Behavior**:

1. Read and validate config.
2. For each connection, check if a credential exists in the store.
3. Print:
   ```
   prod-db    postgres@prod-host:5432/mydb      ✓ credential stored
   staging    postgres@stg-host:5432/mydb        ✓ credential stored
   dev        postgres@localhost:5432/mydb        ✗ no credential
   ```
4. Never display passwords.

---

## Cross-Cutting Behaviors

### Non-TTY Detection

Before any command that prompts for password input, check if stdin is a TTY. If not:

```
Error: password input requires an interactive terminal.
Run this command in an interactive shell.
```

Exit with nonzero code. Do **not** attempt to read from stdin in non-TTY mode. This applies to `--init`, `--update`, and `--reset` (which requires typed confirmation).

`--test` and `--list` do not require TTY and work fine in non-interactive contexts.

### Password Input

Always use a TTY-based secure input method that disables echo (e.g., `termios` manipulation or equivalent). Never display the password as it is typed.

### Credential File Location

Resolved by a dedicated function similar to confdir_resolve but with no `input_path` parameter.

Always resolves to a user-owned directory, regardless of where the config file lives.

### File Fallback Permission Check

On every `cred` subcommand invocation when `FileSecretStore` is active:

1. Check if `credentials` file exists.
2. If it exists, `stat` the file and verify mode is `0600`.
3. If mode has drifted, `chmod` back to `0600` and print to stderr:
   ```
   Permissions on ~/.adbxplorer/credentials were incorrect. Fixed to 0600.
   ```

