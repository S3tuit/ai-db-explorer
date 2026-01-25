# ai-db-explorer v1 Sensitive Columns: Security & Execution Contract

## 0. Scope and goals

**Primary UX goal:** customer-support lookup. Agents can run broad read-only SQL on non-sensitive data. Sensitive columns are never returned in plaintext.

**Security goal:** prevent plaintext disclosure of configured sensitive columns and limit easy inference channels when sensitive columns are referenced, while keeping non-sensitive SQL flexible.

**Non-goal (v1):** preventing all inference or linkage attacks. We reduce obvious oracles, but a determined attacker may still learn facts via allowed queries.

---

## 1. Terminology

* **Broker:** local privileged process with DB credentials and access to the SQLCipher vault.
* **Vault:** SQLCipher DB storing secrets and token mappings. Locked by a user passphrase.
* **Vault open:** user has provided passphrase; broker holds it in memory.
* **Sensitive column:** configured by `columnPolicy.pseudonymize` patterns.
* **Sensitive Mode:** stricter validation & response rules applied to a query when any sensitive column is referenced.
* **Token:** broker-issued opaque identifier representing a sensitive value for a specific column scope.

---

## 2. Configuration (v1)

### 2.1 Column policy

`columnPolicy` supports **only** pseudonymization patterns in v1:

```json
"columnPolicy": {
  "pseudonymize": [
    "public.users.fiscal_code",
    "public.users.*",
    "billing.cards.number"
  ]
}
```

Patterns:

* `schema.table.column`
* `schema.table.*`
* (optional) allow shorthand `table.column` (treated as `public.table.column` in Postgres)

Canonicalization rules:

* identifiers normalized (e.g., lowercased) according to backend conventions (Postgres: unquoted identifiers lowercased).
* invalid patterns rejected at config-load time.

### 2.2 `describe_table` tool is mandatory

Because `SELECT *` is banned (see §7), the broker must expose:

* `describe_table(connectionName, "schema.table")` returning column metadata and whether each column is sensitive.

No sample data is returned.

---

## 3. Vault gating and Sensitive Mode activation

### 3.1 Vault gating

**Sensitive Mode is only possible if the vault is open**, because v1 pseudonymization uses an HMAC key derived from the user passphrase (or a key derived from it).

If the vault is not open:

* queries that reference sensitive columns MUST be rejected with a generic message: “Sensitive data access requires vault to be unlocked.”
* non-sensitive queries MAY proceed normally.

### 3.2 Sensitive Mode trigger

If **any sensitive column identifier appears anywhere in the query text/AST**, the broker must treat the query as Sensitive Mode *candidate*.

Notes:

* “appears anywhere” includes SELECT list, WHERE, JOIN/ON, ORDER BY, GROUP BY, HAVING, function args, casts, expressions, subqueries, etc.
* To avoid bypass via tricky SQL, the broker must use parsing/analysis sufficient to detect identifier references robustly for the supported backend(s). If provenance or parsing is uncertain: **fail closed** (treat as referenced → Sensitive Mode → likely reject by rules).

---

## 4. Session pseudonymization (v1)

### 4.1 Session identity

Each broker-client socket connection has a `session_id`.

**Generation:**

* Prefer CPU RNG (`RDRAND`) if available and trustworthy per platform checks.
* Otherwise prefer OS CSPRNG (recommended): `getrandom()` (Linux), `arc4random_buf()` (BSD/macOS), `BCryptGenRandom` (Windows).
* If you only have a “random 32-bit generator”, you MUST compose it into at least 128 bits (call 4 times) and treat it as weaker; log a warning in operator logs.

**Size:** 128 bits minimum.

### 4.2 HMAC keying

We use HMAC to produce session-stable pseudonyms.

* `K_master`: derived from vault passphrase (see §4.3) and held in memory only while vault is open.
* `K_session = HMAC(K_master, session_id)`
* `K_col = HMAC(K_session, scope_string)`

Where `scope_string` is:

```
connectionName || "\n" || schema || "\n" || table || "\n" || column
```

### 4.3 Deriving `K_master`

v1 proposal (simple, consistent):

* `K_master = KDF(passphrase, salt_from_vault)` where KDF is Argon2id or PBKDF2-HMAC-SHA256 (Argon2id preferred).
* Salt stored in SQLCipher vault metadata.
* `K_master` is not stored; only derived when vault open.

### 4.4 Pseudonym computation

For a sensitive value `v`:

* `tag = HMAC(K_col, v_bytes)`
* `pseud = "ps_" + base32(tag)[0:N]` (truncate for readability; choose N e.g. 20–32 chars)

Properties:

* stable within the session + scope
* changes across sessions
* not reversible without `K_master`

---

## 5. Token binding (v1)

### 5.1 Token purpose

Tokens allow the agent to filter by sensitive values without submitting plaintext literals to SQL.

### 5.2 Token minting API/tool

Broker exposes:

* `mint_token(connectionName, "schema.table.column", plaintext)` → returns `token_id`

Rules:

* minting may be restricted to user-provided inputs depending on product design; at minimum it must be an explicit tool call.
* token is bound to:

  * `connectionName`, `schema`, `table`, `column`
  * optionally `session_id` (recommended for v1)
* token value should be high-entropy random (e.g. 128 bits).

### 5.3 Token storage

Tokens are stored in SQLCipher vault so they can be reused later if desired.

Vault record fields:

* token_id
* scope_string
* plaintext (encrypted by SQLCipher)
* created_at
* optional: session_id binding (v1 recommended)

### 5.4 Token use in queries

Sensitive predicates may only use tokens via prepared statement parameters (no string substitution).

Broker must reject:

* literal comparisons against sensitive columns
* any parameter that is not a token when used with a sensitive predicate.

Token cap:

* maximum 10 token parameters per query (Sensitive Mode only).

---

## 6. Backend “unsecure quirks” policy

Each `DbBackend` is responsible for identifying and rejecting backend-specific constructs that are unsafe or complicate guarantees.

Examples:

* Postgres: “table selection” `SELECT some_table FROM other_table` (composite row value) must be rejected in Sensitive Mode, and may be rejected always.
* Postgres: `COPY ... PROGRAM`, `CREATE EXTENSION`, etc. (should already be blocked by read-only role, but backend may still ban keywords defensively if reachable).
* Mongo: backend-specific query operators once supported.

Implementation rule:

* Backends expose `backend_reject_quirks(ast_or_tokens, mode)` returning a structured rejection reason.

---

## 7. SQL acceptance policy

### 7.1 Global policy (all queries)

* Enforce read-only at DB role/session level.
* Apply `statement_timeout_ms` (and equivalents).
* Cap result rows to `max_row_per_query` (broker-side hard cap).
* **Ban `SELECT *` entirely.** Users/agents must use `describe_table` and explicit columns.
* Every column is qualified (alias.column) everywhere. Every range item must have an alias; references must use that alias.
* Ban any non-safe function calls. Add a way for the user to define a function as safe.
* Each select item must be a simple column reference: `alias.col`
* `AS alias` are mandatory.
* We treat views as tables, so it's up to the user to restrict a specific column of a view.
* Sensitive columns can appear only in the SELECT or WHERE.

### 7.2 Sensitive Mode: strict subset

Sensitive Mode applies only when:

* vault is open AND
* query touches any sensitive column. *A touched column is a column that appears anywhere inside the sql text.*

In Sensitive Mode, sensitive columns can appear only in this mode:

* `SELECT ... FROM ... INNER JOIN ... WHERE ... LIMIT ...`

Disallowed clauses in Sensitive Mode:

* All JOIN except for INNER / ON can only contain = and AND
* GROUP BY / HAVING on non sensitive columns
* ORDER BY
* DISTINCT
* WITH (CTEs)
* UNION/INTERSECT/EXCEPT
* subqueries (anywhere)
* OFFSET
* any casts or expressions that touches a sensitive column

**SELECT list rules (Sensitive Mode):**

* if select item is sensitive → broker pseudonymizes it before returning results
* if select item is not sensitive → return as-is

**WHERE rules (Sensitive Mode):**

* WHERE must be a conjunction of predicates: `pred (AND pred)*`
* No `NOT`, no `OR`, no parentheses.
* Sensitive column predicates allowed only as:

  * `col = $n`
  * `col IN ($n, $m, ...)`
    where each `$k` is a **token parameter** for that same column scope.
* Non-sensitive predicates: (v1 choice)

  * either allow only `=` and `IN` as well (simpler), or
  * allow a larger subset; if expanded, must be carefully specified per backend.

**LIMIT rules (Sensitive Mode):**

* If missing, broker injects `LIMIT 200`.
* If present and > 200 → clamp.

**Small result rule (Sensitive Mode):**

* Query may return `< 5` rows **only if** WHERE contains at least one sensitive predicate.
* Otherwise reject if result size would be `< 5`.

Rationale: prevent “unique row” oracle on non-sensitive filters.

---

## 8. Error handling and logging

### 8.1 Agent-facing errors

* Always return **generic errors** that do not include SQL text, parameter values, or DB-provided messages that might echo sensitive values.
* Include:

  * high-level category (validation error / timeout / db error)
  * SQLSTATE (if available)
  * request_id (random) for operator correlation

### 8.2 Operator logs

Do not “throw away DB errors completely”.

Operator logs MAY include:

* request_id
* SQLSTATE
* backend error code/class
* a redacted summary of query shape (e.g. “SensitiveMode SELECT from public.users with 3 columns, 2 predicates”)
* never log bound parameter values
* never log returned row data

In test builds (e.g. `-DADBX_TESTLOG`), you may increase verbosity but still must never print sensitive plaintext or token plaintext mappings.

---

## 9. Implementation notes (code structure)

### 9.1 Pipeline

For each incoming query:

1. Parse request + parameters (typed params: token vs normal).
2. Identify candidate sensitive identifiers in query (AST analysis; fail closed).
3. If sensitive referenced:

   * require vault open else reject
   * validate Sensitive Mode grammar + backend quirks
   * validate token parameters count and scope binding
4. Apply timeouts and row limits.
5. Execute via prepared statement.
6. Post-process result set:

   * for each selected column: if sensitive → pseudonymize per §4.4
7. On errors:

   * agent gets generic + request_id
   * operator logs get request_id + SQLSTATE + safe metadata

### 9.2 Modules (suggested)

* `config/` parse + validate config patterns
* `vault/` open/close, KDF, token store
* `policy/` sensitive detector + sensitive-mode validator
* `backend/` per-db quirks + schema introspection (`describe_table`)
* `exec/` prepared execution + result limiting + timeouts
* `mask/` HMAC pseudonymization

---

## 10. Open decisions (v1 defaults)

* Do we bind tokens to `session_id` (recommended) or allow reuse across sessions?
* Non-sensitive WHERE operators allowed in Sensitive Mode (strict vs flexible).
* Inject LIMIT vs reject missing LIMIT in Sensitive Mode (recommend inject).
* Enforce `<5 rows` rule by rejecting after execution vs using an additional count step (reject-after is simpler but leaks “it was <5”; acceptable in v1).

