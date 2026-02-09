# ai-db-explorer v1 Sensitive Columns: Security & Execution Contract

## 1. Terminology

* **Broker:** local privileged process with DB credentials and access to the SQLCipher vault.
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

* check connp_is_col_sensitive()

Canonicalization rules:

* identifiers lowercased1.
* invalid patterns rejected at config-load time.

### 2.2 `describe_table` tool is mandatory

Because `SELECT *` is banned (see §7), the broker must expose:

* `describe_table(connectionName, "schema.table")` returning column metadata and whether each column is sensitive.

No sample data is returned.

---

## 3. Sensitive Mode activation

### 3.1 Sensitive Mode trigger

If **any sensitive column identifier appears anywhere in the query text/AST**, the broker must treat the query as Sensitive Mode *candidate*.

Notes:

* “appears anywhere” includes SELECT list, WHERE, JOIN/ON, ORDER BY, GROUP BY, HAVING, function args, casts, expressions, subqueries, etc.
* To avoid bypass via tricky SQL, the broker must use parsing/analysis sufficient to detect identifier references robustly for the supported backend(s). If provenance or parsing is uncertain: **fail closed** (treat as referenced → Sensitive Mode → likely reject by rules).

---

## 4. Token binding (v1)

### 4.1 Token purpose

Tokens allow the agent to filter by sensitive values without submitting plaintext literals to SQL.
Token value should be high-entropy random (e.g. 128 bits).

### 4.2 Token storage

Since tokens are session-scoped and we trust that Broker's memory won't be
peeked, we store tokens in Broker's memory. To avoid unbounded grow we apply
limits:

* A session cannot hold more than 64MB of tokens.
* A query result cannot be bigger than ~5MB (to be defined).

### 4.3 Token use in queries

Sensitive predicates may only use tokens via prepared statement parameters (no string substitution).

Broker must reject:

* literal comparisons against sensitive columns
* any parameter that is not a token when used with a sensitive predicate.

Token cap:

* maximum 10 token parameters per query (Sensitive Mode only).

---

## 5. SQL acceptance policy

### 5.1 Global policy (all queries)

* Enforce read-only at DB role/session level.
* Apply `statement_timeout_ms` (and equivalents).
* Cap result rows to `max_row_per_query` (broker-side hard cap).
* **Ban `SELECT *` entirely.** Users/agents must use `describe_table` and explicit columns.
* Every column is qualified (alias.column) everywhere. Every range item must have an alias; references must use that alias.
* Ban any non-safe function calls.
* We treat views as tables, so it's up to the user to restrict a specific column of a view.
* Sensitive columns can appear only in the main SELECT and only in the SELECT or WHERE.

### 5.2 Sensitive Mode: strict subset

Sensitive Mode applies only when:

* query touches any sensitive column. *A touched column is a column that appears anywhere inside the sql text.*

Disallowed clauses in Sensitive Mode:

* All JOIN except for INNER / ON can only contain = and AND
* GROUP BY / HAVING on sensitive columns (allowed on non sensitive columns)
* ORDER BY on sensitive columns
* DISTINCT
* UNION/INTERSECT/EXCEPT
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

**LIMIT rules (Sensitive Mode):**

* If missing, reject.
* If present and > 200, reject.

**THIS BELOW IS STILL TO BE DEFINED**
**Small result rule (Sensitive Mode):**

* Query may return `< 5` rows **only if** WHERE contains at least one sensitive predicate.
* Otherwise reject if result size would be `< 5`.

Rationale: prevent “unique row” oracle on non-sensitive filters.
