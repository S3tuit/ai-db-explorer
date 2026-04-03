# adbxplorer v1 Sensitive Data Handling

## Why this exists

Some database columns are too sensitive to return to an AI agent in plaintext,
even when the agent is otherwise allowed to inspect the database.

`adbxplorer` handles that by grouping those columns into sensitive domains and
then applying a stricter query policy whenever one of those columns is
touched.

The result is intentionally conservative:

- the agent can still inspect schemas and refine queries
- the Broker stays in control of real sensitive values
- sensitive plaintext does not leave the Broker process

## Configuration

### `sensitiveDomains`

Sensitivity is configured per connection with `sensitiveDomains`.

Each object key is the canonical domain name. Each value is an array of column
patterns using this shape:

- `[schema.][table.]column`

For example:

```json
"sensitiveDomains": {
  "email": [
    "users.mail",
    "*email*"
  ],
  "phone": [
    "private.users.phone",
    "ph_num"
  ]
}
```

Rules are normalized to lowercase at config-load time. `*` is supported only
in the final column segment, and malformed entries are rejected.

Two columns should share a domain only when they represent the same logical
kind of value and are expected to accept the same token parameters.

### `columnPolicy`

The config also supports:

```json
"columnPolicy": {
  "mode": "pseudonymize",
  "strategy": "randomized"
}
```

In v1:

- `mode` must be `pseudonymize`
- `strategy` can be `randomized` or `deterministic`

The strategy affects handle reuse inside one live Broker session:

- `randomized` may issue different handles for repeated equal values
- `deterministic` reuses the same handle for the same
  connection/domain/value while that Broker session state still exists

In both cases, handles should be treated as opaque Broker-owned values, not as
stable identifiers.

### Discovering sensitive columns

Because `SELECT *` is rejected, the intended discovery flow is:

1. call `describe_relation`
2. inspect the returned column metadata
3. write explicit `SELECT` lists

`describe_relation` includes a `sensitive` flag for each column and returns no
sample data.

## When the stricter policy turns on

The Broker enables the stricter sensitive-data path when a query touches any
column that belongs to a configured sensitive domain.

That includes references in places such as:

- the `SELECT` list
- `WHERE`
- `JOIN ... ON`
- `ORDER BY`
- `GROUP BY`
- `HAVING`
- function arguments
- casts and other expressions

The Broker relies on parsed query structure, not on string matching. If the
query cannot be analyzed safely, it fails closed.

Sensitive columns are only allowed in the main query scope. References from
subqueries or other nested scopes are rejected.

## Opaque handles instead of plaintext

When a sensitive output column is returned, the Broker stores the real value in
memory and returns an opaque handle to the agent instead of the plaintext.

At a high level, those handles are:

- owned by the Broker
- tied to one connection and one sensitive domain
- meant for use only while the underlying Broker session state still exists

The agent can later pass them back through `run_sql_query_tokens`, which lets
the Broker resolve the real value internally without exposing it.

This mechanism is what makes “filter by a sensitive value without learning the
value” possible.

## Token storage and lifetime

Sensitive handles are stored in Broker memory, not in the MCP server.

Important consequences:

- a Broker restart drops them
- expired Broker session state drops them
- they are not meant for long-term persistence outside the running system

The Broker currently caps per-session sensitive-token storage at 64 MB.

`run_sql_query_tokens` accepts at most 10 handle parameters in one request.

## Query policy

### Global rules

These rules apply to all queries, not just sensitive ones:

- read-only policy is enforced at the database role and transaction level
- statement timeout is applied when configured
- result rows are capped by `maxRowReturned`
- response payload size is capped by `maxPayloadKiloBytes`
- `SELECT *` and `alias.*` are rejected
- every relation must have an alias
- every column reference must use `alias.column`
- only safe functions are allowed

Parameters are also tightly restricted:

- `run_sql_query_tokens` is the only intended path for parameterized queries
- parameters are allowed only inside `WHERE` comparisons
- parameters may only compare against sensitive columns
- token parameters must belong to the same sensitive domain as the column they
  are compared against

### Additional rules when sensitive columns are touched

When the query touches a sensitive column, the Broker applies extra limits.

#### Sensitive columns in `SELECT`

Sensitive output columns must appear as direct column references.

If that is allowed, the Broker returns opaque handles for those columns instead
of plaintext values.

#### Sensitive columns in `WHERE`

Sensitive predicates are intentionally narrow.

Allowed forms are:

- `col = $n`
- `col IN ($n, $m, ...)`

Where each parameter is an opaque handle for that same sensitive domain.

Additional `WHERE` limits:

- predicates must form an `AND`-only conjunction
- `OR` is rejected
- `NOT` is rejected

#### Joins and other clauses

When sensitive mode is active:

- only `INNER JOIN` is allowed
- `JOIN ... ON` may use only `=` and `AND`
- `JOIN ... ON` cannot reference sensitive columns
- `GROUP BY` cannot reference sensitive columns
- `HAVING` cannot reference sensitive columns
- `ORDER BY` cannot reference sensitive columns
- `DISTINCT` is rejected
- `OFFSET` is rejected
- `LIMIT` is required
- `LIMIT` must not exceed 200

## Practical effect

This design allows workflows like:

- inspect a relation with `describe_relation`
- select explicit non-sensitive columns
- receive opaque handles for sensitive output columns
- run a second query that filters on those handles

Without allowing workflows like:

- dumping sensitive plaintext directly to the agent
- comparing sensitive columns against user-written literals
- using arbitrary SQL structure around sensitive data
