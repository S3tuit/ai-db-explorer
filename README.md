# adbxplorer

`adbxplorer` is a local, security-first MCP server for database exploration.

It is meant for a specific job: letting an AI agent help you inspect a real
database without giving that agent direct access to database credentials, raw
sensitive values, or unrestricted SQL execution.

If you mainly want "some MCP server that can talk to a database", there are
simpler and more mature options.

If you want "a local agent can explore my database and support me, but
credentials and policy stay outside the agent runtime", that is the problem
`adbxplorer` is built for.

This project is still early. Today it is PostgreSQL-focused, local-first, and
aimed at read-only exploration rather than administration or schema changes.

## How it works

`adbxplorer` uses a split trust model:

- The **Broker** is trusted. It holds credentials, talks to the database, and
  enforces policy.
- The **MCP server** is untrusted. It runs next to the agent and only relays
  requests to the Broker.
- The **agent runtime** should be sandboxed by you, because prompt injection,
  hallucinations, or hostile tool use must be assumed possible.

This separation is the core of the project. The Broker is the security
boundary, not the MCP server.

In practice, the flow looks like this:

1. You configure one or more read-only database connections.
2. You store credentials outside the agent environment.
3. You start the Broker outside the agent sandbox.
4. Your MCP client runs `adbxplorer -client` inside the sandbox.
5. The MCP server forwards requests to the Broker, which validates and executes
   them under the configured safety policy.

When a query touches configured sensitive data, the Broker does not return the
plaintext value. It returns a token instead and keeps the real value on the
Broker side.

## How this differs from more general database MCP servers

Projects such as Google's MCP Toolbox for Databases solve a broader problem:
they aim to be general database tool platforms with more integrations,
deployment shapes, and framework features.

`adbxplorer` is narrower.

It is designed around a local threat model where:

- the agent runtime is treated as untrusted
- database credentials must stay outside that runtime
- central policy enforcement matters more than convenience
- returning raw sensitive values to the agent is not acceptable

That narrower scope comes with tradeoffs:

- More setup than a single-process MCP server.
- You still need to sandbox the agent runtime correctly.
- PostgreSQL is the only supported backend today.
- It is best for read-only assistance, not admin workflows.

## Quickstart

### 1. Install `adbxplorer`

If you are on Fedora 43:

```bash
sudo dnf install dnf-plugins-core
dnf copr enable s3tuit/ai-db-explorer
dnf install adbxplorer
```

On other Linux systems make sure to have `libsecret-devel` installed and then
build from source:

```bash
git clone --recurse-submodules https://github.com/S3tuit/ai-db-explorer.git
cd ai-db-explorer
make
sudo make install
```

For macOS, build from source:

```bash
git clone --recurse-submodules https://github.com/S3tuit/ai-db-explorer.git
cd ai-db-explorer
make
sudo make install
```

The installed binary is `adbxplorer`.

### 2. Discover the default paths

Run:

```bash
adbxplorer -which-config
```

This prints:

- the runtime shared directory
- the runtime secret directory
- the internal state/config directory
- the configuration file path

On Linux, the config file is usually under `~/.config/adbxplorer/config.json`.

### 3. Create `config.json`

Start from [`resources/template-config.json`](resources/template-config.json)
and place it at the configuration path shown by `adbxplorer -which-config`.

### 4. Use a dedicated read-only database user

`adbxplorer` expects database access to be read-only.

For PostgreSQL 14+, a simple setup is:

```sql
CREATE USER ro_example WITH PASSWORD 'replace-me';
GRANT CONNECT ON DATABASE mydb TO ro_example;
GRANT pg_read_all_data TO ro_example;
```

### 5. Store credentials outside the agent environment

Do not put database passwords in the agent's MCP config and do not export them
into the same environment as the agent.

Once your config file is ready, run:

```bash
adbxplorer -cred --sync
```

You can also sync one connection only:

```bash
adbxplorer -cred --sync MyPostgres
```

This stores credentials in a secret store instead of leaving them exposed to
the agent side.

### 6. Start the Broker

Run this outside the agent sandbox:

```bash
adbxplorer -broker
```

### 7. Create the sandbox

The important rule is not just "run the agent in a sandbox", but "mount the
runtime directories correctly":

- mount the Broker `run/` directory read-write inside the sandbox
- mount the Broker `secret/` directory read-only inside the sandbox

That lets the MCP server talk to the Broker while preventing writes to the
handshake secret from the agent side.

There is a helper script at
[`resources/agent-sandbox-init.sh`](resources/agent-sandbox-init.sh).
It is a `bubblewrap`-based setups for Codex/Claude Code. Read the comments and
adjust the paths before using it.

### 8. Run the MCP server inside your sandbox

The client mode is the MCP server. It talks to the Broker over the runtime
directories discovered earlier.

For Codex inside a sandbox where the app directory is mounted at
`/apps/adbxplorer` (as it's the case if you're using
`resources/agent-sandbox-init.sh`):

```bash
codex mcp add adbxplorer -- adbxplorer -client -appdir /apps/adbxplorer
```

For Claude Code:

```bash
claude mcp add adbxplorer -- adbxplorer -client -appdir /apps/adbxplorer
```

## What the MCP server exposes

Right now, the tool surface is intentionally small:

- list configured database connections
- describe a relation and its columns
- run a read-only SQL query
- run a read-only SQL query with token parameters

The generated tool reference lives at
[`docs/tools.md`](docs/tools.md).

## Config reference

The current config file is small on purpose. Here is what matters most.

### `configNamespace`

This is the namespace used for credential isolation.

If you use different config files or environments, give them different
namespaces so their stored credentials do not clash.

You can have many config.json files and choose which one to use with:

```bash
adbxplorer -broker -config absolute_path_to_config.json
```

### `safetyPolicy`

This defines the Broker-side limits applied to queries.

Today the template includes:

- `readOnly`
- `statementTimeoutMs`
- `maxRowReturned`
- `maxPayloadKiloBytes`
- `columnPolicy`

The important point is that policy is enforced by the Broker, not delegated to
the MCP server or the agent.

### `databases`

This lists the database connections the Broker may expose.

Each entry currently includes fields such as:

- `type`
- `connectionName`
- `host`
- `port`
- `username`
- `database`
- `safeFunctions`
- `sensitiveDomains` for sensitive columns that should share the same logical
  domain

### `safeFunctions`

This is the per-connection allowlist for SQL functions that the Broker may let
queries use.

Keep this list small. Add only functions you trust in your environment.
Note that basic, safe functions like `LOWER` or `ARRAY_AGG` are already deemed
safe. You can see all the default safe functions at
`docs/pg_safe_functions.json`.

### `sensitiveDomains`

Use `sensitiveDomains` to tell the Broker which columns are sensitive, and
which of those columns should be treated as the same logical kind of value.

Each object key is the domain name. Each value is a list of identifiers or
patterns with shape `[schema.][table.]column`.

Columns in the same domain:

- are treated as sensitive
- can accept the same token parameters in restricted `WHERE col = $n` and
  `WHERE col IN (...)` comparisons
- in deterministic mode, reuse the same token for the same
  `connection + domain + value`

Only group columns together when they really represent the same kind of value.
For example, two fiscal-code columns usually belong in the same domain; a
fiscal-code column and an email column should not.

For example:

```json
"sensitiveDomains": {
  "fiscal_code": [
    "users.fiscal_code",
    "*_cf"
  ],
  "phone": [
    "private.users.phone",
    "ph_num"
  ]
}
```

Pattern rules are normalized to lowercase when the config is loaded. `*` is
supported only in the final column segment.

This example matches, among others:

- fiscal_code: `private.users.fiscal_code`, `users.fiscal_code`,
  `registry.user_cf`
- phone: `private.users.phone`, `public.accounts.ph_num`

It will not match:

- `users.email`
- `public.users.phone`
- `registry.user_phone`

At a high level, more specific rules win over broader ones, and ambiguous
matches fail closed.

In practice, this means the agent can get an opaque token from one column and
reuse it against another column in the same domain, without ever seeing the
plaintext value.

For example, the agent can first run:

```sql
SELECT u.creation_date, u.fiscal_code FROM users u WHERE u.name = 'Matteo' LIMIT 10;
```

and receive an opaque token in place of `u.fiscal_code`.

It can then reuse that token in a second query such as:

```sql
SELECT r.account_id FROM registry r WHERE r.user_cf = $1 LIMIT 10;
```

That works because both `users.fiscal_code` and `registry.user_cf` belong to
the same `fiscal_code` domain.

When a query touches a column that belongs to a sensitive domain, the Broker
switches to the stricter sensitive-data path. Sensitive outputs are tokenized,
and query shape becomes more restricted.

For the deeper design around sensitive-data handling and token use, see
[`docs/sensitive_data.md`](docs/sensitive_data.md).

## Issues and feedback

For bugs, feature requests, setup problems, or general questions, please open a
GitHub issue. Thank you in advance <3.

If you believe you found a security vulnerability, do not open a public issue.
Follow the process at SECURITY.md.

## More reading

- System overview:
  [`docs/sys_overview.md`](docs/sys_overview.md)
- Tool definitions:
  [`docs/tools.md`](docs/tools.md)
- Sensitive data handling:
  [`docs/sensitive_data.md`](docs/sensitive_data.md)
- Vulnerability reporting:
  [`SECURITY.md`](SECURITY.md)
