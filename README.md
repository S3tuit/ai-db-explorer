# adbxplorer

`adbxplorer` is a local, security-first MCP server for database exploration.

It is built for one specific problem: letting an AI agent inspect a database
without giving that agent direct access to database credentials, raw sensitive
values, or unrestricted SQL execution.

The key design choice is a split trust model:

- The **Broker** is trusted. It holds credentials, enforces policy, and talks
  to the database.
- The **MCP server** is untrusted. It runs next to the agent and only relays
  requests to the Broker.

If your main concern is "make database tools available to an agent quickly",
there are easier options. If your main concern is "let an untrusted local agent
explore a real database without handing it secrets or plaintext sensitive
data", this project is aimed at that use case.

## Why use this over Google MCP Toolbox for Databases?

These projects solve different problems.

Google's MCP Toolbox is a broad database tool platform: many data sources,
prebuilt tools, custom tool frameworks, connection pooling, authentication
integrations, and observability. It is a strong fit when you want a general
database MCP layer for apps, agents, or cloud deployments.

`adbxplorer` is narrower and more opinionated. It is designed around a local
threat model where the agent side is treated as hostile and the Broker is the
security boundary.

### Tradeoffs of `adbxplorer`

- More setup than a single-process MCP server.
- PostgreSQL-focused today even if there are plans to support more dbs.
- Not a generic database tool-building framework.
- You still need to sandbox the agent runtime correctly.
- Best suited for **read-only support**, not schema changes or admin flows.

## How It Works

1. You configure one or more database connections in `config.json`.
2. You securely store passwords using `adbxplorer -cred ...` instead of putting
   them in the config file or exporting them into the agent environment.
3. You start the Broker outside the agent sandbox with `adbxplorer -broker`.
4. Your MCP client starts `adbxplorer -client` inside the sandbox.
5. The MCP server relays tool calls to the Broker over a Unix socket.
6. The Broker validates every request, talks to PostgreSQL, and returns a
   policy-filtered response.

## Exposed MCP Tools

The current MCP surface is intentionally small:

- `list_database_connections`
- `describe_relation`
- `run_sql_query`
- `run_sql_query_tokens`

That is deliberate. This project is trying to reduce the unsafe surface area,
not maximize database capability.

## High-Level Setup

### 1. Install `adbxplorer`

From source:

```bash
make
sudo make install
```

After installation, the binary is `adbxplorer`.

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

On Linux, the default configuration file is usually under
`~/.config/adbxplorer/config.json`.

### 3. Create a config file

Start from [`resources/template-config.json`](resources/template-config.json).

Important points:

- `configNamespace` is a unit of isolation so different config files won't
  have clashing credentials.
- `safeFunctions` is the per-connection SQL function allowlist. There's already
  an internal subset of safe SQL functions, add here functions you created and
  want the agent to use.
- `sensitiveColumns` marks columns that should be tokenized instead of returned
  in plaintext.
- Keep `readOnly` since it's the only truly supported mode for now.

### 4. Use a dedicated read-only database user

`adbxplorer` forces you to use a read-only database role.

For PostgreSQL user for the connections you want
the agent to inspect.

### 5. Store credentials outside the agent environment

Do **not** put database passwords in the MCP client config and do **not**
export them as environment variables in the same namespace as the agent.

Use:

```bash
adbxplorer -cred -s
```

Or for one connection:

```bash
adbxplorer -cred -s MyPostgres
```

Useful credential commands:

- `adbxplorer -cred -s [connection]`
  Sync credentials into the configured secret backend.
- `adbxplorer -cred -t [connection]`
  Test stored credentials against configured connections.
- `adbxplorer -cred -r <namespace>`
  Reset stored credentials for one namespace.
- `adbxplorer -cred -r --everything`
  Wipe all stored credentials managed by `adbxplorer`.

### 6. Start the Broker

Run outside the agent sandbox:

```bash
adbxplorer -broker
```

### 7. Configure your MCP client

If the MCP client runs in the same environment and sees the same runtime
directory, the simplest setup is:

```toml
[mcp_servers.adbxplorer]
command = "adbxplorer"
args = ["-client"]
```

If the client runs inside a sandbox and the Broker runtime is mounted at a
different path inside that sandbox, point the client at the mounted runtime
directory:

```toml
[mcp_servers.adbxplorer]
command = "adbxplorer"
args = ["-client", "-appdir", "/apps/adbxplorer"]
```

The Broker and client must refer to the same runtime directory, even if the
host path and sandbox path differ.

## Configuration Concepts

### Sensitive columns

Mark sensitive columns in the config so the Broker can return tokens instead of
plaintext values.

This lets the agent do things like:

- inspect schemas
- run read-only queries
- filter by sensitive values using returned tokens

without ever seeing the underlying secret values directly.

### Safe functions

Only functions listed in `safeFunctions` are allowed in queries for that
connection. This is one of the main ways the Broker reduces the SQL surface
available to the agent.

### Runtime and config locations

- `-config <path>` lets you choose a specific config file.
- `-appdir <path>` lets you choose the shared Broker/MCP runtime directory.
- `-which-config` shows the effective paths for the current environment.

