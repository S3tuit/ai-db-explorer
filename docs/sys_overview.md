# adbxplorer v1 System Overview

## Goal and threat model

`adbxplorer` is designed for a local threat model where an AI agent may be
helpful, confused, prompt-injected, or outright hostile.

The goal is to let that agent explore a real database without:

- receiving database credentials
- receiving plaintext values from configured sensitive columns
- making write-capable database changes

The system also tries to limit database abuse by combining read-only database
roles, read-only transactions, statement timeouts, row caps, and payload caps.
That lowers risk, but it does not make expensive read queries impossible.

The core rule is simple:

- treat everything on the agent side as untrusted
- keep secrets and policy enforcement inside one trusted component

## Trust model

The **Broker** is the primary security boundary.

The **MCP Host** and **MCP Server** are not trusted. They run next to the
agent, and the agent can influence how they are used. For that reason:

- database credentials must never be exposed there
- safety checks must not be delegated there
- sensitive plaintext values must not be returned there

This trust split is the defining design choice of the project.

## Main components

### Broker

The Broker is the trusted local process.

It is responsible for:

- loading connection metadata and secrets
- connecting to PostgreSQL
- enforcing safety policy centrally
- validating every SQL request
- applying read-only and timeout policy
- deciding which functions and columns are allowed
- replacing sensitive output values with opaque handles

The Broker is the only process that talks to the database.

### MCP Host and MCP Server

The MCP Host is the agent runtime, for example Codex or Claude Code.

The MCP Server is the process started by that host. In `adbxplorer`, the MCP
server is intentionally narrow: it speaks MCP over stdio, connects to the
Broker, forwards tool requests, and returns Broker responses.

It does not enforce policy locally. If the Broker is unavailable, the MCP
server returns an error instead of trying to continue in a degraded mode.

### Configuration and secret storage

`config.json` contains connection metadata and safety policy, but not database
passwords.

Passwords are stored separately through `adbxplorer -cred --sync` in the
selected secret store:

- OS-backed secret storage when available
- a user-owned file backend with strict permissions as the fallback

This keeps credentials out of the agent-side runtime and out of the MCP client
configuration.

## Sandbox boundary

For v1, the user is expected to sandbox the MCP Host and MCP Server. The Broker
should run outside that sandbox.

The reason is architectural:

- the host and MCP server are the untrusted surface
- the Broker holds secrets and performs sensitive-value handling
- separating them keeps the trusted state out of reach of the agent runtime

The usual setup is:

1. start the Broker outside the sandbox
2. mount the Broker runtime into the sandbox
3. let the host start `adbxplorer -client` inside the sandbox

## Communication model

### Runtime directories

The Broker creates a private application runtime with two subdirectories:

- `run/` for the Unix-domain socket used for Broker communication
- `secret/` for the handshake secret shared with the MCP server

The intended sandbox mount shape is:

- mount `run/` read-write inside the sandbox
- mount `secret/` read-only inside the sandbox

That allows communication with the Broker while preventing writes to the shared
secret from inside the sandbox.

### IPC and framing

Broker and MCP server communicate over a Unix-domain socket on Linux and macOS.

Every internal Broker frame is length-prefixed. The length prefix is 4 bytes in
big-endian order. Handshake scalar fields also use big-endian encoding on the
wire.

### Broker handshake

The first client message must be a Broker handshake.

That handshake combines:

- same-UID peer verification by the Broker
- a shared secret loaded from the Broker runtime
- an optional resume token for session continuity

The shared secret is not meant to defend against the sandboxed agent. Its job
is simply to reduce accidental local noise and avoid random same-user processes
from connecting by mistake.

## Sessions and continuity

Session state belongs to the Broker, not to the MCP server process ID.

That matters because MCP hosts may restart MCP servers whenever they want. If
session state lived only in the client process, sensitive-data workflows would
break too easily.

The current model is:

- the Broker owns active and idle sessions
- the MCP server persists a resume token locally on a best-effort basis
- on reconnect, the MCP server presents that token to resume the previous
  Broker session when still valid

The Broker currently applies:

- idle TTL: 20 minutes
- absolute TTL: 8 hours

If a session expires or the Broker exits, that session state is lost.

## Sensitive data handling

Sensitive-data handling is driven by per-connection `sensitiveDomains`
configuration.

When a query touches a column that belongs to a configured sensitive domain,
the Broker switches to a stricter validation path. If a sensitive value is
returned in the result set, the Broker does not return the plaintext. It
returns an opaque handle instead and keeps the underlying value in Broker
memory.

Those handles can later be passed back to the Broker in restricted query forms
so the agent can keep refining a search without learning the original value.

The detailed design lives in [`sensitive_data.md`](sensitive_data.md).
