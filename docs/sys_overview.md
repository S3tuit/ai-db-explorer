## Goal and threat model

The system is designed so that an AI agent (even if malicious or compromised)
can **explore and query a database** without being able to **write, stress the
DB** (v1) and **see sensitive data in plaintext**.
The core principle is: **treat everything on the agent side as untrusted**, and
enforce safety entirely in a trusted component.

---

## Main components

### Broker

The **Broker** is the trusted enforcement point. It is the *only* component
that holds database credentials and the *only* component allowed to talk to the
database. It assumes every incoming request could be hostile and therefore
performs all validation, authorization, and policy enforcement itself.

Key responsibilities include validating queries, enforcing read-only behavior,
controlling which columns/functions are allowed, and returning results in a way
that does not leak sensitive values.

---

### MCP Host and MCP Server

The **MCP Host** is the agent runtime (e.g., Codex-like app). It runs the agent
and orchestrates tool calls. When tools are needed, it starts (directly or
indirectly) an **MCP Server** process. In v1, the MCP Server is intentionally
“dumb”: it essentially **echoes/relays** requests and responses, and does not
contain enforcement logic.

Because the Host/Server side can be manipulated by the agent, it must be
assumed compromised. That’s why it cannot be trusted with credentials or
security decisions.

All the database creadentials are kept in a config file that (user's
responsibility) should be kept outside agent's touch.
Still, we try not to keep databases passwords as plaintext. Before the broker
runs the user has to call an utility (e.g., load-connections); we'll read the
configuration file, validate it, and using TTY ask the user for the passwords
of the connections (only the new/changed connections). We're going to store
credentials based on what's available on the environment our system runs:
1) OS store (optional, best UX)
2) config file with strict perms (0600) (simple, portable)

---

### Sandbox boundary

For v1, **the user is expected to sandbox/containerize the MCP Host and MCP
Server**, since those processes are the untrusted surface. On Linux this could
be done with bubblewrap/firejail/container: the agent-side runtime should be
constrained as much as practical.

The Broker is run separately from the sandboxed agent environment, so that
secrets (DB credentials and any sensitive-value handling) stay out of reach of
the agent runtime.

For these reasons, the user has to manually start the broker before the MCP
Server (or the broker may be auto-started at system startup).

---

## Communication model

### IPC via Unix domain socket

The MCP Server talks to the Broker over a **Unix domain socket** (macOS + Linux
in v1). The socket lives in a **private directory** with strict permissions so
only the intended user/processes can reach it.

On connection, the Broker performs **peer verification** (at minimum UID/GID
checks).

### Broker - MCP Server handshake
The goal of the handshake is to prevent random local process from connecting to
the broker. Not because it's unsafe, but to avoid noise and DoS attacks.

During startup, Broker creates:
* run/ directory with mode 0700, socket with mode 0600.
* secret/ directory with mode 0700, token file with 0600.

Then the user mounds the run/ directory into the sanbox with read-write perm,
and the secret/ directory with read-only perm. Even if the sandboxed process
runs as the same UID, a read-only bind mount stops writes, renames, and deletes
(from inside the sandbox).

Since users may get file permissions wrong (by changing them), we also use a
cheap SO_PEERCRED cheek; must be same uid (or allowed gids).

The token is not meant to protect from the agent; it only reduces accidental
local noise.

---

## Sensitive data handling: deterministic tokens (per Broker session)

### High-level idea

When a query touches a sensitive column, the Broker does **not** return the
plaintext. Instead it returns a **token** (a handle) while storing the real
sensitive value on the Broker side. The agent can later use that token only in
restricted ways, enabling it to filter/query without learning the actual
secret.

This lets the agent remain useful (it can still join logic and refine results)
while preventing it from “seeing” sensitive values.

A more in-depth documentation about tokens can be found at pseudonymize.md.

---

### Token binding rules

Tokens are designed to be valid only within a specific scope so they can’t be
replayed broadly.

* Tokens are valid for the current **Broker session** (more on this later).
* Tokens are bound to a specific **table + column**.

When the agent submits a query containing a token, the Broker verifies:

1. the token belongs to the active session scope, and
2. the token is authorized for the specific table/column context,
then it resolves the token internally and binds the real value to the SQL parameter.

---

## Session model and continuity (current direction)

### Why “session” is a Broker concept, not an MCP Server PID

Since MCP Hosts may start/stop MCP Servers unpredictably (restarts, crashes,
tool lifecycle differences), we avoid tying session identity to process IDs.

Instead, session is defined in a way that the **Broker controls** and can
enforce regardless of how the host behaves.

---

### Resumable identity across MCP Server restarts (proposal)

To preserve usability if the MCP Host restarts the MCP Server often, we are
considering a resumable identity mechanism where the MCP Server persists a
client identifier and presents it during handshake. The Broker would attach
TTL semantics to this identifier so it doesn’t last forever and can expire when
disconnected.

The flow is:
1) MCP Server connects.
2) Broker authenticates peer (UID/GID + capability token).
3) Broker creates a session and returns a token (random secret).
4) MCP Server persists that resume token in its own sandbox storage.
5) On restart, MCP Server presents resume token → Broker resumes session if
token valid + not expired.

The token may have an idle TTL of 20 min and absolute TTL 24h.

---

## User experience flow (v1)

### What the user does

In the current v1 plan, the user does two main things:

* Starts the **Broker** manually wherever they want (normal environment or
sandboxed, depending on preference).
* Runs their **agent host (Codex-like)** inside a sandbox/container, and the
host will spawn the MCP Server as needed.

The Broker can later be offered as an optional autostart service, since it is
lightweight and idle most of the time.

---

### What happens during agent work

Once running:

* The agent issues tool calls via the MCP Host.
* The MCP Server forwards SQL requests to the Broker over the Unix socket.
* The Broker validates and executes read-only queries.
* If sensitive columns are involved, the Broker returns tokens instead of
plaintext.
* The agent can refine queries using tokens in strictly limited WHERE patterns,
and the Broker resolves those tokens internally.

This keeps the agent productive while keeping secrets and write capability out
of the untrusted environment.
