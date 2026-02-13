# MCP Server Session Resume Specification v1

## Overview

This document specifies the design for persistent session resume in an MCP (Model Context Protocol) server application.
The goal is to allow an MCP server to resume sessions across restarts while supporting multiple MCP hosts and handling edge cases safely.

## Problem Statement

When an MCP host (e.g., Claude Desktop, VSCode) closes its connection to our MCP server, the server process may be terminated.
Without persistence, any session state (represented by a broker-issued token) is lost.
When the host reconnects and starts a new MCP server instance, it must establish a new session from scratch, losing valuable agent context.

We need a way to:
1. Persist session tokens so they survive MCP server restarts
2. Support multiple different MCP hosts using the same MCP server application simultaneously (e.g., Claude and Codex in the same sandbox)
3. Handle crashes and edge cases safely without corrupting state or resuming invalid sessions

## Architecture

### Token Storage Location

Tokens are stored as individual files in a user-owned directory on the MCP server's filesystem.
The directory path is implementation-specific but must be writable by the MCP server process.

### Token File Naming Strategy

Token files are named using a deterministic pattern based on the MCP host's identity:

```
token-<parent_pid>-<parent_start_time>
```

Where:
- `parent_pid`: The process ID of the MCP host
- `parent_start_time`: The start timestamp of the MCP host process

**Rationale:** This combination uniquely identifies a specific MCP host process instance.
Even if PIDs are reused by the operating system, the combination of PID and start time remains unique.

This logic is handled by procid_parent_identity of src/proc_indentity.h/.c.

### Token File Format

Token files contain the session token as plain text with no additional formatting or metadata.

**Rationale:** Simplicity. Metadata like PID and start time are encoded in the filename, not the file contents.

## Operational Flow

### 1. MCP Server Startup

When the MCP server starts:

1. Obtain parent PID via procid_parent_identity().
2. If procid_parent_identity() fails:
   - Log: "Could not verify parent process start time, session resume disabled for this instance"
   - Continue without attempting to read or write token files
Step 1 and 2 are handled by restok_init of src/resume_token.h/.c.

3. Construct token filename: `token-<parent_pid>-<parent_start_time>`
4. Check if token file exists in the storage directory

Step 3 and 4 are handled by restok_load.

5. If restok_load returns YES:
     - Send token to broker for session resume
     - If broker accepts: session resumed successfully
     - If broker rejects: log "Broker rejected resume token, starting fresh session", delete token file, request new token from broker

The MCP server entity should have no knowledge if the resume token is disables or not.
All the data/function about the resume token should be handled by ResumeTokenStore, for example:
     - If read fails or token is corrupted:
     - Log: "Token file corrupted, treating as stale"
     - restok_load will not return YES. MCP server will request new token from broker

6. If token file exists and is valid the Broker will still return a new token (rotate). So in any case, the MCP server calls restok_store after receiving broker token.

### 2. Token Persistence

When the MCP server receives a token from the broker:

1. Write the token directly to the token file (no temporary file strategy needed, see "Design Decisions" below)
2. If write fails:
   - Log error but continue operation (session resume will not be available on next restart)

### 3. MCP Server Shutdown

No special handling required for v1. Token files remain on disk for the next startup.

### 4. Stale Token Cleanup

When a token file is determined to be stale or corrupted, it is immediately deleted. No separate cleanup process is needed.

## Error Handling & Edge Cases

### Parent Process Information Unavailable

**Scenario:** Cannot read parent PID or start time due to permissions or platform limitations.

**Behavior:** Disable session resume for this instance. Do not attempt to read or write token files. Log: "Could not verify parent process start time, session resume disabled for this instance"

**Rationale:** Conservative approach. Better to lose session resume than risk resuming someone else's session.

### Token File Corrupted

**Scenario:** Token file exists but contains invalid/incomplete data (e.g., due to crash during write).

**Behavior:** Delete token file, treat as fresh start. Log: "Token file corrupted, treating as stale"

**Rationale:** Corrupted tokens are unusable anyway. Starting fresh is safer than attempting to parse partial data.

### Broker Rejects Token

**Scenario:** Token file contains a valid-looking token, but broker rejects it (e.g., token expired, already in use by another session).

**Behavior:** Delete token file. Log: "Broker rejected resume token, starting fresh session"

**Rationale:** Token is no longer valid. Remove it to avoid repeated failed resume attempts.

### PID Reuse

**Scenario:** Host A crashes, host B restarts and gets A's old PID.

**Mitigation:** The parent start time check prevents this. Even if PIDs match, start times won't match unless it's truly the same process instance.

### Multiple MCP Hosts

**Scenario:** User runs Claude Desktop and VSCode simultaneously, both using our MCP server.

**Behavior:** Each host gets its own token file (different parent PIDs). Sessions remain independent. ✅ Works correctly.

### Multiple Server Instances from Same Host (Known Limitation)

**Scenario:** A single MCP host spawns multiple instances of our MCP server simultaneously (same parent PID and start time).

**Behavior:** All instances attempt to use the same token file. The first to connect will work, others will conflict. Session resume will likely fail for all instances due to race conditions.

**Status:** Known limitation for v1. This behavior is not explicitly defined in the MCP specification. If it occurs in practice, no instances will successfully resume sessions, but the system remains safe (no incorrect session reuse).

## Logging Strategy

To aid debugging and understanding real-world behavior, log distinct messages for different failure modes:

- `"Token file not found (first run or clean slate)"` - Normal first-run case
- `"Token file corrupted, treating as stale"` - File read failed or content invalid
- `"Could not verify parent process start time, session resume disabled for this instance"` - Startup case where parent info unavailable
- `"Broker rejected resume token, starting fresh session"` - Token rejected by broker
- `"Session resumed successfully"` - Happy path
- `"Failed to write token file: <error>"` - Token persistence failed (non-fatal)

## Design Decisions

### Why Not Use Atomic Rename (temp file + rename)?

**Decision:** Write tokens directly to the final filename without using a temporary file and atomic rename.

**Rationale:** 
- The atomic rename pattern protects against reading partial/corrupted data in the presence of concurrent readers or during crashes
- Our error handling already treats corrupted tokens gracefully: detect corruption → delete file → continue as fresh start
- The added complexity of temp files is not justified given our corruption handling strategy

### Why Plain Text Token Storage?

**Decision:** Store tokens as plain text without encryption or encoding.

**Rationale:**
- Tokens are already secured by filesystem permissions (user-owned directory)
- Adding encryption would require key management, adding complexity without meaningful security benefit in this context
- The broker is responsible for token security properties (expiration, revocation, etc.)

### Why Use Parent Process Identity Instead of Configuration Hash?

**Decision:** Use parent PID + start time rather than hashing MCP host configuration or client info.

**Rationale:**
- `clientInfo` from MCP handshake typically doesn't differentiate between multiple instances of the same host (e.g., two VSCode windows both send `{name: "vscode", version: "1.95.0"}`)
- Parent process identity is guaranteed to be unique per host instance
- Parent start time prevents PID reuse issues
- Simple to implement and understand

## Platform Support

**Supported Platforms:**
- Linux
- macOS

## Security Considerations

- Token files are stored in a user-owned directory with standard filesystem permissions
- No additional encryption is applied (tokens themselves are broker-managed credentials)
- Race conditions in token file access are handled through error detection and recovery, not locks
- Conservative approach to ambiguous cases prevents accidental session hijacking

## Success Criteria

A successful implementation should:
1. ✅ Resume sessions across MCP server restarts when the same host reconnects
2. ✅ Support multiple different MCP hosts simultaneously
3. ✅ Never resume an incorrect session (prefer losing a session over resuming the wrong one)
4. ✅ Handle crashes gracefully without corrupting persistent state
5. ✅ Provide clear logging for debugging edge cases
6. ⚠️ Known limitation: Multiple server instances from the same host may not resume correctly (acceptable for v1, should be documented)
