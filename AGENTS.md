# AGENTS Guidelines for This Repository

This repository contains a C application named **ai-db-explorer**.

ai-db-explorer is a lightweight MCP server designed around a **strict threat model**:
even a malicious, buggy, or overly clever agent **must not** be able to damage
databases, bypass safety policies, or exfiltrate sensitive data if the system
is used as intended.

Safety is enforced **at execution time**, not by trusting the agent, the MCP host,
or the input data.

Agents working on this repository must respect the constraints below.

---

## 1. Security model (non-negotiable)

The following invariants MUST NOT be violated:

- The **Broker** is the primary security boundary.
- The **MCP server is untrusted** and must be treated as potentially malicious.
- Secrets (DB passwords, vault material, decrypted credentials) **must never**
  leave the Broker process.
- Safety policies are enforced **centrally**, not duplicated or bypassed.
- Input (JSON, SQL, tokens, parameters) must be treated as **hostile**.
- No feature may weaken isolation, policy enforcement, or auditing guarantees.

If a proposed change makes these properties unclear, it must be rejected or
explicitly discussed with the user before implementation.

---

## 2. Simplicity and scope control

Prefer:
- simple, explicit designs
- deep modules that hide a lot of complexity
- conservative defaults

Avoid:
- shortcuts that feel more like "hacks"
- speculative features
- “temporary” bypasses of validation or policy
- expanding scope beyond the explicit request

If a change increases complexity or expands trust boundaries, explain why
and get confirmation before proceeding.

---

## 3. No implicit trust

Agents must NOT assume:
- the MCP server behaves correctly
- inputs are well-formed
- callers respect contracts
- configuration files are safe
- local IPC peers are friendly

All assumptions must be validated or enforced by code.

---

## 4. Documentation and code style

Every non-trivial function MUST be preceded by a comment that explains:

- **What** the function does (high level)
- **Ownership rules** (who allocates/frees memory, can be ignored if does not allocate anything)
- **Side effects** (I/O, global state, security-relevant behavior, can be ignored if no side effects)
- **Error semantics** (OK / ERR / YES / NO and what they mean)

Inside function bodies:
- comment on **why** decisions are made
- especially when related to safety, validation, or resource management

Ambiguous ownership or error behavior is considered a bug.

---

## 5. When in doubt

When unsure:
- choose the safer behavior
- reject or narrow input
- return an error rather than guessing
- prefer explicit failure over silent acceptance

If uncertainty affects security boundaries, ask the user before implementing.

