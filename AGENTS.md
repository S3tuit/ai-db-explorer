# AGENTS Guidelines for This Repository

This repository contains a C application named **ai-db-explorer**.
An overview of the architecture can be found at `./docs/sys_overview.md`.

Agents working on this repository **must** read this file in full before making
any changes, and must respect the constraints below.

---

## 1. Security Model (non-negotiable)

The following invariants **must not** be violated under any circumstances:

- The **Broker** is the primary security boundary.
- The **MCP server is untrusted** and must be treated as potentially malicious.
- Secrets (DB passwords, vault material, decrypted credentials) **must never**
  leave the Broker process.
- Safety policies are enforced **centrally**; they must not be duplicated,
  weakened, or bypassed.
- All input (JSON, SQL, tokens, parameters) must be treated as **hostile** and
  validated before use.
- No feature may weaken isolation, policy enforcement, or auditing guarantees.

If a proposed change makes any of these properties unclear, it must be
**explicitly discussed with the user** before implementation.

---

## 2. Simplicity and Scope Control

Prefer:

- Simple, explicit designs over clever abstractions.
- Deep modules that hide complexity behind narrow interfaces.
- Conservative defaults that fail closed.

Avoid:

- Shortcuts that feel more like hacks.
- Speculative features or premature generalization.
- "Temporary" bypasses of validation or policy (these are never temporary).
- Expanding scope beyond the explicit request.

If a change increases complexity or expands trust boundaries, explain **why**
it is necessary and get confirmation before proceeding.

---

## 3. Testing and Verification

- Every bug fix should include a test or explanation of how the fix was
  verified.
- Security-relevant changes (input validation, policy enforcement, secret
  handling) require **extra scrutiny**: describe the threat the code defends
  against.
- If existing tests break, investigate the root cause rather than adjusting
  assertions to pass.

---

## 4. Documentation and Code Style

Every non-trivial function **must** be preceded by a comment that explains:

1. **What** the function does (high-level purpose).
2. **Ownership**: who allocates and frees memory (omit if no allocation).
3. **Side effects**: I/O, global state, security-relevant behavior (omit if
   pure).
4. **Error semantics**: return conventions (`OK` / `ERR` / `YES` / `NO`) and
   what each means for this function.

Public functions inside .c files should not be preceded by a comment since the
documentation is already available in the .h file.

These are two examples of well-documented functions:

```c
/* Compares the arrays 'a' and 'b' of 'len' size in constant-time (to avoid
 * timing attacks). It borrows 'a' and 'b'.
 * Returns YES when arrays are equal, NO when different, ERR on invalid input.
 */
static int bytes_equal_ct(const uint8_t *a, const uint8_t *b, size_t len) {
    if (!a || !b)
        return ERR;
    // XOR/OR accumulation prevents early-exit timing leaks.
    uint8_t diff = 0;
    for (size_t i = 0; i < len; i++) {
        diff |= (uint8_t)(a[i] ^ b[i]);
    }
    return (diff == 0) ? YES : NO;
}

/* Sets the receive timeout on a socket file descriptor. It borrows 'fd'.
 * Side effects: updates SO_RCVTIMEO kernel state for the socket.
 * Returns OK on success, ERR on invalid input or setsockopt failure.
 */
static int broker_set_rcv_timeout_sec(int fd, int sec) {
    if (fd < 0 || sec < 0)
        return ERR;

    struct timeval tv = {.tv_sec = sec, .tv_usec = 0};
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0)
        return ERR;
    return OK;
}
```

Inside function bodies:

- Comment on **why** decisions are made, not what the code literally does.
- Pay special attention to safety, validation, and resource management.

Ambiguous ownership or error behavior is considered a **bug**.

**Code formatting**: agents should not worry about indentation, braces, or
other stylistic concerns. The user will apply `.clang-format` separately.

---

## 5. Change Discipline

- Keep changes focused: one logical change per patch.
- Do not refactor unrelated code alongside a feature or fix.

---

## 6. When in Doubt

- **Ask the user** before implementing anything ambiguous.
- Prefer quality and correctness over speed.
- If a request conflicts with the security model (§1), say so explicitly rather
  than silently complying.
