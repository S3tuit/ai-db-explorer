# AGENTS Guidelines for This Repository

This repository contains a C application named **adbxplorer**.
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

## 2. Testing and Verification

- Every bug fix should include a test or explanation of how the fix was
  verified.
- Use make parallel jobs (until -j6) when building.
- Security-relevant changes (input validation, policy enforcement, secret
  handling) require **extra scrutiny**: describe the threat the code defends
  against.
- If existing tests break, investigate the root cause rather than adjusting
  assertions to pass.

---

## 3. When in Doubt

- **Ask the user** before implementing anything ambiguous.
- Prefer quality and correctness over speed.
- If a request conflicts with the security model (§1), say so explicitly rather
  than silently complying.

---

## 4. Repository Structure

- `src/` contains the application code.
- `meta/` contains the canonical hand-edited JSON inputs for codegen and
  validation.
- `docs/` contains design documentation.
- `tests/unit/` contains focused unit tests.
- `tests/integration/` contains tests that run against a dedicated environment.
- `tests/data/` contains SQL fixtures used by tests.
- `py_utils/` contains repo-specific helpers and generators. Prefer these over
  ad hoc scripts when they match the task.
- `third_party/` contains vendored dependencies.
- `build/` and `dist/` are generated outputs.

## 5. Preferred Helpers and Fast Paths

- Use `python3 py_utils/pg_dump_ast.py ...` when debugging PostgreSQL parse/AST
  behavior.
- Use `python3 py_utils/mcp_run_sql_query.py ...` for fast manual iteration
  against a user's already-running Broker when that is safer and faster than
  wiring a new test. Tell the user when this helper would speed up validation.
- Prefer focused builds and tests over full sweeps:
  - `make -j6 build/tests/unit/test_<name>` to rebuild one unit test.
  - `build/tests/unit/test_<name>` to run a single unit test binary.

