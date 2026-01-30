---
name: materialize-pr-review
description: Perform local PR review on Materialize by inspecting the git diff and running style/tool checks locally. Use when reviewing changes locally, doing a code review on Materialize code, or when the user asks for a PR review against Materialize standards.
---

# Materialize PR Review (local)

Perform a **local** review: inspect the git diff and run style/tool checks yourself. Do not rely on CI; run the commands below and report what you find.

## How to perform the review

1. **Inspect the diff.** Use the staged diff (`git diff --staged`) or the diff against local-dev-main (`git diff local-dev-main`) to see what changed. Review only the changed files and lines.
2. **Apply the checklists below** to the diff: tests, code style, architecture, polish, release notes.

When reviewing a Materialize PR, apply the standards in CONTRIBUTING.md, doc/developer/guide-changes.md, doc/developer/guide-testing.md, and doc/developer/guide.md.

## Review mindset

- **Review the code, not the author.** Explain the *why* behind suggestions.
- **Err on more feedback** rather than less, even if it slows things short-term.
- **Push for documentation:** if something is unclear and you need an explanation, the code should be documented or rewritten—not only explained in the PR.
- **Favor approving** once the PR clearly improves the codebase, even if it isn’t perfect. Aim for continuous improvement, not perfection on every diff.

Use **nit:** for preferences where reasonable people could disagree; reserve blocking comments for real issues.

## Pre-merge checklist

Verify before approving:

- [ ] **One semantic change per review unit.** The PR (or each commit, if semantic commits) does one thing. No “do X and do Y and do Z”; no bugfix + feature in the same RU. If it spans multiple CODEOWNERS areas (e.g. sql-parser + sql planner), consider asking for a split.
- [ ] **Adequate testing.** Every behavior change has at least one new or modified test. No behavior change with zero test file changes. Prefer observable behavior (sqllogictest, testdrive) over brittle unit tests of stateful internals; see guide-testing.md.
- [ ] **Release notes.** Any user-visible change to stable APIs (SQL syntax/semantics, source/sink behavior, documented CLI flags) has a release note in the PR description (imperative, “This release will…”, link to docs). Experimental/unstable API changes do *not* get release notes until stabilization.
- [ ] **Documentation.** User-facing features have or coordinate on user docs (A-docs). Code has enough docs that a reviewer could read a short precis before reading the code.

## Change description and polish

- **Description:** First line is a short imperative summary (~72 chars). Body explains rationale, alternatives, future work; optional links to issues/benchmarks. “Fix bug” with no context is insufficient.
- **Polish:** No `// XXX`, commented-out code, or stray `println!`/debug code. No obvious dead code or unrelated blank-line churn.
- **Size:** Under ~500 lines is easier to review; over ~1000 often warrants a request to split. Exceptions: generated code, large declarative tests (slt/td), or prose-only docs. If in doubt, smaller PRs are better.

**What to look for (polish):**
- Leftover TODOs, `// FIXME`, `dbg!`, or `println!`.
- Commented-out blocks that should be removed.
- Unrelated formatting or blank-line changes in untouched code.
- New public items (functions, types, modules) without doc comments.

## Software engineering principles

Check that the change aligns with guide-changes.md “Software engineering principles.” See **What to look for: Architecture and design** and **What to look for: Code style** for concrete checkpoints. In short:

- **Simplicity:** No incidental complexity; simplify redundant logic.
- **Avoid special casing:** Prefer composable design over extra booleans/branches.
- **Encapsulation:** Right crate/module (e.g. sql-parser = grammar only; sql = semantics).
- **Dependencies:** Justified, preferably well-maintained.
- **Documentation:** Public behavior and non-obvious design documented so reviewers can judge without reading every line.

## What to look for: Tests

**Coverage**
- Every behavior change has at least one new or modified test. No change to SQL semantics, sources, sinks, or CLI behavior with zero test file changes.
- New SQL/query behavior: look for sqllogictest (`.slt`) in `test/sqllogictest/` or testdrive (`.td`).
- New or changed data types / wire behavior: look for testdrive or pgtest to cover pgwire serialization.
- Pure, stable logic (e.g. decoding, pure functions): unit/integration tests in crate `tests/` or `mod tests` are appropriate.

**Test style (guide-testing.md, style.md)**
- Prefer testing **observable behavior** (SQL results, wire protocol) over implementation details; over-testing internals makes refactors painful.
- Rust tests: use `#[mz_ore::test]` (or `#[mz_ore::test(tokio::test)]` for async) so logging is initialized; prefer panics over `Result` in tests for better backtraces.
- sqllogictest: prefer Materialize-specific `.slt` in `test/sqllogictest/`; avoid modifying upstream SQLite/CockroachDB test files.
- testdrive: `.td` files; copyright header, no trailing spaces/newlines — run `bin/lint` to check.

**Red flags**
- Behavior change with no test changes.
- New unit tests that mock many stateful dependencies for mid-stack code; suggest system tests instead.
- Tests that assert on internal structure (e.g. exact plan shape) unless that’s the feature under test; prefer outcome-based assertions.

---

## What to look for: Code style

**Rust (style.md, guide.md)**
- **Run locally and report:** `cargo fmt --check`, `cargo clippy --workspace --all-targets`, `bin/lint`. If any fail, note the failures and suggest fixes (e.g. run `cargo fmt`, address clippy lints).
- **Imports:** Group order `std` → external crates (including `mz_*`) → `crate`; one `use` per module; `crate::` not `super::` for crate-local imports.
- **Errors:** Structured errors with `thiserror`; no bare `anyhow!("...")`. `Display` should not print the full error chain; use `ResultExt`/`ErrorExt` (e.g. `display_with_causes`) when logging or surfacing to users.
- **Async/tasks:** Use `ore::task::spawn` / `spawn_blocking` (or `RuntimeExt::spawn_named` / `spawn_blocking_named`) instead of raw `tokio::spawn` so task names show in tokio-console.
- **Tests:** `#[mz_ore::test]`; panic in tests rather than returning `Result`.

**SQL (style.md)**
- Keywords capitalized (`SELECT`, `FROM`, `WHERE`, `CREATE TABLE`); identifiers and function names lowercase (`integer`, `varchar`, `coalesce`). Type constructors like `DATE '...'` capitalize the type.
- Spacing: no space between function name and `(`; space after keywords before `(`; spaces around operators; space after commas.

**Error messages (style.md)**
- Primary: short, factual, lowercase first letter, no trailing punctuation.
- Detail/hint: complete sentences, capitalized, period; hint actionable (e.g. “Try using CAST in the CREATE SOURCE statement”).
- No formatting (e.g. newlines) in message text; use quotes for identifiers/file names; avoid “unable”, “bad”, “illegal”, “unknown”; say what kind of object (e.g. “table”) when naming it.

**Logging (style.md)**
- Use `tracing` macros; sentence fragment style (no leading cap, no period).
- Prefer structured fields for searchable values: `info!(shard_id = ?id, "message")`.
- Level semantics: `ERROR` = data loss/corruption/invariant violation (sparingly; consider `panic!`); `WARN` = recoverable or uncertain; `INFO` = normal status changes; `DEBUG`/`TRACE` for noisier detail.

**System catalog / SQL functions (style.md)**
- New catalog relations: normalized schema, `interval` for durations (not `*_ms` except nanosecond), kebab-case enum values, `_at` for timestamps, no `is_` prefix on booleans, pluralized relation names (e.g. `mz_sources`).
- Internal-only functions: `mz_` prefix; match PostgreSQL names/args when present.

---

## What to look for: Architecture and design

**Simplicity (guide-changes.md)**
- Code is as simple as it can be; no incidental complexity. Redundant conditionals (e.g. `if x { true } else { false }` → `x`) should be simplified.
- “Getting tests to pass is half the battle”; ask whether the change could be simpler or better documented.

**Special casing**
- No proliferation of boolean/option parameters or feature-specific branches that could be a composable design. If the interface is growing many “if type X then…” branches, suggest a cleaner abstraction.

**Encapsulation**
- Logic lives in the right crate/module. **sql-parser:** grammar only; no semantic or policy validation (e.g. no rejecting `LIMIT 'foo'` in the parser—that belongs in `sql` with coercion). **sql:** planning, coercion, catalog semantics.
- New code in “the easy place” is often wrong; check whether a new module or different crate would give clearer boundaries.

**Dependencies**
- New crates must be justified; prefer well-maintained (e.g. Tokio, reqwest). Unmaintained deps often become team-maintained forks (e.g. rust-rdkafka, rust-prometheus).

**Subsystem-specific (guide-changes.md “Mentoring”)**
- **dataflow:** allocation-sensitive hot loops; avoid unnecessary allocations.
- **sql:** less allocation-sensitive; clarity and correctness first.
- **coord / persist / compute:** complex, stateful; extra care for correctness and clarity; called out in CONTRIBUTING as sensitive for external contributors.

---

## External contributors (CONTRIBUTING.md)

- **Landing large changes:** Requires coordination with technical writers for user docs and a release note in doc/user/release-notes.md.
- **Sensitive areas:** Coordinator, Persist, Compute are called out as less suited for external contributions; extra care in review there.

## Summary for the author

When leaving the review:

1. **Blocking:** Must fix before merge (correctness, missing tests for behavior changes, missing release notes for user-visible changes, serious simplicity/encapsulation violations).
2. **Strong suggestions:** Should fix unless there’s a good reason (e.g. docs, clarity, test quality).
3. **nits:** Optional improvements.

If the PR improves overall codebase health and blocking items are addressed, approve even if nits remain.
