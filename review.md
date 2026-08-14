---
status: ready-for-review
category: review
title: "PR #38112 review — engineering-quality lens"
updated: 2026-08-14
outcome: "Reviewed; 1 merge-gate (stale migration version vs main), 2 nits, fix closes the bug class cleanly."
---

# PR #38112 — don't leak arrangement compression into the realized config

**Lens: engineering quality** (tests that would fail, seams respected, size/shape,
docs-in-the-same-change, comments that rot). Correctness-under-failure and dataflow
semantics are other reviewers' lenses.

**Verdict:** Solid, minimal, well-tested change that closes a bug *class*, not just an
instance. One real merge-time hazard (the builtin-schema-migration version is already
behind `main`) worth gating on. Two low-priority nits. No blocking issue in the code as
it sits on its branch.

---

## Should-fix (merge gate)

### 1. Migration step pinned to `26.38.0-dev.0` is already behind `main` (`26.39.0-dev.0`)
`src/adapter/src/catalog/open/builtin_schema_migration.rs:363`

The new `MigrationStep::replacement("26.38.0-dev.0", …, "mz_cluster_reconfigurations")`
matches this branch's workspace version (`src/environmentd/Cargo.toml:4` → `26.38.0-dev.0`),
so it is self-consistent *on the branch*. But `upstream/main` has already bumped to
`26.39.0-dev.0` (commit `8c3d47f71c`). The NOTE the comment points at
(`builtin_schema_migration.rs:277`) spells out the failure precisely: "a step pinned to an
older dev version is skipped when upgrading from that release onward, and the fingerprint
check then panics at catalog open."

Concretely: once this stack rebases onto current `main`, `environmentd`'s version becomes
`26.39.0-dev.0`, the `mz_cluster_reconfigurations` MV fingerprint changes (the `changes`
diff gained a clause), but the migration at `26.38.0-dev.0 < 26.39.0-dev.0` does **not**
fire for any catalog already written at `26.39.0-dev.0` → fingerprint mismatch panic at
open. What a maintainer loses: a green PR CI (fresh catalog, branch still at 26.38) that
still bricks every existing `26.39` dev environment on restart.

Fix: bump the step to the workspace's dev version at merge time (currently `26.39.0-dev.0`),
and confirm via the upgrade nightly rather than default PR CI — per repo practice, only the
upgrade/full nightly exercises cross-version catalog open. I could not tell from
`gh pr checks` whether the upgrade nightly ran for this PR; recommend triggering it.

---

## Nits

### 2. The SQL `changes`/summary lists are the un-compile-checked twins of the Rust seams
`src/catalog/src/builtin/mz_internal.rs:957-958` (the `changes` diff),
`src/catalog/src/builtin/mz_internal.rs:5657` (`mz_show_clusters` summary)

The Rust side is guarded well: `apply_reconfiguration_target` /
`realized_reconfiguration_target` / the `From` impls all destructure `ReconfigurationTarget`
with **no** `..`, so a new dimension is a compile error until every site handles it
(`src/catalog/src/memory/objects.rs:3421,3446,3686`). These two SQL expressions are the
exact opposite: hand-maintained `CASE WHEN … arrangement_compression …` clauses that no
compiler checks, and they are *what this PR is fixing an omission in*. Nothing stops the
next dimension from being forgotten here again. A one-line
`-- keep in sync with ReconfigurationTarget dimensions` at both SQL sites (or a testdrive
assertion that a change in each dimension surfaces in `changes`) would convert "remember to
edit five places" into "the test tells you". Low priority; the PR follows the existing
pattern faithfully, this is a pre-existing structural smell it happens to touch.

### 3. The async RESET path lost its only readback
`test/sqllogictest/managed_cluster.slt:474-478`

The old test read back the value after `ALTER … RESET (EXPERIMENTAL ARRANGEMENT
COMPRESSION)`; the rewrite correctly stops relying on the (now-fixed) leak and downgrades
the `RESET` to "acceptance only" (`RESET` can't carry `WITH (WAIT …)` — confirmed against
`src/sql-parser/src/parser.rs:5943`, only the `SET` arm parses `with_options`). Net effect:
no test anywhere asserts that a compression **RESET** actually lands `false` in the realized
config end to end (testdrive `cc_compression` only exercises `SET … = true`). Risk is low —
`RESET` and `SET` share the same reshape/record path, differing only in the target value —
but if you want the coverage, testdrive can await the WAIT-less async RESET where SLT
structurally cannot. Optional.

---

## Positives worth calling out

- **The fix closes the bug class, not the instance.** `reshape_alter_cluster_managed`
  stopped hand-listing `size / replication_factor / availability_zones / logging` (which is
  how compression got missed) and now routes through
  `realized_reconfiguration_target()` + `apply_reconfiguration_target()`
  (`cluster.rs:685,713`). Those helpers destructure without `..`, so a future dimension can
  no longer be dropped at this site — it won't compile. This is the smallest change that
  also makes the same mistake impossible again. Exactly the seam extension the lens rewards.

- **Tests would genuinely fail without the fix.**
  - `test/testdrive/cluster-controller.td` `cc_preserve`: asserts the realized config still
    renders `… COMPRESSION = false` after the fold and that the *baseline* replica id is
    unchanged. Under the old leak both flip, so both assertions fail — a real regression
    guard, not happy-path coverage.
  - `test/sqllogictest/managed_cluster.slt:452`: `ALTER … SET (EXPERIMENTAL ARRANGEMENT
    COMPRESSION = false) WITH (WAIT FOR '0s')` is `statement ok`; before the planner-gate
    change (`ddl.rs:6553`) this errored, so the line fails without the gate fix.

- **A rotting comment was corrected in-flight.** `cluster.rs:294` went from "one of the
  **four** dimensions" to "one of the dimensions" — accurate now that
  `ReconfigurationTarget` carries five. The kind of comment-vs-code drift the method hunts
  for, fixed rather than left.

- **Docs moved with the code.** `mz_internal.md`, the builtin column description
  (`mz_internal.rs:879`), and the autogenerated `mz_internal.slt` all name
  `arrangement_compression` in the same change; the `catalog_server_explain.slt` golden was
  regenerated consistently.

---

## Verification trail

| claim chased | how resolved | verdict |
| --- | --- | --- |
| Do `realized_reconfiguration_target` / `apply_reconfiguration_target` cover *all* dimensions incl. compression? | Read `objects.rs:3421-3461`; both destructure `ReconfigurationTarget` (5 fields, no `..`). | safe — compile-checked seam |
| Are there other hand-maintained dimension lists still missing compression? | Grepped `arrangement_compression` across adapter/cluster-controller/catalog; `fold_reconfiguration_target`, `requests_config_shape_change` (`cluster.rs:2190,2203`), `matches_realized_config` (`objects.rs:3681`) all already include it. Only the reshape reset, WAIT gate, `changes` diff, and `SHOW CLUSTERS` summary needed fixing — all four fixed. | scope complete |
| Does the WAIT gate correctly *not* include REPLICATION FACTOR? | `ddl.rs:6548-6559` omits `replication_factor`; `managed_cluster.slt` still asserts an RF-only WAIT errors. RF-only change needs no hydrate-overlap. | correct, consistent |
| Is the RESET-takes-no-WITH comment true? | `parser.rs:5943` (RESET) parses only `ResetOptions(names)`; only `SET` (5965) parses `with_options`. | comment accurate |
| Does the testdrive actually catch the leak? | `cc_preserve` asserts `SHOW CREATE … = false` and baseline replica id unchanged; both invert under the old leak. | genuine regression test |
| Is the migration version safe? | Branch env version `26.38.0-dev.0` == step version, but `upstream/main` is `26.39.0-dev.0`; NOTE at `:277` documents the skip-and-panic failure. | Finding #1 (merge gate) |
| Is the migration `Replacement` (not `Evolution`) right? | MV SQL fingerprint changed; replacement discards data but an MV recomputes, so no loss. | correct |
| PR-description "known gaps" (audit event omits compression; RESET can't carry WAIT) vs code | Both confirmed against `parser.rs` and the sequencer; intentionally deferred. | accurate, out of scope |
