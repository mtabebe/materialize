---
status: ready-for-review
category: review
title: "PR #38112 review — correctness-under-adversity lens"
updated: 2026-08-14
outcome: "No blocking/should-fix correctness bug; PR is a net robustness improvement. Two documented gaps noted (N1 rebase-fragile migration version, N2 acknowledged audit omission)."
---

# PR #38112 — adapter: don't leak arrangement compression into the realized config

**Lens: correctness under adversity** (invariants, partial failure, concurrency/ordering,
error paths, retries). One lens of a review panel; other reviewers cover style/perf/structure.

## Verdict

No blocking or should-fix correctness bug found. Through this lens the change is a net
robustness improvement: it deletes the one hand-maintained dimension list at the reshape
site and routes the shape reset through `realized_reconfiguration_target()` /
`apply_reconfiguration_target()`, both destructured so a future shape dimension is a compile
error until it is handled. The behavioral delta versus `main` is exactly one dimension
(arrangement compression is now *deferred to cut-over* instead of *leaked into the realized
config immediately*); every other dimension is handled identically to before. CI is green
(buildkite/test #131067, including the cluster-* and checks-parallel jobs that exercise the
new testdrive coverage at runtime).

Two intentional, documented gaps are noted below so a reader does not mistake them for
oversights.

## Reading order

| # | file | why here |
|---|------|----------|
| 1 | `src/adapter-types/src/cluster_state.rs` | `ReconfigurationTarget` (the 5 shape dims) — vocabulary |
| 2 | `src/catalog/src/memory/objects.rs` | `realized_reconfiguration_target` / `apply_reconfiguration_target` / `matches_realized_config` — the helpers the fix leans on |
| 3 | `src/adapter/src/coord/sequencer/inner/cluster.rs` | `reshape_alter_cluster_managed` — the leak site; `fold_reconfiguration_target` (cut-over) for context |
| 4 | `src/sql/src/plan/statement/ddl.rs` | the `WAIT` planner gate |
| 5 | `src/catalog/src/builtin/mz_internal.rs` | `changes` diff + `SHOW CLUSTERS` summary SQL |
| 6 | `src/adapter/src/catalog/open/builtin_schema_migration.rs` | MV replacement migration step |
| 7 | `src/adapter/src/catalog/transact.rs` | audit-details construction (the acknowledged gap) |
| 8 | tests + docs |

## Blocking

None.

## Should-fix

None.

## Nits / watch items

### N1. Migration-step version is correct but rebase-fragile
`src/adapter/src/catalog/open/builtin_schema_migration.rs:362-367`

The `Replacement` step is tagged `"26.38.0-dev.0"`, which matches the workspace version on
this branch (`src/environmentd/Cargo.toml:4` is `26.38.0-dev.0`), so the
`step.version <= target_version` assertion at line 675 holds and the step fires for any env
upgrading from an earlier version. The inline comment already states the constraint ("this
version must stay at the workspace's current dev version until the change ships").

Watch item, not a defect: `main` has since bumped to `26.39.0-dev.0` (commit
`8c3d47f71c`). On rebase past that bump the tag must be bumped to `26.39.0-dev.0` as well.
If it is left at `26.38.0-dev.0`, a dev environment already at `26.38.0-dev.0` (source ==
that version) would skip the step (the filter is `step.version > source_version` at
line 726) and keep the old MV shard schema against the new MV definition. The established
repo convention (every recent step is tagged at its then-current `-dev.0`) makes this the
author's responsibility on rebase; flagging so the synthesis step keeps an eye on it.

### N2. Audit payload intentionally omits compression (documented known gap)
`src/adapter/src/catalog/transact.rs:588-597`

`reconfiguration_audit_details` destructures the target with `arrangement_compression: _`
and a comment explaining the omission. `AlterClusterReconfigurationV1` is an append-only
audit-log payload, so adding a field is a schema-versioned change; the PR body flags it as a
follow-up. This is observability-only and does not affect convergence, cut-over, or the
`changes` MV. Correct to defer; noted only so it is not read as a missed dimension list of
the same class the PR is fixing.

## Positives

- `apply_reconfiguration_target` / `realized_reconfiguration_target` (memory/objects.rs:3421,
  3446) and the `From`/`matches_realized_config` impls all destructure `ReconfigurationTarget`
  and `ClusterVariantManaged` with no `..`, so the next shape dimension cannot be silently
  dropped at any of these sites. This is precisely the fix for the class of bug the PR
  addresses, applied structurally rather than by adding one more hand-listed field.
- `matches_realized_config` (memory/objects.rs:3681) delegates to
  `realized_reconfiguration_target()`, so the cancel-vs-start decision at
  `cluster.rs:689` automatically includes compression: a compression-only `ALTER` back to the
  realized value is correctly classified as a cancel, and a real compression change as
  in-progress, with no separate list to keep in sync.
- The new `cluster-controller.td` `cc_compression` section exercises the invariant at
  runtime end to end (overlap replica provisioned at the flipped shape, cut-over advances the
  realized config, baseline retired, `changes` returns to `{}`), and the `cc_preserve`
  section pins the fixed behavior (folded compression lands in the record target, realized
  config untouched, only the overlap replica bounced). These are load-bearing, not vacuous.

## Verification trail

| claim chased | how resolved | verdict |
|---|---|---|
| Does the shape reset now cover exactly the 5 dims, no more/less? | `apply_reconfiguration_target` (objects.rs:3446) sets size/rf/az/logging/compression and leaves optimizer_overrides/schedule/auto_scaling/reconfiguration/burst untouched; destructured so it must. Matches the intent comment at cluster.rs:671-677. | safe |
| Is the cut-over (`fold_reconfiguration_target`) also compression-aware, or does the fix defer to a fold that drops it? | cluster.rs:2334-2338 folds `arrangement_compression` via `unchanged.arrangement_compression`, computed at 2276. Pre-existing on the branch (stacked #38104). Cut-over applies compression. | safe |
| Could `changes` SQL misbehave when a JSON side lacks `arrangement_compression` (NULL `!=`)? | Both `durable::ClusterVariantManaged` and `durable::ReconfigurationTarget` carry `arrangement_compression: bool` as a required sibling field (durable/objects.rs). Both JSON sides always have the key; `true != false` is well-defined. The `cc_compression` testdrive reads `changes->>'arrangement_compression'` at runtime and CI is green, confirming the key resolves. | safe |
| Does the `WAIT` gate wrongly admit/reject compression, and is replication_factor's exclusion a bug? | ddl.rs:6548-6559 admits WAIT when any of SIZE/AZ/INTROSPECTION/COMPRESSION is present. Compression is part of `ReplicaShape` (cluster_state.rs:211) so it has a hydrate-overlap → correctly admitted. RF is *not* in `ReplicaShape` (cluster-level), so RF+WAIT stays rejected by design; the slt at managed_cluster.slt:224 asserts that rejection. | safe |
| Any other hand-maintained dimension list in the reshape path still omitting compression? | `grep` for per-field `.size =`/`.replication_factor =`/… assignments in cluster.rs now returns nothing (the fix removed the only one). Routing/overlap decisions go through `ReplicaShape`, which includes compression. | safe |
| Fold onto an in-flight size record: does compression leak or clobber? | Traced cc_preserve: new_config{size=realized, compression=true} → target folded to {size=prev workers=4, compression=true}; `apply_reconfiguration_target(realized)` resets realized to {size=workers=1, compression=false}; record carries the folded target. `changes` = {size, arrangement_compression}; SHOW CREATE still false. Matches the test assertions. | safe → confirms fix |
| Partial failure between record write and cut-over | The record write is a single `catalog_transact` (cluster.rs:716). Realized shape + `reconfiguration` record are committed atomically; a crash before the async cut-over just leaves the in-flight record, which the controller resumes. No new intermediate write introduced by this PR. | safe |
| Migration-step version vs assertion/filter | step `26.38.0-dev.0` == branch build version → `<= target` holds; fires for earlier sources. Rebase past the `26.39.0-dev.0` bump requires bumping the tag (see N1). | doc/watch |
| Audit omission a convergence bug? | transact.rs:596 `arrangement_compression: _` is observability-only on an append-only payload; documented follow-up. | doc-only, acknowledged |
