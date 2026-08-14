---
status: ready-for-review
category: cluster-controller
title: PR #38112 review — correctness-under-adversity lens
updated: 2026-08-14
outcome: One blocking finding — migration step pinned to stale dev version (26.38.0-dev.0) will be skipped on the 26.38→26.39 upgrade, panicking catalog open; rest of the change holds up.
---

# Review — PR #38112, lens: correctness under adversity

**Verdict: Request changes.** One blocking correctness bug: the builtin-schema
migration step is pinned to a stale dev version (`26.38.0-dev.0`) while `main`
has already bumped to `26.39.0-dev.0`, so once this PR rebases onto current main
the step is skipped on the `26.38.0 → 26.39.0` release upgrade and catalog open
panics on the `mz_cluster_reconfigurations` fingerprint mismatch. Everything else
in the change holds up: the dimension-handling paths are uniformly
destructure-checked, the SQL diff is NULL-safe, and the `WAIT FOR '0s'` readbacks
are deterministic. Details and the traces I ran below.

---

## Blocking

### B1. Migration step pinned to `26.38.0-dev.0` will be skipped on the 26.38→26.39 upgrade → catalog-open panic
`src/adapter/src/catalog/open/builtin_schema_migration.rs:362-367`

The new step is pinned to `26.38.0-dev.0`:

```rust
MigrationStep::replacement(
    "26.38.0-dev.0",
    CatalogItemType::MaterializedView,
    MZ_INTERNAL_SCHEMA,
    "mz_cluster_reconfigurations",
),
```

The comment two lines up states the invariant explicitly: *"this version must
stay at the workspace's current dev version until the change ships."* The
workspace's current dev version on `main` is **`26.39.0-dev.0`**
(`src/environmentd/Cargo.toml` on `main`), not `26.38.0-dev.0` — the PR branch was
cut before the `v26.39.0-dev.0` bump (commit `8c3d47f71c`) and still carries
`26.38.0-dev.0`. The pin matches the *branch's* version, but it will not match the
tree it merges into.

Concrete failure sequence, once rebased onto current main:

1. `mz_cluster_reconfigurations` already shipped in the **26.38.0 release** with
   the four-dimension `changes` diff (introduced by #37628, which predates the
   26.39 bump). Its durable shard therefore carries the *old* fingerprint
   `F_old`.
2. This PR redefines the MV (`changes` now emits `arrangement_compression`),
   giving the builtin fingerprint `F_new`. The change first appears in a
   **26.39** build.
3. A production env upgrades `26.38.0 → 26.39.0`. At catalog open,
   `get_migration_version` returns `source_version = 26.38.0`, `build_version =
   26.39.0`.
4. `plan_migration` keeps only steps with `s.version > source_version`
   (`builtin_schema_migration.rs:726`). In semver a pre-release orders *below* its
   release, so `26.38.0-dev.0 < 26.38.0` and the step's `26.38.0-dev.0 > 26.38.0`
   is **false**. The step is filtered out.
5. The MV is not replaced, so the durable shard keeps `F_old` while the build
   expects `F_new`. The fingerprint check at catalog open fails and
   `environmentd` panics — the environment cannot boot.

The wrong outcome: a boot-time crash on a real release-upgrade path, i.e. a
failed 0dt upgrade, not a soft degradation.

The same argument shows the *correct* pin is `26.39.0-dev.0`: `26.39.0-dev.0 >
26.38.0` and `26.39.0-dev.0 <= 26.39.0`, so the step fires and the shard is
replaced. (This is exactly why `main`'s existing `26.38.0-dev.0` steps are
correct: they migrate the `26.37.0 → 26.38.0` boundary, where they *do* fall in
the half-open interval.)

**Fix:** bump the step to `26.39.0-dev.0` when rebasing onto current main, per the
step's own comment. Note this cannot be verified from the PR branch in isolation
(there the pin is self-consistent); it is a rebase hazard that bites on merge, so
it needs a human to confirm the final merged version.

---

## Should-fix

None. The remaining shape-dimension surfaces are all handled correctly (see the
verification notes) or explicitly deferred (see N1).

---

## Nits / context (not defects)

### N1. Observability surfaces now disagree on compression, but the PR flags it
The PR routes `arrangement_compression` into `mz_cluster_reconfigurations.changes`
(`src/catalog/src/builtin/mz_internal.rs:957-959`) and the `SHOW CLUSTERS`
activity summary (`mz_internal.rs:5657`), but the audit-event reconfiguration
details still omit it. The PR body calls this out as an intentional, deferred gap,
so it is not a blocker — noting only that the three surfaces are now inconsistent
until the follow-up lands.

### N2. `changes` diff relies on both JSON sides always carrying the key
`mz_internal.rs:957` compares `r.target->'arrangement_compression' !=
r.config->'arrangement_compression'`. If either operand were JSON-absent the `!=`
yields SQL NULL and the `CASE` takes the `ELSE '{}'` branch (no spurious diff).
Both sides always serialize the field (`ClusterVariantManaged.arrangement_compression`
and `ReconfigurationTarget.arrangement_compression` are non-optional `bool`), so
this degrades safely even for any legacy in-progress record. No action needed;
recorded because it is the one non-compile-checked surface in the change.

---

## Verification notes (suspicions chased to a verdict)

These are the adversity angles I checked that turned out **fine**, so the blocking
list stays honest:

- **Reset symmetry (the actual fix).** `reshape_alter_cluster_managed`
  (`cluster.rs:685,713`) now resets the shape via
  `realized_reconfiguration_target()` / `apply_reconfiguration_target()`. Both
  methods (`catalog/src/memory/objects.rs:3421,3446`) destructure
  `ClusterVariantManaged` / `ReconfigurationTarget` with no `..`, so a dropped
  dimension is a compile error. The reset resets exactly the five shape
  dimensions and leaves non-shape fields (`workload_class`, `schedule`,
  `auto_scaling_strategy`, `optimizer_feature_overrides`) untouched — matching the
  cut-over that re-applies them. The `cc_preserve` td test exercises the leak
  regression directly (realized stays `false`, only the overlap replica bounces).

- **Fold under an in-flight record.** `alter_reconfiguration_target` /
  `fold_reconfiguration_target` (`cluster.rs:2220,2298`) and
  `ReconfigurationDimensionsUnchanged` all carry `arrangement_compression`, so a
  compression fold onto an in-flight size reconfiguration keeps the in-flight size
  and re-targets only compression. Traced the folding compression-only ALTER and
  the cancel-back-to-realized case; both classify correctly via
  `matches_realized_config` (`objects.rs:3681`, whole-struct `==`, includes
  compression).

- **Async (background) cut-over.** With `enable_background_alter_cluster` on
  (the slt/default) the reshape returns before cut-over; the overlay that later
  advances the realized config (`coord/cluster_controller.rs:813-836`,
  `StateWrite`) destructures exhaustively and applies
  `new_arrangement_compression`. Not forgotten.

- **slt readback determinism.** The `managed_cluster.slt` readbacks use
  `WAIT FOR '0s'`, which `requests_immediate_cut_over` (`cluster.rs:2166`) maps to
  the synchronous direct cut-over path (`cluster.rs:368-398`, applying the target
  in the ALTER's own transaction), so the non-retrying slt `query` cannot race the
  controller even with background alter enabled. The final `RESET (...)` has no
  readback, consistent with the comment that RESET cannot carry a `WAIT`.

- **Routing.** `alter_changes_replica_shape` (`cluster.rs:2184`) and
  `replica_config_shape` (`objects.rs:3404`) both include compression, so a
  compression-only ALTER (or RESET-to-default no-op) routes correctly whether or
  not a reconfiguration is in flight.
