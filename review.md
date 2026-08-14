---
status: ready-for-review
category: review
title: "PR #38112 review — dataflow semantics & cost lens"
updated: 2026-08-14
---

# PR #38112 — arrangement compression leak into the realized config

**Lens: dataflow semantics and cost.** Is this right, and affordable, as
incremental dataflow? Other reviewers cover correctness-under-failure and
general engineering quality; I do not duplicate them.

## Verdict

**No blocking or should-fix findings in this lens.** The only genuine dataflow
artifact touched is the `mz_cluster_reconfigurations` builtin materialized view.
The change to it is a stateless per-row map that adds one JSON comparison per
record, consolidates correctly, and settles to `{}` on cut-over exactly like the
four dimensions already present. The rest of the diff is adapter Rust
config-plumbing (the reshape reset) and non-materialized view / SQL-surface
text, none of which maintains incremental state.

Notably, the core fix *removes* a real cost cliff rather than adding one (see
Positives): the compression leak was bouncing baseline replicas
mid-reconfiguration, which is a full dataflow rehydration of every object on
that replica.

## Reading order (dataflow-relevant subset)

| # | file:line | why here |
|---|-----------|----------|
| 1 | `src/catalog/src/durable/objects.rs:426` | `ReconfigurationTarget` shape — the JSON keys the MV compares against |
| 2 | `src/catalog/src/memory/objects.rs:3421,3446` | `realized_/apply_reconfiguration_target` — the exhaustive helpers the reshape now delegates to |
| 3 | `src/catalog/src/builtin/mz_internal.rs:957` | the MV `changes` expression — the one maintained dataflow |
| 4 | `src/catalog/src/builtin/mz_internal.rs:5656` | `mz_show_clusters` activity summary — a non-materialized view reading the MV's arrangement |
| 5 | `src/adapter/src/coord/sequencer/inner/cluster.rs:685,713` | the reshape reset (the leak fix) |
| 6 | `src/adapter/src/catalog/open/builtin_schema_migration.rs:360` | the `MigrationStep::replacement` that rebuilds the MV |

## Findings

### Blocking
None.

### Should-fix
None.

### Nits
None in this lens.

### Positives

- **The fix eliminates a dataflow cost cliff, it does not add one.**
  `src/adapter/src/coord/sequencer/inner/cluster.rs:685,713`. Before, a
  compression-carrying `ALTER` leaked the new value into the realized config, so
  the baseline strategy immediately desired the new shape and bounced the
  baseline replicas mid-reconfiguration. Bouncing a replica tears down and
  re-hydrates every dataflow on it, the most expensive thing the controller can
  cause. Routing the reset through `realized_reconfiguration_target()` /
  `apply_reconfiguration_target()` keeps the compression flip confined to the
  record's `target`, so only the overlap replica reshapes and the baseline is
  never re-hydrated. `cluster-controller.td:316-326` asserts exactly this (the
  baseline replica id is unchanged across the fold).

- **The `changes` MV term is the correct incremental shape.**
  `src/catalog/src/builtin/mz_internal.rs:957`. The added
  `CASE WHEN r.target->'arrangement_compression' != r.config->'arrangement_compression' …`
  is a stateless scalar concatenated with `||` onto the other four `{}`-or-object
  terms. Per input record it does one extra comparison (O(1) added cost, no
  per-update scan); on an update the map retracts the old `changes` row and emits
  the new one; on retraction the row is retracted. No `+1/-1` accumulation, so
  nothing grows unbounded. The `records` collection is one row per cluster with a
  live reconfiguration, so even the absolute cost is trivial.

- **The comparison cannot leave a settled record with a spurious diff.** Both
  sides of the `!=` are `bool` fields under the same JSON key
  (`ReconfigurationTarget.arrangement_compression: bool` at
  `src/catalog/src/durable/objects.rs:431`, and the realized
  `ClusterVariantManaged.arrangement_compression: bool`). The comparison is
  boolean-to-boolean with no SQL-NULL / missing-key asymmetry, so at cut-over
  when the realized config advances to the target the term collapses to `{}`.
  `cluster-controller.td:354-358` asserts `finalized {}` for a compression-only
  reconfiguration. The feature is unreleased and the MV is rebuilt by the
  migration, so there are no legacy records with a differently-keyed blob to
  break the comparison.

- **No new or re-formed arrangement.** The MV keeps its key on `cluster_id` and
  its index `mz_cluster_reconfigurations_ind`; the plan golden
  (`catalog_server_explain.slt:5326`) shows the change lives entirely inside the
  existing `map=(…)` over the same arranged source. `mz_show_clusters`
  (`mz_internal.rs:5656`) is a `BuiltinView`, inlined into readers, and still
  reads the shared arrangement (`catalog_server_explain.slt:2009` shows
  `→Arranged mz_internal.mz_cluster_reconfigurations`). No per-key memory-shape
  change.

- **The dimension lists that drive the reconfiguration are all
  destructure-exhaustive.** `realized_/apply_reconfiguration_target`
  (`memory/objects.rs:3421,3446`), `alter_changes_replica_shape`
  (`cluster.rs:2184`), and `alter_reconfiguration_target` (`cluster.rs:2220`)
  destructure both structs with no `..`, so a future shape dimension fails to
  compile until it is handled at each site. This is the structural fix for the
  class of bug the PR is fixing (a hand-maintained list forgetting a dimension),
  and it is applied consistently.

## Verification trail

| claim chased | how resolved | verdict |
|---|---|---|
| Does the reshape reset now cover compression? | `cluster.rs:685` captures `realized_reconfiguration_target()` and `cluster.rs:713` applies it via `apply_reconfiguration_target`; both destructure `ClusterVariantManaged`/`ReconfigurationTarget` exhaustively at `memory/objects.rs:3421/3446` incl. `arrangement_compression`. | fixed correctly |
| Can the MV `changes` term wrongly stay non-empty after settle (a `null`-vs-`false` asymmetry)? | Both operands are `bool` under key `arrangement_compression` on structs that always serialize it (no `Option`); comparison is bool-to-bool. `cluster-controller.td:354` asserts `finalized {}`. | safe |
| Does the extra `||` term add a per-update scan or unbounded growth? | Stateless scalar in a map; `records` is one row per cluster; retract-old/emit-new on update; `{} || {} = {}`. | safe (O(1) added) |
| Does the change add or re-form an arrangement? | Plan goldens show the term inside the existing `map` over the same `Key: (cluster_id)` arrangement; `mz_show_clusters` still reads the shared index. | safe |
| Is the leak fix's claimed replica behavior actually the cheap one? | `cluster-controller.td:316-326` asserts the baseline replica id is preserved (only the overlap bounces); leak would have re-hydrated the baseline. | correct → Positive |
| Migration version `26.38.0-dev.0` vs main at `26.39.0-dev.0`? | Out of this lens (builtins/version-pinning, general-quality reviewer). Branch is self-consistent (`Cargo.toml` version = `26.38.0-dev.0`), CI green. Flag: will need a rebase to the then-current dev version, else fingerprint-panic on the upgrade it skips. Noted, not my finding. | out-of-lens note |

## Boundary notes (deliberately not my findings)

- The migration step is keyed to `26.38.0-dev.0`, which matches this branch's
  workspace version but not `main` (`26.39.0-dev.0`). This is the recurring
  builtin version-pinning bite; it belongs to the general-quality lens and is
  caught on rebase / by the fingerprint check. I raise it only so the synthesis
  step does not assume the dataflow reviewer implicitly cleared it.
- The PR's own "known gap" list (audit event omits compression; `RESET` cannot
  carry `WAIT`) is behavioral scope, not dataflow. Not evaluated here.
