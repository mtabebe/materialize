---
status: ready-for-review
category: cluster-controller
title: PR #38112 review — dataflow semantics and cost lens
updated: 2026-08-14
outcome: No dataflow findings. The only rendered-dataflow surface is a stateless Map-term addition to a catalog-sized builtin MV plus one array element in an unmaterialized view. No new arrangement, no retraction/consolidation hazard, no frontier touch, cost is per-row scalar on a tiny source.
---

# Review — PR #38112, lens: dataflow semantics and cost

**Verdict: LGTM (dataflow lens).** Nothing to block or fix through this lens.
Most of the diff is catalog config plumbing (the reshape reset, the WAIT gate,
the migration step) with no dataflow surface at all. The two objects that
actually render as dataflows both take a purely additive, stateless scalar
change with no cost or semantic hazard. Reasoning traced below.

---

## What renders as a dataflow here

Only two changed objects become dataflows. Everything else
(`reshape_alter_cluster_managed`, `plan_alter_cluster`'s WAIT gate,
`builtin_schema_migration`, `ClusterState` doc, `.slt`/`.td` tests) is
adapter/controller/catalog code outside this lens.

1. `mz_cluster_reconfigurations` — a `BuiltinMaterializedView`
   (`src/catalog/src/builtin/mz_internal.rs:876`).
2. `mz_show_clusters` — a `BuiltinView` (unmaterialized)
   (`src/catalog/src/builtin/mz_internal.rs:5653`).

## `mz_cluster_reconfigurations` — the `changes` Map term

`src/catalog/src/builtin/mz_internal.rs:953` adds a fifth
`CASE WHEN r.target->'arrangement_compression' != r.config->'arrangement_compression'
THEN jsonb_build_object(...) ELSE '{}'::jsonb END` term to the `||`-chain that
builds the `changes` jsonb.

- **Operator shape.** This lives entirely in the existing `Map/Filter/Project`
  over the `mz_catalog_raw` source (confirmed by the rewritten golden at
  `test/sqllogictest/catalog_server_explain.slt:5326`). It adds one deterministic
  scalar sub-expression per row. No new operator, no join, no reduce, no
  distinct.
- **Arrangement / sharing.** Unchanged. The MV keeps its single arrangement
  keyed by `cluster_id` (`Arranged mz_internal.mz_cluster_reconfigurations
  Key: (#0{cluster_id})` at `catalog_server_explain.slt:2009`), the key that
  `mz_show_clusters` joins against. This term is inside the value projection, so
  it neither adds an arrangement nor re-forms/un-shares an existing one. Memory
  shape per key is a few extra bytes of jsonb while a compression-only record is
  in-progress, back to `{}` once it settles.
- **Update / retraction / consolidation.** The row already retracts-and-reinserts
  as a whole on any status transition (in-progress → finalized flips `changes`
  from a populated object to `{}`); that churn is pre-existing and one row per
  cluster. The new term only widens the in-progress object by one key and does
  not add any independent `+1/-1` that fails to cancel. `jsonb_build_object` /
  `||` are pure and deterministic, so equal inputs consolidate exactly. No
  unbounded-growth pattern.
- **Cost per update.** Per-row scalar work on a catalog-sized input (one row per
  managed cluster with a record). There is no per-update scan of a collection.
  This is not a cost cliff.
- **NULL semantics match the existing four dimensions.** `->` yields SQL NULL
  when a key is absent, and `CASE WHEN NULL` takes the ELSE (`{}`) arm, so a
  record predating the dimension contributes no false diff. Identical to the
  `size`/`replication_factor`/`availability_zones`/`logging` arms it sits beside.

## `mz_show_clusters` — the summary array element

`src/catalog/src/builtin/mz_internal.rs:5656` adds one
`CASE WHEN changes->'arrangement_compression' IS NOT NULL THEN 'arrangement
compression' END` element to the `array[...]` feeding `array_to_string`. This is
an unmaterialized view, rendered inline into consumers only when queried, so
there is no standing dataflow cost. Pure scalar. No dataflow concern.

## Nits

- **None blocking.** One observation that is really a correctness-lens item, so I
  only flag the boundary: the `changes` diff is only right if `target`
  (a serialized `ReconfigurationTarget`) and `config` (the serialized managed
  config) render `arrangement_compression` in an identical jsonb shape. If they
  serialized it differently the `!=` would stay true after cut-over and the row
  would show a permanent non-empty `changes` (a wrong-but-stable diff, not
  churn — it would not cause consolidation blowup). This is covered: the new
  `cc_compression` section in `test/testdrive/cluster-controller.td:415` asserts
  `changes::text = '{}'` after the record finalizes, which would catch a shape
  mismatch. Left to the correctness lens; noted here only so it is not assumed to
  be a dataflow hazard.
