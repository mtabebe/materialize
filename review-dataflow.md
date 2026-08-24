# PR #38146 review — lens: dataflow semantics and cost

**Verdict:** Sound as incremental dataflow. No blocking issues. The sink operator's
update/retract/frontier behavior is unchanged by this PR (the only compute-side edit is a
cosmetic gauge label), and the new `MetricSinkFrom::Query` optimizer path is a faithful,
MV-style dataflow construction. The findings below are cost-model observations and one
frontier-hold caveat to settle before `CURATED` is populated, not defects in this scaffold.

## Blocking

None.

## Should-fix

- **Introspection-disabled replica: the sink's input frontier may never advance, holding
  the imported collections' `since`.** `src/adapter/src/coord/metric_sink.rs:159` carries a
  `TODO: Skip replicas created with introspection disabled`, justified only as "their logging
  dataflows never run, so the introspection relations ... stay empty there." Through the
  frontier lens the risk is sharper than wasted work: the metric-sink operator publishes its
  *input* frontier as its `sink_write_frontier`
  (`src/compute/src/sink/metric_sink.rs:186-190`), and the compute controller uses that to
  bound the imported collections' `since`. If a curated definition's `source_sql` imports a
  logging collection that on such a replica is *absent or never advances* (rather than merely
  empty), the sink's write frontier stalls at `Timestamp::MIN` and pins that `since` for the
  life of the replica. The hold is replica-local and released on replica drop, so it is not a
  global leak, but it is a real held-frontier that the TODO's "empty" framing understates.
  Coordinator-side read holds are fine — acquired only across shipping and dropped right after
  (`metric_sink.rs:290-314`). Recommend the TODO be resolved (skip such replicas, or confirm
  the logging collections always exist with an advancing frontier) *before* `CURATED` gains
  any entry, and that the comment be corrected from "stay empty" to the frontier consequence.
  Note this mirrors `coord/introspection.rs`, which also does not gate on logging-disabled
  replicas, so it is a pre-existing shape, not newly introduced — but the metric sink inherits
  it.

## Nits / cost notes (record for when `CURATED` is populated)

- **Cost model is N replicas × M definitions independent dataflows, with no arrangement
  sharing across the curated set.** Each curated sink is optimized and shipped as its own
  dataflow (`install_metric_sink` → one `sink_id`, one `view_id` per definition per replica,
  `metric_sink.rs:135-175`). Base arranged logging collections are shared across dataflows
  (they are imported as indexes), so the *leaf* reads are shared, but any per-definition
  shaping/aggregation is rebuilt per definition and per replica. This is inherent to
  per-replica rendering and fine, but worth stating so definition authors size `CURATED`
  against per-replica memory, not per-cluster.

- **No guard against an expensive definition query; `finishing.is_trivial` only rules out a
  TopK.** `plan_source` correctly rejects ORDER BY/LIMIT/OFFSET
  (`src/adapter/src/coord/metric_sink.rs`, `finishing.is_trivial` check) — good, since a
  finishing on a continuously-maintained sink would force a TopK and also desync `desc` from
  `source`. But nothing bounds the *per-update* cost of the query itself: an unkeyed
  aggregation or a wide join over a high-churn introspection relation (e.g. arrangement-size
  logs) would make every replica pay a large recompute per logging tick. The sink operator
  additionally folds the whole collection to one worker via `Exchange`
  (`src/compute/src/sink/metric_sink.rs:100-108`), so a high-cardinality result concentrates
  on a single worker. This is a definition-authoring concern (CURATED is empty today), but
  there is no code-level backstop, so it should be a review checklist item for each future
  definition.

- **Query-source `with_snapshot: true` + full snapshot read is correct here.** The lowered
  query reads introspection relations only (enforced by convention, documented at
  `metric_sink.rs` `source_sql` doc); a full-snapshot continuous sink over those is the right
  semantics and avoids coupling to envd's write frontier, as the doc-comment argues.

## Verified, not a finding (reads beyond the diff)

- **The `Id` path is behavior-preserving.** The diff drops the explicit
  `import_into_dataflow(from)` + first `maybe_reoptimize_imported_views` call, but
  `import_view_into_dataflow(view_id, shaped_expr)` walks `shaped_expr.depends_on() == [from]`
  and calls `import_into_dataflow(from)` internally (`optimize/dataflows.rs:395-408`), and the
  surviving `maybe_reoptimize_imported_views` at `optimize/metric_sink.rs:232` still covers the
  `from` view builds (transient `view_id` is skipped by that pass). So the assembled dataflow —
  imports, builds, arrangements — is identical to `main` for `CREATE METRIC SINK ... FROM
  <relation>`. The removed pre-import was redundant.

- **Retraction/consolidation semantics unchanged.** The row-wise shaping is a stateless
  `Map` + `Project` (`shape_metric_sink_source`), so diffs (inserts and retractions alike)
  pass through unchanged; consolidation and the dedup/collision/family-conflict fold all live
  in the pre-existing, untouched `SinkState` operator. A retraction-only input sequence
  behaves exactly as on `main`.

- **Read-hold / as-of on ship is the established pattern.** `acquire_read_holds` →
  `set_as_of(least_valid_read())` → `ship_dataflow` → `drop(read_holds)`
  (`metric_sink.rs:290-314`) matches the subscribe/MV install; compute takes its own holds in
  `create_dataflow`, so there is no window where the as-of's inputs can be compacted away, and
  no coordinator-side hold outlives the ship.

- **Targeted (per-replica) rendering does not create a frontier-meet hazard.** A curated sink
  targets one replica, so the controller sees a single write frontier for it rather than
  meeting across replicas — strictly simpler than the untargeted user-sink case, no new
  cross-replica stall surface.
