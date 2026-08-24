---
status: ready-for-review
category: review
title: PR #38146 review — reviewer lens, ggevay
updated: 2026-08-24
outcome: 7 should-fix (2 unenforced load-bearing invariants, 1 circular guard, 3 tests that cannot fail, 1 copied hedge), 4 nits.
---

# PR #38146 — Review through ggevay's lens

**Verdict:** the ungated claim survives ("with the flag on or off the system behaves exactly
as today" is true, and I traced every ungated change to confirm it). What does not survive is
the *coverage* claim: with `CURATED` empty, none of the 472 new lines in
`coord/metric_sink.rs` run in any CI job, and three of the four new tests pass with the code
they name deleted. Two load-bearing invariants are stated in doc comments with nothing at the
other end, and one guard validates itself against the value it is supposed to establish.

Lens: claims the code does not support, invariants held only by convention, the sibling arm /
file left open, duplicated protocol-bearing logic, and tests that cannot distinguish the
outcomes they name. Other lenses cover dataflow, adversity and style.

---

## Should-fix

### 1. `name` "must be unique within `CURATED`" is enforced by nothing, and the collision path ships anyway

`src/adapter/src/coord/metric_sink.rs:63-64` states the invariant. Nothing checks it, and
the failure is not benign, because `Coordinator::metric_sinks` is keyed
`(ReplicaId, &'static str)` (`src/adapter/src/coord.rs:2148`).

Sequence: two `CURATED` entries share a name. Both install on replica `R`. The second
`metric_sink_finish` hits the duplicate insert at
`src/adapter/src/coord/metric_sink.rs:300-309`, soft-panics, and then **ships the dataflow
anyway** at line 311. Wrong outcomes in a release build, where the soft-panic is only a log:

* The first sink's `sink_id` is no longer reachable from the map, so `drop_metric_sinks`
  (line 325) never drops it. Its instance-global compute collection leaks until the cluster
  itself is dropped.
* Both dataflows register a collector under the same `sink` label. `register_collector_with_dropper`
  (`src/ore/src/metrics.rs:313-326`) refuses the second and hands back a no-op dropper, so the
  second sink publishes nothing for its whole life, and once the first one is dropped the
  definition publishes nothing at all on that replica.

The prefix invariant got a tripwire (`curated_prefixes_are_valid`, line 426). This one, whose
violation costs more, got none. Two fixes, both small:

```rust
#[mz_ore::test]
fn curated_names_are_unique() {
    let names: BTreeSet<_> = CURATED.iter().map(|d| d.name).collect();
    assert_eq!(names.len(), CURATED.len(), "duplicate curated metric sink name");
}
```

and make the collision path bail instead of continuing:

```rust
if let Some(prev) = self.metric_sinks.insert((replica_id, definition.name), install) {
    soft_panic_or_log!("metric sink installed twice (name={}, replica_id={replica_id})", definition.name);
    // Don't ship a second dataflow under the same label; the first one is still running.
    self.metric_sinks.insert((replica_id, definition.name), prev);
    return Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink));
}
```

### 2. "The query must read only introspection relations" is checkable and unchecked

`src/adapter/src/coord/metric_sink.rs:69-72` states the contract and its reason: a
catalog-backed relation "would put envd's write frontier on the sink's emission path, which is
exactly the coupling these sinks exist to avoid". Nothing enforces it, and `plan_source`
already holds the material to: it computes `dependencies` from `resolved_ids` at line 380 and
returns them.

A definition reading `mz_catalog.mz_sources` parses, resolves, passes
`validate_metric_sink_desc`, plans, optimizes, installs, and runs. There is no error, no log
and no metric that says the sink is now coupled to envd. The symptom is a metric that stalls
exactly when the thing it was built to observe stalls, which is the hardest possible way to
find this out. A dependency-type check is awkward (`CatalogItemType`, `src/sql/src/catalog.rs:953-976`, has no
`Log` variant to match on), but there is a sharper place to check, and the PR already computes
the value: `metric_sink_finish` builds `dataflow_import_id_bundle` at
`src/adapter/src/coord/metric_sink.rs:289`, and `CollectionIdBundle`
(`src/adapter/src/coord/id_bundle.rs:21-28`) splits imports into `storage_ids` and
`compute_ids`. "Reads only introspection relations" *is* "has no storage imports": per-replica
log relations resolve to compute collections, and everything envd writes (catalog relations,
the storage-managed introspection collections) resolves to a storage id. So the contract the
comment states is exactly one line away from being enforced, right where the read holds are
taken:

```rust
let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
if !id_bundle.storage_ids.is_empty() {
    soft_panic_or_log!(
        "curated metric sink reads storage collections {:?} (name={}), coupling it to envd's \
         write frontier",
        id_bundle.storage_ids, definition.name,
    );
    return Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink));
}
```

That also removes the second-order effect of getting it wrong: `acquire_read_holds` on a
storage collection pins a storage since for the life of the sink.

### 3. `finishing.is_trivial(desc.arity())` assumes the conclusion it is checking

`src/adapter/src/coord/metric_sink.rs:376`. The comment above it (lines 372-375) states the
danger precisely: a finishing's `project` "reorders the output columns and the shaping resolves
the canonical columns by index into `desc`", i.e. the property that must hold is
`desc.arity() == source.arity()`. The check is then run against `desc.arity()` — the side that
is not in question.

`RowSetFinishing::is_trivial` (`src/expr/src/relation.rs:3540-3545`) compares `project` to
`0..arity`. For a finishing that trims a wider source (`project == [0, 1, .., desc.arity())`
over a `source` with more columns), `is_trivial(desc.arity())` returns `true` and the guard
passes on exactly the input it exists to reject.

What that costs downstream: `shape_metric_sink_source`
(`src/adapter/src/optimize/metric_sink.rs:356`) takes `arity` from `source_desc`, then indexes
its five mapped columns at `arity+0 .. arity+4` (lines 428-436) into an expression whose real
arity is larger. Every index is in range, so `optimize_mir_local`'s typechecker sees nothing
wrong; the `Project` silently reads real-but-wrong source columns, and the sink publishes
plausible garbage.

Honest scope: I could not build a query that reaches this today. In `plan_select` a
non-identity `project` comes with `order_by` populated (the extra columns exist *because* of
the ORDER BY), and `is_trivial` rejects on `order_by` first. So this is hardening, not a live
bug. It is still a one-line fix that makes the guard stand on its own —
`finishing.is_trivial(source.arity())` (`HirRelationExpr::arity`, `src/sql/src/plan/hir.rs:1742`)
— plus the invariant written at the *other* end, in the function that relies on it:

```rust
soft_assert_eq_or_log!(source.arity(), arity, "metric sink source arity disagrees with its desc");
```

### 4. `optimizer_shapes_a_query_source` cannot distinguish the outcome it names

`src/adapter/src/optimize/metric_sink.rs:826-857`. The HIR under test is
`HirRelationExpr::Get { id: Global(TABLE_GID), typ }`. Lowering that produces exactly the
`MirRelationExpr::global_get(TABLE_GID, ..)` the `Id` arm builds by hand at lines 196-200, so
the two assembled dataflows are identical and both assertions are the same assertions the
`Id` test already makes.

The test's own doc comment claims the query is "lowered under the shaping instead of a `Get`
of a catalog item" — but the query *is* a `Get` of a catalog item. Concretely: replace the
`Query` arm's body with the `Id` arm's and this test still passes. So does deleting
`expr.lower(..)` and requiring an already-lowered MIR expr.

The PR description makes the matching claim: "An optimizer unit test pins the `Query` source
path to the same assembled shape as the `Id` path ... the source imported rather than rebuilt."
That is literally true and tells us nothing, because the two paths are the same expression.

What distinguishes `Query` is that a non-trivial query *body* is inlined into the shaped view
rather than becoming a second `objects_to_build` entry. Give the HIR a `Filter` (or a `Map`),
then assert both halves: `build_ids == vec![VIEW_GID]` (still one build) **and** that the
build's plan is not a bare `Get` (the body really was inlined). Delete the lowering and that
version goes red.

### 5. With `CURATED` empty, no CI job exercises any of the new install path

`test/testdrive/metric-sink.td:228-279` is the same problem one level up, and its own comment
is accurate about it: "The curated list is empty, so there is no curated series to assert on:
what is under test is that both hooks run over an empty list without erroring."

Tracing what actually runs: `install_metric_sinks`
(`src/adapter/src/coord/metric_sink.rs:115-130`) returns at the flag check or falls through a
zero-iteration `for definition in CURATED`; `drop_metric_sinks` (line 325) scans an empty
`BTreeMap` range. The two `count(*) = 1` assertions are about the pre-existing *user* sink
`s_churn`, which is unchanged behaviour. Empty out the bodies of `install_metric_sink`,
`metric_sink_optimize`, `metric_sink_finish` and `drop_metric_sinks` and this file still
passes, as does every other job.

The flag is already on in CI — `"enable_metric_sink": "true"` in
`get_minimal_system_parameters` (`misc/python/materialize/mzcompose/__init__.py:103`), which is
the right place for it. That means **one** real `CURATED` entry buys coverage of plan →
optimize → ship → per-replica teardown → gauge label across testdrive, platform-checks and
parallel-workload for free, on every PR. Shipping the scaffold with the list empty means the
first real definition is also the first execution of all 472 lines, and whoever writes it
debugs both at once.

If a definition genuinely cannot land in this PR, the next best thing is a Rust integration
test that drives `install_metric_sinks` with an injected definition list, so the path is
executed by *something*.

### 6. "instead of failing the boot" is the release behaviour, not the CI behaviour

`src/adapter/src/coord/metric_sink.rs:147-153` and `:156-167` (and the same shape at
`:244-249`) both promise to "give up on this one sink instead of failing the boot".
`soft_panic_or_log!` panics wherever debug assertions are on, which is `[profile.ci]` — the one
build where a malformed definition would first be noticed. So in CI a typo'd definition panics
envd inside `bootstrap_metric_sinks` (`src/adapter/src/coord.rs:3147`), in every job that
starts envd, and the comment says the opposite.

Two ways out, and the second is better because it makes the first moot: reword to say what
happens in each profile, or make the boot-time path unreachable by adding the tripwire that
`curated_prefixes_are_valid` (line 426) already models for prefixes and
`plan_source_enforces_the_metric_sink_contract` (line 450) already has the harness for:

```rust
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn curated_definitions_plan() {
    Catalog::with_debug(|catalog| async move {
        let session_catalog = catalog.for_system_session();
        for definition in CURATED {
            definition.plan_source(&session_catalog)
                .unwrap_or_else(|err| panic!("curated metric sink {:?} does not plan: {err}", definition.name));
        }
    }).await
}
```

With that in place, a bad definition fails one cargo test instead of crash-looping envd.

### 7. The ignored `drop_collections` result is copied from a mirror whose reason does not apply

`src/adapter/src/coord/metric_sink.rs:337-343`. Two sentences that contradict each other: "An
entry exists only for a sink whose dataflow was shipped, so its collection exists. The result
is ignored defensively, in case the controller already released it."

The mirror it was copied from has a real reason — `drop_introspection_subscribe`
(`src/adapter/src/coord/introspection.rs:370-376`) ignores the result *because* it inserts its
map entry before sequencing finishes ("This can fail if the sequencing hasn't finished yet"),
so the collection genuinely may not exist. `metric_sinks` deliberately does the opposite: it
inserts only after the dataflow ships (lines 293-299). So the metric-sink version inherited the
hedge without inheriting the reason, and no path is named for "already released".

If the invariant in the first sentence holds, this should be
`.expect("metric sink collection exists")` — a swallowed error here means the controller and
the coordinator disagree about what exists, which is worth a crash, not a shrug. If it does
not hold, name the path that releases it and drop the first sentence.

---

## Nits

### 8. The replica walk is written twice

`bootstrap_metric_sinks` (`src/adapter/src/coord/metric_sink.rs:96-107`) is a verbatim copy of
`bootstrap_introspection_subscribes` (`src/adapter/src/coord/introspection.rs:104-116`). It is
not incidental code: it is *the set of replicas a per-replica feature must install on*, so the
two copies have to agree, and the TODO at `metric_sink.rs:124-125` is a scheduled divergence
(skip replicas with introspection disabled applies verbatim to introspection subscribes too).
One `fn all_cluster_replicas(&self) -> Vec<(ClusterId, ReplicaId)>` names it once and gives the
TODO a single place to land.

### 9. A hardcoded family count that will churn

`src/compute/src/sink/metric_sink.rs:1170`: `assert_eq!(families.len(), 6)`. It does earn its
keep — without it the loop below is vacuous if `collect()` returns nothing — but it will churn
on every added gauge and it cannot tell the next person whether they broke the label or just
added a family. `assert!(!families.is_empty())` keeps the anti-vacuity guarantee and drops the
churn.

### 10. The introspection-disabled hole deserves its cost written down

`src/adapter/src/coord/metric_sink.rs:124-125`. Fine to defer, but say what it costs: on a
replica created with introspection disabled every curated definition reads empty relations, so
the sink publishes nothing, and nothing distinguishes that from a definition that is broken.

### 11. The install log fires before the gates that abandon the install

`src/adapter/src/coord/metric_sink.rs:138-139` allocates the transient id and logs "installing
metric sink" before the prefix check (147) and the plan check (156), either of which returns.
For a bad definition the log claims an install that never happened. Move the `info!` below both
gates.

---

## Positives

* **The ungated claim holds, and I checked it the way the description invites.** Every change
  outside the flag is behaviour-preserving: `MetricSinkConnection.label` defaults to
  `self.sink_id.to_string()` (`src/adapter/src/optimize/metric_sink.rs:237-241`), which is
  exactly the `sink_id` the compute operator used before, since the same value keys
  `df_desc.export_sink(self.sink_id, ..)`; `clusterd-test-driver` was moved in lockstep
  (`src/clusterd-test-driver/src/dataflow.rs:403-405`);
  `FeatureFlag::enabled` (`src/sql/src/session/vars.rs:2545-2548`) is a pure extraction of what
  `require` already did; the two `validate_metric_sink_*` functions only gained `pub`. Nothing
  is rewired outside the gate.
* **The optimizer pipeline stays in step with its six siblings.** The diff removes the explicit
  `import_into_dataflow(&metric_sink.from, ..)` but keeps
  `maybe_reoptimize_imported_views` (`src/adapter/src/optimize/metric_sink.rs:232`), and the
  removed call was genuinely redundant: `import_view_into_dataflow`
  (`src/adapter/src/optimize/dataflows.rs:395-407`) walks `view.depends_on()` and imports the
  same id. So the `Id` path's assembled plan is unchanged, and metric sinks remain the seventh
  of seven optimizers calling the reoptimization hook.
* **The flag lives in `get_minimal_system_parameters`**, not special-cased in a harness — the
  right place, and it is what makes suggestion 5 cheap.
* **The finishing is rejected, not silently dropped**, with the reason (`desc` desyncing from
  `source`) written at the site rather than left for the next reader to reconstruct
  (`src/adapter/src/coord/metric_sink.rs:372-378`).
* **`#[cfg_attr(miri, ignore)]` with the actual error quoted** on the debug-catalog test
  (`src/adapter/src/coord/metric_sink.rs:449`) — the nightly-only job that green PR checks do
  not run.
* **`MetricSinkFrom` mirrors `SubscribeFrom`** rather than inventing a third shape for "an id or
  a query", and the doc says which sibling it mirrors.

---

## Verification trail

| claim chased | how resolved | verdict |
| --- | --- | --- |
| "with the flag on or off the system behaves exactly as today" | enumerated every change outside `ENABLE_METRIC_SINK`: label default, `FeatureFlag::enabled`, two `pub`s, `debug_name`, removed redundant import, test-driver update. Each traced to an identical-output argument (see Positives). | holds |
| `maybe_reoptimize_imported_views` dropped from the metric-sink pipeline (the diff removes a line next to it) | read the current file: still called at `optimize/metric_sink.rs:232`; only the redundant explicit import went, subsumed by `import_view_into_dataflow`'s `depends_on()` walk (`dataflows.rs:402-404`). All seven optimizers still call it. | safe, my false positive |
| `arity` derivation changed from `ReprRelationType::from(desc.typ()).column_types.len()` to `desc.typ().columns().len()` | `impl From<&SqlRelationType> for ReprRelationType` (`src/repr/src/relation.rs:430-441`) maps columns 1:1. | safe |
| `resolve_full_name(name, None)` replacing `resolve_full_name(name, from_entry.conn_id())` — panic or wrong name for a temp item? | `CatalogState::resolve_full_name` (`src/adapter/src/catalog/state.rs:894-925`): `SchemaSpecifier::Temporary` short-circuits to `MZ_TEMP_SCHEMA` with no lookup, and `SchemaSpecifier::Id` resolution does not depend on `conn_id`. Debug-name only either way. | safe, and the old pairing (the sink's name resolved through the *source's* conn id) was the odd one |
| the new `MetricSinkConnection` field needs a lockstep `.proto` / protocol version bump | no `ProtoComputeSinkConnection` or `ProtoMetricSinkConnection` anywhere; `src/compute-client/src/protocol/` has no `.proto`, the type is serde-only, and envd/clusterd versions match within a deployment. | no lockstep file |
| "unlike an introspection subscribe there is nothing to reinstall" (`coord/metric_sink.rs:25-26`) | `reinstall_introspection_subscribe` (`introspection.rs:384-429`) exists because a subscribe's *envd-side response stream* breaks on reconnect. A metric sink has no such stream; the controller replays its replica-targeted dataflow from command history. | claim holds |
| does a whole-cluster drop reach `drop_metric_sinks`? | `catalog_implications.rs:1031-1042` drains `cluster_replicas_to_drop` through `drop_replica` *before* `drop_cluster`, and `ddl.rs:720-722` calls `drop_metric_sinks` first. | holds |
| `_sql_impl_ids` discarded in `plan_source` (`metric_sink.rs:364`) — dependencies missing from `PlanValidity`? | `introspection.rs:581` discards it the same way; this is the mirrored convention, not a divergence introduced here. | not a finding on this PR |
| double install for the same `(replica, name)` from `bootstrap_metric_sinks` + `handle_create_cluster_replica` | replica ids are allocated fresh per create and bootstrap runs once, so those two never overlap on one id. The reachable route into the duplicate-insert path is a duplicate `CURATED` name. | routes into Finding 1 |
| duplicate `sink` label ⇒ what actually breaks | `register_collector_with_dropper` (`src/ore/src/metrics.rs:313-326`) soft-panics, returns a no-op dropper, and leaves the second collector unregistered. | Finding 1's second wrong outcome |
| is the `is_trivial` hole reachable from SQL today? | traced `RowSetFinishing.project` construction: a non-identity project exists only when ORDER BY adds columns beyond the select list, and `is_trivial` rejects on non-empty `order_by` first. | not reachable today; Finding 3 stays hardening, stated as such |
| CI status | `gh pr checks 38146`: clippy, lint-and-rustfmt, lint-dependencies, doctests, cargo-doc-tests, rust-build-aarch64/wasm, merge-skew-cargo-check all pass; `buildkite/test` still pending on `rust-build-x86-64`, so the cargo-test and testdrive jobs have not reported. | compile/lint green; **test results not yet available** |
