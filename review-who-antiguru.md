# Review of PR #38146 — antiguru lens (systems / correctness-under-adversity)

**Verdict: Approve with should-fix.** The staged install/teardown mirrors the introspection-subscribe
machinery faithfully and the adversarial cases I chased (replica dropped mid-install, cluster
CASCADE, flag-off, read-hold/as-of window, user-sink label parity) all hold up. My findings are one
unenforced invariant worth a cheap test, one acknowledged correctness gap that needs an issue, and a
couple of nits. Nothing blocking, helped by `CURATED` being empty so the risky query path ships no
live series yet.

Scope note: because `CURATED` is empty (`metric_sink.rs:81`), the `MetricSinkFrom::Query` path,
per-replica introspection read-holds, and the optimize/finish staging are exercised only by the
unit tests (which use a table `GlobalId`, not real introspection relations). No end-to-end coverage
of a curated query reading introspection exists yet. That is inherent to a scaffold PR and honestly
stated in the description, but it means the query path earns trust only once a real definition lands.

---

## Blocking

None confirmed.

I specifically cleared the two things the description leans on:

- *"A replica dropped mid-install is caught by the staged validity recheck before the finish stage
  ships."* Holds. `sequence_staged` re-runs `stage.validity().check()` at the top of every stage hop
  (`sequencer/inner.rs:229`), the `MetricSinkFinish` validity carries `Some(replica_id)`, and
  `PlanValidity::check` errors when `cluster.replica(replica_id).is_none()` (`validity.rs:126-133`).
  A replica drop bumps the transient revision, so the finish hop rejects before shipping.
- *"The coordinator handles one message at a time, so no replica drop runs between that check and the
  ship."* Holds. `metric_sink_finish`'s only await is `ship_dataflow`, which does not re-enter the
  coordinator message loop, so no `drop_replica` interleaves between the top-of-stage check and the
  ship (`metric_sink.rs:296-352`).

Teardown on `DROP CLUSTER ... CASCADE` is also clean: `cluster_replicas_to_drop` routes each replica
through `drop_replica` → `drop_metric_sinks` (`catalog_implications.rs:1032-1035`, `ddl.rs:720-722`)
*before* `controller.drop_cluster`, so the instance-global collection state is released rather than
leaked.

---

## Should-fix

### 1. `CURATED` name-uniqueness invariant is documented but nothing enforces it — a dup silently leaks in prod

`CuratedMetricSink::name` is documented "Must be unique within [`CURATED`]" (`metric_sink.rs:62-65`),
and the whole registry is keyed on it: `metric_sinks: BTreeMap<(ReplicaId, &'static str), _>`
(`coord.rs:2147-2151`). But nothing checks the invariant. If two definitions share a name, on one
replica:

- `install_metric_sink` ships dataflow A (`sink_id_A`), inserts `(r, name)`; then ships dataflow B
  (`sink_id_B`), and the second `metric_sinks.insert` returns `Some(..)` → `soft_panic_or_log!`
  (`metric_sink.rs:322-333`). In CI that panics; **in `optimized`/`release` it only logs and
  overwrites**, so the map now holds `sink_id_B` and `sink_id_A`'s compute collection is never
  dropped on replica teardown → resource leak.
- Both collectors also register the same six gauge families with the same `sink=<name>` const label
  into the one process registry, colliding on `Desc` id and tripping the operator's own
  registration soft-panic.

This is precisely the "invariant that nothing checks" pattern (cf. `1 << 22` in three places,
#36891). Cheapest fix mirrors the existing `curated_prefixes_are_valid` test (`metric_sink.rs:426`):
add a `curated_names_are_unique` test that collects the names into a set and asserts no collision.
Belt-and-braces would also assert unique `prefix` values, since two definitions with distinct names
but the same prefix publish into the same family lane.

### 2. Introspection-disabled replicas are an acknowledged correctness gap with no tracked exit

`install_metric_sinks` carries `// TODO: Skip replicas created with introspection disabled. Their
logging dataflows never run, so the introspection relations every source_sql reads stay empty
there.` (`metric_sink.rs:124-126`). With introspection off, a curated `source_sql` reading those
relations either imports a collection that is empty or is not importable at all — the sink then
publishes nothing or fails to plan/optimize per replica (soft-panicking on every such replica). It
is fine to defer while `CURATED` is empty, but a deferred workaround wants an issue naming the exit
(cf. #34887, #35940) rather than only a `TODO:` that populating `CURATED` will silently step on.

---

## Nits

### 3. Stale/duplicated doc comment on the `sink_label` test helper

`optimize/metric_sink.rs:804-806`:

```rust
/// The assembled dataflow exports exactly one `MetricSink`, reading the shaped view rather
/// than the source relation directly.
/// The `sink` label carried by the export's connection.
fn sink_label(df_desc: &LirDataflowDescription) -> &str {
```

The first two lines are a copy-paste leftover from `optimizer_exports_one_metric_sink`
(`metric_sink.rs:820-821`); only the third line describes `sink_label`. Drop the stale pair.

### 4. `plan_source` re-plans identical curated SQL once per replica (cold path)

`install_metric_sink` → `plan_source` (`metric_sink.rs:356-395`) parses, name-resolves, describes,
validates, and plans the definition's SQL against `for_system_session()` — a catalog that is
replica- and cluster-independent — yet it runs for every `(replica × definition)` at bootstrap and
on every replica create. The HIR/desc/deps result is identical across replicas; only the later
optimize step legitimately depends on cluster features. Hoisting parse→plan to once-per-definition
would avoid the O(replicas) redundancy. Bounded and cold, so low priority, but worth noting before
`CURATED` grows and bootstraps re-plan the same SQL N times.

### 5. Flag-on does not retroactively install on existing replicas

`install_metric_sinks` returns early when `enable_metric_sink` is off (`metric_sink.rs:118-121`), and
the comment correctly documents that turning it back on installs only on replicas created from then
on. That is a reasonable scaffold choice and well-documented; flagging only so the eventual
operator-facing behavior ("toggling the flag on is not sufficient; existing replicas need recreating
or an envd restart") is a conscious decision rather than a surprise.

---

## Things I checked that are fine (so the next reviewer need not re-derive them)

- **User `CREATE METRIC SINK` label parity.** Old operator used `sink_id.to_string()` where
  `sink_id` is the compute export `GlobalId`; the new default `label` is
  `metric_label.unwrap_or_else(|| self.sink_id.to_string())` with `self.sink_id == global_id`
  (`create_metric_sink.rs:145-165`, `metric_sink.rs:237-241`). Same value; no behavior change.
- **`maybe_reoptimize_imported_views` was NOT dropped** — it survives at `optimize/metric_sink.rs:232`
  after `import_view_into_dataflow` (the `git diff main...pr-38146` hunk just didn't show the trailing
  context). View inlining is preserved because `import_view_into_dataflow` recurses through
  `import_into_dataflow`, which inlines catalog views (`dataflows.rs:355-360`); the unit tests confirm
  `source_imports` contains the leaf and only the shaped view is built.
- **Read-hold / as-of window in finish.** Acquire → `set_as_of(least_valid_read())` → ship → drop, all
  synchronous except the ship await, with compute taking its own holds in `create_dataflow`
  (`metric_sink.rs:339-350`). Safer than the introspection variant, which fixes the as-of a stage
  earlier.
- **`drop_metric_sinks` range scan.** `.range((replica_id, "")..).take_while(id == replica_id)` walks
  exactly one replica's contiguous key block; correct given the replica-first key ordering
  (`metric_sink.rs:355-379`).
- **Keying on the stable `&'static` name rather than the transient `GlobalId`** for both the registry
  key and the `sink` health-gauge label is the right call — the `GlobalId` churns every boot, the name
  does not.
