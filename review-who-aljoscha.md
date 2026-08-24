# Review of PR #38146 — metric sinks: coordinator per-replica install scaffold (SQL-555)

Reviewing as **aljoscha**. Lens: adapter/coordinator shape, comment hygiene, "is this how
we do it elsewhere", multi-envd, cost-per-object, fix-at-source vs patch-the-window.

**Verdict:** the shape is right. This mirrors `coord::introspection` closely, which is exactly what I
want for a new per-replica install mechanism, and the atomic ship+register in the finish stage is
sound. Nothing here blocks. I have one prefix-collision gap that will bite the follow-up that
populates `CURATED`, one stale carried-over comment, and a couple of smaller notes. Given `CURATED`
is empty and this is a draft, most of these can land as follow-ups as long as they are named.

---

## Blocking

None.

---

## Should-fix

### 1. Curated prefixes are never checked for collision against user (or other curated) sinks

`src/adapter/src/coord/metric_sink.rs:147` validates only the *format* of a curated prefix
(`validate_metric_sink_prefix`). It does not run the prefix-*freeness* check that the user path runs,
`ensure_metric_sink_prefix_is_free` (`src/adapter/src/coord/sequencer/inner/create_metric_sink.rs:321`).
That check walks the cluster's `bound_objects` looking for `CatalogItem::MetricSink` entries — and a
curated sink is deliberately *not* a catalog item, so it is invisible to it. The gap is two-way:

- A user `CREATE METRIC SINK` whose prefix overlaps a curated one passes `ensure_..._free` (the
  curated sink isn't in `bound_objects`) and is accepted.
- A curated install never checks against user sinks or other curated definitions at all.

Both a user prefix and a curated prefix must start with the same `mz_metric_sink_` reserved marker,
so they share one lane and can genuinely collide (e.g. user `mz_metric_sink_foo_` vs curated
`mz_metric_sink_foo_bar_`). The only backstop is the compute operator's family-conflict detection,
which drops series rather than surfacing the misconfiguration. This is the "fix at the source"
situation: the coordinator is where the conflict is knowable. It is fine to leave for the PR that
first populates `CURATED` since the list is empty today, but please say so in the PR description
rather than let it be discovered later.

### 2. Stale carried-over doc comment on `sink_label`

`src/adapter/src/optimize/metric_sink.rs:804-806`:

```rust
/// The assembled dataflow exports exactly one `MetricSink`, reading the shaped view rather
/// than the source relation directly.
/// The `sink` label carried by the export's connection.
fn sink_label(df_desc: &LirDataflowDescription) -> &str {
```

The first two lines describe `optimizer_exports_one_metric_sink` (which now sits at line 820 with *no*
doc). They are a carry-over from an earlier revision, left stranded above the wrong function. Drop the
first sentence here (keep "The `sink` label carried by the export's connection.") and, if you want,
move the export-shape sentence back onto `optimizer_exports_one_metric_sink`. A comment stuck to code
it doesn't describe is exactly the kind of thing that reads as true and isn't.

---

## Nits

### 3. The new capability has no end-to-end coverage — say so

`test/testdrive/metric-sink.td:227+` exercises the install/drop hooks over an *empty* `CURATED` plus a
user sink surviving replica churn. That is a good hook-smoke test, but it means the actual new
capability — `MetricSinkFrom::Query`, lowered under the shaping, shipped as a *replica-targeted*
dataflow reading *per-replica* introspection relations, with the as-of picked under a read hold over
those per-replica log collections in `metric_sink_finish` — is unexercised. The read-hold / as-of
behavior over per-replica introspection sources is the one genuinely new-and-untested corner. It's
reasonable to defer with `CURATED` empty (`src/adapter/src/coord/metric_sink.rs:81`), but the first
curated definition should land with an integration test of that path, not just the optimizer unit
tests. Please name this in the PR so it isn't lost.

### 4. Register-in-finish diverges from introspection's register-before-sequence — call out that it's deliberate

`src/adapter/src/coord/metric_sink.rs:293-309` records the install in `metric_sinks` in the *finish*
stage. `coord::introspection` does the opposite: it inserts into `introspection_subscribes` *before*
sequencing (`src/adapter/src/coord/introspection.rs:149-159`) specifically "to ensure the subscribe
does not leak" if the replica is dropped mid-sequence. I traced this and the metric-sink choice is
actually safe: `sequence_staged` rechecks `PlanValidity` (which probes the replica,
`src/adapter/src/coord/validity.rs:127-134`) before the finish stage runs, and the register + ship
happen in the same finish turn with no await between them, so a replica drop either fails the recheck
(nothing shipped, nothing to leak) or lands after finish (cleaned by `drop_metric_sinks`). So no bug —
but it is an intentional departure from the neighbour it otherwise mirrors, and the reasoning lives
only implicitly across two comments. A one-line note on the register site ("unlike introspection we
can register late because ship+register are atomic under the finish-stage validity recheck") would
save the next reader the trace I just did.

### 5. `source_sql`'s "introspection relations only" invariant is documented but unenforced

`src/adapter/src/coord/metric_sink.rs:69-73` states the load-bearing contract that a curated query
must read only introspection relations (else envd's write frontier lands on the sink's emission path,
the exact coupling these sinks exist to avoid). The *prefix* invariant right below it gets both a
runtime guard (`validate_metric_sink_prefix` at install) and a unit test (`curated_prefixes_are_valid`);
this one gets only prose. Since `plan_source` already has `resolved_ids` in hand, a cheap
install-time assertion that every dependency is an introspection relation would make the two
invariants consistent, and turn a silent freshness footgun into a `soft_panic_or_log!` on a bad
definition. Not required for an empty list, but worth a TODO at minimum.

### 6. One structuring semicolon in an added comment

`src/adapter/src/coord/metric_sink.rs:143`: "A user sink's prefix is checked at plan time; a curated
one has no such gate...". House style allows one where splitting would mangle the sentence, and this
is the only one in the diff, so it's borderline — but it splits cleanly at the semicolon into two
sentences, so I'd just make it a full stop.

### 7. `info!` on every install/drop scales with fleet × `|CURATED|`

`src/adapter/src/coord/metric_sink.rs:139` and `:334` log at `info!` per sink per replica, so once
`CURATED` is non-empty this fires `|CURATED|` times on every replica create and on every boot across
the fleet. I checked and `coord::introspection` logs its install at `info!` too
(`src/adapter/src/coord/introspection.rs:142`), so this matches the neighbour and I won't push on it —
just flagging that the neighbour's volume is inherited here, and `debug!` would be defensible if the
curated list ever grows.

---

## Things I checked and am satisfied with

- **User-sink behavior is unchanged.** The default `label` is `self.sink_id.to_string()`
  (`src/adapter/src/optimize/metric_sink.rs:237-240`), and `self.sink_id` is the same id used as the
  export key (`export_sink(self.sink_id, ...)`, line 247) that the compute operator previously stringified
  directly. So the `sink` label on a user sink's gauges is byte-for-byte what it was.
- **`drop_metric_sinks` range scan is correct.** Keying `(ReplicaId, &'static str)` replica-first and
  scanning `range((replica_id, "")..).take_while(id == replica_id)` yields exactly that replica's
  contiguous entries; ReplicaIds are globally unique so dropping cluster_id from the key is fine.
- **The `finishing.is_trivial` rejection in `plan_source`** (`metric_sink.rs:376`) is the right guard:
  it keeps `desc` (post-describe) column order in lockstep with `source` (pre-finishing HIR), which the
  by-name→index shaping relies on. Good catch to reject ORDER BY/LIMIT/OFFSET rather than silently drop them.
- **Multi-envd:** install is wired through the replica-create implication
  (`src/adapter/src/coord/catalog_implications.rs:1691`), the same hook introspection uses, so it fires
  on the same trigger the rest of the per-replica machinery does.
- **`MetricSinkFrom` and the `Staged` impl** are both exhaustively matched, so a future variant forces a decision.
