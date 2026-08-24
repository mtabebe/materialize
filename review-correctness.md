---
status: ready-for-review
category: review
title: PR #38146 review — correctness under adversity lens
updated: 2026-08-24
outcome: No blocking correctness bug; 1 should-fix (panic bypasses soft-fail), 2 nits.
---

# PR #38146 — Review: Correctness under adversity

**Verdict:** No blocking correctness bug. The interesting adversity paths — a replica
dropped mid-install, a whole-cluster drop, a crash between the two writes — are all
handled, and I traced each to a verdict below. One should-fix (a panic that bypasses
the file's own soft-fail contract) and two nits.

Scope note: `CURATED` is empty in this PR, so none of these paths execute today. I
reviewed the logic as if `CURATED` were populated, since that is what the scaffold is
for.

---

## What holds up (traced, not assumed)

These are the scenarios the lens asks for; I record the verdict so a reader does not
re-derive them.

- **Replica dropped while its install is optimizing off-thread.** Sequence: replica `R`
  created → `install_metric_sink` runs the `Optimize` stage inline and spawns the
  blocking optimize, returning to the message loop → a `DropReplica(R)` message is
  processed → `drop_metric_sinks(R)` finds no entry (the `Finish` stage has not inserted
  yet) and does nothing → optimize completes and posts `MetricSinkStageReady{Finish}`.
  The second `sequence_staged` call rechecks `validity` before `Finish`
  (`sequencer/inner.rs:229`), and `PlanValidity::check` fails on the missing replica
  (`validity.rs:126-133`) because the drop bumped `transient_revision` past the fast-path
  (`validity.rs:114`). Result: `Finish` never runs, nothing is inserted, nothing shipped.
  **No leak. Correct.**

- **The `Finish` stage's stated invariant.** `metric_sink.rs:280-283` claims the replica
  still exists and no drop interleaves between the recheck and the ship. Verified: the
  recheck is real (above), and `metric_sink_finish` inserts into `metric_sinks` and then
  `ship_dataflow(...).await` within a single `handle_message` turn; the coordinator
  processes one message at a time, so no `DropReplica` runs between the insert and the
  ship. **Invariant holds.**

- **`DROP CLUSTER` (not just `DROP REPLICA`).** `cluster_replicas_to_drop` is populated
  for a dropped cluster's replicas too, and `catalog_implications.rs:1035` calls
  `drop_replica` per replica *before* `controller.drop_cluster`
  (`catalog_implications.rs:1038-1040`). `drop_replica` → `drop_metric_sinks`
  (`ddl.rs:722`) releases each sink's instance-global collection before the instance goes
  away. **No orphaned collection on cluster drop.**

- **`drop_metric_sinks` range scan.** `range((replica_id, "")..).take_while(id == replica_id)`
  (`metric_sink.rs:326-331`) is correct for the `(ReplicaId, &'static str)` key ordering:
  `""` is the minimal `&str`, so the range starts at the first entry for `replica_id` and
  `take_while` stops at the next replica. **Correct.**

- **Crash between the two writes.** `metric_sinks` is in-memory only; `ship_dataflow`
  bottoms out in `unwrap_or_terminate`. A failure there hard-terminates envd, and on
  restart `bootstrap_metric_sinks` re-installs from the static list. There is no durable
  state to leave half-written. **No partial-failure corruption.**

- **Flag off.** `install_metric_sinks` early-returns when `ENABLE_METRIC_SINK` is off
  (`metric_sink.rs:105-107`); `drop_metric_sinks` is unconditional, so turning the flag
  off never strands a registry entry it can no longer reach. **Consistent.**

---

## Should-fix

**1. `plan_source` panics on a `source_sql` that is not exactly one statement, bypassing
the `soft_panic_or_log!` the caller wraps it in — `metric_sink.rs:355`.**

`let parsed = mz_sql::parse::parse(self.source_sql)?.into_element();`

`into_element()` panics unless the parse yields exactly one statement
(`ore/src/collections.rs:46`). The `?` handles a *parse error*, but a *successful* parse
of an empty string (0 statements) or a semicolon-joined pair (2 statements) reaches
`into_element()` and panics.

Sequence: a curated definition with `source_sql = ""` or `source_sql = "SELECT ...; SELECT ..."`
→ during `bootstrap_metric_sinks` / `install_metric_sink`, `plan_source` panics rather
than returning `Err`. The wrong outcome: the panic escapes the `match definition.plan_source(...)
{ Err(err) => soft_panic_or_log!(...) }` guard at `metric_sink.rs:159-166`, so instead of
"give up on this one sink instead of failing the boot" (the comment there, and the same
promise at `metric_sink.rs:143-147`), a single malformed definition panics the
coordinator thread on boot — a crash loop in an optimized build, an immediate panic under
CI. Every other definition bug in this file is routed through `soft_panic_or_log!`; this
one is not.

The definitions are compile-time `&'static str`, so this is low-probability, but it
directly contradicts the file's stated fault model. Fix: inspect the parsed `Vec` length
and `bail!` on `!= 1` (e.g. `parse(...)?; if v.len() != 1 { bail!("source SQL must be a
single statement") }`), so it flows through the existing soft-fail.

---

## Nits

**2. Double-install backstop overwrites-and-leaks instead of early-returning —
`metric_sink.rs:300-310`.**

```
if self.metric_sinks.insert((replica_id, definition.name), install).is_some() {
    soft_panic_or_log!("metric sink installed twice ...");
}
self.ship_dataflow(...).await;
```

`insert` *replaces* the prior `InstalledMetricSink` and returns the old one; the code
logs (in an optimized build `soft_panic_or_log!` does not panic) and proceeds to ship a
second dataflow. The old `sink_id` is now unreferenced in `metric_sinks`, so
`drop_metric_sinks` will never `drop_collections` it — an orphaned compute collection on
that replica. I could not construct a real sequence that reaches this branch (replica ids
are unique and never reused, and install fires once per replica), so this is not a live
bug — it is a backstop that, if it ever did fire, leaks rather than fails cleanly. Prefer
early-returning (or dropping the old collection) on the `is_some()` branch rather than
overwriting.

**3. Testdrive `metric-sink.td` churn case exercises only the *user* sink path, not the
curated hooks it names — `test/testdrive/metric-sink.td:227-279`.**

The comment says the case "drives the coordinator's curated metric-sink install and
teardown," but with `CURATED` empty the only thing installed across the replica churn is
the user `CREATE METRIC SINK s_churn`. What the test actually proves is that
`install_metric_sinks`/`drop_metric_sinks` run over an empty list without erroring and
that a *user* sink survives churn — which is fine and worth having, but the curated
install/teardown logic (the range scan, the staged validity recheck, the collection
release) has no runtime coverage here. Not a correctness defect in the change; flagging so
the coverage gap is not mistaken for coverage. (Rust unit tests do cover `plan_source`,
the label, and the optimizer shape.)
