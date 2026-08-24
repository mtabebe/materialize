# Review of PR #38146 — through SangJunBak's lens

**Verdict:** The docs and naming are unusually careful and mirror the introspection-subscribe
module well. My lens turns up no blocker, but the whole reason this file exists — the
coordinator-driven install pipeline — is never actually run by a test, and one invariant is
guarded by a comment instead of a test. Fix the coverage before this stops being a scaffold.

## Blocking

None through this lens. `CURATED` is empty, so with the flag on or off the running system
behaves as today; correctness of the live install path is another reviewer's lens.

## Should-fix

- **The new install pipeline has zero end-to-end coverage — reproduce it before shipping.**
  `metric_sink.rs:81` ships `const CURATED: &[CuratedMetricSink] = &[]`, so
  `metric_sink_optimize` → `metric_sink_finish`, the read-hold as-of pick, the
  `metric_sinks.insert`, the `PlanValidity` replica recheck, and `drop_metric_sinks` never
  run in any test. The testdrive churn block (`test/testdrive/metric-sink.td:227+`) is honest
  that it only asserts a *user* sink survives replica churn — the new hooks it names
  (`install_metric_sinks` / `drop_metric_sinks`) fire over an empty list and do nothing. The
  unit tests exercise `plan_source` and the optimizer `Query` shape in isolation, but the
  coordinator staging that is the point of this PR is untested. This is the "solid happy-path
  test, now do the real path" ask: add a `#[cfg(test)]`-only curated definition (or a Rust
  integration test) that populates `CURATED`, then assert its series shows up on a replica and
  is gone after the replica is dropped. Right now nothing tells the next person the finish
  stage or the teardown range-scan regressed.

- **Curated `name` uniqueness is a doc-only invariant with no guard.** `metric_sink.rs:63`
  says the name "Must be unique within `CURATED`", but nothing checks it — unlike prefix
  validity, which has the `curated_prefixes_are_valid` guard test. A duplicate name is an
  illegal state left representable: `metric_sinks.insert((replica_id, name), …)` soft-panics
  "installed twice" per replica, and two definitions collide on the same `sink` health-gauge
  label. Add a `curated_names_are_unique` test right next to `curated_prefixes_are_valid`
  (collect names into a `BTreeSet`, assert `len` matches) so a bad addition fails CI instead of
  soft-panicking in prod.

## Nits

- **Leftover doc lines on `sink_label`** — `src/adapter/src/optimize/metric_sink.rs:804-806`.
  The first two lines ("The assembled dataflow exports exactly one `MetricSink`, reading the
  shaped view rather than the source relation directly.") are copy-paste from
  `optimizer_exports_one_metric_sink` and describe a different function; only line 806 ("The
  `sink` label carried by the export's connection.") is about `sink_label`. Cut the first two.

- **`metric_label` vs `label` for the same value.** The builder field/param is
  `MetricSink::metric_label` (`src/adapter/src/optimize/metric_sink.rs:115`) but it lands in
  `MetricSinkConnection::label` (`src/compute-types/src/sinks.rs`). Same value, two names.
  Align on `label` so a reader doesn't have to confirm they're the same thing.

- **`drop_metric_sinks` teardown is subtle and untested.** The
  `range((replica_id, "")..).take_while(|((id, _), _)| *id == replica_id)` range-scan over the
  replica-first key is correct, but it is exactly the kind of index logic that wants a test —
  folded into the coverage ask above rather than a separate item.

- **Bare `TODO` for introspection-disabled replicas** in `install_metric_sinks`
  (`src/adapter/src/coord/metric_sink.rs`, the "Skip replicas created with introspection
  disabled" comment). Harmless while `CURATED` is empty, but a curated `source_sql` reading
  introspection relations would silently publish empty series on such a replica. Point the TODO
  at a tracking issue so it isn't the thing someone rediscovers by adding the first definition.
