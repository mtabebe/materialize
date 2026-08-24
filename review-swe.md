# PR #38146 review — Engineering-quality lens

**Verdict:** Ship-able as a scaffold, but the 472-line orchestration it adds is
untested by construction (the curated list is empty), and one leftover doc comment
should be cleaned up. No blocking defects for *this* lens.

The shape is good: the optimizer gains a `MetricSinkFrom::{Id,Query}` seam instead of
a forked second optimizer, the install/drop hooks mirror `introspection`'s existing
triggers, and `FeatureFlag::enabled` is a clean extraction that `require` now delegates
to. The findings below are about verification and comment rot, not design.

## Blocking

None under this lens.

## Should-fix

- **`src/adapter/src/coord/metric_sink.rs:81` — the new machinery has no test that
  would fail if it regressed.** `CURATED` is `&[]`, so every code path that matters in
  this file — `install_metric_sink` → `metric_sink_optimize` → `metric_sink_finish`
  (read-hold acquisition, as-of pick, registry insert, `ship_dataflow` targeting a
  replica) and `drop_metric_sinks` (the range-scan teardown) — is never entered by any
  test. The tests that exist (`plan_source_enforces_the_metric_sink_contract`,
  `curated_prefixes_are_valid`, the optimizer/gauge tests) cover leaf helpers only.
  The testdrive block at `test/testdrive/metric-sink.td:227` is honest that it runs the
  hooks "over an empty list without erroring" — that is coverage of a no-op, not
  verification. What a maintainer loses: the first PR to add a real `CURATED` entry is
  also the first time this pipeline runs, so a latent bug in read-hold ordering, the
  `(ReplicaId, name)` key, or replica targeting ships with no CI signal.
  Concrete alternative: gate a single fixture definition into `CURATED` behind
  `#[cfg(test)]` (or give `install_metric_sink` a test-only entry point that takes a
  `&CuratedMetricSink`) and drive one install+drop through the staged pipeline in a
  coordinator test, so the orchestration has one path CI actually exercises.

## Nits

- **`src/adapter/src/optimize/metric_sink.rs:804-806` — stale doubled doc comment on
  `sink_label`.** The first two lines ("The assembled dataflow exports exactly one
  `MetricSink`, reading the shaped view rather than the source relation directly.") are
  copy-paste residue from the old `optimizer_exports_one_metric_sink` doc and describe
  the wrong function; only line 806 ("The `sink` label carried by the export's
  connection.") applies. Delete lines 804-805. This is exactly the comment that rots:
  it now contradicts the helper it sits on.

- **`src/adapter/src/coord/metric_sink.rs:426` (`curated_prefixes_are_valid`)** iterates
  the empty `CURATED` and asserts nothing today. Fine as forward insurance once the list
  grows, but note it is inert now — it does not stand in for the orchestration coverage
  called out above.
