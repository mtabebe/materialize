---
status: ready-for-review
category: metrics
title: PR #38149 review — correctness-under-adversity lens
updated: 2026-08-20
branch: pr-38149
outcome: No blocking correctness issues; one should-fix doc drift on mz_objects.type comment.
---

# PR #38149 review — lens: correctness under adversity

**Verdict: No blocking correctness issues found through this lens.** The change is
tightly modeled on the existing `mz_sinks` machinery, and the invariants the new
materialized view asserts are all established at create time. One user-facing doc
drift and two lower-value observations below.

Scope reviewed: parser + AST (`statement.rs`, `parser.rs`), plan
(`show.rs`, `statement.rs`, `query.rs`), the `mz_metric_sinks` MV and the
`mz_objects` union (`mz_internal.rs`, `mz_catalog.rs`), `builtin_table_updates.rs`,
and the `metric_sink.py` platform check. `parse_catalog_create_sql`
(`jsonb.rs`) and `plan_create_metric_sink` (`ddl.rs`) were read as dependencies,
not as part of the diff.

## Blocking

None.

## Should-fix

**1. `mz_objects.type` column comment omits `metric-sink`.**
`src/catalog/src/builtin/mz_catalog.rs:3460` (the `("type", ...)` entry). The
comment enumerates the exact set of type strings the column can hold ("one of
`table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`,
`secret`, `type`, or `function`"), and this PR adds a new value, `metric-sink`,
to the `mz_objects` union at `mz_catalog.rs:3467`. Sequence: with
`enable_metric_sink` on, a user creates a metric sink, then reads the column
comment (via docs or `\d`-style tooling) to learn what `type` can be; the
comment now under-reports the domain. Wrong outcome: a documented, user-facing
enumeration that no longer matches the data. Low blast radius (comment only,
value is flag-gated) but it is a claim the code contradicts.

## Nits / observations (not findings, recorded so the next reviewer needn't re-derive)

- **ASSERT-NOT-NULL blast radius onto `mz_objects` is real but not new.** The MV
  computes `parse_catalog_create_sql` in a `CROSS JOIN LATERAL` over *every*
  `Item` row before the `type = 'metric-sink'` filter, and asserts all seven
  output columns non-null; a parse error or a null on any surviving row would
  fail the MV and, transitively, `mz_objects` (which now unions it,
  `mz_catalog.rs:3467`). I traced this to a non-issue: (a) `mz_sinks`
  (`mz_catalog.rs:1337`) already runs the identical parse-over-all-items and
  already feeds `mz_objects`, so the failure surface is pre-existing and
  identical, documented at `jsonb.rs:741`; (b) the asserted columns are
  guaranteed: `plan_create_metric_sink` resolves `IN CLUSTER` into the statement
  before persisting `create_sql` (`ddl.rs:4360-4372`) so the
  `CreateMetricSink` arm's `in_cluster`/`from` are always the resolved (`Id`)
  forms (`jsonb.rs:718-726`), and `oid`/`schema_id`/`owner_id` come from the
  persisted `value` like every sibling MV. Metric sinks are flag-gated and not
  yet user-creatable, so no legacy rows predate this normalization.

- **`is_retained_metrics_object: false`** on `mz_metric_sinks`
  (`mz_internal.rs`) differs from `mz_sinks` (`true`). I considered whether an
  as-of/retained read of `mz_objects` could fail on the non-retained
  metric-sinks branch, but `mz_relations` — already a `mz_objects` input — is
  itself `false`, so `mz_objects`'s history is already bounded by a non-retained
  input and this value does not regress it. Not a correctness concern; flagging
  only so it is a conscious choice.

- **Inner joins in `show_metric_sinks`** (`show.rs:655+`) on `mz_objects`
  (`from_id`) and `mz_clusters` (`cluster_id`) will silently drop a row if
  either side is absent. Traced to safe: a metric sink's `FROM` must have a
  `relation_desc` (`ddl.rs:4351`), so it is always in `mz_relations` ⊆
  `mz_objects`, and a sink's cluster cannot be dropped out from under it. No
  reachable sequence produces a dropped row.

- **Parser rewind** for `SHOW METRIC <not-SINKS>` (`parser.rs:10192`) mirrors the
  `MATERIALIZED`/`NETWORK` arms exactly: `prev_token()` then `return None`, which
  falls through to the normal SHOW parse error. No misparse, no panic.

- **Feature-flag gating**: the parser accepts `SHOW METRIC SINKS` /
  `SHOW CREATE METRIC SINK` unconditionally; the gate is enforced in planning
  (`show.rs` `require_feature_flag(&ENABLE_METRIC_SINK)` in both
  `plan_show_create_metric_sink` and `show_metric_sinks`), so a flag-off session
  gets a clean error, not a panic. The `mz_metric_sinks` MV and the `mz_objects`
  union are unconditional, but with the flag off no metric sinks exist so both
  are empty — consistent, no adversity.
