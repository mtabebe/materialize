---
status: ready-for-review
category: review
title: "PR #38149 engineering-quality lens"
updated: 2026-08-20
outcome: "No blocking issues. Well-tested, seam-respecting change; a few nits."
---

# Review: PR #38149 — metric sinks: SHOW, catalog visibility, and RBAC (SQL-572)

**Lens: engineering quality — would a maintainer thank you for this in a year?**

**Verdict: No blocking issues.** This is a clean, minimal, seam-respecting change with
genuinely verification-grade tests. Findings below are nits only.

---

## Blocking

None.

## Should-fix

None.

## Nits

### 1. `SHOW METRIC SINKS` reaches around the `mz_show_*` seam that `SHOW SINKS` uses
`src/sql/src/plan/statement/show.rs:658` (`show_metric_sinks`) inlines the
object/cluster joins directly into the query string, whereas `show_sinks`
(`src/sql/src/plan/statement/show.rs` just above) selects from a dedicated
`mz_internal.mz_show_sinks` view that pre-resolves cluster/comment. Both patterns
exist in this file (`show_indexes`/`show_columns` also inline), and inlining here is
the *smaller* change — adding a `mz_show_metric_sinks` builtin would be more surface,
not less — so this is fine as written. Worth a one-line note only so the next
maintainer who wants to add a `comment` column to the output knows there is no
`mz_show_metric_sinks` helper to extend; they will be editing this SQL string.
No action required.

### 2. Redacted `SHOW CREATE` path has parser coverage but no end-to-end test
The `redacted` bool is plumbed through `plan_show_create_metric_sink`
(`src/sql/src/plan/statement/show.rs:210`) into `plan_show_create_item`, and the
parser testdata covers `SHOW REDACTED CREATE METRIC SINK foo`
(`src/sql-parser/tests/testdata/show`). But `test/sqllogictest/metric_sink.slt` only
executes the non-redacted `SHOW CREATE METRIC SINK ms` round-trip (line ~305); no slt
exercises the redacted execution path. Risk is low because it delegates to the same
shared `plan_show_create_item` the other object types use, so a redaction bug would be
caught by their tests — but a one-line `SHOW REDACTED CREATE METRIC SINK ms` assertion
would close the gap for the price it costs.

### 3. `mz_metric_sinks` carries full `column_comments` yet is marked `RELATION_SPEC_UNDOCUMENTED`
`src/catalog/src/builtin/mz_internal.rs` gives the MV a complete `column_comments`
block, but `doc/user/content/reference/system-catalog/mz_internal.md:719` registers it
as `RELATION_SPEC_UNDOCUMENTED`. Meanwhile the relation surfaces in `mz_objects` as the
user-visible type `metric-sink`. This is the right call *while the feature is gated*
(`enable_metric_sink` defaults off in prod), but there is no TODO or reminder tying the
doc to the flag, so when the flag GAs the undocumented marker is easy to leave behind.
A `TODO(SQL-...)` next to the marker, or on the flag, would make the debt discoverable.

---

## Things I checked and deliberately did NOT flag (so another reviewer doesn't)

- **~100 lines of ID churn in `mz_catalog_server_index_accounting.slt`, plus row-count
  bumps in `catalog_server_explain.slt`, is unavoidable, not sloppiness.** `mz_objects`
  must `UNION ALL` `mz_metric_sinks` (`src/catalog/src/builtin/mz_catalog.rs:3467`), so
  the new builtin has to precede `MZ_OBJECTS` in `BUILTINS_STATIC`. A
  `test_builtins_static_dependency_order` test (`src/catalog/src/builtin.rs:1683`)
  enforces that dependencies come first. The MV is placed at `builtin.rs:1175`, only ten
  entries before `MZ_OBJECTS` at `:1185` — i.e. already as late as the topo constraint
  allows. Everything after `MZ_OBJECTS` renumbers on a *fresh* boot regardless; on a real
  upgrade the persisted name→id mapping is authoritative so nothing renumbers. The churn
  is a fixture artifact of the feature's shape, not a placement mistake. Appending at the
  end (the usual "minimize churn" advice) would violate the dependency-order test.

- **The new comment in `builtin_table_updates.rs:193` is accurate.** It claims tables,
  views, and metric sinks are exposed via materialized views derived from
  `mz_catalog_raw`; `MZ_TABLES` (`mz_catalog.rs`) does read `mz_internal.mz_catalog_raw`,
  so the claim holds. The security `NOTE:` it replaces (SELECT-not-ownership egress gap)
  was correctly removed — this PR *is* the catalog relation that closed that gap.

- **The platform-check rewrite is a strict improvement.** `metric_sink.py` `validate()`
  swaps the "re-CREATE and match already-exists" proxy for real `SHOW METRIC SINKS`
  assertions, and *keeps* the two probes that actually mattered: the FROM-edge survival
  (`DROP VIEW ... still depended upon`) and the per-replica dataflow-registry metric
  probe. The class docstring's "probes for the dataflow, not just the item" is still true.

- **Test seams are verification-grade, not happy-path coverage.** `metric_sink.slt`
  asserts the `mz_objects` union preserves identity (`sinks.oid = objs.oid AND
  sinks.id = objs.id`), that a metric sink survives the `mz_show_all_objects` comment
  LEFT JOIN it can never match, that `SHOW SINKS` and `SHOW METRIC SINKS` do not bleed
  into each other, and that the audit `object_type` CASE and `mz_object_history`
  namespace both accept `metric-sink`. Each of these would fail without the
  corresponding line of the change.
