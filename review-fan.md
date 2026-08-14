---
status: ready-for-review
category: cluster-controller
title: PR #38112 review — distilled (fanned lenses)
updated: 2026-08-14
outcome: One blocking rebase hazard (migration step pinned to stale 26.38.0-dev.0 while main is already 26.39.0-dev.0, verified against origin/main → skipped on 26.38→26.39 upgrade → catalog-open panic). Otherwise a well-shaped, compiler-enforced fix; the substantive follow-up is the audit-event gap.
---

# Review — PR #38112 (distilled from 5 lenses)

**Verdict: Request changes — one blocking item, resolvable at rebase.** The core
fix is the right shape and every lens agrees on it: the reshape reset and sync
cut-over now route through the exhaustively-destructured
`realized_reconfiguration_target()` / `apply_reconfiguration_target()` helpers, so
the "forgot a shape dimension" bug class cannot recur *at those sites* (it fails to
compile). Tests fail without the fix, docs and goldens move in-commit. The one
blocker is a version-pin hazard that only bites on merge; the one substantive
should-fix is the durable audit-log gap that two independent reviewer lenses
reached on their own.

---

## Blocking

### B1. Migration step pinned to stale `26.38.0-dev.0` → skipped on the 26.38→26.39 upgrade → catalog-open panic
`src/adapter/src/catalog/open/builtin_schema_migration.rs:362-367`

The new `MigrationStep::replacement` for `mz_cluster_reconfigurations` is pinned to
`26.38.0-dev.0`. **This is verified against the tree, not inferred:** the PR branch
carries `version = "26.38.0-dev.0"`, but `origin/main` is already at
`26.39.0-dev.0` (commit `8c3d47f71c release: bump to version v26.39.0-dev.0`). So
on the branch the pin is self-consistent, but the moment it rebases onto current
main it is stale.

Failure sequence once rebased (from the correctness lens, mechanism confirmed):
1. `mz_cluster_reconfigurations` already shipped in the **26.38.0** release with
   the four-dimension `changes` diff, so its durable shard carries the old
   fingerprint.
2. This PR changes the MV (`changes` now emits `arrangement_compression`),
   producing a new fingerprint, first appearing in a **26.39** build.
3. A prod env upgrades `26.38.0 → 26.39.0`: `source_version = 26.38.0`,
   `build_version = 26.39.0`.
4. `plan_migration` keeps only steps with `s.version > source_version`
   (`builtin_schema_migration.rs:726`). In semver a pre-release orders *below* its
   release, so `26.38.0-dev.0 > 26.38.0` is **false** → the step is filtered out.
5. The MV is not replaced; the shard keeps the old fingerprint while the build
   expects the new one. The catalog-open fingerprint check fails and `environmentd`
   **panics** — a failed 0dt upgrade, not a soft degradation.

The correct pin is `26.39.0-dev.0`: `26.39.0-dev.0 > 26.38.0` and `<= 26.39.0`, so
the step fires in the half-open interval. (This is exactly why main's existing
`26.37.0-dev.0` steps are correct — they cover the `26.37→26.38` boundary.)

**Fix:** bump the step to `26.39.0-dev.0` when rebasing onto current main, per the
step's own NOTE ("must stay at the workspace's current dev version until the change
ships"). The change will ship in 26.39.

> **Lens disagreement, resolved.** swe, antiguru, and ggevay each explicitly
> checked this pin and called it *correct* — but all three verified only against
> the **branch's** `src/environmentd/Cargo.toml` (`26.38.0-dev.0`), which the pin
> does match. None checked `origin/main`, where the version has already moved to
> `26.39.0-dev.0`. The correctness lens flagged it as a rebase hazard precisely
> because it looked past the branch. I confirmed main's version directly, so the
> hazard is real, not hypothetical. Note the branch-local checks aren't *wrong* —
> the pin is fine in isolation — which is why this needs a human to bump it at
> rebase rather than being a defect in the current diff.

---

## Should-fix

### S1. Compression-only reconfiguration writes an audit event indistinguishable from a no-op — no tracking issue
`src/adapter/src/catalog/transact.rs:588-611` (destructure drops `arrangement_compression` at `:593-596`)

*Reached independently by two reviewer lenses (antiguru + ggevay) — the agreement
is the signal.* The audit destructure drops `arrangement_compression`, so a
compression-only ALTER emits `Started`/`Finalized` `AlterClusterReconfigurationV1`
events whose `target_size` / `target_replication_factor` /
`target_availability_zones` / `target_logging` are byte-identical to the
pre-reconfiguration shape. This is not merely an omitted field: to an operator
reading `mz_audit_events` (a durable, append-only, user-facing surface) a real
compression reconfiguration is **indistinguishable from a no-op** — there is no
field explaining *why* the cluster reconfigured. antiguru calls this "the
load-bearing gap."

Deferring the fix itself is reasonable: `AlterClusterReconfigurationV1` is a
versioned append-only audit type (`audit-log/src/lib.rs:415`), so carrying the
dimension needs a proto bump. **The ask is process, not code:** file a tracking
issue and reference it from the comment at `transact.rs:593`, so "intentionally
not part of it" points somewhere instead of evaporating from the PR body on merge.

### S2. A compression `RESET`'s *effect* is verified nowhere
`test/sqllogictest/managed_cluster.slt:477` (swe) / `~466` (antiguru)

*swe (should-fix) + antiguru (nit) agree on the gap; they differ on priority.*
Before this PR the compression `RESET` was followed by a `SHOW CREATE` readback
confirming compression flipped back — but that readback only worked *because* of
the leak this PR removes. The PR correctly drops it (`RESET` can't carry `WAIT`, so
slt can't observe the record-path result deterministically) and leaves the RESET
"exercised for acceptance only". Net: no test anywhere asserts a compression RESET
actually drives the realized value back to default — only that the statement
parses. `cluster-controller.td` covers the compression transition via `SET`, not
`RESET`. Classic coverage-without-verification: a regression in RESET's *effect*
would pass green.

**Concrete recovery (swe):** add the RESET to `test/testdrive/cluster-controller.td`
where the record settles and testdrive's `>` retry makes the readback
deterministic (the new `cc_compression` section already proves this works for
`SET`), then assert `create_sql` no longer contains
`EXPERIMENTAL ARRANGEMENT COMPRESSION = true`.

### S3. The new `'arrangement compression'` branch of the `SHOW CLUSTERS` activity summary has no runtime assertion
`src/catalog/src/builtin/mz_internal.rs:5656` (swe)

The only runtime check of that summary string
(`test/testdrive/cluster-controller.td:861`) covers size/replication-factor/
introspection and never compression; the `catalog_server_explain.slt` golden
validates plan *shape*, not the rendered string. Modest, because the `changes`
diff feeding the summary *is* tested — only the final rendering is unexercised.
The `cc_preserve` section already holds a record in-flight and reads other
reconfiguration fields, so it could add `activity LIKE '%arrangement compression%'`
cheaply.

---

## Nits

### N1. Both SQL dimension lists stay hand-maintained — the exact trap the Rust side just eliminated
`src/catalog/src/builtin/mz_internal.rs:953` (the `changes` CASE-chain) and `:5656` (the `SHOW CLUSTERS` summary array)

*Converged on by four lenses from different angles: swe (hand-maintained lists),
ggevay (cross-struct JSON-key convention), correctness N2, dataflow nit.* The
`changes` diff and the summary array each enumerate the five dimensions by hand,
and nothing couples them to `ReconfigurationTarget` — the compile-time destructure
guards in `memory/objects.rs` will **not** fire when a future sixth dimension is
forgotten in these SQL strings. This PR is itself the second time such a list was
missed. Two facets worth one shared `NOTE:`:
- SQL can't destructure, so there's no structural fix — a short `NOTE:` on one list
  cross-referencing `ReconfigurationTarget` (and the other list) flags the coupling
  for the next person adding a dimension (swe).
- The diff also silently assumes **JSON-key parity**: it compares
  `config->'arrangement_compression'` (serialized from `ClusterVariantManaged`)
  against `target->'arrangement_compression'` (from `ReconfigurationTarget`), two
  independently-serialized Rust types that agree only because both name the field
  identically. A stray `#[serde(rename)]` would make the diff permanently non-empty
  after cut-over. Not introduced by this PR, and the `cc_compression` td assertion
  (`changes::text = '{}'` after finalize) is the thing "at the other end" that
  catches it — but the assumption deserves the same NOTE (ggevay, dataflow,
  correctness N2).

### N2. The three observability surfaces now disagree on compression until the follow-up lands
`changes` + `SHOW CLUSTERS` include it (`mz_internal.rs:957,5656`), audit omits it (`transact.rs:593`)

The PR body discloses this and the audit omission is compile-time-visible
(`arrangement_compression: _` with an explaining comment), so it's deliberate, not
an accident. Recorded for the reviewer; the actionable half is S1.

---

## Not substantiated / dropped

Nothing was dropped for lack of grounding — every finding carries a `file:line` and
a concrete failure mode. The **dataflow lens** returned a clean LGTM: the only two
rendered surfaces (`mz_cluster_reconfigurations` MV, `mz_show_clusters` view) take a
purely additive stateless scalar term with no new arrangement, no
retraction/consolidation hazard, no frontier touch, per-row cost on a catalog-sized
input. No dataflow finding to rank.

---

## Where the lenses landed

| Lens | Verdict | Load-bearing point |
|------|---------|--------------------|
| correctness | Request changes | B1 (only lens to catch the rebase pin) |
| dataflow | LGTM | no dataflow surface at risk |
| swe | Approve | S2 + S3 coverage gaps |
| antiguru | Approve | S1 audit gap (load-bearing), S2 |
| ggevay | Approve | S1 audit gap; verified B1 pin *against branch* (missed main) |

The four Approve lenses would flip to Request-changes on B1 once main's version is
in view. Consensus otherwise: the fix is correct and well-modelled; the real
follow-up work is the durable audit-log gap (S1).
