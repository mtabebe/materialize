# Sealed Clusters and Multi-Query Optimization

## The Problem

Materialize already supports shared arrangements via differential dataflow. When
a user creates an explicit index (`CREATE INDEX`), the resulting arrangement can
be imported by multiple dataflows. The optimizer checks for matching indexes when
planning a new dataflow and, when a match exists, imports the shared arrangement
rather than building a private one.

However, arrangements that are **internal** to a dataflow the `arrange`
operators that a join or reduce builds for its own use are **not shared**
across dataflows. Each dataflow independently builds and maintains its own
private arrangements for its internal needs. For example, if two materialized views
both internally arrange `orders` by `customer_id` for their joins, but the user has
not explicitly created a matching index, those two dataflows each maintain
independent, duplicate copies of the same arranged data.

The gap between what *could* be shared and what *is* shared could lead to significant
wasted memory and CPU on clusters with many views over the same base tables.
Users must manually identify sharing opportunities via `CREATE INDEX`.

Automatic arrangement sharing, where the system identifies and consolidates
duplicate private arrangements, could close this gap. However, this is
impractical on mutable clusters, as:

- A new dataflow could arrive or be removed, at any time, invalidating the sharing strategy.
- Shared arrangements create coupling; restructuring for a new consumer could
  break existing ones.
- Compaction frontiers depend on knowing *all* consumers. An unknown future
  consumer might need timestamps already compacted away.
- The optimizer would need to pessimistically plan for unknown future workloads
  or perform expensive online re-optimization as dataflows change.

Consequently, this project consists of two components: sealing (or freezing) a cluster,
thereby establishing a closed-world, and reoptimization command to share arrangements.

## Success Criteria

1. Users can freeze a cluster's topology, establishing a closed-world assumption
   about its workload.
2. A command triggers automatic identification and consolidation of duplicate
   arrangements across all dataflows on a frozen cluster.
3. Materialized view results remain correct after optimization; identical output
   for the same input data.
4. Memory usage decreases proportionally to the number of deduplicated
   arrangements.
5. The optimization is idempotent; running it multiple times is safe.

## Out of Scope

- **Cross-dataflow common sub-expression elimination (CSE).** Sharing common
  joins, filters, or aggregations across dataflows is a future extension. This
  design addresses only arrangement deduplication.
- **Online re-optimization.** The system does not continuously re-optimize as
  workloads change. Optimization is a discrete, user-triggered step.
- **Automatic sealing.** The user must explicitly seal the cluster. Heuristics
  for auto-sealing are not addressed here.
- **Global memory budgeting and coordinated compaction.** These are future
  optimizations that sealed clusters enable but are not part of the initial
  implementation.

## Solution Proposal

The solution has two parts: **sealed clusters** establish the precondition, and
**REOPTIMIZE** performs the optimization.

### Intended Workflow

The expected user workflow for sealed clusters:

1. **Create** a cluster.
2. **Deploy** all materialized views, indexes, and other dataflows.
3. **Seal** the cluster: `ALTER CLUSTER ... SET (SEALED)`.
4. **Optimize**: `ALTER CLUSTER ... REOPTIMIZE`.
5. The cluster runs with shared arrangements replacing duplicates.

If the workload needs to change:
- **Option A:** Unseal, modify, reseal, re-optimize.
- **Option B:** Create a new cluster with the updated workload, seal and
  optimize it, then cut over.


### Part 1: Sealed Clusters

A sealed cluster is one whose compute topology is frozen. Once sealed, no new
dataflows can be created on the cluster and existing dataflows cannot be removed.
The set of materialized views, indexes, and other dataflows is fixed.



#### Syntax

```sql
-- Seal a cluster
ALTER CLUSTER my_cluster SET (SEALED);

-- Unseal a cluster
ALTER CLUSTER my_cluster SET (UNSEALED);

-- Check seal status
SELECT name, sealed FROM mz_clusters;
```

`SEALED` is a boolean cluster option, default `false`. Sealing is instant and
reversible (via `UNSEALED`).

#### Restrictions

Currently, when a cluster is sealed, the following operations are rejected:

| Blocked operation | Reason |
|---|---|
| `CREATE MATERIALIZED VIEW ... IN CLUSTER` | Cannot add dataflows |
| `CREATE INDEX ... IN CLUSTER` | Cannot add dataflows |
| `CREATE SOURCE ... IN CLUSTER` | Cannot add dataflows |
| `CREATE CONTINUAL TASK ... IN CLUSTER` | Cannot add dataflows |
| `DROP INDEX` (on sealed cluster) | Cannot remove dataflows |
| `DROP MATERIALIZED VIEW` (on sealed cluster) | Cannot remove dataflows |
| `DROP CLUSTER REPLICA` | Cannot alter compute resources |
| `ALTER CLUSTER ... SET (SIZE)` | Cannot alter compute resources |
| `ALTER CLUSTER ... SET (REPLICATION FACTOR)` | Cannot alter compute resources |
| `ALTER CLUSTER ... SET (AVAILABILITY ZONES)` | Cannot alter compute resources |
| `ALTER CLUSTER ... SET (SCHEDULE)` | Cannot alter compute resources |

The following operations remain allowed:

| Allowed operation | Reason |
|---|---|
| `ALTER CLUSTER ... SET (SEALED/UNSEALED)` | Toggle seal state |
| `ALTER CLUSTER ... RENAME` | Metadata-only change |
| `DROP CLUSTER ... CASCADE` | Full teardown is always allowed |
| `ALTER CLUSTER ... REOPTIMIZE` | The optimization pass |
| `SELECT` from MVs on the cluster | Reads are unaffected |
| DML (`INSERT`, `UPDATE`, `DELETE`) on tables | Writes to base tables (and thus dependent MVs) are unaffected; sealed clusters only restrict topology changes |

#### Catalog representation

The `sealed` field is stored as a boolean on `ClusterConfig`, persisted in the
durable catalog, and exposed via the `mz_clusters` system table. A catalog
migration (v80 → v81) adds the field.

### Part 2: REOPTIMIZE

`ALTER CLUSTER <name> REOPTIMIZE` analyzes all dataflows on a sealed cluster,
identifies duplicate private arrangements, creates shared indexes to consolidate
them, and re-optimizes each materialized view to use the new shared indexes.

#### Syntax

```sql
-- Requires a sealed cluster
ALTER CLUSTER my_cluster SET (SEALED);
ALTER CLUSTER my_cluster REOPTIMIZE;
```

Running REOPTIMIZE on an unsealed cluster returns an error. The command is
idempotent running it multiple times is safe (subsequent runs are no-ops if
nothing has changed).

#### Architecture

The command flows through the standard pipeline:

```
SQL Parser → Planner → Sequencer → Compute Controller → Compute Replicas
```

The sequencer is where the core logic lives, implemented in four phases, and defined in reoptimize.rs

##### Phase 1: Arrangement Inventory

Walk every catalog item on the cluster, explicit indexes and materialized
views, and build a complete inventory of all arrangements, both shared and
private.

For **explicit indexes**, the arrangement identity is directly available as an
`(on_id, key)` pair from the `IndexDesc`.

For **materialized views**, the private arrangements are extracted from the
stored MIR plans by analyzing operator nodes:

| MIR operator | Arrangement extracted |
|---|---|
| `Join` (Differential/DeltaQuery) | Input collections arranged by join keys |
| `Reduce` | Input arranged by group-by keys |
| `ArrangeBy` | Explicit arrangement requests |
| `TopK` | Input arranged by group key |

The extractor looks through wrapper operators (Map, Filter, Project) to find
the underlying `Get(Id::Global(gid))` only arrangements on named collections
can be shared across dataflows.

**Note:** The catalog's expression cache stores both the
original optimized MIR (`DataflowDescription<OptimizedMirRelationExpr>`) and
the physical plan for every catalog object. This means we have the full
optimization input available we do not need to reverse-engineer anything from
the running dataflow graph.

##### Phase 2: Optimization Plan

Compare the inventory against existing shared indexes on the cluster. For each
`(on_id, key)` pair with two or more distinct consumers:

- If already backed by an explicit index → no action needed.
- If not → candidate for a new shared index.

Single-consumer arrangements are skipped there is no benefit to sharing when
only one dataflow needs the arrangement.

##### Phase 3: Shared Index Creation

For each candidate, the system:

1. Allocates catalog IDs for the new index.
2. Derives metadata (ownership, schema) from the indexed collection.
3. Generates a system index name: `mz_reoptimize_{collection}_{global_id}`.
4. Runs the index through the standard MIR → LIR optimization pipeline.
5. Generates synthetic `CREATE INDEX` SQL for catalog persistence.
6. Atomically inserts all new index entries via a catalog transaction.
7. Ships the index dataflows to the compute cluster.

##### Phase 4: MV Re-optimization

With new shared indexes available, each materialized view is re-optimized:

1. Retrieve the stored MIR from the expression cache.
2. Re-run the optimizer with the fresh compute instance snapshot which now
   includes the new shared indexes. The optimizer already knows how to use
   available indexes (e.g., choosing delta joins when all required arrangements
   exist).
3. Generate a new physical plan (LIR).
4. Atomically replace the old dataflow via `try_replan_dataflow()` this
   removes the old export and ships the new dataflow in one operation.
5. Re-enable writes for the MV.

The result is that materialized views import shared arrangements instead of
maintaining private duplicates, reducing memory proportionally to the number of
deduplicated arrangements.

##### What REOPTIMIZE does not do

The set of `(collection, key)` pairs is derived from the existing plans
(inventory); we only consolidate duplicates by creating shared indexes. We do
not introduce new arrangement keys that were not already implied by the MVs.
However, because re-optimization re-runs the full optimizer with the new shared
indexes available, it *may* select different join implementations (e.g.
switching from a differential join to a delta join when all required
arrangements are now shared). The arrangement *keys* stay the same; the join
*strategy* and physical sharing may change.


### End-to-End Example

Setup: two tables, data, and a cluster with two materialized views that both
join `orders` and `customers` on the same key (so both arrange `customers` by
`id` and `orders` by `customer_id`):

```sql
-- Setup
CREATE TABLE customers (id INT, name TEXT);
CREATE TABLE orders (id INT, customer_id INT, amount INT);

INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 3, 300);

CREATE CLUSTER reopt_test SIZE 'scale=1,workers=1';
SET cluster = reopt_test;

-- Two MVs that join on the same key (both arrange customers by id)
CREATE MATERIALIZED VIEW mv_spend IN CLUSTER reopt_test AS
  SELECT c.name, SUM(o.amount) AS total
  FROM orders o JOIN customers c ON o.customer_id = c.id
  GROUP BY c.name;

CREATE MATERIALIZED VIEW mv_count IN CLUSTER reopt_test AS
  SELECT c.name, COUNT(*) AS cnt
  FROM orders o JOIN customers c ON o.customer_id = c.id
  GROUP BY c.name;

-- Seal and reoptimize
ALTER CLUSTER reopt_test SET (SEALED);
ALTER CLUSTER reopt_test REOPTIMIZE;
```

Without REOPTIMIZE, each MV independently maintains private arrangements of
`orders` by `customer_id` and `customers` by `id` four arrangements total for
two distinct `(collection, key)` pairs.

After REOPTIMIZE:

- **Phase 1** inventories all arrangements and finds `customers(id)` and
  `orders(customer_id)` each appear in both dataflows.
- **Phase 2** determines both need new shared indexes (no explicit index
  exists).
- **Phase 3** creates shared indexes (e.g. `mz_reoptimize_customers_<id>` and
  `mz_reoptimize_orders_<id>`).
- **Phase 4** re-optimizes both MVs. The optimizer now sees the shared indexes
  and imports them instead of building private arrangements.

## Alternatives

### User-managed indexes

The status quo: users manually create `CREATE INDEX` statements to enable
sharing. This works but requires deep knowledge of internal arrangement
structure, provides no visibility into what arrangements dataflows actually
maintain, and scales poorly as workload complexity grows.

### Always-on automatic sharing

Rather than requiring an explicit seal + optimize step, the system could
continuously share arrangements as dataflows are created. However, this has
the following trade-offs:

- The open-world assumption makes it unsafe to commit to globally optimal
  sharing decisions.
- Compaction frontiers cannot be set aggressively without knowing all consumers.
- The overhead of continuous re-optimization would be significant.
- A discrete optimization step is simpler to reason about and debug.


## Open Questions/Directions

1. **Stale results during REOPTIMIZE.** During the transition window where old
   dataflows are torn down and new ones hydrate, the cluster may serve stale
   results. How should this be communicated to users? Should there be a progress
   indicator?

2. **Memory during transition.** Old and new dataflows may briefly coexist in
   memory during REOPTIMIZE. For memory-constrained clusters, this could be
   problematic. A staged approach (one dataflow at a time) would reduce peak
   memory but increase total transition time.

3. **Cancellation semantics.** If a user cancels REOPTIMIZE mid-execution, the
   cluster should be left in a consistent state. The current implementation
   uses atomic catalog transactions but the compute-side transition may need
   additional safety.

4. **Compatible but non-identical arrangements.** Two arrangements might have
   the same key but different upstream filters. Could the superset arrangement
   serve both? This is a potential future optimization not addressed in the
   current implementation.

5. **Replicas on sealed clusters.** The current implementation blocks
   `CREATE CLUSTER REPLICA` and `DROP CLUSTER REPLICA` on sealed clusters. We
   should allow adding and removing replicas on sealed clusters so that
   operations can keep things running (e.g. replace a failing replica).
   Some thinking needed on how this interacts with the closed-world assumption
   and correctness.

6. **Future: cross-dataflow CSE.** Beyond arrangement deduplication, common
   sub-expressions (shared joins, filters, aggregations) across dataflows could
   be computed once and shared. This is full multi-query optimization and is a
   significantly larger project that sealed clusters enable but do not yet
   implement.

7. **GlobalIDs and replacement.** We cannot reuse GlobalIDs when the logical
   object changes (e.g. a re-optimized materialized view). Support for this
   exists via the alter … replacement work; the same applies to all other
   compute objects (indexes, sources, etc.) that might be re-created or
   re-optimized.

8. **Multi-output dataflows.** Deeper sharing (e.g. cross-dataflow CSE) would
   require a single dataflow to produce multiple exported collections (e.g. one
   shared join feeding several MVs). Today each dataflow typically has one
   “sink” export (one MV or one index). Compute’s model allows multiple exports
   per dataflow in principle, but code paths in the coordinator, controller,
   or compaction/read-policy logic may assume “one export per dataflow” for
   simplicity (e.g. when iterating exports, installing read policies, or
   reporting status). We should validate there is no conceptual dependency on
   that assumption so that multi-output dataflows can be used when we implement
   CSE or other merged-dataflow optimizations.

9. **Finer-grained scheduling / intra-dataflow hydration.** Today we have
   sequential hydration at dataflow boundaries. With merged or more heavily
   shared dataflows, we need intra-dataflow scheduling: hydrate parts of the
   graph so that temporary resource usage stays within cluster capacity. We would
   need to: analyze the graph and hydrate first the parts that, after
   hydration, prune or reduce resource use, so the cluster can complete
   hydration without oversubscribing resources.

10. **New replica or new cluster for optimization.** It may be desirable to
    create a new replica (or a new cluster) to run the optimized workload from
    scratch rather than carrying old state. Creating a new cluster also helps
    determine whether the configuration can hydrate at all. Related: per-replica
    optimization. Today all replicas in a cluster are expected to behave the
    same way, which is important for correctness. Allowing per-replica
    optimization (e.g. one replica hydrates first, others follow) would require
    careful thinking.
