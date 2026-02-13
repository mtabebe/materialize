# Plan: Add SEALED/UNSEALED Cluster Option

A **sealed cluster** is a cluster that has been irreversibly marked as immutable. Once sealed, no new dataflows (materialized views, indexes, subscriptions, etc.) can be created on the cluster, and existing dataflows cannot be removed. The set of dataflows running on a sealed cluster is frozen for its lifetime.

The primary motivation is to enable **multi-query optimization (MQO)** — specifically, sharing physical arrangements across dataflows — which requires a closed-world assumption about the workload running on a cluster.

## Problem

Today, every dataflow on a cluster independently maintains its own arrangements, even when multiple dataflows maintain arrangements over the same data with the same or compatible keys. This leads to duplicated memory usage and redundant maintenance work.

Multi-query optimization could eliminate this duplication by identifying shared arrangements and consolidating them. However, MQO is impractical on mutable clusters because:

- A new dataflow could arrive at any time, invalidating the current sharing strategy.
- Shared arrangements create coupling — restructuring an arrangement for a new dataflow could break existing consumers.
- The compaction frontier for an arrangement depends on knowing *all* consumers. An unknown future consumer might need timestamps that have already been compacted away.
- The optimizer would need to either pessimistically plan for unknown future workloads, or perform expensive online re-optimization as dataflows are added and removed.

Sealing the cluster establishes a **closed-world assumption**: the full workload is known and will not change. This makes it safe to commit to globally optimal physical decisions.

## Intended Workflow

1. **Create** a cluster.
2. **Deploy** all desired materialized views, indexes, and other dataflows onto it.
3. **Seal** the cluster. This triggers re-optimization of the physical plan.
4. The cluster now runs an optimized, consolidated execution plan.

If the workload needs to change, the user creates a new cluster, deploys the updated set of dataflows, seals it, and cuts over.

## Optimizations Enabled

### Arrangement Sharing (Primary)

The core optimization. When multiple dataflows maintain arrangements over the same collection with compatible keys, they can share a single physical arrangement instead of each maintaining their own copy.

For example, if three materialized views all join against `orders` keyed by `customer_id`, a single shared arrangement of `orders` by `customer_id` can serve all three, reducing memory usage and update maintenance by up to 3x for that arrangement.

### Cross-Dataflow Common Sub-Expression Elimination

Beyond arrangement sharing, any common sub-expression across dataflows can be computed once: shared filters, maps, joins, or aggregations. If two views both compute `SELECT ... FROM orders JOIN customers ON ... WHERE region = 'US'`, the filter and join can be evaluated once and the result shared.

### Global Memory Budgeting

With a known, fixed set of dataflows, memory allocation can be optimized globally rather than reserving headroom for unknown future workloads. The system can make precise decisions about which arrangements to keep in memory vs. spill to disk.

## Example

Consider a cluster running three materialized views:

```sql
CREATE MATERIALIZED VIEW revenue_by_region AS
  SELECT r.name, SUM(o.amount)
  FROM orders o JOIN regions r ON o.region_id = r.id
  GROUP BY r.name;

CREATE MATERIALIZED VIEW orders_by_customer AS
  SELECT c.name, COUNT(*)
  FROM orders o JOIN customers c ON o.customer_id = c.id
  GROUP BY c.name;

CREATE MATERIALIZED VIEW recent_large_orders AS
  SELECT *
  FROM orders
  WHERE amount > 1000 AND created_at > now() - INTERVAL '7 days';
```

Without sealing, each view independently maintains its own arrangement of `orders`. After sealing, the optimizer can:

- Share a single base arrangement of `orders` across all three views.
- If the two join views use compatible partitioning, share exchange operators.
- Compact the `orders` arrangement knowing exactly which timestamps the three views require.

## Trade-offs

**Benefits:**
- Reduced memory usage from shared arrangements.
- Reduced CPU from eliminated redundant computation.
- More aggressive compaction from known consumer sets.
- Better resource utilization from global planning.

**Costs:**
- Immutability — adding or removing a dataflow requires creating and migrating to a new cluster.
- Re-optimization at seal time may take non-trivial time for large clusters.
- Shared arrangements introduce coupling that could complicate debugging or per-dataflow resource accounting.



# Syntactical Design

## Goal
Support `ALTER CLUSTER ... SET (SEALED)` / `SET (UNSEALED)` to lock a cluster's dataflow graph (prevent new dataflows while allowing reads/subscribes). Persist this state and expose it via `mz_clusters`.

## Supported SQL Syntax
```sql
ALTER CLUSTER foo SET (SEALED)           -- seal the cluster
ALTER CLUSTER foo SET (SEALED = true)    -- seal the cluster
ALTER CLUSTER foo SET (SEALED = false)   -- unseal the cluster
ALTER CLUSTER foo SET (UNSEALED)         -- unseal the cluster
ALTER CLUSTER foo SET (UNSEALED = false) -- seal the cluster
ALTER CLUSTER foo RESET (SEALED)         -- unseal (reset to default = unsealed)
```

## Design Decision
- `sealed` is a **top-level property on `ClusterConfig`** (like `workload_class`), not nested inside `ClusterVariantManaged`. This means both managed and unmanaged clusters can be sealed.
- Default value: `false` (unsealed).


