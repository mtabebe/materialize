# Test: Why does a replica have freshness problems?

## Ontology Queries

### 1. What relates to replicas?

```sql
SELECT l.name, l.source_entity, l.target_entity, e.relation
FROM ma_mz_ontology.link_types l
JOIN ma_mz_ontology.entity_types e ON e.name = l.target_entity
WHERE l.source_entity = 'replica'
   OR l.target_entity = 'replica';
```

**Found:**
- `frontier` (per-replica, GlobalId)
- `hydration_status` (per-replica, GlobalId)
- `replica_metrics` (CatalogItemId)
- `replica_status` (CatalogItemId)
- `replica_utilization` (CatalogItemId)
- `replica_status_history` (CatalogItemId)
- Links: `belongs_to_cluster`, `has_size`, `owned_by`

### 2. What measures freshness? (with type safety via properties join)

```sql
SELECT l.name, l.source_entity, e.relation,
       sp.semantic_type AS src_type, tp.semantic_type AS tgt_type,
       CASE WHEN sp.semantic_type != tp.semantic_type THEN 'MISMATCH' ELSE 'OK' END AS safety
FROM ma_mz_ontology.link_types l
JOIN ma_mz_ontology.entity_types e ON e.name = l.source_entity
LEFT JOIN ma_mz_ontology.properties sp
  ON sp.entity_type = l.source_entity AND sp.column_name = l.properties->>'source_column'
LEFT JOIN ma_mz_ontology.properties tp
  ON tp.entity_type = l.target_entity AND tp.column_name = l.properties->>'target_column'
WHERE l.name LIKE '%lag%' OR l.name LIKE '%frontier%' OR l.name LIKE '%freshness%';
```

**Found:**
| Entity | Join Safety | Notes |
|---|---|---|
| `wallclock_lag_history` | GlobalId -> CatalogItemId = **MISMATCH** | Needs `mz_object_global_ids` mapping |
| `materialization_lag` | CatalogItemId -> CatalogItemId = **OK** | Direct join safe |
| `wallclock_global_lag` | **MISMATCH** | GlobalId on one side |
| `frontiers` | **MISMATCH** | GlobalId on one side |

Type safety is derived from joining `properties` on both sides of the link -- the `properties` table stores the semantic type for each column. When source and target semantic types differ, the join is unsafe without an ID mapping table.

### 3. What columns do freshness tables have?

```sql
SELECT p.entity_type, p.column_name, p.semantic_type, p.description
FROM ma_mz_ontology.properties p
WHERE p.entity_type IN (
  'materialization_lag', 'wallclock_lag_history', 'replica_status',
  'replica_utilization', 'replica_metrics', 'frontier', 'wallclock_global_lag'
)
ORDER BY p.entity_type, p.column_name;
```

**Found:** 33 columns across 7 entities. Key columns:
- `materialization_lag`: `slowest_local_input_id`, `slowest_global_input_id`
- `replica_status`: `reason`
- `replica_utilization`: `cpu_percent`, `memory_percent`
- `wallclock_lag_history`: `lag`, `occurred_at`, `replica_id`

## Diagnostic Queries

### 1. Per-replica lag (via wallclock_lag_history with GlobalId mapping)

```sql
SELECT r.name AS replica, o.name AS object, wl.lag, wl.occurred_at
FROM mz_internal.mz_wallclock_lag_history wl
JOIN mz_internal.mz_object_global_ids g ON wl.object_id = g.global_id
JOIN mz_catalog.mz_objects o ON g.id = o.id
JOIN mz_catalog.mz_cluster_replicas r ON wl.replica_id = r.id
WHERE wl.occurred_at > mz_now() - INTERVAL '1 hour'
ORDER BY wl.lag DESC
LIMIT 20;
```

### 2. Bottleneck input (via materialization_lag, direct join)

```sql
SELECT o.name AS object, ml.lag,
       si.name AS slowest_local_input,
       sg.name AS slowest_global_input
FROM mz_internal.mz_materialization_lag ml
JOIN mz_catalog.mz_objects o ON ml.object_id = o.id
LEFT JOIN mz_catalog.mz_objects si ON ml.slowest_local_input_id = si.id
LEFT JOIN mz_catalog.mz_objects sg ON ml.slowest_global_input_id = sg.id
ORDER BY ml.lag DESC
LIMIT 20;
```

### 3. Replica health

```sql
SELECT r.name AS replica, c.name AS cluster, rs.status, rs.reason
FROM mz_internal.mz_cluster_replica_statuses rs
JOIN mz_catalog.mz_cluster_replicas r ON rs.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id;
```

### 4. Resource usage

```sql
SELECT r.name AS replica, c.name AS cluster,
       ru.cpu_percent, ru.memory_percent
FROM mz_internal.mz_cluster_replica_utilization ru
JOIN mz_catalog.mz_cluster_replicas r ON ru.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
ORDER BY ru.cpu_percent DESC;
```

## What the ontology helped with

- **Type safety detection**: The properties-based join check immediately flagged that `wallclock_lag_history` uses GlobalId while most replica tables use CatalogItemId. Without this, a naive join (`wl.object_id = o.id`) would silently produce wrong results.
- **Discovery**: Found `materialization_lag.slowest_local_input_id` and `slowest_global_input_id` which pinpoint bottleneck inputs -- not obvious from table names alone.
- **Safe vs unsafe joins**: Knew to use `mz_object_global_ids` for wallclock_lag_history but could join materialization_lag directly.

## What it didn't help with

- No guidance on what constitutes "normal" lag values or thresholds.
- No information about how wallclock lag is computed internally (sampling frequency, aggregation).
- Column descriptions were available but query patterns (e.g., filtering by time window) required domain knowledge.
