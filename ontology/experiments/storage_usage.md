# Test: What's using all the storage?

## Ontology Queries

### 1. Storage-related links with type safety

```sql
SELECT l.name, l.source_entity, l.target_entity, e.relation,
       sp.semantic_type AS src_type, tp.semantic_type AS tgt_type,
       CASE WHEN sp.semantic_type != tp.semantic_type THEN 'MISMATCH' ELSE 'OK' END AS safety
FROM ma_mz_ontology.link_types l
JOIN ma_mz_ontology.entity_types e ON e.name = l.source_entity
LEFT JOIN ma_mz_ontology.properties sp
  ON sp.entity_type = l.source_entity AND sp.column_name = l.properties->>'source_column'
LEFT JOIN ma_mz_ontology.properties tp
  ON tp.entity_type = l.target_entity AND tp.column_name = l.properties->>'target_column'
WHERE l.name LIKE '%storage%' OR l.source_entity LIKE '%storage%' OR l.target_entity LIKE '%storage%';
```

**Found:**
| Entity | Join Safety | Notes |
|---|---|---|
| `recent_storage` | CatalogItemId -> CatalogItemId = **OK** | Direct join safe |
| `storage_usage` | CatalogItemId -> CatalogItemId = **OK** | Direct join safe |
| `storage_shard` | GlobalId -> CatalogItemId = **MISMATCH** | Needs `mz_object_global_ids` mapping |

### 2. What columns do storage tables have?

```sql
SELECT p.entity_type, p.column_name, p.semantic_type, p.description
FROM ma_mz_ontology.properties p
WHERE p.entity_type IN ('recent_storage', 'storage_usage', 'storage_shard', 'storage_usage_by_shard')
ORDER BY p.entity_type, p.column_name;
```

**Found:** 11 columns across the storage entities:
- `recent_storage`: `object_id` (CatalogItemId), `size_bytes`
- `storage_shard`: `shard_id` (ShardId), `object_id` (GlobalId)
- `storage_usage_by_shard`: `shard_id`, `size_bytes`, `collection_timestamp`

## Diagnostic Queries

### 1. Top objects by storage

```sql
SELECT o.name, o.type, s.name AS schema, rs.size_bytes,
       pg_size_pretty(rs.size_bytes) AS size_pretty
FROM mz_catalog.mz_recent_storage_usage rs
JOIN mz_catalog.mz_objects o ON rs.object_id = o.id
JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
ORDER BY rs.size_bytes DESC
LIMIT 20;
```

### 2. Growth over time

```sql
SELECT date_trunc('day', su.collection_timestamp) AS day,
       o.name,
       max(su.size_bytes) AS size_bytes
FROM mz_catalog.mz_storage_usage su
JOIN mz_catalog.mz_objects o ON su.object_id = o.id
WHERE o.name = '<object_name>'
GROUP BY 1, 2
ORDER BY 1;
```

### 3. By object type

```sql
SELECT o.type,
       count(*) AS object_count,
       sum(rs.size_bytes) AS total_bytes,
       pg_size_pretty(sum(rs.size_bytes)) AS total_pretty
FROM mz_catalog.mz_recent_storage_usage rs
JOIN mz_catalog.mz_objects o ON rs.object_id = o.id
GROUP BY o.type
ORDER BY total_bytes DESC;
```

### 4. By schema

```sql
SELECT d.name AS database, s.name AS schema,
       sum(rs.size_bytes) AS total_bytes,
       pg_size_pretty(sum(rs.size_bytes)) AS total_pretty
FROM mz_catalog.mz_recent_storage_usage rs
JOIN mz_catalog.mz_objects o ON rs.object_id = o.id
JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
LEFT JOIN mz_catalog.mz_databases d ON s.database_id = d.id
GROUP BY 1, 2
ORDER BY total_bytes DESC;
```

### 5. Per-shard breakdown (needs GlobalId mapping)

```sql
SELECT o.name, sh.shard_id,
       su.size_bytes, su.collection_timestamp
FROM mz_internal.mz_storage_shards sh
JOIN mz_internal.mz_object_global_ids g ON sh.object_id = g.global_id
JOIN mz_catalog.mz_objects o ON g.id = o.id
JOIN mz_internal.mz_storage_usage_by_shard su ON sh.shard_id = su.shard_id
ORDER BY su.size_bytes DESC
LIMIT 20;
```

## What the ontology helped with

- **Type safety for shard queries**: The properties join immediately flagged that `storage_shard` uses GlobalId while catalog objects use CatalogItemId. Without this, joining `sh.object_id = o.id` would silently return no rows or wrong rows.
- **Safe vs unsafe paths**: Knew that `recent_storage` and `storage_usage` can be joined directly to catalog objects, but `storage_usage_by_shard` requires going through `storage_shard` first (and mapping GlobalId).
- **Discovery of shard-level tables**: Found `storage_usage_by_shard` which provides per-shard size over time -- useful for identifying individual shards that are growing unexpectedly.

## What it didn't help with

- No guidance on what's "too big" -- thresholds depend on the deployment and plan.
- No information about storage retention policies or compaction behavior.
- The `pg_size_pretty` function for human-readable sizes is general Postgres knowledge, not ontology-provided.
- No cost estimation (storage bytes to billing dollars).
