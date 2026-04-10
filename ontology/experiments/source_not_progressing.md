# Test: Why is my source not making progress?

## Ontology Queries

### 1. What relates to sources?

```sql
SELECT l.name, l.source_entity, l.target_entity, e.relation,
       sp.semantic_type AS src_type, tp.semantic_type AS tgt_type,
       CASE WHEN sp.semantic_type != tp.semantic_type THEN 'MISMATCH' ELSE 'OK' END AS safety
FROM ma_mz_ontology.link_types l
JOIN ma_mz_ontology.entity_types e ON e.name = l.target_entity
LEFT JOIN ma_mz_ontology.properties sp
  ON sp.entity_type = l.source_entity AND sp.column_name = l.properties->>'source_column'
LEFT JOIN ma_mz_ontology.properties tp
  ON tp.entity_type = l.target_entity AND tp.column_name = l.properties->>'target_column'
WHERE l.source_entity = 'source' OR l.target_entity = 'source';
```

**Found:**
- `source_status` (CatalogItemId) -- current status and error details
- `source_statistics` (CatalogItemId) -- progress counters
- `source_status_history` (CatalogItemId) -- status timeline
- Kafka-specific: `kafka_source` (topic, group ID), `kafka_source_table` (table-level details)
- Links: `runs_on_cluster`, `uses_connection`, `in_schema`, `owned_by`

All source-related tables use CatalogItemId, so direct joins are safe.

### 2. What columns do source tables have?

```sql
SELECT p.entity_type, p.column_name, p.semantic_type, p.description
FROM ma_mz_ontology.properties p
WHERE p.entity_type IN (
  'source_status', 'source_statistics', 'source_status_history',
  'kafka_source', 'kafka_source_table'
)
ORDER BY p.entity_type, p.column_name;
```

**Found:** 45 columns across 5 entities. Key columns:

| Entity | Column | Significance |
|---|---|---|
| `source_statistics` | `snapshot_records_known` | Total records to snapshot |
| `source_statistics` | `snapshot_records_staged` | Records snapshotted so far |
| `source_statistics` | `offset_known` | Latest upstream offset |
| `source_statistics` | `offset_committed` | Latest committed offset |
| `source_statistics` | `snapshot_committed` | Boolean: snapshot complete? |
| `source_status` | `error` | Current error message |
| `source_status` | `details` | JSON with hints |
| `source_status_history` | `replica_id` | Which replica had the status |

The `snapshot_records_known` vs `snapshot_records_staged` pair is the key progress indicator during initial snapshot. The `offset_known` vs `offset_committed` pair shows upstream lag during steady state.

## Diagnostic Queries

### 1. Current source status

```sql
SELECT s.name, ss.status, ss.error, ss.details
FROM mz_internal.mz_source_statuses ss
JOIN mz_catalog.mz_sources s ON ss.id = s.id
WHERE s.name = '<source_name>';
```

### 2. Status timeline

```sql
SELECT s.name, sh.status, sh.error, sh.occurred_at, sh.replica_id
FROM mz_internal.mz_source_status_history sh
JOIN mz_catalog.mz_sources s ON sh.source_id = s.id
WHERE s.name = '<source_name>'
ORDER BY sh.occurred_at DESC
LIMIT 20;
```

### 3. Snapshot and replication progress

```sql
SELECT s.name,
       st.snapshot_committed,
       st.snapshot_records_known,
       st.snapshot_records_staged,
       CASE WHEN st.snapshot_records_known > 0
            THEN round(100.0 * st.snapshot_records_staged / st.snapshot_records_known, 1)
            ELSE NULL END AS snapshot_pct,
       st.offset_known,
       st.offset_committed
FROM mz_internal.mz_source_statistics st
JOIN mz_catalog.mz_sources s ON st.id = s.id
WHERE s.name = '<source_name>';
```

### 4. Upstream lag check

```sql
SELECT s.name,
       st.offset_known - st.offset_committed AS offset_lag,
       st.offset_known,
       st.offset_committed
FROM mz_internal.mz_source_statistics st
JOIN mz_catalog.mz_sources s ON st.id = s.id
WHERE st.offset_known > st.offset_committed;
```

### 5. Connection config

```sql
SELECT s.name, c.name AS connection_name, c.type AS connection_type
FROM mz_catalog.mz_sources s
JOIN mz_catalog.mz_connections c ON s.connection_id = c.id
WHERE s.name = '<source_name>';
```

### 6. Cluster health

```sql
SELECT s.name AS source, cl.name AS cluster, r.name AS replica, rs.status, rs.reason
FROM mz_catalog.mz_sources s
JOIN mz_catalog.mz_clusters cl ON s.cluster_id = cl.id
JOIN mz_catalog.mz_cluster_replicas r ON r.cluster_id = cl.id
JOIN mz_internal.mz_cluster_replica_statuses rs ON rs.replica_id = r.id
WHERE s.name = '<source_name>';
```

### 7. Kafka-specific: topic and consumer group

```sql
SELECT s.name, ks.topic, ks.group_id_prefix
FROM mz_catalog.mz_sources s
JOIN mz_catalog.mz_kafka_sources ks ON s.id = ks.id
WHERE s.name = '<source_name>';
```

## What the ontology helped with

- **Progress indicators**: Discovered the `snapshot_records_known` / `snapshot_records_staged` pair for snapshot progress and `offset_known` / `offset_committed` for steady-state lag. These column names are not self-documenting; the ontology's descriptions clarified their meaning.
- **Status details with hints**: The `details` column in `source_status` contains structured hints -- the ontology's description revealed this is JSON with actionable remediation info.
- **Kafka-specific tables**: The ontology linked Kafka sources to their topic configuration via `kafka_source` and table-level details via `kafka_source_table`, which wouldn't be found by searching for "source" alone.
- **Replica-level status**: `source_status_history` has `replica_id`, meaning source issues can be isolated to a specific replica.

## What it didn't help with

- No guidance on what constitutes "stuck" vs "slow" (e.g., how long should snapshot_records_staged remain unchanged before it's a problem?).
- No runbook-style remediation steps (e.g., "if error contains X, try Y").
- Kafka connection parameters (broker addresses, authentication) are in the connection object, which the ontology links to but doesn't expose column-level detail for.
