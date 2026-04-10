# Ontology Test Results

These tests evaluate whether the `ma_mz_ontology` tables help an LLM (Claude)
write correct diagnostic queries against the Materialize system catalog.

## Method

For each test:
1. Start with a natural language debugging question
2. Run 2-3 ontology queries to discover relevant tables, columns, and join paths
3. Check for type mismatches between source and target columns via properties
4. Use the ontology results to write diagnostic SQL queries
5. Document what the ontology helped with and what it didn't

## Tests

| Test | Prompt | Key finding |
|------|--------|-------------|
| [Replica freshness](replica_freshness.md) | "Why does a replica have freshness problems?" | `wallclock_lag_history` is per-replica + GlobalId (mismatch detected via properties); `materialization_lag` has `slowest_*_input_id` to find the bottleneck |
| [Source not progressing](source_not_progressing.md) | "Why is my source not making progress?" | `source_statistics` has `snapshot_records_staged` vs `snapshot_records_known` and `offset_known` vs `offset_committed` for diagnosing where ingestion is stuck |
| [Storage usage](storage_usage.md) | "What's using all the storage?" | 3 granularity levels; `storage_shard` uses GlobalId (detected via properties), `recent_storage` uses CatalogItemId |
| [Dependency impact](dependency_impact.md) | "What depends on this source?" | `object_dependency` + `transitive_dependency` for catalog-level impact; `compute_dependency` uses GlobalId (detected via properties) |

## What the ontology consistently helps with

1. **Finding the right tables.** Each question has 2-5 relevant tables out of
   ~300 builtins. The ontology's link_types surface them via "what relates to X?"
   queries instead of guessing or reading docs.

2. **GlobalId vs CatalogItemId.** Type mismatches are detected by joining
   link_types against properties on both the source and target columns:
   ```sql
   SELECT l.name, sp.semantic_type AS src_type, tp.semantic_type AS tgt_type,
          CASE WHEN sp.semantic_type != tp.semantic_type
               THEN 'MISMATCH - needs mapping' ELSE 'direct join OK' END
   FROM ma_mz_ontology.link_types l
   JOIN ma_mz_ontology.properties sp
     ON sp.entity_type = l.source_entity
     AND sp.column_name = l.properties->>'source_column'
   JOIN ma_mz_ontology.properties tp
     ON tp.entity_type = l.target_entity
     AND tp.column_name = l.properties->>'target_column'
   WHERE sp.semantic_type IS NOT NULL AND tp.semantic_type IS NOT NULL;
   ```
   This is not hardcoded — it's derived from the semantic types in properties.

3. **Discovering non-obvious columns.** Properties surface columns like
   `slowest_local_input_id`, `snapshot_records_staged`, `reason` (for offline
   replicas), `offset_known` vs `offset_committed`.

4. **Join direction and semantics.** Link descriptions clarify ambiguous
   relationships. Link names carry meaning ("runs_on_cluster" vs
   "belongs_to_cluster").

## What the ontology doesn't help with

1. **Debugging workflow order.** The ontology knows what tables exist, not what
   order to check them in.

2. **Column value interpretation.** The ontology knows `offset_known` and
   `offset_committed` exist but not that "offset_known >> offset_committed means
   the source is falling behind."

3. **Materialize-specific SQL patterns.** `WITH MUTUALLY RECURSIVE` for transitive
   dependencies, `pg_size_pretty()` for byte formatting.

## Ontology query patterns

Three queries cover most debugging scenarios:

**1. "What relates to X?"**
```sql
SELECT l.name, l.source_entity, l.target_entity, l.description
FROM ma_mz_ontology.link_types l
WHERE l.source_entity = 'X' OR l.target_entity = 'X';
```

**2. "What measures/tracks Y?" (with type safety check)**
```sql
SELECT l.name, l.source_entity, e.relation,
       sp.semantic_type AS src_type, tp.semantic_type AS tgt_type,
       CASE WHEN sp.semantic_type != tp.semantic_type
            THEN 'MISMATCH' ELSE 'OK' END AS join_safety
FROM ma_mz_ontology.link_types l
JOIN ma_mz_ontology.entity_types e ON e.name = l.source_entity
LEFT JOIN ma_mz_ontology.properties sp
  ON sp.entity_type = l.source_entity AND sp.column_name = l.properties->>'source_column'
LEFT JOIN ma_mz_ontology.properties tp
  ON tp.entity_type = l.target_entity AND tp.column_name = l.properties->>'target_column'
WHERE l.properties->>'kind' = 'measures'
   OR l.name LIKE '%keyword%';
```

**3. "What columns does Z have?"**
```sql
SELECT p.column_name, p.semantic_type, p.description
FROM ma_mz_ontology.properties p
WHERE p.entity_type = 'Z'
ORDER BY p.column_name;
```

## Stats

- **Entity types:** 118 (mz_catalog + mz_internal + mz_introspection)
- **Semantic types:** 20
- **Properties:** 382 columns mapped
- **Link types:** 108 named relationships
- **Type mismatches detected:** 11 GlobalId→CatalogItemId joins (derived from properties, not hardcoded)
