# Test: What depends on this source and will break if I drop it?

## Ontology Queries

### 1. Dependency links with type safety

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
WHERE l.name LIKE '%depend%' OR l.name LIKE '%dep%';
```

**Found:**
| Entity | Join Safety | Notes |
|---|---|---|
| `object_dependency` | CatalogItemId -> CatalogItemId = **OK** | Direct dependencies |
| `transitive_dependency` | CatalogItemId -> CatalogItemId = **OK** | Full transitive closure |
| `materialization_dep` | CatalogItemId -> CatalogItemId = **OK** | Materialization-specific |
| `compute_dependency` | GlobalId -> GlobalId = **MISMATCH** | Needs `mz_object_global_ids` mapping |

### 2. Dependency table columns

```sql
SELECT p.entity_type, p.column_name, p.semantic_type, p.description
FROM ma_mz_ontology.properties p
WHERE p.entity_type IN (
  'object_dependency', 'transitive_dependency', 'materialization_dep', 'compute_dependency'
)
ORDER BY p.entity_type, p.column_name;
```

**Found:**
- `object_dependency`: `object_id`, `referenced_object_id` (both CatalogItemId)
- `transitive_dependency`: `object_id`, `referenced_object_id` (both CatalogItemId)
- `compute_dependency`: `object_id`, `dependency_id` (both GlobalId)

Also found `mz_object_fully_qualified_names` for display-friendly names.

## Diagnostic Queries

### 1. Direct dependents

```sql
SELECT o.name AS dependent, o.type AS dependent_type,
       fqn.name AS fully_qualified
FROM mz_internal.mz_object_dependencies d
JOIN mz_catalog.mz_objects o ON d.object_id = o.id
LEFT JOIN mz_internal.mz_object_fully_qualified_names fqn ON d.object_id = fqn.id
WHERE d.referenced_object_id = (
  SELECT id FROM mz_catalog.mz_objects WHERE name = '<source_name>'
);
```

### 2. Transitive dependents (no recursive query needed)

```sql
SELECT o.name AS dependent, o.type AS dependent_type,
       fqn.name AS fully_qualified
FROM mz_internal.mz_object_transitive_dependencies td
JOIN mz_catalog.mz_objects o ON td.object_id = o.id
LEFT JOIN mz_internal.mz_object_fully_qualified_names fqn ON td.object_id = fqn.id
WHERE td.referenced_object_id = (
  SELECT id FROM mz_catalog.mz_objects WHERE name = '<source_name>'
);
```

This is the key advantage: `mz_object_transitive_dependencies` provides the full transitive closure out of the box, so no recursive CTE is needed.

### 3. With depth via WITH MUTUALLY RECURSIVE

```sql
WITH MUTUALLY RECURSIVE
  deps(id text, depth int8) AS (
    SELECT d.object_id, 1::int8
    FROM mz_internal.mz_object_dependencies d
    WHERE d.referenced_object_id = (
      SELECT id FROM mz_catalog.mz_objects WHERE name = '<source_name>'
    )
    UNION
    SELECT d.object_id, deps.depth + 1
    FROM mz_internal.mz_object_dependencies d
    JOIN deps ON d.referenced_object_id = deps.id
  )
SELECT o.name, o.type, min(d.depth) AS min_depth
FROM deps d
JOIN mz_catalog.mz_objects o ON d.id = o.id
GROUP BY o.name, o.type
ORDER BY min_depth, o.name;
```

### 4. Materialization dependencies only

```sql
SELECT o.name AS materialization, o.type,
       fqn.name AS fully_qualified
FROM mz_internal.mz_materialization_dependencies md
JOIN mz_catalog.mz_objects o ON md.object_id = o.id
LEFT JOIN mz_internal.mz_object_fully_qualified_names fqn ON md.object_id = fqn.id
WHERE md.dependency_id = (
  SELECT id FROM mz_catalog.mz_objects WHERE name = '<source_name>'
);
```

### 5. Summary by type

```sql
SELECT o.type, count(*) AS dependent_count
FROM mz_internal.mz_object_transitive_dependencies td
JOIN mz_catalog.mz_objects o ON td.object_id = o.id
WHERE td.referenced_object_id = (
  SELECT id FROM mz_catalog.mz_objects WHERE name = '<source_name>'
)
GROUP BY o.type
ORDER BY dependent_count DESC;
```

## What the ontology helped with

- **Discovered `mz_object_transitive_dependencies`**: This table provides the full transitive closure, eliminating the need for recursive CTEs. This is the single biggest time-saver -- most users would write a recursive query manually.
- **Type safety for compute dependencies**: Flagged that `compute_dependency` uses GlobalId, so it cannot be joined directly to catalog objects without the mapping table.
- **Edge direction clarity**: The ontology's property descriptions clarified that `object_id` is the dependent and `referenced_object_id` is the dependency (not the other way around).
- **Materialization-specific deps**: Found `mz_materialization_dependencies` which filters to only materialized views and indexes, useful when the question is specifically about materialized artifacts.

## What it didn't help with

- No "DROP CASCADE" simulation -- the ontology shows what depends on something but not what Materialize would actually drop.
- No indication of which dependents are "important" (e.g., sinks that feed downstream systems vs. ad-hoc views).
- The `WITH MUTUALLY RECURSIVE` depth query requires Materialize-specific SQL knowledge (not standard recursive CTEs).
