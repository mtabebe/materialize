# Materialize Catalog Ontology

An experiment around creating an _ontology_ based on the Materialize catalog and internal tables.

A set of tables that describe the Materialize system catalog — what entities
exist, how they relate, what columns mean, and what semantic ID type each
column uses.


# The ontology
Defined in `sql/`
Create the schema
```
psql -h localhost -p 6877 -U mz_system materialize -f create_schema.sql
```

Populate the tables
```
for f in entity_types semantic_types properties link_types; do
 psql -h localhost -p 6877 -U mz_system materialize -f ${f}.sql
done
```

## Schema

Four tables:

- **entity_types** (name, relation, properties, description) — what kinds of
  things exist. `properties` jsonb has `{"primary_key": ["id"]}`.
- **semantic_types** (name, sql_type, description) — typed ID domains and other
  semantic column types (ClusterId, GlobalId, ByteCount, etc.)
- **properties** (entity_type, column_name, semantic_type, description) — maps
  every column to its semantic type and describes what it means.
- **link_types** (name, source_entity, target_entity, properties, description)
  — named relationships. `properties` jsonb has freeform keywords:
  - `"kind": "foreign_key"` — column-level join with source_column, target_column, cardinality
  - `"kind": "measures"` — a measurement/metric relationship
  - `"kind": "depends_on"` — a dependency relationship
  - `"kind": "maps_to"` — an ID mapping (e.g., CatalogItemId ↔ GlobalId)
  - `"kind": "union"` — a UNION view includes another entity type

## Stats

- ~110 entity types (mz_catalog + mz_internal + mz_introspection)
- 20 semantic types
- ~380 column properties
- ~110 named relationships

## A Note

Ideally these would be system tables (i.e., `mz_ontology`), but these are
currently implemented as user-space DML with an `ma_` prefix.
This is more to show the utility of the approach.

The data itself is Claude-generated from the Rust source code.
A human-curated version may differ. Additionally, there is a question of
maintenance and keeping these up to date.

# Experimentation
Then, you can point Claude/LLMs at the ontology to help answer more semantic questions like "Why does a replica have freshness problems" to help get diagnostic queries. See `experiments/` for more detail. But at a high level, now the LLM isn't doing `LIKE` queries, but can instead explore the data catalog to understand how concepts relate.


# For Claude / LLMs

If you are connected to a Materialize instance with these ontology tables
loaded (in the `ma_mz_ontology` schema), use them **before** writing catalog
queries. They will help you find the right tables, correct join paths, and
avoid the GlobalId/CatalogItemId trap.

### Three queries to run first

**1. "What relates to X?"** — find all entities connected to what you're
investigating:
```sql
SELECT l.name, l.source_entity, l.target_entity,
       l.properties->>'source_id_type' AS id_type,
       l.description
FROM ma_mz_ontology.link_types l
WHERE l.source_entity = 'X' OR l.target_entity = 'X';
```

**2. "What measures/tracks Y?"** — find diagnostic and measurement tables:
```sql
SELECT l.name, l.source_entity, e.relation,
       l.properties->>'source_id_type' AS id_type,
       l.properties->>'requires_mapping' AS needs_mapping
FROM ma_mz_ontology.link_types l
JOIN ma_mz_ontology.entity_types e ON e.name = l.source_entity
WHERE l.properties->>'kind' = 'measures'
   OR l.name LIKE '%keyword%';
```

**3. "What columns does Z have?"** — discover available data and types:
```sql
SELECT p.column_name, p.semantic_type, p.description
FROM ma_mz_ontology.properties p
WHERE p.entity_type = 'Z'
ORDER BY p.column_name;
```

### Critical: GlobalId vs CatalogItemId

Many `object_id` columns in `mz_internal` and `mz_introspection` use
**GlobalId**, not **CatalogItemId**. Both are `text`, both look like `u42`,
but they are different ID namespaces. A direct join to `mz_objects.id`
(CatalogItemId) will silently return wrong results after ALTER operations.

**Before writing a join**, check whether the source and target columns have
the same semantic type in `properties`:
```sql
SELECT sp.semantic_type AS src_type, tp.semantic_type AS tgt_type
FROM ma_mz_ontology.properties sp, ma_mz_ontology.properties tp
WHERE sp.entity_type = 'SOURCE_ENTITY' AND sp.column_name = 'SOURCE_COL'
  AND tp.entity_type = 'TARGET_ENTITY' AND tp.column_name = 'TARGET_COL';
```

If they differ (e.g., `GlobalId` vs `CatalogItemId`), you need the mapping
table `mz_internal.mz_object_global_ids`:
```sql
JOIN mz_internal.mz_object_global_ids g ON table.object_id = g.global_id
JOIN mz_catalog.mz_objects o ON g.id = o.id
```

### Critical: Always use `entity_types.relation` for table names

The `name` column in `entity_types` is a short alias (e.g., `replica`, `source`,
`mv`). It is **NOT** a valid SQL table name. The `relation` column contains the
fully-qualified table name (e.g., `mz_catalog.mz_cluster_replicas`).

**When writing diagnostic SQL queries, ALWAYS look up the table name from
`entity_types.relation`.** Never guess or abbreviate table names — many don't
match what you'd expect (e.g., `replica` → `mz_catalog.mz_cluster_replicas`,
not `mz_replicas`).

```sql
SELECT name, relation, description
FROM ma_mz_ontology.entity_types
WHERE name = 'mv';
-- mv → mz_catalog.mz_materialized_views
```

Use the `relation` value directly in your FROM/JOIN clauses.

### Primary keys

```sql
SELECT name, properties->>'primary_key' AS pk
FROM ma_mz_ontology.entity_types
WHERE name = 'cluster';
-- ["id"]
```


