-- Materialize Catalog Ontology: Link Types
-- Derived from column naming conventions, cross-checked against Rust types
-- and existing view JOIN conditions in builtin.rs

INSERT INTO ma_mz_ontology.link_types (name, source_entity, target_entity, properties, description) VALUES

-- =============================================================================
-- Hierarchy: database → schema → objects
-- =============================================================================

('in_database', 'schema', 'database',
 '{"kind": "foreign_key", "source_column": "database_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A schema lives in a database. Nullable because system schemas (mz_catalog, pg_catalog) have no database.'),

('in_schema', 'table', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A table lives in a schema.'),

('in_schema', 'source', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A source lives in a schema.'),

('in_schema', 'view', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A view lives in a schema.'),

('in_schema', 'mv', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A materialized view lives in a schema.'),

('in_schema', 'connection', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A connection lives in a schema.'),

('in_schema', 'secret', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A secret lives in a schema.'),

('in_schema', 'type', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A type lives in a schema.'),

('in_schema', 'function', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A function lives in a schema.'),

('in_schema', 'sink', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A sink lives in a schema.'),

-- =============================================================================
-- Ownership: objects → role (via owner_id)
-- =============================================================================

('owned_by', 'database', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A database is owned by a role.'),

('owned_by', 'schema', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A schema is owned by a role.'),

('owned_by', 'cluster', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A cluster is owned by a role.'),

('owned_by', 'replica', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A cluster replica is owned by a role.'),

('owned_by', 'table', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A table is owned by a role.'),

('owned_by', 'source', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A source is owned by a role.'),

('owned_by', 'view', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A view is owned by a role.'),

('owned_by', 'mv', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A materialized view is owned by a role.'),

('owned_by', 'index', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'An index is owned by a role.'),

('owned_by', 'sink', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A sink is owned by a role.'),

('owned_by', 'connection', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A connection is owned by a role.'),

('owned_by', 'secret', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A secret is owned by a role.'),

('owned_by', 'type', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A type is owned by a role.'),

('owned_by', 'function', 'role',
 '{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A function is owned by a role.'),

-- =============================================================================
-- Cluster relationships: objects → cluster (via cluster_id)
-- =============================================================================

('belongs_to_cluster', 'replica', 'cluster',
 '{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A replica belongs to exactly one cluster. A cluster can have multiple replicas.'),

('runs_on_cluster', 'mv', 'cluster',
 '{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A materialized view is maintained on a cluster. The cluster runs the dataflow that keeps the MV up to date.'),

('runs_on_cluster', 'index', 'cluster',
 '{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'An index is maintained on a cluster.'),

('runs_on_cluster', 'source', 'cluster',
 '{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A source is ingested on a cluster. Nullable for subsources and progress sources.'),

('runs_on_cluster', 'sink', 'cluster',
 '{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A sink exports data from a cluster.'),

-- =============================================================================
-- Index relationships
-- =============================================================================

('indexes_relation', 'index', 'mv',
 '{"kind": "foreign_key", "source_column": "on_id", "target_column": "id", "cardinality": "many_to_one", "note": "on_id can also reference tables, sources, views — this is a polymorphic FK"}'::jsonb,
 'An index is built on a relation. The on_id can reference any relation type (table, source, view, MV).'),

('belongs_to_index', 'index_column', 'index',
 '{"kind": "foreign_key", "source_column": "index_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'An index column belongs to an index.'),

-- =============================================================================
-- Column relationships
-- =============================================================================

('belongs_to_relation', 'column', 'object',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "many_to_one", "note": "id in mz_columns is the relation ID, not a unique column ID"}'::jsonb,
 'A column belongs to a relation (table, view, MV, source). The id column in mz_columns is the parent relation ID.'),

-- =============================================================================
-- Source/sink detail tables → parent objects
-- =============================================================================

('details_of', 'kafka_source', 'source',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Kafka-specific source details. Joins to mz_sources on id.'),

('details_of', 'kafka_sink', 'sink',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Kafka-specific sink details. Joins to mz_sinks on id.'),

('details_of', 'iceberg_sink', 'sink',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Iceberg-specific sink details. Joins to mz_sinks on id.'),

('details_of', 'kafka_connection', 'connection',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Kafka-specific connection details. Joins to mz_connections on id.'),

('details_of', 'ssh_tunnel', 'connection',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'SSH tunnel connection details. Joins to mz_connections on id.'),

('details_of', 'aws_privatelink', 'connection',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'AWS PrivateLink connection details. Joins to mz_connections on id.'),

-- =============================================================================
-- Source → connection
-- =============================================================================

('uses_connection', 'source', 'connection',
 '{"kind": "foreign_key", "source_column": "connection_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A source uses a connection for external access. Nullable for sources without connections (e.g., load generators).'),

-- =============================================================================
-- Table → source (for subsources/tables created from a source)
-- =============================================================================

('created_by_source', 'table', 'source',
 '{"kind": "foreign_key", "source_column": "source_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A table can be created by a source (e.g., Postgres source creates tables). Nullable for user-created tables.'),

-- =============================================================================
-- Type system relationships
-- =============================================================================

('is_subtype_of', 'array_type', 'type',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'An array type is a specialization of a type.'),

('has_element_type', 'array_type', 'type',
 '{"kind": "foreign_key", "source_column": "element_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'An array type has an element type.'),

('is_subtype_of', 'list_type', 'type',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'A list type is a specialization of a type.'),

('has_element_type', 'list_type', 'type',
 '{"kind": "foreign_key", "source_column": "element_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A list type has an element type.'),

('is_subtype_of', 'map_type', 'type',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'A map type is a specialization of a type.'),

('has_key_type', 'map_type', 'type',
 '{"kind": "foreign_key", "source_column": "key_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A map type has a key type.'),

('has_value_type', 'map_type', 'type',
 '{"kind": "foreign_key", "source_column": "value_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A map type has a value type.'),

-- =============================================================================
-- Function type references
-- =============================================================================

('returns_type', 'function', 'type',
 '{"kind": "foreign_key", "source_column": "return_type_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A function returns a type. Nullable for functions that return void.'),

-- =============================================================================
-- Role membership
-- =============================================================================

('member_of_role', 'role_member', 'role',
 '{"kind": "foreign_key", "source_column": "role_id", "target_column": "id", "cardinality": "many_to_one", "note": "The role being granted membership in"}'::jsonb,
 'A role membership: the member role is a member of this role.'),

('has_member', 'role_member', 'role',
 '{"kind": "foreign_key", "source_column": "member", "target_column": "id", "cardinality": "many_to_one", "note": "The role that is a member"}'::jsonb,
 'A role membership: this role is the member.'),

('granted_by', 'role_member', 'role',
 '{"kind": "foreign_key", "source_column": "grantor", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A role membership was granted by this role.'),

-- =============================================================================
-- Role parameters
-- =============================================================================

('parameter_of', 'role_parameter', 'role',
 '{"kind": "foreign_key", "source_column": "role_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A session parameter default belongs to a role.'),

-- =============================================================================
-- Default privileges
-- =============================================================================

('default_priv_for_role', 'default_privilege', 'role',
 '{"kind": "foreign_key", "source_column": "role_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A default privilege rule applies when this role creates objects.'),

('default_priv_in_database', 'default_privilege', 'database',
 '{"kind": "foreign_key", "source_column": "database_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A default privilege rule scoped to a database. Nullable for global rules.'),

('default_priv_in_schema', 'default_privilege', 'schema',
 '{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A default privilege rule scoped to a schema. Nullable for database-level or global rules.'),

-- =============================================================================
-- Storage usage → objects
-- =============================================================================

('storage_usage_of', 'storage_usage', 'object',
 '{"kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Storage usage measurement for an object over time.'),

('recent_storage_of', 'recent_storage', 'object',
 '{"kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Most recent storage usage snapshot for an object.'),

-- =============================================================================
-- Replica sizes (reference data, not FK)
-- =============================================================================

('has_size', 'replica', 'replica_size',
 '{"kind": "foreign_key", "source_column": "size", "target_column": "size", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A replica has a size configuration. Nullable for unmanaged replicas.'),

('has_size', 'cluster', 'replica_size',
 '{"kind": "foreign_key", "source_column": "size", "target_column": "size", "cardinality": "many_to_one", "nullable": true}'::jsonb,
 'A managed cluster has a default replica size. Nullable for unmanaged clusters.'),

-- =============================================================================
-- Polymorphic / union relationships (non-FK)
-- =============================================================================

('union_includes', 'relation', 'table',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "table"}'::jsonb,
 'mz_relations is a UNION view that includes tables.'),

('union_includes', 'relation', 'source',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "source"}'::jsonb,
 'mz_relations is a UNION view that includes sources.'),

('union_includes', 'relation', 'view',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "view"}'::jsonb,
 'mz_relations is a UNION view that includes views.'),

('union_includes', 'relation', 'mv',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "materialized-view"}'::jsonb,
 'mz_relations is a UNION view that includes materialized views.'),

('union_includes', 'object', 'relation',
 '{"kind": "union", "note": "mz_objects includes all relations plus indexes, connections, secrets, types, functions"}'::jsonb,
 'mz_objects is a UNION view that includes all relations.'),

('union_includes', 'object', 'index',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "index"}'::jsonb,
 'mz_objects includes indexes.'),

('union_includes', 'object', 'connection',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "connection"}'::jsonb,
 'mz_objects includes connections.'),

('union_includes', 'object', 'secret',
 '{"kind": "union", "discriminator_column": "type", "discriminator_value": "secret"}'::jsonb,
 'mz_objects includes secrets.'),

-- =============================================================================
-- Cross-layer ID mapping (non-FK)
-- =============================================================================

('maps_to_global_id', 'object', 'object',
 '{"kind": "maps_to", "via": "mz_internal.mz_object_global_ids", "from_type": "CatalogItemId", "to_type": "GlobalId", "note": "A CatalogItemId (SQL layer) maps to one or more GlobalIds (runtime layer). After ALTER operations, new GlobalIds may be assigned while CatalogItemId stays the same."}'::jsonb,
 'Maps between SQL-layer CatalogItemIds and runtime GlobalIds. Essential for joining mz_catalog tables with mz_internal introspection tables.');
-- Materialize Catalog Ontology: mz_internal & mz_introspection Link Types
-- Step 8 of implementation plan

INSERT INTO ma_mz_ontology.link_types (name, source_entity, target_entity, properties, description) VALUES

-- =============================================================================
-- Object dependencies
-- =============================================================================
('depends_on', 'object_dependency', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'The dependent object.'),

('depended_on_by', 'object_dependency', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "referenced_object_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'The object being depended on.'),

-- =============================================================================
-- CatalogItemId ↔ GlobalId mapping
-- =============================================================================
('has_global_id', 'object_global_id', 'object',
 '{"kind": "maps_to", "source_column": "id", "target_column": "id", "from_type": "CatalogItemId", "to_type": "GlobalId", "note": "id column is CatalogItemId, global_id column is GlobalId"}'::jsonb,
 'Maps a CatalogItemId to its GlobalId(s). Essential for joining mz_catalog with mz_introspection tables.'),

-- =============================================================================
-- Comments → objects
-- =============================================================================
('comment_on', 'comment', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A comment annotates an object (or a column of an object).'),

-- =============================================================================
-- Object history/lifetimes → objects
-- =============================================================================
('history_of', 'object_history', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Historical events for an object.'),

('lifetime_of', 'object_lifetime', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Lifetime span of an object.'),

-- =============================================================================
-- Replica status/metrics → replicas and clusters
-- =============================================================================
('status_of_replica', 'replica_status', 'replica',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Current status of a replica.'),

('status_history_of_replica', 'replica_status_history', 'replica',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Historical status events for a replica.'),

('metrics_of_replica', 'replica_metrics', 'replica',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'CPU and memory metrics for a replica.'),

('utilization_of_replica', 'replica_utilization', 'replica',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Computed utilization metrics for a replica.'),

-- =============================================================================
-- Compute dependencies and hydration
-- =============================================================================
('compute_depends_on', 'compute_dependency', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one", "note": "Uses GlobalId"}'::jsonb,
 'A compute object depends on another.'),

('compute_depended_by', 'compute_dependency', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "dependency_id", "target_column": "id", "cardinality": "many_to_one", "note": "Uses GlobalId"}'::jsonb,
 'The dependency target.'),

('hydration_of', 'hydration_status', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Hydration status of an object.'),

('hydration_on_replica', 'hydration_status', 'replica',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Which replica the hydration status is for.'),

-- =============================================================================
-- Frontiers → objects and replicas
-- =============================================================================
('frontier_of', 'frontier', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one", "note": "Uses GlobalId"}'::jsonb,
 'Read/write frontier for an object.'),

('frontier_on_replica', 'frontier', 'replica',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Which replica the frontier is measured on.'),

('global_frontier_of', 'global_frontier', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "one_to_one", "note": "Uses GlobalId"}'::jsonb,
 'Aggregated frontier across all replicas.'),

-- =============================================================================
-- Wallclock lag → objects
-- =============================================================================
('measures_lag_of', 'wallclock_lag_history', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "measures", "source_column": "object_id", "target_column": "id", "metric": "wallclock_lag"}'::jsonb,
 'Historical wallclock lag measurements for an object. Shows how fresh the data is.'),

('measures_global_lag_of', 'wallclock_global_lag', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "measures", "source_column": "object_id", "target_column": "id", "metric": "wallclock_lag_global"}'::jsonb,
 'Current wallclock lag aggregated across all replicas.'),

('measures_materialization_lag', 'materialization_lag', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "measures", "source_column": "object_id", "target_column": "id", "metric": "materialization_lag"}'::jsonb,
 'Lag between a materialization and the latest data from its inputs.'),

-- =============================================================================
-- Materialization dependencies
-- =============================================================================
('materialization_depends_on', 'materialization_dep', 'object',
 '{"source_id_type": "CatalogItemId", "kind": "depends_on", "source_column": "object_id", "target_column": "id"}'::jsonb,
 'A materialization depends on another object for its input data.'),

-- =============================================================================
-- Source/sink statistics → sources/sinks
-- =============================================================================
('statistics_of_source', 'source_statistics', 'source',
 '{"source_id_type": "CatalogItemId", "kind": "measures", "source_column": "id", "target_column": "id", "metric": "ingestion_statistics"}'::jsonb,
 'Ingestion statistics (messages, bytes, errors) for a source.'),

('status_of_source', 'source_status', 'source',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Current status of a source (running, stalled, starting, etc.).'),

('status_history_of_source', 'source_status_history', 'source',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "source_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Historical status events for a source.'),

('statistics_of_sink', 'sink_statistics', 'sink',
 '{"source_id_type": "CatalogItemId", "kind": "measures", "source_column": "id", "target_column": "id", "metric": "export_statistics"}'::jsonb,
 'Export statistics (messages, bytes) for a sink.'),

('status_of_sink', 'sink_status', 'sink',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'Current status of a sink.'),

('status_history_of_sink', 'sink_status_history', 'sink',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "sink_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'Historical status events for a sink.'),

-- =============================================================================
-- Storage shards → objects
-- =============================================================================
('shard_of', 'storage_shard', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A persist shard belongs to an object.'),

-- =============================================================================
-- Session/query → clusters
-- =============================================================================
('session_on_cluster', 'activity_log', 'cluster',
 '{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A query was executed on a cluster.'),

-- =============================================================================
-- mz_introspection: Dataflow relationships
-- =============================================================================
('operator_in_dataflow', 'dataflow_operator', 'dataflow',
 '{"kind": "foreign_key", "source_column": "dataflow_id", "target_column": "id", "cardinality": "many_to_one", "note": "Per-worker, join on (id, worker_id)"}'::jsonb,
 'An operator belongs to a dataflow.'),

('channel_in_dataflow', 'dataflow_channel', 'dataflow',
 '{"kind": "foreign_key", "source_column": "dataflow_id", "target_column": "id", "cardinality": "many_to_one"}'::jsonb,
 'A channel belongs to a dataflow.'),

('address_of_operator', 'dataflow_address', 'dataflow_operator',
 '{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}'::jsonb,
 'The scope address path of an operator.'),

-- =============================================================================
-- mz_introspection: Compute exports/frontiers → objects
-- =============================================================================
('export_of', 'compute_export', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "export_id", "target_column": "id", "cardinality": "many_to_one", "note": "Uses GlobalId"}'::jsonb,
 'A compute export corresponds to a maintained collection (MV, index, etc.).'),

('compute_frontier_of', 'compute_frontier', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "export_id", "target_column": "id", "cardinality": "many_to_one", "note": "Uses GlobalId"}'::jsonb,
 'The compute frontier for an exported collection.'),

('compute_import_frontier_of', 'compute_import_frontier', 'object',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "export_id", "target_column": "id", "cardinality": "many_to_one", "note": "Uses GlobalId"}'::jsonb,
 'The import frontier of a compute dependency.'),

-- =============================================================================
-- mz_introspection: Scheduling/peeks → operators
-- =============================================================================
('elapsed_for_operator', 'scheduling_elapsed', 'dataflow_operator',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "measures", "source_column": "id", "target_column": "id", "metric": "cpu_time_ns"}'::jsonb,
 'CPU time spent executing an operator. This is how you find what operators are expensive.'),

('arrangement_of_operator', 'arrangement_size', 'dataflow_operator',
 '{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "measures", "source_column": "operator_id", "target_column": "id", "metric": "arrangement_size"}'::jsonb,
 'Size of the arrangement maintained by an operator (records, batches, bytes).'),

-- =============================================================================
-- Cross-schema: mz_introspection uses GlobalId, mz_catalog uses CatalogItemId
-- =============================================================================
('introspection_uses_global_id', 'compute_export', 'object_global_id',
 '{"kind": "maps_to", "note": "mz_introspection tables use GlobalId. To join with mz_catalog tables (which use CatalogItemId), go through mz_internal.mz_object_global_ids: JOIN mz_object_global_ids ON export_id = global_id, then use the id column to match mz_catalog."}'::jsonb,
 'Introspection tables use GlobalIds. Map them to CatalogItemIds via mz_object_global_ids to join with mz_catalog.'),

-- Transitive dependencies (precomputed)
('transitively_depends_on', 'transitive_dependency', 'object',
 '{"kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one", "source_id_type": "CatalogItemId"}'::jsonb,
 'The dependent object (transitive closure — includes indirect dependencies).'),
('transitively_depended_on_by', 'transitive_dependency', 'object',
 '{"kind": "foreign_key", "source_column": "referenced_object_id", "target_column": "id", "cardinality": "many_to_one", "source_id_type": "CatalogItemId"}'::jsonb,
 'The object being depended on (transitive closure).');
