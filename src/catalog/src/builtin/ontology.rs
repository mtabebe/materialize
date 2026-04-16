// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog ontology views derived from existing builtin definitions.
//!
//! Enumerates builtins that have `ontology: Some(...)` and generates 4 views:
//! - entity_types: from ontology.description + RelationDesc::keys()
//! - properties: from mz_columns + mz_comments + semantic type inference
//! - semantic_types: small const reference data
//! - link_types: from ontology.links on each builtin

use std::collections::BTreeMap;

use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::catalog::NameReference;

use super::{Builtin, BuiltinView, Ontology, PUBLIC_SELECT};

pub(super) fn generate_views(builtins: &[Builtin<NameReference>]) -> Vec<Builtin<NameReference>> {
    let infos: Vec<_> = builtins
        .iter()
        .filter_map(|b| {
            let (name, schema, desc, ontology) = match b {
                Builtin::Table(t) => (t.name, t.schema, &t.desc, t.ontology.as_ref()?),
                Builtin::View(v) => (v.name, v.schema, &v.desc, v.ontology.as_ref()?),
                Builtin::MaterializedView(mv) => {
                    (mv.name, mv.schema, &mv.desc, mv.ontology.as_ref()?)
                }
                Builtin::Source(s) => (s.name, s.schema, &s.desc, s.ontology.as_ref()?),
                _ => return None,
            };
            let entity_name = ontology.entity_name.to_string();
            Some(Info { table_name: name, schema_name: schema, entity_name, desc, ontology })
        })
        .collect();

    vec![
        Builtin::View(leak(entity_types_view(&infos))),
        Builtin::View(leak(semantic_types_view())),
        Builtin::View(leak(properties_view(&infos))),
        Builtin::View(leak(link_types_view(&infos))),
    ]
}

fn leak(v: BuiltinView) -> &'static BuiltinView { Box::leak(Box::new(v)) }

struct Info<'a> {
    table_name: &'static str,
    schema_name: &'static str,
    entity_name: String,
    desc: &'a RelationDesc,
    ontology: &'a Ontology,
}

fn esc(s: &str) -> String { s.replace('\'', "''") }

/// Build a simple ontology view from a name, OID, column defs, and SQL.
fn view(name: &'static str, o: u32, cols: &[(&'static str, SqlScalarType, bool)], sql: String) -> BuiltinView {
    let mut b = RelationDesc::builder();
    for (n, ty, nullable) in cols { b = b.with_column(*n, ty.clone().nullable(*nullable)); }
    BuiltinView {
        name, schema: MZ_INTERNAL_SCHEMA, oid: o,
        desc: b.finish(),
        column_comments: BTreeMap::new(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

fn pk_json(desc: &RelationDesc) -> Option<String> {
    let keys = desc.typ().keys.first()?;
    let cols: Vec<_> = keys.iter().map(|&i| format!("\"{}\"", desc.get_name(i))).collect();
    Some(format!("{{\"primary_key\": [{}]}}", cols.join(", ")))
}

// ── Semantic type inference from Rust types + column names ───

fn infer_semantic_type(col: &str, ty: &SqlScalarType, entity: &str) -> Option<&'static str> {
    // From the Rust type directly
    match ty {
        SqlScalarType::Oid => return Some("OID"),
        SqlScalarType::MzTimestamp => return Some("MzTimestamp"),
        SqlScalarType::TimestampTz { .. } => return Some("WallclockTimestamp"),
        _ => {}
    }
    match col {
        "owner_id" => return Some("RoleId"),
        "schema_id" => return Some("SchemaId"),
        "database_id" => return Some("DatabaseId"),
        "cluster_id" => return Some("ClusterId"),
        "replica_id" => return Some("ReplicaId"),
        "create_sql" | "definition" | "sql" => return Some("SqlDefinition"),
        "redacted_create_sql" => return Some("RedactedSqlDefinition"),
        "credits_per_hour" => return Some("CreditRate"),
        "role_id" | "member" | "grantor" | "grantee" => return Some("RoleId"),
        "global_id" | "transient_index_id" => return Some("GlobalId"),
        "shard_id" => return Some("ShardId"),
        _ => {}
    }
    if col == "id" {
        return match entity {
            "database" => Some("DatabaseId"), "schema" => Some("SchemaId"),
            "role" => Some("RoleId"), "cluster" => Some("ClusterId"),
            "replica" => Some("ReplicaId"), "network_policy" => Some("NetworkPolicyId"),
            "audit_event" | "dataflow" | "dataflow_operator" | "dataflow_address"
            | "dataflow_channel" | "scheduling_elapsed" | "storage_usage_by_shard" => None,
            _ => Some("CatalogItemId"),
        };
    }
    if matches!(col, "object_id" | "dependency_id" | "export_id" | "import_id") {
        return match entity {
            "frontier" | "global_frontier" | "wallclock_lag_history" | "wallclock_global_lag"
            | "wallclock_global_lag_history" | "hydration_status"
            | "compute_hydration_status_view" | "compute_dependency" | "storage_shard"
            | "compute_export" | "compute_frontier" | "compute_import_frontier"
            | "compute_error_count" | "arrangement_size" | "arrangement_sharing"
            | "scheduling_elapsed" | "scheduling_parks" | "peek_duration"
            | "active_peek" | "message_count" | "records_per_dataflow" => Some("GlobalId"),
            _ => Some("CatalogItemId"),
        };
    }
    if col.ends_with("_id") {
        return match col {
            "source_id" | "sink_id" | "on_id" | "connection_id" | "element_id" | "key_id"
            | "value_id" | "index_id" | "return_type_id" | "variadic_argument_type_id"
            | "slowest_local_input_id" | "slowest_global_input_id"
            | "referenced_object_id" => Some("CatalogItemId"),
            _ => None,
        };
    }
    // MzTimestamp stored as UInt64 in some views
    if matches!(ty, SqlScalarType::UInt64) && col.ends_with("_timestamp") {
        return Some("MzTimestamp");
    }
    // Byte/record counts (UInt64 or Int64 depending on the view)
    if matches!(ty, SqlScalarType::UInt64 | SqlScalarType::Int64) {
        if col.ends_with("_bytes") || col.starts_with("bytes_") || matches!(col,
            "size_bytes" | "result_size" | "size" | "capacity" | "heap_limit") {
            return Some("ByteCount");
        }
        if col.starts_with("messages_") || col.starts_with("updates_")
            || col.starts_with("snapshot_records_")
            || matches!(col, "records_indexed" | "rows_returned" | "records") {
            return Some("RecordCount");
        }
    }
    match col {
        "type" if entity == "connection" => Some("ConnectionType"),
        "type" if matches!(entity, "source" | "source_status") => Some("SourceType"),
        "object_type" => Some("ObjectType"),
        "type" if matches!(entity, "relation" | "object" | "audit_event" | "default_privilege") =>
            Some("ObjectType"),
        _ => None,
    }
}

// ── View builders ────────────────────────────────────────────

fn entity_types_view(infos: &[Info]) -> BuiltinView {
    let vals: Vec<_> = infos.iter().map(|i| {
        let pk = pk_json(i.desc).map_or("NULL::jsonb".into(), |j| format!("'{}'::jsonb", esc(&j)));
        format!("('{}','{}.{}',{},'{}')", esc(&i.entity_name), esc(i.schema_name), esc(i.table_name), pk, esc(i.ontology.description))
    }).collect();
    view("mz_ontology_entity_types", oid::VIEW_MZ_ONTOLOGY_ENTITY_TYPES_OID, &[
        ("name", SqlScalarType::String, false), ("relation", SqlScalarType::String, false),
        ("properties", SqlScalarType::Jsonb, true), ("description", SqlScalarType::String, true),
    ], format!("SELECT name::text,relation::text,properties::jsonb,description::text FROM (VALUES {}) AS t(name,relation,properties,description)", vals.join(",")))
}

fn semantic_types_view() -> BuiltinView {
    let vals: Vec<_> = SEMANTIC_TYPE_DEFS.iter()
        .map(|(n, t, d)| format!("('{}','{}','{}')", esc(n), esc(t), esc(d))).collect();
    view("mz_ontology_semantic_types", oid::VIEW_MZ_ONTOLOGY_SEMANTIC_TYPES_OID, &[
        ("name", SqlScalarType::String, false), ("sql_type", SqlScalarType::String, false),
        ("description", SqlScalarType::String, false),
    ], format!("SELECT name::text,sql_type::text,description::text FROM (VALUES {}) AS t(name,sql_type,description)", vals.join(",")))
}

fn properties_view(infos: &[Info]) -> BuiltinView {
    let mut ent = Vec::new();
    let mut ann = Vec::new();
    for i in infos {
        ent.push(format!("('{}','{}','{}')", esc(i.schema_name), esc(i.table_name), esc(&i.entity_name)));
        for (idx, col) in i.desc.iter_names().enumerate() {
            let scalar = &i.desc.typ().column_types[idx].scalar_type;
            if let Some(sem) = infer_semantic_type(col.as_str(), scalar, &i.entity_name) {
                ann.push(format!("('{}','{}','{}')", esc(&i.entity_name), esc(col.as_str()), sem));
            }
        }
    }
    view("mz_ontology_properties", oid::VIEW_MZ_ONTOLOGY_PROPERTIES_OID, &[
        ("entity_type", SqlScalarType::String, false), ("column_name", SqlScalarType::String, false),
        ("semantic_type", SqlScalarType::String, true), ("description", SqlScalarType::String, true),
    ], format!(
        "SELECT ent.entity_name AS entity_type,col.name AS column_name,\
         ann.semantic_type::text AS semantic_type,cmt.comment AS description \
         FROM (VALUES {ent}) AS ent(schema_name,table_name,entity_name) \
         JOIN mz_catalog.mz_schemas s ON s.name=ent.schema_name \
         JOIN mz_catalog.mz_objects o ON o.schema_id=s.id AND o.name=ent.table_name \
         JOIN mz_catalog.mz_columns col ON col.id=o.id \
         LEFT JOIN mz_internal.mz_comments cmt ON cmt.id=o.id AND cmt.object_sub_id=col.position \
         LEFT JOIN (VALUES {ann}) AS ann(entity_name,column_name,semantic_type) \
         ON ann.entity_name=ent.entity_name AND ann.column_name=col.name",
        ent = ent.join(","), ann = ann.join(","),
    ))
}

fn link_types_view(infos: &[Info]) -> BuiltinView {
    let vals: Vec<_> = infos.iter().flat_map(|i| {
        i.ontology.links.iter().map(move |l| format!(
            "('{}','{}','{}','{{}}'::jsonb,'{}')",
            esc(l.name), esc(&i.entity_name), esc(l.target), esc(l.description),
        ))
    }).collect();
    view("mz_ontology_link_types", oid::VIEW_MZ_ONTOLOGY_LINK_TYPES_OID, &[
        ("name", SqlScalarType::String, false), ("source_entity", SqlScalarType::String, false),
        ("target_entity", SqlScalarType::String, false), ("properties", SqlScalarType::Jsonb, true),
        ("description", SqlScalarType::String, true),
    ], format!("SELECT name::text,source_entity::text,target_entity::text,properties::jsonb,description::text FROM (VALUES {}) AS t(name,source_entity,target_entity,properties,description)", vals.join(",")))
}

// ── Semantic type reference data ─────────────────────────────

const SEMANTIC_TYPE_DEFS: &[(&str, &str, &str)] = &[
    ("CatalogItemId", "text", "SQL-layer object ID. Format: s{n}/u{n}."),
    ("GlobalId", "text", "Runtime ID used by compute/storage. Format: s{n}/u{n}/si{n}."),
    ("ClusterId", "text", "Cluster ID. Format: s{n}/u{n}."),
    ("ReplicaId", "text", "Cluster replica ID. Format: s{n}/u{n}."),
    ("SchemaId", "text", "Schema ID. Format: s{n}/u{n}."),
    ("DatabaseId", "text", "Database ID. Format: s{n}/u{n}."),
    ("RoleId", "text", "Role ID. Format: s{n}/g{n}/u{n}/p."),
    ("NetworkPolicyId", "text", "Network policy ID. Format: s{n}/u{n}."),
    ("ShardId", "text", "Persist shard ID. Format: s{uuid}."),
    ("OID", "oid", "PostgreSQL-compatible object identifier."),
    ("ObjectType", "text", "Catalog object type discriminator."),
    ("ConnectionType", "text", "Connection type discriminator."),
    ("SourceType", "text", "Source type discriminator."),
    ("MzTimestamp", "mz_timestamp", "Internal logical timestamp (uint64)."),
    ("WallclockTimestamp", "timestamp with time zone", "Wall clock timestamp."),
    ("ByteCount", "uint8", "A count of bytes."),
    ("RecordCount", "uint8", "A count of records/rows."),
    ("CreditRate", "numeric", "Credits consumed per hour."),
    ("SqlDefinition", "text", "A SQL CREATE statement."),
    ("RedactedSqlDefinition", "text", "A redacted SQL CREATE statement."),
];
