// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sequencer hooks for the BRANCH DDL surface.

use std::sync::Arc;

use mz_catalog::memory::objects::{CatalogItem, Table};
use mz_persist_client::{Diagnostics, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, GlobalId, IntoRowIterator, RelationDesc, Row, Timestamp};
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaSpecifier,
};
use mz_sql::plan::{CreateBranchPlan, DropBranchPlan, ShowBranchStatusPlan};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use uuid::Uuid;

use crate::AdapterError;
use crate::ExecuteResponse;
use crate::catalog;
use crate::coord::Coordinator;
use crate::coord::branch::commit::{
    BranchIdentity, BranchedObject, persist_branch_state,
};
use crate::session::Session;

impl Coordinator {
    /// Execute `CREATE BRANCH <name> FROM SCHEMA <schema>`.
    ///
    /// For each table in the source schema we fork its persist shard at a
    /// coordinated `branch_ts`, register a fresh branched-table catalog item
    /// bound to that fork shard via `storage_collections_to_register`, and
    /// record a [`BranchDescriptor`] for bookkeeping. Reads and writes against
    /// the branched table flow through the standard table machinery: the
    /// table's global id resolves to the fork shard via `storage_metadata`,
    /// the fork's absolute blob keys + `cutoff_ts` serve source history, and
    /// new updates land in the fork's own blob namespace.
    pub(crate) async fn sequence_create_branch(
        &mut self,
        session: &Session,
        plan: CreateBranchPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Resolve the source schema to its (database_id, schema_id).
        let conn_catalog = self.catalog().for_session(session);
        let database_name = match plan.source_schema.0.len() {
            1 => None,
            2 => Some(plan.source_schema.0[0].as_str()),
            _ => {
                return Err(AdapterError::Unstructured(anyhow::anyhow!(
                    "qualified schema name {} has too many parts",
                    plan.source_schema,
                )));
            }
        };
        let source_schema_name = plan
            .source_schema
            .0
            .last()
            .expect("non-empty schema name")
            .as_str()
            .to_owned();
        let resolved = conn_catalog
            .resolve_schema(database_name, &source_schema_name)
            .map_err(|err| {
                AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: mz_catalog::memory::error::ErrorKind::Sql(err),
                })
            })?;
        let database_spec = resolved.database().clone();
        let database_id = match &database_spec {
            ResolvedDatabaseSpecifier::Id(id) => *id,
            ResolvedDatabaseSpecifier::Ambient => {
                return Err(AdapterError::Unstructured(anyhow::anyhow!(
                    "cannot branch from ambient schema {}",
                    source_schema_name,
                )));
            }
        };
        let source_schema_spec: SchemaSpecifier = resolved.id().clone();
        let database_name_str = conn_catalog
            .get_database(&database_id)
            .name()
            .to_owned();
        drop(conn_catalog);

        // Snapshot the source schema's tables. We snapshot identity + desc +
        // create_sql up front so the subsequent fork loop doesn't hold a
        // borrow on catalog state across the await points.
        struct SourceTable {
            item_id: mz_repr::CatalogItemId,
            name: String,
            global_id: GlobalId,
            desc: RelationDesc,
            create_sql: String,
        }
        let source_tables: Vec<SourceTable> = {
            let state = self.catalog().state();
            let schema = state.get_schema(
                &database_spec,
                &source_schema_spec,
                session.conn_id(),
            );
            schema
                .items
                .iter()
                .filter_map(|(name, item_id)| {
                    let entry = state.get_entry(item_id);
                    let item = entry.item();
                    match item {
                        CatalogItem::Table(t) => {
                            let create_sql = t.create_sql.clone()?;
                            Some(SourceTable {
                                item_id: *item_id,
                                name: name.clone(),
                                global_id: entry.latest_global_id(),
                                desc: t.desc.latest(),
                                create_sql,
                            })
                        }
                        _ => None,
                    }
                })
                .collect()
        };

        let branch_id = Uuid::new_v4();
        let now_ms = (self.catalog().config().now)();
        let branch_ts = self.peek_local_write_ts().await;

        // Fork each source table's persist shard. Each fork inherits the
        // source's history up to `branch_ts` via absolute blob keys + cutoff
        // and accepts new writes into its own blob namespace.
        struct ForkedTable {
            source: SourceTable,
            branch_item_id: mz_repr::CatalogItemId,
            branch_global_id: GlobalId,
            fork_shard_id: ShardId,
            absolute_blob_keys: Vec<String>,
        }
        let mut forked = Vec::with_capacity(source_tables.len());
        for source in source_tables {
            let source_meta = self
                .controller
                .storage_collections
                .collection_metadata(source.global_id)
                .map_err(|err| {
                    AdapterError::Unstructured(anyhow::anyhow!(
                        "missing storage metadata for source table {}: {err:?}",
                        source.name
                    ))
                })?;
            let key_schema = Arc::new(source_meta.relation_desc.clone());
            let val_schema = Arc::new(UnitSchema);
            let fork = self
                .persist_client
                .fork_shard::<SourceData, (), Timestamp, StorageDiff>(
                    source_meta.data_shard,
                    branch_ts,
                    Diagnostics {
                        shard_name: format!("branch:{}:{}", plan.branch_name, source.name),
                        handle_purpose: "branch fork".to_owned(),
                    },
                    key_schema,
                    val_schema,
                )
                .await
                .map_err(|err| {
                    AdapterError::Unstructured(anyhow::anyhow!(
                        "fork_shard failed for {}: {err:?}",
                        source.name
                    ))
                })?;
            let (branch_item_id, branch_global_id) = self.allocate_user_id().await?;
            forked.push(ForkedTable {
                source,
                branch_item_id,
                branch_global_id,
                fork_shard_id: fork.fork_shard_id,
                absolute_blob_keys: fork.absolute_blob_keys,
            });
        }

        // Phase 1: create the branch schema. We need its id (assigned during
        // this transaction) to build QualifiedItemName for the branched-table
        // CreateItem ops, so the schema and items go in separate transactions.
        let schema_op = catalog::Op::CreateSchema {
            database_id: database_spec.clone(),
            schema_name: plan.branch_name.clone(),
            owner_id: *session.current_role_id(),
        };
        self.catalog_transact(Some(session), vec![schema_op]).await?;

        // Resolve the freshly-created branch schema's id.
        let branch_schema_spec = {
            let conn_catalog = self.catalog().for_session(session);
            conn_catalog
                .resolve_schema(Some(&database_name_str), &plan.branch_name)
                .map_err(|err| {
                    AdapterError::Catalog(mz_catalog::memory::error::Error {
                        kind: mz_catalog::memory::error::ErrorKind::Sql(err),
                    })
                })?
                .id()
                .clone()
        };

        // Phase 2: register one branched-table item per forked source table.
        let mut item_ops: Vec<catalog::Op> = Vec::new();
        for f in &forked {
            let branched_create_sql = retarget_create_sql(
                &f.source.create_sql,
                &database_name_str,
                &source_schema_name,
                &plan.branch_name,
            );
            let mut collections = std::collections::BTreeMap::new();
            collections.insert(mz_repr::RelationVersion::root(), f.branch_global_id);
            let table = Table {
                create_sql: Some(branched_create_sql),
                desc: mz_repr::VersionedRelationDesc::new(f.source.desc.clone()),
                collections,
                conn_id: None,
                resolved_ids: mz_sql::names::ResolvedIds::empty(),
                custom_logical_compaction_window: None,
                is_retained_metrics_object: false,
                data_source: mz_catalog::memory::objects::TableDataSource::TableWrites {
                    defaults: vec![mz_sql::ast::Expr::null(); f.source.desc.arity()],
                },
                branch_target_shard: Some(f.fork_shard_id),
            };
            item_ops.push(catalog::Op::CreateItem {
                id: f.branch_item_id,
                name: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: database_spec.clone(),
                        schema_spec: branch_schema_spec.clone(),
                    },
                    item: f.source.name.clone(),
                },
                item: CatalogItem::Table(table),
                owner_id: *session.current_role_id(),
            });
        }
        if !item_ops.is_empty() {
            self.catalog_transact(Some(session), item_ops).await?;
        }

        // Bookkeeping: pin fork blobs against GC and write descriptor rows.
        let objects: Vec<BranchedObject> = forked
            .iter()
            .map(|f| BranchedObject {
                branch_catalog_id: f.branch_item_id,
                branch_global_id: f.branch_global_id,
                source_catalog_id: f.source.item_id,
                fork_shard_id: f.fork_shard_id,
                relation_desc: encode_relation_desc(&f.source.desc),
                absolute_blob_keys: f.absolute_blob_keys.clone(),
            })
            .collect();
        let identity = BranchIdentity {
            branch_id,
            branch_name: plan.branch_name.clone(),
            owner: *session.current_role_id(),
            branch_ts,
            created_at_ms: now_ms,
        };
        let mut storage = self.catalog().storage_mut().await;
        let mut txn = storage
            .transaction()
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        persist_branch_state(
            self.fork_blob_refs.as_ref(),
            &mut txn,
            identity,
            objects,
        )
        .await
        .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err.to_string())))?;
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts)
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        drop(storage);

        Ok(ExecuteResponse::CreatedSchema)
    }

    /// Execute `DROP BRANCH [IF EXISTS] <name>`.
    pub(crate) async fn sequence_drop_branch(
        &mut self,
        _session: &Session,
        plan: DropBranchPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let DropBranchPlan {
            branch_name,
            if_exists,
        } = plan;

        let mut storage = self.catalog().storage_mut().await;
        let mut txn = storage
            .transaction()
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;

        let descriptors: Vec<_> = txn
            .get_branch_descriptors()
            .filter(|d| d.branch_name == branch_name)
            .collect();
        if descriptors.is_empty() {
            if if_exists {
                return Ok(ExecuteResponse::DroppedObject(
                    mz_sql::catalog::ObjectType::Schema,
                ));
            }
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "branch \"{branch_name}\" does not exist"
            )));
        }
        let branch_id_str = descriptors[0].branch_id.clone();
        let branch_id_uuid = Uuid::parse_str(&branch_id_str).map_err(|err| {
            AdapterError::Unstructured(anyhow::anyhow!(
                "branch row has invalid branch_id: {err}"
            ))
        })?;

        let _removed = self
            .fork_blob_refs
            .delete_by_branch(branch_id_uuid)
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err.to_string())))?;
        let _ = txn
            .drop_branch_descriptors_by_branch(&branch_id_str)
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts)
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        drop(storage);

        Ok(ExecuteResponse::DroppedObject(
            mz_sql::catalog::ObjectType::Schema,
        ))
    }

    /// Execute `SHOW BRANCHES`.
    pub(crate) async fn sequence_show_branches(
        &mut self,
        _session: &Session,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut storage = self.catalog().storage_mut().await;
        let txn = storage
            .transaction()
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        let mut rows: Vec<Row> = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for d in txn.get_branch_descriptors() {
            if !seen.insert((d.branch_name.clone(), d.branch_id.clone())) {
                continue;
            }
            let mut row = Row::default();
            let mut packer = row.packer();
            packer.push(Datum::String(&d.branch_name));
            packer.push(Datum::String(&d.branch_id));
            packer.push(Datum::Int64(
                i64::try_from(d.created_at_ms).unwrap_or(i64::MAX),
            ));
            rows.push(row);
        }
        rows.sort();
        Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        })
    }

    /// Execute `SHOW BRANCH STATUS <name>`.
    pub(crate) async fn sequence_show_branch_status(
        &mut self,
        _session: &Session,
        plan: ShowBranchStatusPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut storage = self.catalog().storage_mut().await;
        let txn = storage
            .transaction()
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        let descriptors: Vec<_> = txn
            .get_branch_descriptors()
            .filter(|d| d.branch_name == plan.branch_name)
            .collect();
        if descriptors.is_empty() {
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "branch \"{}\" does not exist",
                plan.branch_name,
            )));
        }
        let head = &descriptors[0];
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.push(Datum::String(&head.branch_name));
        packer.push(Datum::String(&head.branch_id));
        packer.push(Datum::Int64(
            i64::try_from(head.created_at_ms).unwrap_or(i64::MAX),
        ));
        packer.push(Datum::Int64(descriptors.len() as i64));
        let rows = vec![row];
        Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        })
    }
}

/// Encode `desc` to the proto-bytes form stored on a [`BranchDescriptor`].
fn encode_relation_desc(desc: &RelationDesc) -> Vec<u8> {
    use mz_proto::RustType;
    use prost::Message;
    desc.into_proto().encode_to_vec()
}

/// Rewrite a source table's `create_sql` so it names the branch schema in
/// place of the source schema. The catalog persists `create_sql` in a
/// canonicalized, fully-quoted form (`"db"."schema"."name"`), so the
/// substitution matches the quoted `"db"."source_schema".` prefix.
fn retarget_create_sql(
    source_sql: &str,
    database: &str,
    source_schema: &str,
    branch_schema: &str,
) -> String {
    let from_quoted = format!("\"{database}\".\"{source_schema}\".");
    let to_quoted = format!("\"{database}\".\"{branch_schema}\".");
    if source_sql.contains(&from_quoted) {
        return source_sql.replacen(&from_quoted, &to_quoted, 1);
    }
    let from_bare = format!("{database}.{source_schema}.");
    let to_bare = format!("{database}.{branch_schema}.");
    if source_sql.contains(&from_bare) {
        return source_sql.replacen(&from_bare, &to_bare, 1);
    }
    source_sql.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn retarget_create_sql_handles_quoted_qualified_names() {
        let src = "CREATE TABLE \"materialize\".\"public\".\"t\" (\"i\" [s20 AS \"pg_catalog\".\"int4\"])";
        let out = retarget_create_sql(src, "materialize", "public", "nightly");
        assert!(
            out.contains("\"materialize\".\"nightly\".\"t\""),
            "expected branch schema substitution; got: {out}"
        );
        assert!(
            !out.contains("\"materialize\".\"public\".\"t\""),
            "source schema should be replaced; got: {out}"
        );
    }

    #[mz_ore::test]
    fn retarget_create_sql_handles_bare_qualified_names() {
        let src = "CREATE TABLE materialize.public.t (i int)";
        let out = retarget_create_sql(src, "materialize", "public", "nightly");
        assert_eq!(out, "CREATE TABLE materialize.nightly.t (i int)");
    }

    #[mz_ore::test]
    fn retarget_create_sql_unchanged_when_no_match() {
        let src = "CREATE TABLE t (i int)";
        let out = retarget_create_sql(src, "materialize", "public", "nightly");
        assert_eq!(out, src);
    }
}
