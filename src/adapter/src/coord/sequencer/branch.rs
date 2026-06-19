// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sequencer hooks for the BRANCH DDL surface.

use mz_repr::{Datum, IntoRowIterator, Row};
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{CreateBranchPlan, DropBranchPlan, ShowBranchStatusPlan};
use mz_sql::session::metadata::SessionMetadata;
use uuid::Uuid;

use crate::AdapterError;
use crate::ExecuteResponse;
use crate::coord::Coordinator;
use crate::coord::branch::commit::{
    BranchIdentity, BranchedObject, persist_branch_state,
};
use crate::session::Session;

impl Coordinator {
    /// Execute `CREATE BRANCH <name> FROM SCHEMA <schema>`.
    ///
    /// Resolves the source schema, allocates a fresh `branch_id`, and writes
    /// a `BranchDescriptor` row recording the branch's identity. This MVP
    /// slice records the branch without forking any per-object shards yet,
    /// so `SHOW BRANCHES` / `DROP BRANCH` / source DDL freeze become
    /// observable.
    pub(crate) async fn sequence_create_branch(
        &mut self,
        session: &Session,
        plan: CreateBranchPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
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
        let schema_name = plan
            .source_schema
            .0
            .last()
            .expect("non-empty schema name")
            .as_str();
        conn_catalog
            .resolve_schema(database_name, schema_name)
            .map_err(|err| {
                AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: mz_catalog::memory::error::ErrorKind::Sql(err),
                })
            })?;
        drop(conn_catalog);

        let branch_id = Uuid::new_v4();
        let now_ms = (self.catalog().config().now)();
        let identity = BranchIdentity {
            branch_id,
            branch_name: plan.branch_name.clone(),
            owner: *session.current_role_id(),
            branch_ts: self.peek_local_write_ts().await,
            created_at_ms: now_ms,
        };
        let objects: Vec<BranchedObject> = Vec::new();

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
        // Drain the ops we just inserted; the standard catalog pipeline
        // does this in `catalog_transact_inner` before calling commit. We
        // take the direct-Transaction path because branch state isn't a
        // `catalog::Op` yet.
        let _ = txn.get_and_commit_op_updates();
        let commit_ts = txn.upper();
        txn.commit(commit_ts)
            .await
            .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!(err)))?;
        drop(storage);

        let rows: Vec<Row> = Vec::new();
        Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        })
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
                let rows: Vec<Row> = Vec::new();
                return Ok(ExecuteResponse::SendingRowsImmediate {
                    rows: Box::new(rows.into_row_iter()),
                });
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

        let rows: Vec<Row> = Vec::new();
        Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        })
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
