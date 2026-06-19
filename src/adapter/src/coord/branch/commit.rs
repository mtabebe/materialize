// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable commit of a branch's state and registration of its catalog items.
//!
//! Given a set of freshly initialized fork shards plus the branch's identity,
//! [`persist_branch_state`] writes the `fork_blob_refs` rows and the
//! `BranchDescriptor` entries for each branched object in a single
//! transaction. After the transaction commits, the branch's catalog items
//! become visible through normal catalog APIs: branched sources are
//! registered with `ingestion_enabled = false`, branched sinks with
//! `emission_enabled = false`, and branched tables / MVs / indexes with
//! their normal flags.

use mz_persist::fork_blob_refs::{ForkBlobRef, ForkBlobRefs};
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use uuid::Uuid;

use mz_catalog::durable::objects::BranchDescriptor;

/// Per-object information needed to register one branched object in the
/// catalog and pin its blobs against GC.
#[derive(Debug, Clone)]
pub struct BranchedObject {
    pub branch_catalog_id: CatalogItemId,
    pub branch_global_id: GlobalId,
    pub source_catalog_id: CatalogItemId,
    pub fork_shard_id: mz_persist_client::ShardId,
    pub relation_desc: Vec<u8>,
    pub absolute_blob_keys: Vec<String>,
}

/// Branch-level identity shared by every object in [`BranchedObject`].
#[derive(Debug, Clone)]
pub struct BranchIdentity {
    pub branch_id: Uuid,
    pub branch_name: String,
    pub owner: mz_repr::role_id::RoleId,
    pub branch_ts: Timestamp,
    pub created_at_ms: u64,
}

/// Failure modes for [`persist_branch_state`].
#[derive(Debug)]
pub enum PersistBranchError {
    /// `fork_blob_refs` insert failed mid-flight; no descriptors were
    /// written. Retrying with the same inputs is safe.
    RefInsertFailed(String),
    /// `BranchDescriptor` writes failed after `fork_blob_refs` were
    /// inserted. The inserted ref rows will be cleaned up on the next
    /// `DROP BRANCH`-style retry.
    DescriptorWriteFailed(String),
    /// The orchestration needed to coordinate the two writes atomically is
    /// not yet wired into the coordinator. Concretely this needs the
    /// catalog `Transaction::insert_branch_descriptor` helper from the
    /// follow-on plus the `ForkBlobRefs` handle to be plumbed onto the
    /// coordinator.
    NotYetWired,
}

impl std::fmt::Display for PersistBranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistBranchError::RefInsertFailed(msg) => {
                write!(f, "fork_blob_refs insert failed: {msg}")
            }
            PersistBranchError::DescriptorWriteFailed(msg) => {
                write!(f, "BranchDescriptor write failed: {msg}")
            }
            PersistBranchError::NotYetWired => f.write_str(
                "branch commit requires catalog Transaction + ForkBlobRefs plumbing",
            ),
        }
    }
}

impl std::error::Error for PersistBranchError {}

/// Build the [`ForkBlobRef`] rows that should be inserted for `objects` under
/// the given `branch_id`. Pure: no I/O, suitable for unit tests of the row
/// shape.
pub fn build_ref_rows(branch_id: Uuid, objects: &[BranchedObject]) -> Vec<ForkBlobRef> {
    let mut rows = Vec::new();
    for obj in objects {
        for blob_key in &obj.absolute_blob_keys {
            rows.push(ForkBlobRef {
                blob_key: blob_key.clone(),
                fork_shard_id: obj.fork_shard_id.to_string(),
                branch_id,
            });
        }
    }
    rows
}

/// Build the [`BranchDescriptor`] entries that should be inserted into the
/// catalog. Pure: no I/O.
pub fn build_descriptors(
    identity: &BranchIdentity,
    objects: &[BranchedObject],
) -> Vec<BranchDescriptor> {
    objects
        .iter()
        .map(|obj| BranchDescriptor {
            branch_catalog_id: obj.branch_catalog_id,
            fork_shard_id: obj.fork_shard_id,
            branch_ts: u64::from(identity.branch_ts),
            source_catalog_id: obj.source_catalog_id,
            branch_global_id: obj.branch_global_id,
            relation_desc: obj.relation_desc.clone(),
            branch_id: identity.branch_id.to_string(),
            branch_name: identity.branch_name.clone(),
            owner: identity.owner,
            created_at_ms: identity.created_at_ms,
        })
        .collect()
}

/// Durably record the branch in one CRDB transaction and register its catalog
/// items. The implementation needs catalog `Transaction` plumbing and the
/// coordinator's `ForkBlobRefs` handle, both of which are deferred. The
/// function exposes the signature so the sequencer hook can call it.
#[allow(dead_code)]
pub async fn persist_branch_state(
    _refs: &ForkBlobRefs,
    _identity: BranchIdentity,
    _objects: Vec<BranchedObject>,
) -> Result<(), PersistBranchError> {
    Err(PersistBranchError::NotYetWired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist_client::ShardId;
    use mz_repr::role_id::RoleId;

    fn make_identity(branch_id: Uuid) -> BranchIdentity {
        BranchIdentity {
            branch_id,
            branch_name: "b".to_owned(),
            owner: RoleId::User(1),
            branch_ts: Timestamp::new(7),
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn make_object(catalog_id: u64, blob_keys: Vec<String>) -> BranchedObject {
        BranchedObject {
            branch_catalog_id: CatalogItemId::User(catalog_id),
            branch_global_id: GlobalId::User(catalog_id),
            source_catalog_id: CatalogItemId::User(catalog_id - 1),
            fork_shard_id: ShardId::new(),
            relation_desc: Vec::new(),
            absolute_blob_keys: blob_keys,
        }
    }

    #[mz_ore::test]
    fn build_ref_rows_emits_one_row_per_blob_per_object() {
        let branch = Uuid::new_v4();
        let objects = vec![
            make_object(11, vec!["a".to_owned(), "b".to_owned()]),
            make_object(12, vec!["c".to_owned()]),
        ];
        let rows = build_ref_rows(branch, &objects);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.branch_id == branch));
        // Every row's fork_shard_id matches one of the input objects.
        let object_shards: Vec<String> = objects
            .iter()
            .map(|o| o.fork_shard_id.to_string())
            .collect();
        for row in &rows {
            assert!(object_shards.contains(&row.fork_shard_id));
        }
    }

    #[mz_ore::test]
    fn build_descriptors_carries_branch_identity() {
        let branch = Uuid::new_v4();
        let identity = make_identity(branch);
        let objects = vec![make_object(11, vec![]), make_object(12, vec![])];
        let descs = build_descriptors(&identity, &objects);
        assert_eq!(descs.len(), 2);
        for desc in &descs {
            assert_eq!(desc.branch_id, branch.to_string());
            assert_eq!(desc.branch_name, "b");
            assert_eq!(desc.branch_ts, 7);
            assert_eq!(desc.owner, RoleId::User(1));
        }
        // Each descriptor's branch_catalog_id matches one of the input
        // objects: the function is faithful per-object.
        let descriptor_ids: Vec<_> = descs.iter().map(|d| d.branch_catalog_id).collect();
        for obj in &objects {
            assert!(descriptor_ids.contains(&obj.branch_catalog_id));
        }
    }

    #[mz_ore::test]
    fn build_ref_rows_empty_objects_yields_empty() {
        assert!(build_ref_rows(Uuid::new_v4(), &[]).is_empty());
    }

    #[mz_ore::test]
    fn build_ref_rows_object_with_no_blobs_yields_no_rows() {
        let branch = Uuid::new_v4();
        let objects = vec![make_object(11, vec![])];
        assert!(build_ref_rows(branch, &objects).is_empty());
    }
}
