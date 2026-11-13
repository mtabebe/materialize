// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: data shards, served by a resolver from manifest shards.
//!
//! A data shard is a view over rows in two manifest shards (persist shards as they
//! work today): the control manifest shard (which data shards exist, their codecs
//! and schemas, which partition they're in) and a partition manifest shard (each
//! data shard's upper, batches and read intents, and the partition's claim). The
//! resolver (`mz-persist-resolver`) checks commits against its view, writes them
//! to the manifest shards, and serves views to clients.
//!
//! This module holds what both sides need: the row and wire types, and the client.
//! A client redirects a data shard at [crate::internal::apply::Applier]: it builds
//! today's `State` from the resolver's view of the data shard, and turns each
//! state transition into ops for the resolver ([client]). The code calls data
//! shards "v2" and persist shards "v1" in names.
//!
//! Times are `Codec64`-encoded `T`s read as little-endian u64s, so the resolver
//! orders them as u64s. That is right for `mz_repr::Timestamp` and `u64`, and is a
//! prototype assumption for any other `T`.

#![allow(missing_docs)]

use std::str::FromStr;

use mz_persist_types::ShardId;
use mz_persist_types::schema::SchemaId;
use prost::Message;
use timely::progress::Antichain;

pub mod client;

// The generated code names state.proto's types as `super::internal::state::*`.
use crate::internal;

#[allow(clippy::as_conversions, clippy::clone_on_ref_ptr)] // Generated code does this
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_client.v2.rs"));
}

pub use proto::proto_commit_response::Result as ProtoCommitResult;
pub use proto::proto_manifest_val::Kind as ValKind;
pub use proto::proto_op::Kind as OpKind;
pub use proto::proto_persist_resolver_client::ProtoPersistResolverClient;
pub use proto::proto_persist_resolver_server::{ProtoPersistResolver, ProtoPersistResolverServer};
pub use proto::*;

pub use crate::internal::state::{
    ProtoEncodedSchemas, ProtoHollowBatch, ProtoU64Antichain, ProtoU64Description,
};

/// A frontier of v2 virtual time, as the resolver sees it.
pub type Frontier = Antichain<u64>;

pub fn frontier(f: &Option<ProtoFrontier>) -> Frontier {
    Antichain::from(f.as_ref().map(|f| f.elements.clone()).unwrap_or_default())
}

pub fn proto_frontier(f: &Frontier) -> ProtoFrontier {
    ProtoFrontier {
        elements: f.elements().to_vec(),
    }
}

fn u64_antichain(f: &Option<ProtoU64Antichain>) -> Vec<u64> {
    f.as_ref()
        .map(|f| {
            f.elements
                .iter()
                .map(|x| u64::from_le_bytes(x.to_le_bytes()))
                .collect()
        })
        .unwrap_or_default()
}

/// Identifies a batch within its shard. Batches of one shard never overlap in time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BatchId {
    pub lower: Vec<u64>,
    pub upper: Vec<u64>,
}

impl BatchId {
    pub fn of(batch: &ProtoHollowBatch) -> BatchId {
        let desc = batch.desc.as_ref();
        BatchId {
            lower: u64_antichain(&desc.and_then(|d| d.lower.clone())),
            upper: u64_antichain(&desc.and_then(|d| d.upper.clone())),
        }
    }

    pub fn is_empty_batch(batch: &ProtoHollowBatch) -> bool {
        batch.len == 0 && batch.parts.is_empty()
    }
}

pub type PartitionId = String;
/// A reader's id: a `LeasedReaderId` or `CriticalReaderId` as a string, or
/// [SENTINEL_READER].
pub type ReaderId = String;

/// The reader id of the intent that keeps a shard's last frontier once every real
/// reader has dropped its intent.
pub const SENTINEL_READER: &str = "sentinel";

/// The key of a manifest row. Each key has at most one live value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ManifestKey {
    // Control manifest.
    Shard(ShardId),
    Schema(ShardId, SchemaId),
    Assign(ShardId),
    Partition(PartitionId),
    // Partition manifest.
    Claim,
    Upper(ShardId),
    Batch(ShardId, BatchId),
    Intent(ShardId, ReaderId),
    /// The control manifest time this partition is consistent with.
    ControlAt,
}

/// Which manifest a row lives in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Manifest {
    Control,
    Partition,
}

impl ManifestKey {
    pub fn manifest(&self) -> Manifest {
        match self {
            ManifestKey::Shard(_)
            | ManifestKey::Schema(..)
            | ManifestKey::Assign(_)
            | ManifestKey::Partition(_) => Manifest::Control,
            ManifestKey::Claim
            | ManifestKey::ControlAt
            | ManifestKey::Upper(_)
            | ManifestKey::Batch(..)
            | ManifestKey::Intent(..) => Manifest::Partition,
        }
    }

    pub fn shard(&self) -> Option<ShardId> {
        match self {
            ManifestKey::Shard(s)
            | ManifestKey::Schema(s, _)
            | ManifestKey::Assign(s)
            | ManifestKey::Upper(s)
            | ManifestKey::Batch(s, _)
            | ManifestKey::Intent(s, _) => Some(*s),
            ManifestKey::Partition(_) | ManifestKey::Claim | ManifestKey::ControlAt => None,
        }
    }

    pub fn to_proto(&self) -> ProtoManifestKey {
        use proto::proto_manifest_key::Kind;
        let kind = match self {
            ManifestKey::Shard(s) => Kind::Shard(s.to_string()),
            ManifestKey::Schema(s, id) => Kind::Schema(ProtoSchemaKey {
                shard: s.to_string(),
                schema_id: u64::try_from(id.0).expect("schema id fits"),
            }),
            ManifestKey::Assign(s) => Kind::Assign(s.to_string()),
            ManifestKey::Partition(p) => Kind::Partition(p.clone()),
            ManifestKey::Claim => Kind::Claim(ProtoEmpty {}),
            ManifestKey::Upper(s) => Kind::Upper(s.to_string()),
            ManifestKey::Batch(s, id) => Kind::Batch(ProtoBatchKey {
                shard: s.to_string(),
                id: Some(ProtoBatchId {
                    lower: Some(ProtoFrontier {
                        elements: id.lower.clone(),
                    }),
                    upper: Some(ProtoFrontier {
                        elements: id.upper.clone(),
                    }),
                }),
            }),
            ManifestKey::Intent(s, r) => Kind::Intent(ProtoIntentKey {
                shard: s.to_string(),
                reader: r.clone(),
            }),
            ManifestKey::ControlAt => Kind::ControlAt(ProtoEmpty {}),
        };
        ProtoManifestKey { kind: Some(kind) }
    }

    pub fn from_proto(proto: ProtoManifestKey) -> Result<ManifestKey, String> {
        use proto::proto_manifest_key::Kind;
        let shard = |s: &str| ShardId::from_str(s);
        let key = match proto.kind.ok_or("missing key kind")? {
            Kind::Shard(s) => ManifestKey::Shard(shard(&s)?),
            Kind::Schema(k) => ManifestKey::Schema(
                shard(&k.shard)?,
                SchemaId(usize::try_from(k.schema_id).map_err(|e| e.to_string())?),
            ),
            Kind::Assign(s) => ManifestKey::Assign(shard(&s)?),
            Kind::Partition(p) => ManifestKey::Partition(p),
            Kind::Claim(_) => ManifestKey::Claim,
            Kind::Upper(s) => ManifestKey::Upper(shard(&s)?),
            Kind::Batch(k) => {
                let id = k.id.unwrap_or_default();
                ManifestKey::Batch(
                    shard(&k.shard)?,
                    BatchId {
                        lower: id.lower.map(|f| f.elements).unwrap_or_default(),
                        upper: id.upper.map(|f| f.elements).unwrap_or_default(),
                    },
                )
            }
            Kind::Intent(k) => ManifestKey::Intent(shard(&k.shard)?, k.reader),
            Kind::ControlAt(_) => ManifestKey::ControlAt,
        };
        Ok(key)
    }

    pub fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    pub fn decode(buf: &[u8]) -> Result<ManifestKey, String> {
        let proto = ProtoManifestKey::decode(buf).map_err(|e| e.to_string())?;
        ManifestKey::from_proto(proto)
    }
}

pub fn encode_val(val: &ValKind) -> Vec<u8> {
    ProtoManifestVal {
        kind: Some(val.clone()),
    }
    .encode_to_vec()
}

pub fn decode_val(buf: &[u8]) -> Result<ValKind, String> {
    ProtoManifestVal::decode(buf)
        .map_err(|e| e.to_string())?
        .kind
        .ok_or_else(|| "missing val kind".to_owned())
}

/// Sets the inline write limits of persist shards in `configs`, which in a
/// resolver process are its manifest shards.
pub fn set_inline_write_limits(configs: &mz_dyncfg::ConfigSet, single: usize, total: usize) {
    let mut updates = mz_dyncfg::ConfigUpdates::default();
    updates.add(&crate::batch::INLINE_WRITES_SINGLE_MAX_BYTES, single);
    updates.add(&crate::batch::INLINE_WRITES_TOTAL_MAX_BYTES, total);
    updates.apply(configs);
}

/// The data shard an op names.
pub fn op_shard(op: &ProtoOp) -> Result<ShardId, String> {
    ShardId::from_str(&op.shard)
}
