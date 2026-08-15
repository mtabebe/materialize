// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retain-only references: pinning specific blobs against GC without holding a
//! compaction `since`.
//!
//! A copy-on-write fork of a shard inherits its source's batch parts by pointer
//! rather than by copy, so those blobs have to outlive the source's compaction
//! of them. The capability that gates blob deletion, `seqno_since`, lives only
//! on `LeasedReaderState`, which also carries a `since`; holding blobs through
//! it would freeze the source's compaction for as long as the fork exists,
//! which is the one cost a branch is supposed to avoid.
//!
//! A retain-only reference names the exact keys the fork inherited. GC keeps
//! those and nothing more, so retention is proportional to the fork point
//! rather than to the source's churn, and the reference contributes no `since`,
//! no seqno capability, and no constraint on rollups or truncation.
//!
//! NOTE: GC truncates the diffs that record a blob's deletion, so once a pinned
//! blob is skipped the source forgets it was ever deleted. Release therefore
//! deletes the blobs itself rather than leaving them to a later GC that can no
//! longer see them.

use std::fmt::Debug;

use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use uuid::Uuid;

use crate::internal::machine::Machine;
use crate::parse_id;

/// An opaque identifier for a retain-only reference on a shard.
///
/// It is durable: a branch records it alongside its fork so teardown can
/// release the pin and bootstrap can find it again.
#[derive(
    Arbitrary,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
#[serde(try_from = "String", into = "String")]
pub struct RetainId(pub(crate) [u8; 16]);

impl std::fmt::Display for RetainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for RetainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetainId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for RetainId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id("t", "RetainId", s).map(RetainId)
    }
}

impl From<RetainId> for String {
    fn from(id: RetainId) -> Self {
        id.to_string()
    }
}

impl TryFrom<String> for RetainId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl RetainId {
    /// Returns a random [`RetainId`] that is reasonably likely never to have
    /// been generated before.
    pub fn new() -> Self {
        RetainId(*Uuid::new_v4().as_bytes())
    }
}

/// A live retain-only reference on a shard.
#[derive(Debug)]
pub struct RetainHandle<K, V, T, D> {
    pub(crate) id: RetainId,
    pub(crate) machine: Machine<K, V, T, D>,
}

impl<K, V, T, D> RetainHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64,
{
    /// Drops the pin and deletes every blob it was the last reference to,
    /// returning how many were deleted.
    ///
    /// Blobs another reference still pins, or that are still live in the
    /// shard's own trace, are left alone. Releasing an id that is not held is a
    /// no-op, which makes teardown safe to retry.
    pub async fn release(self) -> usize {
        self.machine.release_parts(self.id).await
    }
}
