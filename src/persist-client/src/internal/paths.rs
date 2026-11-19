// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter, Write};
use std::ops::Deref;
use std::str::FromStr;

use mz_persist::location::SeqNo;
use proptest_derive::Arbitrary;
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::internal::encoding::parse_id;
use crate::{BlobLeaseId, ShardId, WriterId};

/// An opaque identifier for an individual batch of a persist durable TVC (aka
/// shard).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PartId(pub(crate) [u8; 16]);

impl std::fmt::Display for PartId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for PartId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartId({})", Uuid::from_bytes(self.0))
    }
}

impl FromStr for PartId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id("p", "PartId", s).map(PartId)
    }
}

impl PartId {
    pub(crate) fn new() -> Self {
        PartId(*Uuid::new_v4().as_bytes())
    }
}

/// A component that provides information about the writer of a blob.
/// For older blobs, this is a UUID for the specific writer instance;
/// for newer blobs, this is a string representing the version at which the blob was written.
/// In either case, it's used to help determine whether a blob may eventually
/// be linked into state, or whether it's junk that we can clean up.
/// Note that the ordering is meaningful: all writer-id keys are considered smaller than
/// all version keys.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum WriterKey {
    Id(WriterId),
    Version(String),
}

impl WriterKey {
    /// This uses the version numbering scheme specified in [mz_build_info::BuildInfo::version_num].
    /// (And asserts that the version isn't so large that that scheme is no longer sufficient.)
    pub fn for_version(version: &Version) -> WriterKey {
        assert!(version.major <= 99);
        assert!(version.minor <= 999);
        assert!(version.patch <= 99);
        WriterKey::Version(format!(
            "{:02}{:03}{:02}",
            version.major, version.minor, version.patch
        ))
    }
}

impl FromStr for WriterKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("empty version string".to_owned());
        }

        let key = match &s[..1] {
            "w" => WriterKey::Id(WriterId::from_str(s)?),
            "n" => WriterKey::Version(s[1..].to_owned()),
            c => {
                return Err(format!("unknown prefix for version: {c}"));
            }
        };
        Ok(key)
    }
}

impl Display for WriterKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterKey::Id(id) => id.fmt(f),
            WriterKey::Version(s) => {
                f.write_char('n')?;
                f.write_str(s)
            }
        }
    }
}

/// Partially encoded path used in [mz_persist::location::Blob] storage.
/// Composed of a [WriterId] and [PartId]. Can be completed with a [ShardId] to
/// form a full [BlobKey].
///
/// Used to reduce the bytes needed to refer to a blob key in memory and in
/// persistent state, all access to blobs are always within the context of an
/// individual shard.
#[derive(
    Arbitrary,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct PartialBatchKey(pub(crate) String);

fn split_batch_key(key: &str) -> Result<(WriterKey, PartId), String> {
    let (writer_key, part_id) = key
        .split_once('/')
        .ok_or_else(|| "partial batch key should contain a /".to_owned())?;

    let writer_key = WriterKey::from_str(writer_key)?;
    let part_id = PartId::from_str(part_id)?;
    Ok((writer_key, part_id))
}

impl PartialBatchKey {
    pub fn new(version: &WriterKey, part_id: &PartId) -> Self {
        PartialBatchKey(format!("{}/{}", version, part_id))
    }

    pub fn split(&self) -> Option<(WriterKey, PartId)> {
        split_batch_key(&self.0).ok()
    }

    pub fn complete(&self, shard_id: &ShardId) -> BlobKey {
        KeyRoot::Shard(*shard_id).complete(self)
    }
}

/// The prefix a batch's parts are written under.
///
/// A `Leased` batch never joins a shard: its parts live until its lease expires, and it must
/// never be passed to `compare_and_append`.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum KeyRoot {
    /// `s<shard>`, the root of every shard's own blobs.
    Shard(ShardId),
    /// `leased/l<lease>/<object>`.
    Leased { lease: BlobLeaseId, object: Uuid },
}

/// The top-level segment of every leased key.
pub const LEASED_PREFIX: &str = "leased";

impl KeyRoot {
    pub fn complete(&self, key: &PartialBatchKey) -> BlobKey {
        BlobKey(format!("{}/{}", self, key))
    }

    pub fn is_leased(&self) -> bool {
        matches!(self, KeyRoot::Leased { .. })
    }

    /// The shard id this root's metrics are recorded under. For a leased root this is a shard id
    /// formed from the object uuid, matching what the peek stash used before leased roots.
    pub fn metrics_shard(&self) -> ShardId {
        match self {
            KeyRoot::Shard(shard) => *shard,
            KeyRoot::Leased { object, .. } => {
                ShardId::from_str(&format!("s{}", object)).expect("a uuid is a valid shard id")
            }
        }
    }
}

impl From<ShardId> for KeyRoot {
    fn from(shard: ShardId) -> Self {
        KeyRoot::Shard(shard)
    }
}

impl From<&ShardId> for KeyRoot {
    fn from(shard: &ShardId) -> Self {
        KeyRoot::Shard(*shard)
    }
}

impl std::fmt::Display for KeyRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyRoot::Shard(shard) => write!(f, "{}", shard),
            KeyRoot::Leased { lease, object } => write!(f, "{LEASED_PREFIX}/{lease}/{object}"),
        }
    }
}

impl std::fmt::Display for PartialBatchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// An opaque identifier for an individual blob of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RollupId(pub(crate) [u8; 16]);

impl std::fmt::Display for RollupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for RollupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RollupId({})", Uuid::from_bytes(self.0))
    }
}

impl FromStr for RollupId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id("r", "RollupId", s).map(RollupId)
    }
}

impl RollupId {
    pub(crate) fn new() -> Self {
        RollupId(*Uuid::new_v4().as_bytes())
    }
}

/// Partially encoded path used in [mz_persist::location::Blob] storage.
/// Composed of a [SeqNo] and [RollupId]. Can be completed with a [ShardId] to
/// form a full [BlobKey].
///
/// Used to reduce the bytes needed to refer to a blob key in memory and in
/// persistent state, all access to blobs are always within the context of an
/// individual shard.
#[derive(
    Arbitrary,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct PartialRollupKey(pub(crate) String);

impl PartialRollupKey {
    pub fn new(seqno: SeqNo, rollup_id: &RollupId) -> Self {
        PartialRollupKey(format!("{}/{}", seqno, rollup_id))
    }

    pub fn complete(&self, shard_id: &ShardId) -> BlobKey {
        BlobKey(format!("{}/{}", shard_id, self))
    }
}

impl std::fmt::Display for PartialRollupKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for PartialRollupKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A parsed, partial path used in [mz_persist::location::Blob] storage.
///
/// This enumerates all types of partial blob keys used in persist.
#[derive(Debug, PartialEq)]
pub enum PartialBlobKey {
    /// A parsed [PartialBatchKey].
    Batch(WriterKey, PartId),
    /// A parsed [PartialRollupKey].
    Rollup(SeqNo, RollupId),
}

/// Fully encoded path used in [mz_persist::location::Blob] storage. Composed of
/// a [ShardId], [WriterId] and [PartId].
///
/// Use when directly interacting with a [mz_persist::location::Blob], otherwise
/// use [PartialBatchKey] or [PartialRollupKey] to refer to a blob without
/// needing to copy the [ShardId].
#[derive(Clone, Debug, PartialEq)]
pub struct BlobKey(String);

impl std::fmt::Display for BlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Deref for BlobKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Any key persist writes to [mz_persist::location::Blob], parsed.
#[derive(Debug, PartialEq)]
pub enum ParsedBlobKey {
    Shard(ShardId, PartialBlobKey),
    Leased {
        lease: BlobLeaseId,
        object: Uuid,
        writer: WriterKey,
        part: PartId,
    },
}

impl BlobKey {
    /// Parses a shard key or a leased key.
    pub fn parse(key: &str) -> Result<ParsedBlobKey, String> {
        let Some(rest) = key
            .strip_prefix(LEASED_PREFIX)
            .and_then(|r| r.strip_prefix('/'))
        else {
            return Self::parse_ids(key).map(|(s, k)| ParsedBlobKey::Shard(s, k));
        };
        let mut it = rest.splitn(3, '/');
        let (Some(lease), Some(object), Some(part)) = (it.next(), it.next(), it.next()) else {
            return Err(format!("invalid leased blob key: {}", key));
        };
        let lease = BlobLeaseId::from_str(lease)?;
        let object = Uuid::parse_str(object).map_err(|e| format!("invalid object {key}: {e}"))?;
        let (writer, part) = split_batch_key(part)?;
        Ok(ParsedBlobKey::Leased {
            lease,
            object,
            writer,
            part,
        })
    }

    pub fn parse_ids(key: &str) -> Result<(ShardId, PartialBlobKey), String> {
        let err = || {
            format!(
                "invalid blob key format. expected either <shard_id>/<writer_id>/<part_id> or <shard_id>/<seqno>/<rollup_id>. got: {}",
                key
            )
        };
        let (shard, blob) = key.split_once('/').ok_or_else(err)?;
        let shard_id = ShardId::from_str(shard)?;

        let blob_key = if blob.starts_with('w') | blob.starts_with('n') {
            let (writer, part) = split_batch_key(blob)?;
            PartialBlobKey::Batch(writer, part)
        } else {
            let (seqno, rollup) = blob.split_once('/').ok_or_else(err)?;
            PartialBlobKey::Rollup(SeqNo::from_str(seqno)?, RollupId::from_str(rollup)?)
        };
        Ok((shard_id, blob_key))
    }
}

/// Represents the prefix of a blob path. Used for selecting subsets of blobs
#[derive(Debug)]
pub enum BlobKeyPrefix<'a> {
    /// For accessing all blobs
    All,
    /// Scoped to the batch and state rollup blobs of an individual shard
    Shard(&'a ShardId),
    /// Scoped to every blob under one lease.
    Leased(&'a BlobLeaseId),
    /// Scoped to the batch blobs of an individual writer
    #[cfg(test)]
    Writer(&'a ShardId, &'a WriterKey),
    /// Scoped to all state rollup blobs  of an individual shard
    #[cfg(test)]
    Rollups(&'a ShardId),
}

impl std::fmt::Display for BlobKeyPrefix<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BlobKeyPrefix::All => "".into(),
            BlobKeyPrefix::Shard(shard) => format!("{}", shard),
            BlobKeyPrefix::Leased(lease) => format!("{LEASED_PREFIX}/{lease}/"),
            #[cfg(test)]
            BlobKeyPrefix::Writer(shard, writer) => format!("{}/{}", shard, writer),
            #[cfg(test)]
            BlobKeyPrefix::Rollups(shard) => format!("{}/v", shard),
        };
        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use semver::Version;

    fn gen_version() -> impl Strategy<Value = Version> {
        (0u64..=99, 0u64..=999, 0u64..=99)
            .prop_map(|(major, minor, patch)| Version::new(major, minor, patch))
    }

    #[mz_ore::test]
    fn key_ordering_compatible() {
        // The WriterKey's ordering should never disagree with the Version ordering.
        // (Though the writer key might compare equal when the version does not.)
        proptest!(|(a in gen_version(), b in gen_version())| {
            let a_key = WriterKey::for_version(&a);
            let b_key = WriterKey::for_version(&b);
            if a >= b {
                assert!(a_key >= b_key);
            }
            if a <= b {
                assert!(a_key <= b_key);
            }
        })
    }

    #[mz_ore::test]
    fn partial_blob_key_completion() {
        let (shard_id, writer_id, part_id) = (ShardId::new(), WriterId::new(), PartId::new());
        let partial_key = PartialBatchKey::new(&WriterKey::Id(writer_id.clone()), &part_id);
        assert_eq!(
            partial_key.complete(&shard_id),
            BlobKey(format!("{}/{}/{}", shard_id, writer_id, part_id))
        );
    }

    #[mz_ore::test]
    fn blob_key_parse() -> Result<(), String> {
        let (shard_id, writer_id, part_id) = (ShardId::new(), WriterId::new(), PartId::new());

        // can parse full blob key
        assert_eq!(
            BlobKey::parse_ids(&format!("{}/{}/{}", shard_id, writer_id, part_id)),
            Ok((
                shard_id,
                PartialBlobKey::Batch(WriterKey::Id(writer_id), part_id.clone())
            ))
        );

        let version = Version::new(1, 0, 0);
        assert_eq!(
            BlobKey::parse_ids(&format!(
                "{}/{}/{}",
                shard_id,
                WriterKey::for_version(&version),
                part_id
            )),
            Ok((
                shard_id,
                PartialBlobKey::Batch(WriterKey::for_version(&version), part_id.clone())
            ))
        );

        // fails on invalid blob key formats
        assert!(matches!(
            BlobKey::parse_ids(&format!("{}/{}", WriterId::new(), PartId::new())),
            Err(_)
        ));
        assert!(matches!(
            BlobKey::parse_ids(&format!(
                "{}/{}/{}/{}",
                ShardId::new(),
                WriterId::new(),
                PartId::new(),
                PartId::new()
            )),
            Err(_)
        ));
        assert!(matches!(BlobKey::parse_ids("abc/def/ghi"), Err(_)));
        assert!(matches!(BlobKey::parse_ids(""), Err(_)));

        // fails if shard/writer/part id are in the wrong spots
        assert!(matches!(
            BlobKey::parse_ids(&format!(
                "{}/{}/{}",
                PartId::new(),
                ShardId::new(),
                WriterId::new()
            )),
            Err(_)
        ));

        // leased keys round trip through `parse` and are rejected by `parse_ids`.
        let (lease, object) = (BlobLeaseId::new(), Uuid::new_v4());
        let root = KeyRoot::Leased { lease, object };
        let partial = PartialBatchKey::new(&WriterKey::for_version(&version), &part_id);
        let key = root.complete(&partial);
        assert_eq!(
            key.to_string(),
            format!("leased/{lease}/{object}/{partial}")
        );
        assert_eq!(
            BlobKey::parse(&key),
            Ok(ParsedBlobKey::Leased {
                lease,
                object,
                writer: WriterKey::for_version(&version),
                part: part_id.clone(),
            })
        );
        assert!(BlobKey::parse_ids(&key).is_err());
        assert!(key.starts_with(&BlobKeyPrefix::Leased(&lease).to_string()));
        assert!(matches!(
            BlobKey::parse(&partial.complete(&shard_id)),
            Ok(ParsedBlobKey::Shard(..))
        ));

        Ok(())
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn leased_and_shard_keys_disjoint() {
        proptest!(|(shard in any::<ShardId>(), lease in any::<BlobLeaseId>(), object in any::<[u8; 16]>())| {
            let shard_s = shard.to_string();
            prop_assert!(!shard_s.starts_with(LEASED_PREFIX));
            let partial = PartialBatchKey::new(&WriterKey::Version("0010000".into()), &PartId::new());
            let leased = KeyRoot::Leased { lease, object: Uuid::from_bytes(object) }.complete(&partial);
            prop_assert!(BlobKey::parse_ids(&leased).is_err());
            let is_leased = matches!(BlobKey::parse(&leased), Ok(ParsedBlobKey::Leased { .. }));
            prop_assert!(is_leased);
        });
    }
}
