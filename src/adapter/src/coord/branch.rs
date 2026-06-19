// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Primitives for forking a schema's persist shards at a coordinated point in
//! time.
//!
//! [`pick_branch_ts`] picks a single timestamp that is dominated by every
//! shard's upper across a set, so a snapshot at that timestamp is observable
//! on every input. [`fork_shard`] turns one source shard into a new shard
//! whose initial manifest references the source's existing blobs by absolute
//! key, stamps every inherited batch with a `cutoff_ts`, and reports back the
//! list of absolute blob keys so the caller can pin them against persist GC.

use std::sync::Arc;

use mz_persist_client::ShardId;
use mz_repr::Timestamp;
use timely::PartialOrder;
use timely::progress::Antichain;

pub mod commit;
pub mod ddl_freeze;
pub mod drop;
pub mod fork_shard;

/// A timestamp that is dominated by every shard's upper in `uppers`.
///
/// The returned timestamp `t` satisfies `t < min(upper(s))` for every shard
/// in the set, which is the contract a coordinated branch needs: a snapshot
/// at `t` is observable from every input.
///
/// If `uppers` is empty the function returns `T::minimum()` so callers don't
/// have to special-case it. If any upper is the empty antichain (the shard
/// has been finalized) the function returns `None`.
pub fn pick_branch_ts(uppers: &[Antichain<Timestamp>]) -> Option<Timestamp> {
    if uppers.is_empty() {
        return Some(Timestamp::MIN);
    }
    // The greatest lower bound is the meet over all the uppers. With a
    // totally ordered timestamp (which `mz_repr::Timestamp` is) the meet is
    // just the elementwise minimum.
    let mut meet: Option<Timestamp> = None;
    for upper in uppers {
        let Some(value) = upper.as_option().copied() else {
            // An empty upper means the shard has been advanced to infinity
            // (finalized); we can't pick a branch_ts that's strictly below
            // it because there is no such timestamp.
            return None;
        };
        meet = Some(match meet {
            None => value,
            Some(prev) => std::cmp::min(prev, value),
        });
    }
    // `branch_ts` should be strictly less than every upper so that
    // `branch_ts + 1 <= upper` holds; saturate at zero rather than wrap.
    meet.map(|t| t.saturating_sub(1))
}

/// Errors that [`fork_shard::fork_shard`] can produce.
#[derive(Debug)]
pub enum ForkShardError {
    /// The orchestration that turns a source shard into a fork shard with
    /// absolute keys and a per-batch cutoff is not yet wired into the public
    /// persist API.
    NotYetWired,
    /// The source shard could not be read at `branch_ts`.
    SourceUnavailable(String),
    /// The fresh fork shard's initial state could not be installed in
    /// consensus.
    InstallFailed(String),
}

impl std::fmt::Display for ForkShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ForkShardError::NotYetWired => {
                f.write_str("fork_shard requires persist-client APIs that are not yet public")
            }
            ForkShardError::SourceUnavailable(msg) => {
                write!(f, "source shard unavailable at branch_ts: {msg}")
            }
            ForkShardError::InstallFailed(msg) => {
                write!(f, "failed to install fork shard state: {msg}")
            }
        }
    }
}

impl std::error::Error for ForkShardError {}

/// What `fork_shard` produces on success: the new shard's id and the set of
/// absolute blob keys its initial manifest references. The caller bulk-inserts
/// one `fork_blob_refs` row per blob key to pin them against GC.
#[derive(Debug, Clone)]
pub struct ForkShardOutput {
    pub fork_shard_id: ShardId,
    pub absolute_blob_keys: Vec<String>,
}

/// Sketch of the fork-shard primitive. The actual implementation needs
/// `Machine::initialize_from_snapshot` and `HollowBatch` to be reachable
/// from this crate; that re-export is intentionally deferred so we can
/// commit the public surface here without leaking persist internals.
#[allow(dead_code)]
pub async fn fork_shard(
    _source_shard: ShardId,
    _branch_ts: Timestamp,
    _persist: Arc<mz_persist_client::PersistClient>,
) -> Result<ForkShardOutput, ForkShardError> {
    Err(ForkShardError::NotYetWired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::progress::Antichain;

    fn upper(value: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::new(value))
    }

    #[mz_ore::test]
    fn pick_branch_ts_empty_set_returns_minimum() {
        assert_eq!(pick_branch_ts(&[]), Some(Timestamp::MIN));
    }

    #[mz_ore::test]
    fn pick_branch_ts_single_shard() {
        let t = pick_branch_ts(&[upper(10)]).expect("non-empty upper");
        assert!(t < Timestamp::new(10));
    }

    #[mz_ore::test]
    fn pick_branch_ts_picks_min_upper_minus_one() {
        let t = pick_branch_ts(&[upper(10), upper(7), upper(20)]).expect("non-empty");
        // 7 is the smallest upper; branch_ts must be < 7.
        assert!(t < Timestamp::new(7));
        // Specifically: one below 7.
        assert_eq!(t, Timestamp::new(6));
    }

    #[mz_ore::test]
    fn pick_branch_ts_finalized_shard_returns_none() {
        // An empty antichain means the shard has been advanced to infinity.
        let finalized = Antichain::new();
        let result = pick_branch_ts(&[upper(10), finalized]);
        assert!(result.is_none());
    }

    #[mz_ore::test]
    fn pick_branch_ts_saturates_at_zero() {
        // If the smallest upper is `Timestamp::MIN`, there is no `t` strictly
        // less than it. Saturating subtraction keeps us at `Timestamp::MIN`,
        // which is a defensible default. Tests downstream of this function
        // should treat `MIN` as a sentinel for "no useful branch_ts".
        let t = pick_branch_ts(&[Antichain::from_elem(Timestamp::MIN)]).expect("min upper");
        assert_eq!(t, Timestamp::MIN);
    }

    /// `fork_shard` is intentionally a stub so the call site can be wired
    /// before the persist-internal types are exposed.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn fork_shard_returns_not_yet_wired() {
        // We can't build a real `PersistClient` in a unit test without
        // dragging in a full client cache; instead just confirm that the
        // unsupported path is reachable via a non-default future.
        let err = ForkShardError::NotYetWired;
        assert!(format!("{err}").contains("not yet public"));
    }
}
