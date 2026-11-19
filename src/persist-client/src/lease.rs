// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Blob leases: the lifetime that owns everything under `leased/l<lease>/`.
//!
//! Leases are rows of a dedicated persist shard, the lease shard, one row per lease. Every
//! change is a snapshot at the shard's upper followed by a `compare_and_append` at that upper, so
//! two changes to one row can't both land on the same observed state.
//!
//! PROTOTYPE: the shard keeps one reader at its initial since and never compacts, and rows of
//! expired leases are never removed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};
use mz_ore::now::SYSTEM_TIME;
use mz_persist_types::codec_impls::{StringSchema, VecU8Schema};
use prost::Message;
use timely::progress::Antichain;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::error::UpperMismatch;
use crate::read::ReadHandle;
use crate::write::WriteHandle;
use crate::{BlobLeaseId, Diagnostics, PersistClient, ShardId};

pub(crate) const LEASE_RENEW_INTERVAL: Config<Duration> = Config::new(
    "persist_lease_renew_interval",
    Duration::from_secs(15 * 60),
    "How often a blob lease holder renews its lease.",
    mz_dyncfg::ParameterScope::Environment,
);

pub(crate) const LEASE_FENCE_TIMEOUT: Config<Duration> = Config::new(
    "persist_lease_fence_timeout",
    Duration::from_secs(30 * 60),
    "A blob lease holder halts after this long without a successful renewal. Copied into the \
    lease's row at create.",
    mz_dyncfg::ParameterScope::Environment,
);

pub(crate) const LEASE_EXPIRY_SLACK: Config<Duration> = Config::new(
    "persist_lease_expiry_slack",
    Duration::from_secs(5 * 60),
    "An observer expires a lease after seeing it unrenewed for its fence timeout plus this.",
    mz_dyncfg::ParameterScope::Environment,
);

/// Timing for a [BlobLeaseHolder].
#[derive(Debug, Clone, Copy)]
pub struct LeaseTiming {
    pub renew_interval: Duration,
    pub fence_timeout: Duration,
    pub expiry_slack: Duration,
}

impl LeaseTiming {
    pub fn from_config(cfg: &ConfigSet) -> Self {
        LeaseTiming {
            renew_interval: LEASE_RENEW_INTERVAL.get(cfg),
            fence_timeout: LEASE_FENCE_TIMEOUT.get(cfg),
            expiry_slack: LEASE_EXPIRY_SLACK.get(cfg),
        }
    }
}

/// The wire form of a [LeaseRow]. Unknown fields survive decoding, and an unknown `state` decodes
/// to [LeaseState::Unknown].
#[derive(Clone, PartialEq, Message)]
struct ProtoLeaseRow {
    #[prost(string, tag = "1")]
    purpose: String,
    #[prost(string, tag = "2")]
    build_version: String,
    #[prost(uint64, tag = "3")]
    holder_wall_ms: u64,
    #[prost(uint64, tag = "4")]
    fence_timeout_ms: u64,
    #[prost(uint64, tag = "5")]
    renewal: u64,
    #[prost(int32, tag = "6")]
    state: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Live,
    Expired,
    /// Written by a newer version. Neither renewed nor expired by this one.
    Unknown(i32),
}

impl LeaseState {
    fn to_i32(self) -> i32 {
        match self {
            LeaseState::Live => 1,
            LeaseState::Expired => 2,
            LeaseState::Unknown(x) => x,
        }
    }
    fn from_i32(x: i32) -> Self {
        match x {
            1 => LeaseState::Live,
            2 => LeaseState::Expired,
            x => LeaseState::Unknown(x),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseDebug {
    pub purpose: String,
    pub build_version: String,
    /// The holder's wall clock at its last write. Never used for expiry.
    pub holder_wall_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRow {
    pub debug: LeaseDebug,
    pub fence_timeout: Duration,
    pub renewal: u64,
    pub state: LeaseState,
}

impl LeaseRow {
    fn encode(&self) -> Vec<u8> {
        ProtoLeaseRow {
            purpose: self.debug.purpose.clone(),
            build_version: self.debug.build_version.clone(),
            holder_wall_ms: self.debug.holder_wall_ms,
            fence_timeout_ms: u64::try_from(self.fence_timeout.as_millis()).expect("fits"),
            renewal: self.renewal,
            state: self.state.to_i32(),
        }
        .encode_to_vec()
    }

    fn decode(buf: &[u8]) -> Self {
        let p = ProtoLeaseRow::decode(buf).expect("valid lease row");
        LeaseRow {
            debug: LeaseDebug {
                purpose: p.purpose,
                build_version: p.build_version,
                holder_wall_ms: p.holder_wall_ms,
            },
            fence_timeout: Duration::from_millis(p.fence_timeout_ms),
            renewal: p.renewal,
            state: LeaseState::from_i32(p.state),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Exists;

#[derive(Debug, PartialEq, Eq)]
pub enum RenewError {
    Expired,
    Missing,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExpireError {
    Renewed(u64),
    AlreadyExpired,
    Missing,
    NotLive(LeaseState),
}

/// The lease shard's rows as of one upper. A change computed from it can only land at that upper.
#[derive(Debug, Clone)]
pub struct Observed {
    upper: u64,
    rows: BTreeMap<BlobLeaseId, (Vec<u8>, LeaseRow)>,
}

impl Observed {
    pub fn rows(&self) -> impl Iterator<Item = (&BlobLeaseId, &LeaseRow)> {
        self.rows.iter().map(|(k, (_, r))| (k, r))
    }
}

type K = String;
type V = Vec<u8>;

/// A handle to the lease shard.
#[derive(Debug)]
pub struct LeaseShard {
    write: WriteHandle<K, V, u64, i64>,
    read: ReadHandle<K, V, u64, i64>,
    build_version: String,
}

impl LeaseShard {
    pub async fn open(client: &PersistClient, shard_id: ShardId) -> Self {
        let diagnostics = Diagnostics {
            shard_name: "blob_leases".into(),
            handle_purpose: "blob lease shard".into(),
        };
        let (mut write, read) = client
            .open::<K, V, u64, i64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(VecU8Schema),
                diagnostics,
                false,
            )
            .await
            .expect("lease shard codecs never change");
        // Make time 0 readable, as `DurableCache::new` does, so a snapshot at `upper - 1` exists.
        let _ = write
            .compare_and_append_batch(
                &mut [],
                Antichain::from_elem(0),
                Antichain::from_elem(1),
                true,
            )
            .await
            .expect("valid usage");
        LeaseShard {
            write,
            read,
            build_version: client.cfg.build_version.to_string(),
        }
    }

    /// Reads every row as of the current upper.
    pub async fn observe(&mut self) -> Observed {
        {
            let upper = *self
                .write
                .fetch_recent_upper()
                .await
                .as_option()
                .expect("lease shard is never closed");
            let updates = self
                .read
                .snapshot_and_fetch(Antichain::from_elem(upper - 1))
                .await
                .expect("the lease shard's since is never advanced");
            let mut rows = BTreeMap::new();
            for ((k, v), _t, d) in updates {
                assert_eq!(d, 1, "lease rows consolidate to one per lease");
                let lease: BlobLeaseId = k.parse().expect("valid lease id");
                let row = LeaseRow::decode(&v);
                let prev = rows.insert(lease, (v, row));
                assert!(prev.is_none(), "one row per lease");
            }
            Observed { upper, rows }
        }
    }

    /// Replaces `lease`'s row as `observed` saw it with `new`, at `observed`'s upper.
    async fn commit(
        &mut self,
        observed: &Observed,
        lease: BlobLeaseId,
        new: &LeaseRow,
    ) -> Result<(), UpperMismatch<u64>> {
        let key = lease.to_string();
        let mut updates = vec![((key.clone(), new.encode()), observed.upper, 1i64)];
        if let Some((old, _)) = observed.rows.get(&lease) {
            updates.push(((key, old.clone()), observed.upper, -1));
        }
        self.write
            .compare_and_append(
                updates,
                Antichain::from_elem(observed.upper),
                Antichain::from_elem(observed.upper + 1),
            )
            .await
            .expect("valid usage")
    }

    fn now_debug(&self, purpose: &str) -> LeaseDebug {
        LeaseDebug {
            purpose: purpose.to_owned(),
            build_version: self.build_version.clone(),
            holder_wall_ms: SYSTEM_TIME(),
        }
    }

    pub async fn create(
        &mut self,
        lease: BlobLeaseId,
        fence_timeout: Duration,
        purpose: &str,
    ) -> Result<(), Exists> {
        loop {
            let observed = self.observe().await;
            if observed.rows.contains_key(&lease) {
                return Err(Exists);
            }
            let row = LeaseRow {
                debug: self.now_debug(purpose),
                fence_timeout,
                renewal: 0,
                state: LeaseState::Live,
            };
            if self.commit(&observed, lease, &row).await.is_ok() {
                return Ok(());
            }
        }
    }

    /// Decides a renewal against `observed`.
    pub fn plan_renew(observed: &Observed, lease: BlobLeaseId) -> Result<LeaseRow, RenewError> {
        let Some((_, row)) = observed.rows.get(&lease) else {
            return Err(RenewError::Missing);
        };
        match row.state {
            LeaseState::Live => Ok(LeaseRow {
                renewal: row.renewal + 1,
                ..row.clone()
            }),
            LeaseState::Expired | LeaseState::Unknown(_) => Err(RenewError::Expired),
        }
    }

    /// Decides an expiry against `observed`.
    pub fn plan_expire(
        observed: &Observed,
        lease: BlobLeaseId,
        expected_renewal: u64,
    ) -> Result<LeaseRow, ExpireError> {
        let Some((_, row)) = observed.rows.get(&lease) else {
            return Err(ExpireError::Missing);
        };
        match row.state {
            LeaseState::Expired => Err(ExpireError::AlreadyExpired),
            LeaseState::Unknown(_) => Err(ExpireError::NotLive(row.state)),
            LeaseState::Live if row.renewal != expected_renewal => {
                Err(ExpireError::Renewed(row.renewal))
            }
            LeaseState::Live => Ok(LeaseRow {
                state: LeaseState::Expired,
                ..row.clone()
            }),
        }
    }

    /// Applies a change planned against `observed`. `Ok(Err(_))` means another write landed
    /// first, and the caller must observe again.
    pub async fn try_apply(
        &mut self,
        observed: &Observed,
        lease: BlobLeaseId,
        new: &LeaseRow,
    ) -> Result<(), UpperMismatch<u64>> {
        let mut new = new.clone();
        new.debug.holder_wall_ms = SYSTEM_TIME();
        self.commit(observed, lease, &new).await
    }

    /// Bumps `lease`'s renewal if it is live.
    ///
    /// Only the holder renews its own lease, so finding a higher renewal than it last wrote (a
    /// renewal whose acknowledgement was lost) is still a successful renewal.
    pub async fn renew(&mut self, lease: BlobLeaseId) -> Result<u64, RenewError> {
        loop {
            let observed = self.observe().await;
            let new = Self::plan_renew(&observed, lease)?;
            if self.try_apply(&observed, lease, &new).await.is_ok() {
                return Ok(new.renewal);
            }
        }
    }

    /// Marks `lease` expired iff it is live with renewal `expected_renewal`.
    pub async fn expire(
        &mut self,
        lease: BlobLeaseId,
        expected_renewal: u64,
    ) -> Result<(), ExpireError> {
        loop {
            let observed = self.observe().await;
            let new = Self::plan_expire(&observed, lease, expected_renewal)?;
            if self.try_apply(&observed, lease, &new).await.is_ok() {
                return Ok(());
            }
        }
    }

    pub async fn snapshot(&mut self) -> BTreeMap<BlobLeaseId, LeaseRow> {
        self.observe()
            .await
            .rows
            .into_iter()
            .map(|(k, (_, r))| (k, r))
            .collect()
    }
}

/// Called when a holder must stop using its lease. Production passes a closure that halts.
pub type OnFenced = Arc<dyn Fn(&str) + Send + Sync>;

/// Holds one blob lease: renews it, self-fences, and expires other holders' leases.
#[derive(Debug)]
pub struct BlobLeaseHolder;

impl BlobLeaseHolder {
    /// Creates a lease and spawns the task that holds it. The task ends after calling
    /// `on_fenced`.
    pub async fn start(
        mut shard: LeaseShard,
        timing: LeaseTiming,
        purpose: &str,
        on_fenced: OnFenced,
    ) -> (BlobLeaseId, mz_ore::task::JoinHandle<()>) {
        let lease = BlobLeaseId::new();
        // Measured from before the create, so the holder's deadline can't trail the durable
        // write an observer measures from.
        let started = Instant::now();
        shard
            .create(lease, timing.fence_timeout, purpose)
            .await
            .expect("a fresh lease id is unique");
        let handle = mz_ore::task::spawn(|| "blob_lease_holder", async move {
            Self::run(shard, lease, timing, started, on_fenced).await
        });
        (lease, handle)
    }

    async fn run(
        mut shard: LeaseShard,
        lease: BlobLeaseId,
        timing: LeaseTiming,
        mut last_renew_started: Instant,
        on_fenced: OnFenced,
    ) {
        // Other leases: the renewal last seen, and when this process first saw it.
        let mut seen: BTreeMap<BlobLeaseId, (u64, Instant)> = BTreeMap::new();
        let mut next_tick = Instant::now() + timing.renew_interval;
        loop {
            // The deadline counts from the start of the last successful renewal, not its
            // completion: an observer's clock starts no earlier than the renewal's commit, which
            // comes after the start. Every await below races this deadline, so a renewal stuck
            // retrying an unreachable shard can't delay the fence.
            macro_rules! or_fence {
                ($fut:expr) => {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(
                            last_renew_started + timing.fence_timeout
                        ) => {
                            on_fenced("blob lease: no successful renewal within fence timeout");
                            return;
                        }
                        x = $fut => x,
                    }
                };
            }

            or_fence!(tokio::time::sleep_until(next_tick));
            next_tick += timing.renew_interval;

            let attempt_started = Instant::now();
            match or_fence!(shard.renew(lease)) {
                Ok(_) => last_renew_started = attempt_started,
                Err(err) => {
                    on_fenced(&format!("blob lease {lease} no longer live: {err:?}"));
                    return;
                }
            }

            let observed = or_fence!(shard.observe());
            let now = Instant::now();
            let mut still_live = BTreeMap::new();
            for (other, row) in observed.rows() {
                if *other == lease || row.state != LeaseState::Live {
                    continue;
                }
                let first_seen = match seen.get(other) {
                    Some((renewal, at)) if *renewal == row.renewal => *at,
                    _ => now,
                };
                still_live.insert(*other, (row.renewal, first_seen));
                if now.duration_since(first_seen) >= row.fence_timeout + timing.expiry_slack {
                    match or_fence!(shard.expire(*other, row.renewal)) {
                        Ok(()) => {
                            info!("blob lease {other} expired");
                            still_live.remove(other);
                        }
                        Err(ExpireError::Renewed(r)) => {
                            still_live.insert(*other, (r, Instant::now()));
                        }
                        Err(err) => {
                            warn!("blob lease {other}: expire: {err:?}");
                            still_live.remove(other);
                        }
                    }
                }
            }
            seen = still_live;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
    use mz_persist::unreliable::{UnreliableBlob, UnreliableConsensus, UnreliableHandle};

    use super::*;
    use crate::cache::StateCache;
    use crate::rpc::NoopPubSubSender;
    use crate::{IsolatedRuntime, Metrics, PersistConfig};

    fn row(renewal: u64, state: LeaseState) -> LeaseRow {
        LeaseRow {
            debug: LeaseDebug {
                purpose: "p".into(),
                build_version: "v".into(),
                holder_wall_ms: 7,
            },
            fence_timeout: Duration::from_secs(1800),
            renewal,
            state,
        }
    }

    #[mz_ore::test]
    fn lease_row_roundtrip() {
        for r in [
            row(0, LeaseState::Live),
            row(9, LeaseState::Expired),
            row(3, LeaseState::Unknown(17)),
        ] {
            assert_eq!(LeaseRow::decode(&r.encode()), r);
        }
    }

    /// A persist client over blob and consensus that `handle` can make unreachable.
    fn unreliable_client(handle: &UnreliableHandle) -> PersistClient {
        let cfg = PersistConfig::new_for_tests();
        let blob = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let consensus = Arc::new(MemConsensus::default());
        let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
        PersistClient::new(
            cfg,
            Arc::new(UnreliableBlob::new(blob, handle.clone())),
            Arc::new(UnreliableConsensus::new(consensus, handle.clone())),
            metrics,
            Arc::new(IsolatedRuntime::new_for_tests()),
            Arc::new(StateCache::new_no_metrics()),
            Arc::new(NoopPubSubSender),
        )
        .expect("client construction failed")
    }

    fn reliable() -> UnreliableHandle {
        let h = UnreliableHandle::default();
        h.totally_available();
        h
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn lease_shard_ops() {
        let client = unreliable_client(&reliable());
        let shard_id = ShardId::new();
        let mut a = LeaseShard::open(&client, shard_id).await;
        let mut b = LeaseShard::open(&client, shard_id).await;
        let lease = BlobLeaseId::new();
        let ft = Duration::from_secs(1800);

        assert_eq!(a.create(lease, ft, "test").await, Ok(()));
        assert_eq!(b.create(lease, ft, "test").await, Err(Exists));
        assert_eq!(a.renew(lease).await, Ok(1));
        // A stale expected renewal loses.
        assert_eq!(b.expire(lease, 0).await, Err(ExpireError::Renewed(1)));
        assert_eq!(b.expire(lease, 1).await, Ok(()));
        assert_eq!(a.renew(lease).await, Err(RenewError::Expired));
        assert_eq!(b.expire(lease, 1).await, Err(ExpireError::AlreadyExpired));
        assert_eq!(a.renew(BlobLeaseId::new()).await, Err(RenewError::Missing));
        let snap = a.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[&lease].state, LeaseState::Expired);
        assert_eq!(snap[&lease].fence_timeout, ft);
    }

    /// Both sides observe the same upper before either writes, in both orders.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn lease_renew_expire_race() {
        for renew_first in [true, false] {
            let client = unreliable_client(&reliable());
            let shard_id = ShardId::new();
            let mut holder = LeaseShard::open(&client, shard_id).await;
            let mut observer = LeaseShard::open(&client, shard_id).await;
            let lease = BlobLeaseId::new();
            holder
                .create(lease, Duration::from_secs(1), "test")
                .await
                .unwrap();

            let h_obs = holder.observe().await;
            let o_obs = observer.observe().await;
            assert_eq!(h_obs.upper, o_obs.upper);
            let renewed = LeaseShard::plan_renew(&h_obs, lease).unwrap();
            let expired = LeaseShard::plan_expire(&o_obs, lease, 0).unwrap();

            let (first, second) = if renew_first {
                let r = holder.try_apply(&h_obs, lease, &renewed).await;
                let e = observer.try_apply(&o_obs, lease, &expired).await;
                (r, e)
            } else {
                let e = observer.try_apply(&o_obs, lease, &expired).await;
                let r = holder.try_apply(&h_obs, lease, &renewed).await;
                (e, r)
            };
            assert!(first.is_ok());
            assert!(second.is_err(), "both a renewal and an expiry landed");

            // The loser's retry sees the winner's write.
            let state = holder.snapshot().await[&lease].clone();
            if renew_first {
                assert_eq!(state.state, LeaseState::Live);
                assert_eq!(
                    observer.expire(lease, 0).await,
                    Err(ExpireError::Renewed(1))
                );
            } else {
                assert_eq!(state.state, LeaseState::Expired);
                assert_eq!(holder.renew(lease).await, Err(RenewError::Expired));
            }
        }
    }

    fn fence_recorder() -> (OnFenced, Arc<Mutex<Vec<(Instant, String)>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = Arc::clone(&log);
        let f: OnFenced =
            Arc::new(move |msg: &str| log2.lock().unwrap().push((Instant::now(), msg.to_owned())));
        (f, log)
    }

    // NOTE: these tests run in real time, scaled down. Under `tokio::time::pause()` a renewal
    // that awaits persist lets auto-advance jump the clock straight to the fence deadline
    // (observed: 50s of virtual time in 0.55ms of real time), so every holder self-fences.
    const TIMING: LeaseTiming = LeaseTiming {
        renew_interval: Duration::from_millis(150),
        fence_timeout: Duration::from_millis(1000),
        expiry_slack: Duration::from_millis(300),
    };

    // Test 1: renews on schedule.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn holder_renews() {
        let client = unreliable_client(&reliable());
        let shard_id = ShardId::new();
        let (fenced, log) = fence_recorder();
        let (lease, _task) = BlobLeaseHolder::start(
            LeaseShard::open(&client, shard_id).await,
            TIMING,
            "t",
            fenced,
        )
        .await;
        let mut reader = LeaseShard::open(&client, shard_id).await;
        tokio::time::sleep(TIMING.renew_interval * 3 + TIMING.renew_interval / 2).await;
        let row = reader.snapshot().await[&lease].clone();
        assert_eq!(row.renewal, 3);
        assert!(log.lock().unwrap().is_empty());
    }

    // Test 2: the lease shard becomes unreachable. The holder fences at its fence timeout even
    // though its renewal is stuck retrying inside persist.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn holder_self_fences_while_renew_in_flight() {
        let handle = reliable();
        let client = unreliable_client(&handle);
        let (fenced, log) = fence_recorder();
        let start = Instant::now();
        let (_lease, task) = BlobLeaseHolder::start(
            LeaseShard::open(&client, ShardId::new()).await,
            TIMING,
            "t",
            fenced,
        )
        .await;
        handle.totally_unavailable();
        task.await;
        let log = log.lock().unwrap();
        assert_eq!(log.len(), 1, "{log:?}");
        let (at, msg) = &log[0];
        assert!(msg.contains("fence timeout"), "{msg}");
        let elapsed = at.duration_since(start);
        assert!(
            elapsed >= TIMING.fence_timeout
                && elapsed <= TIMING.fence_timeout + Duration::from_millis(200),
            "fenced after {elapsed:?}, timeout {:?}",
            TIMING.fence_timeout
        );
    }

    // Test 3: a stopped holder is expired at fence timeout plus slack, and not before.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn observer_expires_stopped_holder() {
        let client = unreliable_client(&reliable());
        let shard_id = ShardId::new();
        let (fenced_a, log_a) = fence_recorder();
        let (fenced_b, log_b) = fence_recorder();
        let (lease_a, task_a) = BlobLeaseHolder::start(
            LeaseShard::open(&client, shard_id).await,
            TIMING,
            "a",
            fenced_a,
        )
        .await;
        let (_lease_b, _task_b) = BlobLeaseHolder::start(
            LeaseShard::open(&client, shard_id).await,
            TIMING,
            "b",
            fenced_b,
        )
        .await;
        let mut reader = LeaseShard::open(&client, shard_id).await;

        // Let A renew once, then stop it.
        tokio::time::sleep(TIMING.renew_interval + TIMING.renew_interval / 2).await;
        let a_last = reader.snapshot().await[&lease_a].renewal;
        assert_eq!(a_last, 1);
        drop(task_a.abort_on_drop());
        let stopped = Instant::now();

        // B samples once per renew interval, so it first sees A's final renewal up to one
        // interval late, and expires on the first tick at least fence + slack after that.
        let lower = TIMING.fence_timeout + TIMING.expiry_slack - TIMING.renew_interval;
        tokio::time::sleep(lower - Duration::from_millis(50)).await;
        assert_eq!(reader.snapshot().await[&lease_a].state, LeaseState::Live);
        let mut expired_at = None;
        for _ in 0..1000 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if reader.snapshot().await[&lease_a].state == LeaseState::Expired {
                expired_at = Some(Instant::now());
                break;
            }
        }
        let expired_after = expired_at.expect("A expired").duration_since(stopped);
        eprintln!("A stopped, expired {expired_after:?} later");
        assert!(expired_after >= lower);
        assert!(
            expired_after
                <= TIMING.fence_timeout
                    + TIMING.expiry_slack
                    + TIMING.renew_interval
                    + Duration::from_millis(100)
        );
        assert!(log_a.lock().unwrap().is_empty());
        assert!(log_b.lock().unwrap().is_empty());
    }

    // Test 4: A is paused (not stopped) past fence + slack, B expires it, A resumes. A fences,
    // and its row never shows a renewal after the expiry.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn paused_holder_fences_on_resume() {
        let client = unreliable_client(&reliable());
        let shard_id = ShardId::new();
        let mut shard_a = LeaseShard::open(&client, shard_id).await;
        let lease_a = BlobLeaseId::new();
        let ft = TIMING.fence_timeout;
        shard_a.create(lease_a, ft, "a").await.unwrap();

        let (fenced_b, _log_b) = fence_recorder();
        let (_lease_b, _task_b) = BlobLeaseHolder::start(
            LeaseShard::open(&client, shard_id).await,
            TIMING,
            "b",
            fenced_b,
        )
        .await;

        // "Paused": A's task does nothing for longer than fence + slack + one tick.
        tokio::time::sleep(ft + TIMING.expiry_slack + TIMING.renew_interval * 2).await;
        let mut reader = LeaseShard::open(&client, shard_id).await;
        assert_eq!(reader.snapshot().await[&lease_a].state, LeaseState::Expired);

        // Resumed, A's own self-fence deadline (from its create) is long past, so the loop's
        // first await fences before any renewal.
        let (fenced_a, log_a) = fence_recorder();
        BlobLeaseHolder::run(shard_a, lease_a, TIMING, Instant::now() - ft * 2, fenced_a).await;
        assert_eq!(log_a.lock().unwrap().len(), 1);
        let row = reader.snapshot().await[&lease_a].clone();
        assert_eq!((row.state, row.renewal), (LeaseState::Expired, 0));

        // Even if its clock said it was in time (a pause the monotonic clock didn't see), the
        // renewal itself fails and fences.
        let (fenced_a, log_a) = fence_recorder();
        let shard_a = LeaseShard::open(&client, shard_id).await;
        BlobLeaseHolder::run(shard_a, lease_a, TIMING, Instant::now(), fenced_a).await;
        let msg = log_a.lock().unwrap()[0].1.clone();
        assert!(msg.contains("no longer live"), "{msg}");
        let row = reader.snapshot().await[&lease_a].clone();
        assert_eq!((row.state, row.renewal), (LeaseState::Expired, 0));
    }

    // Test 5: the fence timeout in the row wins over the observer's dyncfg.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn fence_timeout_pinned_in_row() {
        let client = unreliable_client(&reliable());
        let shard_id = ShardId::new();
        let mut shard_a = LeaseShard::open(&client, shard_id).await;
        let lease_a = BlobLeaseId::new();
        let pinned = Duration::from_millis(2000);
        shard_a.create(lease_a, pinned, "a").await.unwrap();
        let short = LeaseTiming {
            renew_interval: Duration::from_millis(50),
            fence_timeout: Duration::from_millis(300),
            expiry_slack: Duration::from_millis(100),
        };
        let (fenced_b, log_b) = fence_recorder();
        let (_lease_b, _task_b) = BlobLeaseHolder::start(
            LeaseShard::open(&client, shard_id).await,
            short,
            "b",
            fenced_b,
        )
        .await;
        let mut reader = LeaseShard::open(&client, shard_id).await;
        // B's own fence + slack is 400ms. A must still be live well past that.
        tokio::time::sleep(pinned - Duration::from_millis(100)).await;
        assert_eq!(reader.snapshot().await[&lease_a].state, LeaseState::Live);
        tokio::time::sleep(short.expiry_slack + short.renew_interval * 4).await;
        assert!(log_b.lock().unwrap().is_empty());
        assert_eq!(reader.snapshot().await[&lease_a].state, LeaseState::Expired);
    }
}
