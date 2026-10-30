// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coalesces concurrent [Consensus::compare_and_set] calls into batched statements.
//!
//! Every call joins the pending queue of the range its key maps to. A range runs at most
//! [BatchLimits::max_in_flight] statements at once. Batches are formed both when a call arrives
//! and when an in-flight statement returns, so under capacity a call goes out alone and at once,
//! and at capacity it waits for at most one statement to return before joining the next batch.
//! There is no timer.
//!
//! Liveness: a pending op waits only while its range is at capacity or while its key is in an
//! in-flight statement of the same range. Either way some statement of that range is in flight,
//! and its completion re-runs scheduling for the range. [BatchGuard] makes "completion" cover
//! every exit of a batch task, including errors and panics. Busy keys are tracked per range
//! because only that range's completions re-schedule its queue: a set shared across ranges could
//! park an op behind a key whose statement completes in a different range.
//!
//! The batcher never retries. Only the caller can recompute state after a mismatch.
//!
//! [Consensus::compare_and_set]: crate::location::Consensus::compare_and_set

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use mz_ore::cast::CastLossy;
use tokio::sync::oneshot;

use crate::location::{CaSResult, Determinate, ExternalError, Indeterminate, VersionedData};
use crate::metrics::ConsensusBatchMetrics;

/// A partition of the consensus table, named by the first shard key in it.
///
/// `None` is the partition holding the start of the table, which is the only partition when no
/// range boundaries are known.
pub(crate) type RangeId = Option<String>;

/// Limits on how a [CasBatcher] forms statements, read anew at every scheduling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchLimits {
    /// Statements in flight per range. 0 is treated as 1, since with nothing in flight nothing
    /// would ever schedule a queued call.
    pub max_in_flight: usize,
    /// Calls per statement. Values below 1 are treated as 1.
    pub max_rows: usize,
    /// Total data bytes per statement. A batch always takes its first call, however large.
    pub max_bytes: usize,
}

/// One compare-and-set call.
#[derive(Debug)]
pub(crate) struct CasOp {
    pub key: String,
    pub new: VersionedData,
}

/// The backend-specific half of a [CasBatcher].
#[async_trait]
pub(crate) trait CasBackend: Debug + Send + Sync + 'static {
    fn limits(&self) -> BatchLimits;

    fn range(&self, key: &str) -> RangeId;

    /// Runs `ops` as one atomic statement and returns the keys that committed.
    ///
    /// `ops` is sorted by key and holds each key at most once. A key that is not returned failed
    /// its expectation. A [ExternalError::Determinate] error must mean that nothing committed.
    async fn execute(&self, ops: &[CasOp]) -> Result<BTreeSet<String>, ExternalError>;
}

type CasResponse = oneshot::Sender<Result<CaSResult, ExternalError>>;

#[derive(Debug)]
struct PendingOp {
    id: u64,
    op: CasOp,
    tx: CasResponse,
}

#[derive(Debug, Default)]
struct RangeQueue {
    in_flight: usize,
    in_flight_keys: BTreeSet<String>,
    pending: VecDeque<PendingOp>,
}

impl RangeQueue {
    /// Pops the next batch off `pending`, sorted by key. Ops left behind keep their order.
    fn take_batch(&mut self, limits: &BatchLimits) -> Vec<PendingOp> {
        let max_rows = limits.max_rows.max(1);
        let mut batch: Vec<PendingOp> = Vec::new();
        let mut batch_keys = BTreeSet::new();
        let mut bytes = 0usize;
        let mut skipped = VecDeque::new();
        while let Some(p) = self.pending.pop_front() {
            // The caller is gone, so nobody can observe whether this op ran. Not running it is
            // the cheaper of the two outcomes that caller already had to tolerate.
            if p.tx.is_closed() {
                continue;
            }
            if batch_keys.contains(&p.op.key) || self.in_flight_keys.contains(&p.op.key) {
                skipped.push_back(p);
                continue;
            }
            let len = p.op.new.data.len();
            if batch.len() >= max_rows
                || (!batch.is_empty() && bytes.saturating_add(len) > limits.max_bytes)
            {
                self.pending.push_front(p);
                break;
            }
            bytes = bytes.saturating_add(len);
            batch_keys.insert(p.op.key.clone());
            batch.push(p);
        }
        skipped.append(&mut self.pending);
        self.pending = skipped;
        batch.sort_by(|a, b| a.op.key.cmp(&b.op.key));
        batch
    }
}

#[derive(Debug, Default)]
struct Scheduler {
    next_id: u64,
    ranges: BTreeMap<RangeId, RangeQueue>,
}

/// A batch handed out by [Scheduler::try_schedule], which already counts it as in flight.
#[derive(Debug)]
struct Batch {
    range: RangeId,
    ops: Vec<PendingOp>,
}

impl Scheduler {
    fn try_schedule(&mut self, range: &RangeId, limits: &BatchLimits) -> Vec<Batch> {
        let Some(queue) = self.ranges.get_mut(range) else {
            return Vec::new();
        };
        let mut batches = Vec::new();
        while queue.in_flight < limits.max_in_flight.max(1) {
            let ops = queue.take_batch(limits);
            if ops.is_empty() {
                break;
            }
            queue.in_flight += 1;
            queue
                .in_flight_keys
                .extend(ops.iter().map(|p| p.op.key.clone()));
            batches.push(Batch {
                range: range.clone(),
                ops,
            });
        }
        if queue.in_flight == 0 && queue.pending.is_empty() {
            self.ranges.remove(range);
        }
        batches
    }

    fn complete(&mut self, range: &RangeId, keys: &[String]) {
        let queue = self
            .ranges
            .get_mut(range)
            .expect("a range with a statement in flight has a queue");
        queue.in_flight -= 1;
        for key in keys {
            queue.in_flight_keys.remove(key);
        }
    }
}

#[derive(Debug)]
struct Shared<B> {
    backend: B,
    scheduler: Mutex<Scheduler>,
    metrics: ConsensusBatchMetrics,
}

/// Coalesces concurrent compare-and-set calls into statements run by a [CasBackend].
#[derive(Debug)]
pub(crate) struct CasBatcher<B> {
    shared: Arc<Shared<B>>,
}

impl<B: CasBackend> CasBatcher<B> {
    pub fn new(backend: B, metrics: ConsensusBatchMetrics) -> Self {
        CasBatcher {
            shared: Arc::new(Shared {
                backend,
                scheduler: Mutex::new(Scheduler::default()),
                metrics,
            }),
        }
    }

    #[cfg(test)]
    pub fn backend(&self) -> &B {
        &self.shared.backend
    }

    /// Compare-and-sets `new` at `key`, whose expected seqno is `new.seqno.previous()`.
    pub async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let range = self.shared.backend.range(key);
        let limits = self.shared.backend.limits();
        let (tx, rx) = oneshot::channel();
        let batches = {
            let mut scheduler = self.shared.scheduler.lock().expect("lock poisoned");
            let id = scheduler.next_id;
            scheduler.next_id += 1;
            scheduler
                .ranges
                .entry(range.clone())
                .or_default()
                .pending
                .push_back(PendingOp {
                    id,
                    op: CasOp {
                        key: key.to_owned(),
                        new,
                    },
                    tx,
                });
            let batches = scheduler.try_schedule(&range, &limits);
            if !batches.iter().flat_map(|b| &b.ops).any(|p| p.id == id) {
                self.shared.metrics.calls_coalesced.inc();
            }
            batches
        };
        spawn_batches(&self.shared, batches);
        rx.await.unwrap_or_else(|_| {
            Err(ExternalError::from(anyhow!(
                "consensus batch dropped a compare-and-set without a result"
            )))
        })
    }
}

fn spawn_batches<B: CasBackend>(shared: &Arc<Shared<B>>, batches: Vec<Batch>) {
    for batch in batches {
        shared
            .metrics
            .batch_size
            .observe(f64::cast_lossy(batch.ops.len()));
        let (ops, txs) = batch.ops.into_iter().map(|p| (p.op, p.tx)).unzip();
        let mut guard = BatchGuard {
            shared: Arc::clone(shared),
            range: batch.range,
            ops,
            txs,
            result: None,
        };
        mz_ore::task::spawn(|| "persist::consensus::cas_batch", async move {
            let result = guard.shared.backend.execute(&guard.ops).await;
            guard.result = Some(result);
            drop(guard);
        });
    }
}

/// Owns one in-flight statement. Dropping it, on every exit of the batch task, answers each
/// caller and frees the statement's slot and keys.
///
/// NOTE: this cleanup must not depend on the task reaching its end. A connection error or panic
/// that skipped it would leak the range's in-flight slot and pin the batch's keys, parking every
/// later call for those keys forever.
struct BatchGuard<B: CasBackend> {
    shared: Arc<Shared<B>>,
    range: RangeId,
    ops: Vec<CasOp>,
    txs: Vec<CasResponse>,
    result: Option<Result<BTreeSet<String>, ExternalError>>,
}

impl<B: CasBackend> Drop for BatchGuard<B> {
    fn drop(&mut self) {
        let responses = match self.result.take() {
            Some(Ok(committed)) => self
                .ops
                .iter()
                .map(|op| {
                    Ok(if committed.contains(&op.key) {
                        CaSResult::Committed
                    } else {
                        CaSResult::ExpectationMismatch
                    })
                })
                .collect(),
            Some(Err(err)) => fan_out(err, self.ops.len()).into_iter().map(Err).collect(),
            None => fan_out(
                ExternalError::from(anyhow!(
                    "consensus batch task exited before its statement returned"
                )),
                self.ops.len(),
            )
            .into_iter()
            .map(Err)
            .collect::<Vec<_>>(),
        };
        let limits = self.shared.backend.limits();
        let keys: Vec<String> = self.ops.drain(..).map(|op| op.key).collect();
        let batches = {
            let mut scheduler = self.shared.scheduler.lock().expect("lock poisoned");
            scheduler.complete(&self.range, &keys);
            scheduler.try_schedule(&self.range, &limits)
        };
        // Answer only after freeing the slot and keys, so a caller that retries at once finds
        // them free.
        for (tx, response) in self.txs.drain(..).zip_eq(responses) {
            // The caller may have given up, which is fine.
            let _ = tx.send(response);
        }
        spawn_batches(&self.shared, batches);
    }
}

/// Copies `err` for each of `n` callers of one statement, keeping its determinacy and message.
pub(crate) fn fan_out(err: ExternalError, n: usize) -> Vec<ExternalError> {
    let mut errs = Vec::with_capacity(n);
    if n == 0 {
        return errs;
    }
    for _ in 1..n {
        errs.push(match &err {
            ExternalError::Determinate(e) => Determinate::new(anyhow!("{:#}", e.inner)).into(),
            ExternalError::Indeterminate(e) => Indeterminate::new(anyhow!("{:#}", e.inner)).into(),
        });
    }
    errs.push(err);
    errs
}

/// Maps shard keys to the CockroachDB range their newest rows live in.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RangeMap {
    /// First shard key of every range except the one holding the table start, sorted and
    /// distinct.
    starts: Vec<String>,
}

impl RangeMap {
    /// Builds a map from the `start_key` column of `SHOW RANGES FROM TABLE consensus`.
    ///
    /// Depending on the CockroachDB version, start keys print as `/"s0100"` or `…/1/"s0100"`,
    /// with a `/<seqno>` suffix when a range starts inside a shard, and the first range prints as
    /// NULL or without a quoted segment. The shard is the first double-quoted segment. A range
    /// starting inside a shard is mapped as if it started at that shard, which holds that shard's
    /// newest rows. CockroachDB's key encoding orders strings bytewise, as `str` does.
    pub fn from_start_keys<I, S>(start_keys: I) -> Result<Self, anyhow::Error>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let mut starts = BTreeSet::new();
        for start_key in start_keys.into_iter().flatten() {
            let start_key = start_key.as_ref();
            let Some((_, rest)) = start_key.split_once('"') else {
                continue;
            };
            let Some((shard, _)) = rest.split_once('"') else {
                return Err(anyhow!(
                    "unterminated shard in range start key {:?}",
                    start_key
                ));
            };
            // A backslash means the key printer escaped something, and we would have to undo
            // that to recover the shard's bytes.
            if shard.contains('\\') {
                return Err(anyhow!("escaped shard in range start key {:?}", start_key));
            }
            starts.insert(shard.to_owned());
        }
        Ok(RangeMap {
            starts: starts.into_iter().collect(),
        })
    }

    /// The number of ranges.
    pub fn len(&self) -> usize {
        self.starts.len() + 1
    }

    pub fn range(&self, key: &str) -> RangeId {
        match self.starts.partition_point(|start| start.as_str() <= key) {
            0 => None,
            i => Some(self.starts[i - 1].clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use mz_ore::cast::CastFrom;
    use mz_ore::metrics::MetricsRegistry;
    use tokio::sync::Semaphore;

    use crate::location::SeqNo;

    use super::*;

    #[mz_ore::test]
    fn range_map_lookup() {
        let empty = RangeMap::from_start_keys(Vec::<Option<&str>>::new()).unwrap();
        assert_eq!(empty.len(), 1);
        assert_eq!(empty.range("s1"), None);

        // Both `SHOW RANGES` output formats, with a mid-shard start and a duplicate shard.
        let map = RangeMap::from_start_keys([
            None,
            Some("…/<TableMin>"),
            Some("…/1/\"s0100\""),
            Some("/\"s0200\""),
            Some("/\"s0200\"/5"),
            Some("…/1/\"s0250\"/5"),
        ])
        .unwrap();
        assert_eq!(map.len(), 4);
        assert_eq!(map.range(""), None);
        assert_eq!(map.range("s0099"), None);
        assert_eq!(map.range("s0100"), Some("s0100".to_owned()));
        assert_eq!(map.range("s01000"), Some("s0100".to_owned()));
        assert_eq!(map.range("s0199"), Some("s0100".to_owned()));
        assert_eq!(map.range("s0200"), Some("s0200".to_owned()));
        assert_eq!(map.range("s0250"), Some("s0250".to_owned()));
        assert_eq!(map.range("s9999"), Some("s0250".to_owned()));

        assert!(RangeMap::from_start_keys([Some("/\"s0100")]).is_err());
        assert!(RangeMap::from_start_keys([Some("/\"s\\x00\"")]).is_err());
    }

    #[mz_ore::test]
    fn fan_out_keeps_determinacy() {
        let det = fan_out(Determinate::new(anyhow!("40001")).into(), 3);
        assert_eq!(det.len(), 3);
        for e in &det {
            assert!(matches!(e, ExternalError::Determinate(_)), "{e:?}");
            assert_eq!(e.to_string(), "determinate: 40001");
        }
        let indet = fan_out(anyhow!("timeout").into(), 2);
        assert_eq!(indet.len(), 2);
        for e in &indet {
            assert!(matches!(e, ExternalError::Indeterminate(_)), "{e:?}");
            assert!(e.is_timeout());
        }
        assert!(fan_out(anyhow!("x").into(), 0).is_empty());
    }

    #[mz_ore::test]
    fn take_batch_limits() {
        fn op(id: u64, key: &str, len: usize) -> PendingOp {
            let (tx, rx) = oneshot::channel();
            std::mem::forget(rx);
            PendingOp {
                id,
                op: CasOp {
                    key: key.to_owned(),
                    new: VersionedData {
                        seqno: SeqNo(1),
                        data: Bytes::from(vec![0; len]),
                    },
                },
                tx,
            }
        }
        let ids = |ops: &[PendingOp]| ops.iter().map(|p| p.id).collect::<Vec<_>>();
        let limits = BatchLimits {
            max_in_flight: 1,
            max_rows: 3,
            max_bytes: 10,
        };

        // Sorted by key, duplicate keys and busy keys stay queued in order, row cap.
        let mut q = RangeQueue::default();
        q.in_flight_keys.insert("busy".into());
        for (id, key) in [
            (0, "c"),
            (1, "busy"),
            (2, "a"),
            (3, "c"),
            (4, "b"),
            (5, "d"),
        ] {
            q.pending.push_back(op(id, key, 1));
        }
        assert_eq!(ids(&q.take_batch(&limits)), vec![2, 4, 0]);
        assert_eq!(ids(q.pending.make_contiguous()), vec![1, 3, 5]);

        // Byte cap, but an oversized first op still goes alone.
        let mut q = RangeQueue::default();
        for (id, len) in [(0, 20), (1, 4), (2, 4), (3, 4)] {
            q.pending.push_back(op(id, &format!("k{id}"), len));
        }
        assert_eq!(ids(&q.take_batch(&limits)), vec![0]);
        assert_eq!(ids(&q.take_batch(&limits)), vec![1, 2]);
        assert_eq!(ids(&q.take_batch(&limits)), vec![3]);
        assert!(q.take_batch(&limits).is_empty());

        // Abandoned calls are dropped.
        let mut q = RangeQueue::default();
        let (tx, rx) = oneshot::channel();
        drop(rx);
        q.pending.push_back(PendingOp {
            tx,
            ..op(0, "a", 1)
        });
        q.pending.push_back(op(1, "b", 1));
        assert_eq!(ids(&q.take_batch(&limits)), vec![1]);
        assert!(q.pending.is_empty());
    }

    /// How a [FakeBackend] statement ends.
    #[derive(Debug, Clone, Copy)]
    enum Outcome {
        /// Commits every op whose expected seqno is the key's current seqno.
        Apply,
        Determinate,
        Indeterminate,
        Panic,
    }

    /// An in-memory backend that blocks every statement on a semaphore, so a test controls when
    /// statements return.
    #[derive(Debug)]
    struct FakeBackend {
        limits: Mutex<BatchLimits>,
        ranges: RangeMap,
        heads: Mutex<BTreeMap<String, SeqNo>>,
        outcome: Mutex<Outcome>,
        gate: Semaphore,
        started: AtomicUsize,
        statements: Mutex<Vec<Vec<String>>>,
    }

    impl FakeBackend {
        fn new(limits: BatchLimits, ranges: RangeMap) -> Self {
            FakeBackend {
                limits: Mutex::new(limits),
                ranges,
                heads: Mutex::new(BTreeMap::new()),
                outcome: Mutex::new(Outcome::Apply),
                gate: Semaphore::new(0),
                started: AtomicUsize::new(0),
                statements: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl CasBackend for FakeBackend {
        fn limits(&self) -> BatchLimits {
            *self.limits.lock().unwrap()
        }

        fn range(&self, key: &str) -> RangeId {
            self.ranges.range(key)
        }

        async fn execute(&self, ops: &[CasOp]) -> Result<BTreeSet<String>, ExternalError> {
            let keys: Vec<String> = ops.iter().map(|op| op.key.clone()).collect();
            let mut sorted = keys.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(keys, sorted, "ops must be sorted with distinct keys");
            self.statements.lock().unwrap().push(keys);
            self.started.fetch_add(1, Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            let outcome = *self.outcome.lock().unwrap();
            match outcome {
                Outcome::Apply => {}
                Outcome::Determinate => return Err(Determinate::new(anyhow!("40001")).into()),
                Outcome::Indeterminate => return Err(anyhow!("connection reset").into()),
                Outcome::Panic => panic!("injected batch panic"),
            }
            let mut heads = self.heads.lock().unwrap();
            let mut committed = BTreeSet::new();
            for op in ops {
                let head = heads.get(&op.key).copied().unwrap_or(SeqNo(0));
                if op.new.seqno.previous() == Some(head) {
                    heads.insert(op.key.clone(), op.new.seqno);
                    committed.insert(op.key.clone());
                }
            }
            Ok(committed)
        }
    }

    fn crdb_limits(k: usize) -> BatchLimits {
        BatchLimits {
            max_in_flight: k,
            max_rows: 128,
            max_bytes: 1 << 20,
        }
    }

    fn data(seqno: u64) -> VersionedData {
        VersionedData {
            seqno: SeqNo(seqno),
            data: Bytes::from_static(b"x"),
        }
    }

    async fn wait_started(backend: &FakeBackend, n: usize) {
        while backend.started.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn assert_idle<B>(batcher: &CasBatcher<B>) {
        let scheduler = batcher.shared.scheduler.lock().unwrap();
        assert!(
            scheduler.ranges.is_empty(),
            "scheduler should be idle: {:?}",
            scheduler.ranges
        );
    }

    fn spawn_cas(
        batcher: &Arc<CasBatcher<FakeBackend>>,
        key: &str,
        seqno: u64,
    ) -> mz_ore::task::JoinHandle<Result<CaSResult, ExternalError>> {
        let batcher = Arc::clone(batcher);
        let key = key.to_owned();
        mz_ore::task::spawn(|| "cas", async move {
            batcher.compare_and_set(&key, data(seqno)).await
        })
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn coalesces_at_capacity() {
        let registry = MetricsRegistry::new();
        let metrics = ConsensusBatchMetrics::new(&registry);
        let batcher = Arc::new(CasBatcher::new(
            FakeBackend::new(crdb_limits(2), RangeMap::default()),
            metrics.clone(),
        ));
        let backend = batcher.backend();

        let first = spawn_cas(&batcher, "s0", 1);
        wait_started(backend, 1).await;
        // A second call for an in-flight key waits even though a slot is free.
        let dup = spawn_cas(&batcher, "s0", 2);
        while metrics.calls_coalesced.get() < 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let other = spawn_cas(&batcher, "s1", 1);
        wait_started(backend, 2).await;
        // The range is at capacity, so these queue.
        let queued: Vec<_> = ["s4", "s2", "s3"]
            .into_iter()
            .map(|k| spawn_cas(&batcher, k, 1))
            .collect();
        while metrics.calls_coalesced.get() < 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(backend.started.load(Ordering::SeqCst), 2);

        // The first completion sweeps every queued call with an idle key into one statement.
        backend.gate.add_permits(10);
        for handle in [first, dup, other].into_iter().chain(queued) {
            assert_eq!(handle.await, Ok(CaSResult::Committed));
        }
        let statements = backend.statements.lock().unwrap().clone();
        assert!(
            statements.iter().any(|s| ["s2", "s3", "s4"]
                .iter()
                .all(|k| s.contains(&k.to_string()))),
            "{statements:?}"
        );
        assert!(statements.len() <= 4, "{statements:?}");
        assert_eq!(
            metrics.batch_size.get_sample_count(),
            u64::cast_from(statements.len())
        );
        assert_eq!(metrics.batch_size.get_sample_sum(), 6.0);
        assert_idle(&batcher);
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn ranges() {
        // Two ranges with K = 1 each run one statement apiece at once.
        let ranges = RangeMap::from_start_keys([Some("/\"s5\"")]).unwrap();
        let batcher = Arc::new(CasBatcher::new(
            FakeBackend::new(crdb_limits(1), ranges),
            ConsensusBatchMetrics::new(&MetricsRegistry::new()),
        ));
        let a = spawn_cas(&batcher, "s1", 1);
        let b = spawn_cas(&batcher, "s7", 1);
        wait_started(batcher.backend(), 2).await;
        batcher.backend().gate.add_permits(2);
        assert_eq!(a.await, Ok(CaSResult::Committed));
        assert_eq!(b.await, Ok(CaSResult::Committed));
        assert_idle(&batcher);
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn errors_fan_out_and_free_the_range() {
        for (outcome, determinate) in [
            (Outcome::Determinate, Some(true)),
            (Outcome::Indeterminate, Some(false)),
            (Outcome::Panic, Some(false)),
        ] {
            let batcher = Arc::new(CasBatcher::new(
                FakeBackend::new(crdb_limits(1), RangeMap::default()),
                ConsensusBatchMetrics::new(&MetricsRegistry::new()),
            ));
            let backend = batcher.backend();
            *backend.outcome.lock().unwrap() = outcome;
            let first = spawn_cas(&batcher, "s0", 1);
            wait_started(backend, 1).await;
            let queued: Vec<_> = ["s1", "s2"]
                .into_iter()
                .map(|k| spawn_cas(&batcher, k, 1))
                .collect();
            while batcher.shared.metrics.calls_coalesced.get() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // The first statement fails. Its caller sees the error, and the queued calls form
            // the next statement, which fails the same way.
            backend.gate.add_permits(2);
            let mut errors = vec![first.await];
            for handle in queued {
                errors.push(handle.await);
            }
            for err in errors {
                let err = err.expect_err("statement failed");
                assert_eq!(
                    matches!(err, ExternalError::Determinate(_)),
                    determinate.unwrap(),
                    "{outcome:?}: {err}"
                );
            }
            assert_eq!(
                *backend.statements.lock().unwrap().last().unwrap(),
                vec!["s1".to_owned(), "s2".to_owned()]
            );
            assert_idle(&batcher);

            // The range and its keys are free again.
            *backend.outcome.lock().unwrap() = Outcome::Apply;
            backend.gate.add_permits(1);
            assert_eq!(
                batcher.compare_and_set("s0", data(1)).await,
                Ok(CaSResult::Committed)
            );
            assert_idle(&batcher);
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn lowering_the_cap_drains_through_completions() {
        let batcher = Arc::new(CasBatcher::new(
            FakeBackend::new(crdb_limits(3), RangeMap::default()),
            ConsensusBatchMetrics::new(&MetricsRegistry::new()),
        ));
        let backend = batcher.backend();
        let mut handles: Vec<_> = ["s0", "s1", "s2"]
            .into_iter()
            .map(|k| spawn_cas(&batcher, k, 1))
            .collect();
        wait_started(backend, 3).await;
        *backend.limits.lock().unwrap() = crdb_limits(0);
        handles.extend(["s3", "s4"].into_iter().map(|k| spawn_cas(&batcher, k, 1)));
        while batcher.shared.metrics.calls_coalesced.get() < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Three statements over the new cap (0, treated as 1) are in flight. Their completions
        // run nothing new until only the last one is left, which then sweeps both queued calls.
        backend.gate.add_permits(4);
        for handle in handles {
            assert_eq!(handle.await, Ok(CaSResult::Committed));
        }
        assert_eq!(backend.statements.lock().unwrap().len(), 4);
        assert_idle(&batcher);
    }
}
