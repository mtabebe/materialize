// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coalesces concurrent single-key consensus calls into batched statements.
//!
//! A [Batcher] serves one kind of call, such as [Consensus::compare_and_set] or [Consensus::scan],
//! through a [BatchBackend] that runs many of them as one statement. Every call joins the pending queue of the range its key maps to. A range runs at most
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
//! [Consensus::scan]: crate::location::Consensus::scan

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use mz_ore::cast::CastLossy;
use tokio::sync::oneshot;

use crate::location::{Determinate, ExternalError, Indeterminate};
use crate::metrics::BatcherMetrics;

/// A partition of the consensus table, named by the first shard key in it.
///
/// `None` is the partition holding the start of the table, which is the only partition when no
/// range boundaries are known.
pub(crate) type RangeId = Option<String>;

/// Limits on how a [Batcher] forms statements, read anew at every scheduling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchLimits {
    /// Statements in flight per range, `None` for no limit. `Some(0)` is treated as `Some(1)`,
    /// since with nothing in flight nothing would ever schedule a queued call.
    pub max_in_flight: Option<usize>,
    /// Calls per statement. Values below 1 are treated as 1.
    pub max_rows: usize,
    /// Total data bytes per statement. A batch always takes its first call, however large.
    pub max_bytes: usize,
    /// Whether a key waits while it is in an in-flight statement of its range. A key appears at
    /// most once per statement regardless.
    pub exclusive_keys: bool,
}

/// One call to a [Batcher].
pub(crate) trait BatchOp: Debug + Send + Sync + 'static {
    type Output: Debug + Send + 'static;

    fn key(&self) -> &str;

    /// The bytes this op counts against [BatchLimits::max_bytes].
    fn bytes(&self) -> usize;
}

/// The backend-specific half of a [Batcher].
#[async_trait]
pub(crate) trait BatchBackend: Debug + Send + Sync + 'static {
    type Op: BatchOp;

    fn limits(&self) -> BatchLimits;

    fn range(&self, key: &str) -> RangeId;

    /// Runs `ops` as one atomic statement and returns one output per op, in order.
    ///
    /// `ops` is sorted by key and holds each key at most once. A [ExternalError::Determinate]
    /// error must mean that no op took effect.
    async fn execute(&self, ops: &[Self::Op]) -> Result<Vec<Output<Self>>, ExternalError>;
}

type Output<B> = <<B as BatchBackend>::Op as BatchOp>::Output;

type Response<O> = oneshot::Sender<Result<<O as BatchOp>::Output, ExternalError>>;

#[derive(Debug)]
struct PendingOp<O: BatchOp> {
    id: u64,
    op: O,
    enqueued: Instant,
    tx: Response<O>,
}

#[derive(Debug)]
struct RangeQueue<O: BatchOp> {
    in_flight: usize,
    in_flight_keys: BTreeSet<String>,
    pending: VecDeque<PendingOp<O>>,
}

impl<O: BatchOp> Default for RangeQueue<O> {
    fn default() -> Self {
        RangeQueue {
            in_flight: 0,
            in_flight_keys: BTreeSet::new(),
            pending: VecDeque::new(),
        }
    }
}

impl<O: BatchOp> RangeQueue<O> {
    /// Pops the next batch off `pending`, sorted by key. Ops left behind keep their order.
    fn take_batch(&mut self, limits: &BatchLimits) -> Vec<PendingOp<O>> {
        let max_rows = limits.max_rows.max(1);
        let mut batch: Vec<PendingOp<O>> = Vec::new();
        let mut batch_keys = BTreeSet::new();
        let mut bytes = 0usize;
        let mut skipped = VecDeque::new();
        while let Some(p) = self.pending.pop_front() {
            // The caller is gone, so nobody can observe whether this op ran. Not running it is
            // the cheaper of the two outcomes that caller already had to tolerate.
            if p.tx.is_closed() {
                continue;
            }
            if batch_keys.contains(p.op.key())
                || (limits.exclusive_keys && self.in_flight_keys.contains(p.op.key()))
            {
                skipped.push_back(p);
                continue;
            }
            let len = p.op.bytes();
            if batch.len() >= max_rows
                || (!batch.is_empty() && bytes.saturating_add(len) > limits.max_bytes)
            {
                self.pending.push_front(p);
                break;
            }
            bytes = bytes.saturating_add(len);
            batch_keys.insert(p.op.key().to_owned());
            batch.push(p);
        }
        skipped.append(&mut self.pending);
        self.pending = skipped;
        batch.sort_by(|a, b| a.op.key().cmp(b.op.key()));
        batch
    }
}

#[derive(Debug)]
struct Scheduler<O: BatchOp> {
    next_id: u64,
    ranges: BTreeMap<RangeId, RangeQueue<O>>,
}

/// A batch handed out by [Scheduler::try_schedule], which already counts it as in flight.
#[derive(Debug)]
struct Batch<O: BatchOp> {
    range: RangeId,
    registered_keys: bool,
    ops: Vec<PendingOp<O>>,
}

impl<O: BatchOp> Scheduler<O> {
    fn try_schedule(&mut self, range: &RangeId, limits: &BatchLimits) -> Vec<Batch<O>> {
        let Some(queue) = self.ranges.get_mut(range) else {
            return Vec::new();
        };
        let mut batches = Vec::new();
        while limits
            .max_in_flight
            .map_or(true, |k| queue.in_flight < k.max(1))
        {
            let ops = queue.take_batch(limits);
            if ops.is_empty() {
                break;
            }
            queue.in_flight += 1;
            if limits.exclusive_keys {
                queue
                    .in_flight_keys
                    .extend(ops.iter().map(|p| p.op.key().to_owned()));
            }
            batches.push(Batch {
                range: range.clone(),
                registered_keys: limits.exclusive_keys,
                ops,
            });
        }
        if queue.in_flight == 0 && queue.pending.is_empty() {
            self.ranges.remove(range);
        }
        batches
    }

    fn complete(&mut self, range: &RangeId, keys: Option<&[String]>) {
        let queue = self
            .ranges
            .get_mut(range)
            .expect("a range with a statement in flight has a queue");
        queue.in_flight -= 1;
        for key in keys.into_iter().flatten() {
            queue.in_flight_keys.remove(key);
        }
    }
}

#[derive(Debug)]
struct Shared<B: BatchBackend> {
    backend: B,
    scheduler: Mutex<Scheduler<B::Op>>,
    metrics: BatcherMetrics,
}

/// Coalesces concurrent calls into statements run by a [BatchBackend].
#[derive(Debug)]
pub(crate) struct Batcher<B: BatchBackend> {
    shared: Arc<Shared<B>>,
}

impl<B: BatchBackend> Batcher<B> {
    pub fn new(backend: B, metrics: BatcherMetrics) -> Self {
        Batcher {
            shared: Arc::new(Shared {
                backend,
                scheduler: Mutex::new(Scheduler {
                    next_id: 0,
                    ranges: BTreeMap::new(),
                }),
                metrics,
            }),
        }
    }

    #[cfg(test)]
    pub fn backend(&self) -> &B {
        &self.shared.backend
    }

    /// Runs `op` in the next statement its range can take.
    pub async fn run(&self, op: B::Op) -> Result<Output<B>, ExternalError> {
        let range = self.shared.backend.range(op.key());
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
                    op,
                    enqueued: Instant::now(),
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
                "consensus batch dropped a call without a result"
            )))
        })
    }
}

fn spawn_batches<B: BatchBackend>(shared: &Arc<Shared<B>>, batches: Vec<Batch<B::Op>>) {
    for batch in batches {
        shared.metrics.statements.inc();
        shared.metrics.in_flight.inc();
        shared
            .metrics
            .batch_size
            .observe(f64::cast_lossy(batch.ops.len()));
        let now = Instant::now();
        for p in &batch.ops {
            shared
                .metrics
                .queue_wait_seconds
                .observe(now.duration_since(p.enqueued).as_secs_f64());
        }
        let (ops, txs) = batch.ops.into_iter().map(|p| (p.op, p.tx)).unzip();
        let mut guard = BatchGuard {
            shared: Arc::clone(shared),
            range: batch.range,
            registered_keys: batch.registered_keys,
            ops,
            txs,
            result: None,
        };
        mz_ore::task::spawn(|| "persist::consensus::batch", async move {
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
struct BatchGuard<B: BatchBackend> {
    shared: Arc<Shared<B>>,
    range: RangeId,
    registered_keys: bool,
    ops: Vec<B::Op>,
    txs: Vec<Response<B::Op>>,
    result: Option<Result<Vec<Output<B>>, ExternalError>>,
}

impl<B: BatchBackend> Drop for BatchGuard<B> {
    fn drop(&mut self) {
        let responses: Vec<_> = match self.result.take() {
            Some(Ok(outputs)) if outputs.len() == self.ops.len() => {
                outputs.into_iter().map(Ok).collect()
            }
            Some(Ok(outputs)) => fan_out(
                ExternalError::from(anyhow!(
                    "consensus batch returned {} results for {} calls",
                    outputs.len(),
                    self.ops.len()
                )),
                self.ops.len(),
            )
            .into_iter()
            .map(Err)
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
            .collect(),
        };
        let limits = self.shared.backend.limits();
        let keys: Vec<String> = if self.registered_keys {
            self.ops.drain(..).map(|op| op.key().to_owned()).collect()
        } else {
            Vec::new()
        };
        let batches = {
            let mut scheduler = self.shared.scheduler.lock().expect("lock poisoned");
            scheduler.complete(&self.range, self.registered_keys.then_some(&keys[..]));
            scheduler.try_schedule(&self.range, &limits)
        };
        self.shared.metrics.in_flight.dec();
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
fn fan_out(err: ExternalError, n: usize) -> Vec<ExternalError> {
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

    use crate::location::{CaSResult, SeqNo, VersionedData};
    use crate::metrics::ConsensusBatchMetrics;

    use super::*;

    #[derive(Debug)]
    struct CasOp {
        key: String,
        new: VersionedData,
    }

    impl BatchOp for CasOp {
        type Output = CaSResult;

        fn key(&self) -> &str {
            &self.key
        }

        fn bytes(&self) -> usize {
            self.new.data.len()
        }
    }

    fn metrics() -> BatcherMetrics {
        ConsensusBatchMetrics::new(&MetricsRegistry::new()).cas
    }

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
        fn op(id: u64, key: &str, len: usize) -> PendingOp<CasOp> {
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
                enqueued: Instant::now(),
                tx,
            }
        }
        let ids = |ops: &[PendingOp<CasOp>]| ops.iter().map(|p| p.id).collect::<Vec<_>>();
        let limits = BatchLimits {
            max_in_flight: Some(1),
            max_rows: 3,
            max_bytes: 10,
            exclusive_keys: true,
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

        // Not exclusive: busy keys go out, but still once per batch.
        let limits = BatchLimits {
            exclusive_keys: false,
            ..limits
        };
        let mut q = RangeQueue::default();
        q.in_flight_keys.insert("busy".into());
        for (id, key) in [(0, "busy"), (1, "busy")] {
            q.pending.push_back(op(id, key, 1));
        }
        assert_eq!(ids(&q.take_batch(&limits)), vec![0]);
        assert_eq!(ids(q.pending.make_contiguous()), vec![1]);

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
    impl BatchBackend for FakeBackend {
        type Op = CasOp;

        fn limits(&self) -> BatchLimits {
            *self.limits.lock().unwrap()
        }

        fn range(&self, key: &str) -> RangeId {
            self.ranges.range(key)
        }

        async fn execute(&self, ops: &[CasOp]) -> Result<Vec<CaSResult>, ExternalError> {
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
            let results = ops
                .iter()
                .map(|op| {
                    let head = heads.get(&op.key).copied().unwrap_or(SeqNo(0));
                    if op.new.seqno.previous() == Some(head) {
                        heads.insert(op.key.clone(), op.new.seqno);
                        CaSResult::Committed
                    } else {
                        CaSResult::ExpectationMismatch
                    }
                })
                .collect();
            Ok(results)
        }
    }

    fn crdb_limits(k: usize) -> BatchLimits {
        BatchLimits {
            max_in_flight: Some(k),
            max_rows: 128,
            max_bytes: 1 << 20,
            exclusive_keys: true,
        }
    }

    fn cas(key: &str, seqno: u64) -> CasOp {
        CasOp {
            key: key.to_owned(),
            new: VersionedData {
                seqno: SeqNo(seqno),
                data: Bytes::from_static(b"x"),
            },
        }
    }

    async fn wait_started(backend: &FakeBackend, n: usize) {
        while backend.started.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn assert_idle<B: BatchBackend>(batcher: &Batcher<B>) {
        let scheduler = batcher.shared.scheduler.lock().unwrap();
        assert!(
            scheduler.ranges.is_empty(),
            "scheduler should be idle: {:?}",
            scheduler.ranges
        );
    }

    fn spawn_cas(
        batcher: &Arc<Batcher<FakeBackend>>,
        key: &str,
        seqno: u64,
    ) -> mz_ore::task::JoinHandle<Result<CaSResult, ExternalError>> {
        let batcher = Arc::clone(batcher);
        let key = key.to_owned();
        mz_ore::task::spawn(|| "cas", async move { batcher.run(cas(&key, seqno)).await })
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn coalesces_at_capacity() {
        let metrics = metrics();
        let batcher = Arc::new(Batcher::new(
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
        assert_eq!(metrics.statements.get(), u64::cast_from(statements.len()));
        assert_eq!(metrics.batch_size.get_sample_sum(), 6.0);
        assert_eq!(metrics.in_flight.get(), 0);
        assert_idle(&batcher);
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn ranges_and_uncapped() {
        // Two ranges with K = 1 each run one statement apiece at once.
        let ranges = RangeMap::from_start_keys([Some("/\"s5\"")]).unwrap();
        let batcher = Arc::new(Batcher::new(
            FakeBackend::new(crdb_limits(1), ranges),
            metrics(),
        ));
        let a = spawn_cas(&batcher, "s1", 1);
        let b = spawn_cas(&batcher, "s7", 1);
        wait_started(batcher.backend(), 2).await;
        batcher.backend().gate.add_permits(2);
        assert_eq!(a.await, Ok(CaSResult::Committed));
        assert_eq!(b.await, Ok(CaSResult::Committed));

        // Postgres-style limits: no cap and no key exclusion, so same-key calls race.
        *batcher.backend().limits.lock().unwrap() = BatchLimits {
            max_in_flight: None,
            max_rows: 1,
            max_bytes: 0,
            exclusive_keys: false,
        };
        let racers: Vec<_> = (0..4).map(|_| spawn_cas(&batcher, "s1", 2)).collect();
        wait_started(batcher.backend(), 6).await;
        batcher.backend().gate.add_permits(4);
        let mut committed = 0;
        for racer in racers {
            if racer.await == Ok(CaSResult::Committed) {
                committed += 1;
            }
        }
        assert_eq!(committed, 1);
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
            let batcher = Arc::new(Batcher::new(
                FakeBackend::new(crdb_limits(1), RangeMap::default()),
                metrics(),
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
            assert_eq!(batcher.run(cas("s0", 1)).await, Ok(CaSResult::Committed));
            assert_eq!(batcher.shared.metrics.in_flight.get(), 0);
            assert_idle(&batcher);
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn lowering_the_cap_drains_through_completions() {
        let batcher = Arc::new(Batcher::new(
            FakeBackend::new(crdb_limits(3), RangeMap::default()),
            metrics(),
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
