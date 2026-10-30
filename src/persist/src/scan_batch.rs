// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coalesces concurrent unlimited [Consensus::scan]s into one statement.
//!
//! Every scan joins one process-wide queue, and at most [CONSENSUS_SCAN_BATCH_MAX_IN_FLIGHT]
//! statements run at once. Under that cap a scan goes out at once with whatever is queued. At the
//! cap it waits for a statement to return and joins the next one. There is no timer, so batches
//! only grow when scans arrive together.
//!
//! NOTE: batches ignore CockroachDB ranges. On a single-node CockroachDB v23.1 with the table
//! split into 64 ranges, polls of 128 shards returning 0-1 rows each measured 64-85 µs of CPU per
//! shard as one statement, 295-321 µs as one statement per range, and 386-410 µs as single-shard
//! scans (medians of 3 runs).
//!
//! [Consensus::scan]: crate::location::Consensus::scan

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::anyhow;
use itertools::Itertools;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastLossy;
use tokio::sync::oneshot;

use crate::cas_batch::fan_out;
use crate::location::{ExternalError, SeqNo, VersionedData};
use crate::metrics::ConsensusBatchMetrics;
use crate::postgres::{
    CONSENSUS_SCAN_BATCH_MAX_IN_FLIGHT, CONSENSUS_SCAN_BATCH_MAX_SHARDS, PostgresConsensusClient,
};

#[derive(Debug)]
struct PendingScan {
    key: String,
    from: SeqNo,
    enqueued: Instant,
    tx: oneshot::Sender<Result<Vec<VersionedData>, ExternalError>>,
}

/// The scans of one statement, by shard.
type Batch = BTreeMap<String, Vec<PendingScan>>;

#[derive(Debug, Default)]
struct Queue {
    in_flight: usize,
    pending: VecDeque<PendingScan>,
}

/// Coalesces concurrent unlimited scans into statements run by
/// [PostgresConsensusClient::scan_batch].
#[derive(Debug)]
pub(crate) struct ScanBatcher {
    client: PostgresConsensusClient,
    cfg: Arc<ConfigSet>,
    queue: Mutex<Queue>,
    metrics: ConsensusBatchMetrics,
}

impl ScanBatcher {
    pub fn new(
        client: PostgresConsensusClient,
        cfg: Arc<ConfigSet>,
        metrics: ConsensusBatchMetrics,
    ) -> Arc<Self> {
        Arc::new(ScanBatcher {
            client,
            cfg,
            queue: Mutex::default(),
            metrics,
        })
    }

    /// Returns every version of `key` at or after `from`.
    pub async fn scan(
        self: &Arc<Self>,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .lock()
            .expect("lock poisoned")
            .pending
            .push_back(PendingScan {
                key: key.to_owned(),
                from,
                enqueued: Instant::now(),
                tx,
            });
        spawn_batches(self);
        rx.await.unwrap_or_else(|_| {
            Err(ExternalError::from(anyhow!(
                "consensus scan batch exited without a result"
            )))
        })
    }

    /// Pops batches off the queue while fewer than `max_in_flight` statements run, counting each
    /// as in flight.
    fn take_batches(&self) -> Vec<Batch> {
        // A cap of 0 would leave nothing to schedule a queued scan, and the shard floor keeps a
        // low shard limit from turning every scan into its own statement.
        let max_in_flight = CONSENSUS_SCAN_BATCH_MAX_IN_FLIGHT.get(&self.cfg).max(1);
        let max_shards = CONSENSUS_SCAN_BATCH_MAX_SHARDS.get(&self.cfg).max(8);
        let mut queue = self.queue.lock().expect("lock poisoned");
        let mut batches = Vec::new();
        while queue.in_flight < max_in_flight {
            let mut batch = Batch::new();
            while let Some(p) = queue.pending.pop_front() {
                if p.tx.is_closed() {
                    continue;
                }
                if batch.len() >= max_shards && !batch.contains_key(&p.key) {
                    queue.pending.push_front(p);
                    break;
                }
                batch.entry(p.key.clone()).or_default().push(p);
            }
            if batch.is_empty() {
                break;
            }
            queue.in_flight += 1;
            batches.push(batch);
        }
        batches
    }
}

fn spawn_batches(batcher: &Arc<ScanBatcher>) {
    for batch in batcher.take_batches() {
        let now = Instant::now();
        let metrics = &batcher.metrics;
        metrics
            .scan_batch_size
            .observe(f64::cast_lossy(batch.len()));
        for p in batch.values().flatten() {
            metrics
                .scan_queue_wait_seconds
                .observe((now - p.enqueued).as_secs_f64());
        }
        let slot = InFlight(Arc::clone(batcher));
        mz_ore::task::spawn(|| "persist::consensus::scan_batch", async move {
            // Scans of the same shard share its rows from the earliest `from`.
            let scans: Vec<_> = batch
                .iter()
                .map(|(key, ps)| {
                    let from = ps.iter().map(|p| p.from).min().expect("non-empty");
                    (key.as_str(), from)
                })
                .collect();
            let result = slot.0.client.scan_batch(&scans).await;
            // The caller may have given up, which is fine.
            match result {
                Ok(rows) => {
                    for ((_, ps), rows) in batch.into_iter().zip_eq(rows) {
                        for p in ps {
                            let versions = rows.iter().filter(|v| v.seqno >= p.from).cloned();
                            let _ = p.tx.send(Ok(versions.collect()));
                        }
                    }
                }
                Err(err) => {
                    let callers: Vec<_> = batch.into_values().flatten().collect();
                    let errs = fan_out(err, callers.len());
                    for (p, err) in callers.into_iter().zip_eq(errs) {
                        let _ = p.tx.send(Err(err));
                    }
                }
            }
        });
    }
}

/// Frees one in-flight slot and schedules the queue when dropped, on every exit of a batch task.
///
/// NOTE: a panic or cancellation that skipped this would leak the slot, and after
/// `max_in_flight` leaks no queued scan would ever run.
struct InFlight(Arc<ScanBatcher>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.queue.lock().expect("lock poisoned").in_flight -= 1;
        spawn_batches(&self.0);
    }
}
