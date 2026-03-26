// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A pool of pre-opened persist shards for reducing DDL latency.
//!
//! A single `CREATE TABLE` normally makes ~30 CRDB round-trips. The biggest
//! chunk is `open_data_handles` (13 CRDB calls, ~32ms local). This module
//! pre-performs `upgrade_version` + `open_critical_since` (with epoch fencing)
//! in the background so they can be skipped at DDL time.
//!
//! The write handle still needs `RelationDesc` (the table schema), so it stays
//! on the critical path.

// TODO(ddl-perf): Track pre-opened ShardIds in a new `pre_allocated_shards` catalog
// collection (same pattern as `unfinalized_shards`). Without this, shards pre-opened
// but never claimed are leaked on crash. Recovery: on restart, move any remaining
// pre_allocated_shards to unfinalized_shards for GC by finalize_shards_task.

// TODO(ddl-perf): Add durability. Pre-opened shards are lost on restart, so no
// benefit until the pool refills. With catalog tracking, we could re-open handles
// for pre-allocated shards at startup.

// TODO(ddl-perf): Consider pre-opening WriteHandle too. Would need a dummy
// RelationDesc, with schema evolution via try_register_schema at DDL time.
// Saves additional CRDB calls but adds complexity.

use std::collections::VecDeque;
use std::num::NonZeroI64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::{Opaque, SinceHandle};
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::Codec64;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::dyncfgs::{SHARD_POOL_ENABLED, SHARD_POOL_TARGET_SIZE};
use mz_storage_types::sources::SourceData;
use mz_storage_types::StorageDiff;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::controller::PersistEpoch;
use crate::storage_collections::metrics::ShardPoolMetrics;

/// A pre-opened shard with its critical since handle already epoch-fenced.
#[derive(Debug)]
pub struct PreOpenedShard<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    /// The shard ID that was pre-allocated.
    pub shard_id: ShardId,
    /// The critical since handle, already epoch-fenced with `envd_epoch`.
    pub since_handle: SinceHandle<SourceData, (), T, StorageDiff>,
}

/// A thread-safe pool of pre-opened shards.
#[derive(Debug)]
pub struct ShardPool<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    inner: Mutex<VecDeque<PreOpenedShard<T>>>,
    /// When true, the replenishment task skips pre-opening new shards.
    /// Set during active DDL to avoid CRDB connection contention.
    paused: AtomicBool,
    metrics: ShardPoolMetrics,
}

impl<T> ShardPool<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    /// Creates a new pool with the given metrics.
    pub fn new(metrics: ShardPoolMetrics) -> Self {
        ShardPool {
            inner: Mutex::new(VecDeque::new()),
            paused: AtomicBool::new(false),
            metrics,
        }
    }

    /// Takes a pre-opened shard from the pool, if one is available.
    /// Updates hit/miss metrics accordingly.
    pub fn take(&self) -> Option<PreOpenedShard<T>> {
        let result = self.inner.lock().expect("lock poisoned").pop_front();
        if result.is_some() {
            self.metrics.hits.inc();
            self.update_size_metric();
        } else {
            self.metrics.misses.inc();
        }
        result
    }

    /// Returns a pre-opened shard to the pool.
    pub fn put(&self, shard: PreOpenedShard<T>) {
        self.inner.lock().expect("lock poisoned").push_back(shard);
        self.update_size_metric();
    }

    /// Returns the current number of shards in the pool.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("lock poisoned").len()
    }

    fn update_size_metric(&self) {
        self.metrics
            .pool_size
            .set(u64::cast_from(self.len()));
    }

    /// Returns whether the replenishment task is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Pause replenishment (e.g., during active DDL to avoid CRDB contention).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume replenishment.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }
}

/// Pre-opens a single shard by generating a new `ShardId`, calling
/// `upgrade_version`, and then `open_critical_since` with epoch fencing.
///
/// This mirrors the logic in `open_data_handles` (upgrade_version) and
/// `open_critical_handle` (epoch fencing CAS loop).
pub async fn pre_open_shard<T>(
    persist_client: &PersistClient,
    envd_epoch: NonZeroI64,
) -> Result<PreOpenedShard<T>, anyhow::Error>
where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync,
{
    let shard_id = ShardId::new();

    let diagnostics = Diagnostics {
        shard_name: format!("pre-opened:{shard_id}"),
        handle_purpose: "shard pool pre-open".to_owned(),
    };

    // Step 1: upgrade_version (same as open_data_handles line 602-611)
    persist_client
        .upgrade_version::<SourceData, (), T, StorageDiff>(shard_id, diagnostics.clone())
        .await
        .map_err(|e| anyhow::anyhow!("upgrade_version failed: {e:?}"))?;

    // Step 2: open_critical_since with epoch fencing CAS loop
    // (same as open_critical_handle line 696-734)
    let mut handle: SinceHandle<SourceData, (), T, StorageDiff> = persist_client
        .open_critical_since(
            shard_id,
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            Opaque::encode(&PersistEpoch::default()),
            diagnostics,
        )
        .await
        .map_err(|e| anyhow::anyhow!("open_critical_since failed: {e:?}"))?;

    let since = Antichain::from_elem(T::minimum());

    loop {
        let current_epoch: PersistEpoch = handle.opaque().decode();
        let unchecked_success = current_epoch.0.map(|e| e <= envd_epoch).unwrap_or(true);

        if unchecked_success {
            let checked_success = handle
                .compare_and_downgrade_since(
                    &Opaque::encode(&current_epoch),
                    (
                        &Opaque::encode(&PersistEpoch::from(envd_epoch)),
                        &since,
                    ),
                )
                .await
                .is_ok();
            if checked_success {
                break;
            }
        } else {
            mz_ore::halt!("shard pool: fenced by envd @ {current_epoch:?}. ours = {envd_epoch}");
        }
    }

    Ok(PreOpenedShard {
        shard_id,
        since_handle: handle,
    })
}

/// Configuration for the shard pool replenishment background task.
pub struct ShardPoolReplenishConfig<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    pub envd_epoch: NonZeroI64,
    pub config: Arc<Mutex<StorageConfiguration>>,
    pub persist_location: PersistLocation,
    pub persist: Arc<PersistClientCache>,
    pub pool: Arc<ShardPool<T>>,
}

/// Background task that keeps the shard pool filled to the target size.
///
/// Checks the pool size every second and pre-opens shards as needed.
pub async fn shard_pool_replenish_task<T>(
    ShardPoolReplenishConfig {
        envd_epoch,
        config,
        persist_location,
        persist,
        pool,
    }: ShardPoolReplenishConfig<T>,
) where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync,
{
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let (enabled, target_size) = {
            let config = config.lock().expect("lock poisoned");
            let config_set = config.config_set();
            (
                SHARD_POOL_ENABLED.get(config_set),
                SHARD_POOL_TARGET_SIZE.get(config_set),
            )
        };

        if !enabled || pool.is_paused() {
            continue;
        }

        let current_size = pool.len();
        if current_size >= target_size {
            continue;
        }

        let deficit = target_size - current_size;
        debug!(
            current_size,
            target_size, deficit, "replenishing shard pool"
        );

        let persist_client = match persist.open(persist_location.clone()).await {
            Ok(client) => client,
            Err(e) => {
                warn!("shard pool: failed to open persist client: {e}");
                continue;
            }
        };

        for _ in 0..deficit {
            match pre_open_shard::<T>(&persist_client, envd_epoch).await {
                Ok(shard) => {
                    debug!(shard_id = %shard.shard_id, "pre-opened shard added to pool");
                    pool.put(shard);
                }
                Err(e) => {
                    warn!("shard pool: failed to pre-open shard: {e}");
                    break;
                }
            }
        }
    }
}

/// Spawns the shard pool replenishment task and returns its handle.
///
/// The task is only spawned when not in read-only mode.
pub fn spawn_shard_pool_task<T>(
    envd_epoch: NonZeroI64,
    config: Arc<Mutex<StorageConfiguration>>,
    persist_location: PersistLocation,
    persist: Arc<PersistClientCache>,
    pool: Arc<ShardPool<T>>,
    read_only: bool,
) -> Option<Arc<AbortOnDropHandle<()>>>
where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync + 'static,
{
    if read_only {
        info!("disabling shard pool in read-only mode");
        return None;
    }

    let task = mz_ore::task::spawn(
        || "storage_collections::shard_pool_replenish_task",
        shard_pool_replenish_task(ShardPoolReplenishConfig {
            envd_epoch,
            config,
            persist_location,
            persist,
            pool,
        }),
    );

    Some(Arc::new(task.abort_on_drop()))
}
