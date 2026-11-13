// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: a resolver serving v2 persist shards from two manifests.
//!
//! The control manifest holds which shards exist, their codecs, schemas and
//! partition, and which partitions exist. One partition manifest holds its
//! shards' uppers, batches and read intents, and the partition's claim. Both are
//! ordinary v1 shards of `(key, val, manifest time, diff)` with proto-encoded
//! keys and values ([mz_persist_client::v2]).
//!
//! The resolver claims its partition, then runs a cycle: take every request that
//! arrived, check each commit against its [View] and apply it in place, then
//! write the cycle's row changes with one `compare_and_append` per manifest that
//! changed, at the manifest's current time. Correctness rests on that CaS, not on
//! the resolver: if it loses, something else wrote the manifest, so the resolver
//! rebuilds its view from the manifests and reruns the same commits, or stops if
//! the claim is no longer its own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::v2::{Manifest, PartitionId, ProtoClaim, ProtoCommitRequest};
use mz_persist_client::v2::{
    ManifestKey, ProtoCommitResponse, ProtoCommitResult, ProtoEmpty, ProtoFetchRequest,
    ProtoFetchResponse, ProtoOp, ProtoPartitionVal, ProtoPersistResolver,
    ProtoPersistResolverServer, ProtoShardView, ValKind, decode_val, encode_val, op_shard,
};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::VecU8Schema;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::view::{CommitError, RowDiff, View};

pub mod view;

/// How a resolver identifies itself and what it serves.
#[derive(Clone, Debug)]
pub struct ResolverConfig {
    /// Unique per resolver process.
    pub resolver_id: String,
    /// The address clients reach this resolver at, recorded in its claim.
    pub advertise_addr: String,
    pub control_shard: ShardId,
    pub partition: PartitionId,
}

/// A request to the resolver's cycle.
#[derive(Debug)]
pub enum Request {
    Fetch(ShardId, oneshot::Sender<Option<ProtoShardView>>),
    Commit(Vec<ProtoOp>, oneshot::Sender<ProtoCommitResponse>),
}

type ManifestUpdate = ((Vec<u8>, Vec<u8>), u64, i64);

/// A manifest: a v1 shard, and the handles the resolver uses on it.
struct ManifestShard {
    name: &'static str,
    write: WriteHandle<Vec<u8>, Vec<u8>, u64, i64>,
    read: ReadHandle<Vec<u8>, Vec<u8>, u64, i64>,
    writes: u64,
}

/// How often, in writes, the resolver advances a manifest's since. The since
/// trails the manifest's upper, so the snapshot at `upper - 1` it rebuilds its
/// view from stays readable.
const DOWNGRADE_EVERY: u64 = 64;

impl ManifestShard {
    async fn open(client: &PersistClient, shard: ShardId, name: &'static str) -> Self {
        if let Some(v2) = mz_persist_client::v2::client::resolver() {
            v2.mark_v1(&shard);
        }
        let (write, read) = client
            .open::<Vec<u8>, Vec<u8>, u64, i64>(
                shard,
                Arc::new(VecU8Schema),
                Arc::new(VecU8Schema),
                Diagnostics {
                    shard_name: format!("resolver {name} manifest"),
                    handle_purpose: "resolver".to_owned(),
                },
                false,
            )
            .await
            .expect("manifest codecs");
        ManifestShard {
            name,
            write,
            read,
            writes: 0,
        }
    }

    /// The manifest's live rows and its upper.
    async fn read_all(&mut self) -> (Vec<(ManifestKey, ValKind)>, u64) {
        let upper = *self
            .write
            .fetch_recent_upper()
            .await
            .as_option()
            .expect("manifest is never finalized");
        if upper == 0 {
            return (Vec::new(), 0);
        }
        let updates = self
            .read
            .snapshot_and_fetch(Antichain::from_elem(upper - 1))
            .await
            .expect("the resolver holds the manifest's since below its upper");
        let mut consolidated: BTreeMap<(Vec<u8>, Vec<u8>), i64> = BTreeMap::new();
        for ((k, v), _t, d) in updates {
            *consolidated.entry((k, v)).or_default() += d;
        }
        let rows = consolidated
            .into_iter()
            .filter(|(_, d)| *d != 0)
            .map(|((k, v), d)| {
                assert_eq!(d, 1, "{} manifest row with diff {d}", self.name);
                let key = ManifestKey::decode(&k).expect("valid manifest key");
                let val = decode_val(&v).expect("valid manifest val");
                (key, val)
            })
            .collect();
        (rows, upper)
    }

    /// Writes `diffs` at manifest time `at`. On a lost CaS, returns the
    /// manifest's current upper.
    async fn write(&mut self, at: u64, diffs: &[&RowDiff]) -> Result<(), u64> {
        let updates: Vec<ManifestUpdate> = diffs
            .iter()
            .map(|(k, v, d)| ((k.encode(), encode_val(v)), at, *d))
            .collect();
        let res = self
            .write
            .compare_and_append(
                updates.iter(),
                Antichain::from_elem(at),
                Antichain::from_elem(at + 1),
            )
            .await
            .expect("valid manifest write");
        match res {
            Ok(()) => {
                self.writes += 1;
                if self.writes % DOWNGRADE_EVERY == 0 {
                    self.read.downgrade_since(&Antichain::from_elem(at)).await;
                }
                Ok(())
            }
            Err(mismatch) => Err(*mismatch.current.as_option().expect("not finalized")),
        }
    }
}

/// A running resolver.
pub struct Resolver {
    cfg: ResolverConfig,
    control: ManifestShard,
    partition: ManifestShard,
    view: View,
    /// Cycles and write latencies, for rough numbers.
    pub stats: Arc<std::sync::Mutex<Stats>>,
}

/// Rough numbers about a resolver's cycles.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub cycles: u64,
    pub commits: u64,
    pub manifest_writes: u64,
    pub rows_written: u64,
    pub encoded_bytes_written: u64,
    pub write_micros: Vec<u64>,
}

impl Resolver {
    /// Opens the manifests, creates the partition if it doesn't exist, and claims
    /// it.
    pub async fn start(client: &PersistClient, cfg: ResolverConfig) -> Resolver {
        let mut control = ManifestShard::open(client, cfg.control_shard, "control").await;
        let (rows, control_time) = control.read_all().await;
        let mut view = View::new(cfg.partition.clone());
        for (k, v) in rows {
            view.apply(k, v, 1);
        }
        view.control_time = control_time;

        // Bootstrap: record the partition, and its manifest's shard, in control.
        let partition_key = ManifestKey::Partition(cfg.partition.clone());
        let partition_shard = loop {
            if let Some(ValKind::Partition(p)) = view.rows.get(&partition_key) {
                break p.manifest_shard.parse::<ShardId>().expect("valid shard");
            }
            let val = ValKind::Partition(ProtoPartitionVal {
                manifest_shard: ShardId::new().to_string(),
                retired: false,
            });
            let diff = (partition_key.clone(), val.clone(), 1);
            match control.write(view.control_time, &[&diff]).await {
                Ok(()) => {
                    view.apply(partition_key.clone(), val, 1);
                    view.control_time += 1;
                }
                Err(_) => {
                    // Another resolver bootstrapped it first.
                    let (rows, t) = control.read_all().await;
                    view = View::new(cfg.partition.clone());
                    rows.into_iter().for_each(|(k, v)| view.apply(k, v, 1));
                    view.control_time = t;
                }
            }
        };
        let partition = ManifestShard::open(client, partition_shard, "partition").await;
        let mut resolver = Resolver {
            cfg,
            control,
            partition,
            view,
            stats: Default::default(),
        };
        resolver.reload().await;
        resolver.claim().await;
        resolver
    }

    pub fn view(&self) -> &View {
        &self.view
    }

    /// Rebuilds the view from the manifests.
    async fn reload(&mut self) {
        let (control_rows, control_time) = self.control.read_all().await;
        let (partition_rows, partition_time) = self.partition.read_all().await;
        let mut view = View::new(self.cfg.partition.clone());
        for (k, v) in control_rows.into_iter().chain(partition_rows) {
            view.apply(k, v, 1);
        }
        view.control_time = control_time;
        view.partition_time = partition_time;
        let violations = view.invariant_violations();
        assert!(violations.is_empty(), "manifest invariants: {violations:?}");
        self.view = view;
    }

    /// Takes the partition's claim, fencing whoever held it.
    async fn claim(&mut self) {
        loop {
            let old = self.view.claim();
            let token = old.as_ref().map_or(1, |c| c.token + 1);
            let new = ProtoClaim {
                resolver: self.cfg.resolver_id.clone(),
                token,
                addr: self.cfg.advertise_addr.clone(),
            };
            let mut diffs = Vec::new();
            if let Some(old) = old {
                diffs.push((ManifestKey::Claim, ValKind::Claim(old), -1));
            }
            diffs.push((ManifestKey::Claim, ValKind::Claim(new.clone()), 1));
            let refs: Vec<_> = diffs.iter().collect();
            match self.partition.write(self.view.partition_time, &refs).await {
                Ok(()) => {
                    info!(
                        "resolver {} claimed partition {} with token {token}",
                        self.cfg.resolver_id, self.cfg.partition
                    );
                    self.reload().await;
                    return;
                }
                Err(_) => self.reload().await,
            }
        }
    }

    fn holds_claim(&self) -> bool {
        self.view
            .claim()
            .is_some_and(|c| c.resolver == self.cfg.resolver_id)
    }

    /// Serves requests until the claim is lost or every sender is dropped.
    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Request>) {
        while let Some(first) = rx.recv().await {
            let mut commits = Vec::new();
            let mut next = Some(first);
            while let Some(req) = next.take().or_else(|| rx.try_recv().ok()) {
                match req {
                    Request::Fetch(shard, tx) => {
                        let _ = tx.send(self.view.shard_view(&shard));
                    }
                    Request::Commit(ops, tx) => commits.push((ops, tx)),
                }
            }
            if commits.is_empty() {
                continue;
            }
            if !self.cycle(commits).await {
                warn!("resolver {} lost its claim, stopping", self.cfg.resolver_id);
                return;
            }
        }
    }

    /// Commits one cycle's worth of commits. Returns false if the claim was lost.
    async fn cycle(
        &mut self,
        commits: Vec<(Vec<ProtoOp>, oneshot::Sender<ProtoCommitResponse>)>,
    ) -> bool {
        loop {
            let mut outcomes = Vec::with_capacity(commits.len());
            let mut diffs = Vec::new();
            for (ops, _) in commits.iter() {
                match self.view.commit(ops) {
                    Ok(d) => {
                        diffs.extend(d);
                        outcomes.push(Ok(()));
                    }
                    Err(e) => outcomes.push(Err(e)),
                }
            }
            // NOTE: not atomic across manifests (no multi-key CaS). A commit that
            // touches both, such as a registration, lands control first.
            let control: Vec<_> = diffs
                .iter()
                .filter(|(k, _, _)| k.manifest() == Manifest::Control)
                .collect();
            let partition: Vec<_> = diffs
                .iter()
                .filter(|(k, _, _)| k.manifest() == Manifest::Partition)
                .collect();
            let mut lost = false;
            if !control.is_empty() {
                lost = !self.write(true, &control).await;
            }
            if !lost && !partition.is_empty() {
                lost = !self.write(false, &partition).await;
            }
            if lost {
                self.reload().await;
                if !self.holds_claim() {
                    for (_, tx) in commits {
                        let _ = tx.send(ProtoCommitResponse {
                            result: Some(ProtoCommitResult::Fenced("claim lost".into())),
                            views: Vec::new(),
                        });
                    }
                    return false;
                }
                debug!(
                    "resolver cycle lost a CaS, rerunning {} commits",
                    commits.len()
                );
                continue;
            }
            let mut stats = self.stats.lock().expect("lock");
            stats.cycles += 1;
            stats.commits += u64::try_from(commits.len()).expect("fits");
            drop(stats);
            for ((ops, tx), outcome) in commits.into_iter().zip(outcomes) {
                let mut shards: Vec<_> = ops.iter().filter_map(|op| op_shard(op).ok()).collect();
                shards.dedup();
                let views = shards
                    .iter()
                    .filter_map(|s| self.view.shard_view(s))
                    .collect();
                let result = match outcome {
                    Ok(()) => ProtoCommitResult::Ok(ProtoEmpty {}),
                    Err(CommitError::Rejected(msg)) => ProtoCommitResult::Rejected(msg),
                    Err(CommitError::Invalid(msg)) => ProtoCommitResult::Invalid(msg),
                };
                let _ = tx.send(ProtoCommitResponse {
                    result: Some(result),
                    views,
                });
            }
            return true;
        }
    }

    /// Writes to one manifest at its current time. Returns false on a lost CaS.
    async fn write(&mut self, control: bool, diffs: &[&RowDiff]) -> bool {
        let (manifest, at) = if control {
            (&mut self.control, self.view.control_time)
        } else {
            (&mut self.partition, self.view.partition_time)
        };
        let start = Instant::now();
        let res = manifest.write(at, diffs).await;
        let elapsed = start.elapsed();
        match res {
            Ok(()) => {
                if control {
                    self.view.control_time += 1;
                } else {
                    self.view.partition_time += 1;
                }
                let mut stats = self.stats.lock().expect("lock");
                stats.manifest_writes += 1;
                stats.rows_written += u64::try_from(diffs.len()).expect("fits");
                stats.encoded_bytes_written += diffs
                    .iter()
                    .map(|(k, v, _)| k.encode().len() + encode_val(v).len())
                    .sum::<usize>() as u64;
                stats
                    .write_micros
                    .push(u64::try_from(elapsed.as_micros()).expect("fits"));
                true
            }
            Err(current) => {
                debug!(
                    "resolver lost CaS on {} manifest at {at}, now {current}",
                    manifest.name
                );
                false
            }
        }
    }
}

/// The resolver's gRPC front: forwards each request to the cycle.
#[derive(Clone, Debug)]
pub struct Service {
    pub tx: mpsc::UnboundedSender<Request>,
}

#[tonic::async_trait]
impl ProtoPersistResolver for Service {
    async fn commit(
        &self,
        request: tonic::Request<ProtoCommitRequest>,
    ) -> Result<tonic::Response<ProtoCommitResponse>, tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Request::Commit(request.into_inner().ops, tx))
            .map_err(|_| tonic::Status::unavailable("resolver stopped"))?;
        let resp = rx
            .await
            .map_err(|_| tonic::Status::unavailable("resolver stopped"))?;
        Ok(tonic::Response::new(resp))
    }

    async fn fetch(
        &self,
        request: tonic::Request<ProtoFetchRequest>,
    ) -> Result<tonic::Response<ProtoFetchResponse>, tonic::Status> {
        let shard = request
            .into_inner()
            .shard
            .parse::<ShardId>()
            .map_err(tonic::Status::invalid_argument)?;
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Request::Fetch(shard, tx))
            .map_err(|_| tonic::Status::unavailable("resolver stopped"))?;
        let view = rx
            .await
            .map_err(|_| tonic::Status::unavailable("resolver stopped"))?;
        Ok(tonic::Response::new(ProtoFetchResponse { view }))
    }
}

/// Serves `service` on `listener` until the task is dropped.
pub async fn serve(service: Service, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(
            ProtoPersistResolverServer::new(service)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

/// The tasks of a running resolver.
pub struct Running {
    /// Sends requests straight to the cycle, bypassing gRPC.
    pub tx: mpsc::UnboundedSender<Request>,
    pub stats: Arc<std::sync::Mutex<Stats>>,
    pub serve: AbortOnDropHandle<anyhow::Result<()>>,
    /// Ends when the resolver loses its claim.
    pub cycle: AbortOnDropHandle<()>,
}

/// Starts a resolver and serves it on `listener`.
pub async fn start_and_serve(
    client: &PersistClient,
    mut cfg: ResolverConfig,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<Running> {
    let addr = listener.local_addr()?;
    if cfg.advertise_addr.is_empty() {
        cfg.advertise_addr = format!("http://{addr}");
    }
    let resolver = Resolver::start(client, cfg).await;
    let stats = Arc::clone(&resolver.stats);
    let (tx, rx) = mpsc::unbounded_channel();
    let service = Service { tx: tx.clone() };
    let serve =
        mz_ore::task::spawn(|| "persist-resolver-serve", serve(service, listener)).abort_on_drop();
    let cycle = mz_ore::task::spawn(|| "persist-resolver-cycle", resolver.run(rx)).abort_on_drop();
    Ok(Running {
        tx,
        stats,
        serve,
        cycle,
    })
}
