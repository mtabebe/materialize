// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: a resolver serving data shards from two manifest shards.
//!
//! The control manifest shard holds which data shards exist, their codecs,
//! schemas and partition, and which partitions exist. One partition manifest
//! shard holds its data shards' uppers, batches and read intents, the
//! partition's claim, and `ControlAt`. Both are persist shards as they work today,
//! of `(key, val, manifest time, diff)` with proto-encoded keys and values
//! ([mz_persist_client::v2]).
//!
//! The resolver claims its partition, then runs a cycle: take every request that
//! arrived, check each commit against its [View] and apply it in place, then
//! write the cycle's row changes with one `compare_and_append` per manifest shard
//! that changed, at its current time. Correctness rests on that CaS, not on the
//! resolver: if it loses, something else wrote the manifest shard, so the
//! resolver hydrates its view again and reruns the same commits, or stops if the
//! claim is no longer its own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::v2::{
    Manifest, PartitionId, ProtoClaim, ProtoCommitRequest, ProtoWatchRequest,
};
use mz_persist_client::v2::{
    ManifestKey, ProtoCommitResponse, ProtoCommitResult, ProtoEmpty, ProtoFetchRequest,
    ProtoFetchResponse, ProtoOp, ProtoPartitionVal, ProtoPersistResolver,
    ProtoPersistResolverServer, ProtoShardView, ValKind, decode_val, encode_val, op_shard,
};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::VecU8Schema;
use timely::PartialOrder;
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
    /// Answered once the shard's rows change past the given view seqno.
    Watch(ShardId, u64, oneshot::Sender<Option<ProtoShardView>>),
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

    /// Hydrates the manifest shard's live rows the way any persist reader
    /// starts: a snapshot at its since, then a listen up to its current upper.
    /// Returns the rows and that upper.
    async fn hydrate(&mut self) -> (Vec<(ManifestKey, ValKind)>, u64) {
        let upper = *self
            .write
            .fetch_recent_upper()
            .await
            .as_option()
            .expect("manifest is never finalized");
        if upper == 0 {
            return (Vec::new(), 0);
        }
        let since = self.read.since().clone();
        let mut rows: BTreeMap<(Vec<u8>, Vec<u8>), i64> = BTreeMap::new();
        let snapshot = self
            .read
            .snapshot_and_fetch(since.clone())
            .await
            .expect("the resolver holds the manifest's since");
        for ((k, v), _t, d) in snapshot {
            *rows.entry((k, v)).or_default() += d;
        }
        let mut listen = self
            .read
            .clone("resolver hydrate")
            .await
            .listen(since)
            .await
            .expect("the resolver holds the manifest's since");
        'listen: loop {
            for event in listen.fetch_next().await {
                match event {
                    ListenEvent::Updates(updates) => {
                        for ((k, v), t, d) in updates {
                            if t < upper {
                                *rows.entry((k, v)).or_default() += d;
                            }
                        }
                    }
                    ListenEvent::Progress(p) => {
                        if PartialOrder::less_equal(&Antichain::from_elem(upper), &p) {
                            break 'listen;
                        }
                    }
                }
            }
        }
        let rows = rows
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
    /// Parked watches, by shard: the seqno the watcher knows, and where to reply.
    watches: BTreeMap<ShardId, Vec<(u64, oneshot::Sender<Option<ProtoShardView>>)>>,
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
    /// The view's `ControlAt` and control manifest time, as of the last cycle or
    /// hydration.
    pub control_at: Option<u64>,
    pub control_time: u64,
}

impl Resolver {
    /// Opens the manifests, creates the partition if it doesn't exist, and claims
    /// it.
    pub async fn start(client: &PersistClient, cfg: ResolverConfig) -> Resolver {
        let mut control = ManifestShard::open(client, cfg.control_shard, "control").await;
        let (rows, control_time) = control.hydrate().await;
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
                    let (rows, t) = control.hydrate().await;
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
            watches: BTreeMap::new(),
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
        let (partition_rows, partition_time) = self.partition.hydrate().await;
        // The partition's view is only usable with control at or past the time
        // its `ControlAt` names: a registration's partition rows can land before
        // this resolver's view of control has the shard.
        let control_at = partition_rows.iter().find_map(|(k, v)| match (k, v) {
            (ManifestKey::ControlAt, ValKind::ControlAt(c)) => Some(*c),
            _ => None,
        });
        let (control_rows, control_time) = loop {
            let (rows, time) = self.control.hydrate().await;
            match control_at {
                Some(c) if time <= c => {
                    debug!("control at {time}, partition needs it past {c}; waiting");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                _ => break (rows, time),
            }
        };
        let mut view = View::new(self.cfg.partition.clone());
        for (k, v) in control_rows.into_iter().chain(partition_rows) {
            view.apply(k, v, 1);
        }
        view.control_time = control_time;
        view.partition_time = partition_time;
        // Watchers may have missed anything: wake them all.
        let seqno = view.seqno();
        let shards: Vec<_> = view.rows.keys().filter_map(|k| k.shard()).collect();
        view.changed_at = shards.into_iter().map(|s| (s, seqno)).collect();
        let violations = view.invariant_violations();
        assert!(violations.is_empty(), "manifest invariants: {violations:?}");
        let mut stats = self.stats.lock().expect("lock");
        stats.control_at = view.control_at();
        stats.control_time = view.control_time;
        drop(stats);
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
                    Request::Watch(shard, known, tx) => self.watch(shard, known, tx),
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

    fn watch(&mut self, shard: ShardId, known: u64, tx: oneshot::Sender<Option<ProtoShardView>>) {
        let changed = self.view.changed_at.get(&shard).copied().unwrap_or(0);
        if changed > known || self.view.shard_meta(&shard).is_none() {
            let _ = tx.send(self.view.shard_view(&shard));
        } else {
            self.watches.entry(shard).or_default().push((known, tx));
        }
    }

    /// Records which shards a successful cycle changed, and answers their
    /// watches.
    fn notify(&mut self, diffs: &[RowDiff]) {
        let seqno = self.view.seqno();
        let mut shards: Vec<_> = diffs.iter().filter_map(|(k, _, _)| k.shard()).collect();
        shards.sort();
        shards.dedup();
        for shard in shards {
            self.view.changed_at.insert(shard, seqno);
            let Some(watches) = self.watches.remove(&shard) else {
                continue;
            };
            let view = self.view.shard_view(&shard);
            for (_, tx) in watches {
                let _ = tx.send(view.clone());
            }
        }
        // Drop watches whose callers went away.
        self.watches.retain(|_, ws| {
            ws.retain(|(_, tx)| !tx.is_closed());
            !ws.is_empty()
        });
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
            let mut partition: Vec<RowDiff> = partition.into_iter().cloned().collect();
            let mut lost = false;
            if !control.is_empty() {
                let c = self.view.control_time;
                lost = !self.write(true, &control).await;
                if !partition.is_empty() {
                    // NOTE: without a multi-key CaS (C7) this is a second CaS. A
                    // crash between the two leaves control ahead of the partition:
                    // a registered data shard with no `Upper` row, which the
                    // invariant check in `reload` reports.
                    partition.extend(self.view.set_control_at(c));
                }
            }
            if !lost && !partition.is_empty() {
                let refs: Vec<_> = partition.iter().collect();
                lost = !self.write(false, &refs).await;
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
            self.notify(&diffs);
            let mut stats = self.stats.lock().expect("lock");
            stats.control_at = self.view.control_at();
            stats.control_time = self.view.control_time;
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

    async fn watch(
        &self,
        request: tonic::Request<ProtoWatchRequest>,
    ) -> Result<tonic::Response<ProtoFetchResponse>, tonic::Status> {
        let req = request.into_inner();
        let shard = req
            .shard
            .parse::<ShardId>()
            .map_err(tonic::Status::invalid_argument)?;
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Request::Watch(shard, req.known_seqno, tx))
            .map_err(|_| tonic::Status::unavailable("resolver stopped"))?;
        let view = rx
            .await
            .map_err(|_| tonic::Status::unavailable("resolver stopped"))?;
        Ok(tonic::Response::new(ProtoFetchResponse { view }))
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
