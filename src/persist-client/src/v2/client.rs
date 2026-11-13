// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: the client side of v2 shards.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist::location::SeqNo;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::{Codec, Codec64, ShardId};
use mz_proto::RustType;
use timely::progress::{Antichain, Timestamp};
use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

use crate::cfg::PersistConfig;
use crate::critical::{CriticalReaderId, Opaque};
use crate::error::CodecMismatch;
use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{
    CriticalReaderState, EncodedSchemas, HandleDebugState, HollowBatch, HollowRollup,
    LeasedReaderState, State, StateCollections, TypedState,
};
use crate::internal::trace::Trace;
use crate::read::LeasedReaderId;
use crate::v2::{
    BatchId, OpKind, ProtoAppend, ProtoCodecs, ProtoCommitRequest, ProtoCommitResponse,
    ProtoCommitResult, ProtoCompact, ProtoDropIntent, ProtoEmpty, ProtoEvolve, ProtoFetchRequest,
    ProtoHollowBatch, ProtoIntent, ProtoOp, ProtoPersistResolverClient, ProtoReadIntent,
    ProtoRegister, ProtoShardView, ProtoWatchRequest, SENTINEL_READER, frontier, proto_frontier,
};

/// The env var that points a process's persist clients at a resolver. Unset, every
/// shard is v1.
pub const RESOLVER_URL_ENV: &str = "MZ_PERSIST_RESOLVER_URL";

static RESOLVER: OnceLock<Option<Arc<ResolverClient>>> = OnceLock::new();

/// This process's resolver client, if [RESOLVER_URL_ENV] is set.
pub fn resolver() -> Option<Arc<ResolverClient>> {
    RESOLVER
        .get_or_init(|| {
            let url = std::env::var(RESOLVER_URL_ENV).ok()?;
            if url.is_empty() {
                return None;
            }
            info!("persist v2: routing v2 shards to resolver at {url}");
            Some(Arc::new(ResolverClient::new(&url)))
        })
        .clone()
}

/// Points this process's persist clients at the resolver at `url`, in place of
/// [RESOLVER_URL_ENV]. Must run before any persist client is created.
pub fn set_resolver_url(url: &str) {
    RESOLVER
        .set(Some(Arc::new(ResolverClient::new(url))))
        .expect("resolver already initialized");
}

/// Makes every shard in this process v1, whatever [RESOLVER_URL_ENV] says.
///
/// The resolver calls this: its manifests are v1 shards, and looking them up in
/// itself would deadlock its startup.
pub fn disable_for_process() {
    let _ = RESOLVER.set(None);
}

/// The outcome of a v2 commit.
pub(crate) enum Committed<K, V, T, D> {
    /// The commit landed. The shard's state after it.
    Ok(TypedState<K, V, T, D>),
    /// A precondition failed against the resolver's view: the caller's state was
    /// stale. Refetch and recompute.
    Rejected,
}

#[derive(Debug)]
pub struct ResolverClient {
    url: String,
    /// Replaced after any failed call, so a restarted resolver at the same
    /// address is reached on a new connection.
    channel: Mutex<Channel>,
    /// Whether each shard looked up so far is v2. A shard's answer can't change
    /// once a process has opened it: registration happens before first open.
    known: Mutex<BTreeMap<ShardId, bool>>,
}

impl ResolverClient {
    pub fn new(url: &str) -> Self {
        ResolverClient {
            url: url.to_owned(),
            channel: Mutex::new(Self::connect(url)),
            known: Mutex::new(BTreeMap::new()),
        }
    }

    fn connect(url: &str) -> Channel {
        Endpoint::from_str(url)
            .expect("valid resolver url")
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy()
    }

    fn reconnect(&self) {
        *self.channel.lock().expect("lock") = Self::connect(&self.url);
    }

    fn rpc(&self) -> ProtoPersistResolverClient<Channel> {
        let channel = self.channel.lock().expect("lock").clone();
        ProtoPersistResolverClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }

    /// Records that `shard` is v1 without asking the resolver.
    pub fn mark_v1(&self, shard: &ShardId) {
        self.known.lock().expect("lock").insert(*shard, false);
    }

    /// Whether this process has already decided `shard` is v1.
    pub(crate) fn known_v1(&self, shard: &ShardId) -> bool {
        self.known.lock().expect("lock").get(shard) == Some(&false)
    }

    /// Whether `shard` is v2, as of the last [Self::fetch] in this process.
    pub fn is_v2(&self, shard: &ShardId) -> bool {
        self.known
            .lock()
            .expect("lock")
            .get(shard)
            .copied()
            .unwrap_or(false)
    }

    /// The resolver's view of `shard`, or None if it isn't a v2 shard. Retries
    /// transport errors forever, as persist does for consensus.
    pub async fn fetch(&self, shard: &ShardId) -> Option<ProtoShardView> {
        let req = ProtoFetchRequest {
            shard: shard.to_string(),
        };
        let view = self
            .retry_forever("fetch", || {
                let mut rpc = self.rpc();
                let req = req.clone();
                async move { rpc.fetch(req).await }
            })
            .await
            .view;
        self.known
            .lock()
            .expect("lock")
            .insert(*shard, view.is_some());
        view
    }

    /// Waits for `shard`'s view to change past `known_seqno`. Returns None if it
    /// didn't within a while, or the call failed: the caller asks again.
    pub async fn watch(&self, shard: &ShardId, known_seqno: u64) -> Option<ProtoShardView> {
        let req = ProtoWatchRequest {
            shard: shard.to_string(),
            known_seqno,
        };
        let mut rpc = self.rpc();
        match tokio::time::timeout(Duration::from_secs(30), rpc.watch(req)).await {
            Ok(Ok(resp)) => resp.into_inner().view,
            Ok(Err(status)) => {
                warn!("persist v2 watch to {} failed: {status}", self.url);
                self.reconnect();
                tokio::time::sleep(Duration::from_millis(100)).await;
                None
            }
            Err(_elapsed) => None,
        }
    }

    /// Sends `ops` as one commit. Retries transport errors forever.
    ///
    /// NOTE: a commit that landed but whose reply was lost is retried, and the
    /// retry fails its precondition. v2 has no idempotency tokens yet, so the
    /// caller then sees an `UpperMismatch` for a write that happened.
    pub async fn commit(&self, ops: Vec<ProtoOp>) -> ProtoCommitResponse {
        let req = ProtoCommitRequest { ops };
        loop {
            let resp = self
                .retry_forever("commit", || {
                    let mut rpc = self.rpc();
                    let req = req.clone();
                    async move { rpc.commit(req).await }
                })
                .await;
            if let Some(ProtoCommitResult::Fenced(msg)) = &resp.result {
                // Only one resolver address is known, so wait for the partition's
                // new holder to come up there.
                warn!("persist v2 commit: resolver fenced ({msg}), retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            return resp;
        }
    }

    async fn retry_forever<R, F, Fut>(&self, name: &str, mut f: F) -> R
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<R>, tonic::Status>>,
    {
        let mut backoff = Duration::from_millis(4);
        loop {
            match f().await {
                Ok(resp) => return resp.into_inner(),
                Err(status) => {
                    warn!(
                        "persist v2 {name} to {} failed, retrying in {backoff:?}: {status}",
                        self.url
                    );
                    self.reconnect();
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(1));
                }
            }
        }
    }

    /// Registers `shard` as a v2 shard with the given codecs. Returns false if it
    /// was already registered.
    pub async fn register<K: Codec, V: Codec, T: Codec64, D: Codec64>(
        &self,
        shard: &ShardId,
    ) -> bool {
        let op = ProtoOp {
            shard: shard.to_string(),
            kind: Some(OpKind::Register(ProtoRegister {
                codecs: Some(ProtoCodecs {
                    key: K::codec_name(),
                    val: V::codec_name(),
                    ts: T::codec_name(),
                    diff: D::codec_name(),
                }),
            })),
        };
        let resp = self.commit(vec![op]).await;
        let registered = match resp.result {
            Some(ProtoCommitResult::Ok(_)) => true,
            Some(ProtoCommitResult::Rejected(_)) => false,
            x => panic!("persist v2 register {shard}: {x:?}"),
        };
        self.known.lock().expect("lock").insert(*shard, true);
        registered
    }

    /// Commits `ops`, all on `shard`, from [ops_for_transition].
    pub(crate) async fn commit_transition<K, V, T, D>(
        &self,
        cfg: &PersistConfig,
        shard: &ShardId,
        ops: Vec<ProtoOp>,
    ) -> Committed<K, V, T, D>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Monoid + Codec64,
    {
        if ops.is_empty() {
            // Nothing the manifests keep changed (a heartbeat, a writer
            // registration, a rollup): report the current view.
            let view = self.fetch(shard).await.expect("v2 shard");
            return Committed::Ok(state_from_view(cfg, &view).expect("codecs checked at open"));
        }
        let resp = self.commit(ops).await;
        match resp.result {
            Some(ProtoCommitResult::Ok(ProtoEmpty {})) => {
                let view = resp
                    .views
                    .into_iter()
                    .find(|v| v.shard == shard.to_string())
                    .expect("commit reply has a view of each shard it touched");
                Committed::Ok(state_from_view(cfg, &view).expect("codecs checked at open"))
            }
            Some(ProtoCommitResult::Rejected(_)) => Committed::Rejected,
            x => panic!("persist v2 commit on {shard}: {x:?}"),
        }
    }
}

fn to_u64_frontier<T: Codec64>(f: &Antichain<T>) -> crate::v2::Frontier {
    Antichain::from(
        f.elements()
            .iter()
            .map(|t| u64::from_le_bytes(T::encode(t)))
            .collect::<Vec<_>>(),
    )
}

fn from_u64_frontier<T: Timestamp + Codec64>(f: &crate::v2::Frontier) -> Antichain<T> {
    Antichain::from(
        f.elements()
            .iter()
            .map(|t| T::decode(t.to_le_bytes()))
            .collect::<Vec<_>>(),
    )
}

/// The intents a state's readers hold, keyed by reader id.
fn intents_of<T: Timestamp + Codec64>(
    state: &StateCollections<T>,
) -> BTreeMap<String, ProtoIntent> {
    let leased = state.leased_readers.iter().map(|(id, r)| {
        let intent = ProtoIntent {
            frontier: Some(proto_frontier(&to_u64_frontier(&r.since))),
            opaque_codec: None,
            opaque: None,
        };
        (id.to_string(), intent)
    });
    let critical = state.critical_readers.iter().map(|(id, r)| {
        let intent = ProtoIntent {
            frontier: Some(proto_frontier(&to_u64_frontier(&r.since))),
            opaque_codec: Some(r.opaque.0.clone()),
            opaque: Some(u64::from_le_bytes(r.opaque.1)),
        };
        (id.to_string(), intent)
    });
    leased.chain(critical).collect()
}

fn nonempty_batches<T: Timestamp + Codec64>(
    state: &StateCollections<T>,
) -> BTreeMap<BatchId, ProtoHollowBatch> {
    state
        .trace
        .batches()
        .map(|b| b.into_proto())
        .filter(|b| !BatchId::is_empty_batch(b))
        .map(|b| (BatchId::of(&b), b))
        .collect()
}

/// The ops that take the manifests' rows for `old`'s shard to `new`'s.
///
/// Each op carries the old value of the rows it changes as its precondition.
/// Fields the manifests don't keep (writers, rollups, leases, seqno holds, the
/// applier version) produce no ops.
pub(crate) fn ops_for_transition<T: Timestamp + Lattice + Codec64>(
    old: &State<T>,
    new: &State<T>,
) -> Vec<ProtoOp> {
    let shard = new.shard_id.to_string();
    let op = |kind| ProtoOp {
        shard: shard.clone(),
        kind: Some(kind),
    };
    let (old, new) = (&old.collections, &new.collections);
    if new.is_tombstone() && !old.is_tombstone() {
        return vec![op(OpKind::Finalize(ProtoEmpty {}))];
    }
    let mut ops = Vec::new();

    for (id, schema) in new.schemas.iter() {
        if !old.schemas.contains_key(id) {
            ops.push(op(OpKind::Evolve(ProtoEvolve {
                schema_id: u64::cast_from(id.0),
                schema: Some(schema.into_proto()),
            })));
        }
    }

    let old_batches = nonempty_batches(old);
    let mut new_batches = nonempty_batches(new);
    let remove: Vec<_> = old_batches
        .iter()
        .filter(|(id, b)| new_batches.get(*id) != Some(*b))
        .map(|(_, b)| b.clone())
        .collect();
    let old_upper = to_u64_frontier(old.trace.upper());
    let new_upper = to_u64_frontier(new.trace.upper());
    let mut append = None;
    if old_upper != new_upper {
        let appended = new_batches
            .iter()
            .find(|(id, _)| id.lower == old_upper.elements())
            .map(|(id, _)| id.clone());
        let batch = appended.and_then(|id| new_batches.remove(&id));
        append = Some(OpKind::Append(ProtoAppend {
            batch,
            expected: Some(proto_frontier(&old_upper)),
            new: Some(proto_frontier(&new_upper)),
        }));
    }
    let add: Vec<_> = new_batches
        .into_iter()
        .filter(|(id, b)| old_batches.get(id) != Some(b))
        .map(|(_, b)| b)
        .collect();
    if !remove.is_empty() || !add.is_empty() {
        ops.push(op(OpKind::Compact(ProtoCompact { remove, add })));
    }
    if let Some(append) = append {
        ops.push(op(append));
    }

    let old_intents = intents_of(old);
    let new_intents = intents_of(new);
    for (reader, intent) in new_intents.iter() {
        let expected = old_intents.get(reader);
        if expected != Some(intent) {
            ops.push(op(OpKind::ReadIntent(ProtoReadIntent {
                reader: reader.clone(),
                expected: expected.cloned(),
                new: Some(intent.clone()),
            })));
        }
    }
    for (reader, intent) in old_intents.iter() {
        if !new_intents.contains_key(reader) {
            ops.push(op(OpKind::DropIntent(ProtoDropIntent {
                reader: reader.clone(),
                expected: Some(intent.clone()),
            })));
        }
    }
    ops
}

/// Today's per-shard state, built from a resolver's view of a v2 shard.
///
/// The trace is rebuilt from the view's batches in time order, with an empty batch
/// filling each gap the upper covers (a v2 shard has no batch for an empty append).
pub(crate) fn state_from_view<K, V, T, D>(
    cfg: &PersistConfig,
    view: &ProtoShardView,
) -> Result<TypedState<K, V, T, D>, Box<CodecMismatch>>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Monoid + Codec64,
{
    let shard_id = ShardId::from_str(&view.shard).expect("valid shard id");
    let meta = view.meta.clone().unwrap_or_default();
    let codecs = meta.codecs.unwrap_or_default();
    let requested = (
        K::codec_name(),
        V::codec_name(),
        T::codec_name(),
        D::codec_name(),
    );
    let actual = (codecs.key, codecs.val, codecs.ts, codecs.diff);
    if requested != actual {
        return Err(Box::new(CodecMismatch {
            requested: (requested.0, requested.1, requested.2, requested.3, None),
            actual: (actual.0, actual.1, actual.2, actual.3, None),
        }));
    }

    let now = (cfg.now)();
    let seqno = SeqNo(view.seqno);
    let debug = HandleDebugState {
        hostname: "v2".to_owned(),
        purpose: "v2 read intent".to_owned(),
    };
    let mut leased_readers = BTreeMap::new();
    let mut critical_readers = BTreeMap::new();
    let mut since: Option<Antichain<T>> = None;
    for (reader, intent) in view.intents.iter() {
        let f = from_u64_frontier::<T>(&frontier(&intent.frontier));
        since = Some(match since {
            None => f.clone(),
            Some(s) => s.meet(&f),
        });
        if reader == SENTINEL_READER {
            continue;
        }
        if let Ok(id) = LeasedReaderId::from_str(reader) {
            leased_readers.insert(
                id,
                LeasedReaderState {
                    seqno,
                    since: f,
                    last_heartbeat_timestamp_ms: now,
                    // v2 intents don't expire.
                    lease_duration_ms: 365 * 24 * 3600 * 1000,
                    debug: debug.clone(),
                },
            );
        } else if let Ok(id) = CriticalReaderId::from_str(reader) {
            let opaque = Opaque(
                intent.opaque_codec.clone().unwrap_or_default(),
                intent.opaque.unwrap_or_default().to_le_bytes(),
            );
            critical_readers.insert(
                id,
                CriticalReaderState {
                    since: f,
                    opaque,
                    debug: debug.clone(),
                },
            );
        } else {
            panic!("persist v2: unknown reader id {reader}");
        }
    }
    let since = since.unwrap_or_else(|| Antichain::from_elem(T::minimum()));
    let upper = from_u64_frontier::<T>(&frontier(&view.upper));

    let mut trace = Trace::default();
    let min_since = Antichain::from_elem(T::minimum());
    if meta.finalized {
        trace.push_batch_no_merge_reqs(HollowBatch::empty(Description::new(
            Antichain::from_elem(T::minimum()),
            Antichain::new(),
            min_since.clone(),
        )));
    } else {
        let mut batches: Vec<HollowBatch<T>> = view
            .batches
            .iter()
            .map(|b| HollowBatch::from_proto(b.clone()).expect("valid batch"))
            .collect();
        batches.sort_by_key(|b| BatchId::of(&b.into_proto()));
        for batch in batches {
            if trace.upper() != batch.desc.lower() {
                trace.push_batch_no_merge_reqs(HollowBatch::empty(Description::new(
                    trace.upper().clone(),
                    batch.desc.lower().clone(),
                    min_since.clone(),
                )));
            }
            trace.push_batch_no_merge_reqs(batch);
        }
        if trace.upper() != &upper {
            trace.push_batch_no_merge_reqs(HollowBatch::empty(Description::new(
                trace.upper().clone(),
                upper.clone(),
                min_since.clone(),
            )));
        }
    }
    let since = if meta.finalized {
        Antichain::new()
    } else {
        since
    };
    trace.downgrade_since(&since);

    let schemas = view
        .schemas
        .iter()
        .map(|(id, s)| {
            (
                SchemaId(usize::cast_from(*id)),
                EncodedSchemas::from_proto(s.clone()).expect("valid schema"),
            )
        })
        .collect();
    // Machinery above the applier expects a rollup at or below the current seqno.
    // v2 shards have none, and rollups and GC are skipped for them.
    let mut rollups = BTreeMap::new();
    rollups.insert(
        SeqNo::minimum(),
        HollowRollup {
            key: PartialRollupKey("v2-no-rollup".to_owned()),
            encoded_size_bytes: None,
            finalized: false,
        },
    );
    let state = State {
        shard_id,
        seqno,
        walltime_ms: now,
        hostname: "v2".to_owned(),
        collections: StateCollections {
            version: cfg.build_version.clone(),
            last_gc_req: SeqNo::minimum(),
            rollups,
            active_rollup: None,
            active_gc: None,
            leased_readers,
            critical_readers,
            writers: BTreeMap::new(),
            schemas,
            trace,
        },
    };
    Ok(TypedState {
        state,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::ProtoShardMeta;

    fn desc(lower: u64, upper: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0),
        )
    }

    fn batch(lower: u64, upper: u64) -> HollowBatch<u64> {
        HollowBatch::new_run(desc(lower, upper), vec![], 1)
    }

    fn view(
        shard: ShardId,
        upper: u64,
        batches: &[(u64, u64)],
        intents: &[(&str, u64)],
    ) -> ProtoShardView {
        ProtoShardView {
            shard: shard.to_string(),
            seqno: 7,
            meta: Some(ProtoShardMeta {
                codecs: Some(ProtoCodecs {
                    key: "String".into(),
                    val: "()".into(),
                    ts: "u64".into(),
                    diff: "i64".into(),
                }),
                finalized: false,
            }),
            schemas: BTreeMap::new(),
            upper: Some(proto_frontier(&Antichain::from_elem(upper))),
            batches: batches
                .iter()
                .map(|(l, u)| batch(*l, *u).into_proto())
                .collect(),
            intents: intents
                .iter()
                .map(|(r, t)| {
                    let intent = ProtoIntent {
                        frontier: Some(proto_frontier(&Antichain::from_elem(*t))),
                        opaque_codec: None,
                        opaque: None,
                    };
                    (r.to_string(), intent)
                })
                .collect(),
        }
    }

    fn state(view: &ProtoShardView) -> TypedState<String, (), u64, i64> {
        let cfg = PersistConfig::new_for_tests();
        state_from_view(&cfg, view).expect("codecs match")
    }

    #[mz_ore::test]
    fn view_to_state_fills_gaps() {
        let shard = ShardId::new();
        let reader = LeasedReaderId::new().to_string();
        let v = view(
            shard,
            10,
            &[(2, 4), (6, 8)],
            &[(&reader, 3), (SENTINEL_READER, 5)],
        );
        let s = state(&v);
        assert_eq!(s.seqno, SeqNo(7));
        assert_eq!(s.collections.trace.upper(), &Antichain::from_elem(10));
        assert_eq!(s.collections.trace.since(), &Antichain::from_elem(3));
        // [0,2) [2,4) [4,6) [6,8) [8,10): two batches, three fillers.
        let descs: Vec<_> = s
            .collections
            .trace
            .batches()
            .map(|b| (b.desc.lower().clone(), b.len))
            .collect();
        assert_eq!(descs.len(), 5, "{descs:?}");
        assert_eq!(s.collections.leased_readers.len(), 1);
        // The state round trips to the view's rows.
        assert_eq!(
            nonempty_batches(&s.collections)
                .into_values()
                .collect::<Vec<_>>(),
            v.batches
        );
        let mut intents = intents_of(&s.collections);
        intents.insert(SENTINEL_READER.into(), v.intents[SENTINEL_READER].clone());
        assert_eq!(intents, v.intents);
        // A different codec is refused.
        let cfg = PersistConfig::new_for_tests();
        assert!(state_from_view::<String, String, u64, i64>(&cfg, &v).is_err());
    }

    #[mz_ore::test]
    fn transitions_become_ops() {
        let shard = ShardId::new();
        let old = state(&view(shard, 4, &[(0, 4)], &[]));
        assert!(ops_for_transition(&old.state, &old.state).is_empty());

        // An append.
        let mut new = state(&view(shard, 4, &[(0, 4)], &[]));
        new.collections.trace.push_batch_no_merge_reqs(batch(4, 9));
        let ops = ops_for_transition(&old.state, &new.state);
        assert_eq!(ops.len(), 1);
        let Some(OpKind::Append(a)) = &ops[0].kind else {
            panic!("{ops:?}")
        };
        assert_eq!(frontier(&a.expected), Antichain::from_elem(4));
        assert_eq!(frontier(&a.new), Antichain::from_elem(9));
        assert_eq!(BatchId::of(a.batch.as_ref().expect("data")).lower, vec![4]);

        // A reader registering, a writer registering (no op), a schema.
        let mut new = state(&view(shard, 4, &[(0, 4)], &[]));
        let reader = LeasedReaderId::new();
        new.collections.leased_readers.insert(
            reader.clone(),
            LeasedReaderState {
                seqno: SeqNo(7),
                since: Antichain::from_elem(0),
                last_heartbeat_timestamp_ms: 0,
                lease_duration_ms: 0,
                debug: HandleDebugState {
                    hostname: String::new(),
                    purpose: String::new(),
                },
            },
        );
        let ops = ops_for_transition(&old.state, &new.state);
        assert_eq!(ops.len(), 1);
        let Some(OpKind::ReadIntent(r)) = &ops[0].kind else {
            panic!("{ops:?}")
        };
        assert_eq!(r.reader, reader.to_string());
        assert_eq!(r.expected, None);

        // Compaction: a merged batch replaces its inputs, upper unchanged.
        let old = state(&view(shard, 4, &[(0, 2), (2, 4)], &[]));
        let mut compacted = view(shard, 4, &[(0, 4)], &[]);
        compacted.batches[0].len = 2;
        let new = state(&compacted);
        let ops = ops_for_transition(&old.state, &new.state);
        let Some(OpKind::Compact(c)) = &ops[0].kind else {
            panic!("{ops:?}")
        };
        assert_eq!((c.remove.len(), c.add.len()), (2, 1));
    }
}
