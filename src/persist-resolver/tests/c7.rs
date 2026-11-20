// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A write that touches both manifest shards (`Register`, `Finalize`) is one
//! multi-key CaS: when it loses on either manifest shard, neither half lands,
//! and the resolver reruns it. Each case first moves one manifest shard's upper
//! behind the resolver's back, with an empty append from another client.
//!
//! Needs `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL`, like `resolver.rs`.

use std::str::FromStr;
use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::v2::{OpKind, ProtoCommitResult, ProtoEmpty, ProtoOp};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_resolver::{Request, ResolverConfig, Running, Stats, start_and_serve};
use mz_persist_types::codec_impls::VecU8Schema;
use timely::progress::Antichain;

async fn client(location: &PersistLocation) -> PersistClient {
    PersistClientCache::new(
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &MetricsRegistry::new(),
        |_, _| PubSubClientConnection::noop(),
    )
    .open(location.clone())
    .await
    .expect("open")
}

/// Binds `addr`, retrying while a dropped resolver's serve task, which aborts
/// asynchronously, still holds it.
async fn bind(addr: std::net::SocketAddr) -> tokio::net::TcpListener {
    for _ in 0..100 {
        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            return listener;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("{addr} stayed in use");
}

async fn start(client: &PersistClient, addr: std::net::SocketAddr, control: ShardId) -> Running {
    let cfg = ResolverConfig {
        resolver_id: format!("resolver-{}", uuid::Uuid::new_v4()),
        advertise_addr: String::new(),
        control_shard: control,
        partition: "p0".into(),
        manifest_inline_single_max_bytes: ResolverConfig::DEFAULT_MANIFEST_INLINE_SINGLE_MAX_BYTES,
        manifest_inline_total_max_bytes: ResolverConfig::DEFAULT_MANIFEST_INLINE_TOTAL_MAX_BYTES,
        upper_groups: true,
    };
    start_and_serve(client, cfg, bind(addr).await)
        .await
        .expect("start")
}

type Manifest = WriteHandle<Vec<u8>, Vec<u8>, u64, i64>;

async fn manifest(client: &PersistClient, shard: ShardId) -> Manifest {
    client
        .open_writer(
            shard,
            Arc::new(VecU8Schema),
            Arc::new(VecU8Schema),
            Diagnostics::for_tests(),
        )
        .await
        .expect("manifest codecs")
}

async fn upper(m: &mut Manifest) -> u64 {
    *m.fetch_recent_upper().await.as_option().expect("open")
}

/// Advances `m`'s upper by one with no rows, as another writer would.
async fn bump(m: &mut Manifest) {
    let t = upper(m).await;
    let empty: [((Vec<u8>, Vec<u8>), u64, i64); 0] = [];
    m.compare_and_append(&empty, Antichain::from_elem(t), Antichain::from_elem(t + 1))
        .await
        .expect("usage")
        .expect("upper");
}

fn stats(running: &Running) -> Stats {
    running.stats.lock().expect("lock").clone()
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)]
async fn lost_multi_key_cas_leaves_neither_half() {
    let Ok(consensus) = std::env::var("MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL") else {
        eprintln!("skipping: MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL unset");
        return;
    };
    let blob_dir = tempfile::tempdir().expect("tempdir");
    let location = PersistLocation {
        blob_uri: SensitiveUrl::from_str(&format!("file://{}", blob_dir.path().display()))
            .expect("url"),
        consensus_uri: SensitiveUrl::from_str(&consensus).expect("url"),
    };
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr");
    mz_persist_client::v2::client::set_resolver_url(&format!("http://{addr}"));

    let resolver_client = client(&location).await;
    let control_shard = ShardId::new();
    let running = start(&resolver_client, addr, control_shard).await;
    let other = client(&location).await;
    let mut control = manifest(&other, control_shard).await;
    let mut partition = manifest(&other, running.partition_shard).await;
    let resolver = mz_persist_client::v2::client::resolver().expect("routing on");

    // Register with each manifest shard moved in turn. The resolver's first
    // multi-key CaS loses, nothing of it lands, and the rerun writes both
    // manifest shards once.
    let mut shards = Vec::new();
    for moved in ["partition", "control"] {
        let m = if moved == "partition" {
            &mut partition
        } else {
            &mut control
        };
        bump(m).await;
        let (c0, p0) = (upper(&mut control).await, upper(&mut partition).await);
        let before = stats(&running);

        let shard = ShardId::new();
        // A resolver that panics on a half-landed write never answers.
        let register = other.maybe_register_v2::<String, String, u64, i64>(shard);
        let registered = tokio::time::timeout(std::time::Duration::from_secs(30), register)
            .await
            .unwrap_or_else(|_| panic!("{moved}: registration never answered"));
        assert!(registered);

        let after = stats(&running);
        assert_eq!(
            after.lost_writes - before.lost_writes,
            1,
            "{moved}: one lost CaS"
        );
        assert_eq!(
            after.multi_writes - before.multi_writes,
            1,
            "{moved}: one landed"
        );
        assert_eq!(
            (upper(&mut control).await, upper(&mut partition).await),
            (c0 + 1, p0 + 1),
            "{moved}: the lost CaS wrote neither manifest shard"
        );
        let view = resolver.fetch(&shard).await.expect("registered");
        assert_eq!(view.upper.expect("upper").elements, vec![0], "{moved}");
        assert_eq!(view.intents.len(), 1, "{moved}: the sentinel");
        shards.push(shard);
    }

    // Finalize, with the partition moved: the shard is finalized in control and
    // its partition rows are gone, in one write.
    bump(&mut partition).await;
    let (c0, p0) = (upper(&mut control).await, upper(&mut partition).await);
    let before = stats(&running);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let finalize = ProtoOp {
        shard: shards[0].to_string(),
        kind: Some(OpKind::Finalize(ProtoEmpty {})),
    };
    running
        .tx
        .send(Request::Commit(vec![finalize], tx))
        .expect("send");
    let resp = rx.await.expect("reply");
    assert!(
        matches!(resp.result, Some(ProtoCommitResult::Ok(_))),
        "{resp:?}"
    );
    let after = stats(&running);
    assert_eq!(after.lost_writes - before.lost_writes, 1);
    assert_eq!(
        (upper(&mut control).await, upper(&mut partition).await),
        (c0 + 1, p0 + 1)
    );

    // A new resolver hydrates both manifest shards, and its invariant check
    // (which reports a registered shard with no upper, or a finalized shard
    // with partition rows) passes.
    drop(running);
    let running = start(&resolver_client, addr, control_shard).await;
    let view = resolver.fetch(&shards[1]).await.expect("registered");
    assert_eq!(view.upper.expect("upper").elements, vec![0]);
    let finalized = resolver.fetch(&shards[0]).await.expect("tombstone");
    assert!(finalized.meta.expect("meta").finalized);
    assert!(finalized.batches.is_empty() && finalized.intents.is_empty());
    drop(running);
}
