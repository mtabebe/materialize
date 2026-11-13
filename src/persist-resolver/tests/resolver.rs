// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A resolver on real consensus and blob, driven through the ordinary persist
//! client API with v2 routing on.
//!
//! Needs `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL` (CRDB or Postgres), and
//! is a no-op without it, like persist's other external-storage tests.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::critical::{CriticalReaderId, Opaque};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::v2::{OpKind, ProtoAppend, ProtoCommitResult, ProtoFrontier, ProtoOp};
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_resolver::{Request, ResolverConfig, Running, start_and_serve};
use mz_persist_types::codec_impls::StringSchema;
use timely::progress::Antichain;

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
    let listener = bind(addr).await;
    let cfg = ResolverConfig {
        resolver_id: format!("resolver-{}", uuid::Uuid::new_v4()),
        advertise_addr: String::new(),
        control_shard: control,
        partition: "p0".into(),
        manifest_inline_single_max_bytes: ResolverConfig::DEFAULT_MANIFEST_INLINE_SINGLE_MAX_BYTES,
        manifest_inline_total_max_bytes: ResolverConfig::DEFAULT_MANIFEST_INLINE_TOTAL_MAX_BYTES,
    };
    start_and_serve(client, cfg, listener).await.expect("start")
}

fn diag() -> Diagnostics {
    Diagnostics::for_tests()
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)]
async fn resolver_end_to_end() {
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

    // Route this process's v2 shards to a resolver on a port we pick now.
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr");
    mz_persist_client::v2::client::set_resolver_url(&format!("http://{addr}"));

    let cache = PersistClientCache::new(
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &MetricsRegistry::new(),
        |_, _| PubSubClientConnection::noop(),
    );
    let client = cache.open(location).await.expect("open");
    let control = ShardId::new();
    let running = start(&client, addr, control).await;

    // A new shard registers as v2, an existing v1 shard stays v1.
    let v1_shard = ShardId::new();
    let (mut v1_write, _v1_read) = client
        .open::<String, String, u64, i64>(
            v1_shard,
            Arc::new(StringSchema),
            Arc::new(StringSchema),
            diag(),
            true,
        )
        .await
        .expect("open v1");
    v1_write
        .compare_and_append(
            &[(("v1".to_owned(), "x".to_owned()), 0u64, 1i64)],
            Antichain::from_elem(0),
            Antichain::from_elem(1),
        )
        .await
        .expect("usage")
        .expect("upper matches");
    assert!(
        !client
            .maybe_register_v2::<String, String, u64, i64>(v1_shard)
            .await
    );
    let shard = ShardId::new();
    assert!(
        client
            .maybe_register_v2::<String, String, u64, i64>(shard)
            .await
    );
    // Registering wrote control, then the partition with the control time it
    // landed at.
    {
        let stats = running.stats.lock().expect("lock").clone();
        let c = stats.control_at.expect("registration writes ControlAt");
        assert!(c < stats.control_time, "{stats:?}");
    }

    let (mut write, mut read) = client
        .open::<String, String, u64, i64>(
            shard,
            Arc::new(StringSchema),
            Arc::new(StringSchema),
            diag(),
            true,
        )
        .await
        .expect("open v2");
    let listen_read = read.clone("listen").await;
    let mut listen = listen_read
        .listen(Antichain::from_elem(0))
        .await
        .expect("listen");

    // Appends, with an empty append leaving a gap in the batches.
    let mut latencies = Vec::new();
    for t in 0..20u64 {
        let start = Instant::now();
        let data = [((format!("k{t}"), "v".to_owned()), t, 1i64)];
        let updates: &[_] = if t % 5 == 4 { &[] } else { &data };
        write
            .compare_and_append(
                updates,
                Antichain::from_elem(t),
                Antichain::from_elem(t + 1),
            )
            .await
            .expect("usage")
            .expect("upper matches");
        latencies.push(start.elapsed());
    }
    latencies.sort();
    eprintln!(
        "v2 compare_and_append latency: p50 {:?} p99 {:?} max {:?}",
        latencies[latencies.len() / 2],
        latencies[latencies.len() * 99 / 100],
        latencies.last().expect("some")
    );

    // A stale expected upper is an UpperMismatch with the current upper.
    let mismatch = write
        .compare_and_append(
            &[(("late".to_owned(), "v".to_owned()), 5u64, 1i64)],
            Antichain::from_elem(5),
            Antichain::from_elem(30),
        )
        .await
        .expect("usage")
        .expect_err("stale upper");
    assert_eq!(mismatch.current, Antichain::from_elem(20));

    // A snapshot sees every append below the as_of.
    let mut snap = read
        .snapshot_and_fetch(Antichain::from_elem(19))
        .await
        .expect("snapshot");
    snap.sort();
    assert_eq!(snap.len(), 16, "{snap:?}");

    // The listen saw the same updates, and progress to 20.
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    'listen: loop {
        assert!(Instant::now() < deadline, "listen stalled: {seen:?}");
        for event in listen.fetch_next().await {
            match event {
                ListenEvent::Updates(u) => seen.extend(u),
                ListenEvent::Progress(p) if p == Antichain::from_elem(20) => break 'listen,
                ListenEvent::Progress(_) => {}
            }
        }
    }
    // A listen at as_of 0 yields the updates after 0.
    assert_eq!(seen.len(), 15, "{seen:?}");

    // Read intents: the reader's since moves the shard's since.
    read.downgrade_since(&Antichain::from_elem(10)).await;

    // A critical since handle, matching on its opaque token.
    let critical_id = CriticalReaderId::new();
    let mut since = client
        .open_critical_since::<String, String, u64, i64>(
            shard,
            critical_id.clone(),
            Opaque::encode(&0u64),
            diag(),
        )
        .await
        .expect("critical");
    since
        .compare_and_downgrade_since(
            &Opaque::encode(&0u64),
            (&Opaque::encode(&1u64), &Antichain::from_elem(3)),
        )
        .await
        .expect("opaque matches");
    let wrong = since
        .compare_and_downgrade_since(
            &Opaque::encode(&0u64),
            (&Opaque::encode(&2u64), &Antichain::from_elem(4)),
        )
        .await;
    assert_eq!(wrong, Err(Opaque::encode(&1u64)));

    // The resolver's view, read back through the client's own routing.
    let resolver = mz_persist_client::v2::client::resolver().expect("routing on");
    let view = resolver.fetch(&shard).await.expect("v2 shard");
    // Compaction (requested by the writer, as in v1) may have merged batches.
    let updates: u64 = view.batches.iter().map(|b| b.len).sum();
    assert_eq!(updates, 16, "{} batches", view.batches.len());
    eprintln!(
        "v2 shard has {} batches after 20 appends",
        view.batches.len()
    );
    assert_eq!(view.intents.len(), 3, "{:?}", view.intents.keys());
    assert!(resolver.fetch(&v1_shard).await.is_none());
    let stats = running.stats.lock().expect("lock").clone();
    eprintln!(
        "resolver: {} cycles, {} commits, {} manifest writes, {} rows, {} bytes, {} bytes/write",
        stats.cycles,
        stats.commits,
        stats.manifest_writes,
        stats.rows_written,
        stats.encoded_bytes_written,
        stats.encoded_bytes_written / stats.manifest_writes.max(1)
    );

    // Restart the resolver: a new one rebuilds the views from the manifests,
    // claims the partition with a newer token, and the open handles keep working.
    drop(running);
    let running = start(&client, addr, control).await;
    {
        // Hydration reached at least the control time the partition names.
        let stats = running.stats.lock().expect("lock").clone();
        assert!(stats.control_at.expect("ControlAt") < stats.control_time);
    }
    let view_after = resolver.fetch(&shard).await.expect("v2 shard");
    assert_eq!(view_after.batches, view.batches);
    assert_eq!(view_after.intents, view.intents);
    assert_eq!(view_after.upper, view.upper);
    write
        .compare_and_append(
            &[(("after".to_owned(), "restart".to_owned()), 20u64, 1i64)],
            Antichain::from_elem(20),
            Antichain::from_elem(21),
        )
        .await
        .expect("usage")
        .expect("upper matches");
    let snap = read
        .snapshot_and_fetch(Antichain::from_elem(20))
        .await
        .expect("snapshot");
    assert_eq!(snap.len(), 17);

    // A second resolver claims the partition. The first learns it was fenced on
    // its next manifest write, which fails its CaS, and stops.
    let other_addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr");
    let newer = start(&client, other_addr, control).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let op = ProtoOp {
        shard: shard.to_string(),
        kind: Some(OpKind::Append(ProtoAppend {
            batch: None,
            expected: Some(ProtoFrontier { elements: vec![21] }),
            new: Some(ProtoFrontier { elements: vec![22] }),
        })),
    };
    running
        .tx
        .send(Request::Commit(vec![op], tx))
        .expect("send");
    let resp = rx.await.expect("reply");
    assert!(
        matches!(resp.result, Some(ProtoCommitResult::Fenced(_))),
        "{resp:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), running.cycle)
        .await
        .expect("fenced resolver stops");
    drop(newer);
}
