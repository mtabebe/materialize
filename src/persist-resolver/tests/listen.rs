// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A listener in its own `PersistClient` follows a writer on a v2 shard through
//! appends and idle ticks (upper advances with no data), in lockstep.
//!
//! Needs `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL`, like `resolver.rs`.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_dyncfg::ConfigUpdates;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::{PERSIST_V2_WATCH, PersistConfig};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_resolver::{ResolverConfig, start_and_serve};
use mz_persist_types::codec_impls::StringSchema;
use timely::progress::Antichain;

async fn client(location: &PersistLocation, watch: bool) -> PersistClient {
    let cache = PersistClientCache::new(
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &MetricsRegistry::new(),
        |_, _| PubSubClientConnection::noop(),
    );
    let client = cache.open(location.clone()).await.expect("open");
    let mut updates = ConfigUpdates::default();
    updates.add(&PERSIST_V2_WATCH, watch);
    updates.apply(client.dyncfgs());
    client
}

/// Writes `steps` ticks to a new v2 shard with `writer`, every third one idle,
/// and checks that a listen through `listener` sees each tick's update and then
/// exactly that tick's upper, before the next tick is written. Returns the time
/// from each append's ack to the listener seeing its upper.
async fn lockstep(writer: &PersistClient, listener: &PersistClient, steps: u64) -> Vec<Duration> {
    let shard = ShardId::new();
    assert!(
        writer
            .maybe_register_v2::<String, String, u64, i64>(shard)
            .await
    );
    let mut write = writer
        .open_writer::<String, String, u64, i64>(
            shard,
            Arc::new(StringSchema),
            Arc::new(StringSchema),
            Diagnostics::for_tests(),
        )
        .await
        .expect("writer");
    let read = listener
        .open_leased_reader::<String, String, u64, i64>(
            shard,
            Arc::new(StringSchema),
            Arc::new(StringSchema),
            Diagnostics::for_tests(),
            true,
        )
        .await
        .expect("reader");
    let mut listen = read.listen(Antichain::from_elem(0)).await.expect("listen");

    let mut latencies = Vec::new();
    for t in 0..steps {
        let idle = t % 3 == 2;
        let update = ((format!("k{t}"), "v".to_owned()), t, 1i64);
        let updates: Vec<_> = if idle { vec![] } else { vec![update.clone()] };
        write
            .compare_and_append(
                &updates,
                Antichain::from_elem(t),
                Antichain::from_elem(t + 1),
            )
            .await
            .expect("usage")
            .expect("upper matches");
        let acked = Instant::now();

        // A listen at as_of 0 yields updates after 0, so the first tick's data
        // isn't expected.
        let mut expected = if idle || t == 0 { vec![] } else { vec![update] };
        let deadline = acked + Duration::from_secs(30);
        loop {
            assert!(Instant::now() < deadline, "listener stuck before {}", t + 1);
            let mut done = false;
            for event in listen.fetch_next().await {
                match event {
                    ListenEvent::Updates(u) => {
                        for x in u {
                            assert_eq!(Some(&x), expected.first(), "tick {t}");
                            expected.remove(0);
                        }
                    }
                    ListenEvent::Progress(p) => {
                        assert_eq!(
                            p,
                            Antichain::from_elem(t + 1),
                            "tick {t}: progress in order"
                        );
                        assert!(expected.is_empty(), "tick {t}: progress before data");
                        done = true;
                    }
                }
            }
            if done {
                break;
            }
        }
        latencies.push(acked.elapsed());
    }
    latencies
}

fn summary(name: &str, mut l: Vec<Duration>) -> String {
    l.sort();
    format!(
        "{name}: n={} p50 {:?} p90 {:?} max {:?}",
        l.len(),
        l[l.len() / 2],
        l[l.len() * 9 / 10],
        l.last().expect("some")
    )
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)]
async fn listener_follows_appends_and_idle_ticks() {
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    mz_persist_client::v2::client::set_resolver_url(&format!("http://{addr}"));
    let resolver_client = client(&location, true).await;
    let cfg = ResolverConfig {
        resolver_id: "resolver-listen-test".into(),
        advertise_addr: String::new(),
        control_shard: ShardId::new(),
        partition: "p0".into(),
    };
    let _running = start_and_serve(&resolver_client, cfg, listener)
        .await
        .expect("start");

    let writer = client(&location, true).await;
    let watching = client(&location, true).await;
    let polling = client(&location, false).await;
    let with_watch = lockstep(&writer, &watching, 30).await;
    let without_watch = lockstep(&writer, &polling, 30).await;
    eprintln!("{}", summary("ack-to-listen, watch", with_watch));
    eprintln!("{}", summary("ack-to-listen, polling", without_watch));
}
