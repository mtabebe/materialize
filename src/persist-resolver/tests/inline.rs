// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! How big a resolver cycle's manifest shard write gets when many data shards
//! append small batches at once, and how often it outgrows the manifest shard's
//! inline limit, at today's 4 KiB and at the resolver's default.
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
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_resolver::{ResolverConfig, Stats, start_and_serve};
use mz_persist_types::codec_impls::StringSchema;
use timely::progress::Antichain;

const SHARDS: usize = 32;
const ROUNDS: u64 = 10;

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

/// `shard`'s part files (not rollups) in a file blob at `dir`, which writes a
/// key's `/` as `\u{2215}`.
fn part_files(dir: &std::path::Path, shard: ShardId) -> usize {
    std::fs::read_dir(dir)
        .expect("blob dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(&shard.to_string()) && name.contains("\u{2215}p"))
        .count()
}

/// Runs a resolver with the given manifest inline limit while `SHARDS` data shards
/// append one small row each per round, concurrently.
async fn run(
    location: &PersistLocation,
    blob_dir: &std::path::Path,
    addr: std::net::SocketAddr,
    single_max: usize,
) -> (Stats, usize) {
    let resolver_client = client(location).await;
    let cfg = ResolverConfig {
        resolver_id: format!("resolver-inline-{single_max}"),
        advertise_addr: String::new(),
        control_shard: ShardId::new(),
        partition: "p0".into(),
        manifest_inline_single_max_bytes: single_max,
        manifest_inline_total_max_bytes: ResolverConfig::DEFAULT_MANIFEST_INLINE_TOTAL_MAX_BYTES,
    };
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    let running = start_and_serve(&resolver_client, cfg, listener)
        .await
        .expect("start");
    let parts_before = part_files(blob_dir, running.partition_shard);

    let writer = client(location).await;
    let mut writes = Vec::new();
    for _ in 0..SHARDS {
        let shard = ShardId::new();
        assert!(
            writer
                .maybe_register_v2::<String, String, u64, i64>(shard)
                .await
        );
        let write = writer
            .open_writer::<String, String, u64, i64>(
                shard,
                Arc::new(StringSchema),
                Arc::new(StringSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("writer");
        writes.push(write);
    }
    running.stats.lock().expect("lock").write_bytes.clear();
    let before = running.stats.lock().expect("lock").clone();
    for t in 0..ROUNDS {
        let appends = writes.iter_mut().enumerate().map(|(i, write)| async move {
            let update = ((format!("shard{i}-k{t}"), "v".repeat(40)), t, 1i64);
            write
                .compare_and_append(
                    &[update],
                    Antichain::from_elem(t),
                    Antichain::from_elem(t + 1),
                )
                .await
                .expect("usage")
                .expect("upper matches");
        });
        futures::future::join_all(appends).await;
    }
    // Expire the writers while this resolver still serves their data shards.
    for write in writes {
        write.expire().await;
    }
    let mut stats = running.stats.lock().expect("lock").clone();
    stats.manifest_writes -= before.manifest_writes;
    stats.commits -= before.commits;
    stats.spilled_writes -= before.spilled_writes;
    let parts = part_files(blob_dir, running.partition_shard) - parts_before;
    drop(running);
    (stats, parts)
}

fn report(name: &str, single_max: usize, stats: &Stats, parts: usize) -> String {
    let mut bytes = stats.write_bytes.clone();
    bytes.sort();
    let commits_per_write = stats.commits as f64 / stats.manifest_writes.max(1) as f64;
    format!(
        "{name} (manifest inline limit {single_max} B): {} writes, {:.1} commits/write, \
         bytes/write p50 {} p90 {} max {}, {} writes over the limit, {} new part files",
        stats.manifest_writes,
        commits_per_write,
        bytes[bytes.len() / 2],
        bytes[bytes.len() * 9 / 10],
        bytes.last().expect("some"),
        stats.spilled_writes,
        parts,
    )
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)]
async fn manifest_inline_limit_sizing() {
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

    let (today, today_parts) = run(&location, blob_dir.path(), addr, 4096).await;
    let default = ResolverConfig::DEFAULT_MANIFEST_INLINE_SINGLE_MAX_BYTES;
    let (tuned, tuned_parts) = run(&location, blob_dir.path(), addr, default).await;
    eprintln!("{}", report("today's limit", 4096, &today, today_parts));
    eprintln!(
        "{}",
        report("resolver default", default, &tuned, tuned_parts)
    );
    assert!(tuned.spilled_writes <= today.spilled_writes);
}
