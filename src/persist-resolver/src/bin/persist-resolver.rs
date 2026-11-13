// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: runs one resolver serving one partition of v2 persist shards.

use std::net::SocketAddr;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{PersistLocation, ShardId};
use mz_persist_resolver::ResolverConfig;
use tracing::info;

/// The control manifest's shard id when none is given. Every process of an
/// environment has to agree on it, so it can't be random.
const DEFAULT_CONTROL_SHARD: &str = "s00000000-0000-0000-0000-00000000c0de";

#[derive(clap::Parser)]
struct Args {
    /// The persist blob URL of the manifests (and of the v2 shards' data).
    #[clap(long, env = "PERSIST_BLOB_URL")]
    blob_url: SensitiveUrl,
    /// The persist consensus URL of the manifests.
    #[clap(long, env = "PERSIST_CONSENSUS_URL")]
    consensus_url: SensitiveUrl,
    /// Where to serve the resolver's gRPC API.
    #[clap(long, default_value = "127.0.0.1:6890")]
    listen_addr: SocketAddr,
    #[clap(long, default_value = DEFAULT_CONTROL_SHARD)]
    control_shard: ShardId,
    #[clap(long, default_value = "p0")]
    partition: String,
    /// The manifest shards' `persist_inline_writes_single_max_bytes`.
    #[clap(long, default_value_t = ResolverConfig::DEFAULT_MANIFEST_INLINE_SINGLE_MAX_BYTES)]
    manifest_inline_single_max_bytes: usize,
    /// The manifest shards' `persist_inline_writes_total_max_bytes`.
    #[clap(long, default_value_t = ResolverConfig::DEFAULT_MANIFEST_INLINE_TOTAL_MAX_BYTES)]
    manifest_inline_total_max_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args: Args = clap::Parser::parse();
    // This process's own persist clients read and write the manifests, which are
    // v1 shards.
    mz_persist_client::v2::client::disable_for_process();

    let cache = PersistClientCache::new(
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &MetricsRegistry::new(),
        |_, _| PubSubClientConnection::noop(),
    );
    let client = cache
        .open(PersistLocation {
            blob_uri: args.blob_url,
            consensus_uri: args.consensus_url,
        })
        .await?;
    let cfg = ResolverConfig {
        resolver_id: format!("resolver-{}", uuid::Uuid::new_v4()),
        advertise_addr: String::new(),
        control_shard: args.control_shard,
        partition: args.partition,
        manifest_inline_single_max_bytes: args.manifest_inline_single_max_bytes,
        manifest_inline_total_max_bytes: args.manifest_inline_total_max_bytes,
    };
    let listener = tokio::net::TcpListener::bind(args.listen_addr).await?;
    info!(
        "persist resolver serving at http://{}",
        listener.local_addr()?
    );
    let running = mz_persist_resolver::start_and_serve(&client, cfg, listener).await?;
    running.cycle.await;
    anyhow::bail!("resolver stopped: its claim was taken")
}
