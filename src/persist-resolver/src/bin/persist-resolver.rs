// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: runs one resolver serving one partition of v2 persist shards.

use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{PersistLocation, ShardId};
use mz_persist_resolver::{LatencyHist, ResolverConfig, Stats};
use prometheus::Encoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    /// Where to serve the read-only JSON debug endpoint (`/views`), if anywhere.
    #[clap(long)]
    http_listen_addr: Option<SocketAddr>,
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
    /// Where to serve Prometheus metrics (`GET /metrics`): this process's
    /// persist metrics and the resolver's cycle stats.
    #[clap(long)]
    metrics_listen_addr: Option<SocketAddr>,
    /// Whether to keep data shards whose uppers move together in upper groups.
    /// `--upper-groups=false` keeps every upper on its own row.
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    upper_groups: bool,
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

    let registry = MetricsRegistry::new();
    let cache = PersistClientCache::new(
        PersistConfig::new_default_configs(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &registry,
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
        upper_groups: args.upper_groups,
    };
    let listener = tokio::net::TcpListener::bind(args.listen_addr).await?;
    info!(
        "persist resolver serving at http://{}",
        listener.local_addr()?
    );
    let running = mz_persist_resolver::start_and_serve(&client, cfg, listener).await?;
    if let Some(addr) = args.metrics_listen_addr {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        info!(
            "persist resolver metrics at http://{}/metrics",
            listener.local_addr()?
        );
        mz_ore::task::spawn(
            || "persist-resolver-metrics",
            serve_metrics(listener, registry, Arc::clone(&running.stats)),
        );
    }
    let _http = match args.http_listen_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            info!("debug view at http://{}/views", listener.local_addr()?);
            let serve = mz_persist_resolver::debug::serve_http(running.tx.clone(), listener);
            Some(mz_ore::task::spawn(|| "persist-resolver-http", serve).abort_on_drop())
        }
        None => None,
    };
    running.cycle.await;
    anyhow::bail!("resolver stopped: its claim was taken")
}

/// Answers every connection with the metrics, whatever it asked for: enough for
/// a scraper, without an HTTP server dependency.
async fn serve_metrics(
    listener: tokio::net::TcpListener,
    registry: MetricsRegistry,
    stats: Arc<Mutex<Stats>>,
) {
    loop {
        let Ok((mut conn, _)) = listener.accept().await else {
            continue;
        };
        let mut body = Vec::new();
        if let Err(e) = prometheus::TextEncoder::new().encode(&registry.gather(), &mut body) {
            tracing::warn!("encoding metrics: {e}");
        }
        let stats = stats.lock().expect("lock").clone();
        body.extend(render_stats(&stats).into_bytes());
        let mut buf = [0u8; 4096];
        let _ = conn.read(&mut buf).await;
        let head = format!(
            "HTTP/1.0 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let _ = conn.write_all(head.as_bytes()).await;
        let _ = conn.write_all(&body).await;
    }
}

fn render_stats(stats: &Stats) -> String {
    let mut out = String::new();
    for (name, value) in [
        ("cycles", stats.cycles),
        ("commits", stats.commits),
        ("manifest_writes", stats.manifest_writes),
        ("multi_writes", stats.multi_writes),
        ("lost_writes", stats.lost_writes),
        ("rows_written", stats.rows_written),
        ("encoded_bytes_written", stats.encoded_bytes_written),
        ("spilled_writes", stats.spilled_writes),
        ("watch_requests", stats.watch_requests),
        ("fetch_requests", stats.fetch_requests),
    ] {
        writeln!(out, "# TYPE mz_persist_resolver_{name}_total counter").expect("infallible");
        writeln!(out, "mz_persist_resolver_{name}_total {value}").expect("infallible");
    }
    render_hist(
        &mut out,
        "mz_persist_resolver_commit_seconds",
        &stats.commit_latency,
    );
    render_hist(
        &mut out,
        "mz_persist_resolver_manifest_write_seconds",
        &stats.write_latency,
    );
    out
}

fn render_hist(out: &mut String, name: &str, hist: &LatencyHist) {
    writeln!(out, "# TYPE {name} histogram").expect("infallible");
    let mut cumulative = 0;
    for (i, count) in hist.counts.iter().enumerate() {
        cumulative += count;
        // Bucket `i` holds observations below 2^i us.
        let le = f64::from(1u32 << i) / 1e6;
        writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}").expect("infallible");
    }
    writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}").expect("infallible");
    writeln!(out, "{name}_sum {}", hist.sum_micros as f64 / 1e6).expect("infallible");
    writeln!(out, "{name}_count {cumulative}").expect("infallible");
}
