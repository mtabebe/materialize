// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the synthetic catalog state toolkit.

#![recursion_limit = "256"]

use std::net::SocketAddr;
use std::time::Duration;

use mz_catalog::synthetic::{
    self, EffectsTier, GenerateRequest, HistoryRequest, StatsRequest, SyntheticHistoryKind,
    SyntheticItemKind,
};
use mz_environmentd::test_util::{self, TestServerWithRuntime};
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use reqwest::StatusCode;
use tempfile::TempDir;

const COUNT: u64 = 4;

/// An object id nothing in the catalog uses, so the injected history has no object.
const SYNTHETIC_SINK_ID: &str = "u9999";

/// The default `keep_n_sink_status_history_entries`.
const RETAINED_ROWS: usize = 5;

const MESSAGES_STAGED: u64 = 12_345;

/// How many one-second retention windows to hold the seeded statistics through.
const RETENTION_WINDOWS: usize = 10;

/// Objects injected while the environment is down come back on the next boot, and cost
/// nothing beyond catalog metadata: no storage collection, and so no dataflow over one.
///
/// Booting at all is half the assertion. Every durable row is re-planned at boot, so a
/// generated `create_sql` that does not plan takes the environment down with it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_objects_injected_offline() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());

    {
        let server = harness.clone().start_blocking();
        declare_disposable(&server);
    }

    // The environment has to be down for this, so it gets a runtime of its own rather
    // than borrowing one from a server.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(harness.with_durable_catalog(|tx| {
            for kind in [
                SyntheticItemKind::Table,
                SyntheticItemKind::MaterializedView,
            ] {
                let spec = request(kind).resolve(tx)?;
                synthetic::generate_objects(tx, &spec)?;
            }
            Ok(())
        }))
        .unwrap();
    drop(runtime);

    let server = harness.start_blocking();
    assert_metadata_only(&server);
}

/// The same objects, created into a running environment, appear without a restart and
/// are just as inert. Restarting then runs them through the boot path, so the online and
/// offline halves of the toolkit converge on the same rows.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_objects_injected_online() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());

    {
        let server = harness.clone().start_blocking();
        declare_disposable(&server);
        for kind in [
            SyntheticItemKind::Table,
            SyntheticItemKind::MaterializedView,
        ] {
            let response = inject(server.internal_http_local_addr(), kind);
            assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
        }
        assert_metadata_only(&server);
    }

    let server = harness.start_blocking();
    assert_metadata_only(&server);
}

/// Injection needs both gates. Unsafe mode says "this is a debug build", the disposable
/// declaration says "this environment in particular".
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_injection_requires_both_gates() {
    let server = test_util::TestHarness::default().start_blocking();
    let response = inject(server.internal_http_local_addr(), SyntheticItemKind::Table);
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().unwrap().contains("not supported"));
    drop(server);

    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .start_blocking();
    let response = inject(server.internal_http_local_addr(), SyntheticItemKind::Table);
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().unwrap().contains("disposable environments"));
}

fn request(kind: SyntheticItemKind) -> GenerateRequest {
    GenerateRequest {
        kind,
        count: COUNT,
        database: "materialize".to_string(),
        schema: "public".to_string(),
        name_prefix: "synthetic".to_string(),
        columns: 3,
        cluster: "quickstart".to_string(),
        tier: EffectsTier::MetadataOnly,
        on: None,
    }
}

/// Declares the environment disposable durably, which is what the offline path can see.
fn declare_disposable(server: &TestServerWithRuntime) {
    server
        .connect_internal(postgres::NoTls)
        .unwrap()
        .batch_execute("ALTER SYSTEM SET enable_synthetic_catalog_state = on")
        .unwrap();
}

fn inject(addr: SocketAddr, kind: SyntheticItemKind) -> reqwest::blocking::Response {
    reqwest::blocking::Client::new()
        .post(format!(
            "http://{addr}/api/catalog/inject-synthetic-objects"
        ))
        .header("x-materialize-user", "mz_system")
        .json(&request(kind))
        .send()
        .unwrap()
}

fn assert_metadata_only(server: &TestServerWithRuntime) {
    let mut client = server.connect(postgres::NoTls).unwrap();

    for relation in ["mz_tables", "mz_materialized_views"] {
        let count: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE name LIKE 'synthetic%'"),
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(count, i64::try_from(COUNT).unwrap(), "{relation}");
    }

    // A real table is the positive control: it is registered last, so once its shard is
    // visible, any shard the synthetic objects registered is visible too.
    client.batch_execute("CREATE TABLE real_t (a int)").unwrap();
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| {
            let row = client
                .query_one(
                    "SELECT
                         count(*) FILTER (WHERE o.name LIKE 'synthetic%'),
                         count(*) FILTER (WHERE o.name = 'real_t')
                     FROM mz_internal.mz_storage_shards s
                     JOIN mz_objects o ON o.id = s.object_id",
                    &[],
                )
                .unwrap();
            match (row.get::<_, i64>(0), row.get::<_, i64>(1)) {
                (_, 0) => Err("real table has no shard yet"),
                (0, _) => Ok(()),
                (synthetic, _) => panic!("{synthetic} synthetic objects registered a shard"),
            }
        })
        .unwrap();
    client.batch_execute("DROP TABLE real_t").unwrap();
}

/// A run of identical statuses for an object that does not exist lands in full and
/// survives a restart. Nothing retracts an append-only history, which is what makes it a
/// cheap way to model a stuck object without the object.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_history_injected_online() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());

    {
        let server = harness.clone().start_blocking();
        declare_disposable(&server);

        let response = inject_history(
            server.internal_http_local_addr(),
            &history_request(vec!["stalled"; RETAINED_ROWS]),
        );
        assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
        assert_history_rows(&server);

        // One row past the retention window would be truncated at the next boot, quietly
        // changing the state being modelled.
        let response = inject_history(
            server.internal_http_local_addr(),
            &history_request(vec!["stalled"; RETAINED_ROWS + 1]),
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response.text().unwrap().contains("keeps per object"));
    }

    let server = harness.start_blocking();
    assert_history_rows(&server);
}

fn history_request(statuses: Vec<&str>) -> HistoryRequest {
    HistoryRequest {
        kind: SyntheticHistoryKind::Sink,
        object_id: SYNTHETIC_SINK_ID.to_string(),
        statuses: statuses.into_iter().map(str::to_string).collect(),
        error: Some("synthetic".to_string()),
    }
}

fn inject_history(addr: SocketAddr, request: &HistoryRequest) -> reqwest::blocking::Response {
    reqwest::blocking::Client::new()
        .post(format!(
            "http://{addr}/api/catalog/inject-synthetic-history"
        ))
        .header("x-materialize-user", "mz_system")
        .json(request)
        .send()
        .unwrap()
}

fn assert_history_rows(server: &TestServerWithRuntime) {
    let mut client = server.connect(postgres::NoTls).unwrap();

    // The append goes through the collection manager, so it lands asynchronously.
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| {
            let rows: i64 = client
                .query_one(
                    "SELECT count(*) FROM mz_internal.mz_sink_status_history WHERE sink_id = $1",
                    &[&SYNTHETIC_SINK_ID],
                )
                .unwrap()
                .get(0);
            (rows == i64::try_from(RETAINED_ROWS).unwrap())
                .then_some(())
                .ok_or(rows)
        })
        .unwrap();

    let sinks: i64 = client
        .query_one(
            "SELECT count(*) FROM mz_sinks WHERE id = $1",
            &[&SYNTHETIC_SINK_ID],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        sinks, 0,
        "the history is supposed to have no sink behind it"
    );
}

/// Statistics seeded for a sink that produces none of its own show up in the aggregating
/// view and stay there.
///
/// Staying is the interesting half: the scraper evicts an entry that has stopped being
/// updated, and a synthetic entry is quiet by construction, so it survives only because
/// the scraper is told to keep it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_stats_injected_online() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        // Short enough that a quiet entry would be gone well before the loop below ends.
        .with_system_parameter_default(
            "storage_statistics_retention_duration".to_string(),
            "1s".to_string(),
        )
        .with_system_parameter_default("storage_statistics_interval".to_string(), "1s".to_string())
        .start_blocking();
    declare_disposable(&server);

    let response = inject_stats(server.internal_http_local_addr());
    assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());

    let mut client = server.connect(postgres::NoTls).unwrap();
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| match seeded_messages(&mut client) {
            Some(MESSAGES_STAGED) => Ok(()),
            other => Err(format!("{other:?}")),
        })
        .unwrap();

    for _ in 0..RETENTION_WINDOWS {
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(
            seeded_messages(&mut client),
            Some(MESSAGES_STAGED),
            "the seeded statistics were evicted"
        );
    }

    let sinks: i64 = client
        .query_one(
            "SELECT count(*) FROM mz_sinks WHERE id = $1",
            &[&SYNTHETIC_SINK_ID],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        sinks, 0,
        "the statistics are supposed to have no sink behind them"
    );
}

fn inject_stats(addr: SocketAddr) -> reqwest::blocking::Response {
    let request = StatsRequest::Sink {
        object_id: SYNTHETIC_SINK_ID.to_string(),
        replica_id: "u1".to_string(),
        messages_staged: MESSAGES_STAGED,
        messages_committed: MESSAGES_STAGED,
        bytes_staged: 4096,
        bytes_committed: 4096,
    };
    reqwest::blocking::Client::new()
        .post(format!("http://{addr}/api/catalog/inject-synthetic-stats"))
        .header("x-materialize-user", "mz_system")
        .json(&request)
        .send()
        .unwrap()
}

fn seeded_messages(client: &mut postgres::Client) -> Option<u64> {
    client
        .query_opt(
            "SELECT messages_staged::bigint FROM mz_internal.mz_sink_statistics WHERE id = $1",
            &[&SYNTHETIC_SINK_ID],
        )
        .unwrap()
        .map(|row| u64::try_from(row.get::<_, i64>(0)).expect("counters are non-negative"))
}

/// A Tier 1 object pays what a real one pays: its table registers a storage collection
/// and its index ships a dataflow to a replica.
///
/// This is the same generator and the same boot path as the Tier 0 test above. All that
/// differs is the owner, which is what every effects seam reads.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_tier1_objects_pay_real_effects() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());

    {
        let server = harness.clone().start_blocking();
        declare_disposable(&server);
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(harness.with_durable_catalog(|tx| {
            // The empty table the index hydrates over. An index over an empty input still
            // pays the per-dataflow bootstrap handshake, which is the cost being modelled.
            let table = tier1_request(SyntheticItemKind::Table, None).resolve(tx)?;
            let table_id = synthetic::generate_objects(tx, &table)?.into_element();
            let on = synthetic::synthetic_item_name(&table.name_prefix, table_id);

            let index = tier1_request(SyntheticItemKind::Index, Some(on)).resolve(tx)?;
            synthetic::generate_objects(tx, &index)?;
            Ok(())
        }))
        .unwrap();
    drop(runtime);

    let server = harness.start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();

    Retry::default()
        .max_duration(Duration::from_secs(120))
        .retry(|_| {
            let row = client
                .query_one(
                    "SELECT
                         (SELECT count(*)
                          FROM mz_internal.mz_storage_shards s
                          JOIN mz_objects o ON o.id = s.object_id
                          WHERE o.name LIKE 'tier1%'),
                         (SELECT count(*)
                          FROM mz_catalog.mz_cluster_replica_frontiers f
                          JOIN mz_internal.mz_object_global_ids g ON g.global_id = f.object_id
                          JOIN mz_objects o ON o.id = g.id
                          WHERE o.name LIKE 'tier1%')",
                    &[],
                )
                .unwrap();
            match (row.get::<_, i64>(0), row.get::<_, i64>(1)) {
                (1, 1) => Ok(()),
                counts => Err(format!("{counts:?}")),
            }
        })
        .unwrap();
}

fn tier1_request(kind: SyntheticItemKind, on: Option<String>) -> GenerateRequest {
    GenerateRequest {
        kind,
        count: 1,
        database: "materialize".to_string(),
        schema: "public".to_string(),
        name_prefix: "tier1".to_string(),
        columns: 3,
        cluster: "quickstart".to_string(),
        tier: EffectsTier::ShippedOverEmpty,
        on,
    }
}

/// Purge takes the environment back to where it started, and running it again is a no-op.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_purge_round_trips() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());
    let server = harness.clone().start_blocking();
    declare_disposable(&server);

    let mut client = server.connect(postgres::NoTls).unwrap();
    let objects_before = object_count(&mut client);

    for kind in [
        SyntheticItemKind::Table,
        SyntheticItemKind::MaterializedView,
    ] {
        let response = inject(server.internal_http_local_addr(), kind);
        assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
    }
    let response = inject_history(
        server.internal_http_local_addr(),
        &history_request(vec!["stalled"; RETAINED_ROWS]),
    );
    assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
    let response = inject_stats(server.internal_http_local_addr());
    assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
    assert!(object_count(&mut client) > objects_before);

    let report = purge(server.internal_http_local_addr());
    assert_eq!(
        report["objects"].as_u64(),
        Some(2 * COUNT),
        "unexpected report: {report}"
    );
    assert_eq!(
        report["history_rows"].as_u64(),
        Some(u64::try_from(RETAINED_ROWS).unwrap())
    );
    assert_eq!(report["statistics"].as_u64(), Some(1));
    assert_eq!(object_count(&mut client), objects_before);

    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry(|_| {
            let rows: i64 = client
                .query_one(
                    "SELECT count(*) FROM mz_internal.mz_sink_status_history WHERE sink_id = $1",
                    &[&SYNTHETIC_SINK_ID],
                )
                .unwrap()
                .get(0);
            (rows == 0).then_some(()).ok_or(rows)
        })
        .unwrap();

    let report = purge(server.internal_http_local_addr());
    assert_eq!(report["objects"].as_u64(), Some(0), "{report}");
    assert_eq!(report["history_rows"].as_u64(), Some(0));
    assert_eq!(report["statistics"].as_u64(), Some(0));
}

fn object_count(client: &mut postgres::Client) -> i64 {
    client
        .query_one("SELECT count(*) FROM mz_objects", &[])
        .unwrap()
        .get(0)
}

fn purge(addr: SocketAddr) -> serde_json::Value {
    let response = reqwest::blocking::Client::new()
        .post(format!("http://{addr}/api/catalog/purge-synthetic"))
        .header("x-materialize-user", "mz_system")
        .send()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
    response.json().unwrap()
}

/// The object limits bound what a user creates, not what the toolkit models: seeding a
/// catalog larger than `max_tables` allows is the point of it.
///
/// Synthetic objects are invisible to the limits in both directions. Counting them when
/// creating would refuse the batch; counting them afterwards would refuse a real `CREATE`
/// because of a fleet that only exists to be modelled. The offline path writes durable
/// rows and never reaches the check at all, so this is also what keeps the two front-ends
/// agreeing about the same batch.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_objects_ignore_object_limits() {
    let server = test_util::TestHarness::default()
        .unsafe_mode()
        .with_system_parameter_default("max_tables".to_string(), "1".to_string())
        .start_blocking();
    declare_disposable(&server);

    let response = inject(server.internal_http_local_addr(), SyntheticItemKind::Table);
    assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());

    let mut client = server.connect(postgres::NoTls).unwrap();
    let count: i64 = client
        .query_one(
            "SELECT count(*) FROM mz_tables WHERE name LIKE 'synthetic%'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(count, i64::try_from(COUNT).unwrap());

    // A real table still fits: the synthetic fleet does not crowd it out.
    client.batch_execute("CREATE TABLE real_t (a int)").unwrap();

    // And the limit still bounds real objects, which is the half that must not change.
    let err = client
        .batch_execute("CREATE TABLE real_t2 (a int)")
        .unwrap_err();
    let message = err.as_db_error().expect("a database error").message();
    assert!(
        message.contains("max_tables"),
        "unexpected error: {message}"
    );
}

/// Purging after a restart is a different path from purging what this session created.
///
/// Boot is what puts the objects through the durable path, and dropping one whose storage
/// collection was never registered has to stay as much of a no-op as creating it was.
/// Naming it to the storage controller panics the coordinator, which a same-session purge
/// never reaches.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_synthetic_purge_after_restart() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());

    let objects_before = {
        let server = harness.clone().start_blocking();
        declare_disposable(&server);
        let mut client = server.connect(postgres::NoTls).unwrap();
        let objects_before = object_count(&mut client);

        for kind in [
            SyntheticItemKind::Table,
            SyntheticItemKind::MaterializedView,
        ] {
            let response = inject(server.internal_http_local_addr(), kind);
            assert_eq!(response.status(), StatusCode::OK, "{:?}", response.text());
        }
        objects_before
    };

    let server = harness.start_blocking();
    let report = purge(server.internal_http_local_addr());
    assert_eq!(
        report["objects"].as_u64(),
        Some(2 * COUNT),
        "unexpected report: {report}"
    );

    let mut client = server.connect(postgres::NoTls).unwrap();
    assert_eq!(object_count(&mut client), objects_before);
}
