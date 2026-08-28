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

use mz_catalog::synthetic::{self, GenerateRequest, SyntheticItemKind};
use mz_environmentd::test_util::{self, TestServerWithRuntime};
use mz_ore::retry::Retry;
use reqwest::StatusCode;
use tempfile::TempDir;

const COUNT: u64 = 4;

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
