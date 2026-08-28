// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the synthetic catalog state toolkit's offline injection.

#![recursion_limit = "256"]

use std::time::Duration;

use anyhow::anyhow;
use mz_catalog::synthetic::{self, GenerateSpec, SyntheticItemKind};
use mz_environmentd::test_util;
use mz_ore::retry::Retry;
use tempfile::TempDir;

const COUNT: u64 = 4;

/// Objects injected while the environment is down come back on the next boot, and cost
/// nothing beyond catalog metadata: no storage collection, and so no dataflow over one.
///
/// Booting at all is half the assertion. Every durable row is re-planned at boot, so a
/// generated `create_sql` that does not plan takes the environment down with it.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_synthetic_tier0_objects_are_metadata_only() {
    let tmpdir = TempDir::new().unwrap();
    let harness = test_util::TestHarness::default()
        .unsafe_mode()
        .data_directory(tmpdir.path());

    // Boot once to create the catalog, and declare the environment disposable. This has
    // to be an `ALTER SYSTEM SET`, not a harness default: the offline gate reads the
    // durable setting, which is the only thing it can see with the environment down.
    {
        let server = harness.clone().start().await;
        let client = server.connect().internal().await.unwrap();
        client
            .batch_execute("ALTER SYSTEM SET enable_synthetic_catalog_state = on")
            .await
            .unwrap();
    }

    harness
        .with_durable_catalog(|tx| {
            let database = tx
                .get_databases()
                .find(|database| database.name == "materialize")
                .ok_or_else(|| anyhow!("no materialize database"))?;
            let schema = tx
                .get_schemas()
                .find(|schema| schema.database_id == Some(database.id) && schema.name == "public")
                .ok_or_else(|| anyhow!("no public schema"))?;
            let cluster_id = tx
                .get_clusters()
                .find(|cluster| cluster.name == "quickstart")
                .map(|cluster| cluster.id);

            for kind in [
                SyntheticItemKind::Table,
                SyntheticItemKind::MaterializedView,
            ] {
                synthetic::generate_objects(
                    tx,
                    &GenerateSpec {
                        kind,
                        count: COUNT,
                        schema_id: schema.id,
                        database_name: database.name.clone(),
                        schema_name: schema.name.clone(),
                        name_prefix: "synthetic".to_string(),
                        columns: 3,
                        cluster_id,
                    },
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();

    let server = harness.start().await;
    let client = server.connect().await.unwrap();

    for relation in ["mz_tables", "mz_materialized_views"] {
        let count: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE name LIKE 'synthetic%'"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, i64::try_from(COUNT).unwrap(), "{relation}");
    }

    // A real table is the positive control for the Tier 0 claim: it is registered after
    // boot, so once its shard is visible, any registration boot made is visible too.
    client
        .batch_execute("CREATE TABLE real_t (a int)")
        .await
        .unwrap();
    Retry::default()
        .max_duration(Duration::from_secs(60))
        .retry_async(|_| async {
            let shards = synthetic_and_real_shards(&client).await;
            match shards {
                (_, 0) => Err("real table has no shard yet"),
                (0, _) => Ok(()),
                (synthetic, _) => panic!("{synthetic} synthetic objects registered a shard"),
            }
        })
        .await
        .unwrap();
}

/// The number of shards registered for synthetic objects and for `real_t`.
async fn synthetic_and_real_shards(client: &tokio_postgres::Client) -> (i64, i64) {
    let row = client
        .query_one(
            "SELECT
                 count(*) FILTER (WHERE o.name LIKE 'synthetic%'),
                 count(*) FILTER (WHERE o.name = 'real_t')
             FROM mz_internal.mz_storage_shards s
             JOIN mz_objects o ON o.id = s.object_id",
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}
