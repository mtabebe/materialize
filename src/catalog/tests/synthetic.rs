// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for writing synthetic objects into the durable catalog.

#![recursion_limit = "256"]

use mz_catalog::durable::{
    DurableCatalogState, TestCatalogStateBuilder, Transaction, test_bootstrap_args,
};
use mz_catalog::synthetic::{self, EffectsTier, GenerateSpec, SyntheticItemKind};
use mz_ore::collections::CollectionExt;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistClient;
use mz_sql::session::user::MZ_SYNTHETIC_ROLE_ID;

async fn open_state() -> Box<dyn DurableCatalogState> {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    // A transaction refuses to open until the bootstrap updates have been consumed.
    let _updates = state.sync_to_current_updates().await.unwrap();
    state
}

/// A spec for `count` objects in `materialize.public`, on the first cluster there is.
fn spec(tx: &Transaction, kind: SyntheticItemKind, count: u64) -> GenerateSpec {
    let database = tx
        .get_databases()
        .find(|database| database.name == "materialize")
        .unwrap();
    let schema = tx
        .get_schemas()
        .find(|schema| schema.database_id == Some(database.id) && schema.name == "public")
        .unwrap();
    GenerateSpec {
        kind,
        count,
        schema_id: schema.id,
        database_name: database.name,
        schema_name: schema.name,
        name_prefix: "synthetic".to_string(),
        columns: 2,
        cluster_id: tx.get_clusters().next().map(|cluster| cluster.id),
        tier: EffectsTier::MetadataOnly,
        on: None,
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn test_disposable_env_gate_reads_durable_state() {
    let mut state = open_state().await;
    let mut tx = state.transaction().await.unwrap();

    let err = synthetic::require_disposable_env_durable(&tx).unwrap_err();
    assert!(
        err.to_string().contains("disposable environments"),
        "unexpected error: {err}"
    );

    tx.upsert_system_config("enable_synthetic_catalog_state", "on".to_string())
        .unwrap();
    synthetic::require_disposable_env_durable(&tx).unwrap();
}

/// Ids must come off the shared user-item allocator, or a later real `CREATE` is handed
/// an id a synthetic object already took.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn test_generate_objects_advances_the_id_allocator() {
    let mut state = open_state().await;
    let mut tx = state.transaction().await.unwrap();

    let spec = spec(&tx, SyntheticItemKind::Table, 3);
    let item_ids = synthetic::generate_objects(&mut tx, &spec).unwrap();
    assert_eq!(item_ids.len(), 3);

    let (next_id, _) = tx
        .allocate_user_item_ids(1)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(
        item_ids.iter().all(|id| *id < next_id),
        "{next_id} was handed out again after {item_ids:?}"
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn test_generated_items_are_owned_by_mz_synthetic() {
    let mut state = open_state().await;
    let mut tx = state.transaction().await.unwrap();

    let spec = spec(&tx, SyntheticItemKind::Table, 1);
    let item_id = synthetic::generate_objects(&mut tx, &spec)
        .unwrap()
        .into_element();
    let item = tx.get_item(&item_id).unwrap();

    assert_eq!(item.owner_id, MZ_SYNTHETIC_ROLE_ID);
    assert_eq!(
        item.create_sql,
        format!(
            "CREATE TABLE \"materialize\".\"public\".\"synthetic_{item_id}\" \
             (c0 integer, c1 text)"
        )
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
async fn test_generated_materialized_view_names_its_cluster() {
    let mut state = open_state().await;
    let mut tx = state.transaction().await.unwrap();

    let spec = spec(&tx, SyntheticItemKind::MaterializedView, 1);
    let cluster_id = spec.cluster_id.unwrap();
    let item_id = synthetic::generate_objects(&mut tx, &spec)
        .unwrap()
        .into_element();

    assert_eq!(
        tx.get_item(&item_id).unwrap().create_sql,
        format!(
            "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"synthetic_{item_id}\" \
             IN CLUSTER [{cluster_id}] AS SELECT CAST(0 AS integer) AS c0, \
             CAST('' AS text) AS c1"
        )
    );
}
