// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PROTOTYPE: a read-only HTTP view of the resolver's in-memory manifests, as
//! JSON, for demos and debugging.
//!
//! `GET /views` is the whole partition, `GET /views/{shard}` one data shard.
//! Requests go through the cycle like a `Fetch`, so a payload is a view the
//! resolver had between cycles.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use mz_persist_client::ShardId;
use mz_persist_client::v2::{ManifestKey, ProtoHollowBatch, ProtoU64Antichain, frontier};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use crate::Request;
use crate::view::View;

/// The partition's view: its manifest shards and times, its claim, its upper
/// groups, and every data shard assigned to it.
pub fn partition_json(view: &View, control_shard: &ShardId, partition_shard: &ShardId) -> Value {
    let shards: Vec<Value> = view
        .rows
        .keys()
        .filter_map(|k| match k {
            ManifestKey::Shard(s) => shard_json(view, s),
            _ => None,
        })
        .collect();
    let members = view.members();
    let groups: Vec<Value> = view
        .groups()
        .into_iter()
        .map(|(g, upper)| {
            let members: Vec<String> = members
                .get(&g)
                .map(|m| m.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default();
            json!({"group": g, "upper": upper.elements(), "members": members})
        })
        .collect();
    json!({
        "partition": view.partition,
        "seqno": view.seqno(),
        "control_manifest": {"shard": control_shard.to_string(), "time": view.control_time},
        "partition_manifest": {"shard": partition_shard.to_string(), "time": view.partition_time},
        "control_at": view.control_at(),
        "claim": view.claim().map(|c| json!({"resolver": c.resolver, "token": c.token, "addr": c.addr})),
        "upper_groups": groups,
        "data_shards": shards,
    })
}

/// One data shard's view, or None if it isn't registered. Frontiers are lists
/// of times, and `[]` is the empty frontier (closed, or finalized). Batch part
/// keys are left out, only counted.
pub fn shard_json(view: &View, shard: &ShardId) -> Option<Value> {
    let meta = view.shard_meta(shard)?;
    let intents: Vec<Value> = view
        .intents(shard)
        .into_iter()
        .map(|(reader, i)| {
            json!({
                "reader": reader,
                "frontier": frontier(&i.frontier).elements(),
                "opaque": i.opaque,
            })
        })
        .collect();
    let batches: Vec<Value> = view
        .batches(shard)
        .into_values()
        .map(|b| batch_json(&b))
        .collect();
    Some(json!({
        "shard": shard.to_string(),
        "finalized": meta.finalized,
        "upper": view.upper(shard).map(|u| u.elements().to_vec()),
        "upper_group": view.group_of(shard),
        "since": view.readable(shard).map(|f| f.elements().to_vec()),
        "intents": intents,
        "batches": batches,
    }))
}

fn batch_json(b: &ProtoHollowBatch) -> Value {
    let desc = b.desc.clone().unwrap_or_default();
    json!({
        "lower": u64s(&desc.lower),
        "upper": u64s(&desc.upper),
        "since": u64s(&desc.since),
        "updates": b.len,
        "parts": b.parts.len(),
    })
}

fn u64s(f: &Option<ProtoU64Antichain>) -> Vec<u64> {
    f.as_ref()
        .map(|f| f.elements.iter().map(|x| x.cast_unsigned()).collect())
        .unwrap_or_default()
}

async fn inspect(tx: &mpsc::UnboundedSender<Request>, shard: Option<ShardId>) -> Response {
    let (reply, rx) = oneshot::channel();
    if tx.send(Request::Inspect(shard, reply)).is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "resolver stopped").into_response();
    }
    match rx.await {
        Ok(Some(v)) => {
            let mut body = serde_json::to_string_pretty(&v).expect("json values serialize");
            body.push('\n');
            ([("content-type", "application/json")], body).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "not a data shard\n").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "resolver stopped").into_response(),
    }
}

/// Serves the debug endpoint on `listener` until the task is dropped.
pub async fn serve_http(
    tx: mpsc::UnboundedSender<Request>,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    let app = axum::Router::new()
        .route(
            "/views",
            axum::routing::get(async |State(tx)| inspect(&tx, None).await),
        )
        .route(
            "/views/{shard}",
            axum::routing::get(async |State(tx), Path(shard): Path<String>| {
                match shard.parse::<ShardId>() {
                    Ok(shard) => inspect(&tx, Some(shard)).await,
                    Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")).into_response(),
                }
            }),
        )
        .with_state(tx);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use mz_persist_client::v2::{
        OpKind, ProtoAppend, ProtoCodecs, ProtoFrontier, ProtoOp, ProtoRegister,
        ProtoU64Description,
    };

    use super::*;

    #[mz_ore::test]
    fn json_shows_shard_upper_and_batches() {
        let shard = ShardId::new();
        let op = |kind| ProtoOp {
            shard: shard.to_string(),
            kind: Some(kind),
        };
        let t = |t: i64| Some(ProtoU64Antichain { elements: vec![t] });
        let batch = ProtoHollowBatch {
            desc: Some(ProtoU64Description {
                lower: t(0),
                upper: t(5),
                since: t(0),
            }),
            len: 3,
            ..Default::default()
        };
        let mut view = View::new("p0".into());
        view.commit(&[
            op(OpKind::Register(ProtoRegister {
                codecs: Some(ProtoCodecs::default()),
            })),
            op(OpKind::Append(ProtoAppend {
                batch: Some(batch),
                expected: Some(ProtoFrontier { elements: vec![0] }),
                new: Some(ProtoFrontier { elements: vec![5] }),
            })),
        ])
        .expect("commit applies");

        let json = partition_json(&view, &ShardId::new(), &ShardId::new());
        let shards = json["data_shards"].as_array().expect("array");
        assert_eq!(shards.len(), 1);
        let s = &shards[0];
        assert_eq!(s["shard"], shard.to_string());
        assert_eq!(s["upper"], json!([5]));
        assert_eq!(
            s["batches"],
            json!([{"lower": [0], "upper": [5], "since": [0], "updates": 3, "parts": 0}])
        );
        assert_eq!(s["intents"][0]["reader"], "sentinel");
        assert_eq!(shard_json(&view, &ShardId::new()), None);
    }
}
