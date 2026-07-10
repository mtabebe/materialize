// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sinks.

use mz_expr::ColumnOrder;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, Timestamp};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::sinks::S3UploadInfo;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeSinkDesc<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    pub from: GlobalId,
    /// TODO(database-issues#7533): Add documentation.
    pub from_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub connection: ComputeSinkConnection<S>,
    /// TODO(database-issues#7533): Add documentation.
    pub with_snapshot: bool,
    /// TODO(database-issues#7533): Add documentation.
    pub up_to: Antichain<Timestamp>,
    /// TODO(database-issues#7533): Add documentation.
    pub non_null_assertions: Vec<usize>,
    /// TODO(database-issues#7533): Add documentation.
    pub refresh_schedule: Option<RefreshSchedule>,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ComputeSinkConnection<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    Subscribe(SubscribeSinkConnection),
    /// TODO(database-issues#7533): Add documentation.
    MaterializedView(MaterializedViewSinkConnection<S>),
    /// A compute sink to do a oneshot copy to s3.
    CopyToS3Oneshot(CopyToS3OneshotSinkConnection),
    /// A compute sink that writes its input into clusterd's local Prometheus
    /// `MetricsRegistry`. See [`PrometheusSinkConnection`].
    Prometheus(PrometheusSinkConnection),
}

impl<S> ComputeSinkConnection<S> {
    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        match self {
            ComputeSinkConnection::Subscribe(_) => "subscribe",
            ComputeSinkConnection::MaterializedView(_) => "materialized_view",
            ComputeSinkConnection::CopyToS3Oneshot(_) => "copy_to_s3_oneshot",
            ComputeSinkConnection::Prometheus(_) => "prometheus",
        }
    }

    /// True if the sink is a subscribe, which is differently recoverable than other sinks.
    pub fn is_subscribe(&self) -> bool {
        if let ComputeSinkConnection::Subscribe(_) = self {
            true
        } else {
            false
        }
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SubscribeSinkConnection {
    /// An ordering for the data in the subscribe.
    pub output: Vec<ColumnOrder>,
}

/// Connection attributes required to do a oneshot copy to s3.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CopyToS3OneshotSinkConnection {
    /// Information specific to the upload.
    pub upload_info: S3UploadInfo,
    /// The AWS connection information to do the writes.
    pub aws_connection: AwsConnection,
    /// The ID of the Connection object, used to generate the External ID when
    /// using AssumeRole with AWS connection.
    pub connection_id: CatalogItemId,
    /// The number of batches the COPY TO output will be divided into
    /// where each worker will process 0 or more batches of data.
    pub output_batch_count: u64,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializedViewSinkConnection<S> {
    /// TODO(database-issues#7533): Add documentation.
    pub value_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub storage_metadata: S,
}

/// The kind of a Prometheus metric emitted by a Prometheus sink.
///
/// Mirrors `mz_catalog`'s `PromKind`, but lives here because it crosses the
/// controller-to-compute wire. Gauges and counters only.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum PromKind {
    /// A value that can move up or down. The operator publishes the latest
    /// value for a label set.
    Gauge,
    /// A monotonic accumulator. The operator publishes the additive fold of
    /// `value * diff` over the input stream.
    Counter,
}

/// A single label column carried by a [`PrometheusSinkConnection`].
///
/// Carries both the resolved column index (how the operator reads the value,
/// positionally) and the Prometheus label name (for the emitted series and for
/// diagnostics). The planner resolves `column_index` against the planned view's
/// `RelationDesc`.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PromLabelSpec {
    /// The Prometheus label name.
    pub name: String,
    /// The index of this label's column in the sink's input row.
    pub column_index: usize,
}

/// A single metric family emitted by a [`PrometheusSinkConnection`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PromValueSpec {
    /// The source column name, kept for diagnostics.
    pub column_name: String,
    /// The index of this value's column in the sink's input row. The operator
    /// reads it positionally.
    pub column_index: usize,
    /// Prometheus metric family name.
    pub metric: String,
    /// Whether this family is a gauge or a counter.
    pub kind: PromKind,
    /// Prometheus `# HELP` text for the family.
    pub help: String,
}

/// Connection attributes for a Prometheus sink.
///
/// The sink writes its input rows into clusterd's local Prometheus
/// `MetricsRegistry`. The operator reads label and value columns positionally,
/// so the order of [`labels`](Self::labels) and [`values`](Self::values) is the
/// wire contract between planning and rendering.
///
/// This mirrors `mz_catalog`'s `BuiltinPrometheusSink` / `PromValue`, but is a
/// separate type living in `compute-types` because it crosses the wire and
/// carries resolved column indices rather than the raw SQL declaration.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PrometheusSinkConnection {
    /// The sink's builtin name, used as the `sink="..."` label on the auxiliary
    /// liveness and error series.
    pub sink_name: String,
    /// Ordered label columns.
    pub labels: Vec<PromLabelSpec>,
    /// One entry per emitted metric family.
    pub values: Vec<PromValueSpec>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `ComputeSinkConnection::Prometheus` survives a serde round-trip. Sink
    /// connections cross the controller-to-compute wire via serde, so this is
    /// the analog of the proto round-trip test the other variants would use.
    #[mz_ore::test]
    fn prometheus_connection_serde_roundtrip() {
        let connection: ComputeSinkConnection =
            ComputeSinkConnection::Prometheus(PrometheusSinkConnection {
                sink_name: "mz_prom_arrangement_sizes".into(),
                labels: vec![
                    PromLabelSpec {
                        name: "object_id".into(),
                        column_index: 0,
                    },
                    PromLabelSpec {
                        name: "object_name".into(),
                        column_index: 1,
                    },
                ],
                values: vec![
                    PromValueSpec {
                        column_name: "size_bytes".into(),
                        column_index: 2,
                        metric: "mz_arrangement_size_bytes".into(),
                        kind: PromKind::Gauge,
                        help: "Size of all arrangements for an object, in bytes.".into(),
                    },
                    PromValueSpec {
                        column_name: "elapsed_seconds".into(),
                        column_index: 3,
                        metric: "mz_dataflow_elapsed_seconds_total".into(),
                        kind: PromKind::Counter,
                        help: "Elapsed worker time.".into(),
                    },
                ],
            });
        assert_eq!(connection.name(), "prometheus");

        let encoded = serde_json::to_string(&connection).expect("serialize");
        let decoded: ComputeSinkConnection = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(connection, decoded);
    }
}
