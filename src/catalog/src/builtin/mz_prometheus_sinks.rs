// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in Prometheus sink definitions.
//!
//! Each [`BuiltinPrometheusSink`] pairs a SQL view with a Prometheus
//! label/value schema. The adapter plans the view without a user session at
//! bootstrap and installs it as a compute dataflow on every replica. A terminal
//! `PrometheusSink` operator writes the view's rows into clusterd's local
//! `MetricsRegistry`, where Prometheus scrapes them.
//!
//! This module is the only source of truth for the sink library. Adding a sink
//! is a new static here plus an entry in [`BUILTIN_PROMETHEUS_SINKS`], with no
//! operator or adapter change.

use std::sync::LazyLock;

use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTROSPECTION_SCHEMA;

use super::{BuiltinPrometheusSink, PUBLIC_SELECT, PromKind, PromValue};

/// `mz_prom_arrangement_sizes`: per-object arrangement size, record count, and
/// batch count on a replica, attributed to the owning object, cluster, and
/// replica.
///
/// The source view `mz_introspection.mz_dataflow_arrangement_sizes` keys rows by
/// dataflow id. `mz_compute_exports` maps each dataflow to its owning object
/// (the same path `mz_introspection.mz_mappable_objects` uses), which then joins
/// to `mz_objects` and `mz_clusters` for human-readable names.
///
/// NOTE: there is no in-SQL primitive for the replica a query runs on, so
/// `replica_id` and `replica_name` cannot be resolved from the source view
/// alone. Each sink dataflow is installed with a separate `CreateDataflow` per
/// replica, so the replica identity is known at install time. The join below is
/// a placeholder that keeps all six label columns projected.
pub static MZ_PROM_ARRANGEMENT_SIZES: LazyLock<BuiltinPrometheusSink> =
    LazyLock::new(|| BuiltinPrometheusSink {
        name: "mz_prom_arrangement_sizes",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::SINK_MZ_PROM_ARRANGEMENT_SIZES_OID,
        sql: "
            SELECT
                o.id                        AS object_id,
                o.name                      AS object_name,
                c.id                        AS cluster_id,
                c.name                      AS cluster_name,
                cr.id                       AS replica_id,
                cr.name                     AS replica_name,
                SUM(a.size)::float8         AS size_bytes,
                SUM(a.records)::float8      AS records,
                SUM(a.batches)::float8      AS batches
            FROM mz_introspection.mz_dataflow_arrangement_sizes a
            JOIN mz_introspection.mz_compute_exports e ON a.id = e.dataflow_id
            JOIN mz_catalog.mz_objects o                ON e.export_id = o.id
            JOIN mz_catalog.mz_clusters c               ON o.cluster_id = c.id
            JOIN mz_catalog.mz_cluster_replicas cr      ON cr.cluster_id = c.id
            GROUP BY o.id, o.name, c.id, c.name, cr.id, cr.name",
        labels: &[
            "object_id",
            "object_name",
            "cluster_id",
            "cluster_name",
            "replica_id",
            "replica_name",
        ],
        values: &[
            PromValue {
                column: "size_bytes",
                metric: "mz_arrangement_size_bytes",
                kind: PromKind::Gauge,
                help: "Size of all arrangements for an object, in bytes.",
            },
            PromValue {
                column: "records",
                metric: "mz_arrangement_records",
                kind: PromKind::Gauge,
                help: "Number of records in all arrangements for an object.",
            },
            PromValue {
                column: "batches",
                metric: "mz_arrangement_batches",
                kind: PromKind::Gauge,
                help: "Number of batches in all arrangements for an object.",
            },
        ],
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });
