// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Session-less planning of builtin Prometheus sinks.
//!
//! A [`BuiltinPrometheusSink`] is a SQL view plus a label/value schema. To ship
//! it to a replica we plan its view SQL through the real planner at bootstrap,
//! with no user session (the same session-less path builtin materialized views
//! use), then lower the sink's schema against the planned view's
//! [`RelationDesc`], enforcing the column contract from the catalog type.
//!
//! The sink's input includes per-cluster logging arrangements, whose
//! `GlobalId`s differ per cluster, so the final per-replica `DataflowDescription`
//! with cluster-specific import bindings is assembled at install time. What is
//! cluster-agnostic is the planned view expression, its `RelationDesc`, and the
//! lowered [`PrometheusSinkConnection`], held in [`PlannedPrometheusSink`].

use mz_adapter_types::dyncfgs::ENABLE_PROMETHEUS_SINKS;
use mz_catalog::builtin::{BuiltinPrometheusSink, PromKind as CatalogPromKind};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::sinks::{
    ComputeSinkConnection, PromKind, PromLabelSpec, PromValueSpec, PrometheusSinkConnection,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::optimize::OverrideFrom;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::plan::{HirRelationExpr, Plan};

use crate::AdapterError;
use crate::catalog::CatalogState;
use crate::coord::Coordinator;
use crate::optimize::{Optimize, OptimizerConfig, materialized_view};

/// A builtin Prometheus sink after session-less planning.
///
/// Cluster-agnostic: the planned view expression, its `RelationDesc`, and the
/// lowered connection carrying resolved column indices. The coordinator turns
/// this into a per-replica `DataflowDescription`.
#[derive(Debug, Clone)]
pub struct PlannedPrometheusSink {
    /// The sink's builtin name.
    pub name: String,
    /// The planned (unoptimized) view expression. The coordinator optimizes it
    /// against a target cluster to bind the per-cluster logging imports.
    pub expr: HirRelationExpr,
    /// The view's output schema.
    pub desc: RelationDesc,
    /// The lowered connection with resolved column indices.
    pub connection: PrometheusSinkConnection,
}

/// Plans a single builtin Prometheus sink session-lessly and lowers its schema.
///
/// Runs the planner on the sink's `CREATE VIEW` SQL with the system session,
/// builds the view's `RelationDesc`, and lowers the label/value schema against
/// it, enforcing the column contract. A contract violation is a loud error
/// naming the sink and the offending column.
pub fn plan_builtin_prometheus_sink(
    catalog_state: &CatalogState,
    builtin: &BuiltinPrometheusSink,
) -> Result<PlannedPrometheusSink, AdapterError> {
    let create_sql = builtin.create_sql();
    let conn_catalog = catalog_state.for_system_session();
    let (plan, _resolved_ids) = CatalogState::parse_plan(&create_sql, None, &conn_catalog)?;

    let Plan::CreateView(plan) = plan else {
        return Err(AdapterError::Internal(format!(
            "prometheus sink {}: SQL did not plan as a view",
            builtin.name
        )));
    };
    let view = plan.view;

    // The view's output schema. HIR column types carry the scalar types the
    // contract check needs; optimization only refines nullability, which the
    // contract does not depend on (the SQL author guarantees non-null labels).
    let typ = view.expr.typ(&[], &std::collections::BTreeMap::new());
    let desc = RelationDesc::new(typ, view.column_names.iter().cloned());

    let connection = lower_prometheus_sink(builtin, &desc)?;

    Ok(PlannedPrometheusSink {
        name: builtin.name.to_string(),
        expr: view.expr,
        desc,
        connection,
    })
}

/// Plans every registered builtin Prometheus sink. On a contract or planning
/// failure the sink is skipped with a logged error rather than aborting
/// bootstrap: a malformed sink must not take down `environmentd`. The contract
/// itself is validated loudly in CI by the catalog and planning tests.
pub fn plan_all_prometheus_sinks(catalog_state: &CatalogState) -> Vec<PlannedPrometheusSink> {
    let mut planned = Vec::new();
    for builtin in mz_catalog::builtin::BUILTIN_PROMETHEUS_SINKS.iter() {
        match plan_builtin_prometheus_sink(catalog_state, builtin) {
            Ok(sink) => planned.push(sink),
            Err(err) => {
                tracing::error!(
                    sink = builtin.name,
                    "failed to plan builtin prometheus sink: {err}"
                );
            }
        }
    }
    planned
}

impl Coordinator {
    /// Installs the builtin Prometheus sinks on a newly created replica.
    ///
    /// A no-op unless the `enable_prometheus_sinks` flag is on. Skips replicas
    /// with introspection disabled, whose `mz_internal.*` logging arrangements
    /// the sinks read do not exist. Ships one `CreateDataflow` per sink to this
    /// replica. Teardown comes for free when the replica drops and its
    /// dataflows are cancelled, dropping the operator's lifecycle token.
    pub(crate) async fn install_prometheus_sinks_on_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        introspection_enabled: bool,
    ) {
        if !ENABLE_PROMETHEUS_SINKS.get(self.catalog().system_config().dyncfgs()) {
            return;
        }
        if !introspection_enabled {
            tracing::debug!(
                %cluster_id, %replica_id,
                "skipping prometheus sinks: replica has introspection disabled"
            );
            return;
        }

        // Planned once here per install. The flag is off by default, so this
        // runs only for replicas created while the feature is enabled.
        for sink in plan_all_prometheus_sinks(self.catalog().state()) {
            match self.build_prometheus_sink_dataflow(cluster_id, &sink) {
                Ok(df_desc) => {
                    self.ship_dataflow(df_desc, cluster_id, Some(replica_id))
                        .await;
                }
                Err(err) => {
                    tracing::error!(
                        sink = %sink.name, %cluster_id, %replica_id,
                        "failed to build prometheus sink dataflow: {err}"
                    );
                }
            }
        }
    }

    /// Optimizes a planned Prometheus sink into a per-cluster
    /// `DataflowDescription` with a `Prometheus` sink terminal, reusing the
    /// materialized-view optimizer with an overridden sink connection. The
    /// global optimization step binds the cluster's logging index imports.
    fn build_prometheus_sink_dataflow(
        &mut self,
        cluster_id: ClusterId,
        sink: &PlannedPrometheusSink,
    ) -> Result<DataflowDescription<LirRelationExpr>, AdapterError> {
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .map_err(|e| AdapterError::Internal(format!("compute instance missing: {e}")))?;
        let (_, sink_id) = self.allocate_transient_id();
        let (_, view_id) = self.allocate_transient_id();
        let column_names = sink.desc.iter_names().cloned().collect();

        let optimizer_config = {
            let system_config = self.catalog().system_config();
            let overrides = self.catalog().get_cluster(cluster_id).config.features();
            OptimizerConfig::from(system_config).override_from(&overrides)
        };

        let mut optimizer = materialized_view::Optimizer::new(
            self.owned_catalog().as_optimizer_catalog(),
            compute_instance,
            sink_id,
            view_id,
            column_names,
            Vec::new(),
            None,
            format!("prometheus-sink-{}", sink.name),
            optimizer_config,
            self.optimizer_metrics(),
        )
        .with_sink_connection(ComputeSinkConnection::Prometheus(sink.connection.clone()));

        let local_plan = optimizer.optimize(sink.expr.clone())?;
        let global_mir_plan = optimizer.optimize(local_plan)?;
        let global_lir_plan = optimizer.optimize(global_mir_plan)?;
        let (df_desc, _df_meta) = global_lir_plan.unapply();
        Ok(df_desc)
    }
}

/// Lowers a [`BuiltinPrometheusSink`]'s label/value schema against the planned
/// view's [`RelationDesc`], resolving column names to indices and enforcing the
/// contract:
///
/// - Every label and value column must exist in the view.
/// - Label columns must be `text`.
/// - Value columns must be a numeric type.
///
/// The resulting [`PrometheusSinkConnection`] carries resolved indices in the
/// declared order, which the operator reads positionally.
pub fn lower_prometheus_sink(
    builtin: &BuiltinPrometheusSink,
    desc: &RelationDesc,
) -> Result<PrometheusSinkConnection, AdapterError> {
    let resolve = |column: &str| -> Result<(usize, &SqlScalarType), AdapterError> {
        let name = column.into();
        desc.get_by_name(&name)
            .map(|(idx, col_type)| (idx, &col_type.scalar_type))
            .ok_or_else(|| {
                AdapterError::Internal(format!(
                    "prometheus sink {}: declared column {column:?} not found in view",
                    builtin.name
                ))
            })
    };

    let mut labels = Vec::with_capacity(builtin.labels.len());
    for label in builtin.labels {
        let (idx, scalar_type) = resolve(label)?;
        if !matches!(scalar_type, SqlScalarType::String) {
            return Err(AdapterError::Internal(format!(
                "prometheus sink {}: label column {label:?} must be text, got {scalar_type:?}",
                builtin.name
            )));
        }
        labels.push(PromLabelSpec {
            name: label.to_string(),
            column_index: idx,
        });
    }

    let mut values = Vec::with_capacity(builtin.values.len());
    for value in builtin.values {
        let (idx, scalar_type) = resolve(value.column)?;
        if !is_numeric(scalar_type) {
            return Err(AdapterError::Internal(format!(
                "prometheus sink {}: value column {:?} must be numeric, got {scalar_type:?}",
                builtin.name, value.column
            )));
        }
        values.push(PromValueSpec {
            column_name: value.column.to_string(),
            column_index: idx,
            metric: value.metric.to_string(),
            kind: match value.kind {
                CatalogPromKind::Gauge => PromKind::Gauge,
                CatalogPromKind::Counter => PromKind::Counter,
            },
            help: value.help.to_string(),
        });
    }

    Ok(PrometheusSinkConnection {
        sink_name: builtin.name.to_string(),
        labels,
        values,
    })
}

/// Whether a scalar type is an allowed Prometheus value type: the integer and
/// float families plus `numeric`.
fn is_numeric(scalar_type: &SqlScalarType) -> bool {
    matches!(
        scalar_type,
        SqlScalarType::Int16
            | SqlScalarType::Int32
            | SqlScalarType::Int64
            | SqlScalarType::UInt16
            | SqlScalarType::UInt32
            | SqlScalarType::UInt64
            | SqlScalarType::Float32
            | SqlScalarType::Float64
            | SqlScalarType::Numeric { .. }
    )
}

#[cfg(test)]
mod tests {
    use mz_catalog::builtin::{PromKind as CatalogPromKind, PromValue};
    use mz_ore::collections::CollectionExt;
    use mz_repr::SqlScalarType;

    use super::*;

    fn desc_from(columns: &[(&str, SqlScalarType)]) -> RelationDesc {
        let mut builder = RelationDesc::builder();
        for (name, ty) in columns {
            builder = builder.with_column(*name, ty.clone().nullable(false));
        }
        builder.finish()
    }

    fn sink(
        labels: &'static [&'static str],
        values: &'static [PromValue],
    ) -> BuiltinPrometheusSink {
        BuiltinPrometheusSink {
            name: "test_sink",
            schema: "mz_introspection",
            oid: 0,
            sql: "",
            labels,
            values,
            access: vec![],
            ontology: None,
        }
    }

    #[mz_ore::test]
    fn lower_happy_path() {
        let desc = desc_from(&[
            ("object_id", SqlScalarType::String),
            ("size_bytes", SqlScalarType::Float64),
        ]);
        static VALUES: &[PromValue] = &[PromValue {
            column: "size_bytes",
            metric: "mz_arrangement_size_bytes",
            kind: CatalogPromKind::Gauge,
            help: "size",
        }];
        let builtin = sink(&["object_id"], VALUES);
        let connection = lower_prometheus_sink(&builtin, &desc).expect("lowers");
        assert_eq!(connection.labels.len(), 1);
        assert_eq!(connection.labels.into_element().column_index, 0);
        assert_eq!(connection.values.into_element().column_index, 1);
    }

    #[mz_ore::test]
    fn lower_missing_column() {
        let desc = desc_from(&[("object_id", SqlScalarType::String)]);
        static VALUES: &[PromValue] = &[PromValue {
            column: "size_bytes",
            metric: "m",
            kind: CatalogPromKind::Gauge,
            help: "",
        }];
        let builtin = sink(&["object_id"], VALUES);
        let err = lower_prometheus_sink(&builtin, &desc).unwrap_err();
        assert!(err.to_string().contains("size_bytes"), "{err}");
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[mz_ore::test]
    fn lower_label_not_text() {
        let desc = desc_from(&[("object_id", SqlScalarType::Int64)]);
        static VALUES: &[PromValue] = &[];
        let builtin = sink(&["object_id"], VALUES);
        let err = lower_prometheus_sink(&builtin, &desc).unwrap_err();
        assert!(err.to_string().contains("must be text"), "{err}");
    }

    #[mz_ore::test]
    fn lower_value_not_numeric() {
        let desc = desc_from(&[
            ("object_id", SqlScalarType::String),
            ("size_bytes", SqlScalarType::String),
        ]);
        static VALUES: &[PromValue] = &[PromValue {
            column: "size_bytes",
            metric: "m",
            kind: CatalogPromKind::Gauge,
            help: "",
        }];
        let builtin = sink(&["object_id"], VALUES);
        let err = lower_prometheus_sink(&builtin, &desc).unwrap_err();
        assert!(err.to_string().contains("must be numeric"), "{err}");
    }

    /// Plans every registered sink end to end against a real debug catalog and
    /// asserts each lowers cleanly. This is the modularity guard: adding a sink
    /// is covered here automatically, and a sink whose SQL does not plan or
    /// whose declared columns do not match its view fails in CI.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn plan_all_sinks() {
        use mz_catalog::builtin::BUILTIN_PROMETHEUS_SINKS;

        use crate::catalog::Catalog;

        Catalog::with_debug(|catalog| async move {
            for builtin in BUILTIN_PROMETHEUS_SINKS.iter() {
                let planned = plan_builtin_prometheus_sink(catalog.state(), builtin)
                    .unwrap_or_else(|e| panic!("sink {} must plan: {e}", builtin.name));
                assert_eq!(planned.name, builtin.name);
                assert_eq!(planned.connection.labels.len(), builtin.labels.len());
                assert_eq!(planned.connection.values.len(), builtin.values.len());
                let arity = planned.desc.arity();
                for label in &planned.connection.labels {
                    assert!(label.column_index < arity, "label index in range");
                }
                for value in &planned.connection.values {
                    assert!(value.column_index < arity, "value index in range");
                }
            }
        })
        .await;
    }

    /// Spot-checks the worked example's shape: six labels, three gauge families.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn plan_worked_example() {
        use mz_catalog::builtin::MZ_PROM_ARRANGEMENT_SIZES;

        use crate::catalog::Catalog;

        Catalog::with_debug(|catalog| async move {
            let planned = plan_builtin_prometheus_sink(catalog.state(), &MZ_PROM_ARRANGEMENT_SIZES)
                .expect("mz_prom_arrangement_sizes plans");
            assert_eq!(planned.connection.labels.len(), 6);
            assert_eq!(planned.connection.values.len(), 3);
            let metrics: Vec<_> = planned
                .connection
                .values
                .iter()
                .map(|v| v.metric.as_str())
                .collect();
            assert!(metrics.contains(&"mz_arrangement_size_bytes"));
            assert!(metrics.contains(&"mz_arrangement_records"));
            assert!(metrics.contains(&"mz_arrangement_batches"));
        })
        .await;
    }
}
