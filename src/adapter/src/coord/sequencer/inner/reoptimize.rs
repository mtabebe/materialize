// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::CatalogItem;
use mz_controller_types::ClusterId;
use mz_expr::{Id, JoinImplementation, MirRelationExpr, MirScalarExpr};
use mz_repr::GlobalId;
use mz_repr::optimize::OverrideFrom;
use mz_repr::role_id::RoleId;
use mz_sql::ast::Ident;
use mz_sql::catalog::{CatalogCluster, CatalogItem as _, ObjectType};
use mz_sql::names::{QualifiedItemName, ResolvedIds};
use mz_sql::plan::AlterClusterReoptimizePlan;
use tracing::debug;

use crate::coord::Coordinator;
use crate::optimize::{self, Optimize, dataflows::dataflow_import_id_bundle};
use crate::util::ResultExt;
use crate::{AdapterError, ExecuteContext, ExecuteResponse, catalog};

impl Coordinator {
    /// Sequences `ALTER CLUSTER ... REOPTIMIZE` on a sealed cluster.
    ///
    /// The algorithm has four phases:
    ///
    /// 1. **Arrangement inventory:** Walk every index and MV on the cluster,
    ///    collecting `(on_id, key)` pairs for both explicit indexes and
    ///    implicit arrangements inferred from MIR plans (joins, reduces,
    ///    ArrangeBy, TopK).
    ///
    /// 2. **Optimization plan:** Group the inventory by `(on_id, key)`.
    ///    Arrangements needed by ≥ 2 MVs that are not already backed by a
    ///    shared index become candidates for new shared indexes.
    ///
    /// 3. **Shared index creation:** For each candidate, allocate catalog
    ///    IDs, run the index optimizer, register the catalog entry, and
    ///    ship the dataflow.
    ///
    /// 4. **MV re-optimization:** Re-optimize every MV from its stored MIR.
    ///    The fresh compute-instance snapshot now includes the new shared
    ///    indexes, so the optimizer can import them instead of building
    ///    private arrangements. Old dataflows are atomically torn down and
    ///    replaced via [`Coordinator::try_replan_dataflow`].
    ///
    /// Shared indexes created in phase 3 are named following the convention
    /// `mz_reoptimize_{collection}_{global_id}`, e.g.
    /// `mz_reoptimize_customers_u11`. The `global_id` suffix ensures
    /// uniqueness when multiple shared indexes exist on the same collection
    /// (arranged by different keys).
    pub(crate) async fn sequence_alter_cluster_reoptimize(
        &mut self,
        _ctx: &mut ExecuteContext,
        plan: AlterClusterReoptimizePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let cluster = self.catalog().get_cluster(plan.id);
        if !cluster.config.sealed {
            coord_bail!(
                "cannot reoptimize unsealed cluster '{}'; seal it first with ALTER CLUSTER {} SET (SEALED)",
                plan.name,
                plan.name
            );
        }

        // Phase 1: Arrangement inventory.
        // Collect all arrangements (explicit indexes and implicit arrangements
        // from MV plans) on this cluster, grouped by (on_id, key).
        let inventory = self.collect_arrangement_inventory(cluster);

        // Phase 2: Generate optimization plan.
        // Determine which arrangements are duplicated and which new shared
        // indexes would be needed to consolidate them.
        let opt_plan = self.generate_optimization_plan(&inventory, cluster);

        debug!(
            cluster = %plan.name,
            total_arrangement_groups = inventory.len(),
            duplicate_groups = opt_plan.new_indexes.len(),
            already_shared = opt_plan.already_shared.len(),
            "reoptimize analysis complete"
        );

        for new_idx in &opt_plan.new_indexes {
            let consumers: Vec<_> = new_idx
                .consumer_names
                .iter()
                .map(|(name, src)| format!("{name}({src})"))
                .collect();
            debug!(
                on_id = %new_idx.desc.on_id,
                key = ?new_idx.desc.key,
                num_consumers = new_idx.consumer_names.len(),
                consumers = ?consumers,
                "new shared index needed"
            );
        }

        for shared in &opt_plan.already_shared {
            debug!(
                on_id = %shared.desc.on_id,
                key = ?shared.desc.key,
                index_id = %shared.existing_index_id,
                "arrangement already covered by existing index"
            );
        }

        // Phase 3: Create shared indexes for duplicate arrangements.
        if opt_plan.new_indexes.is_empty() {
            debug!(
                cluster = %plan.name,
                "reoptimize: no new shared indexes needed"
            );
            return Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster));
        }

        let cluster_id = plan.id;
        let num_new_indexes = opt_plan.new_indexes.len();

        // Allocate IDs and optimize each new shared index.
        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance exists for sealed cluster");
        let optimizer_config =
            optimize::OptimizerConfig::from(self.catalog().system_config())
                .override_from(&self.catalog.get_cluster(cluster_id).config.features());

        let mut index_plans: Vec<ReoptimizeIndexPlan> = Vec::new();

        for new_idx in &opt_plan.new_indexes {
            let on_id = new_idx.desc.on_id;
            let key = new_idx.desc.key.clone();

            // Allocate catalog IDs for this new index.
            let id_ts = self.get_catalog_write_ts().await;
            let (item_id, global_id) = self.catalog().allocate_user_id(id_ts).await?;

            // Derive the index name and qualifiers from the indexed collection.
            let on_entry = self.catalog().get_entry_by_global_id(&on_id);
            let on_name = on_entry.name().clone();
            let owner_id = on_entry.owner_id();
            let on_item_id = on_entry.id();

            // Generate a system name for this index.
            let index_name = QualifiedItemName {
                qualifiers: on_name.qualifiers.clone(),
                item: format!(
                    "mz_reoptimize_{}_{}",
                    on_name.item,
                    global_id
                ),
            };

            // Build the optimizer and run it.
            let catalog = self.owned_catalog();
            let instance = compute_instance.clone();
            let config = optimizer_config.clone();
            let metrics = self.optimizer_metrics();
            let name_for_optimizer = index_name.clone();

            let mut optimizer = optimize::index::Optimizer::new(
                catalog,
                instance,
                global_id,
                config,
                metrics,
            );

            let index_input = optimize::index::Index::new(
                name_for_optimizer,
                on_id,
                key.clone(),
            );

            let global_mir_plan = optimizer
                .catch_unwind_optimize(index_input)
                .map_err(|e| AdapterError::Internal(format!(
                    "reoptimize: failed to optimize shared index on {}: {}",
                    on_name.item, e
                )))?;
            let global_lir_plan = optimizer
                .catch_unwind_optimize(global_mir_plan.clone())
                .map_err(|e| AdapterError::Internal(format!(
                    "reoptimize: failed to lower shared index on {}: {}",
                    on_name.item, e
                )))?;

            // Generate synthetic create_sql for the catalog entry.
            // The create_sql must be valid, parseable SQL since the catalog
            // validates it by re-parsing.
            let on_full_name = self
                .catalog()
                .resolve_full_name(&on_name, on_entry.conn_id());

            // Convert MirScalarExpr keys to SQL column references.
            let on_desc = on_entry.relation_desc_latest();
            let key_sql: Vec<String> = key
                .iter()
                .map(|k| mir_scalar_to_sql_column(k, on_desc.as_deref()))
                .collect();

            let on_sql_name: mz_sql::ast::UnresolvedItemName =
                on_full_name.into();
            let create_sql = format!(
                "CREATE INDEX {} IN CLUSTER {} ON {} ({})",
                Ident::new_unchecked(index_name.item.clone()),
                Ident::new_unchecked(plan.name.clone()),
                on_sql_name,
                key_sql.join(", ")
            );

            let mut resolved_ids = ResolvedIds::empty();
            resolved_ids.extend(std::iter::once((on_item_id, on_id)));

            index_plans.push(ReoptimizeIndexPlan {
                item_id,
                global_id,
                index_name,
                create_sql,
                on_id,
                key: key.into(),
                owner_id,
                resolved_ids,
                cluster_id,
                global_mir_plan,
                global_lir_plan,
            });
        }

        // Build catalog operations for all new indexes.
        let ops: Vec<catalog::Op> = index_plans
            .iter()
            .map(|p| catalog::Op::CreateItem {
                id: p.item_id,
                name: p.index_name.clone(),
                item: CatalogItem::Index(mz_catalog::memory::objects::Index {
                    create_sql: p.create_sql.clone(),
                    global_id: p.global_id,
                    on: p.on_id,
                    keys: p.key.clone(),
                    conn_id: None,
                    resolved_ids: p.resolved_ids.clone(),
                    cluster_id: p.cluster_id,
                    is_retained_metrics_object: false,
                    custom_logical_compaction_window: None,
                }),
                owner_id: p.owner_id,
            })
            .collect();

        // Execute the catalog transaction and ship dataflows.
        self.catalog_transact_with_side_effects(Some(_ctx), ops, move |coord, _ctx| {
            Box::pin(async move {
                for p in index_plans {
                    let (mut df_desc, _df_meta) = p.global_lir_plan.unapply();
                    let id_bundle =
                        dataflow_import_id_bundle(&df_desc, p.cluster_id);

                    // Save plan structures in the catalog.
                    coord
                        .catalog_mut()
                        .set_optimized_plan(p.global_id, p.global_mir_plan.df_desc().clone());
                    coord
                        .catalog_mut()
                        .set_physical_plan(p.global_id, df_desc.clone());
                    // System-generated indexes don't produce user-facing
                    // optimizer notices, so use an empty metainfo.
                    coord
                        .catalog_mut()
                        .set_dataflow_metainfo(p.global_id, Default::default());

                    // Acquire read holds and set as_of.
                    let read_holds = coord.acquire_read_holds(&id_bundle);
                    let since = read_holds.least_valid_read();
                    df_desc.set_as_of(since);

                    // Ship the dataflow to the compute cluster.
                    coord
                        .ship_dataflow(df_desc, p.cluster_id, None)
                        .await;

                    // Drop read holds after dataflow is shipped.
                    drop(read_holds);

                    // Set default compaction policy.
                    coord.update_compute_read_policy(
                        p.cluster_id,
                        p.item_id,
                        CompactionWindow::Default.into(),
                    );

                    debug!(
                        global_id = %p.global_id,
                        on_id = %p.on_id,
                        "reoptimize: created shared index"
                    );
                }
            })
        })
        .await?;

        debug!(
            cluster = %plan.name,
            num_new_indexes = num_new_indexes,
            "reoptimize: shared index creation complete"
        );

        // Phase 4: Re-optimize existing MV dataflows to use the new shared indexes.
        // Each MV is re-optimized from its stored MIR expression, producing a new
        // physical plan that imports the shared indexes. The old dataflow is torn down
        // and a new one shipped atomically.
        let optimizer_config =
            optimize::OptimizerConfig::from(self.catalog().system_config())
                .override_from(&self.catalog.get_cluster(cluster_id).config.features());

        // Collect MVs on this cluster.
        struct MvInfo {
            global_id: GlobalId,
            optimized_expr: Arc<mz_expr::OptimizedMirRelationExpr>,
            raw_expr: Arc<mz_sql::plan::HirRelationExpr>,
            desc: mz_repr::VersionedRelationDesc,
            non_null_assertions: Vec<usize>,
            refresh_schedule: Option<mz_repr::refresh_schedule::RefreshSchedule>,
            item_name: String,
        }

        let mut mvs: Vec<MvInfo> = Vec::new();
        let cluster = self.catalog().get_cluster(cluster_id);
        for item_id in cluster.bound_objects() {
            let entry = self.catalog().get_entry(item_id);
            if let CatalogItem::MaterializedView(mv) = &entry.item {
                mvs.push(MvInfo {
                    global_id: mv.global_id_writes(),
                    optimized_expr: mv.optimized_expr.clone(),
                    raw_expr: mv.raw_expr.clone(),
                    desc: mv.desc.clone(),
                    non_null_assertions: mv.non_null_assertions.clone(),
                    refresh_schedule: mv.refresh_schedule.clone(),
                    item_name: self
                        .catalog()
                        .resolve_full_name(entry.name(), None)
                        .to_string(),
                });
            }
        }

        let num_mvs = mvs.len();
        if num_mvs == 0 {
            debug!(
                cluster = %plan.name,
                "reoptimize: no materialized views to re-optimize"
            );
            return Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster));
        }

        // Re-optimize each MV with a fresh compute instance snapshot that sees
        // the newly created shared indexes.
        for mv_info in mvs {
            let compute_instance = self
                .instance_snapshot(cluster_id)
                .expect("compute instance exists for sealed cluster");

            let (_, internal_view_id) = self.allocate_transient_id();
            let force_non_monotonic = Default::default();

            let mut optimizer = optimize::materialized_view::Optimizer::new(
                self.owned_catalog().as_optimizer_catalog(),
                compute_instance,
                mv_info.global_id,
                internal_view_id,
                mv_info.desc.latest().iter_names().cloned().collect(),
                mv_info.non_null_assertions.clone(),
                mv_info.refresh_schedule.clone(),
                mv_info.item_name.clone(),
                optimizer_config.clone(),
                self.optimizer_metrics(),
                force_non_monotonic,
            );

            // MIR ⇒ MIR optimization (global)
            let typ = crate::coord::infer_sql_type_for_catalog(
                &mv_info.raw_expr,
                &mv_info.optimized_expr.as_ref().clone(),
            );
            let global_mir_plan = optimizer
                .catch_unwind_optimize((mv_info.optimized_expr.as_ref().clone(), typ))
                .map_err(|e| AdapterError::Internal(format!(
                    "reoptimize: failed to re-optimize MV {}: {}",
                    mv_info.item_name, e
                )))?;
            let optimized_plan = global_mir_plan.df_desc().clone();

            // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
            let global_lir_plan = optimizer
                .catch_unwind_optimize(global_mir_plan)
                .map_err(|e| AdapterError::Internal(format!(
                    "reoptimize: failed to lower MV {}: {}",
                    mv_info.item_name, e
                )))?;

            let (mut df_desc, _df_meta) = global_lir_plan.unapply();

            // Update stored plans in the catalog.
            let catalog = self.catalog_mut();
            catalog.set_optimized_plan(mv_info.global_id, optimized_plan);
            catalog.set_physical_plan(mv_info.global_id, df_desc.clone());
            catalog.set_dataflow_metainfo(mv_info.global_id, Default::default());

            // Acquire read holds to compute the as_of, then drop them before
            // replanning. We must not hold coordinator-level compute read holds
            // across the replan_dataflow call because replan_dataflow's inner
            // closure modifies the same SharedCollectionState asynchronously,
            // which can cause read capability mismatches when these holds are
            // released. On a sealed cluster, sinces are stable so dropping
            // early is safe.
            let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
            let since = {
                let read_holds = self.acquire_read_holds(&id_bundle);
                let since = read_holds.least_valid_read();
                drop(read_holds);
                since
            };
            df_desc.set_as_of(since);

            // The old dataflow's export is the sink_id = global_id_writes().
            let old_export_ids = vec![mv_info.global_id];

            // Atomically remove old collections and create new dataflow.
            // On a sealed cluster we have already validated the cluster and plans;
            // replan_dataflow only fails for invalid state, so we terminate rather
            // than leave the cluster inconsistent.
            self.try_replan_dataflow(old_export_ids, df_desc, cluster_id, None)
                .await
                .unwrap_or_terminate("replan_dataflow cannot fail for sealed cluster MV");

            // Re-enable writes for this MV.
            self.allow_writes(cluster_id, mv_info.global_id);

            debug!(
                global_id = %mv_info.global_id,
                mv = %mv_info.item_name,
                "reoptimize: re-optimized MV dataflow"
            );
        }

        debug!(
            cluster = %plan.name,
            num_mvs = num_mvs,
            "reoptimize: MV re-optimization complete"
        );

        Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster))
    }

    /// Collects an inventory of all arrangements on the given cluster.
    ///
    /// Returns a map from arrangement identity `(on_id, key)` to the list of
    /// catalog items that require that arrangement. Explicit indexes and
    /// implicit arrangements (from joins, reduces, arrange-by nodes in MV
    /// plans) are both included.
    ///
    /// Uses both the catalog entries (for explicit indexes) and the global MIR
    /// plans (from the plans cache) for more accurate analysis of implicit
    /// arrangements.
    fn collect_arrangement_inventory(
        &self,
        cluster: &mz_catalog::memory::objects::Cluster,
    ) -> BTreeMap<ArrangementDesc, Vec<ArrangementRef>> {
        let mut inventory: BTreeMap<ArrangementDesc, Vec<ArrangementRef>> = BTreeMap::new();

        for item_id in cluster.bound_objects() {
            let entry = self.catalog().get_entry(item_id);
            let item_name = entry.name.item.clone();

            match &entry.item {
                CatalogItem::Index(idx) => {
                    let desc = ArrangementDesc {
                        on_id: idx.on,
                        key: idx.keys.to_vec(),
                    };
                    inventory.entry(desc).or_default().push(ArrangementRef {
                        owner_item_id: *item_id,
                        owner_name: item_name,
                        source: ArrangementSource::ExplicitIndex,
                    });
                }
                CatalogItem::MaterializedView(mv) => {
                    // Try the global MIR plan first (more accurate: it shows
                    // the full DataflowDescription including index_imports).
                    // Fall back to the local optimized_expr if unavailable.
                    let global_id = mv.global_id_writes();
                    if let Some(global_mir) = self.catalog().try_get_optimized_plan(&global_id) {
                        // Extract implied arrangements from the MIR plan.
                        for build in &global_mir.objects_to_build {
                            let implied = extract_implied_arrangements(&build.plan.0);
                            for (on_id, key) in implied {
                                let desc = ArrangementDesc { on_id, key };
                                inventory.entry(desc).or_default().push(ArrangementRef {
                                    owner_item_id: *item_id,
                                    owner_name: item_name.clone(),
                                    source: ArrangementSource::ImplicitInMv,
                                });
                            }
                        }
                    } else {
                        // Fall back to local optimized_expr.
                        let implied = extract_implied_arrangements(&mv.optimized_expr.0);
                        for (on_id, key) in implied {
                            let desc = ArrangementDesc { on_id, key };
                            inventory.entry(desc).or_default().push(ArrangementRef {
                                owner_item_id: *item_id,
                                owner_name: item_name.clone(),
                                source: ArrangementSource::ImplicitInMv,
                            });
                        }
                    }
                }
                // Sources, sinks, continual tasks, etc. don't have
                // MIR plans with arrangements we can analyze here.
                _ => {}
            }
        }

        inventory
    }

    /// Generates an optimization plan from the arrangement inventory.
    ///
    /// Compares the inventory of implicit arrangements against existing shared
    /// indexes on the cluster to determine:
    /// 1. Which arrangements are already covered by existing indexes.
    /// 2. Which arrangements need new shared indexes to be created.
    fn generate_optimization_plan(
        &self,
        inventory: &BTreeMap<ArrangementDesc, Vec<ArrangementRef>>,
        cluster: &mz_catalog::memory::objects::Cluster,
    ) -> OptimizationPlan {
        // Build a lookup of existing shared indexes on this cluster:
        // (on_id, key) → GlobalId of the index.
        let mut existing_indexes: BTreeMap<ArrangementDesc, GlobalId> = BTreeMap::new();
        for item_id in cluster.bound_objects() {
            let entry = self.catalog().get_entry(item_id);
            if let CatalogItem::Index(idx) = &entry.item {
                let desc = ArrangementDesc {
                    on_id: idx.on,
                    key: idx.keys.to_vec(),
                };
                existing_indexes.insert(desc, idx.global_id);
            }
        }

        build_optimization_plan(inventory, &existing_indexes)
    }
}

fn mir_scalar_to_sql_column(
    expr: &MirScalarExpr,
    on_desc: Option<&mz_repr::RelationDesc>,
) -> String {
    match expr {
        MirScalarExpr::Column(idx, _) => {
            if let Some(desc) = on_desc {
                if *idx < desc.arity() {
                    let name = desc.get_name(*idx);
                    return format!("{}", Ident::new_unchecked(name.as_str()));
                }
            }
            // Fallback: use 1-based positional reference.
            format!("{}", idx + 1)
        }
        // For non-column expressions, use a generic representation.
        // This is a limitation — complex key expressions can't be
        // represented as simple column references.
        _ => format!("{}", expr),
    }
}

/// Identity of an arrangement: a (collection, key) pair. Two arrangements
/// with the same `ArrangementDesc` are duplicates that could be shared.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ArrangementDesc {
    on_id: GlobalId,
    key: Vec<MirScalarExpr>,
}

/// Where an arrangement reference comes from.
#[derive(Debug, Clone)]
enum ArrangementSource {
    /// An explicit `CREATE INDEX` on the cluster.
    ExplicitIndex,
    /// An implicit arrangement inside a materialized view's plan (join,
    /// reduce, or explicit arrange-by node).
    ImplicitInMv,
}

impl std::fmt::Display for ArrangementSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrangementSource::ExplicitIndex => write!(f, "explicit_index"),
            ArrangementSource::ImplicitInMv => write!(f, "implicit_in_mv"),
        }
    }
}

/// A reference to an arrangement from a specific catalog item.
#[derive(Debug, Clone)]
struct ArrangementRef {
    owner_item_id: mz_repr::CatalogItemId,
    owner_name: String,
    source: ArrangementSource,
}

/// The output of the REOPTIMIZE analysis phase: a plan describing what shared
/// indexes to create and which existing indexes already cover duplicate
/// arrangements.
#[derive(Debug)]
struct OptimizationPlan {
    /// Arrangements that are duplicated across multiple dataflows and need a
    /// new shared index to be created.
    new_indexes: Vec<NewSharedIndex>,
    /// Arrangements that are duplicated but already covered by an existing
    /// shared index on the cluster.
    already_shared: Vec<AlreadySharedArrangement>,
}

/// A new shared index that should be created to consolidate duplicate
/// private arrangements.
#[derive(Debug)]
struct NewSharedIndex {
    /// The (on_id, key) identity of the arrangement.
    desc: ArrangementDesc,
    /// Names and sources of the catalog items that need this arrangement.
    consumer_names: Vec<(String, ArrangementSource)>,
}

/// An arrangement that is already covered by an existing shared index.
#[derive(Debug)]
struct AlreadySharedArrangement {
    /// The (on_id, key) identity of the arrangement.
    desc: ArrangementDesc,
    /// The GlobalId of the existing shared index.
    existing_index_id: GlobalId,
}

/// All the pieces needed to create a single shared index as part of
/// REOPTIMIZE: catalog IDs, optimizer output, and metadata.
struct ReoptimizeIndexPlan {
    item_id: mz_repr::CatalogItemId,
    global_id: GlobalId,
    index_name: mz_sql::names::QualifiedItemName,
    create_sql: String,
    on_id: GlobalId,
    key: Arc<[MirScalarExpr]>,
    owner_id: RoleId,
    resolved_ids: mz_sql::names::ResolvedIds,
    cluster_id: ClusterId,
    global_mir_plan: optimize::index::GlobalMirPlan,
    global_lir_plan: optimize::index::GlobalLirPlan,
}

/// Builds an optimization plan by comparing duplicate arrangements against
/// existing shared indexes.
///
/// This is a free function (not a method on Coordinator) so it can be unit
/// tested without constructing a full Coordinator.
fn build_optimization_plan(
    inventory: &BTreeMap<ArrangementDesc, Vec<ArrangementRef>>,
    existing_indexes: &BTreeMap<ArrangementDesc, GlobalId>,
) -> OptimizationPlan {
    let mut new_indexes = Vec::new();
    let mut already_shared = Vec::new();

    for (desc, refs) in inventory {
        // Only consider arrangements referenced by multiple distinct items
        // (implicit arrangements that could benefit from sharing).
        let implicit_refs: Vec<_> = refs
            .iter()
            .filter(|r| matches!(r.source, ArrangementSource::ImplicitInMv))
            .collect();

        // Count distinct owners of implicit arrangements.
        let implicit_owners: BTreeSet<_> =
            implicit_refs.iter().map(|r| &r.owner_item_id).collect();

        if implicit_owners.len() < 2 {
            // Only one dataflow needs this arrangement — no sharing benefit.
            continue;
        }

        // Check if an existing shared index already covers this arrangement.
        if let Some(&index_id) = existing_indexes.get(desc) {
            already_shared.push(AlreadySharedArrangement {
                desc: desc.clone(),
                existing_index_id: index_id,
            });
        } else {
            // This arrangement is duplicated and has no shared index.
            let consumer_names: Vec<_> = refs
                .iter()
                .map(|r| (r.owner_name.clone(), r.source.clone()))
                .collect();
            new_indexes.push(NewSharedIndex {
                desc: desc.clone(),
                consumer_names,
            });
        }
    }

    OptimizationPlan {
        new_indexes,
        already_shared,
    }
}

/// Walk a `MirRelationExpr` tree and extract all implied arrangements as
/// `(on_id, key)` pairs.
///
/// An implied arrangement is one that the compute layer will build at render
/// time for joins, reduces, and explicit `ArrangeBy` nodes. Only arrangements
/// on named collections (`Get(Id::Global(gid))`) are returned, since
/// arrangements on intermediate sub-expressions cannot be shared across
/// dataflows.
///
/// This is a best-effort analysis at the MIR level: the actual arrangements
/// created at render time depend on the lowering pass (MIR → LIR), which may
/// make different decisions. The results are conservative — they capture the
/// common cases (direct `Get` inputs to joins/reduces) but may miss
/// arrangements on complex sub-expressions.
fn extract_implied_arrangements(expr: &MirRelationExpr) -> Vec<(GlobalId, Vec<MirScalarExpr>)> {
    let mut arrangements = Vec::new();

    expr.visit_pre(|node| match node {
        MirRelationExpr::Join {
            inputs,
            implementation,
            ..
        } => {
            match implementation {
                JoinImplementation::Differential(start, lookup_stages) => {
                    // The start relation may be arranged by its key.
                    if let (idx, Some(key), _) = start {
                        if let Some(gid) = input_global_id(&inputs[*idx]) {
                            arrangements.push((gid, key.clone()));
                        }
                    }
                    // Each lookup stage arranges its input by the lookup key.
                    for (idx, key, _) in lookup_stages {
                        if let Some(gid) = input_global_id(&inputs[*idx]) {
                            arrangements.push((gid, key.clone()));
                        }
                    }
                }
                JoinImplementation::DeltaQuery(paths) => {
                    for path in paths {
                        for (idx, key, _) in path {
                            if let Some(gid) = input_global_id(&inputs[*idx]) {
                                arrangements.push((gid, key.clone()));
                            }
                        }
                    }
                }
                JoinImplementation::IndexedFilter(_coll_id, _index_id, key, _) => {
                    // IndexedFilter uses an existing index; record the key
                    // for completeness.
                    if let Some(gid) = input_global_id(&inputs[0]) {
                        arrangements.push((gid, key.clone()));
                    }
                }
                JoinImplementation::Unimplemented => {}
            }
        }
        MirRelationExpr::Reduce {
            input, group_key, ..
        } => {
            // A Reduce arranges its input by the group key.
            if let Some(gid) = input_global_id(input) {
                arrangements.push((gid, group_key.clone()));
            }
        }
        MirRelationExpr::ArrangeBy { input, keys } => {
            // Explicit ArrangeBy requests arrangements on each key set.
            if let Some(gid) = input_global_id(input) {
                for key in keys {
                    arrangements.push((gid, key.clone()));
                }
            }
        }
        MirRelationExpr::TopK {
            input, group_key, ..
        } => {
            // TopK arranges by its group key.
            if let Some(gid) = input_global_id(input) {
                let key_exprs: Vec<MirScalarExpr> = group_key
                    .iter()
                    .map(|c| MirScalarExpr::Column(*c, Default::default()))
                    .collect();
                if !key_exprs.is_empty() {
                    arrangements.push((gid, key_exprs));
                }
            }
        }
        _ => {}
    });

    arrangements
}

/// If the expression is a direct `Get(Id::Global(gid))`, return the `GlobalId`.
/// This also looks through simple wrappers like `Mfp` (Map/Filter/Project)
/// to find the underlying `Get`.
fn input_global_id(expr: &MirRelationExpr) -> Option<GlobalId> {
    match expr {
        MirRelationExpr::Get {
            id: Id::Global(gid),
            ..
        } => Some(*gid),
        // Look through Map, Filter, Project wrappers (common pattern:
        // `Project(Filter(Get(...)))`)
        MirRelationExpr::Map { input, .. }
        | MirRelationExpr::Filter { input, .. }
        | MirRelationExpr::Project { input, .. } => input_global_id(input),
        _ => None,
    }
}

#[cfg(test)]
mod reoptimize_tests {
    use super::*;
    use mz_repr::SqlRelationType;

    fn test_gid(id: u64) -> GlobalId {
        GlobalId::User(id)
    }

    fn col(c: usize) -> MirScalarExpr {
        MirScalarExpr::Column(c, Default::default())
    }

    fn get(id: u64) -> MirRelationExpr {
        MirRelationExpr::global_get(test_gid(id), SqlRelationType::empty())
    }

    #[mz_ore::test]
    fn test_input_global_id_direct_get() {
        let expr = get(42);
        assert_eq!(input_global_id(&expr), Some(test_gid(42)));
    }

    #[mz_ore::test]
    fn test_input_global_id_through_filter() {
        let expr = get(7).filter(vec![col(0)]);
        assert_eq!(input_global_id(&expr), Some(test_gid(7)));
    }

    #[mz_ore::test]
    fn test_input_global_id_through_project() {
        let expr = get(7).project(vec![0, 1]);
        assert_eq!(input_global_id(&expr), Some(test_gid(7)));
    }

    #[mz_ore::test]
    fn test_input_global_id_through_map() {
        let expr = get(7).map(vec![col(0)]);
        assert_eq!(input_global_id(&expr), Some(test_gid(7)));
    }

    #[mz_ore::test]
    fn test_input_global_id_through_nested_wrappers() {
        // Project(Filter(Map(Get(5))))
        let expr = get(5)
            .map(vec![col(0)])
            .filter(vec![col(0)])
            .project(vec![0]);
        assert_eq!(input_global_id(&expr), Some(test_gid(5)));
    }

    #[mz_ore::test]
    fn test_input_global_id_local_get_returns_none() {
        let expr = MirRelationExpr::local_get(mz_expr::LocalId::new(1), SqlRelationType::empty());
        assert_eq!(input_global_id(&expr), None);
    }

    #[mz_ore::test]
    fn test_input_global_id_join_returns_none() {
        // A join is not a simple wrapper — we don't look into it.
        let expr = MirRelationExpr::join(vec![get(1), get(2)], vec![]);
        assert_eq!(input_global_id(&expr), None);
    }

    #[mz_ore::test]
    fn test_extract_arrangements_from_reduce() {
        // Reduce { input: Get(10), group_key: [col(0), col(1)] }
        let expr = MirRelationExpr::Reduce {
            input: Box::new(get(10)),
            group_key: vec![col(0), col(1)],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };

        let arrangements = extract_implied_arrangements(&expr);
        assert_eq!(arrangements.len(), 1);
        assert_eq!(arrangements[0].0, test_gid(10));
        assert_eq!(arrangements[0].1, vec![col(0), col(1)]);
    }

    #[mz_ore::test]
    fn test_extract_arrangements_from_arrange_by() {
        let expr = MirRelationExpr::ArrangeBy {
            input: Box::new(get(20)),
            keys: vec![vec![col(0)], vec![col(1), col(2)]],
        };

        let arrangements = extract_implied_arrangements(&expr);
        assert_eq!(arrangements.len(), 2);
        assert_eq!(arrangements[0].0, test_gid(20));
        assert_eq!(arrangements[0].1, vec![col(0)]);
        assert_eq!(arrangements[1].0, test_gid(20));
        assert_eq!(arrangements[1].1, vec![col(1), col(2)]);
    }

    #[mz_ore::test]
    fn test_extract_arrangements_from_differential_join() {
        let expr = MirRelationExpr::Join {
            inputs: vec![get(1), get(2)],
            equivalences: vec![],
            implementation: JoinImplementation::Differential(
                (0, Some(vec![col(0)]), None),
                vec![(1, vec![col(0)], None)],
            ),
        };

        let arrangements = extract_implied_arrangements(&expr);
        // Should find arrangement on get(1) by col(0) and get(2) by col(0).
        assert_eq!(arrangements.len(), 2);
        assert!(arrangements.iter().any(|(gid, _)| *gid == test_gid(1)));
        assert!(arrangements.iter().any(|(gid, _)| *gid == test_gid(2)));
    }

    #[mz_ore::test]
    fn test_extract_arrangements_none_from_constant() {
        let expr = MirRelationExpr::constant(vec![], SqlRelationType::empty());
        let arrangements = extract_implied_arrangements(&expr);
        assert!(arrangements.is_empty());
    }

    #[mz_ore::test]
    fn test_extract_arrangements_skips_complex_inputs() {
        // Join where one input is itself a join (complex sub-expression).
        // input_global_id should return None for the join input.
        let inner_join = MirRelationExpr::join(vec![get(1), get(2)], vec![]);
        let expr = MirRelationExpr::Join {
            inputs: vec![inner_join, get(3)],
            equivalences: vec![],
            implementation: JoinImplementation::Differential(
                (0, Some(vec![col(0)]), None),
                vec![(1, vec![col(0)], None)],
            ),
        };

        let arrangements = extract_implied_arrangements(&expr);
        // The inner join input should be skipped (no global id).
        // Only get(3) contributes an arrangement.
        // But the inner join also contains get(1) and get(2) as nested nodes
        // that visit_pre will traverse — however the inner join itself has
        // JoinImplementation::Unimplemented, so no arrangements from it.
        let outer_arrangements: Vec<_> = arrangements
            .iter()
            .filter(|(gid, _)| *gid == test_gid(3))
            .collect();
        assert_eq!(outer_arrangements.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Tests for build_optimization_plan
    // -----------------------------------------------------------------------

    fn test_item_id(id: u64) -> mz_repr::CatalogItemId {
        mz_repr::CatalogItemId::User(id)
    }

    fn make_ref(item_id: u64, name: &str, source: ArrangementSource) -> ArrangementRef {
        ArrangementRef {
            owner_item_id: test_item_id(item_id),
            owner_name: name.to_string(),
            source,
        }
    }

    fn make_desc(on_id: u64, key_cols: &[usize]) -> ArrangementDesc {
        ArrangementDesc {
            on_id: test_gid(on_id),
            key: key_cols.iter().map(|c| col(*c)).collect(),
        }
    }

    #[mz_ore::test]
    fn test_plan_no_duplicates() {
        // Single owner per arrangement — nothing to optimize.
        let mut inventory = BTreeMap::new();
        inventory.insert(
            make_desc(1, &[0]),
            vec![make_ref(100, "mv1", ArrangementSource::ImplicitInMv)],
        );
        inventory.insert(
            make_desc(2, &[0]),
            vec![make_ref(101, "mv2", ArrangementSource::ImplicitInMv)],
        );

        let plan = build_optimization_plan(&inventory, &BTreeMap::new());
        assert!(plan.new_indexes.is_empty());
        assert!(plan.already_shared.is_empty());
    }

    #[mz_ore::test]
    fn test_plan_duplicate_creates_new_index() {
        // Two MVs share the same arrangement — should create a new index.
        let mut inventory = BTreeMap::new();
        inventory.insert(
            make_desc(1, &[0]),
            vec![
                make_ref(100, "mv1", ArrangementSource::ImplicitInMv),
                make_ref(101, "mv2", ArrangementSource::ImplicitInMv),
            ],
        );

        let plan = build_optimization_plan(&inventory, &BTreeMap::new());
        assert_eq!(plan.new_indexes.len(), 1);
        assert_eq!(plan.new_indexes[0].desc, make_desc(1, &[0]));
        assert_eq!(plan.new_indexes[0].consumer_names.len(), 2);
        assert!(plan.already_shared.is_empty());
    }

    #[mz_ore::test]
    fn test_plan_duplicate_covered_by_existing_index() {
        // Two MVs share an arrangement, but an explicit index already exists.
        let mut inventory = BTreeMap::new();
        let desc = make_desc(1, &[0]);
        inventory.insert(
            desc.clone(),
            vec![
                make_ref(100, "mv1", ArrangementSource::ImplicitInMv),
                make_ref(101, "mv2", ArrangementSource::ImplicitInMv),
                make_ref(200, "idx1", ArrangementSource::ExplicitIndex),
            ],
        );

        let mut existing_indexes = BTreeMap::new();
        existing_indexes.insert(desc.clone(), test_gid(200));

        let plan = build_optimization_plan(&inventory, &existing_indexes);
        assert!(plan.new_indexes.is_empty());
        assert_eq!(plan.already_shared.len(), 1);
        assert_eq!(plan.already_shared[0].existing_index_id, test_gid(200));
    }

    #[mz_ore::test]
    fn test_plan_explicit_index_only_not_duplicate() {
        // An explicit index and one MV — only 1 implicit owner, so no sharing.
        let mut inventory = BTreeMap::new();
        inventory.insert(
            make_desc(1, &[0]),
            vec![
                make_ref(100, "mv1", ArrangementSource::ImplicitInMv),
                make_ref(200, "idx1", ArrangementSource::ExplicitIndex),
            ],
        );

        let plan = build_optimization_plan(&inventory, &BTreeMap::new());
        // Only one implicit owner — no sharing benefit.
        assert!(plan.new_indexes.is_empty());
        assert!(plan.already_shared.is_empty());
    }

    #[mz_ore::test]
    fn test_plan_mixed_duplicates_and_covered() {
        // Two arrangement groups: one needs a new index, one already covered.
        let mut inventory = BTreeMap::new();
        let desc_a = make_desc(1, &[0]);
        let desc_b = make_desc(2, &[1]);

        inventory.insert(
            desc_a.clone(),
            vec![
                make_ref(100, "mv1", ArrangementSource::ImplicitInMv),
                make_ref(101, "mv2", ArrangementSource::ImplicitInMv),
            ],
        );
        inventory.insert(
            desc_b.clone(),
            vec![
                make_ref(100, "mv1", ArrangementSource::ImplicitInMv),
                make_ref(102, "mv3", ArrangementSource::ImplicitInMv),
            ],
        );

        let mut existing_indexes = BTreeMap::new();
        existing_indexes.insert(desc_a.clone(), test_gid(300));

        let plan = build_optimization_plan(&inventory, &existing_indexes);
        assert_eq!(plan.new_indexes.len(), 1);
        assert_eq!(plan.new_indexes[0].desc, desc_b);
        assert_eq!(plan.already_shared.len(), 1);
        assert_eq!(plan.already_shared[0].desc, desc_a);
    }

    #[mz_ore::test]
    fn test_plan_three_way_duplicate() {
        // Three MVs sharing the same arrangement.
        let mut inventory = BTreeMap::new();
        inventory.insert(
            make_desc(5, &[0, 1]),
            vec![
                make_ref(100, "mv1", ArrangementSource::ImplicitInMv),
                make_ref(101, "mv2", ArrangementSource::ImplicitInMv),
                make_ref(102, "mv3", ArrangementSource::ImplicitInMv),
            ],
        );

        let plan = build_optimization_plan(&inventory, &BTreeMap::new());
        assert_eq!(plan.new_indexes.len(), 1);
        assert_eq!(plan.new_indexes[0].consumer_names.len(), 3);
    }
}
