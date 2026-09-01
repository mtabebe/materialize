// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sequencing for `CREATE ... ENRICH WITH`, which is one statement that creates
//! several catalog items.
//!
//! **Why the items commit one at a time rather than in one transaction.** The ops
//! side would be happy to take them together: `transact` applies ops sequentially to
//! the in-memory state, so op N can see the item op N-1 created, which is how a
//! source and its subsources already land atomically. Planning is the problem.
//! Everything is planned before any catalog write, and a view body resolves its
//! inputs by looking each one up and reading its `RelationDesc`, so
//! `CREATE VIEW tickets AS SELECT ... FROM tickets_raw` cannot be planned until
//! `tickets_raw` is genuinely in a `CatalogState`. The trick the source path uses
//! for subsources, hand-building a `ResolvedItemName` over a pre-allocated id, does
//! not carry over: subsources never read the source's shape and views do.
//!
//! So each generated statement is planned against the catalog the previous one
//! committed. The cost is that the expansion is not atomic: a failure partway leaves
//! the earlier items behind. That is the same class of leak as `DROP` on the
//! underlying relation leaving the generated objects behind, which is an accepted
//! demo-grade tradeoff for this feature.

use mz_catalog::memory::objects::{CatalogItem, View};
use mz_ore::collections::CollectionExt;
use mz_repr::RelationDesc;
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{self, Plan};
use mz_sql::session::metadata::SessionMetadata;

use crate::coord::{Coordinator, infer_sql_type_for_catalog};
use crate::error::AdapterError;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{ExecuteContext, ExecuteResponse, catalog};

impl Coordinator {
    /// Creates the underlying relation, then the objects that enrich it.
    pub(crate) async fn sequence_create_enriched_relation(
        &mut self,
        ctx: &mut ExecuteContext,
        plan: plan::CreateEnrichedRelationPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateEnrichedRelationPlan { base, generated } = plan;

        let response = match *base {
            Plan::CreateTable(plan) => self.sequence_create_table(ctx, plan, resolved_ids).await?,
            Plan::CreateSources(plans) => self.sequence_create_source(ctx, plans).await?,
            Plan::CreateSource(plan) => {
                let (item_id, global_id) = self.allocate_user_id().await?;
                self.sequence_create_source(
                    ctx,
                    vec![plan::CreateSourcePlanBundle {
                        item_id,
                        global_id,
                        plan,
                        resolved_ids,
                        available_source_references: None,
                    }],
                )
                .await?
            }
            other => {
                return Err(AdapterError::Internal(format!(
                    "ENRICH WITH produced an unexpected base plan: {}",
                    other.name()
                )));
            }
        };

        for sql in &generated {
            self.sequence_generated_item(ctx, sql).await?;
        }

        Ok(response)
    }

    /// Plans and creates one generated item against the catalog as it stands now.
    async fn sequence_generated_item(
        &mut self,
        ctx: &mut ExecuteContext,
        sql: &str,
    ) -> Result<(), AdapterError> {
        let stmt = mz_sql::parse::parse(sql)?.into_element().ast;
        let (plan, resolved_ids) = {
            let catalog = self.catalog().for_session(ctx.session());
            let (stmt, resolved_ids) = mz_sql::names::resolve(&catalog, stmt)?;
            let (plan, _sql_impl_ids) = mz_sql::plan::plan(
                Some(ctx.session().pcx()),
                &catalog,
                stmt,
                &mz_sql::plan::Params::empty(),
                &resolved_ids,
            )?;
            (plan, resolved_ids)
        };

        match plan {
            Plan::CreateTable(plan) => {
                self.sequence_create_table(ctx, plan, resolved_ids).await?;
            }
            Plan::CreateView(plan) => {
                self.create_generated_view(ctx.session(), plan, resolved_ids)
                    .await?;
            }
            other => {
                return Err(AdapterError::Internal(format!(
                    "ENRICH WITH generated an unexpected statement: {}",
                    other.name()
                )));
            }
        }
        Ok(())
    }

    /// Optimizes and installs one generated view.
    ///
    /// `sequence_create_view` cannot be reused: it consumes the `ExecuteContext` and
    /// drives the staged optimizer machinery, neither of which composes inside a loop
    /// that has more items to create afterwards. This is the same three steps without
    /// the staging.
    async fn create_generated_view(
        &mut self,
        session: &Session,
        plan: plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<(), AdapterError> {
        let (item_id, global_id) = self.allocate_user_id().await?;

        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());
        let mut optimizer =
            optimize::view::Optimizer::new(optimizer_config, Some(self.optimizer_metrics()));

        let raw_expr = plan.view.expr;
        let to_optimize = raw_expr.clone();
        let optimized_expr = mz_ore::task::spawn_blocking(
            || "optimize enriched view",
            move || optimizer.catch_unwind_optimize(to_optimize),
        )
        .await?;

        let typ = infer_sql_type_for_catalog(&raw_expr, &optimized_expr);
        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: plan.name,
            item: CatalogItem::View(View {
                create_sql: plan.view.create_sql,
                global_id,
                raw_expr: raw_expr.into(),
                desc: RelationDesc::new(typ, plan.view.column_names),
                locally_optimized_expr: optimized_expr.into(),
                // The generated objects are never temporary: they outlive the
                // session that declared the enrichment, as the relation does.
                conn_id: None,
                resolved_ids,
                dependencies: plan.view.dependencies,
            }),
            owner_id: *session.current_role_id(),
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }
}
