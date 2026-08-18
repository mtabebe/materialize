// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deciding whether a dataflow's arrangements are worth checkpointing, and
//! whether a checkpoint still matches the plan that would consume it.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hasher};

use mz_expr::Id;
use mz_ore::cast::CastFrom;
use mz_repr::GlobalId;
use serde::{Deserialize, Serialize};

use crate::plan::render_plan::{Expr, RenderPlan};

/// Identifies the shape of a render plan, ignoring which collections it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanFingerprint(u64);

impl PlanFingerprint {
    /// Fingerprints `plan` modulo the global ids it references.
    ///
    /// A branch's plan differs from production's in its `GlobalId`s by
    /// construction, so comparing plans directly would reject every checkpoint.
    /// Ids are renumbered in first-appearance order instead, so two plans stay
    /// distinct whenever they read *differently* rather than merely elsewhere.
    ///
    /// NOTE: meaningful only within one binary. It hashes a serialization with
    /// a hasher whose output is not stable across Rust or Materialize versions.
    /// That is sound because a checkpoint's batch encodings are not stable
    /// across versions either, so a checkpoint never outlives the binary that
    /// wrote it.
    pub fn of(plan: &RenderPlan) -> Self {
        let mut plan = plan.clone();
        let mut renumbered: BTreeMap<GlobalId, GlobalId> = BTreeMap::new();
        plan.replace_ids(&mut |id| {
            let next = GlobalId::Transient(u64::cast_from(renumbered.len()));
            *renumbered.entry(id).or_insert(next)
        });

        // Serialized rather than debug-formatted: `Serialize` is derived for
        // every node, so no field can be quietly left out of the comparison,
        // which a hand-written `Debug` could do.
        let encoded = serde_json::to_vec(&plan).expect("render plan is serializable");
        let mut hasher = DefaultHasher::new();
        hasher.write(&encoded);
        Self(hasher.finish())
    }
}

/// Whether checkpointing the arrangements of `plan` pays for itself.
///
/// A checkpoint trades `write(state) + read(state) + build` for
/// `read(inputs) + sort + recompute`, and the write sits on the critical path
/// to readiness, so it pays only when the state it captures is small relative
/// to the recomputation it skips.
///
/// The dividing line is whether the plan has an operator whose output is
/// smaller than its input. Without one, the arrangement is the same size as the
/// input, a branch's fork already shares production's blobs so there is no read
/// to save, and the checkpoint's write only makes the branch ready *later*.
pub fn worth_checkpointing(plan: &RenderPlan) -> bool {
    plan.exprs().any(|expr| match expr {
        Expr::Reduce { .. } | Expr::TopK { .. } | Expr::Threshold { .. } | Expr::Join { .. } => {
            true
        }
        Expr::Constant { .. }
        | Expr::Get { .. }
        | Expr::Mfp { .. }
        | Expr::FlatMap { .. }
        | Expr::Negate { .. }
        | Expr::Union { .. }
        | Expr::ArrangeBy { .. } => false,
    })
}

/// Selects, from every collection a cluster maintains, those whose arrangements
/// may be checkpointed.
///
/// Two conditions, both necessary:
///
///  * The plan must be worth checkpointing at all ([`worth_checkpointing`]).
///  * Nothing else on the cluster may import the collection. A restored
///    arrangement holds its restored contents outside the batch stream it
///    replays to an importer, so an importing dataflow would see only what
///    arrived after the as-of. Restoring the importer instead does not help:
///    it would then have the import's history replayed into arrangements that
///    already hold its effect. The restriction is symmetric, so the only safe
///    scope is a collection nobody imports.
pub fn checkpointable(plans: &BTreeMap<GlobalId, RenderPlan>) -> BTreeSet<GlobalId> {
    let imported: BTreeSet<_> = plans
        .iter()
        .flat_map(|(id, plan)| {
            plan.depends()
                .into_iter()
                .filter_map(|dep| match dep {
                    Id::Global(dep) => Some(dep),
                    Id::Local(_) => None,
                })
                // A plan referring to its own export is not an importer of it.
                .filter(move |dep| dep != id)
                .collect::<Vec<_>>()
        })
        .collect();

    plans
        .iter()
        .filter(|(id, plan)| worth_checkpointing(plan) && !imported.contains(id))
        .map(|(id, _)| *id)
        .collect()
}

#[cfg(test)]
mod tests {
    use mz_expr::MapFilterProject;

    use super::*;
    use crate::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
    use crate::plan::{
        ArrangementStrategy, AvailableCollections, GetPlan, LirId, LirRelationExpr, LirRelationNode,
    };

    fn node(id: u64, node: LirRelationNode) -> LirRelationExpr {
        LirRelationExpr {
            lir_id: LirId::from(id),
            node,
        }
    }

    fn get(id: GlobalId) -> LirRelationExpr {
        node(
            1,
            LirRelationNode::Get {
                id: Id::Global(id),
                keys: AvailableCollections::new_raw(),
                plan: GetPlan::Collection(MapFilterProject::new(1).into_plan().expect("linear")),
            },
        )
    }

    fn arrange_by(input: LirRelationExpr) -> RenderPlan {
        let plan = node(
            2,
            LirRelationNode::ArrangeBy {
                input: Box::new(input),
                input_key: None,
                input_mfp: MapFilterProject::new(1).into_plan().expect("linear"),
                forms: AvailableCollections::new_raw(),
                strategy: ArrangementStrategy::Direct,
            },
        );
        RenderPlan::try_from(plan).expect("valid plan")
    }

    fn threshold(input: LirRelationExpr) -> RenderPlan {
        let plan = node(
            2,
            LirRelationNode::Threshold {
                input: Box::new(input),
                threshold_plan: ThresholdPlan::Basic(BasicThresholdPlan {
                    ensure_arrangement: (Vec::new(), Vec::new(), Vec::new()),
                }),
            },
        );
        RenderPlan::try_from(plan).expect("valid plan")
    }

    #[mz_ore::test]
    fn an_index_on_a_table_is_not_worth_checkpointing() {
        assert!(!worth_checkpointing(&arrange_by(get(GlobalId::User(1)))));
    }

    #[mz_ore::test]
    fn an_index_over_an_operator_that_shrinks_its_input_is() {
        assert!(worth_checkpointing(&threshold(get(GlobalId::User(1)))));
    }

    #[mz_ore::test]
    fn the_same_plan_over_a_different_collection_fingerprints_the_same() {
        let one = arrange_by(get(GlobalId::User(1)));
        let other = arrange_by(get(GlobalId::User(77)));
        assert_eq!(PlanFingerprint::of(&one), PlanFingerprint::of(&other));
    }

    #[mz_ore::test]
    fn a_collection_another_dataflow_imports_is_not_checkpointable() {
        let alone = GlobalId::User(1);
        let imported = GlobalId::User(2);
        let plans = BTreeMap::from([
            (alone, threshold(get(GlobalId::User(10)))),
            (imported, threshold(get(GlobalId::User(11)))),
            (GlobalId::User(3), arrange_by(get(imported))),
        ]);

        assert_eq!(checkpointable(&plans), BTreeSet::from([alone]));
    }

    #[mz_ore::test]
    fn a_different_plan_over_the_same_collection_fingerprints_differently() {
        let arranged = arrange_by(get(GlobalId::User(1)));
        let thresholded = threshold(get(GlobalId::User(1)));
        assert_ne!(
            PlanFingerprint::of(&arranged),
            PlanFingerprint::of(&thresholded)
        );
    }
}
