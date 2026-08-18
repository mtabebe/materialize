// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Capturing a dataflow's arrangements for a checkpoint.
//!
//! A capture is asked for, not taken. Nothing outside a dataflow holds a handle
//! to the arrangements inside it: a `reduce`'s output trace is reachable only
//! from within the operator. Acquiring a handle would be worse than useless
//! anyway, since `set_logical_compaction` only ever advances, so a handle
//! released after a capture could never read again and one kept would pin the
//! arrangement's compaction for the life of the process.
//!
//! Instead each arrangement leaves a capture slot behind when it is built.
//! [`DataflowCapture::request`] writes an as-of into every slot of one dataflow,
//! and each arrangement answers the next time its operator runs it, holding its
//! own compaction at the as-of until it has. That is also the earliest moment an
//! answer is possible, since a trace cannot report its contents as of a time its
//! upper has not passed.
//!
//! The capture runs on production's workers, where the arrangements are, and
//! never stops the live dataflow.
//!
//! Nothing calls this yet: the compute command that carries a capture request,
//! and the coordinator that issues it, are the next step.
#![allow(dead_code)]

pub mod format;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use differential_dataflow::trace::{Batch, TraceReader};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_row_spine::checkpoint::{
    ArrangementName, Capture, CaptureSlot, Captured, capturable, capture_batches, capture_slot,
};
use mz_row_spine::{DatumContainer, RowRowSpine};
use mz_timely_util::columnation::ColumnationStack;
use timely::progress::Antichain;

use crate::render::errors::DataflowErrorSer;
use crate::typedefs::ErrSpine;

type RowRowBatch = <RowRowSpine<Timestamp, Diff> as TraceReader>::Batch;
type ErrBatch = <ErrSpine<Timestamp, Diff> as TraceReader>::Batch;

/// One arrangement's contents, in whichever shape its spine holds them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedArrangement {
    /// A collection's `oks`, keyed and valued by [`Row`].
    Rows(Captured<Row, Row, Diff>),
    /// A collection's `errs`, keyed by the error and unvalued.
    Errors(Captured<DataflowErrorSer, (), Diff>),
}

/// Every arrangement of one dataflow, captured as of one time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedDataflow {
    /// The time every arrangement was read as of.
    pub as_of: Timestamp,
    /// Keyed by the name the plan gave the arrangement, so a restore of a
    /// different rendering of the same plan finds them again.
    pub arrangements: BTreeMap<ArrangementName, CapturedArrangement>,
}

/// Why a dataflow could not be captured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureError {
    /// The dataflow has no arrangements on this worker.
    NotArranged,
    /// An arrangement's spine holds a key and value shape a checkpoint cannot
    /// carry. Reported rather than skipped: a silently omitted arrangement
    /// restores as an empty one, which is a wrong answer rather than a slow one.
    UnsupportedShape {
        /// The arrangement that could not be read.
        name: ArrangementName,
    },
    /// An arrangement compacted past the requested time before it could answer,
    /// so it can no longer report its contents there. The caller must fall back
    /// to rebuilding.
    Compacted {
        /// The arrangement that had already compacted.
        name: ArrangementName,
        /// Its compaction frontier.
        since: Antichain<Timestamp>,
    },
}

/// A capture of one dataflow, in progress.
pub struct DataflowCapture {
    as_of: Timestamp,
    /// Arrangements that have yet to answer.
    waiting: BTreeMap<ArrangementName, Slot>,
    /// Answers so far. Held here rather than left in the slots, since a slot
    /// hands its answer over once.
    collected: BTreeMap<ArrangementName, CapturedArrangement>,
}

impl DataflowCapture {
    /// Asks every arrangement of `dataflow` on this worker for its contents as
    /// of `as_of`.
    pub fn request(dataflow: GlobalId, as_of: Timestamp) -> Result<Self, CaptureError> {
        let mut waiting = BTreeMap::new();
        for name in capturable() {
            if name.dataflow() != dataflow {
                continue;
            }
            let slot = Slot::find(name).ok_or(CaptureError::UnsupportedShape { name })?;
            slot.request(as_of);
            waiting.insert(name, slot);
        }
        if waiting.is_empty() {
            return Err(CaptureError::NotArranged);
        }
        Ok(Self {
            as_of,
            waiting,
            collected: BTreeMap::new(),
        })
    }

    /// Collects whatever has been answered, reporting the finished capture once
    /// every arrangement has.
    ///
    /// Call after stepping the worker. An arrangement answers when its operator
    /// next runs, so a capture as of a time the dataflow has not reached stays
    /// pending until it does.
    pub fn poll(&mut self) -> Result<Option<CapturedDataflow>, CaptureError> {
        let answered: Vec<_> = self
            .waiting
            .iter()
            .filter_map(|(name, slot)| Some((*name, slot.take(*name, self.as_of)?)))
            .collect();
        for (name, captured) in answered {
            self.waiting.remove(&name);
            self.collected.insert(name, captured?);
        }

        if !self.waiting.is_empty() {
            return Ok(None);
        }
        Ok(Some(CapturedDataflow {
            as_of: self.as_of,
            arrangements: std::mem::take(&mut self.collected),
        }))
    }
}

/// A handle to one arrangement's capture slot, in the shape that arrangement
/// holds.
///
/// The shapes are enumerated rather than erased because reading a trace needs
/// its key and value containers, which a spine does not carry. Adding a spine
/// flavour to the render path has to add it here too, and until it does a
/// capture of a dataflow using that flavour fails loudly.
enum Slot {
    Rows(Rc<RefCell<CaptureSlot<RowRowBatch>>>),
    Errors(Rc<RefCell<CaptureSlot<ErrBatch>>>),
}

impl Slot {
    fn find(name: ArrangementName) -> Option<Self> {
        if let Some(slot) = capture_slot::<RowRowBatch>(name) {
            return Some(Self::Rows(slot));
        }
        capture_slot::<ErrBatch>(name).map(Self::Errors)
    }

    fn request(&self, as_of: Timestamp) {
        match self {
            Self::Rows(slot) => slot.borrow_mut().request(as_of),
            Self::Errors(slot) => slot.borrow_mut().request(as_of),
        }
    }

    fn take(
        &self,
        name: ArrangementName,
        as_of: Timestamp,
    ) -> Option<Result<CapturedArrangement, CaptureError>> {
        match self {
            Self::Rows(slot) => {
                let batches = readable(slot.borrow_mut().take()?, name, as_of);
                Some(batches.map(|batches| {
                    CapturedArrangement::Rows(
                        capture_batches::<_, DatumContainer, DatumContainer, _>(batches, as_of),
                    )
                }))
            }
            Self::Errors(slot) => {
                let batches = readable(slot.borrow_mut().take()?, name, as_of);
                Some(batches.map(|batches| {
                    CapturedArrangement::Errors(capture_batches::<
                        _,
                        ColumnationStack<DataflowErrorSer>,
                        ColumnationStack<()>,
                        _,
                    >(batches, as_of))
                }))
            }
        }
    }
}

/// Checks that an answer can still speak for `as_of` before its batches are read.
///
/// A trace whose `since` has passed `as_of` has already coalesced the times we
/// would be reading, so its answer there is not the arrangement's contents.
fn readable<B: Batch<Time = Timestamp>>(
    capture: Capture<B>,
    name: ArrangementName,
    as_of: Timestamp,
) -> Result<Vec<B>, CaptureError> {
    if capture.since.less_equal(&as_of) {
        Ok(capture.batches)
    } else {
        Err(CaptureError::Compacted {
            name,
            since: capture.since,
        })
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::AsCollection;
    use mz_expr::EvalError;
    use mz_repr::Datum;
    use mz_row_spine::checkpoint::{self, restored_batch};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder};
    use mz_timely_util::columnation::ColumnationChunker;
    use timely::dataflow::operators::ToStream;

    use super::*;
    use crate::extensions::arrange::MzArrange;
    use crate::extensions::reduce::MzReduce;
    use crate::typedefs::{ErrBatcher, ErrBuilder};

    /// The arrangement `mz_arrange` builds, and the one `reduce_core` builds for
    /// its output. Numbered within their plan nodes, as the render loop does.
    const DATAFLOW: GlobalId = GlobalId::User(1);
    const INPUT: ArrangementName = ArrangementName::new(DATAFLOW, 1, 0);
    const REDUCED: ArrangementName = ArrangementName::new(DATAFLOW, 2, 0);

    type RowRowBuild = RowRowBuilder<Timestamp, Diff>;
    type Update = ((Row, Row), Timestamp, Diff);

    fn row(n: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(n)])
    }

    fn update(key: i64, val: i64, time: u64, diff: i64) -> Update {
        ((row(key), row(val)), Timestamp::new(time), Diff::from(diff))
    }

    fn rows(updates: &[((i64, i64), i64)], as_of: u64) -> CapturedArrangement {
        let updates = updates
            .iter()
            .map(|((k, v), d)| ((row(*k), row(*v)), Diff::from(*d)))
            .collect();
        CapturedArrangement::Rows(Captured::new(Timestamp::new(as_of), updates))
    }

    /// Runs a dataflow that arranges `updates` and counts the values per key,
    /// capturing every arrangement in it as of `as_of`.
    ///
    /// With `restore` set, the arrangements come up holding it and `updates`
    /// carries only what happened after the checkpoint, which is the shape a
    /// branch's source read gives (`SnapshotMode::Exclude` from the as-of).
    ///
    /// Nothing here holds a handle to either arrangement: the reduce's output
    /// trace lives inside the operator, and the input's is dropped with the
    /// `Arranged` the dataflow closure returns.
    fn run(
        updates: Vec<Update>,
        restore: Option<CapturedDataflow>,
        as_of: u64,
    ) -> CapturedDataflow {
        timely::execute_directly(move |worker| {
            let as_of = Timestamp::new(as_of);
            checkpoint::clear();
            for (name, arrangement) in restore.iter().flat_map(|r| &r.arrangements) {
                let CapturedArrangement::Rows(rows) = arrangement else {
                    panic!("this dataflow has only row arrangements");
                };
                checkpoint::publish(*name, restored_batch::<RowRowBuild>(rows));
            }

            worker.dataflow::<Timestamp, _, _>(|scope| {
                let arranged = checkpoint::with_node(DATAFLOW, INPUT.node(), || {
                    updates
                        .to_stream(scope)
                        .as_collection()
                        .mz_arrange::<ColumnationChunker<_>, RowRowBatcher<_, _>, RowRowBuild, RowRowSpine<_, _>>(
                            "Input",
                        )
                });
                checkpoint::with_node(DATAFLOW, REDUCED.node(), || {
                    arranged
                        .mz_reduce_abelian::<_, RowRowBuild, RowRowSpine<_, _>, DatumContainer>(
                            "Count",
                            |_key, input, output| {
                                let count = i64::try_from(input.len()).expect("small");
                                output.push((Row::pack_slice(&[Datum::Int64(count)]), Diff::ONE));
                            },
                        )
                });
            });
            assert!(checkpoint::unclaimed().is_empty());

            let mut capture = DataflowCapture::request(DATAFLOW, as_of).expect("arranged");
            loop {
                worker.step();
                if let Some(captured) = capture.poll().expect("capturable") {
                    return captured;
                }
            }
        })
    }

    #[mz_ore::test]
    fn capture_reads_every_arrangement_in_a_live_dataflow() {
        let captured = run(vec![update(1, 10, 0, 1), update(2, 20, 0, 2)], None, 0);

        assert_eq!(captured.as_of, Timestamp::new(0));
        assert_eq!(
            captured.arrangements,
            BTreeMap::from([
                (INPUT, rows(&[((1, 10), 1), ((2, 20), 2)], 0)),
                (REDUCED, rows(&[((1, 1), 1), ((2, 1), 1)], 0)),
            ])
        );
    }

    /// A dataflow restored from a checkpoint, then advanced, ends up exactly
    /// where the same dataflow rebuilt from scratch does.
    ///
    /// This covers the arrangement `reduce_core` builds for its own output,
    /// which is the case that decides the mechanism: nobody outside differential
    /// can write into it, so it can only be restored at construction.
    #[mz_ore::test]
    fn a_restored_dataflow_follows_forward() {
        let before = vec![update(1, 10, 0, 1), update(2, 20, 0, 1)];
        // A new value for key 1, so the count it already reported must be
        // retracted and replaced rather than accumulated onto.
        let after = vec![update(1, 11, 5, 1)];

        let checkpoint = run(before.clone(), None, 0);
        let restored = run(after.clone(), Some(checkpoint), 5);

        let rebuilt = run(before.into_iter().chain(after).collect(), None, 5);
        assert_eq!(restored, rebuilt);
        assert_eq!(
            restored.arrangements[&REDUCED],
            rows(&[((1, 2), 1), ((2, 1), 1)], 5)
        );
    }

    /// Without a restore the same dataflow sees only what arrived after the
    /// checkpoint, so the equivalence above is the restore's doing.
    #[mz_ore::test]
    fn an_unrestored_dataflow_is_missing_the_checkpoint() {
        let restored = run(vec![update(1, 11, 5, 1)], None, 5);
        assert_eq!(restored.arrangements[&REDUCED], rows(&[((1, 1), 1)], 5));
    }

    /// The `errs` half restores too. It is a separate arrangement over a
    /// different key type, so restoring only `oks` would silently drop the
    /// errors a collection has.
    #[mz_ore::test]
    fn a_restored_dataflow_keeps_its_errors() {
        fn err(msg: &str) -> DataflowErrorSer {
            DataflowErrorSer::from(EvalError::Internal(msg.into()))
        }
        type ErrUpdate = ((DataflowErrorSer, ()), Timestamp, Diff);
        type ErrCaptured = Captured<DataflowErrorSer, (), Diff>;

        fn run_errs(
            updates: Vec<ErrUpdate>,
            restore: Option<ErrCaptured>,
            as_of: u64,
        ) -> ErrCaptured {
            timely::execute_directly(move |worker| {
                let as_of = Timestamp::new(as_of);
                checkpoint::clear();
                if let Some(errs) = &restore {
                    checkpoint::publish(INPUT, restored_batch::<ErrBuilder<_, _>>(errs));
                }
                // The handle is kept because nothing else reads this
                // arrangement, and an arrangement with no reader is dropped
                // with the dataflow's closure. An exported one is held by the
                // trace manager for the same reason.
                let _exported = worker.dataflow::<Timestamp, _, _>(|scope| {
                    let arranged = checkpoint::with_node(DATAFLOW, INPUT.node(), || {
                        updates
                            .to_stream(scope)
                            .as_collection()
                            .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
                                "Errs",
                            )
                    });
                    arranged.trace.clone()
                });

                let mut capture = DataflowCapture::request(DATAFLOW, as_of).expect("arranged");
                let captured = loop {
                    worker.step();
                    if let Some(captured) = capture.poll().expect("capturable") {
                        break captured;
                    }
                };

                match &captured.arrangements[&INPUT] {
                    CapturedArrangement::Errors(errs) => errs.clone(),
                    CapturedArrangement::Rows(_) => panic!("an error arrangement"),
                }
            })
        }

        let before = vec![((err("first"), ()), Timestamp::new(0), Diff::ONE)];
        let after = vec![((err("second"), ()), Timestamp::new(5), Diff::ONE)];

        let checkpoint = run_errs(before.clone(), None, 0);
        assert_eq!(checkpoint.len(), 1);

        let restored = run_errs(after.clone(), Some(checkpoint), 5);
        let rebuilt = run_errs(before.into_iter().chain(after).collect(), None, 5);
        assert_eq!(restored, rebuilt);
        assert_eq!(restored.len(), 2);
    }
}
