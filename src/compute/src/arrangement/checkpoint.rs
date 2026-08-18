// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Capturing a collection's arrangements for a checkpoint.
//!
//! Captures read through the handles the trace manager already holds. Acquiring
//! a handle for a capture alone is not an option: `set_logical_compaction` only
//! ever advances, so a handle released after a capture could never read again,
//! and one kept would pin its arrangement's compaction for the life of the
//! process. An exported arrangement is already pinned to serve peeks, so reading
//! it costs no additional hold.
//!
//! This is why only *exported* arrangements are capturable today. An arrangement
//! internal to a plan node is held by nobody once its dataflow is built, by
//! design, so capturing one needs an in-dataflow reader that tracks the trace's
//! own compaction frontier and pauses it only for the capture window.
//!
//! `capture_exported` has no caller yet: the coordinator side that requests a
//! capture is the next step, and landing the read path first is what lets the
//! tests below prove a whole dataflow restores.
#![allow(dead_code)]

use differential_dataflow::trace::TraceReader;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_row_spine::DatumContainer;
use mz_row_spine::checkpoint::{Captured, capture};
use mz_timely_util::columnation::ColumnationStack;
use timely::progress::Antichain;

use crate::arrangement::manager::TraceManager;
use crate::render::errors::DataflowErrorSer;

/// A collection's arrangements, captured as of one time.
///
/// Both halves are always present. Restoring `oks` alone would silently drop the
/// errors the collection holds, so they travel together.
#[derive(Clone, Debug)]
pub struct CapturedCollection {
    /// The captured `oks` arrangement.
    pub oks: Captured<Row, Row, Diff>,
    /// The captured `errs` arrangement. Usually empty.
    pub errs: Captured<DataflowErrorSer, (), Diff>,
}

/// Why a collection could not be captured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureError {
    /// The collection has no arrangements on this worker.
    NotArranged,
    /// An arrangement has compacted past the requested time, so it can no longer
    /// report its contents there. The caller must fall back to rebuilding.
    Compacted {
        /// The arrangement's compaction frontier.
        since: Antichain<Timestamp>,
    },
}

/// Captures the arrangements of exported collection `id` as of `as_of`.
///
/// Reads only, so the live dataflow is never stopped.
pub fn capture_exported(
    traces: &mut TraceManager,
    id: &GlobalId,
    as_of: Timestamp,
) -> Result<CapturedCollection, CaptureError> {
    let bundle = traces.get_mut(id).ok_or(CaptureError::NotArranged)?;

    // A trace whose `since` has passed `as_of` has already coalesced the times
    // we would be reading, so its answer there is not the collection's contents.
    for since in [
        bundle.oks_mut().get_logical_compaction().to_owned(),
        bundle.errs_mut().get_logical_compaction().to_owned(),
    ] {
        if !since.less_equal(&as_of) {
            return Err(CaptureError::Compacted { since });
        }
    }

    let oks = capture::<_, DatumContainer, DatumContainer, _>(bundle.oks_mut(), as_of);
    let errs = capture::<_, ColumnationStack<DataflowErrorSer>, ColumnationStack<()>, _>(
        bundle.errs_mut(),
        as_of,
    );
    Ok(CapturedCollection { oks, errs })
}

#[cfg(test)]
mod tests {
    use differential_dataflow::AsCollection;
    use mz_repr::{Datum, Row};
    use mz_row_spine::checkpoint::{self, ArrangementName, restored_batch};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
    use mz_timely_util::columnation::ColumnationChunker;
    use timely::dataflow::operators::{Probe, ToStream};

    use super::*;
    use mz_expr::EvalError;

    use crate::extensions::arrange::MzArrange;
    use crate::extensions::reduce::MzReduce;
    use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine};

    /// The arrangement `mz_arrange` builds, and the one `reduce_core` builds for
    /// its output. Numbered within their plan nodes, as the render loop does.
    const INPUT: ArrangementName = ArrangementName::new(1, 0);
    const REDUCED: ArrangementName = ArrangementName::new(2, 0);

    type RowRowBuild = RowRowBuilder<Timestamp, Diff>;
    type Update = ((Row, Row), Timestamp, Diff);

    fn row(n: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(n)])
    }

    fn update(key: i64, val: i64, time: u64, diff: i64) -> Update {
        ((row(key), row(val)), Timestamp::new(time), Diff::from(diff))
    }

    fn captured(updates: &[((i64, i64), i64)], as_of: u64) -> Captured<Row, Row, Diff> {
        let updates = updates
            .iter()
            .map(|((k, v), d)| ((row(*k), row(*v)), Diff::from(*d)))
            .collect();
        Captured::new(Timestamp::new(as_of), updates)
    }

    /// Runs a dataflow that arranges `updates` and counts the values per key,
    /// returning both arrangements as of `as_of`.
    ///
    /// With `restore` set, both arrangements come up holding it and `updates`
    /// carries only what happened after the checkpoint, which is the shape a
    /// branch's source read gives (`SnapshotMode::Exclude` from the as-of).
    fn run(
        updates: Vec<Update>,
        restore: Option<(Captured<Row, Row, Diff>, Captured<Row, Row, Diff>)>,
        as_of: u64,
    ) -> (Captured<Row, Row, Diff>, Captured<Row, Row, Diff>) {
        timely::execute_directly(move |worker| {
            let as_of = Timestamp::new(as_of);
            checkpoint::clear();
            if let Some((input, reduced)) = &restore {
                checkpoint::publish(INPUT, restored_batch::<RowRowBuild>(input));
                checkpoint::publish(REDUCED, restored_batch::<RowRowBuild>(reduced));
            }

            let (mut input, mut reduced, probe) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let arranged = checkpoint::with_node(INPUT.node(), || {
                    updates
                        .to_stream(scope)
                        .as_collection()
                        .mz_arrange::<ColumnationChunker<_>, RowRowBatcher<_, _>, RowRowBuild, RowRowSpine<_, _>>(
                            "Input",
                        )
                });
                let input = arranged.trace.clone();
                let reduced = checkpoint::with_node(REDUCED.node(), || {
                    arranged
                        .mz_reduce_abelian::<_, RowRowBuild, RowRowSpine<_, _>, DatumContainer>(
                            "Count",
                            |_key, input, output| {
                                let count = i64::try_from(input.len()).expect("small");
                                output.push((Row::pack_slice(&[Datum::Int64(count)]), Diff::ONE));
                            },
                        )
                });
                let (probe, _stream) = reduced.stream.probe();
                (input, reduced.trace.clone(), probe)
            });
            while probe.less_equal(&as_of) {
                worker.step();
            }
            assert!(checkpoint::unclaimed().is_empty());

            (
                capture::<_, DatumContainer, DatumContainer, _>(&mut input, as_of),
                capture::<_, DatumContainer, DatumContainer, _>(&mut reduced, as_of),
            )
        })
    }

    #[mz_ore::test]
    fn capture_reads_a_live_arrangement() {
        let updates = vec![update(1, 10, 0, 1), update(2, 20, 0, 2)];
        let (input, reduced) = run(updates, None, 0);

        assert_eq!(input, captured(&[((1, 10), 1), ((2, 20), 2)], 0));
        assert_eq!(reduced, captured(&[((1, 1), 1), ((2, 1), 1)], 0));
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
        assert_eq!(restored.1, captured(&[((1, 2), 1), ((2, 1), 1)], 5));
    }

    /// Without a restore the same dataflow sees only what arrived after the
    /// checkpoint, so the equivalence above is the restore's doing.
    #[mz_ore::test]
    fn an_unrestored_dataflow_is_missing_the_checkpoint() {
        let restored = run(vec![update(1, 11, 5, 1)], None, 5);
        assert_eq!(restored.1, captured(&[((1, 1), 1)], 5));
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
                let (mut trace, probe) = worker.dataflow::<Timestamp, _, _>(|scope| {
                    let arranged = checkpoint::with_node(INPUT.node(), || {
                        updates
                            .to_stream(scope)
                            .as_collection()
                            .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
                                "Errs",
                            )
                    });
                    let (probe, _stream) = arranged.stream.probe();
                    (arranged.trace.clone(), probe)
                });
                while probe.less_equal(&as_of) {
                    worker.step();
                }
                capture::<_, ColumnationStack<DataflowErrorSer>, ColumnationStack<()>, _>(
                    &mut trace, as_of,
                )
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
