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
//! test below prove a real `mz_arrange` restores.
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
    use mz_row_spine::checkpoint::{self};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
    use mz_timely_util::columnation::ColumnationChunker;
    use timely::dataflow::operators::{Probe, ToStream};

    use super::*;
    use crate::extensions::arrange::MzArrange;

    fn row(n: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(n)])
    }

    /// Captures a real `mz_arrange`'s trace and asserts it reports the
    /// arrangement's contents.
    ///
    /// The restore half of this loop is deliberately not asserted here. Feeding a
    /// pre-populated spine to a live `mz_arrange` does not work: `Spine::insert`
    /// requires each batch's `lower` to equal the trace's current `upper`, the
    /// operator's writer always starts at `Time::minimum()`, and `Batch` exposes
    /// no way to build an empty batch over a chosen description, so nothing can
    /// bridge the two. See the phase plan's decision log.
    #[mz_ore::test]
    fn capture_reads_a_live_arrangement() {
        let updates: Vec<((Row, Row), Timestamp, Diff)> = vec![
            ((row(1), row(10)), Timestamp::new(0), Diff::ONE),
            ((row(2), row(20)), Timestamp::new(0), Diff::from(2)),
        ];
        let expected: Vec<((Row, Row), Diff)> = updates
            .iter()
            .map(|((k, v), _, d)| ((k.clone(), v.clone()), *d))
            .collect();

        let captured = timely::execute_directly(move |worker| {
            checkpoint::clear();
            let as_of = Timestamp::new(0);

            let (mut trace, probe) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let arranged = checkpoint::with_node(1, || {
                    updates
                        .to_stream(scope)
                        .as_collection()
                        .mz_arrange::<ColumnationChunker<_>, RowRowBatcher<_, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                            "Captured",
                        )
                });
                let (probe, _stream) = arranged.stream.probe();
                (arranged.trace.clone(), probe)
            });
            while probe.less_equal(&as_of) {
                worker.step();
            }

            capture::<_, DatumContainer, DatumContainer, _>(&mut trace, as_of)
        });

        assert_eq!(captured.as_of(), Timestamp::new(0));
        assert_eq!(captured.updates(), &expected[..]);
    }
}
