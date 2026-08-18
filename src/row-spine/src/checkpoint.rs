// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Capturing an arrangement's contents and restoring them into a fresh trace.
//!
//! An arrangement is the state of the operator that maintains it: a `join`'s
//! persistent state is its two input arrangements and nothing else, and a
//! `reduce`'s is its source trace plus the output trace it builds. So capturing
//! every arrangement in a dataflow captures everything needed to resume it,
//! rather than rebuild it.
//!
//! Restoring works without any change to differential because Materialize
//! declares its own spine types. Differential constructs every trace through
//! [`Trace::new`], including ones it creates internally (a `reduce`'s output
//! trace comes from `Tr2::new(..)` inside `reduce_core`), so a spine whose
//! `new` consults [`RestoreRegistry`] reaches those as well as the ones we
//! build ourselves.
//!
//! # Restored state sits beside the spine, not inside it
//!
//! Restored contents are held as a *base batch* the wrapper owns, not inserted
//! into the spine. A spine's batches must tile `[minimum, upper)` with no gap,
//! and the operator that writes into a trace always starts its writer at
//! `Time::minimum()` regardless of what the trace already holds
//! (`TraceAgent::new` hardcodes it, and `reduce_core` starts its own
//! `lower_limit` there). A batch covering `[minimum, as_of + 1)` inside the
//! spine therefore collides with the operator's first append, and neither the
//! batch nor the append can be retagged.
//!
//! Beside the spine, none of that arises: the spine tiles from `minimum`
//! exactly as it would with no restore, and the base batch is folded in on the
//! read path. The base batch's description is `[minimum, minimum)`, which says
//! what it is: state that predates the interval accounting rather than a step
//! within it. That is also what makes it unconditionally readable, since no
//! query frontier can straddle an empty interval.
//!
//! # The base batch is read-visible and replay-invisible
//!
//! [`TraceReader::batches_through`] includes it, so every accumulated-state read
//! sees it: a `join` joining a new batch against the other side, a `reduce`
//! reading its source trace and differencing against its prior output.
//!
//! [`TraceReader::map_batches`] does not, so it is invisible to
//! `TraceAgent::new_listener`, which replays a trace's batches to prime a
//! late-attaching consumer. Replaying it would be a double apply: the consumer
//! is itself restored and already holds the result of that data. Excluding it is
//! also what keeps `read_upper` and `advance_upper` reporting the writer's own
//! upper, which is what their callers mean.
//!
//! **This is why a restored arrangement must not be imported by a dataflow that
//! is not itself restored.** An import primes through `map_batches`, so the
//! importer would see only what arrived after the as-of. Exported arrangements,
//! which anything may import later, are therefore restored by feeding their
//! captured contents in as operator input instead, which puts them inside the
//! spine where they behave like any other batch. This wrapper is for the
//! arrangements internal to a dataflow, which nothing outside it can import.
//!
//! # Cost
//!
//! The base batch never merges with the spine's batches, so a key retracted
//! after the restore keeps both its base entry and its retraction rather than
//! annihilating. It is already consolidated at one time, so nothing is lost to
//! missed compaction, but the space is held for the arrangement's lifetime.
//!
//! A capture holds no history below its as-of. The restored trace's `since` is
//! the as-of, and nothing reads a trace below its `since`, so history would be
//! dead weight.

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;

use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::{
    Batch, Builder, Cursor, Description, ExertionLogic, Navigable, Trace, TraceReader,
};
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::columnation::ColumnationStack;
use timely::PartialOrder;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, Timestamp as _, frontier::AntichainRef};

/// Names an arrangement within a dataflow, stably across the capture that
/// writes it and the restore that consumes it.
///
/// A name is the plan node that built the arrangement plus which arrangement it
/// is among those that node built. Both halves come from the plan, so the same
/// plan names the same arrangements. Timely's operator address would be free,
/// since it arrives in [`Trace::new`], but it is positional across the whole
/// dataflow: an unrelated structural change shifts every address after it, and
/// the restore then populates a trace from a different arrangement without
/// complaining.
///
/// `node` is a plan node id, kept as a `u64` so this crate does not depend on
/// the compute plan types. Callers pass `LirId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArrangementName {
    node: u64,
    ordinal: u64,
}

impl ArrangementName {
    /// Names the `ordinal`th arrangement built by plan node `node`.
    pub const fn new(node: u64, ordinal: u64) -> Self {
        Self { node, ordinal }
    }

    /// The plan node that built the arrangement.
    pub fn node(&self) -> u64 {
        self.node
    }
}

impl std::fmt::Display for ArrangementName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}/{}", self.node, self.ordinal)
    }
}

/// One arrangement's contents, consolidated as of a single time.
///
/// Every update carries `as_of` as its time, so the time is stored once rather
/// than per update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Captured<K = Row, V = Row, D = Diff> {
    as_of: Timestamp,
    /// Sorted by `(key, val)`, consolidated, and free of zero diffs.
    updates: Vec<((K, V), D)>,
}

impl<K, V, D> Captured<K, V, D> {
    /// The time every update in this capture is stamped with.
    pub fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// The number of updates captured.
    pub fn len(&self) -> usize {
        self.updates.len()
    }

    /// Whether the arrangement was empty as of the capture.
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// The captured updates, sorted by `(key, val)`.
    pub fn updates(&self) -> &[((K, V), D)] {
        &self.updates
    }

    /// Builds a capture directly. For tests and for decoding a stored checkpoint.
    pub fn new(as_of: Timestamp, updates: Vec<((K, V), D)>) -> Self {
        Self { as_of, updates }
    }
}

/// Reads `trace` as of `as_of`, consolidating its history into updates stamped
/// `as_of`.
///
/// This is a read: the live dataflow is never stopped. The one thing it needs
/// from the trace is `since <= as_of`, which holds while `as_of` is near the
/// dataflow's frontier. A trace compacted past `as_of` cannot answer, and the
/// caller must fall back to rebuilding.
pub fn capture<Tr, KC, VC, D>(trace: &mut Tr, as_of: Timestamp) -> Captured<KC::Owned, VC::Owned, D>
where
    Tr: TraceReader<Time = Timestamp>,
    Tr::Batch: Navigable,
    KC: BatchContainer,
    VC: BatchContainer,
    D: Semigroup + Clone,
    for<'a> <Tr::Batch as Navigable>::Cursor:
        Cursor<Time = Timestamp, Diff = D, Key<'a> = KC::ReadItem<'a>, Val<'a> = VC::ReadItem<'a>>,
{
    let (mut cursor, storage) = trace.cursor();
    let mut updates: Vec<((KC::Owned, VC::Owned), D)> = Vec::new();

    while let Some(key) = cursor.get_key(&storage) {
        let key = KC::into_owned(key);
        while let Some(val) = cursor.get_val(&storage) {
            let val = VC::into_owned(val);
            if let Some(diff) = accumulate(&mut cursor, &storage, &as_of) {
                updates.push(((key.clone(), val), diff));
            }
            cursor.step_val(&storage);
        }
        cursor.step_key(&storage);
    }

    Captured { as_of, updates }
}

/// Accumulates the history of the cursor's current `(key, val)` at or below
/// `as_of` into one diff, dropping it if it cancels to zero.
fn accumulate<C>(cursor: &mut C, storage: &C::Storage, as_of: &C::Time) -> Option<C::Diff>
where
    C: Cursor,
    C::Time: Lattice,
    C::Diff: Semigroup + Clone,
{
    let mut total: Option<C::Diff> = None;
    cursor.map_times(storage, |time, diff| {
        if C::owned_time(time).less_equal(as_of) {
            let diff = C::owned_diff(diff);
            match &mut total {
                Some(total) => total.plus_equals(&diff),
                None => total = Some(diff),
            }
        }
    });
    total.filter(|total| !total.is_zero())
}

/// Rebuilds a capture's contents into the single base batch a restore holds
/// beside its spine.
///
/// The description is `[minimum, minimum)` with `since = as_of`: an empty
/// interval, because this batch is not a step in the trace's interval
/// accounting but the state that predates it. The updates inside are all
/// stamped `as_of`, which is above that interval, so this batch is only ever
/// correct beside a spine rather than in one.
pub fn restored_batch<Bu>(
    captured: &Captured<
        <Bu::Input as StackOf>::Key,
        <Bu::Input as StackOf>::Val,
        <Bu::Input as StackOf>::Diff,
    >,
) -> Bu::Output
where
    Bu: Builder<Time = Timestamp>,
    Bu::Input: StackOf,
{
    let description = Description::new(
        Antichain::from_elem(Timestamp::minimum()),
        Antichain::from_elem(Timestamp::minimum()),
        Antichain::from_elem(captured.as_of),
    );

    let mut chunk = Bu::Input::with_updates(captured.as_of, &captured.updates);
    let mut builder = Bu::with_capacity(0, 0, captured.updates.len());
    builder.push(&mut chunk);
    builder.done(description)
}

/// A builder input chunk that can be filled from captured updates.
///
/// Exists so [`rebuild_batch`] does not have to name the columnation stack a
/// given builder consumes.
pub trait StackOf {
    /// The key type the chunk carries.
    type Key;
    /// The value type the chunk carries.
    type Val;
    /// The diff type the chunk carries.
    type Diff;
    /// Builds a chunk holding `updates`, all stamped `as_of`.
    fn with_updates(as_of: Timestamp, updates: &[((Self::Key, Self::Val), Self::Diff)]) -> Self;
}

impl<K, V, D> StackOf for ColumnationStack<((K, V), Timestamp, D)>
where
    K: Clone + columnation::Columnation + 'static,
    V: Clone + columnation::Columnation + 'static,
    D: Clone + columnation::Columnation + 'static,
{
    type Key = K;
    type Val = V;
    type Diff = D;

    fn with_updates(as_of: Timestamp, updates: &[((K, V), D)]) -> Self {
        let mut stack = ColumnationStack::with_capacity(updates.len());
        for ((key, val), diff) in updates {
            stack.copy(&((key.clone(), val.clone()), as_of, diff.clone()));
        }
        stack
    }
}

/// Per-worker store of arrangement state waiting to be restored.
///
/// Lives in a thread-local because [`Trace::new`] is called deep inside
/// operator construction with no channel to pass state through, and a timely
/// worker is a thread.
#[derive(Default)]
pub struct RestoreRegistry {
    /// Keyed by name, each entry holds the base batch for one arrangement.
    pending: BTreeMap<ArrangementName, Box<dyn Any>>,
    /// The plan nodes currently being rendered, innermost last, each with the
    /// number of arrangements built under it so far. Set by [`with_node`].
    building: Vec<(u64, u64)>,
}

thread_local! {
    static REGISTRY: RefCell<RestoreRegistry> = RefCell::new(RestoreRegistry::default());
}

/// Publishes `batch` as the state for `name` on this worker.
///
/// The spine that claims `name` while its plan node renders comes up holding
/// it. `B` must match that spine's batch type exactly; a mismatch leaves the
/// state unclaimed rather than restoring the wrong thing.
pub fn publish<B: 'static>(name: ArrangementName, batch: B) {
    REGISTRY.with_borrow_mut(|registry| {
        registry.pending.insert(name, Box::new(batch));
    });
}

/// Renders plan node `node`, naming every arrangement built inside `f` after it.
///
/// Arrangements are numbered in construction order within the node, which is
/// determined by the node's plan, so the same plan numbers them the same way.
/// Nests, so a node whose rendering renders another still numbers each
/// separately.
pub fn with_node<T>(node: u64, f: impl FnOnce() -> T) -> T {
    REGISTRY.with_borrow_mut(|registry| registry.building.push((node, 0)));
    let result = f();
    REGISTRY.with_borrow_mut(|registry| {
        registry.building.pop();
    });
    result
}

/// Whether any state is still waiting to be claimed on this worker.
///
/// A non-empty result after a dataflow is built means a name mismatch: state was
/// published that no spine asked for.
pub fn unclaimed() -> Vec<ArrangementName> {
    REGISTRY.with_borrow(|registry| registry.pending.keys().cloned().collect())
}

/// Clears this worker's registry. For tests, and for abandoning a restore.
pub fn clear() {
    REGISTRY.with_borrow_mut(|registry| {
        registry.pending.clear();
        registry.building.clear();
    });
}

/// Assigns the next name under the plan node being rendered, and takes any state
/// published for it.
///
/// The name is consumed whether or not state was published for it. Skipping the
/// ordinal when nothing is restored would renumber the arrangements after it, so
/// a checkpoint of a subset would restore into the wrong ones.
fn claim<B: 'static>() -> (Option<ArrangementName>, Option<B>) {
    REGISTRY.with_borrow_mut(|registry| {
        let Some((node, ordinal)) = registry.building.last_mut() else {
            return (None, None);
        };
        let name = ArrangementName::new(*node, *ordinal);
        *ordinal += 1;

        let Some(entry) = registry.pending.remove(&name) else {
            return (Some(name), None);
        };
        match entry.downcast::<B>() {
            Ok(batch) => (Some(name), Some(*batch)),
            Err(entry) => {
                // Put it back rather than dropping it, so `unclaimed` reports the
                // mismatch instead of the restore silently starting empty.
                registry.pending.insert(name, entry);
                (Some(name), None)
            }
        }
    })
}

/// A spine that comes up holding a base batch when this worker has state
/// published for the arrangement being constructed.
///
/// The spine itself is untouched by a restore, so the operator writing into it
/// tiles from `Time::minimum()` exactly as it would with no restore. See the
/// module docs for why the base batch sits beside the spine, and for the
/// read-visible/replay-invisible split that makes it safe.
pub struct RestorableSpine<B: Batch> {
    inner: Spine<B>,
    /// The name this spine claimed, absent when it was built outside any
    /// [`with_node`] scope (a dataflow that is not participating).
    name: Option<ArrangementName>,
    /// The restored contents, outside the spine's interval accounting.
    base: Option<B>,
}

impl<B> RestorableSpine<B>
where
    B: Batch + Clone + 'static,
{
    /// Whether this spine came up populated from a checkpoint.
    pub fn was_restored(&self) -> bool {
        self.base.is_some()
    }

    /// The name this spine claimed while its plan node rendered.
    pub fn name(&self) -> Option<ArrangementName> {
        self.name
    }
}

impl<B> TraceReader for RestorableSpine<B>
where
    B: Batch + Clone + 'static,
    B::Time: Lattice + Ord + Clone,
{
    type Time = B::Time;
    type Batch = B;

    fn batches_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Self::Batch>> {
        let mut batches = self.inner.batches_through(upper)?;
        // Included whatever `upper` is: the base batch is the state the trace
        // held before this dataflow's accounting begins, so every cut over the
        // trace includes it. Its empty description is what makes that sound,
        // since no frontier can straddle an empty interval.
        match &self.base {
            Some(base) if !base.is_empty() => batches.push(base.clone()),
            _ => {}
        }
        Some(batches)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.inner.set_logical_compaction(frontier)
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_logical_compaction()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>) {
        self.inner.set_physical_compaction(frontier)
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_physical_compaction()
    }

    /// Does **not** yield the base batch. Callers use this to replay the batches
    /// that moved past the trace and to read the writer's upper, and the base
    /// batch is neither.
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.inner.map_batches(f)
    }
}

impl<B> Trace for RestorableSpine<B>
where
    B: Batch + Clone + 'static,
    B::Time: Lattice + Ord + Clone,
{
    fn new(info: OperatorInfo, logging: Option<Logger>, activator: Option<Activator>) -> Self {
        let inner = Spine::new(info, logging, activator);
        let (name, base) = claim::<B>();
        Self { inner, name, base }
    }

    fn exert(&mut self) {
        self.inner.exert()
    }

    fn set_exert_logic(&mut self, logic: ExertionLogic) {
        self.inner.set_exert_logic(logic)
    }

    fn insert(&mut self, batch: Self::Batch) {
        self.inner.insert(batch)
    }

    fn close(&mut self) {
        self.inner.close()
    }
}

type Logger = differential_dataflow::logging::Logger;
type Activator = timely::scheduling::activate::Activator;

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::implementations::ord_neu::OrdValBatch;
    use mz_repr::{Datum, Row};
    use std::rc::Rc;

    use super::*;
    use crate::DatumContainer;
    use crate::spines::RowRowLayout;

    type Layout = RowRowLayout<((Row, Row), Timestamp, Diff)>;
    type RowRowBatch = Rc<OrdValBatch<Layout>>;
    type RowRowBuild = crate::RowRowBuilder<Timestamp, Diff>;

    fn row(datum: Datum) -> Row {
        Row::pack_slice(&[datum])
    }

    /// Builds a batch over an explicit interval, standing in for one a live
    /// arrangement produced. `rebuild_batch` cannot do this: it always spans
    /// from `minimum`, which is correct for a restore's single batch but cannot
    /// express a trace that holds history.
    fn batch_at(
        lower: u64,
        upper: u64,
        at: Timestamp,
        updates: &[((Row, Row), Diff)],
    ) -> RowRowBatch {
        let description = Description::new(
            Antichain::from_elem(Timestamp::new(lower)),
            Antichain::from_elem(Timestamp::new(upper)),
            Antichain::from_elem(Timestamp::minimum()),
        );
        let mut chunk =
            <ColumnationStack<((Row, Row), Timestamp, Diff)> as StackOf>::with_updates(at, updates);
        let mut builder = RowRowBuild::with_capacity(0, 0, updates.len());
        builder.push(&mut chunk);
        builder.done(description)
    }

    /// Builds the base batch a restore would hold.
    fn batch(as_of: Timestamp, updates: &[((Row, Row), Diff)]) -> RowRowBatch {
        let captured = Captured {
            as_of,
            updates: updates.to_vec(),
        };
        restored_batch::<RowRowBuild>(&captured)
    }

    fn contents(
        trace: &mut RestorableSpine<RowRowBatch>,
        as_of: Timestamp,
    ) -> Vec<((Row, Row), Diff)> {
        capture::<_, DatumContainer, DatumContainer, _>(trace, as_of).updates
    }

    /// Stands in for rendering plan node `node`, building `count` arrangements.
    fn render_node(node: u64, count: usize) -> Vec<RestorableSpine<RowRowBatch>> {
        with_node(node, || {
            (0..count)
                .map(|_| {
                    <RestorableSpine<RowRowBatch> as Trace>::new(fake_operator_info(), None, None)
                })
                .collect()
        })
    }

    #[mz_ore::test]
    fn capture_restore_round_trip() {
        clear();
        let as_of = Timestamp::new(10);
        let updates = vec![
            ((row(Datum::Int64(1)), row(Datum::String("a"))), Diff::ONE),
            (
                (row(Datum::Int64(2)), row(Datum::String("b"))),
                Diff::from(3),
            ),
        ];

        // A trace holding the updates, as a live arrangement would.
        let mut source =
            <RestorableSpine<RowRowBatch> as Trace>::new(fake_operator_info(), None, None);
        source.insert(batch_at(0, 11, as_of, &updates));

        let captured = capture::<_, DatumContainer, DatumContainer, _>(&mut source, as_of);
        assert_eq!(captured.as_of(), as_of);
        assert_eq!(captured.updates(), &updates[..]);

        // Restore it into the arrangement the same plan node builds.
        let name = ArrangementName::new(7, 0);
        publish(name, restored_batch::<RowRowBuild>(&captured));
        let mut restored = render_node(7, 1);

        assert!(restored[0].was_restored());
        assert_eq!(contents(&mut restored[0], as_of), updates);
        assert!(unclaimed().is_empty());
    }

    #[mz_ore::test]
    fn the_same_plan_names_the_same_arrangements() {
        clear();
        let first: Vec<_> = render_node(3, 2).iter().map(|s| s.name()).collect();
        let second: Vec<_> = render_node(3, 2).iter().map(|s| s.name()).collect();

        assert_eq!(first, second);
        assert_eq!(
            first,
            vec![
                Some(ArrangementName::new(3, 0)),
                Some(ArrangementName::new(3, 1))
            ]
        );

        // A different node numbers from zero again, so one node's arrangement
        // count does not shift another's names.
        let other: Vec<_> = render_node(4, 1).iter().map(|s| s.name()).collect();
        assert_eq!(other, vec![Some(ArrangementName::new(4, 0))]);
    }

    #[mz_ore::test]
    fn an_unrestored_arrangement_still_consumes_its_name() {
        clear();
        // Publish for the *second* arrangement only. The first must still take
        // ordinal 0, or the second would claim it and restore the wrong state.
        let second = ArrangementName::new(1, 1);
        let updates = vec![((row(Datum::Int64(9)), row(Datum::String("z"))), Diff::ONE)];
        publish(second, batch(Timestamp::new(4), &updates));

        let mut spines = render_node(1, 2);

        assert!(!spines[0].was_restored());
        assert_eq!(spines[0].name(), Some(ArrangementName::new(1, 0)));
        assert!(spines[1].was_restored());
        assert_eq!(spines[1].name(), Some(second));
        assert_eq!(contents(&mut spines[1], Timestamp::new(4)), updates);
        assert!(unclaimed().is_empty());
    }

    #[mz_ore::test]
    fn a_spine_outside_any_node_claims_nothing() {
        clear();
        publish(ArrangementName::new(0, 0), batch(Timestamp::new(1), &[]));

        // Dataflows that are not restoring build spines with no node scope, and
        // must not pick up state published for a plan node.
        let spine = <RestorableSpine<RowRowBatch> as Trace>::new(fake_operator_info(), None, None);

        assert!(spine.name().is_none());
        assert!(!spine.was_restored());
        assert_eq!(unclaimed(), vec![ArrangementName::new(0, 0)]);
    }

    #[mz_ore::test]
    fn capture_drops_history_above_as_of() {
        clear();
        let mut source =
            <RestorableSpine<RowRowBatch> as Trace>::new(fake_operator_info(), None, None);
        let key = row(Datum::Int64(1));
        let val = row(Datum::String("a"));

        source.insert(batch_at(
            0,
            6,
            Timestamp::new(5),
            &[((key.clone(), val.clone()), Diff::ONE)],
        ));
        source.insert(batch_at(
            6,
            10,
            Timestamp::new(9),
            &[((key.clone(), val.clone()), Diff::ONE)],
        ));

        // As of 5 the second update has not happened yet.
        assert_eq!(
            capture::<_, DatumContainer, DatumContainer, _>(&mut source, Timestamp::new(5))
                .updates(),
            &[((key.clone(), val.clone()), Diff::ONE)]
        );
        // As of 9 both accumulate.
        assert_eq!(
            capture::<_, DatumContainer, DatumContainer, _>(&mut source, Timestamp::new(9))
                .updates(),
            &[((key, val), Diff::from(2))]
        );
    }

    /// The operator writing into a restored trace starts at `Time::minimum()`
    /// and overshoots the as-of with a batch carrying data. That is the case an
    /// earlier design could not express, since a batch's description cannot be
    /// retagged and no filler can bridge `[minimum, X)` to a trace already at
    /// `as_of + 1`.
    #[mz_ore::test]
    fn a_restored_spine_takes_the_operators_first_batch() {
        clear();
        let as_of = Timestamp::new(10);
        let key = row(Datum::Int64(1));
        let restored = vec![((key.clone(), row(Datum::String("a"))), Diff::ONE)];

        publish(ArrangementName::new(2, 0), batch(as_of, &restored));
        let mut spines = render_node(2, 1);
        assert!(spines[0].was_restored());

        let fresh = vec![((key.clone(), row(Datum::String("b"))), Diff::ONE)];
        spines[0].insert(batch_at(0, 15, Timestamp::new(12), &fresh));

        // Both halves read back, and the restored half is unchanged by the
        // append that straddled it.
        let mut all = restored.clone();
        all.extend(fresh);
        all.sort();
        assert_eq!(contents(&mut spines[0], Timestamp::new(14)), all);
        assert_eq!(contents(&mut spines[0], as_of), restored);
    }

    /// The base batch is state that predates this dataflow, so it must not be
    /// replayed to a consumer priming itself off the trace, and must not move
    /// the writer's upper.
    #[mz_ore::test]
    fn the_base_batch_is_not_replayed() {
        clear();
        let as_of = Timestamp::new(10);
        let updates = vec![((row(Datum::Int64(1)), row(Datum::String("a"))), Diff::ONE)];

        publish(ArrangementName::new(8, 0), batch(as_of, &updates));
        let mut spines = render_node(8, 1);

        let mut replayed = 0;
        spines[0].map_batches(|_| replayed += 1);
        assert_eq!(replayed, 0);

        let mut upper = Antichain::new();
        spines[0].read_upper(&mut upper);
        assert_eq!(upper, Antichain::from_elem(Timestamp::minimum()));

        // Still readable, which is the whole point of holding it beside the spine.
        assert_eq!(contents(&mut spines[0], as_of), updates);
    }

    #[mz_ore::test]
    fn an_unmatched_name_leaves_state_unclaimed() {
        clear();
        let published = ArrangementName::new(99, 0);
        publish(published, batch(Timestamp::new(1), &[]));

        let spines = render_node(5, 1);

        assert!(!spines[0].was_restored());
        assert_eq!(unclaimed(), vec![published]);
    }

    fn fake_operator_info() -> OperatorInfo {
        OperatorInfo::new(0, 0, std::rc::Rc::from(&[][..]))
    }
}
