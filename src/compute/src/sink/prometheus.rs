// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering for the Prometheus sink.
//!
//! A Prometheus sink is a terminal compute sink whose output is a set of live
//! series in clusterd's local `MetricsRegistry`. The operator consumes the
//! sink's OK and ERR streams and writes one gauge (or accumulated counter) per
//! value column per label set, drops series when their rows retract, and emits
//! the two auxiliary series every sink carries: liveness (last-update
//! timestamp) and data-plane errors (error count).
//!
//! ## Diff-to-metric translation
//!
//! For every value column the operator maintains, per label set, the additive
//! fold `sum(value * diff)` and the row-presence count `sum(diff)`. It then
//! `set`s the metric to that fold. For a consolidated input this is exactly the
//! current value of a gauge (a row's value change arrives as retract-old plus
//! add-new, which folds to the new value) and the accumulated total of a
//! counter (which is why `rate()` stays sane across compaction and object
//! churn). When a label set's presence count returns to zero the series is
//! dropped, so retractions clean up leak-free via the delete-on-drop handles.
//! Gauges and counters therefore share one code path; they differ only in the
//! Prometheus family semantics the SQL author declares.
//!
//! ## Concurrency across workers
//!
//! `render_sink` runs once per timely worker, and all workers in a process
//! share one `MetricsRegistry`. Registering the same family name twice panics,
//! so each worker registers its families with distinct `worker_id` (and
//! `process_id`) const labels. Each worker then emits its own key partition;
//! because the sink's input is reduced per label set, a given series is owned by
//! exactly one worker, so the per-worker series do not collide. Consumers roll
//! up over `process_id` (and `worker_id`) on the Prometheus side to get a
//! replica-level total.
//!
//! NOTE: a sink is installed once per replica lifetime and torn down when the
//! replica (process) stops, so a family is never re-registered inside a live
//! process. Dropping the sink removes its series via the delete-on-drop
//! handles. The empty family descriptor is harmless and vanishes with the
//! process.

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use differential_dataflow::VecCollection;
use mz_compute_types::sinks::{ComputeSinkDesc, PromKind, PrometheusSinkConnection};
use mz_ore::metrics::{
    DeleteOnDropGauge, GaugeVec, MakeCollectorOpts, MetricsRegistry, PrometheusOpts,
};
use mz_ore::now::NowFn;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::probe::Handle;
use prometheus::core::AtomicF64;
use prometheus::{Gauge, IntCounter};
use timely::PartialOrder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::Antichain;
use timely::progress::Timestamp as _;
use tracing::warn;

use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;

/// The name of the liveness auxiliary series. Consumers alert on
/// `time() - <this>` crossing a staleness threshold.
const LAST_UPDATE_METRIC: &str = "mz_prom_sink_last_update_timestamp_seconds";
/// The name of the data-plane-error auxiliary series.
const ERRORS_METRIC: &str = "mz_prom_sink_errors_total";

impl<'scope> SinkRender<'scope> for PrometheusSinkConnection {
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        _sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: VecCollection<'scope, Timestamp, Row, Diff>,
        err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
        _output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        let worker_index = sinked_collection.scope().index();
        let workers_per_process = compute_state.workers_per_process;
        Some(prometheus_sink(
            sinked_collection,
            err_collection,
            self,
            &compute_state.metrics_registry,
            worker_index,
            workers_per_process,
            mz_ore::now::SYSTEM_TIME.clone(),
            sink_id,
        ))
    }
}

/// The mutable state of a rendered Prometheus sink. Held behind an
/// `Rc<RefCell<Option<..>>>` shared between the operator (which mutates it) and
/// the returned lifecycle token (which drops it on teardown). Dropping it drops
/// every [`DeleteOnDropGauge`] handle, removing the sink's series.
struct SinkState {
    now: NowFn,
    /// Registered gauge vectors, one per declared value column, in `values`
    /// order.
    value_vecs: Vec<GaugeVec>,
    /// The kind of each value column, in `values` order. Retained for
    /// documentation of the emitted family; the fold math is identical for both.
    #[allow(dead_code)]
    value_kinds: Vec<PromKind>,
    /// Per-label-set series state.
    series: BTreeMap<Vec<String>, SeriesState>,
    /// Liveness gauge (Unix timestamp in seconds).
    last_update: Gauge,
    /// Data-plane error counter.
    errors_total: IntCounter,
    /// The highest input frontier observed so far. The liveness timestamp
    /// advances only when this strictly advances.
    last_frontier: Antichain<Timestamp>,
}

/// Per-label-set state: the delete-on-drop handles plus the running folds.
struct SeriesState {
    /// `sum(diff)` over the input for this label set. The series is removed when
    /// this returns to zero.
    presence: i64,
    /// Per value column: the running `sum(value * diff)` and the handle whose
    /// drop removes the series.
    values: Vec<ValueSeries>,
}

struct ValueSeries {
    accum: f64,
    gauge: DeleteOnDropGauge<AtomicF64, Vec<String>>,
}

/// Builds the terminal Prometheus sink operator. Factored out of
/// [`SinkRender::render_sink`] so tests can drive it against a hand-built
/// dataflow and a fresh [`MetricsRegistry`] without a full [`ComputeState`].
fn prometheus_sink<'scope>(
    sinked_collection: VecCollection<'scope, Timestamp, Row, Diff>,
    err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
    connection: &PrometheusSinkConnection,
    registry: &MetricsRegistry,
    worker_index: usize,
    workers_per_process: usize,
    now: NowFn,
    sink_id: GlobalId,
) -> Rc<dyn Any> {
    let process_id = worker_index / workers_per_process.max(1);

    // Const labels that distinguish this worker's registrations from those of
    // other workers sharing the process registry.
    let base_const_labels = |extra: &[(&str, String)]| -> Vec<(String, String)> {
        let mut labels = vec![
            ("process_id".to_string(), process_id.to_string()),
            ("worker_id".to_string(), worker_index.to_string()),
        ];
        labels.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
        labels
    };

    let var_label_names: Vec<String> = connection.labels.iter().map(|l| l.name.clone()).collect();

    let value_vecs: Vec<GaugeVec> = connection
        .values
        .iter()
        .map(|value| {
            register_metric(
                registry,
                &value.metric,
                &value.help,
                base_const_labels(&[]),
                var_label_names.clone(),
            )
        })
        .collect();
    let value_kinds: Vec<PromKind> = connection.values.iter().map(|v| v.kind).collect();

    let last_update: Gauge = register_metric(
        registry,
        LAST_UPDATE_METRIC,
        "Unix timestamp of the last time this sink's frontier advanced.",
        base_const_labels(&[("sink", connection.sink_name.clone())]),
        Vec::new(),
    );
    let errors_total: IntCounter = register_metric(
        registry,
        ERRORS_METRIC,
        "Count of errors produced by this sink's input.",
        base_const_labels(&[("sink", connection.sink_name.clone())]),
        Vec::new(),
    );

    let state = Rc::new(RefCell::new(Some(SinkState {
        now,
        value_vecs,
        value_kinds,
        series: BTreeMap::new(),
        last_update,
        errors_total,
        last_frontier: Antichain::from_elem(Timestamp::minimum()),
    })));
    let state_weak = Rc::downgrade(&state);

    // Resolved column indices, captured by the operator closure.
    let label_indices: Vec<usize> = connection.labels.iter().map(|l| l.column_index).collect();
    let value_indices: Vec<usize> = connection.values.iter().map(|v| v.column_index).collect();

    let name = format!("prometheus-sink-{sink_id}");
    let mut op = OperatorBuilder::new(name, sinked_collection.scope());
    let mut ok_input = op.new_input(sinked_collection.inner, Pipeline);
    let mut err_input = op.new_input(err_collection.inner, Pipeline);

    op.build(move |_caps| {
        move |frontiers| {
            let Some(state) = state_weak.upgrade() else {
                // The sink has been torn down; drain inputs so the operator is
                // not rescheduled forever.
                ok_input.for_each(|_, _| {});
                err_input.for_each(|_, _| {});
                return;
            };
            let mut state_ref = state.borrow_mut();
            let Some(state) = state_ref.as_mut() else {
                ok_input.for_each(|_, _| {});
                err_input.for_each(|_, _| {});
                return;
            };

            ok_input.for_each(|_cap, data| {
                for (row, _time, diff) in data.drain(..) {
                    state.process_ok_row(&label_indices, &value_indices, &row, diff);
                }
            });
            err_input.for_each(|_cap, data| {
                for (_err, _time, diff) in data.drain(..) {
                    // A counter counts occurrences and cannot go down, so
                    // retractions of errors (negative diffs) are not subtracted.
                    let d = diff.into_inner();
                    if d > 0 {
                        state.errors_total.inc_by(u64::try_from(d).unwrap_or(0));
                    }
                }
            });

            // Advance the liveness timestamp when the combined input frontier
            // strictly advances. A stalled worker freezes the frontier, so the
            // timestamp stops moving and `time() - metric` grows.
            let mut frontier = Antichain::new();
            for input_frontier in frontiers {
                frontier.extend(input_frontier.frontier().iter().copied());
            }
            if PartialOrder::less_than(&state.last_frontier, &frontier) {
                state.last_frontier = frontier;
                let secs = (state.now)() as f64 / 1000.0;
                state.last_update.set(secs);
            }
        }
    });

    // The lifecycle token. Dropping it takes and drops the `SinkState`, whose
    // delete-on-drop handles remove every series this sink registered. The
    // dataflow's sink token holds it, so a replica drop removes the series.
    Rc::new(scopeguard::guard((), move |_| {
        drop(state.borrow_mut().take());
    }))
}

impl SinkState {
    fn process_ok_row(
        &mut self,
        label_indices: &[usize],
        value_indices: &[usize],
        row: &Row,
        diff: Diff,
    ) {
        let datums: Vec<Datum> = row.iter().collect();

        // Decode label values. Labels must be non-null text (the SQL author's
        // responsibility per the sink contract); a violation drops the row
        // rather than inventing an empty label.
        let mut label_values = Vec::with_capacity(label_indices.len());
        for &idx in label_indices {
            match datums.get(idx) {
                Some(Datum::String(s)) => label_values.push(s.to_string()),
                other => {
                    warn!(
                        "prometheus sink: expected non-null text label at column {idx}, got {other:?}; dropping row"
                    );
                    return;
                }
            }
        }

        let diff_i = diff.into_inner();
        let diff_f = diff_i as f64;

        let value_vecs = &self.value_vecs;
        let entry = self
            .series
            .entry(label_values.clone())
            .or_insert_with(|| SeriesState {
                presence: 0,
                values: value_vecs
                    .iter()
                    .map(|vec| ValueSeries {
                        accum: 0.0,
                        gauge: vec.get_delete_on_drop_metric(label_values.clone()),
                    })
                    .collect(),
            });

        entry.presence += diff_i;

        for (vi, &col) in value_indices.iter().enumerate() {
            match datum_to_f64(datums.get(col).copied()) {
                Some(v) => {
                    entry.values[vi].accum += v * diff_f;
                    entry.values[vi].gauge.set(entry.values[vi].accum);
                }
                None => {
                    warn!(
                        "prometheus sink: null or non-numeric value at column {col}; dropping value"
                    );
                }
            }
        }

        // A label set whose rows have all retracted drops its handles, removing
        // the series.
        if entry.presence == 0 {
            self.series.remove(&label_values);
        }
    }
}

/// Registers a metric with the given const and variable labels, building the
/// options by hand because [`mz_ore::metric!`] only accepts compile-time
/// variable label lists and ours are resolved at plan time.
fn register_metric<M: mz_ore::metrics::MakeCollector>(
    registry: &MetricsRegistry,
    name: &str,
    help: &str,
    const_labels: Vec<(String, String)>,
    var_labels: Vec<String>,
) -> M {
    let mut opts = PrometheusOpts::new(name, help);
    for (key, value) in const_labels {
        opts = opts.const_label(key, value);
    }
    opts = opts.variable_labels(var_labels);
    registry.register(MakeCollectorOpts {
        opts,
        buckets: None,
    })
}

/// Decodes a numeric datum to `f64`, returning `None` for null or non-numeric
/// datums (which are dropped, not emitted as zero, per the sink contract).
fn datum_to_f64(datum: Option<Datum>) -> Option<f64> {
    match datum? {
        Datum::Int16(i) => Some(f64::from(i)),
        Datum::Int32(i) => Some(f64::from(i)),
        Datum::Int64(i) => Some(i as f64),
        Datum::UInt16(i) => Some(f64::from(i)),
        Datum::UInt32(i) => Some(f64::from(i)),
        Datum::UInt64(i) => Some(i as f64),
        Datum::Float32(f) => Some(f64::from(*f)),
        Datum::Float64(f) => Some(*f),
        Datum::Numeric(n) => f64::try_from(n.into_inner()).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use differential_dataflow::input::InputSession;
    use mz_compute_types::sinks::{PromLabelSpec, PromValueSpec};
    use mz_expr::EvalError;
    use prometheus::proto::MetricFamily;

    use super::*;

    /// The six-label, three-gauge shape of `mz_prom_arrangement_sizes`.
    fn arrangement_sizes_connection() -> PrometheusSinkConnection {
        let labels = [
            "object_id",
            "object_name",
            "cluster_id",
            "cluster_name",
            "replica_id",
            "replica_name",
        ];
        PrometheusSinkConnection {
            sink_name: "mz_prom_arrangement_sizes".to_string(),
            labels: labels
                .iter()
                .enumerate()
                .map(|(i, name)| PromLabelSpec {
                    name: name.to_string(),
                    column_index: i,
                })
                .collect(),
            values: vec![
                PromValueSpec {
                    column_name: "size_bytes".to_string(),
                    column_index: 6,
                    metric: "mz_arrangement_size_bytes".to_string(),
                    kind: PromKind::Gauge,
                    help: "size".to_string(),
                },
                PromValueSpec {
                    column_name: "records".to_string(),
                    column_index: 7,
                    metric: "mz_arrangement_records".to_string(),
                    kind: PromKind::Gauge,
                    help: "records".to_string(),
                },
                PromValueSpec {
                    column_name: "batches".to_string(),
                    column_index: 8,
                    metric: "mz_arrangement_batches".to_string(),
                    kind: PromKind::Gauge,
                    help: "batches".to_string(),
                },
            ],
        }
    }

    /// A single-counter connection for the accumulation test.
    fn elapsed_connection() -> PrometheusSinkConnection {
        PrometheusSinkConnection {
            sink_name: "mz_prom_dataflow_elapsed".to_string(),
            labels: vec![PromLabelSpec {
                name: "object_id".to_string(),
                column_index: 0,
            }],
            values: vec![PromValueSpec {
                column_name: "elapsed_seconds".to_string(),
                column_index: 1,
                metric: "mz_dataflow_elapsed_seconds_total".to_string(),
                kind: PromKind::Counter,
                help: "elapsed".to_string(),
            }],
        }
    }

    fn sizes_row(labels: [&str; 6], size: f64, records: f64, batches: f64) -> Row {
        Row::pack_slice(&[
            Datum::String(labels[0]),
            Datum::String(labels[1]),
            Datum::String(labels[2]),
            Datum::String(labels[3]),
            Datum::String(labels[4]),
            Datum::String(labels[5]),
            Datum::Float64(size.into()),
            Datum::Float64(records.into()),
            Datum::Float64(batches.into()),
        ])
    }

    fn find_series<'a>(
        families: &'a [MetricFamily],
        name: &str,
        label: &str,
        value: &str,
    ) -> Option<&'a prometheus::proto::Metric> {
        families
            .iter()
            .find(|f| f.name() == name)?
            .get_metric()
            .iter()
            .find(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.name() == label && l.value() == value)
            })
    }

    /// Feeds a row into the gauge sink and asserts all three families appear
    /// with the six labels populated, then retracts it and asserts the series
    /// disappear.
    #[mz_ore::test]
    fn test_gauges_and_retraction() {
        let registry = MetricsRegistry::new();
        let reg = registry.clone();
        timely::execute_directly(move |worker| {
            let mut ok_input: InputSession<Timestamp, Row, Diff> = InputSession::new();
            let mut err_input: InputSession<Timestamp, DataflowErrorSer, Diff> =
                InputSession::new();
            let connection = arrangement_sizes_connection();
            let now = NowFn::from(|| 1_000_000u64);

            let _token = worker.dataflow(|scope| {
                let ok = ok_input.to_collection(scope);
                let err = err_input.to_collection(scope);
                prometheus_sink(
                    ok,
                    err,
                    &connection,
                    &reg,
                    0,
                    1,
                    now,
                    GlobalId::Transient(1),
                )
            });

            let labels = ["u1", "obj", "u2", "clus", "u3", "rep"];
            ok_input.advance_to(Timestamp::from(1u64));
            err_input.advance_to(Timestamp::from(1u64));
            ok_input.update(sizes_row(labels, 100.0, 5.0, 2.0), Diff::ONE);
            ok_input.advance_to(Timestamp::from(2u64));
            err_input.advance_to(Timestamp::from(2u64));
            ok_input.flush();
            err_input.flush();
            for _ in 0..20 {
                worker.step();
            }

            let families = reg.gather();
            let size = find_series(&families, "mz_arrangement_size_bytes", "object_name", "obj")
                .expect("size series present");
            assert_eq!(size.get_gauge().value(), 100.0);
            // All six labels populated (plus process_id/worker_id).
            for (name, value) in [
                ("object_id", "u1"),
                ("cluster_name", "clus"),
                ("replica_name", "rep"),
            ] {
                assert!(
                    size.get_label()
                        .iter()
                        .any(|l| l.name() == name && l.value() == value),
                    "label {name}={value} present"
                );
            }
            assert_eq!(
                find_series(&families, "mz_arrangement_records", "object_name", "obj")
                    .expect("records series")
                    .get_gauge()
                    .value(),
                5.0
            );
            assert_eq!(
                find_series(&families, "mz_arrangement_batches", "object_name", "obj")
                    .expect("batches series")
                    .get_gauge()
                    .value(),
                2.0
            );

            // Retract the row.
            ok_input.update(sizes_row(labels, 100.0, 5.0, 2.0), -Diff::ONE);
            ok_input.advance_to(Timestamp::from(3u64));
            err_input.advance_to(Timestamp::from(3u64));
            ok_input.flush();
            err_input.flush();
            for _ in 0..20 {
                worker.step();
            }

            let families = reg.gather();
            assert!(
                find_series(&families, "mz_arrangement_size_bytes", "object_name", "obj").is_none(),
                "size series removed after retraction"
            );
        });
    }

    /// An ERR row bumps `mz_prom_sink_errors_total`.
    #[mz_ore::test]
    fn test_error_counter() {
        let registry = MetricsRegistry::new();
        let reg = registry.clone();
        timely::execute_directly(move |worker| {
            let mut ok_input: InputSession<Timestamp, Row, Diff> = InputSession::new();
            let mut err_input: InputSession<Timestamp, DataflowErrorSer, Diff> =
                InputSession::new();
            let connection = arrangement_sizes_connection();
            let now = NowFn::from(|| 1_000_000u64);

            let _token = worker.dataflow(|scope| {
                let ok = ok_input.to_collection(scope);
                let err = err_input.to_collection(scope);
                prometheus_sink(
                    ok,
                    err,
                    &connection,
                    &reg,
                    0,
                    1,
                    now,
                    GlobalId::Transient(2),
                )
            });

            err_input.advance_to(Timestamp::from(1u64));
            ok_input.advance_to(Timestamp::from(1u64));
            err_input.update(DataflowErrorSer::from(EvalError::DivisionByZero), Diff::ONE);
            err_input.advance_to(Timestamp::from(2u64));
            ok_input.advance_to(Timestamp::from(2u64));
            err_input.flush();
            ok_input.flush();
            for _ in 0..20 {
                worker.step();
            }

            let families = reg.gather();
            let errors = families
                .iter()
                .find(|f| f.name() == ERRORS_METRIC)
                .expect("errors family");
            assert_eq!(errors.get_metric()[0].get_counter().value(), 1.0);
        });
    }

    /// The liveness timestamp advances when the frontier advances and holds
    /// still when the frontier does not. The clock is injected so the test is
    /// deterministic.
    #[mz_ore::test]
    fn test_freshness_timestamp() {
        let registry = MetricsRegistry::new();
        let reg = registry.clone();
        let clock = Arc::new(AtomicU64::new(1_000));
        let clock2 = Arc::clone(&clock);
        timely::execute_directly(move |worker| {
            let mut ok_input: InputSession<Timestamp, Row, Diff> = InputSession::new();
            let mut err_input: InputSession<Timestamp, DataflowErrorSer, Diff> =
                InputSession::new();
            let connection = arrangement_sizes_connection();
            let now = NowFn::from(move || clock2.load(Ordering::SeqCst));

            let _token = worker.dataflow(|scope| {
                let ok = ok_input.to_collection(scope);
                let err = err_input.to_collection(scope);
                prometheus_sink(
                    ok,
                    err,
                    &connection,
                    &reg,
                    0,
                    1,
                    now,
                    GlobalId::Transient(3),
                )
            });

            // Advance the frontier: timestamp should move to clock (1000ms => 1s).
            ok_input.advance_to(Timestamp::from(1u64));
            err_input.advance_to(Timestamp::from(1u64));
            ok_input.flush();
            err_input.flush();
            for _ in 0..20 {
                worker.step();
            }
            let read_ts = |families: &[MetricFamily]| -> f64 {
                families
                    .iter()
                    .find(|f| f.name() == LAST_UPDATE_METRIC)
                    .expect("liveness family")
                    .get_metric()[0]
                    .get_gauge()
                    .value()
            };
            assert_eq!(read_ts(&reg.gather()), 1.0);

            // Change the clock but do NOT advance the frontier: timestamp holds.
            clock.store(5_000, Ordering::SeqCst);
            for _ in 0..20 {
                worker.step();
            }
            assert_eq!(
                read_ts(&reg.gather()),
                1.0,
                "timestamp holds while frontier is still"
            );

            // Advance the frontier again: timestamp moves to the new clock.
            ok_input.advance_to(Timestamp::from(2u64));
            err_input.advance_to(Timestamp::from(2u64));
            ok_input.flush();
            err_input.flush();
            for _ in 0..20 {
                worker.step();
            }
            assert_eq!(
                read_ts(&reg.gather()),
                5.0,
                "timestamp moves when frontier advances"
            );
        });
    }

    /// A counter reflects the accumulated fold of `value * diff`, not the raw
    /// last value: feeding 10 then partially retracting 4 leaves 6.
    #[mz_ore::test]
    fn test_counter_accumulation() {
        let registry = MetricsRegistry::new();
        let reg = registry.clone();
        timely::execute_directly(move |worker| {
            let mut ok_input: InputSession<Timestamp, Row, Diff> = InputSession::new();
            let mut err_input: InputSession<Timestamp, DataflowErrorSer, Diff> =
                InputSession::new();
            let connection = elapsed_connection();
            let now = NowFn::from(|| 1_000_000u64);

            let _token = worker.dataflow(|scope| {
                let ok = ok_input.to_collection(scope);
                let err = err_input.to_collection(scope);
                prometheus_sink(
                    ok,
                    err,
                    &connection,
                    &reg,
                    0,
                    1,
                    now,
                    GlobalId::Transient(4),
                )
            });

            let row = |v: f64| Row::pack_slice(&[Datum::String("u1"), Datum::Float64(v.into())]);
            let read = |families: &[MetricFamily]| -> f64 {
                find_series(
                    families,
                    "mz_dataflow_elapsed_seconds_total",
                    "object_id",
                    "u1",
                )
                .expect("elapsed series")
                .get_gauge()
                .value()
            };

            // Two coexisting rows for the same label set accumulate to their
            // sum (14), proving the value is the fold of `value * diff`, not the
            // raw last row's value (which would be 10 or 4).
            ok_input.advance_to(Timestamp::from(1u64));
            err_input.advance_to(Timestamp::from(1u64));
            ok_input.update(row(10.0), Diff::ONE);
            ok_input.update(row(4.0), Diff::ONE);
            ok_input.advance_to(Timestamp::from(2u64));
            err_input.advance_to(Timestamp::from(2u64));
            ok_input.flush();
            err_input.flush();
            for _ in 0..20 {
                worker.step();
            }
            assert_eq!(read(&reg.gather()), 14.0, "accumulated fold 10 + 4 = 14");

            // Partially retracting one row leaves the accumulated fold at 4, and
            // the series stays because a row is still present (a counter does not
            // reset on churn).
            ok_input.update(row(10.0), -Diff::ONE);
            ok_input.advance_to(Timestamp::from(3u64));
            err_input.advance_to(Timestamp::from(3u64));
            ok_input.flush();
            err_input.flush();
            for _ in 0..20 {
                worker.step();
            }
            assert_eq!(
                read(&reg.gather()),
                4.0,
                "fold after partial retraction 14 - 10 = 4"
            );
        });
    }
}
