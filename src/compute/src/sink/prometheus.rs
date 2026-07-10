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
//! sink's OK and ERR streams and writes one gauge or counter per value column
//! per label set, plus two auxiliary series per sink (liveness and data-plane
//! errors).
//!
//! This file holds the stub that wires `PrometheusSinkConnection` into the
//! render dispatch (`get_sink_render_for` in `crate::render::sinks`). The
//! operator body is not implemented yet.

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::VecCollection;
use mz_compute_types::sinks::{ComputeSinkDesc, PrometheusSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::probe::Handle;
use timely::progress::Antichain;

use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;

impl<'scope> SinkRender<'scope> for PrometheusSinkConnection {
    fn render_sink(
        &self,
        _compute_state: &mut crate::compute_state::ComputeState,
        _sink: &ComputeSinkDesc<CollectionMetadata>,
        _sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        _sinked_collection: VecCollection<'scope, Timestamp, Row, Diff>,
        _err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
        _output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        // Nothing constructs a `ComputeSinkConnection::Prometheus` yet, so this
        // is never reached in a passing build.
        todo!("PrometheusSink render operator")
    }
}
