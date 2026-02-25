# mz-compute

Compute layer: executes query plans using timely and differential dataflow.

## Key Files

- `server.rs` — Main compute server event loop
- `render.rs` — Converts MIR expressions into timely dataflow operators
- `compute_state.rs` — Per-instance state (dataflows, arrangements, frontiers)
- `arrangement.rs` — Indexed arrangement management for joins
- `sink.rs` — Result export (SUBSCRIBE, COPY TO, etc.)
- `row_spine.rs` — Row storage for differential dataflow arrangements

## Conventions

- Async Tokio for message handling, timely for dataflow execution
- Memory limiting via `MemoryLimiter` to prevent OOM
- Arrangements are cached and shared across dataflows
- Prometheus metrics for operator-level monitoring
- `lgalloc` for large allocation tracking

## Test

```bash
cargo test -p mz-compute
```

## Dependencies

Executes: `mz-expr` plans. Depends on: `timely`, `differential-dataflow`, `mz-persist-client`, `mz-cluster-client`, `mz-storage-operators`.
