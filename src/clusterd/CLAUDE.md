# mz-clusterd

Standalone cluster process: runs isolated compute and storage instances.

## Key Files

- `lib.rs` — Server bootstrap: starts compute + storage servers, listens for controller connections

## Conventions

- Minimal crate — delegates to `mz-compute` and `mz-storage`
- Configured with timely cluster parameters
- Process-level metrics and tracing

## Build & Run

```bash
# Usually started by the orchestrator, not directly
cargo build -p mz-clusterd
```

## Dependencies

Depends on: `mz-compute`, `mz-storage`, `mz-cluster-client`.
