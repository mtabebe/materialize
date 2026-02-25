# mz-controller

Unified interface over storage and compute controllers. Manages cluster lifecycle.

## Key Files

- `lib.rs` — `Controller` type: combines `StorageController` + `ComputeController`
- `clusters.rs` — Cluster and replica management

## Conventions

- Wrapper that presents a single API over both storage and compute
- Handles introspection collection management
- Integrates with orchestrator for cluster provisioning

## Test

```bash
cargo test -p mz-controller
```

## Dependencies

Depends on: `mz-compute-client`, `mz-storage-client`, `mz-persist-client`, `mz-orchestrator`.
