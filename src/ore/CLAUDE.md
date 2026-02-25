# mz-ore

Internal utility library — stdlib extensions and common patterns. Apache 2.0 licensed.

## Key Files

- `task.rs` — `mz_ore::task::spawn` (use instead of `tokio::spawn`)
- `collections.rs` — `HashMap`/`HashSet` wrappers (when hash map is truly needed)
- `metrics.rs` — Prometheus metric helpers
- `error.rs`, `result.rs` — Error handling utilities
- `future.rs` — Async combinators and helpers
- `stack.rs` — Stack depth limiting (for recursive expression traversal)
- `id_gen.rs` — Unique ID generation
- `tracing.rs` — Observability setup

## Conventions

- Intentionally minimal dependencies — mostly feature-gated
- Heavy use of Cargo feature flags to control what's compiled
- Many crates across the workspace depend on this — keep it lean
- Provides wrappers that clippy.toml enforces usage of (task::spawn, collections, etc.)

## Test

```bash
cargo test -p mz-ore
```
