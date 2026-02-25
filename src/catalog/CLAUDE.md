# mz-catalog

Persistent system catalog: metadata storage for all database objects.

## Key Files

- `durable.rs` — Durable catalog backed by persist
- `memory.rs` — In-memory catalog representation
- `builtin.rs` — Built-in system catalog items (views, functions, types)
- `config.rs` — Cluster replica sizing configuration

## Conventions

- Dual storage: durable (persist-backed) for crash recovery + in-memory for fast access
- Catalog items are versioned for migration
- Expression caching for materialized view definitions
- Bootstrap process initializes built-in objects on first startup

## Test

```bash
cargo test -p mz-catalog
```

## Dependencies

Depends on: `mz-persist-client`, `mz-sql`, `mz-expr`, `mz-repr`.
