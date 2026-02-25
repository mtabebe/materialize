# mz-sql

SQL-to-plan translation via two phases: purification (async, resolves external state) and planning (pure, deterministic).

## Key Files

- `plan.rs` — `Plan` enum: all executable statement types (Select, Insert, CreateSource, etc.)
- `plan/` — Plan builders for each statement type
- `pure.rs` — Purification: fetches schemas from registries, validates external connections
- `catalog.rs` — Catalog interface for name resolution during planning
- `session.rs` — Session state and variable handling
- `names.rs` — Name resolution logic
- `rbac.rs` — Role-based access control checks

## Conventions

- Purification is async (talks to Kafka, schema registry, etc.); planning is sync and pure
- `bail_unsupported!` macro for features not yet implemented
- `bail_never_supported!` macro for features that will never be supported
- Plans reference catalog items by `GlobalId`, not by name
- Heavy use of `QueryContext` for scoped resolution

## Test

```bash
cargo test -p mz-sql
REWRITE=1 cargo test -p mz-sql    # update datadriven snapshots
```

## Dependencies

Produces: `mz-expr` plans. Depends on: `mz-sql-parser`, `mz-repr`, `mz-transform`, `mz-storage-types`, `mz-compute-types`.
