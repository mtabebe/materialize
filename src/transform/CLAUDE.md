# mz-transform

Query optimizer: transforms MIR expressions to preserve semantics while improving performance.

## Key Files

- `lib.rs` — `Transform` trait and `optimize_dataflow()` entry point
- `analysis/` — Analysis passes (column knowledge, demand, monotonicity)
- `fold_constants.rs` — Constant folding
- `predicate_pushdown.rs` — Push filters closer to data sources
- `join_implementation.rs` — Join strategy selection (hash, binary tree)
- `literal_lifting.rs` — Hoist constants out of loops
- `reduce_elision.rs` — Remove unnecessary aggregations
- `movement.rs` — Projection and join reordering
- `dataflow.rs` — Multi-dataflow optimization (cross-query)

## Conventions

- Each transform implements the `Transform` trait
- Analysis passes run before transformations to gather metadata
- Soft panic on recursion limits (log error, don't crash)
- Notice system emits optimizer warnings/hints to users
- Transforms are ordered carefully — changing order can affect correctness

## Test

```bash
cargo test -p mz-transform
REWRITE=1 cargo test -p mz-transform    # update datadriven snapshots
```

## Dependencies

Transforms: `mz-expr` expressions. Depends on: `mz-repr`, `mz-ore`, `mz-compute-types`.
