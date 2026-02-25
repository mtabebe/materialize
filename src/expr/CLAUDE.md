# mz-expr

Core expression language for Materialize's query representation. Defines MIR (Mid-level IR) for both relation operations and scalar computations.

## Key Files

- `relation.rs` — `MirRelationExpr`: joins, aggregates, filters, projections, unions
- `scalar.rs` — `MirScalarExpr`: unary/binary/variadic functions, literals, column refs
- `linear.rs` — `MapFilterProject` (MFP): fused operator for row-level transforms
- `interpret.rs` — Expression interpreter for runtime evaluation
- `visit.rs` — Tree visitor pattern for expression traversal
- `scalar/` — Function definitions: `UnaryFunc`, `BinaryFunc`, `VariadicFunc`, `AggregateFunc`
- `row/` — Row iteration and encoding logic

## Conventions

- Protobuf serialization for all expression types (see `*.proto` files)
- Visitor pattern via `Visit` trait for traversal and mutation
- MFP is the key optimization unit — many transforms produce or consume MFP chains
- `relation_and_scalar.proto` defines the wire format

## Test

```bash
cargo test -p mz-expr
REWRITE=1 cargo test -p mz-expr    # update datadriven snapshots
```

## Dependencies

Core consumers: `mz-transform` (optimizes), `mz-sql` (produces), `mz-compute` (executes).
Depends on: `mz-repr` (data types), `mz-ore` (utilities).
