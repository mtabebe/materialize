# mz-repr

Fundamental data representation types — the "lingua franca" used across all layers.

## Key Files

- `scalar.rs` — `Datum` enum: all literal value types (Int32, String, Timestamp, etc.)
- `row.rs` — `Row` type: columnar-encoded tuple; `RowArena` for temporary allocations
- `relation.rs` — `RelationDesc`: column names, types, nullability
- `adt/` — Abstract data types: ranges, lists, maps, arrays, intervals, numerics
- `strconv.rs` — String ↔ Datum conversion
- `timestamp.rs` — Timestamp types and manipulation

## Conventions

- `Row` uses compact columnar encoding — not a simple Vec of values
- `RowRef` (borrowed) vs `Row` (owned) distinction for zero-copy access
- `Datum<'a>` is lifetime-bound to a `RowArena` or `Row` — cannot outlive its source
- Protobuf for serialization of all types
- `RelationDesc` is immutable once constructed

## Test

```bash
cargo test -p mz-repr
```

## Dependencies

Used by virtually every crate. Depends on: `mz-ore`, `mz-persist-types`, `ordered-float`, `prost`.
