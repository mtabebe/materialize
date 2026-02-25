# mz-interchange

Data format encoding/decoding: translates between external formats and Materialize's internal representation.

## Key Files

- `avro.rs` — Avro encoding/decoding
- `protobuf.rs` — Protobuf handling
- `json.rs` — JSON format
- `text_binary.rs` — Text and binary formats
- `confluent.rs` — Confluent Schema Registry integration
- `envelopes.rs` — CDC envelopes (Debezium, upsert, append-only)

## Conventions

- Each format has encoder/decoder implementations
- Confluent Schema Registry integration for schema evolution
- Envelope patterns determine how rows map to insert/update/delete operations
- Benchmarking harness included for performance testing

## Test

```bash
cargo test -p mz-interchange
```

## Dependencies

Depends on: `mz-avro`, `mz-ccsr` (schema registry client), `mz-repr`, `prost`.
