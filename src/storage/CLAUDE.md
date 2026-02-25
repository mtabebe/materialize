# mz-storage

Storage layer: source ingestion, sink export, and upsert state management.

## Key Files

- `server.rs` — Storage instance server
- `source/` — Source connectors: Kafka, PostgreSQL, MySQL, S3, load generator, webhook
- `sink/` — Sink connectors: Kafka, S3, etc.
- `render.rs` — Converts storage plans into timely operators
- `decode.rs` — Format decoding (Avro, Protobuf, CSV, JSON, text)
- `upsert/` — In-memory and RocksDB-backed upsert state
- `storage_state.rs` — Per-instance state tracking

## Conventions

- Each source type has its own submodule under `source/`
- RocksDB used for persistent upsert state when memory is insufficient
- Async Tokio for external connections, timely for dataflow
- Detailed error reporting with source-specific context

## Test

```bash
cargo test -p mz-storage
```

## Dependencies

Depends on: `mz-interchange` (format handling), `mz-storage-operators`, `rdkafka`, `tokio-postgres`, `mysql_async`, `rocksdb`.
