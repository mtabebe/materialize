# mz-persist

Durable storage abstraction for differential dataflow collections. Provides blob and consensus backends.

## Key Files

- `s3.rs` — S3 blob storage backend
- `postgres.rs` — PostgreSQL consensus backend
- `azure/` — Azure Blob Storage backend
- `file.rs` — Local filesystem backend (dev/test)
- `mem.rs` — In-memory backend (test)
- `location.rs` — Blob URI parsing and routing
- `metrics.rs` — Persistence performance metrics

## Conventions

- Intentionally minimal dependencies on other mz-* crates (abstraction boundary)
- Blob storage (S3/Azure/file) separated from consensus (PostgreSQL/mem)
- Extensive retry logic for cloud storage operations
- `persist-client` is the primary consumer API; this crate provides backends

## Test

```bash
cargo test -p mz-persist
```

## Dependencies

Minimal: `aws-sdk-s3`, `azure_storage_blobs`, `tokio-postgres`. See also `mz-persist-client` (client API) and `mz-persist-types` (shared types).
