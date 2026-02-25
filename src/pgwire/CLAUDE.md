# mz-pgwire

PostgreSQL wire protocol (v3) server implementation.

## Key Files

- `server.rs` — Server initialization and connection accept loop
- `protocol.rs` — Protocol state machine: startup, auth, query, terminate
- `codec.rs` — Message framing and encoding/decoding
- `message.rs` — PostgreSQL message type definitions

## Conventions

- Async message handling with Tokio
- TLS support via tokio-openssl
- Per-connection metrics
- Delegates query execution to `mz-adapter` via `Client` interface

## Test

```bash
cargo test -p mz-pgwire
```

## Dependencies

Depends on: `mz-adapter` (execution), `mz-pgrepr` (PG type representations), `mz-pgwire-common`, `postgres-protocol`.
