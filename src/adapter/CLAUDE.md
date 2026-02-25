# mz-adapter

The coordinator: routes client requests through planning, optimization, and execution across storage and compute layers.

## Key Files

- `coord.rs` — Main coordinator state machine and event loop
- `client.rs` — Client connection handling
- `catalog.rs` — Coordinator-level catalog management
- `command.rs` — `ExecuteResponse` and execution dispatch
- `optimize.rs` — Query optimization orchestration (calls mz-transform)
- `session.rs` — Session state and variables
- `peek_client.rs` — Point-in-time query execution (SELECT)
- `subscribe.rs` — SUBSCRIBE streaming results

## Conventions

- Central state machine processing messages from clients and controllers
- Async message passing via mpsc channels
- Timestamp selection and advancement policies in coordinator
- Audit logging for DDL/DML operations
- Read policies control how long arrangements are held

## Test

```bash
cargo test -p mz-adapter
```

## Dependencies

Depends on: `mz-sql` (planning), `mz-controller` (storage+compute), `mz-catalog`, `mz-transform`, `mz-persist-client`.
