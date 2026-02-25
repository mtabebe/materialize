# mz-environmentd

Main server binary. Starts HTTP/pgwire listeners, initializes the coordinator, and manages deployment lifecycle.

## Key Files

- `lib.rs` — Server configuration and bootstrap
- `http.rs` — Axum HTTP/WebSocket server
- `deployment/` — Zero-downtime deployment (generation-based)
- `telemetry.rs` — Analytics and monitoring setup

## Conventions

- Axum web framework for HTTP endpoints
- Multiple listener types: SQL (pgwire), HTTP, internal
- TLS and authentication configured at startup
- Deploy generations enable graceful restarts
- Extensive CLI flags and environment variable configuration

## Build & Run

```bash
bin/environmentd                    # build and run (dev mode)
bin/environmentd --release          # optimized build
```

## Dependencies

Top-level orchestrator: `mz-adapter`, `mz-pgwire`, `mz-controller`, `mz-authenticator`, `axum`, `tokio`.
