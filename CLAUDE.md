# Materialize

Distributed SQL streaming database built on timely/differential dataflow. Rust workspace with 100+ crates in `src/`.

## Build & Test

```bash
# Build and run main server
bin/environmentd

# Run Rust unit/integration tests
bin/cargo-test                          # all tests
bin/cargo-test -- <pattern>             # filter by name
REWRITE=1 cargo test                    # rewrite datadriven expectations

# SQLLogicTest
bin/sqllogictest -- test/sqllogictest/FILE.slt
bin/sqllogictest -- --rewrite-results test/sqllogictest/FILE.slt

# Testdrive (containerized)
bin/mzcompose --find testdrive run default FILE.td

# Format and lint
bin/fmt                                 # format Rust + Python + Protobuf
ci/test/lint-fast.sh                    # fast linters (no build)
ci/test/lint-slow.sh                    # slow linters (requires build)
ci/test/lint-clippy.sh                  # cargo clippy --all-targets
```

## Architecture

```
environmentd          — main server: HTTP, pgwire listeners, coordinator
├── adapter           — coordinator: routes client requests to dataflow
│   ├── sql           — SQL purification + planning (AST → Plan)
│   │   ├── sql-parser — recursive descent SQL parser
│   │   └── expr      — MIR expression types (relation + scalar)
│   ├── transform     — query optimizer passes
│   ├── catalog       — persistent system catalog (persist-backed)
│   └── controller    — unified storage + compute controller
├── pgwire            — PostgreSQL wire protocol
└── clusterd          — isolated compute/storage process
    ├── compute       — timely/differential dataflow execution
    └── storage       — source ingestion + sink export

Cross-cutting:
  repr        — core data types (Datum, Row, RelationDesc)
  ore         — internal utility library (stdlib extensions)
  persist     — durable storage abstraction (S3, Azure, Postgres backends)
  interchange — format encoding/decoding (Avro, Protobuf, JSON)
```

## Code Conventions

- **Edition 2024**, MSRV 1.89.0, rustfmt with 4-space indent
- Use `BTreeMap`/`BTreeSet`, not `HashMap`/`HashSet` (clippy-enforced)
- Use `mz_ore::task::spawn`, not `tokio::spawn` directly
- Use `tracing` crate, not `log`; lowercase sentence fragments for messages
- SQL keywords UPPERCASE, type/function names lowercase
- Error messages: lowercase first letter, no period. Detail: full sentences with period
- Protobuf for serialization of persisted/RPC types (prost)
- Run `bin/fmt` before committing

## Key Patterns

- **MIR (Mid-level IR):** `MirRelationExpr` and `MirScalarExpr` in `mz-expr` are the core query representation
- **Purification + Planning:** SQL compilation is two-phase: async purification (resolves external state), then pure planning
- **Datadriven tests:** Many crates use `datadriven` for snapshot-style testing; `REWRITE=1` updates expectations
- **Map-Filter-Project (MFP):** Fused operator in `expr::linear` for optimized row processing
- **Visitor pattern:** Expression trees use `Visit` trait for traversal and mutation
- **Feature flags:** `mz-dyncfg` for runtime feature configuration

## Workflow

- **Always test after changes.** Before committing, run the relevant tests:
  - Rust crate changes: `cargo test -p mz-<crate>` for each affected crate
  - SQL behavior changes: run the relevant SLT files with `bin/sqllogictest -- test/sqllogictest/FILE.slt`
  - If both apply, run both. Don't skip testing.
- **Large files:** Many source files here are 1000+ lines. For edits >100 lines, prefer targeted sed/bash commands over repeated Edit tool attempts.
- **Git rewrites:** When squashing, cherry-picking, rebasing, or amending, ALWAYS preserve the full commit message body (use `git log --format=%B`).
- **Docs in sync:** If you modify behavior that's documented (in `doc/`, READMEs, or code comments), update the docs in the same change.

## Gotchas

- `clippy.toml` disallows many stdlib methods — check it before using `HashMap`, `tokio::spawn`, `Iterator::zip`, etc.
- SLT tests with large data need `--optimized` flag: `bin/sqllogictest --optimized -- FILE.slt`
- Protobuf files need `buf format src -w` (included in `bin/fmt`)
- The `workspace-hack` crate exists for build performance — don't add deps to it manually
- `bin/` scripts are Python wrappers — they require the repo's Python environment
