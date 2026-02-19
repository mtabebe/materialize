---
name: materialize-testing
description: Guides adding and running tests in the Materialize repo: testdrive (.td), sqllogictest (.slt), pgtest (.pt), and Rust unit/integration tests. Use when the user asks to add a test, run testdrive, run sqllogictest, reproduce a bug, test SQL, run unit tests, or where to put a test.
---

# Materialize testing

How to add and run tests for SQL, APIs, and pipelines. Official docs: [guide-testing.md](doc/developer/guide-testing.md), [testdrive.md](doc/developer/testdrive.md), [sqllogictest.md](doc/developer/sqllogictest.md), [mzcompose.md](doc/developer/mzcompose.md).

## Where to put tests

| What you're testing | Where | Format |
|--------------------|--------|--------|
| SQL correctness, query plans, types/functions | `test/sqllogictest/` | `.slt` |
| Sources/sinks, Kafka, catalog, pgwire serialization | `test/testdrive/` | `.td` |
| Postgres direct replication | `test/pg-cdc/` | `.td` |
| Debezium | `test/debezium-avro` | `.td` |
| Raw pgwire message sequences (COPY, extended protocol) | `test/pgtest/` | `.pt` |
| Pure logic, decoding, pure functions | next to code | Rust `#[test]` in `mod tests` or `tests/` |
| Non-standard services or options | new dir under `test/` with `mzcompose.py` | workflow + `.td` or other |

Do **not** add or modify files in `test/sqllogictest/sqlite` or `test/sqllogictest/cockroach` (upstream); add Materialize-specific tests in `test/sqllogictest/`.

## Running tests

### Unit / integration tests (Rust)

```bash
# Full suite
bin/cargo-test

# One crate
cargo test -p CRATE_NAME

# Filter by test name
bin/cargo-test -- avro

# Show println/dbg output
cargo test -- --nocapture

# Log filter (default is info)
MZ_TEST_LOG_FILTER=debug cargo test -p environmentd
```

Tests live in `mod tests { ... }` in the same crate or in the crate’s `tests/` directory. Some crates need Kafka + Schema Registry (see [guide.md](doc/developer/guide.md)).

**pgtest** (`.pt` files in `test/pgtest/`) are run as integration tests from the `environmentd` crate:

```bash
cargo test -p environmentd pgtest
# or a single file, e.g. copy-from-range
cargo test -p environmentd test_pgtest_copy_from_range
```

New `.pt` files must be wired in `src/environmentd/tests/pgwire.rs` (e.g. a `test_pgtest_*` that calls `pg_test_inner(Path::new("../../test/pgtest/FOO.pt"), false)`).

### sqllogictest

```bash
# One or more files (use --optimized for large files)
bin/sqllogictest [--release] [--optimized] -- test/sqllogictest/FILE.slt

# With logging
bin/sqllogictest [--optimized] -- test/sqllogictest/FILE.slt -vv

# Rewrite expected results (e.g. after planner changes)
bin/sqllogictest -- --rewrite-results test/sqllogictest/FILE.slt
```

- Materialize-specific: `test/sqllogictest/*.slt` (and subdirs, but not `sqlite/` or `cockroach/`).
- Basic syntax: `query I rowsort` (or T, R, etc.), query text, `----`, expected rows. See [sqllogictest.md](doc/developer/sqllogictest.md) for `statement error`, `query error`, `simple`, `reset-server`, `mode cockroach`/`mode standard`, etc.

### testdrive

**With mzcompose (recommended):** starts Materialize, Kafka, Schema Registry, etc.

```bash
cd test/testdrive
./mzcompose down -v
./mzcompose run default              # all .td in test/testdrive
./mzcompose run default FILE.td      # single file
./mzcompose run default --redpanda   # use Redpanda instead of Confluent (better on ARM/M1)
```

**Local (two terminals):** Terminal 1: `./bin/environmentd [--release]`. Terminal 2:

```bash
cargo run --bin testdrive [--release] -- test/testdrive/FILE.td
```

For tests that need all features: `MZ_ALL_FEATURES=true bin/environmentd` and same testdrive command. Kafka/Schema Registry must be running for many tests (see [guide-testing](doc/developer/guide-testing.md)).

### Other workflows (pg-cdc, debezium, etc.)

```bash
cd test/<directory>   # e.g. test/pg-cdc, test/debezium-avro
./mzcompose down -v
./mzcompose run default
```

## testdrive syntax (quick reference)

- **`> SELECT ...`** — SQL that must succeed; next lines are expected output (space-separated columns). Rows compared unordered unless order matters.
- **`! SELECT ...`** — SQL that must fail. Next line: `contains:substring` or `exact:full message` or `regex:...`.
- **`$ set name=value`** — Variable; use `${name}` in later commands.
- **`$ kafka-ingest topic=... format=avro schema=${schema} ...`** — Ingest into Kafka (body is JSON/records).
- **`$ set-regex match=... replacement=...`** — Normalize variable output (e.g. EXPLAIN).
- **`? EXPLAIN SELECT ...`** — EXPLAIN with expected plan (use set-regex for unstable bits).
- Multi-line SQL: indent continuation lines with two spaces.
- Optional column names: put them above the rows, then `---`, then data.

Files: copyright header, no trailing spaces/newlines (enforced by `bin/lint`). Kafka-related: name `kafka-*.td`.

## sqllogictest (quick reference)

- **`query I rowsort`** — Integer result, sort rows before comparing (similar: T text, R real, B bool).
- **`statement error`** / **`query error`** — Expect failure; optional error substring after.
- **`----`** — Separator before expected output.
- **`mode cockroach`** / **`mode standard`** — Result formatting; use `mode cockroach` for most Materialize-specific tests.
- **`reset-server`** — New Mz instance (e.g. for stable object IDs in EXPLAIN).
- **`simple`** — Multi-statement / simple query over pgwire (see [sqllogictest.md](doc/developer/sqllogictest.md)).

Test new functions with NULLs and edge cases (0, negative, infinity) as in the [sqllogictest extended guide](doc/developer/sqllogictest.md).

## Reproducing a bug

1. **SQL / planner / type bug** — Add a minimal `.slt` in `test/sqllogictest/` (or extend an existing file), run with `bin/sqllogictest -- test/sqllogictest/FILE.slt`.
2. **Protocol / COPY / pgwire** — Add or extend a `.pt` in `test/pgtest/`, wire in `environmentd/tests/pgwire.rs`, run `cargo test -p environmentd pgtest`.
3. **End-to-end with Kafka/sources/sinks** — Add a `.td` in `test/testdrive/` (or the right test dir), run with `./mzcompose run default FILE.td` from that directory.
4. **Unit-level** — Add a `#[test]` in the crate and run `cargo test -p CRATE [-- --nocapture]`.

## Checklist when adding a test

- [ ] Chosen the right framework and directory (slt vs td vs pt vs Rust).
- [ ] testdrive: copyright header, no trailing spaces; Kafka tests named `kafka-*.td`.
- [ ] sqllogictest: only add under `test/sqllogictest/`, not in sqlite/cockroach.
- [ ] pgtest: new `.pt` added to `environmentd/tests/pgwire.rs`.
- [ ] Run the test locally before pushing (commands above).
