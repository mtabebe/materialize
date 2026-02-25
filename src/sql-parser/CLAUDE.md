# mz-sql-parser

Recursive descent SQL parser for Materialize's SQL dialect. Forked from sqlparser-rs (Dec 2019), heavily modified.

## Key Files

- `parser.rs` — Main parser: recursive descent, produces AST
- `ast.rs` — AST node definitions for all SQL constructs
- `lib.rs` — Entry point with datadriven test harness

## Conventions

- Minimal dependencies by design (stdlib extension philosophy)
- Roundtrip validation: parse → pretty-print → reparse must be identical
- Datadriven tests for parser regression coverage
- No dependency on `tracing` (`assert-no-tracing` feature)

## Test

```bash
cargo test -p mz-sql-parser
REWRITE=1 cargo test -p mz-sql-parser    # update snapshots
```

## Dependencies

Depends on: `mz-sql-lexer` (tokenization), `mz-ore`. No other mz-* dependencies.
