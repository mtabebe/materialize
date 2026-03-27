# DDL Benchmark

Benchmark DDL (CREATE TABLE) latency at scale, with optional trace collection
to understand where time is spent.

## Quick Start

```bash
# Create 1000 tables in 10 chunks of 100
./bench.sh create "postgres://user@host:6875/materialize" --tables 1000 --chunks 10

# Same but with tracing (requires local monitoring stack)
./bench.sh create "postgres://user@host:6875/materialize" --tables 1000 --chunks 10 --trace

# With a realistic schema
./bench.sh create "postgres://..." --tables 1000 --chunks 10 --preset medium --trace

# Estimate network-adjusted latency (for scratch/local runs)
./bench.sh create "postgres://..." --tables 1000 --chunks 10 --trace --crdb-rtt 3

# Clean up
./bench.sh drop "postgres://..."
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--tables N` | 1000 | Number of tables to create |
| `--chunks K` | 10 | Number of timed batches |
| `--prefix PREFIX` | bench | Table name prefix |
| `--preset PRESET` | - | Schema preset: `narrow`, `medium`, `wide`, `json` |
| `--cols N --col-type TYPE` | - | Generate N columns of TYPE |
| `--schema SCHEMA` | `a int` | Exact column definition |
| `--trace` | off | Collect traces per chunk via Tempo |
| `--tempo-url URL` | `http://localhost:3200` | Tempo API URL |
| `--crdb-rtt MS` | 0 | CRDB round-trip latency for network cost estimate |
| `--output FILE` | auto | Output CSV path |

## Schema Presets

| Preset | Columns |
|--------|---------|
| `narrow` | `id int, name text, value double precision, created_at timestamp` |
| `medium` | `id int, user_id int, org_id int, status text, amount double precision, currency text, description text, created_at timestamp, updated_at timestamp, metadata jsonb` |
| `wide` | `c1 int, c2 int, .. c50 int` |
| `json` | `id int, payload jsonb, created_at timestamp` |

## Tracing Setup

Tracing requires the local monitoring stack (Tempo + Grafana):

```bash
# 1. Start monitoring stack
bin/mzcompose --find monitoring run default

# 2. Start environmentd with tracing
bin/environmentd --optimized --monitoring

# 3. Set trace filter
psql -U mz_system -h localhost -p 6877 materialize \
  -c "ALTER SYSTEM SET opentelemetry_filter = 'debug';"
```

## Output

- **CSV file**: Per-chunk timing with columns `chunk,tables_in_chunk,start_index,end_index,elapsed_ms[,trace_id]`
- **Traces directory** (with `--trace`): One JSON file per chunk, plus analysis output showing:
  - Top spans by self-time
  - CRDB round-trip count and breakdown
  - Network-adjusted latency estimate (with `--crdb-rtt`)

## Analyzing Traces Standalone

```bash
python3 analyze_trace.py trace.json "my label" --crdb-rtt 3
```
