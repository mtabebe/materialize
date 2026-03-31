# DDL Benchmark

Benchmark DDL (CREATE TABLE) latency at scale, with optional trace collection
to understand where time is spent.

## Quick Start

```bash
# 1. Set resource limits (Materialize must be running)
./set_limits.sh 1000

# 2. Create 1000 tables in 10 chunks of 100
./bench.sh create "postgres://materialize@localhost:6875/materialize" --tables 1000 --chunks 10

# 3. Stop Materialize, save the catalog for reuse
./catalog_snapshot.sh save 1000-tables --tables 1000

# 4. Later, restore and benchmark without recreating tables
./catalog_snapshot.sh restore 1000-tables
# start Materialize — boots with 1000 tables already present
```

## Scripts

| Script | Purpose |
|--------|---------|
| `bench.sh` | Create/drop tables in timed chunks with optional tracing |
| `catalog_snapshot.sh` | Save/restore catalog state for fast benchmarking |
| `set_limits.sh` | Configure `max_tables` and `max_objects_per_schema` |
| `analyze_trace.py` | Analyze OpenTelemetry trace JSON from Tempo |

## bench.sh

### Usage

```bash
./bench.sh create <conn_string> [options]
./bench.sh drop   <conn_string> [--prefix PREFIX]
```

### Options

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

### Schema Presets

| Preset | Columns |
|--------|---------|
| `narrow` | `id int, name text, value double precision, created_at timestamp` |
| `medium` | `id int, user_id int, org_id int, status text, amount double precision, currency text, description text, created_at timestamp, updated_at timestamp, metadata jsonb` |
| `wide` | `c1 int, c2 int, .. c50 int` |
| `json` | `id int, payload jsonb, created_at timestamp` |

### Examples

```bash
# Basic run
./bench.sh create "postgres://materialize@localhost:6875/materialize" --tables 1000 --chunks 10

# With tracing
./bench.sh create "postgres://materialize@localhost:6875/materialize" --tables 1000 --chunks 10 --trace

# With a realistic schema and network cost estimate
./bench.sh create "postgres://materialize@localhost:6875/materialize" --preset medium --trace --crdb-rtt 3

# Clean up
./bench.sh drop "postgres://materialize@localhost:6875/materialize"
```

### Output

- **CSV file**: Per-chunk timing with columns `chunk,tables_in_chunk,start_index,end_index,elapsed_ms[,trace_id]`
- **Traces directory** (with `--trace`): One JSON file per chunk, plus analysis output showing:
  - Top spans by self-time
  - CRDB round-trip count and breakdown
  - Network-adjusted latency estimate (with `--crdb-rtt`)

## catalog_snapshot.sh

Save and restore Materialize catalog state so you can benchmark with different
catalog sizes without recreating tables each time. Snapshots capture the persist
blob, CRDB consensus/tsoracle state, and environment metadata.

**Materialize must be stopped before save/restore.**

### Usage

```bash
./catalog_snapshot.sh save    <name> [options]
./catalog_snapshot.sh restore <name> [options]
./catalog_snapshot.sh list    [options]
./catalog_snapshot.sh delete  <name> [options]
./catalog_snapshot.sh info    <name> [options]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mzdata DIR` | `../../mzdata` | Path to mzdata directory |
| `--crdb-url URL` | `postgres://root@localhost:26257` | CockroachDB URL |
| `--snapshot-dir DIR` | `./snapshots` | Where to store snapshots |
| `--tables N` | - | Number of tables (saved in metadata) |

### Examples

```bash
# Build a library of catalog sizes
./set_limits.sh 100
./bench.sh create "postgres://materialize@localhost:6875/materialize" --tables 100
# stop Materialize
./catalog_snapshot.sh save 100-tables --tables 100

# restore, set limits, add more tables
./catalog_snapshot.sh restore 100-tables
# start Materialize
./set_limits.sh 1000
./bench.sh create "postgres://materialize@localhost:6875/materialize" --tables 900 --prefix bench2
# stop Materialize
./catalog_snapshot.sh save 1000-tables --tables 1000

# Manage snapshots
./catalog_snapshot.sh list
./catalog_snapshot.sh info 1000-tables
./catalog_snapshot.sh delete old-snap
```

## set_limits.sh

Materialize enforces `max_tables` (default 200) and `max_objects_per_schema`
(default 1000). Use this script to raise them before creating or benchmarking
with large catalogs.

**Materialize must be running when this script is executed.**

### Usage

```bash
./set_limits.sh <tables> [options]
./set_limits.sh --show
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--multiplier N` | 2 | Multiplier applied to table count |
| `--system-port PORT` | 6877 | Port for mz_system connections |
| `--host HOST` | localhost | Materialize host |
| `--show` | - | Show current limits instead of setting them |

### Examples

```bash
./set_limits.sh 5000                    # sets max_tables=10000, max_objects_per_schema=10000
./set_limits.sh 5000 --multiplier 3     # sets both to 15000
./set_limits.sh --show                  # display current values
```

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

## Analyzing Traces Standalone

```bash
python3 analyze_trace.py trace.json "my label" --crdb-rtt 3
```
