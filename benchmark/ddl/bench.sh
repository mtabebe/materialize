#!/usr/bin/env bash
#
# DDL Benchmark Utility
#
# Creates N tables in K chunks and times each chunk.
# Optionally collects OpenTelemetry traces per chunk via Tempo.
# Results are saved to a CSV file; traces to a directory.
#
# Usage:
#   ./bench.sh create <conn_string> [options]
#   ./bench.sh drop   <conn_string> [--prefix PREFIX]
#
# Options:
#   --tables N                 Number of tables to create (default: 1000)
#   --chunks K                 Number of timed chunks (default: 10)
#   --prefix PREFIX            Table name prefix (default: bench)
#   --output FILE              Output CSV path (default: auto-generated)
#   --preset PRESET            Schema preset: narrow, medium, wide, json
#   --cols N --col-type TYPE   Generate N columns of TYPE
#   --schema SCHEMA            Exact column definition string
#   --trace                    Collect traces per chunk via Tempo (localhost:3200)
#   --tempo-url URL            Tempo API base URL (default: http://localhost:3200)
#   --crdb-rtt MS              Estimated CRDB round-trip latency in ms for cost model (default: 0)
#
# Schema presets:
#   narrow   4 cols: id int, name text, value double precision, created_at timestamp
#   medium  10 cols: id int, user_id int, org_id int, status text, amount double precision,
#                    currency text, description text, created_at timestamp, updated_at timestamp, metadata jsonb
#   wide    50 cols: c1 int, c2 int, .. c50 int
#   json     3 cols: id int, payload jsonb, created_at timestamp
#
# Examples:
#   ./bench.sh create "postgres://user@host:6875/materialize" --tables 1000 --chunks 10
#   ./bench.sh create "postgres://user@host:6875/materialize" --preset medium --trace
#   ./bench.sh create "postgres://user@host:6875/materialize" --preset json --trace --crdb-rtt 3
#   ./bench.sh drop   "postgres://user@host:6875/materialize"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
TABLES=1000
CHUNKS=10
PREFIX="bench"
OUTPUT=""
COLS=""
COL_TYPE=""
SCHEMA=""
PRESET=""
TRACE=false
TEMPO_URL="http://localhost:3200"
CRDB_RTT=0

usage() {
    sed -n '3,37p' "$0" | sed 's/^# \?//'
    exit 1
}

now_ns() {
    python3 -c 'import time; print(int(time.time()*1e9))'
}

if [[ $# -lt 1 ]] || [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    usage
fi

CMD="$1"

if [[ $# -lt 2 ]]; then
    usage
fi

CONN="$2"
shift 2

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tables)    TABLES="$2"; shift 2 ;;
        --chunks)    CHUNKS="$2"; shift 2 ;;
        --prefix)    PREFIX="$2"; shift 2 ;;
        --output)    OUTPUT="$2"; shift 2 ;;
        --cols)      COLS="$2"; shift 2 ;;
        --col-type)  COL_TYPE="$2"; shift 2 ;;
        --schema)    SCHEMA="$2"; shift 2 ;;
        --preset)    PRESET="$2"; shift 2 ;;
        --trace)     TRACE=true; shift ;;
        --tempo-url) TEMPO_URL="$2"; shift 2 ;;
        --crdb-rtt)  CRDB_RTT="$2"; shift 2 ;;
        *)           echo "Unknown option: $1"; usage ;;
    esac
done

# Resolve table schema
opt_count=0
[[ -n "$PRESET" ]] && opt_count=$((opt_count + 1))
[[ -n "$SCHEMA" ]] && opt_count=$((opt_count + 1))
[[ -n "$COLS" ]] && opt_count=$((opt_count + 1))
if [[ $opt_count -gt 1 ]]; then
    echo "Error: --preset, --schema, and --cols are mutually exclusive"
    exit 1
fi

if [[ -n "$PRESET" ]]; then
    case "$PRESET" in
        narrow)
            TABLE_SCHEMA="id int, name text, value double precision, created_at timestamp"
            ;;
        medium)
            TABLE_SCHEMA="id int, user_id int, org_id int, status text, amount double precision, currency text, description text, created_at timestamp, updated_at timestamp, metadata jsonb"
            ;;
        wide)
            TABLE_SCHEMA=""
            for c in $(seq 1 50); do
                [[ -n "$TABLE_SCHEMA" ]] && TABLE_SCHEMA+=", "
                TABLE_SCHEMA+="c${c} int"
            done
            ;;
        json)
            TABLE_SCHEMA="id int, payload jsonb, created_at timestamp"
            ;;
        *)
            echo "Error: unknown preset '$PRESET' (available: narrow, medium, wide, json)"
            exit 1
            ;;
    esac
elif [[ -n "$SCHEMA" ]]; then
    TABLE_SCHEMA="$SCHEMA"
elif [[ -n "$COLS" ]]; then
    if [[ -z "$COL_TYPE" ]]; then
        echo "Error: --cols requires --col-type"
        exit 1
    fi
    TABLE_SCHEMA=""
    for c in $(seq 1 "$COLS"); do
        [[ -n "$TABLE_SCHEMA" ]] && TABLE_SCHEMA+=", "
        TABLE_SCHEMA+="c${c} ${COL_TYPE}"
    done
else
    TABLE_SCHEMA="a int"
fi

# Fetch a trace from Tempo with retries
fetch_trace() {
    local trace_id="$1"
    local output_file="$2"
    for i in $(seq 1 6); do
        local resp
        resp=$(curl -s "${TEMPO_URL}/api/traces/${trace_id}")
        if echo "$resp" | grep -q "batches"; then
            echo "$resp" > "$output_file"
            return 0
        fi
        sleep 5
    done
    echo "  WARNING: Could not fetch trace $trace_id after 30s" >&2
    return 1
}

case "$CMD" in
create)
    PER_CHUNK=$(( TABLES / CHUNKS ))
    REMAINDER=$(( TABLES % CHUNKS ))

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    if [[ -z "$OUTPUT" ]]; then
        OUTPUT="results_${PREFIX}_${TABLES}t_${CHUNKS}c_${TIMESTAMP}.csv"
    fi

    TRACE_DIR=""
    if [[ "$TRACE" == true ]]; then
        TRACE_DIR="traces_${PREFIX}_${TABLES}t_${CHUNKS}c_${TIMESTAMP}"
        mkdir -p "$TRACE_DIR"
        echo "Traces will be saved to $TRACE_DIR/"
        # Enable trace ID notices for the session
        TRACE_SET="SET emit_trace_id_notice = true;"
    fi

    echo "Creating $TABLES tables in $CHUNKS chunks of ~$PER_CHUNK (prefix: ${PREFIX}_)"
    echo "Schema: ($TABLE_SCHEMA)"
    if [[ "$TRACE" == true ]]; then
        echo "chunk,tables_in_chunk,start_index,end_index,elapsed_ms,trace_id" > "$OUTPUT"
    else
        echo "chunk,tables_in_chunk,start_index,end_index,elapsed_ms" > "$OUTPUT"
    fi

    idx=1
    total_ms=0
    for chunk in $(seq 1 "$CHUNKS"); do
        count=$PER_CHUNK
        if [[ $chunk -le $REMAINDER ]]; then
            count=$(( count + 1 ))
        fi

        end_idx=$(( idx + count - 1 ))

        # Generate SQL for this chunk
        sql=""
        for i in $(seq "$idx" "$end_idx"); do
            sql+="CREATE TABLE ${PREFIX}_${i} (${TABLE_SCHEMA});"
        done

        trace_id=""
        if [[ "$TRACE" == true ]]; then
            # Run with trace ID capture — each DDL gets its own trace ID,
            # but they all run in the same psql session/connection
            start_ns=$(now_ns)
            psql_output=$(echo "${TRACE_SET} ${sql}" | psql "$CONN" 2>&1)
            end_ns=$(now_ns)
            # Extract the last trace ID (the one for the DDL batch)
            trace_id=$(echo "$psql_output" | grep "trace id:" | tail -1 | sed 's/.*trace id: //')
        else
            start_ns=$(now_ns)
            echo "$sql" | psql "$CONN" -q > /dev/null
            end_ns=$(now_ns)
        fi

        elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
        total_ms=$(( total_ms + elapsed_ms ))
        avg_per_table=$(( elapsed_ms / count ))

        if [[ -n "$trace_id" ]]; then
            echo "  chunk $chunk/$CHUNKS: tables $idx-$end_idx ($count tables) in ${elapsed_ms}ms (${avg_per_table}ms/table) [trace: $trace_id]"
            echo "$chunk,$count,$idx,$end_idx,$elapsed_ms,$trace_id" >> "$OUTPUT"
        else
            echo "  chunk $chunk/$CHUNKS: tables $idx-$end_idx ($count tables) in ${elapsed_ms}ms (${avg_per_table}ms/table)"
            echo "$chunk,$count,$idx,$end_idx,$elapsed_ms" >> "$OUTPUT"
        fi

        idx=$(( end_idx + 1 ))
    done

    avg_total=$(( total_ms / TABLES ))
    echo ""
    echo "Done. Total: ${total_ms}ms, Avg: ${avg_total}ms/table"
    echo "Results saved to $OUTPUT"

    # Fetch traces after all chunks complete (gives Tempo time to ingest)
    if [[ "$TRACE" == true && -n "$TRACE_DIR" ]]; then
        echo ""
        echo "Waiting 10s for trace ingestion..."
        sleep 10
        echo "Fetching traces from Tempo..."
        while IFS=, read -r chunk_num tables_in_chunk start_index end_index elapsed trace; do
            [[ "$chunk_num" == "chunk" ]] && continue  # skip header
            trace=$(echo "$trace" | tr -d '[:space:]')
            if [[ -n "$trace" ]]; then
                trace_file="${TRACE_DIR}/chunk_${chunk_num}.json"
                if fetch_trace "$trace" "$trace_file"; then
                    echo "  chunk $chunk_num: saved $trace_file"
                fi
            fi
        done < "$OUTPUT"

        # Analyze traces
        echo ""
        echo "Analyzing traces..."
        for trace_file in "$TRACE_DIR"/chunk_*.json; do
            [[ -f "$trace_file" ]] || continue
            chunk_name=$(basename "$trace_file" .json)
            python3 "${SCRIPT_DIR}/analyze_trace.py" "$trace_file" "$chunk_name" --crdb-rtt "$CRDB_RTT"
        done
    fi
    ;;

drop)
    echo "Discovering tables with prefix '${PREFIX}_'..."
    drop_sql=$(psql "$CONN" -t -A -c \
        "SELECT 'DROP TABLE ' || name || ' CASCADE;'
         FROM mz_tables
         WHERE schema_id = (SELECT id FROM mz_schemas WHERE name = 'public')
           AND name LIKE '${PREFIX}_%'
         ORDER BY name;")

    count=$(echo "$drop_sql" | grep -c 'DROP TABLE' || true)
    if [[ $count -eq 0 ]]; then
        echo "No tables found with prefix '${PREFIX}_'"
        exit 0
    fi

    echo "Dropping $count tables..."
    start_ns=$(now_ns)
    echo "$drop_sql" | psql "$CONN" -q > /dev/null
    end_ns=$(now_ns)

    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "Dropped $count tables in ${elapsed_ms}ms"
    ;;

*)
    echo "Unknown command: $CMD"
    usage
    ;;
esac
