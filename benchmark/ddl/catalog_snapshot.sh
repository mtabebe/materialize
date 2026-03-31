#!/usr/bin/env bash
#
# Catalog Snapshot Manager
#
# Save and restore Materialize catalog state for benchmarking with
# different catalog sizes. Snapshots capture the persist blob, CRDB
# consensus/tsoracle state, and environment metadata.
#
# IMPORTANT: Materialize (environmentd) must be STOPPED before save/restore.
#
# Usage:
#   ./catalog_snapshot.sh save   <name> [options]
#   ./catalog_snapshot.sh restore <name> [options]
#   ./catalog_snapshot.sh list   [options]
#   ./catalog_snapshot.sh delete  <name> [options]
#   ./catalog_snapshot.sh info    <name> [options]
#
# Options:
#   --mzdata DIR        Path to mzdata directory (default: ../../mzdata)
#   --crdb-url URL      CockroachDB URL (default: postgres://root@localhost:26257)
#   --snapshot-dir DIR  Where to store snapshots (default: ./snapshots)
#   --tables N          Number of tables in this snapshot (saved in metadata)
#
# Examples:
#   # Create 1000 tables, then snapshot
#   ./bench.sh create "postgres://materialize@localhost:6875/materialize" --tables 1000
#   # Stop Materialize, then:
#   ./catalog_snapshot.sh save 1000-tables
#
#   # Later, restore and benchmark
#   ./catalog_snapshot.sh restore 1000-tables
#   # Start Materialize, run benchmarks
#
#   # Manage snapshots
#   ./catalog_snapshot.sh list
#   ./catalog_snapshot.sh info 1000-tables
#   ./catalog_snapshot.sh delete 1000-tables

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MZ_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Defaults
MZDATA="$MZ_ROOT/mzdata"
CRDB_URL="postgres://root@localhost:26257"
SNAPSHOT_DIR="$SCRIPT_DIR/snapshots"
SNAP_TABLES=""

usage() {
    sed -n '3,39p' "$0" | sed 's/^# \?//'
    exit 1
}

if [[ $# -lt 1 ]] || [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    usage
fi

CMD="$1"
shift

# Commands that need a name
NAME=""
case "$CMD" in
    save|restore|delete|info)
        if [[ $# -lt 1 ]]; then
            echo "Error: '$CMD' requires a snapshot name"
            exit 1
        fi
        NAME="$1"
        shift
        ;;
    list) ;;
    *) echo "Unknown command: $CMD"; usage ;;
esac

# Parse options
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mzdata)       MZDATA="$2"; shift 2 ;;
        --crdb-url)     CRDB_URL="$2"; shift 2 ;;
        --snapshot-dir) SNAPSHOT_DIR="$2"; shift 2 ;;
        --tables)       SNAP_TABLES="$2"; shift 2 ;;
        *)              echo "Unknown option: $1"; usage ;;
    esac
done

CRDB_DB="materialize"

# Check that environmentd is not running
check_mz_stopped() {
    if pgrep -f "environmentd" > /dev/null 2>&1; then
        echo "Error: environmentd appears to be running. Stop it before save/restore."
        echo "  hint: kill the environmentd process or stop your dev environment"
        exit 1
    fi
}

crdb_sql() {
    psql "$CRDB_URL/$CRDB_DB" -t -A -c "$1" 2>/dev/null
}

case "$CMD" in
save)
    check_mz_stopped

    SNAP="$SNAPSHOT_DIR/$NAME"
    if [[ -d "$SNAP" ]]; then
        echo "Error: snapshot '$NAME' already exists. Delete it first or choose another name."
        exit 1
    fi

    echo "Saving snapshot '$NAME'..."
    mkdir -p "$SNAP"

    # 1. Copy persist blob directory
    BLOB_DIR="$MZDATA/persist/blob"
    if [[ -d "$BLOB_DIR" ]]; then
        echo "  Copying persist blob..."
        cp -a "$BLOB_DIR" "$SNAP/blob"
    else
        echo "  Warning: no persist blob directory at $BLOB_DIR"
        mkdir -p "$SNAP/blob"
    fi

    # 2. Copy environment-id
    ENV_ID_FILE="$MZDATA/environment-id"
    if [[ -f "$ENV_ID_FILE" ]]; then
        cp "$ENV_ID_FILE" "$SNAP/environment-id"
    fi

    # 3. Dump CRDB consensus and tsoracle schemas
    echo "  Dumping CRDB consensus schema..."
    psql "$CRDB_URL/$CRDB_DB" -c "SET search_path = consensus" -c "COPY (SELECT * FROM consensus) TO STDOUT WITH (FORMAT csv, HEADER)" \
        2>/dev/null | sed '1{/^SET$/d}' > "$SNAP/consensus.csv" || echo "  Warning: could not dump consensus table"

    echo "  Dumping CRDB tsoracle schema..."
    psql "$CRDB_URL/$CRDB_DB" -c "SET search_path = tsoracle" -c "COPY (SELECT * FROM tsoracle) TO STDOUT WITH (FORMAT csv, HEADER)" \
        2>/dev/null | sed '1{/^SET$/d}' > "$SNAP/tsoracle.csv" || echo "  Warning: could not dump tsoracle table"

    # 4. Record metadata
    cat > "$SNAP/metadata.json" <<METAEOF
{
    "name": "$NAME",
    "created_at": "$(date -Iseconds)",
    "tables": ${SNAP_TABLES:-null},
    "mzdata": "$MZDATA",
    "crdb_url": "$CRDB_URL",
    "blob_size": "$(du -sh "$SNAP/blob" 2>/dev/null | cut -f1 || echo "unknown")",
    "total_size": "$(du -sh "$SNAP" 2>/dev/null | cut -f1 || echo "unknown")"
}
METAEOF

    TOTAL_SIZE=$(du -sh "$SNAP" | cut -f1)
    echo "  Snapshot saved: $SNAP ($TOTAL_SIZE)"
    echo ""
    echo "Done. Restore with: ./catalog_snapshot.sh restore $NAME"
    ;;

restore)
    check_mz_stopped

    SNAP="$SNAPSHOT_DIR/$NAME"
    if [[ ! -d "$SNAP" ]]; then
        echo "Error: snapshot '$NAME' not found in $SNAPSHOT_DIR"
        echo "Available snapshots:"
        ls -1 "$SNAPSHOT_DIR" 2>/dev/null || echo "  (none)"
        exit 1
    fi

    echo "Restoring snapshot '$NAME'..."

    # 1. Restore persist blob
    BLOB_DIR="$MZDATA/persist/blob"
    echo "  Restoring persist blob..."
    rm -rf "$BLOB_DIR"
    mkdir -p "$(dirname "$BLOB_DIR")"
    cp -a "$SNAP/blob" "$BLOB_DIR"

    # 2. Restore environment-id
    if [[ -f "$SNAP/environment-id" ]]; then
        cp "$SNAP/environment-id" "$MZDATA/environment-id"
    fi

    # 3. Restore CRDB consensus
    if [[ -f "$SNAP/consensus.csv" ]]; then
        echo "  Restoring CRDB consensus..."
        psql "$CRDB_URL/$CRDB_DB" -c "SET search_path = consensus" -c "DELETE FROM consensus;" 2>/dev/null
        psql "$CRDB_URL/$CRDB_DB" -c "SET search_path = consensus" -c "COPY consensus FROM STDIN WITH (FORMAT csv, HEADER)" \
            < "$SNAP/consensus.csv" 2>/dev/null || echo "  Warning: could not restore consensus table"
    fi

    # 4. Restore CRDB tsoracle
    if [[ -f "$SNAP/tsoracle.csv" ]]; then
        echo "  Restoring CRDB tsoracle..."
        psql "$CRDB_URL/$CRDB_DB" -c "SET search_path = tsoracle" -c "DELETE FROM tsoracle;" 2>/dev/null
        psql "$CRDB_URL/$CRDB_DB" -c "SET search_path = tsoracle" -c "COPY tsoracle FROM STDIN WITH (FORMAT csv, HEADER)" \
            < "$SNAP/tsoracle.csv" 2>/dev/null || echo "  Warning: could not restore tsoracle table"
    fi

    echo ""
    echo "Done. Start Materialize to use the restored catalog."
    ;;

list)
    if [[ ! -d "$SNAPSHOT_DIR" ]] || [[ -z "$(ls -A "$SNAPSHOT_DIR" 2>/dev/null)" ]]; then
        echo "No snapshots found in $SNAPSHOT_DIR"
        exit 0
    fi

    printf "%-25s %-10s %-10s %-25s\n" "NAME" "TABLES" "SIZE" "CREATED"
    printf "%-25s %-10s %-10s %-25s\n" "----" "------" "----" "-------"
    for snap_dir in "$SNAPSHOT_DIR"/*/; do
        [[ -d "$snap_dir" ]] || continue
        snap_name=$(basename "$snap_dir")
        snap_size=$(du -sh "$snap_dir" 2>/dev/null | cut -f1)
        snap_date=""
        snap_tables="-"
        if [[ -f "$snap_dir/metadata.json" ]]; then
            snap_date=$(grep '"created_at"' "$snap_dir/metadata.json" | sed 's/.*": "//;s/".*//')
            snap_tables=$(grep '"tables"' "$snap_dir/metadata.json" | sed 's/[^0-9]//g')
            [[ -z "$snap_tables" ]] && snap_tables="-"
        fi
        printf "%-25s %-10s %-10s %-25s\n" "$snap_name" "$snap_tables" "$snap_size" "$snap_date"
    done
    ;;

delete)
    SNAP="$SNAPSHOT_DIR/$NAME"
    if [[ ! -d "$SNAP" ]]; then
        echo "Error: snapshot '$NAME' not found"
        exit 1
    fi

    SIZE=$(du -sh "$SNAP" | cut -f1)
    rm -rf "$SNAP"
    echo "Deleted snapshot '$NAME' ($SIZE)"
    ;;

info)
    SNAP="$SNAPSHOT_DIR/$NAME"
    if [[ ! -d "$SNAP" ]]; then
        echo "Error: snapshot '$NAME' not found"
        exit 1
    fi

    if [[ -f "$SNAP/metadata.json" ]]; then
        cat "$SNAP/metadata.json"
    fi
    echo ""
    echo "Contents:"
    du -sh "$SNAP"/* 2>/dev/null | sed 's|.*/||' | while read -r line; do
        echo "  $line"
    done
    ;;
esac
