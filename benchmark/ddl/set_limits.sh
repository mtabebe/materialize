#!/usr/bin/env bash
#
# Set Resource Limits
#
# Configures max_tables and max_objects_per_schema to accommodate
# large catalogs for benchmarking. Sets both limits to 2x the
# specified table count.
#
# Materialize must be RUNNING when this script is executed.
#
# Usage:
#   ./set_limits.sh <tables> [options]
#
# Options:
#   --multiplier N       Multiplier for the limit (default: 2)
#   --system-port PORT   Port for mz_system connections (default: 6877)
#   --host HOST          Materialize host (default: localhost)
#   --show               Show current limits instead of setting them
#
# Examples:
#   ./set_limits.sh 1000                    # sets max_tables=2000, max_objects_per_schema=2000
#   ./set_limits.sh 5000 --multiplier 3     # sets both to 15000
#   ./set_limits.sh --show                  # display current limits

set -euo pipefail

# Defaults
MULTIPLIER=2
SYSTEM_PORT=6877
HOST="localhost"
SHOW=false
TABLES=""

usage() {
    sed -n '3,26p' "$0" | sed 's/^# \?//'
    exit 1
}

if [[ $# -lt 1 ]] || [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    usage
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --multiplier)   MULTIPLIER="$2"; shift 2 ;;
        --system-port)  SYSTEM_PORT="$2"; shift 2 ;;
        --host)         HOST="$2"; shift 2 ;;
        --show)         SHOW=true; shift ;;
        -*)             echo "Unknown option: $1"; usage ;;
        *)
            if [[ -z "$TABLES" ]]; then
                TABLES="$1"; shift
            else
                echo "Unknown argument: $1"; usage
            fi
            ;;
    esac
done

SYS_CONN="postgres://mz_system@${HOST}:${SYSTEM_PORT}/materialize"

if [[ "$SHOW" == true ]]; then
    echo "Current resource limits:"
    echo -n "  max_tables = "
    psql "$SYS_CONN" -t -A -c "SHOW max_tables;"
    echo -n "  max_objects_per_schema = "
    psql "$SYS_CONN" -t -A -c "SHOW max_objects_per_schema;"
    exit 0
fi

if [[ -z "$TABLES" ]]; then
    echo "Error: table count required (or use --show)"
    usage
fi

LIMIT=$(( TABLES * MULTIPLIER ))

echo "Setting resource limits (${TABLES} tables x${MULTIPLIER}):"
echo "  max_tables = $LIMIT"
psql "$SYS_CONN" -c "ALTER SYSTEM SET max_tables = $LIMIT;" -q

echo "  max_objects_per_schema = $LIMIT"
psql "$SYS_CONN" -c "ALTER SYSTEM SET max_objects_per_schema = $LIMIT;" -q

echo "Done."
