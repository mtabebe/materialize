#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# PROTOTYPE: helpers for the hand-run persist resolver demo. README.md in this
# directory is the runbook. Run from anywhere; paths are the repository root's.
#
#   demo.sh reset                stop everything, wipe mzdata/ and the CRDB schemas
#   demo.sh environmentd [ARG..] run environmentd in the foreground, logging to a file
#   demo.sh labels               load each shard's kind and name into CRDB, as demo.shard_labels
#   demo.sh down                 stop everything

set -euo pipefail

cd "$(dirname "$0")/../../.."
root=$PWD
target=${CARGO_TARGET_DIR:-$root/target}/${PROFILE:-debug}
crdb=postgres://root@localhost:26257/materialize
rel_state=scratch/persist-resolver-demo
state=$root/$rel_state
resolver_http=127.0.0.1:6891
control_shard=s00000000-0000-0000-0000-00000000c0de

die() { echo "demo.sh: $*" >&2; exit 1; }

need_binary() {
    [ -x "$target/$1" ] || die "$target/$1 isn't built: cargo build --bin $1"
}

uuid() {
    if command -v uuidgen >/dev/null; then
        uuidgen | tr '[:upper:]' '[:lower:]'
    else
        cat /proc/sys/kernel/random/uuid
    fi
}

mz_quiet() { PGOPTIONS='--welcome_message=off' psql -h localhost -p 6875 -U materialize -At -F ' ' -c "$1"; }

# Stops every environmentd, clusterd and persist-resolver run from a target/
# directory. The demo starts them by hand in their own terminals, so there are
# no pids to go by.
down() {
    local pattern='target/[^ ]*/(environmentd|clusterd|persist-resolver)( |$)'
    pkill -f "$pattern" 2>/dev/null || true
    for _ in $(seq 50); do
        pgrep -f "$pattern" >/dev/null || return 0
        sleep 0.2
    done
    pkill -KILL -f "$pattern" 2>/dev/null || true
    sleep 0.5
    if pgrep -f "$pattern" >/dev/null; then
        die "processes survived SIGKILL: $(pgrep -f "$pattern" | tr '\n' ' ')"
    fi
}

reset() {
    down
    rm -rf "$root/mzdata"
    mkdir -p "$root/mzdata/persist/blob" "$root/mzdata/secrets" "$root/mzdata/prometheus" "$root/scratch" "$state"
    psql "$crdb" -q -c "CREATE DATABASE IF NOT EXISTS materialize" >/dev/null
    psql "$crdb" -q -c "DROP SCHEMA IF EXISTS demo CASCADE" >/dev/null
    for schema in consensus tsoracle storage; do
        psql "$crdb" -q -c "DROP SCHEMA IF EXISTS $schema CASCADE" -c "CREATE SCHEMA $schema" >/dev/null
    done
    echo "reset: mzdata/ and the consensus, tsoracle and storage schemas are empty."
}

# The arguments `bin/environmentd` passes, minus its Python wrapper, plus "$@".
environmentd() {
    need_binary environmentd
    need_binary clusterd
    mkdir -p "$state"
    local size='{"cpu_exclusive":false,"cpu_limit":null,"credits_per_hour":"1","disabled":false,"disk_limit":null,"is_cc":true,"memory_limit":"4 GiB","scale":1,"workers":1}'
    local sizes="{\"bootstrap\":$size,\"scale=1,workers=1\":$size}"
    local consensus="$crdb?options=--search_path=consensus"
    echo "environmentd: logging to $rel_state/environmentd.log. Ctrl-C stops it." >&2
    exec "$target/environmentd" \
        --listeners-config-path="$root/src/materialized/ci/listener_configs/v26_32_0/no_auth.json" \
        --orchestrator=process \
        --orchestrator-process-secrets-directory="$root/mzdata/secrets" \
        --orchestrator-process-tcp-proxy-listen-addr=0.0.0.0 \
        --orchestrator-process-prometheus-service-discovery-directory="$root/mzdata/prometheus" \
        --orchestrator-process-scratch-directory="$root/scratch" \
        --secrets-controller=local-file \
        --persist-consensus-url="$consensus" \
        --persist-blob-url="file://$root/mzdata/persist/blob" \
        --timestamp-oracle-url="$crdb?options=--search_path=tsoracle" \
        --environment-id="local-az1-$(uuid)-0" \
        --bootstrap-role=materialize \
        --cluster-replica-sizes="$sizes" \
        --bootstrap-default-cluster-replica-size=bootstrap \
        "$@" >"$state/environmentd.log" 2>&1
}

# Writes demo.shard_labels (shard, kind, name) to CRDB: every shard Materialize
# lists by name, the resolver's manifest shards if it's up, and every other
# shard with a consensus row as kind 'other'.
labels() {
    local rows
    rows=$(mz_quiet "SELECT s.shard_id,
            CASE WHEN s.object_id LIKE 'u%' THEN 'user' WHEN o.name = 'mz_catalog_raw' THEN 'catalog' ELSE 'builtin' END,
            o.name
        FROM mz_internal.mz_storage_shards s JOIN mz_objects o ON s.object_id = o.id")
    local partition_shard
    if partition_shard=$(curl -sf "$resolver_http/views" | jq -r .partition_manifest.shard); then
        rows+=$'\n'"$control_shard resolver-manifest control"$'\n'"$partition_shard resolver-manifest partition"
    fi
    {
        echo "CREATE SCHEMA IF NOT EXISTS demo;"
        echo "DROP TABLE IF EXISTS demo.shard_labels;"
        echo "CREATE TABLE demo.shard_labels (shard STRING PRIMARY KEY, kind STRING NOT NULL, name STRING NOT NULL);"
        echo "INSERT INTO demo.shard_labels VALUES"
        awk 'NF { printf "%s('\''%s'\'', '\''%s'\'', '\''%s'\'')\n", (n++ ? "," : ""), $1, $2, $3 }' <<<"$rows"
        echo ";"
        echo "INSERT INTO demo.shard_labels SELECT DISTINCT shard, 'other', '' FROM consensus.consensus WHERE shard NOT IN (SELECT shard FROM demo.shard_labels);"
    } | psql "$crdb" -q -v ON_ERROR_STOP=1 >/dev/null
    psql "$crdb" -At -F ' ' -c "SELECT kind, count(*) FROM demo.shard_labels GROUP BY kind ORDER BY kind" |
        awk '{ printf "%s%s %s", (NR > 1 ? ", " : "labels: "), $2, $1 } END { print "" }'
}

case "${1:-}" in
reset) reset ;;
environmentd) shift; environmentd "$@" ;;
labels) labels ;;
down) down; echo "down." ;;
*) sed -n '/^#   demo.sh/s/^# *//p' "$0" >&2; exit 2 ;;
esac
