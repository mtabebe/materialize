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
# PROTOTYPE: runs Materialize with user collections on v2 shards.
#
# Starts a fresh local environment (mzdata and the CRDB schemas are wiped), a
# resolver on the same consensus and blob, and environmentd with
# MZ_PERSIST_RESOLVER_URL set, then runs resolver.td and checks that every user
# collection's shard has no row of its own in consensus: it lives in the
# resolver's manifests.
#
# Needs a local CockroachDB on localhost:26257 and the binaries built:
#   cargo build --bin environmentd --bin clusterd --bin persist-resolver --bin testdrive

set -euo pipefail

cd "$(dirname "$0")/../.."
root=$PWD
target=${CARGO_TARGET_DIR:-$root/target}/debug
postgres=postgres://root@localhost:26257/materialize
logs=${LOGS:-$root/scratch/persist-resolver}
mkdir -p "$logs"

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

rm -rf "$root/mzdata"
psql "$postgres" -q -c "CREATE DATABASE IF NOT EXISTS materialize" >/dev/null
for schema in consensus tsoracle storage; do
    psql "$postgres" -q -c "DROP SCHEMA IF EXISTS $schema CASCADE" -c "CREATE SCHEMA $schema" >/dev/null
done
mkdir -p "$root/mzdata/persist/blob"

"$target/persist-resolver" \
    --blob-url "file://$root/mzdata/persist/blob" \
    --consensus-url "$postgres?options=--search_path=consensus" \
    --listen-addr 127.0.0.1:6890 >"$logs/resolver.log" 2>&1 &
pids+=($!)
until grep -q "claimed partition" "$logs/resolver.log"; do sleep 0.5; done

# The arguments `bin/environmentd` passes, minus its Python wrapper.
size='{"cpu_exclusive":false,"cpu_limit":null,"credits_per_hour":"1","disabled":false,"disk_limit":null,"is_cc":true,"memory_limit":"4 GiB","scale":1,"workers":1}'
sizes="{\"bootstrap\":$size,\"scale=1,workers=1\":$size}"
mkdir -p "$root/mzdata/secrets" "$root/mzdata/prometheus" "$root/scratch"
MZ_PERSIST_RESOLVER_URL=http://127.0.0.1:6890 RUST_MIN_STACK=${RUST_MIN_STACK:-16777216} \
"$target/environmentd" \
    --listeners-config-path="$root/src/materialized/ci/listener_configs/v26_32_0/no_auth.json" \
    --orchestrator=process \
    --orchestrator-process-secrets-directory="$root/mzdata/secrets" \
    --orchestrator-process-tcp-proxy-listen-addr=0.0.0.0 \
    --orchestrator-process-prometheus-service-discovery-directory="$root/mzdata/prometheus" \
    --orchestrator-process-scratch-directory="$root/scratch" \
    --secrets-controller=local-file \
    --persist-consensus-url="$postgres?options=--search_path=consensus" \
    --persist-blob-url="file://$root/mzdata/persist/blob" \
    --timestamp-oracle-url="$postgres?options=--search_path=tsoracle" \
    --environment-id="local-az1-$(cat /proc/sys/kernel/random/uuid)-0" \
    --bootstrap-role=materialize \
    --cluster-replica-sizes="$sizes" \
    --bootstrap-default-cluster-replica-size=bootstrap \
    "--system-parameter-default=persist_v2_register_user_shards=true" \
    >"$logs/environmentd.log" 2>&1 &
pids+=($!)
for _ in $(seq 300); do
    psql -h localhost -p 6875 -U materialize -c "SELECT 1" >/dev/null 2>&1 && break
    kill -0 "${pids[-1]}" || { echo "environmentd exited"; tail -20 "$logs/environmentd.log"; exit 1; }
    sleep 1
done

"$target/testdrive" \
    --materialize-url=postgres://materialize@localhost:6875/materialize \
    --materialize-internal-url=postgres://mz_system@localhost:6877/materialize \
    --no-reset --default-timeout=60s \
    --cluster-replica-sizes="$sizes" \
    test/persist-resolver/resolver.td

# Every user collection's shard is v2: no consensus row of its own.
shards=$(psql -h localhost -p 6875 -U materialize -At -c \
    "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%'")
[ -n "$shards" ] || { echo "no user shards"; exit 1; }
for shard in $shards; do
    rows=$(psql "$postgres" -At -c "SELECT count(*) FROM consensus.consensus WHERE shard = '$shard'")
    if [ "$rows" != 0 ]; then
        echo "FAIL: user shard $shard has $rows consensus rows: it is v1"
        exit 1
    fi
    echo "user shard $shard: v2 (no consensus rows)"
done
grep "is v2: true" "$logs/environmentd.log" | sed 's/.*INFO//' || true
echo "OK"
