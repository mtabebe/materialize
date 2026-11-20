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
# PROTOTYPE: a presenter-driven demo of the persist resolver, against classic
# persist. See README.md in this directory for the runbook.
#
#   demo.sh up resolver|classic   wipe everything and start fresh
#   demo.sh step N                run step N (1-5) against what's up
#   demo.sh all                   run every step in order
#   demo.sh down                  stop everything

set -euo pipefail

cd "$(dirname "$0")/../../.."
root=$PWD
target=${CARGO_TARGET_DIR:-$root/target}/${PROFILE:-debug}
crdb=postgres://root@localhost:26257/materialize
state=$root/scratch/persist-resolver-demo
resolver_http=127.0.0.1:6891
control_shard=s00000000-0000-0000-0000-00000000c0de

bold=$'\e[1m' dim=$'\e[2m' cyan=$'\e[36m' reset=$'\e[0m'
[ -t 1 ] || bold='' dim='' cyan='' reset=''

mode() { cat "$state/mode" 2>/dev/null || echo none; }

banner() { printf '\n%s== %s%s\n' "$bold" "$*" "$reset"; }
say() { printf '%s# %s%s\n' "$dim" "$*" "$reset"; }

mzpsql() { PGOPTIONS='--welcome_message=off' psql -h localhost -p 6875 -U materialize "$@"; }

# Runs SQL against Materialize, printing it first.
mz() {
    printf '%smaterialize=> %s%s\n' "$cyan" "$1" "$reset"
    mzpsql -q -P pager=off -c "$1"
}
mz_quiet() { mzpsql -At -F ' ' -c "$1"; }

# Runs SQL against the consensus database, printing it first.
crdb() {
    printf '%sconsensus=> %s%s\n' "$cyan" "$1" "$reset"
    psql "$crdb" -q -P pager=off -c "$1"
}
crdb_quiet() { psql "$crdb" -At -F ' ' -c "$1"; }

# Runs a shell command line, printing it first.
show() {
    printf '%s$ %s%s\n' "$cyan" "$1" "$reset"
    eval "$1"
}

down() {
    for f in "$state"/*.pid; do
        [ -e "$f" ] || continue
        # Each process was started in its own process group, so this also stops
        # the clusterd processes environmentd spawned.
        kill -- -"$(cat "$f")" 2>/dev/null || true
    done
    for f in "$state"/*.pid; do
        [ -e "$f" ] || continue
        local pgid
        pgid=$(cat "$f")
        for _ in $(seq 50); do
            kill -0 -- -"$pgid" 2>/dev/null || break
            sleep 0.2
        done
        kill -KILL -- -"$pgid" 2>/dev/null || true
        sleep 0.2
        if kill -0 -- -"$pgid" 2>/dev/null; then
            echo "process group $pgid ($(basename "$f" .pid)) survived SIGKILL" >&2
            exit 1
        fi
        rm -f "$f"
    done
    rm -f "$state/mode"
}

start() {
    local name=$1
    shift
    setsid "$@" >"$state/$name.log" 2>&1 </dev/null &
    echo $! >"$state/$name.pid"
}

up() {
    local mode=$1
    case "$mode" in resolver | classic) ;; *) echo "usage: demo.sh up resolver|classic" >&2; exit 2 ;; esac
    down
    mkdir -p "$state"
    rm -f "$state"/*.log
    rm -rf "$root/mzdata"
    psql "$crdb" -q -c "CREATE DATABASE IF NOT EXISTS materialize" >/dev/null
    for schema in consensus tsoracle storage; do
        psql "$crdb" -q -c "DROP SCHEMA IF EXISTS $schema CASCADE" -c "CREATE SCHEMA $schema" >/dev/null
    done
    mkdir -p "$root/mzdata/persist/blob" "$root/mzdata/secrets" "$root/mzdata/prometheus" "$root/scratch"
    local blob="file://$root/mzdata/persist/blob"
    local consensus="$crdb?options=--search_path=consensus"

    local env=() params=()
    if [ "$mode" = resolver ]; then
        start resolver "$target/persist-resolver" \
            --blob-url "$blob" --consensus-url "$consensus" \
            --listen-addr 127.0.0.1:6890 --http-listen-addr "$resolver_http"
        for _ in $(seq 300); do
            grep -q "claimed partition" "$state/resolver.log" && break
            kill -0 "$(cat "$state/resolver.pid")" || { tail -20 "$state/resolver.log"; exit 1; }
            sleep 0.2
        done
        grep -q "claimed partition" "$state/resolver.log" || { echo "resolver didn't claim its partition"; exit 1; }
        env=(MZ_PERSIST_RESOLVER_URL=http://127.0.0.1:6890)
        params=("--system-parameter-default=persist_v2_register_user_shards=true")
    fi

    # The arguments `bin/environmentd` passes, minus its Python wrapper.
    local size='{"cpu_exclusive":false,"cpu_limit":null,"credits_per_hour":"1","disabled":false,"disk_limit":null,"is_cc":true,"memory_limit":"4 GiB","scale":1,"workers":1}'
    local sizes="{\"bootstrap\":$size,\"scale=1,workers=1\":$size}"
    start environmentd env "${env[@]}" RUST_MIN_STACK=16777216 \
        "$target/environmentd" \
        --listeners-config-path="$root/src/materialized/ci/listener_configs/v26_32_0/no_auth.json" \
        --orchestrator=process \
        --orchestrator-process-secrets-directory="$root/mzdata/secrets" \
        --orchestrator-process-tcp-proxy-listen-addr=0.0.0.0 \
        --orchestrator-process-prometheus-service-discovery-directory="$root/mzdata/prometheus" \
        --orchestrator-process-scratch-directory="$root/scratch" \
        --secrets-controller=local-file \
        --persist-consensus-url="$consensus" \
        --persist-blob-url="$blob" \
        --timestamp-oracle-url="$crdb?options=--search_path=tsoracle" \
        --environment-id="local-az1-$(cat /proc/sys/kernel/random/uuid)-0" \
        --bootstrap-role=materialize \
        --cluster-replica-sizes="$sizes" \
        --bootstrap-default-cluster-replica-size=bootstrap \
        "${params[@]}"
    # SQL, then the default cluster's replica, which comes up after the SQL port.
    local ready=0
    for _ in $(seq 300); do
        kill -0 "$(cat "$state/environmentd.pid")" || { echo "environmentd exited"; tail -20 "$state/environmentd.log"; exit 1; }
        if [ "$(PGCONNECT_TIMEOUT=5 mz_quiet "SELECT count(*) FROM mz_internal.mz_cluster_replica_statuses s JOIN mz_cluster_replicas r ON s.replica_id = r.id JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'quickstart' AND s.status = 'online'" 2>/dev/null)" = 1 ]; then
            ready=1
            break
        fi
        sleep 1
    done
    [ "$ready" = 1 ] || { echo "environmentd not ready after 300 s"; tail -20 "$state/environmentd.log"; exit 1; }
    echo "$mode" >"$state/mode"
    echo "up: $mode. Logs in $state."
}

need_up() {
    [ "$(mode)" != none ] || { echo "nothing is up: run demo.sh up resolver|classic" >&2; exit 1; }
}

# `shard label` lines for every shard Materialize knows a name for, plus the
# resolver's manifest shards.
labels() {
    mz_quiet "SELECT s.shard_id, CASE WHEN s.object_id LIKE 'u%' THEN 'user: ' ELSE 'builtin: ' END || o.name
        FROM mz_internal.mz_storage_shards s JOIN mz_objects o ON s.object_id = o.id"
    if [ "$(mode)" = resolver ]; then
        echo "$control_shard manifest: control"
        curl -sf "$resolver_http/views" | jq -r '"\(.partition_manifest.shard) manifest: partition \(.partition)"'
    fi
}

user_shards() {
    mz_quiet "SELECT s.shard_id, o.name FROM mz_internal.mz_storage_shards s JOIN mz_objects o ON s.object_id = o.id WHERE s.object_id LIKE 'u%' ORDER BY o.name"
}

# Consensus rows and the max seqno of each shard in `$1` (`shard label` lines).
consensus_by_shard() {
    local rows
    rows=$(crdb_quiet "SELECT shard, count(*), max(sequence_number) FROM consensus.consensus GROUP BY shard")
    awk -v rows="$rows" '
        BEGIN { n = split(rows, r, "\n"); for (i = 1; i <= n; i++) { split(r[i], f, " "); c[f[1]] = f[2]; s[f[1]] = f[3] } }
        { shard = $1; $1 = ""; label = substr($0, 2);
          if (shard in c) printf "  %-26s %-40s rows=%-3s seqno=%s\n", label, shard, c[shard], s[shard];
          else printf "  %-26s %-40s (no consensus row)\n", label, shard }' <<<"$1"
}

step1() {
    banner "1. Create a table and a materialized view, and write to them ($(mode))"
    mz "CREATE TABLE accounts (id int, owner text, balance int)"
    mz "CREATE MATERIALIZED VIEW balances AS SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner"
    mz "INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25)"
}

step2() {
    banner "2. Read it back: SELECT, and a SUBSCRIBE that sees a new write ($(mode))"
    mz "SELECT * FROM balances ORDER BY owner"
    local secs=${SUBSCRIBE_SECS:-6}
    say "SUBSCRIBE for ${secs}s in one session; another session inserts a row two seconds in."
    (sleep 2 && mzpsql -q -c "INSERT INTO accounts VALUES (4, 'carol', 75)") &
    # NOTE: not `DECLARE ... CURSOR FOR SUBSCRIBE` + `FETCH`: in a dev build,
    # that FETCH overflowed environmentd's 3 MiB tokio worker stack and aborted
    # it (see README.md). psql cancels the COPY on SIGINT.
    printf '%smaterialize=> %s%s\n' "$cyan" "COPY (SUBSCRIBE balances) TO STDOUT" "$reset"
    timeout -k 5 -s INT "$secs" bash -c "$(declare -f mzpsql); mzpsql -c 'COPY (SUBSCRIBE balances) TO STDOUT'" \
        2> >(grep -v -e '^Cancel request sent' -e 'canceling statement due to user request' >&2) || true
    wait
}

step3() {
    banner "3. What's in consensus ($(mode))"
    crdb "SELECT count(DISTINCT shard) AS shards, count(*) AS rows FROM consensus.consensus"
    say "Every shard with a consensus row, by kind:"
    local labels
    labels=$(labels)
    crdb_quiet "SELECT DISTINCT shard FROM consensus.consensus" | awk -v labels="$labels" '
        BEGIN { n = split(labels, l, "\n"); for (i = 1; i <= n; i++) { split(l[i], f, " "); k[f[1]] = f[2] } }
        { kind = ($1 in k) ? k[$1] : "other:"; sub(":", "", kind); count[kind]++ }
        END { for (kind in count) printf "  %-9s %d\n", kind, count[kind] }' | sort
    say "('other': shards Materialize doesn't list by name, such as the catalog and txns shards.)"
    say "The user collections, and the resolver's manifest shards:"
    consensus_by_shard "$( (user_shards | sed 's/ / user: /'; labels | grep ' manifest:') )"
    if [ "$(mode)" = resolver ]; then
        say "User collections have no consensus row: their state is rows in the manifest shards."
    else
        say "Each user collection is a persist shard: its own consensus row, its own seqno."
    fi
}

step4() {
    if [ "$(mode)" != resolver ]; then
        banner "4. Where a user collection's state lives ($(mode))"
        say "Classic persist has no resolver. A shard's state is a versioned blob in consensus:"
        local shard
        shard=$(user_shards | awk '$2 == "accounts" { print $1 }')
        crdb "SELECT shard, sequence_number, length(data) AS state_bytes FROM consensus.consensus WHERE shard = '$shard' ORDER BY sequence_number DESC LIMIT 3"
        return
    fi
    banner "4. Ask the resolver for the partition's view ($(mode))"
    show "curl -sf $resolver_http/views | jq '{partition, seqno, control_manifest, partition_manifest, claim, data_shards: (.data_shards | length)}'"
    local shard
    shard=$(user_shards | awk '$2 == "accounts" { print $1 }')
    say "The 'accounts' table is data shard $shard:"
    show "curl -sf $resolver_http/views/$shard"
}

step5() {
    local n=${TICKS:-10}
    banner "5. Watch consensus while writing, $n inserts one second apart ($(mode))"
    local shard
    shard=$(user_shards | awk '$2 == "accounts" { print $1 }')
    local partition_shard=''
    if [ "$(mode)" = resolver ]; then
        partition_shard=$(curl -sf "$resolver_http/views" | jq -r .partition_manifest.shard)
        printf '  %-4s %-28s %-28s %-28s\n' tick "consensus: accounts" "consensus: partition manifest" "resolver: accounts"
    else
        printf '  %-4s %-28s\n' tick "consensus: accounts"
    fi
    for i in $(seq "$n"); do
        mzpsql -q -c "INSERT INTO accounts VALUES ($((100 + i)), 'dave', $i)"
        local acct part view
        acct=$(crdb_quiet "SELECT count(*), coalesce(max(sequence_number)::text, '-') FROM consensus.consensus WHERE shard = '$shard'" |
            awk '$1 == 0 { print "absent"; next } { print "rows=" $1 " seqno=" $2 }')
        if [ "$(mode)" = resolver ]; then
            part=$(crdb_quiet "SELECT count(*), max(sequence_number) FROM consensus.consensus WHERE shard = '$partition_shard'" |
                awk '{ print "rows=" $1 " seqno=" $2 }')
            view=$(curl -sf "$resolver_http/views" | jq -r --arg s "$shard" '"view_seqno=\(.seqno) upper=\(.data_shards[] | select(.shard == $s) | .upper[0])"')
            printf '  %-4s %-28s %-28s %-28s\n' "$i" "$acct" "$part" "$view"
        else
            printf '  %-4s %-28s\n' "$i" "$acct"
        fi
        sleep 1
    done
}

case "${1:-}" in
up) up "${2:-}" ;;
down) down; echo "down." ;;
step)
    need_up
    case "${2:-}" in 1 | 2 | 3 | 4 | 5) "step$2" ;; *) echo "usage: demo.sh step 1-5" >&2; exit 2 ;; esac
    ;;
all) need_up; for i in 1 2 3 4 5; do "step$i"; done ;;
*) sed -n '/^#   demo.sh/s/^# *//p' "$0" >&2; exit 2 ;;
esac
