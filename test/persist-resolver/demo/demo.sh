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
#   demo.sh step N                run step N (1-6) against what's up
#   demo.sh all                   run every step in order
#   demo.sh down                  stop everything

set -euo pipefail

cd "$(dirname "$0")/../../.."
root=$PWD
target=${CARGO_TARGET_DIR:-$root/target}/${PROFILE:-debug}
crdb=postgres://root@localhost:26257/materialize
rel_state=scratch/persist-resolver-demo
state=$root/$rel_state
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

# `shard kind name` lines for every shard Materialize knows a name for, plus the
# resolver's manifest shards. Kinds: user, catalog, builtin, manifest.
labels() {
    mz_quiet "SELECT s.shard_id,
            CASE WHEN s.object_id LIKE 'u%' THEN 'user' WHEN o.name = 'mz_catalog_raw' THEN 'catalog' ELSE 'builtin' END,
            o.name
        FROM mz_internal.mz_storage_shards s JOIN mz_objects o ON s.object_id = o.id"
    if [ "$(mode)" = resolver ]; then
        echo "$control_shard manifest control"
        curl -sf "$resolver_http/views" | jq -r '"\(.partition_manifest.shard) manifest partition \(.partition)"'
    fi
}

# `shard name type` lines for the user collections, by name.
user_shards() {
    mz_quiet "SELECT s.shard_id, o.name, o.type FROM mz_internal.mz_storage_shards s JOIN mz_objects o ON s.object_id = o.id WHERE s.object_id LIKE 'u%' ORDER BY o.name"
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

# A short column name for each user collection, in `user_shards` order.
short_names() {
    user_shards | awk '{ n = $2; sub(/^mv_/, "", n); if (n == $2) n = substr(n, 1, 3); print n }'
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
    local n=${VIEWS:-20}
    banner "3. Fan out: $n more materialized views over the table ($(mode))"
    create_mv() { printf "CREATE MATERIALIZED VIEW mv_%02d AS SELECT owner, sum(balance) * %d AS total FROM accounts GROUP BY owner" "$1" "$1"; }
    mz "$(create_mv 1)"
    if [ "$n" -gt 1 ]; then
        say "mv_02 to mv_$(printf %02d "$n") the same, each with its own multiplier."
        for i in $(seq 2 "$n"); do echo "$(create_mv "$i");"; done | mzpsql -q
    fi
    if [ "$(mode)" = resolver ]; then
        say "Every user collection is a data shard in the resolver's partition:"
        printf '%s$ %s%s\n' "$cyan" "curl -sf $resolver_http/views | jq -r '.data_shards[] | \"\\(.shard) \\(.upper[0]) \\(.batches | length)\"'" "$reset"
        local views
        views=$(curl -sf "$resolver_http/views" | jq -r '.data_shards[] | "\(.shard) \(.upper[0]) \(.batches | length)"')
        user_shards | awk -v views="$views" '
            BEGIN { n = split(views, v, "\n"); for (i = 1; i <= n; i++) { split(v[i], f, " "); u[f[1]] = f[2]; b[f[1]] = f[3] } }
            { if ($1 in u) printf "  %-9s %-40s upper=%s batches=%s\n", $2, $1, u[$1], b[$1];
              else printf "  %-9s %-40s (not on the resolver)\n", $2, $1 }'
    else
        say "Each one is its own persist shard, with its own consensus row and seqno:"
        consensus_by_shard "$(user_shards | awk '{ print $1, "user: " $2 }')"
    fi
}

step4() {
    banner "4. Which shards go through the resolver ($(mode))"
    say "The opt-in, part one: environmentd (and the clusterd processes it starts) get the resolver's address."
    show "tr '\\0' '\\n' </proc/$(cat "$state/environmentd.pid")/environ | grep '^MZ_PERSIST_RESOLVER_URL=' || echo '(not set)'"
    say "Part two: a system parameter turns on registration."
    printf '%smz_system=> %s%s\n' "$cyan" "SHOW persist_v2_register_user_shards" "$reset"
    PGOPTIONS='--welcome_message=off' psql -h localhost -p 6877 -U mz_system -d materialize -q -P pager=off -c "SHOW persist_v2_register_user_shards"
    say "It is per collection kind: the storage controller registers a new collection on the resolver"
    say "only if it has a user id. Catalog, builtin and txns shards open as ordinary persist shards."
    show "grep -ac 'is v2: true' $rel_state/environmentd.log || true"
    show "grep -aoE -m3 'u[0-9]+ shard s[0-9a-f-]+ is v2: [a-z]+' $rel_state/environmentd.log || echo '(none)'"

    say "Every shard, by kind, and where its state lives:"
    local labels consensus on_resolver=''
    labels=$(labels)
    consensus=$(crdb_quiet "SELECT DISTINCT shard FROM consensus.consensus")
    [ "$(mode)" != resolver ] || on_resolver=$(curl -sf "$resolver_http/views" | jq -r '.data_shards[].shard')
    awk -v labels="$labels" -v consensus="$consensus" -v on_resolver="$on_resolver" '
        function load(str, arr,   n, i, parts) { n = split(str, parts, "\n"); for (i = 1; i <= n; i++) if (parts[i] != "") arr[parts[i]] = 1 }
        BEGIN {
            n = split(labels, l, "\n"); for (i = 1; i <= n; i++) { split(l[i], f, " "); kind[f[1]] = f[2] }
            load(consensus, cons); load(on_resolver, res)
            for (s in cons) if (!(s in kind)) kind[s] = "other"
            for (s in kind) { k = kind[s]; total[k]++; if (s in cons) c[k]++; if (s in res) r[k]++ }
            printf "  %-9s %6s  %-12s %-12s %s\n", "kind", "shards", "in consensus", "on resolver", "served by"
            split("user catalog builtin other manifest", order, " ")
            for (i = 1; i <= 5; i++) {
                k = order[i]; if (!total[k]) continue
                by = (r[k] == total[k]) ? "resolver" : (c[k] == total[k]) ? "consensus" : "mixed"
                if (k == "manifest" && by == "consensus") by = "consensus (the resolver'"'"'s own state)"
                printf "  %-9s %6d  %-12d %-12d %s\n", k, total[k], c[k], r[k], by
            }
        }'
    say "(catalog: mz_catalog_raw. other: shards Materialize doesn't list by name, such as txns and the expression cache.)"
    say "The two user collections from step 1, and the resolver's manifest shards:"
    consensus_by_shard "$( (user_shards | awk '$2 == "accounts" || $2 == "balances" { print $1, "user: " $2 }'
        labels | awk '$2 == "manifest" { $2 = "manifest:"; print }') )"
    if [ "$(mode)" = resolver ]; then
        say "User collections have no consensus row: their state is rows in the manifest shards."
    else
        say "Each user collection is a persist shard: its own consensus row, its own seqno."
    fi
}

step5() {
    if [ "$(mode)" != resolver ]; then
        banner "5. Where a user collection's state lives ($(mode))"
        say "Classic persist has no resolver. A shard's state is a versioned blob in consensus:"
        local shard
        shard=$(user_shards | awk '$2 == "accounts" { print $1 }')
        crdb "SELECT shard, sequence_number, length(data) AS state_bytes FROM consensus.consensus WHERE shard = '$shard' ORDER BY sequence_number DESC LIMIT 3"
        return
    fi
    banner "5. Ask the resolver for the partition's view ($(mode))"
    show "curl -sf $resolver_http/views | jq '{partition, seqno, control_manifest, partition_manifest, claim, data_shards: (.data_shards | length)}'"
    local shard
    shard=$(user_shards | awk '$2 == "accounts" { print $1 }')
    say "The 'accounts' table is data shard $shard:"
    show "curl -sf $resolver_http/views/$shard"
}

# Prints the column header for a per-shard grid, one 4-wide column per user
# collection.
grid_header() {
    printf '  %-20s' "$1"
    short_names | awk '{ printf "%4s", $1 } END { printf "  %s\n", "'"$2"'" }'
}

step6() {
    local n=${TICKS:-10}
    banner "6. Watch the uppers while writing, $n inserts one second apart ($(mode))"
    local order
    order=$(user_shards | awk '{ print $1 }' | paste -sd, -)
    (for i in $(seq "$n"); do mzpsql -q -c "INSERT INTO accounts VALUES ($((100 + i)), 'dave', $i)"; sleep 1; done) &
    if [ "$(mode)" = resolver ]; then
        say "One line per resolver write (view seqno). '+': that shard's upper moved in this write."
        grid_header "view_seqno  upper" "moved, at max upper"
        local end=$((SECONDS + n))
        while [ "$SECONDS" -lt "$end" ]; do
            curl -sf "$resolver_http/views" | jq -r --arg order "$order" '
                (.data_shards | map({key: .shard, value: (.upper[0] // -1 | tostring)}) | from_entries) as $u
                | "\(.seqno) " + ([$order | split(",")[] | $u[.] // "-"] | join(" "))'
            sleep 0.05
        done | awk '
            $1 == seqno { next }
            {
                max = 0; for (i = 2; i <= NF; i++) if ($i + 0 > max) max = $i + 0
                line = ""; moved = 0; at = 0
                for (i = 2; i <= NF; i++) {
                    m = (seqno == "") ? "" : ($i != prev[i]) ? "+" : "."
                    moved += (m == "+"); at += ($i + 0 == max); line = line sprintf("%4s", m); prev[i] = $i
                }
                d = (seqno == "") ? "" : sprintf("(+%d)", $1 - seqno)
                printf "  %-5s %-5s %-13s%s  %2d, %d/%d\n", $1, d, max, line, moved, at, NF - 1; fflush()
                seqno = $1
            }'
    else
        say "Once a second, each user shard's seqno in consensus:"
        grid_header "tick" "moved"
        local shards prev=''
        shards=$(user_shards | awk '{ printf "%s'"'"'%s'"'"'", (NR > 1 ? "," : ""), $1 }')
        for i in $(seq "$n"); do
            local seqnos
            seqnos=$(crdb_quiet "SELECT shard, max(sequence_number) FROM consensus.consensus WHERE shard IN ($shards) GROUP BY shard" |
                awk -v order="$order" 'BEGIN { split(order, o, ",") } { s[$1] = $2 } END { for (i = 1; i in o; i++) printf "%s ", (o[i] in s) ? s[o[i]] : "-" }')
            awk -v tick="$i" -v cur="$seqnos" -v prev="$prev" 'BEGIN {
                n = split(cur, c, " "); split(prev, p, " "); moved = 0; line = ""
                for (i = 1; i <= n; i++) { line = line sprintf("%4s", c[i]); moved += (prev != "" && c[i] != p[i]) }
                printf "  %-20s%s  %s\n", tick, line, (prev == "" ? "" : moved)
            }'
            prev=$seqnos
            sleep 1
        done
    fi
    wait
    local shard
    shard=$(user_shards | awk '$2 == "accounts" { print $1 }')
    consensus_by_shard "$shard user: accounts"
}

case "${1:-}" in
up) up "${2:-}" ;;
down) down; echo "down." ;;
step)
    need_up
    case "${2:-}" in 1 | 2 | 3 | 4 | 5 | 6) "step$2" ;; *) echo "usage: demo.sh step 1-6" >&2; exit 2 ;; esac
    ;;
all) need_up; for i in 1 2 3 4 5 6; do "step$i"; done ;;
*) sed -n '/^#   demo.sh/s/^# *//p' "$0" >&2; exit 2 ;;
esac
