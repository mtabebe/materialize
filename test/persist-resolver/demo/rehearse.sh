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
# PROTOTYPE: runs the runbook in README.md for each mode from a clean start,
# executing its `<!-- run: NAME -->` blocks verbatim, and fails if a block
# fails, a process dies or logs a panic, or the transcript lacks what the demo
# is meant to show. Transcripts go to $OUT (default
# scratch/persist-resolver-demo/rehearsal), one per mode.
#
#   rehearse.sh [resolver] [classic]

set -euo pipefail

cd "$(dirname "$0")/../../.."
dir=test/persist-resolver/demo
readme=$dir/README.md
state=scratch/persist-resolver-demo
out=${OUT:-$state/rehearsal}
mkdir -p "$out"
if [ $# -gt 0 ]; then modes=("$@"); else modes=(resolver classic); fi
command -v timeout >/dev/null || { echo "needs timeout (macOS: brew install coreutils)"; exit 1; }
target=${CARGO_TARGET_DIR:-$PWD/target}/${PROFILE:-debug}
for bin in environmentd clusterd persist-resolver; do
    [ -x "$target/$bin" ] || { echo "$target/$bin isn't built: cargo build --bin $bin"; exit 1; }
done
transcript=''
on_exit() {
    local rc=$?
    if [ "$rc" != 0 ] && [ -n "$transcript" ]; then
        echo "FAIL: exit $rc, end of $transcript:"
        tail -30 "$transcript"
    fi
    "$dir/demo.sh" down >/dev/null
}
trap on_exit EXIT

# The body of the README block marked `<!-- run: $1 -->`, unindented. In
# classic mode a `classic-$1` block, if there is one, comes first.
prefix=''
block() {
    if [ -n "$prefix" ] && grep -q -- "<!-- run: $prefix$1 -->" "$readme"; then
        set -- "$prefix$1"
    fi
    local body
    body=$(awk -v name="$1" '
        { line = $0; sub(/^[ \t]+/, "", line) }
        line == "<!-- run: " name " -->" { want = 1; next }
        want && !inside && line ~ /^```/ { inside = 1; indent = index($0, "`") - 1; next }
        inside && line ~ /^```/ { exit }
        inside { print substr($0, indent + 1) }' "$readme")
    [ -n "$body" ] || { echo "FAIL: no block '$1' in $readme" >&2; exit 1; }
    printf '%s\n' "$body"
}

# Prints a block the way it appears on screen, then runs it.
run() {
    local name=$1
    shift
    printf '\n$ # %s\n%s\n' "$name" "$(block "$name")"
    "$@"
}
sh_block() { bash -o pipefail -c "$(block "$1")"; }
shard_block() { bash -e -o pipefail -c "$(block shard-id) >/dev/null; [ -n \"\$S\" ]; $(block "$1")"; }
# Runs SQL block $2 through the client the shell block $1 starts.
sql_block() { bash -c "$(block "$1") -v ON_ERROR_STOP=1 -f -" <<<"$(block "$2")"; }

# Waits until `$@` succeeds while background job `$pid` lives.
wait_for() {
    local what=$1 secs=$2 pid=$3
    shift 3
    local deadline=$((SECONDS + secs))
    while [ "$SECONDS" -lt "$deadline" ]; do
        "$@" && return 0
        kill -0 "$pid" 2>/dev/null || { echo "FAIL: $what exited"; tail -20 "$state"/*.log "$out"/*.out 2>/dev/null || true; exit 1; }
        sleep 1
    done
    echo "FAIL: $what not ready after ${secs}s"
    tail -20 "$state"/*.log "$out"/*.out 2>/dev/null || true
    exit 1
}
claimed() { grep -q "claimed partition" "$out/resolver.out"; }
mz_ready() {
    [ "$(PGCONNECT_TIMEOUT=5 PGOPTIONS='--welcome_message=off' psql -h localhost -p 6875 -U materialize -At -c \
        "SELECT count(*) FROM mz_internal.mz_cluster_replica_statuses s JOIN mz_cluster_replicas r ON s.replica_id = r.id JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'quickstart' AND s.status = 'online'" 2>/dev/null)" = 1 ]
}

expect() {
    local file=$1
    shift
    for pattern in "$@"; do
        grep -q -E -- "$pattern" "$file" || { echo "FAIL: no line matching '$pattern' in $file"; exit 1; }
    done
}

rehearse() {
    local mode=$1
    prefix=''
    [ "$mode" = resolver ] || prefix=classic-
    run reset sh_block reset
    if [ "$mode" = resolver ]; then
        run resolver true
        # Empty it first: the background shell truncates it only once it runs, so
        # `claimed` could otherwise match the previous run's log.
        : >"$out/resolver.out"
        bash -c "$(block resolver)" >"$out/resolver.out" 2>&1 &
        wait_for "resolver claim" 60 $! claimed
        grep -a "claimed partition" "$out/resolver.out"
        run environmentd-resolver true
        bash -c "$(block environmentd-resolver)" >"$out/environmentd.out" 2>&1 &
        env_pid=$!
    else
        run environmentd-classic true
        bash -c "$(block environmentd-classic)" >"$out/environmentd.out" 2>&1 &
        env_pid=$!
    fi
    wait_for environmentd 300 "$env_pid" mz_ready
    run psql true
    run mz-create sql_block psql mz-create
    run mz-subscribe true
    (sleep 2 && run insert-carol sh_block insert-carol) &
    local insert=$!
    timeout -k 5 -s INT 6 bash -c "$(block psql) -f -" <<<"$(block mz-subscribe)" 2>&1 |
        grep -v -e '^Cancel request sent' -e 'canceling statement due to user request' || true
    wait "$insert"
    run mz-fanout sql_block psql mz-fanout
    run labels sh_block labels
    run crdb true
    run crdb-kinds sql_block crdb crdb-kinds
    run crdb-user-shards sql_block crdb crdb-user-shards
    if [ "$mode" = resolver ]; then
        run views-groups sh_block views-groups
        run views-watch true
        timeout -k 5 -s INT 6 bash -c "$(block views-watch)" || [ $? = 124 ]
        run write-rate sh_block write-rate
        run snapshot-before sh_block snapshot-before
        run shard-id sh_block shard-id
        # `$S` from shard-id doesn't outlive its shell, so it runs again first.
        run shard-logical shard_block shard-logical
        run shard-rows shard_block shard-rows
        run shard-crdb shard_block shard-crdb
        run mz-insert sql_block psql mz-insert
        sleep 2
        run shard-logical-again shard_block shard-logical-again
        run shard-rows-again shard_block shard-rows-again
        run mz-writes sql_block psql mz-writes
        # Let the writes' ticks land in every MV.
        sleep 2
        run snapshot-after sh_block snapshot-after
        run batch-diff sh_block batch-diff
        run crdb-again true
        run crdb-kinds-again sql_block crdb-again crdb-kinds-again
        run mz-fanout-more sql_block psql mz-fanout-more
        sleep ${SETTLE_SECS:-30}
        run write-rate-more sh_block write-rate-more
    else
        run crdb-watch true
        timeout -k 5 -s INT 4 bash -c "$(block crdb) -f -" <<<"$(block crdb-watch)" || [ $? = 124 ]
        run mz-insert sql_block psql mz-insert
        run mz-writes sql_block psql mz-writes
        sleep 2
        run crdb-user-shards-again sql_block crdb crdb-user-shards-again
    fi
    for p in "$out/resolver.out" "$state/environmentd.log"; do
        [ "$mode" = resolver ] || [ "$p" != "$out/resolver.out" ] || continue
        if grep -a -E "panicked at|overflowed its stack|fatal runtime error" "$p"; then
            echo "FAIL: panic in $p"
            exit 1
        fi
    done
    local expected=1
    [ "$mode" = classic ] || expected=2
    [ "$(jobs -rp | wc -l)" -eq "$expected" ] || { echo "FAIL: a process exited early"; jobs -l; exit 1; }
    run down sh_block down
    prefix=''
}

for mode in "${modes[@]}"; do
    echo "== $mode"
    start=$(date +%s)
    t=$out/$mode.txt
    transcript=$t
    # Not `rehearse ... || ...`: that would turn off errexit inside it.
    PGOPTIONS='--welcome_message=off' rehearse "$mode" >"$t" 2>&1
    transcript=''
    expect "$t" $'\tcarol\t75\t1$' 'labels: [0-9]+ builtin, 1 catalog, [0-9]+ other' ' mv_20 ' \
        '^ dave +\| +40 +\| +1$' '^ bob +\| +60 +\| +1$' '^ dave +\| +800$' '^ bob +\| +1200$'
    if grep -qE '^ carol +\|' "$t"; then echo "FAIL: carol is still in an MV after the DELETE"; exit 1; fi
    if [ "$mode" = resolver ]; then
        expect "$t" 'claimed partition p0' \
            '^ user +\| +22 \| +0 \| +0$' \
            '^ builtin +\| +([0-9]+) \| +\1 \| +[0-9]+$' \
            '^ resolver-manifest +\| +2 \| +2 \|' \
            '^ mv_20 +\| +0 \| *$' \
            '"members": \[' \
            '^\{"seqno":[0-9]+,"groups":\[\{"group":[0-9]+,"upper":[0-9]+,"members":21\}\],"own_upper_rows":1\}$'
        # After the writes, the MVs are in the group they were in before, and
        # every user shard's batches or updates changed.
        diff_lines=$(grep -E '^[a-z_0-9]+ +group [^ ]+ -> ' "$t")
        [ "$(grep -cE '^(balances|mv_[0-9]+) +group ([0-9]+) -> \2 ' <<<"$diff_lines")" = 21 ] ||
            { echo "FAIL: an MV changed group across the writes"; exit 1; }
        expect "$t" '^accounts +group own -> own '
        changed=$(awk '{ if ($7 != $9 || $11 != $13) n++ } END { print n + 0 }' <<<"$diff_lines")
        [ "$changed" = 22 ] || { echo "FAIL: only $changed of 22 user shards changed batches or updates"; exit 1; }
        [ "$(grep -cE '^ user +\| +22 \| +0 \| +0$' "$t")" = 2 ] ||
            { echo "FAIL: user shards in consensus after the writes"; exit 1; }
        # mv_01 up close: a member of a group before and after the INSERT, with
        # one more Batch row after, and its logical view one more batch.
        rows=$(awk '/^\$ # (classic-)?shard-rows(-again)?$/ { n++ } n && /^\{"manifest"/ { print n, $0 }' "$t")
        before=$(grep -c '^1 .*"key":"Batch"' <<<"$rows")
        after=$(grep -c '^2 .*"key":"Batch"' <<<"$rows")
        [ "$after" = $((before + 1)) ] || { echo "FAIL: mv_01 Batch rows $before -> $after"; exit 1; }
        [ "$(grep -c '"key":"Member","val":[0-9]' <<<"$rows")" = 2 ] || { echo "FAIL: mv_01 not a member before and after"; exit 1; }
        [ "$(grep -c '"key":"Group([0-9]*)","val":\[[0-9]' <<<"$rows")" = 2 ] || { echo "FAIL: mv_01's group row missing"; exit 1; }
        if grep -q '"key":"Upper"' <<<"$rows"; then echo "FAIL: mv_01 has its own Upper row"; exit 1; fi
        expect "$t" '"upper_group": [0-9]+' '"lower": [0-9]+'
        # Twice the MVs: the group doubles, the resolver's write rate doesn't.
        rate() { grep -E "^group members: $1, resolver writes in 30 s: [0-9]+$" "$t" | grep -oE '[0-9]+$'; }
        r21=$(rate 21) r41=$(rate 41)
        [ -n "$r21" ] && [ -n "$r41" ] || { echo "FAIL: write-rate lines missing (21: '$r21', 41: '$r41')"; exit 1; }
        # Bursts make one window noisy, so this only catches writes doubling with the MVs.
        [ "$r41" -lt $((r21 * 2)) ] || { echo "FAIL: resolver writes grew from $r21 to $r41 with twice the MVs"; exit 1; }
        echo "write rate: 21 members $r21, 41 members $r41 (per 30 s)"
        # mv_01 has no consensus rows; the partition manifest shard does.
        crdb_rows=$(awk '/^\$ # shard-crdb$/ { on = 1 } on && /^\([0-9]+ rows?\)$/ { print; if (++n == 2) exit }' "$t")
        [ "$(head -1 <<<"$crdb_rows")" = "(0 rows)" ] || { echo "FAIL: mv_01 has consensus rows"; exit 1; }
        [ "$(tail -1 <<<"$crdb_rows")" = "(1 row)" ] || { echo "FAIL: no consensus rows for the partition manifest"; exit 1; }
        # The one group's upper moved: at least 3 distinct uppers over the watch.
        local_uppers=$(grep -E '"members":21' "$t" | grep -oE '"upper":[0-9]+' | sort -u | wc -l)
        [ "$local_uppers" -ge 3 ] || { echo "FAIL: the group's upper took $local_uppers values"; exit 1; }
    else
        expect "$t" '^ user +\| +22 \| +22 \| +[0-9]+$' \
            '^ mv_20 +\| +[0-9]+ \| +[0-9]+$' \
            '^ [0-9:]{8} \| [0-9]+( [0-9]+){21}$'
        # Every user shard has its own consensus seqno, and each one grew between
        # the two crdb-user-shards outputs (writes and ticks both count).
        grew=$(awk -F '|' '/^ (accounts|balances|mv_[0-9]+) +\| +[0-9]+ \| +[0-9]+$/ {
                n = $1; gsub(/ /, "", n); if (n in s && $3 + 0 > s[n]) g++; s[n] = $3 + 0 }
            END { print g + 0 }' "$t")
        [ "$grew" = 22 ] || { echo "FAIL: only $grew of 22 user shards' seqnos grew across the writes"; exit 1; }
        # The seqnos climbed during the watch.
        lines=$(grep -E '^ [0-9:]{8} \| [0-9]+( [0-9]+){21}$' "$t" | cut -d'|' -f2 | sort -u | wc -l)
        [ "$lines" -ge 2 ] || { echo "FAIL: user shard seqnos didn't move during the watch"; exit 1; }
        if grep "^labels:" "$t" | grep -q resolver-manifest; then echo "FAIL: classic run has resolver manifests"; exit 1; fi
    fi
    echo "== $mode: OK in $(($(date +%s) - start))s, transcript $t"
done
