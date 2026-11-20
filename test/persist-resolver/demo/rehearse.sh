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
# PROTOTYPE: runs the whole demo in each mode from a clean start, and fails if
# a process died or logged a panic, or the transcript lacks what the demo is
# meant to show. Transcripts go to $OUT (default
# scratch/persist-resolver-demo/rehearsal), one per mode, without colors.
#
#   rehearse.sh [resolver] [classic]

set -euo pipefail

cd "$(dirname "$0")/../../.."
demo=test/persist-resolver/demo/demo.sh
state=scratch/persist-resolver-demo
out=${OUT:-$state/rehearsal}
mkdir -p "$out"
modes=("$@")
[ ${#modes[@]} -gt 0 ] || modes=(resolver classic)
trap '$demo down >/dev/null' EXIT

# Fails unless the transcript has a line matching each pattern.
expect() {
    local file=$1
    shift
    for pattern in "$@"; do
        grep -q -E -- "$pattern" "$file" || { echo "FAIL: no line matching '$pattern' in $file"; exit 1; }
    done
}

for mode in "${modes[@]}"; do
    echo "== $mode"
    start=$(date +%s)
    $demo up "$mode"
    $demo all | tee "$out/$mode.txt"
    for pid in "$state"/*.pid; do
        kill -0 "$(cat "$pid")" || { echo "FAIL: $(basename "$pid" .pid) died"; exit 1; }
    done
    if grep -a -E "panicked at|overflowed its stack|fatal runtime error" "$state"/*.log; then
        echo "FAIL: panic in $mode"
        exit 1
    fi
    # The SUBSCRIBE saw the concurrent insert.
    expect "$out/$mode.txt" $'\tcarol\t75\t1$'
    if [ "$mode" = resolver ]; then
        expect "$out/$mode.txt" \
            'user: accounts +s[0-9a-f-]+ +\(no consensus row\)' \
            'user: balances +s[0-9a-f-]+ +\(no consensus row\)' \
            'manifest: partition p0 +s[0-9a-f-]+ +rows=[0-9]+ +seqno=[0-9]+' \
            '"data_shards": 2' \
            '"reader": "c[0-9a-f-]+"' \
            "^  ${TICKS:-10} +absent +rows=[0-9]+ seqno=[0-9]+ +view_seqno=[0-9]+ upper=[0-9]+"
    else
        expect "$out/$mode.txt" \
            'user: accounts +s[0-9a-f-]+ +rows=[0-9]+ +seqno=[0-9]+' \
            'user: balances +s[0-9a-f-]+ +rows=[0-9]+ +seqno=[0-9]+' \
            "^  ${TICKS:-10} +rows=[0-9]+ seqno=[0-9]+"
    fi
    $demo down
    echo "== $mode: OK in $(($(date +%s) - start))s"
done
