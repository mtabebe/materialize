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
# a process died or logged a panic. Transcripts go to $OUT (default
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
    $demo down
    echo "== $mode: OK in $(($(date +%s) - start))s"
done
