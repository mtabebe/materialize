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
# PROTOTYPE: runs bench.py over a list of points, one at a time, cycling
# through the arms at each point so drift on the box lands on all of them.
#
# Usage: [ARMS="classic resolver resolver-grouped"] sweep.sh TICK_MS VIEWS... [-- extra bench.py args]
#   e.g. sweep.sh 100 50 100 200 400 -- --duration 600

set -uo pipefail

cd "$(dirname "$0")" || exit
tick=$1
shift
views=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do views+=("$1"); shift; done
[ "${1:-}" = "--" ] && shift

for v in "${views[@]}"; do
    for arm in ${ARMS:-classic resolver resolver-grouped}; do
        ./bench.py --arm "$arm" --views "$v" --tick-ms "$tick" "$@" ||
            echo "bench.py failed: $arm $v $tick"
    done
done
