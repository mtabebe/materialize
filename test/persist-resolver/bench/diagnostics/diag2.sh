#!/usr/bin/env bash
# After sweep 4: does a bigger consensus pool move classic's ceiling?
# Kept as run: from scratch/ in the worktree, so its paths are relative to there.
cd "$(dirname "$0")/../test/persist-resolver/bench" || exit
while kill -0 1774198 2>/dev/null; do sleep 10; done
D=(--duration 300 --hydration-timeout 300 --results-root ../../../scratch/diag)
P=(--system-param persist_consensus_connection_pool_max_size=200)
./bench.py --arm classic --views 400 --tick-ms 100 "${D[@]}" "${P[@]}" --name diag-classic-v400-pool200
./bench.py --arm classic --views 800 --tick-ms 100 "${D[@]}" "${P[@]}" --name diag-classic-v800-pool200
echo DIAG2-DONE
