#!/usr/bin/env bash
# After diag2: unreported resolver runs with a 60 s flat CPU profile (bpftrace,
# 49 Hz, leaf symbol) of the resolver, taken 60 s into the measurement window.
# Kept as run: from scratch/ in the worktree, so its paths are relative to there.
cd "$(dirname "$0")/../test/persist-resolver/bench" || exit
until grep -q DIAG2-DONE ../../../scratch/diag2.log 2>/dev/null; do sleep 10; done
D=(--duration 300 --hydration-timeout 300 --results-root ../../../scratch/diag)
prof() {
    local name=$1; shift
    ./bench.py "$@" "${D[@]}" --name "$name" > "../../../scratch/diag-$name.out" 2>&1 &
    local b=$!
    until grep -q "measurement window starts" "../../../scratch/diag-$name.out" || ! kill -0 $b 2>/dev/null; do sleep 5; done
    sleep 60
    local p; p=$(pgrep -x persist-resolve | head -1)
    # The profile is written as this user; only bpftrace needs root.
    # shellcheck disable=SC2024
    sudo timeout 90 bpftrace -e "profile:hz:49 /pid == $p/ { @fn[usym(reg(\"ip\"))] = count(); @thread[comm] = count(); } interval:s:60 { exit(); }" \
        > "../../../scratch/diag/profile-$name.txt" 2>&1
    wait $b
}
prof diag-resolver-v1600-t1000 --arm resolver --views 1600 --tick-ms 1000
prof diag-resolver-v800-t100 --arm resolver --views 800 --tick-ms 100
echo DIAG3-DONE
