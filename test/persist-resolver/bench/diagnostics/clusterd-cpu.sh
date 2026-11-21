#!/usr/bin/env bash
# Per-clusterd CPU (cores) and thread count every 10 s, for diagnostic runs.
while true; do
  for pid in $(pgrep -x clusterd); do
    a=$(tr '\0' ' ' < /proc/"$pid"/cmdline | grep -o 'cluster-[us][0-9]*\|--scratch-directory=[^ ]*' | head -1)
    t1=$(awk '{print $14+$15}' /proc/"$pid"/stat 2>/dev/null)
    echo "$pid $t1 $a"
  done > /tmp/cpu0.$$; sleep 10
  for pid in $(pgrep -x clusterd); do
    t2=$(awk '{print $14+$15}' /proc/"$pid"/stat 2>/dev/null); l=$(grep "^$pid " /tmp/cpu0.$$)
    [ -n "$l" ] && echo "$(date +%H:%M:%S) pid=$pid cores=$(echo "$l $t2" | awk '{printf "%.2f", ($NF-$2)/1000}') $(echo "$l" | cut -d' ' -f3)"
  done
done
