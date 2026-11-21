#!/usr/bin/env bash
# Disk write bytes/s per Materialize process kind and the CRDB container, over $1 s.
s=${1:-20}
snap() {
  for e in persist-resolver environmentd clusterd; do
    t=0; for p in $(pgrep -f "target/release/$e" ); do v=$(sudo awk '/^write_bytes/{print $2}' /proc/$p/io 2>/dev/null); t=$((t+${v:-0})); done; echo "$e $t"
  done
  cg=/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' persist-resolver-bench-crdb).scope/io.stat
  echo "crdb $(awk '{for(i=2;i<=NF;i++) if($i~/^wbytes=/){split($i,a,"=");t+=a[2]}} END{print t+0}' $cg)"
  echo "crdb_wios $(awk '{for(i=2;i<=NF;i++) if($i~/^wios=/){split($i,a,"=");t+=a[2]}} END{print t+0}' $cg)"
}
a=$(snap); sleep "$s"; b=$(snap)
paste <(echo "$a") <(echo "$b") | awk -v s="$s" '{printf "%-18s %8.1f %s/s\n", $1, ($4-$2)/s/($1=="crdb_wios"?1:1048576), ($1=="crdb_wios"?"ios":"MB")}'
