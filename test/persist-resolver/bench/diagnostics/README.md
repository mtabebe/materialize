# Diagnostics (not in the report)

Unreported runs that name each arm's limiter (benchmark checklist Q1/Q2). Same harness and binaries as
sweep 2, 300 s windows. Each `diag-*` dir is a bench.py results dir.

| run | what it tests |
|---|---|
| `diag-classic-v400-c4` | classic, 400 views x 10 Hz, as in the sweep, with per-clusterd CPU sampled (`clusterd-cpu.sh`) |
| `diag-classic-v400-c8` | the same with 8 view clusters (32 workers): is compute the limiter? |
| `diag-classic-v400-crdb8` | the same with CRDB on 8 cores: is CRDB CPU the limiter? |
| `diag-classic-v400-pool200`, `diag-classic-v800-pool200` | `persist_consensus_connection_pool_max_size=200` (default 50): is the pool? |
| `diag-classic-v400-crdb-tmpfs` | the same with CRDB's store on tmpfs (`--crdb-tmpfs 40g`): is the disk? (run after codex's review asked for an intervention) |
| `diag-resolver-v1600-t1000`, `diag-resolver-v800-t100` | resolver arm with a 60 s bpftrace profile (`profile-*.txt`, `diag3.sh`) |

Spot observations taken by hand during those runs (not stored in the results dirs):

- `diag-classic-v800-pool200`, measurement window, `diskstat.py` over 30 s and 20 s:
  `writes/s 3114 write MB/s 80.3 mean write ms 6.58 util 90%`;
  `writes/s 3141 write MB/s 80.7 mean write ms 5.37 util 88%`. CRDB `_status/vars` cumulative at that
  point: `raft_process_logcommit_latency` sum 6.675e13 ns over 1,199,775 (mean ~56 ms);
  `admission_wait_durations_kv` sum 2.696e13 ns over 928,948 (mean ~29 ms).
- `diag-resolver-v1600-t1000`, window: `writes/s 1394 write MB/s 31.6 mean write ms 1.33 util 25%`;
  `top -H` on the resolver: one tokio worker at ~91% CPU, the rest under 40%.
- `diag-resolver-v800-t100`, window: `writes/s 3209 write MB/s 68.7 mean write ms 2.97 util 71%`;
  `procio.sh` over 20 s: resolver 30.2 MB/s, clusterd 7.1 MB/s, environmentd 0.1 MB/s, CRDB 0.6 MB/s
  (99.7 write IOs/s).

The disk is `nvme0n1`, an EBS volume; ~3,000 write IOPS is gp3's baseline. `perf` is not installed for
this kernel; the profiles are `bpftrace` `profile:hz:49` on the leaf user symbol, so frames in
`libc.so.6` (no symbols here) show as raw addresses; `profile-*.txt` holds the raw maps.
