#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""PROTOTYPE: one A/B point of the persist resolver against classic persist.

Brings up a fresh CockroachDB container, a file blob, (for the resolver arm) a
persist resolver, and `environmentd` with process-orchestrated `clusterd`s;
creates the cloud MV freshness workload (a COUNTER load generator and N
materialized views that each turn a tick into `--rows-per-tick` rows, 10 in
the report); waits for hydration and
a warmup; measures for `--duration`; tears everything down; and writes one
results directory.

The arms differ only in `MZ_PERSIST_RESOLVER_URL` and
`persist_v2_register_user_shards`, and the two resolver arms only in the
resolver's `--upper-groups`: `resolver` keeps every data shard's upper on its
own row, `resolver-grouped` shares one row among shards whose uppers move
together. CockroachDB is pinned to its own cores
(`--crdb-cpus`) and every Materialize process (resolver included) to the rest,
so CockroachDB's CPU is measured apart from the load it serves.

Needs Docker with the CockroachDB image, `psql`, and release binaries:
    cargo build --release --bin environmentd --bin clusterd --bin persist-resolver

Standard library only, so it runs on the box's system Python.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BENCH = Path(__file__).resolve().parent

CRDB_IMAGE = "cockroachdb/cockroach:v23.1.11"
CRDB_CONTAINER = "persist-resolver-bench-crdb"
CRDB_SQL_PORT = 26300
CRDB_HTTP_PORT = 26301
RESOLVER_PORT = 6890
RESOLVER_METRICS_PORT = 6891
RESOLVER_HTTP_PORT = 6892
ARMS = ["classic", "resolver", "resolver-grouped"]
MZ_SQL = "postgres://materialize@localhost:6875/materialize"
MZ_SYSTEM = "postgres://mz_system@localhost:6877/materialize"
MZ_INTERNAL_HTTP = "http://localhost:6878"


# Prometheus series kept from each Materialize process's `/metrics`.
KEEP_MZ = re.compile(
    r"^(mz_persist_external_(started|succeeded|failed|bytes)_count|"
    r"mz_persist_external_op_latency_(bucket|sum|count)|"
    r"mz_persist_cmd_(started|succeeded|failed|cas_mismatch)_count|"
    r"mz_persist_cmd_seconds|"
    r"mz_persist_resolver_\w+)"
    r"[{ ]"
)
KEEP_CRDB = re.compile(
    r"^(sql_(query|select|insert|update|delete|txn_commit|txn_rollback|txn_begin|failure)_count|"
    r"txn_restarts\w*|sys_cpu_\w+|sys_rss|sql_service_latency_(bucket|sum|count)|"
    r"sql_exec_latency_(bucket|sum|count)|txn_durations_(bucket|sum|count)|"
    r"exec_latency_(bucket|sum|count)|kv_\w*_count|storage_disk_\w+|"
    r"rocksdb_\w+|livebytes|keybytes|valbytes|intentbytes|gcbytesage|"
    r"livecount|keycount|valcount)[{ ]"
)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd: list[str], check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True, **kw)


def psql(url: str, sql: str, timeout: float = 120) -> list[list[str]]:
    """Runs `sql` and returns rows of `|`-separated fields."""
    out = run(
        ["psql", url, "-AtX", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout=timeout
    ).stdout
    return [line.split("|") for line in out.splitlines() if line]


def crdb_url(schema: str | None = None) -> str:
    url = f"postgres://root@localhost:{CRDB_SQL_PORT}/materialize"
    if schema:
        url += f"?options=--search_path={schema}"
    return url


def http_get(url: str, timeout: float = 30) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode()


def filter_metrics(text: str, keep: re.Pattern) -> str:
    return "\n".join(line for line in text.splitlines() if keep.match(line)) + "\n"


def parse_metrics(text: str) -> list[tuple[str, dict[str, str], float]]:
    out = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(\w+)(?:\{(.*)\})?\s+(\S+)", line)
        if not m:
            continue
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(2) or ""))
        try:
            out.append((m.group(1), labels, float(m.group(3))))
        except ValueError:
            pass
    return out


class Cpu:
    """Cumulative CPU seconds and RSS of a set of processes, read from /proc."""

    @staticmethod
    def pids_of(exe: Path) -> list[int]:
        pids = []
        for p in Path("/proc").iterdir():
            if not p.name.isdigit():
                continue
            try:
                if Path(os.readlink(p / "exe")) == exe:
                    pids.append(int(p.name))
            except OSError:
                pass
        return pids

    @staticmethod
    def read(pids: list[int]) -> tuple[float, int]:
        tick = os.sysconf("SC_CLK_TCK")
        cpu, rss = 0.0, 0
        for pid in pids:
            try:
                stat = Path(f"/proc/{pid}/stat").read_text()
                fields = stat[stat.rindex(")") + 2 :].split()
                cpu += (int(fields[11]) + int(fields[12])) / tick
                rss += int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
            except (OSError, ValueError, IndexError):
                pass
        return cpu, rss


def docker_cgroup(container: str) -> Path:
    cid = run(["docker", "inspect", "-f", "{{.Id}}", container]).stdout.strip()
    return Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope")


def cgroup_cpu_seconds(cg: Path) -> float:
    for line in (cg / "cpu.stat").read_text().splitlines():
        k, v = line.split()
        if k == "usage_usec":
            return int(v) / 1e6
    raise RuntimeError("no usage_usec")


def cgroup_memory(cg: Path) -> int:
    return int((cg / "memory.current").read_text())


class Bench:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.arm = args.arm
        self.resolver_arm = args.arm != "classic"
        name = args.name or (
            f"{args.arm}-v{args.views}-t{args.tick_ms}-{time.strftime('%Y%m%dT%H%M%S')}"
        )
        self.out = Path(args.results_root) / name
        self.out.mkdir(parents=True, exist_ok=False)
        # Data and full logs are large; only the results dir is kept.
        self.scratch = ROOT / "scratch" / "persist-resolver-bench" / name
        self.mzdata = self.scratch / "mzdata"
        self.target = Path(args.target_dir)
        self.procs: list[subprocess.Popen] = []
        self.samples: list[dict] = []
        self.stop_sampling = threading.Event()
        self.mz_cpus = f"{args.crdb_cpus}-{os.cpu_count() - 1}"
        self.crdb_cpus = f"0-{args.crdb_cpus - 1}"

    # Setup and teardown.

    def start_crdb(self) -> None:
        run(["docker", "rm", "-f", CRDB_CONTAINER], check=False)
        run(
            [
                "docker", "run", "-d", "--name", CRDB_CONTAINER,
                f"--cpuset-cpus={self.crdb_cpus}",
                f"--memory={self.args.crdb_memory}",
                "-p", f"127.0.0.1:{CRDB_SQL_PORT}:26257",
                "-p", f"127.0.0.1:{CRDB_HTTP_PORT}:8080",
                CRDB_IMAGE, "start-single-node", "--insecure",
                f"--cache={self.args.crdb_cache}",
            ]
        )  # fmt: skip
        for _ in range(120):
            try:
                psql(f"postgres://root@localhost:{CRDB_SQL_PORT}/defaultdb", "SELECT 1", 5)
                break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                time.sleep(1)
        else:
            raise RuntimeError("cockroach did not come up")
        psql(f"postgres://root@localhost:{CRDB_SQL_PORT}/defaultdb", "CREATE DATABASE materialize")
        for schema in ["consensus", "tsoracle", "storage"]:
            psql(crdb_url(), f"CREATE SCHEMA {schema}")
        self.crdb_cg = docker_cgroup(CRDB_CONTAINER)

    def spawn(self, name: str, cmd: list[str], env: dict[str, str]) -> subprocess.Popen:
        logf = open(self.scratch / f"{name}.log", "w")
        p = subprocess.Popen(
            ["taskset", "-c", self.mz_cpus, *cmd],
            stdout=logf,
            stderr=subprocess.STDOUT,
            env={**os.environ, **env},
            start_new_session=True,
        )
        self.procs.append(p)
        return p

    def start_resolver(self) -> None:
        self.spawn(
            "resolver",
            [
                str(self.target / "persist-resolver"),
                f"--blob-url=file://{self.mzdata}/persist/blob",
                f"--consensus-url={crdb_url('consensus')}",
                f"--listen-addr=127.0.0.1:{RESOLVER_PORT}",
                f"--metrics-listen-addr=127.0.0.1:{RESOLVER_METRICS_PORT}",
                f"--http-listen-addr=127.0.0.1:{RESOLVER_HTTP_PORT}",
                f"--upper-groups={str(self.arm == 'resolver-grouped').lower()}",
            ],
            {"RUST_LOG": "info"},
        )
        deadline = time.time() + 120
        while "claimed partition" not in (self.scratch / "resolver.log").read_text():
            if time.time() > deadline or self.procs[-1].poll() is not None:
                raise RuntimeError("resolver did not claim its partition")
            time.sleep(0.5)

    def start_environmentd(self) -> None:
        def size(workers: int) -> dict:
            return {
                "cpu_exclusive": False, "cpu_limit": None, "credits_per_hour": "1",
                "disabled": False, "disk_limit": None, "is_cc": True,
                "memory_limit": "16 GiB", "scale": 1, "workers": workers,
            }  # fmt: skip

        sizes = {
            "bootstrap": size(1),
            "scale=1,workers=1": size(1),
            "bench": size(self.args.workers),
        }
        resolver = self.resolver_arm
        params = {
            "persist_v2_register_user_shards": str(resolver).lower(),
            "enable_load_generator_counter": "true",
            "min_timestamp_interval": "10ms",
            "max_materialized_views": "100000",
            "max_objects_per_schema": "100000",
            "max_clusters": "1000",
            "max_sources": "1000",
            "max_tables": "1000",
            "max_credit_consumption_rate": "100000",
        }
        env = {"RUST_MIN_STACK": "16777216"}
        if resolver:
            env["MZ_PERSIST_RESOLVER_URL"] = f"http://127.0.0.1:{RESOLVER_PORT}"
        for d in ["secrets", "prometheus", "scratch"]:
            (self.mzdata / d).mkdir(parents=True, exist_ok=True)
        self.spawn(
            "environmentd",
            [
                str(self.target / "environmentd"),
                f"--listeners-config-path={ROOT}/src/materialized/ci/listener_configs/v26_32_0/no_auth.json",
                "--orchestrator=process",
                f"--orchestrator-process-secrets-directory={self.mzdata}/secrets",
                "--orchestrator-process-tcp-proxy-listen-addr=0.0.0.0",
                f"--orchestrator-process-prometheus-service-discovery-directory={self.mzdata}/prometheus",
                f"--orchestrator-process-scratch-directory={self.mzdata}/scratch",
                "--secrets-controller=local-file",
                f"--persist-consensus-url={crdb_url('consensus')}",
                f"--persist-blob-url=file://{self.mzdata}/persist/blob",
                f"--timestamp-oracle-url={crdb_url('tsoracle')}",
                f"--environment-id=local-az1-{uuid.uuid4()}-0",
                "--bootstrap-role=materialize",
                f"--cluster-replica-sizes={json.dumps(sizes)}",
                "--bootstrap-default-cluster-replica-size=bootstrap",
                *[f"--system-parameter-default={k}={v}" for k, v in params.items()],
            ],
            env,
        )
        for _ in range(300):
            try:
                psql(MZ_SQL, "SELECT 1", 5)
                return
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                if self.procs[-1].poll() is not None:
                    raise RuntimeError("environmentd exited")
                time.sleep(1)
        raise RuntimeError("environmentd did not come up")

    def teardown(self) -> None:
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        time.sleep(2)
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        # clusterd processes are in environmentd's session, but kill any stragglers.
        for pid in Cpu.pids_of(self.target / "clusterd"):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        run(["docker", "rm", "-f", CRDB_CONTAINER], check=False)

    # Workload.

    def create_workload(self) -> None:
        """A COUNTER load generator on its own cluster, and views that each
        turn a tick into `--rows-per-tick` rows (10 in the cloud report),
        spread over `--clusters` clusters."""
        a = self.args
        tick = f"{a.tick_ms}ms"
        stmts = [
            "CREATE CLUSTER ingest SIZE 'bench'",
            f"CREATE SOURCE ticks_src IN CLUSTER ingest FROM LOAD GENERATOR COUNTER "
            f"(TICK INTERVAL '{tick}') WITH (TIMESTAMP INTERVAL '{tick}')",
            "CREATE TABLE ticks FROM SOURCE ticks_src",
        ]
        for c in range(a.clusters):
            stmts.append(f"CREATE CLUSTER c{c} SIZE 'bench'")
        r = a.rows_per_tick
        for i in range(a.views):
            if r == int(r):
                rows = str(int(r))
            else:
                # The spec sheet's `persist_mvs` formula: `r` rows per tick on
                # average, staggered by view so the ticks a view skips (writing
                # no data, only advancing its upper) differ from view to view.
                phase = f"{i}::numeric / {a.views}"
                rows = (
                    f"(floor((counter + 1) * {r} + {phase}) "
                    f"- floor(counter * {r} + {phase}))::int"
                )
            stmts.append(
                f"CREATE MATERIALIZED VIEW mv_{i} IN CLUSTER c{i % a.clusters} AS "
                f"SELECT counter, g FROM ticks, generate_series(1, {rows}) AS g"
            )
        sql = self.out / "workload.sql"
        sql.write_text(";\n".join(stmts) + ";\n")
        start = time.time()
        run(["psql", MZ_SQL, "-qX", "-v", "ON_ERROR_STOP=1", "-f", str(sql)], timeout=7200)
        self.setup_seconds = time.time() - start
        log(f"created workload in {self.setup_seconds:.0f}s")
        self.mv_ids = [
            r[0]
            for r in psql(
                MZ_SQL,
                "SELECT id FROM mz_materialized_views WHERE name LIKE 'mv\\_%' ORDER BY id",
            )
        ]
        assert len(self.mv_ids) == a.views, (len(self.mv_ids), a.views)

    def mv_filter(self) -> str:
        return "object_id IN (SELECT id FROM mz_materialized_views WHERE name LIKE 'mv\\_%')"

    def frontier_lags(self, timeout: float = 120) -> list[float]:
        """Each view's live lag: wall clock minus its write frontier, as the
        controller knows it. `mz_wallclock_global_lag` won't do here: it is the
        per-minute maximum from the lag history."""
        rows = psql(
            MZ_SQL,
            f"SELECT write_frontier::text FROM mz_internal.mz_frontiers "
            f"WHERE {self.mv_filter()}",
            timeout,
        )
        now_ms = time.time() * 1000
        return [(now_ms - int(f)) / 1000 if f else math.inf for [f] in rows]

    def wait_hydrated(self) -> None:
        """Every view's write frontier within 10 s of now, or the timeout."""
        deadline = time.time() + self.args.hydration_timeout
        while time.time() < deadline:
            lags = self.frontier_lags()
            if len(lags) == self.args.views and max(lags) < 10:
                log("all views caught up")
                return
            time.sleep(5)
        log("WARNING: hydration timeout, measuring anyway")

    # Measurement.

    def scrape_targets(self) -> dict[str, str]:
        """Every Materialize process's metrics URL, by name."""
        targets = {"environmentd": f"{MZ_INTERNAL_HTTP}/metrics"}
        if self.resolver_arm:
            targets["resolver"] = f"http://127.0.0.1:{RESOLVER_METRICS_PORT}/metrics"
        for f in (self.mzdata / "prometheus").glob("*.json"):
            for entry in json.loads(f.read_text()):
                labels = entry["labels"]
                if labels.get("mz_orchestrator_port") != "internal-http":
                    continue
                name = (
                    f"clusterd-{labels.get('mz_orchestrator_cluster_id', labels['mz_orchestrator_service_id'])}"
                    f"-{labels['mz_orchestrator_ordinal']}"
                )
                targets[name] = f"http://{entry['targets'][0]}/metrics"
        return targets

    def snapshot(self, tag: str) -> dict:
        """Cumulative counters at one instant: metrics from every process and
        CockroachDB, process CPU, the lag histogram, and view row counts."""
        snap: dict = {"t": time.time()}
        metrics = {}
        for name, url in self.scrape_targets().items():
            try:
                metrics[name] = filter_metrics(http_get(url), KEEP_MZ)
            except Exception as e:  # noqa: BLE001 - a dead process is a finding, not a crash
                log(f"WARNING: scraping {name}: {e}")
                metrics[name] = ""
        snap["metrics"] = metrics
        snap["crdb"] = filter_metrics(
            http_get(f"http://localhost:{CRDB_HTTP_PORT}/_status/vars"), KEEP_CRDB
        )
        snap["cpu"] = self.cpu_snapshot()
        snap["histogram"] = psql(
            MZ_SQL,
            f"SELECT object_id, coalesce(lag_seconds::text, 'null'), sum(count) "
            f"FROM mz_internal.mz_wallclock_global_lag_histogram WHERE {self.mv_filter()} "
            f"GROUP BY 1, 2 ORDER BY 1, 2",
        )
        if self.resolver_arm:
            snap["groups"] = self.group_stats()
        [[snap["ticks_rows"]]] = psql(MZ_SQL, "SELECT count(*) FROM ticks")
        [[snap["mv0_rows"]]] = psql(MZ_SQL, "SELECT count(*) FROM mv_0") if self.args.views else [["0"]]
        # Everything `summarize` reads, so a results dir can be summarized again.
        with gzip.open(self.out / f"snapshot-{tag}.json.gz", "wt") as f:
            json.dump(snap, f)
        return snap

    def group_stats(self) -> dict | None:
        """Upper groups as the resolver's debug endpoint lists them."""
        try:
            view = json.loads(http_get(f"http://127.0.0.1:{RESOLVER_HTTP_PORT}/views", timeout=60))
        except Exception as e:  # noqa: BLE001 - a missing stat is not a failed run
            log(f"WARNING: group stats: {e}")
            return None
        sizes = sorted((len(g["members"]) for g in view.get("upper_groups", [])), reverse=True)
        return {
            "data_shards": len(view.get("data_shards", [])),
            "groups": len(sizes),
            "grouped_members": sum(sizes),
            "largest_groups": sizes[:5],
        }

    def cpu_snapshot(self) -> dict:
        roles = {
            "environmentd": self.target / "environmentd",
            "clusterd": self.target / "clusterd",
            "resolver": self.target / "persist-resolver",
        }
        out = {}
        for role, exe in roles.items():
            cpu, rss = Cpu.read(Cpu.pids_of(exe))
            out[role] = {"cpu_s": cpu, "rss": rss}
        out["crdb"] = {
            "cpu_s": cgroup_cpu_seconds(self.crdb_cg),
            "rss": cgroup_memory(self.crdb_cg),
        }
        return out

    def sampler(self) -> None:
        """Every `--sample-secs`: CPU and memory per role, and live view lag."""
        prev = self.cpu_snapshot()
        prev_t = time.time()
        while not self.stop_sampling.wait(self.args.sample_secs):
            try:
                now = self.cpu_snapshot()
                t = time.time()
                row = {"t": t}
                for role, v in now.items():
                    row[f"{role}_cores"] = (v["cpu_s"] - prev[role]["cpu_s"]) / (t - prev_t)
                    row[f"{role}_rss_mb"] = v["rss"] / 2**20
                lags = sorted(self.frontier_lags(timeout=30))
                row["views"] = len(lags)
                # The histogram's bucket 1 is `as_secs().next_power_of_two() == 1`,
                # so lag under 2 s.
                row["views_fresh_lt_2s"] = sum(1 for x in lags if x < 2)
                row["max_lag_s"] = lags[-1] if lags else None
                row["p50_lag_s"] = lags[len(lags) // 2] if lags else None
                row["p99_lag_s"] = lags[min(len(lags) - 1, int(0.99 * len(lags)))] if lags else None
                self.samples.append(row)
                prev, prev_t = now, t
            except Exception as e:  # noqa: BLE001 - keep sampling through a slow query
                log(f"WARNING: sample: {e}")

    def measure(self) -> tuple[dict, dict]:
        """Snapshots bracketing the window, aligned to the lag histogram's
        recording: it records once per `wallclock_lag_recording_interval`
        (60 s), at the minute, so the window is whole minutes and each snapshot
        is taken `--align-slack` after a boundary, once that minute's histogram
        rows have landed."""
        slack = self.args.align_slack

        def wait_boundary() -> None:
            now = time.time()
            time.sleep(math.ceil((now - slack) / 60) * 60 + slack - now)

        wait_boundary()
        log("measurement window starts")
        start = self.snapshot("start")
        thread = threading.Thread(target=self.sampler, daemon=True)
        thread.start()
        minutes = max(1, round(self.args.duration / 60))
        time.sleep(start["t"] + minutes * 60 - time.time())
        end = self.snapshot("end")
        self.stop_sampling.set()
        thread.join()
        log("measurement window ends")
        return start, end

    # Verification and summary.

    def verify_mode(self) -> dict:
        """Which user shards have consensus rows of their own: none in the
        resolver arm, all in the classic arm."""
        shards = [
            r[0]
            for r in psql(
                MZ_SQL,
                "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%'",
            )
        ]
        with_rows = {
            r[0]
            for r in psql(
                crdb_url(),
                "SELECT DISTINCT shard FROM consensus.consensus",
                timeout=600,
            )
        }
        own = sum(1 for s in shards if s in with_rows)
        expect = len(shards) if self.arm == "classic" else 0
        return {
            "user_shards": len(shards),
            "user_shards_with_consensus_rows": own,
            "ok": own == expect and len(shards) > 0,
        }

    def consensus_table(self) -> dict:
        [[rows, shards]] = psql(
            crdb_url(), "SELECT count(*), count(DISTINCT shard) FROM consensus.consensus", 600
        )
        [[live, total, ranges]] = psql(
            crdb_url(),
            "SELECT live_bytes, total_bytes, range_count FROM crdb_internal.tenant_span_stats() "
            "WHERE table_id = 'consensus.consensus'::regclass::oid",
            600,
        )
        return {
            "rows": int(rows),
            "shards": int(shards),
            "ranges": int(ranges),
            "live_bytes": int(live),
            "total_bytes": int(total),
            # Deleted and overwritten versions not yet GCed: CockroachDB's
            # analogue of Postgres dead rows.
            "mvcc_garbage_bytes": int(total) - int(live),
        }

    def log_problems(self) -> dict:
        out = {}
        for f in self.scratch.glob("*.log"):
            text = f.read_text(errors="replace")
            hits = [
                line
                for line in text.splitlines()
                if re.search(r"panicked|thread '.*' panicked|FATAL|halting process|lost its claim", line)
            ]
            out[f.stem] = {"lines": len(text.splitlines()), "problems": hits[:20]}
            # Keep each log's tail and its problem lines with the results.
            (self.out / f"{f.stem}.log.tail").write_text("\n".join(text.splitlines()[-200:]) + "\n")
        clusterd_logs = list((self.mzdata / "scratch").glob("**/*.log")) if (self.mzdata / "scratch").exists() else []
        out["clusterd_log_files"] = len(clusterd_logs)
        return out

    def go(self) -> None:
        a = self.args
        self.scratch.mkdir(parents=True, exist_ok=True)
        config = {**vars(a), "mz_cpus": self.mz_cpus, "crdb_cpus_set": self.crdb_cpus,
                  "git_head": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).stdout.strip(),
                  "git_dirty": bool(run(["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=no"]).stdout.strip()),
                  "started": time.strftime("%Y-%m-%dT%H:%M:%S%z")}  # fmt: skip
        (self.out / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        summary: dict = {"ok": False}
        try:
            log(f"{self.out.name}: cockroach")
            self.start_crdb()
            if self.resolver_arm:
                log("resolver")
                self.start_resolver()
            log("environmentd")
            self.start_environmentd()
            log(f"workload: {a.views} views, tick {a.tick_ms} ms")
            self.create_workload()
            self.wait_hydrated()
            log(f"warmup {a.warmup}s")
            time.sleep(a.warmup)
            start, end = self.measure()
            summary = summarize(start, end, self.samples, a)
            summary["setup_seconds"] = self.setup_seconds
            summary["mode"] = self.verify_mode()
            summary["consensus_table"] = self.consensus_table()
            summary["ok"] = summary["mode"]["ok"]
        except Exception as e:  # noqa: BLE001 - a failed run is still a result
            log(f"FAILED: {e!r}")
            summary["error"] = repr(e)
        finally:
            dead = [p.args for p in self.procs if p.poll() is not None]
            summary["exited_early"] = [" ".join(map(str, d[3:4])) for d in dead]
            summary["logs"] = self.log_problems()
            with open(self.out / "samples.jsonl", "w") as f:
                for s in self.samples:
                    f.write(json.dumps(s) + "\n")
            (self.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            self.teardown()
            if not a.keep_data:
                shutil.rmtree(self.scratch / "mzdata", ignore_errors=True)
        log(f"summary: {json.dumps({k: v for k, v in summary.items() if k != 'logs'})}")


def counter(metrics: dict[str, str], name: str, **match: str) -> float:
    total = 0.0
    for text in metrics.values():
        for n, labels, v in parse_metrics(text):
            if n == name and all(labels.get(k) == val for k, val in match.items()):
                total += v
    return total


def by_process(metrics: dict[str, str], name: str, **match: str) -> dict[str, float]:
    return {p: counter({p: t}, name, **match) for p, t in metrics.items()}


def histogram(metrics: dict[str, str], name: str, **match: str) -> dict[float, float]:
    """Cumulative bucket counts by upper bound, summed over processes."""
    out: dict[float, float] = {}
    for text in metrics.values():
        for n, labels, v in parse_metrics(text):
            if n == f"{name}_bucket" and all(labels.get(k) == val for k, val in match.items()):
                le = math.inf if labels["le"] == "+Inf" else float(labels["le"])
                out[le] = out.get(le, 0.0) + v
    return out


def quantile(start: dict[float, float], end: dict[float, float], q: float) -> float | None:
    """The upper bound of the bucket holding quantile `q` of the window's
    observations."""
    les = sorted(end)
    delta = [(le, end[le] - start.get(le, 0.0)) for le in les]
    if not delta or delta[-1][1] <= 0:
        return None
    total = delta[-1][1]
    for le, c in delta:
        if c >= q * total:
            return le
    return None


def summarize(start: dict, end: dict, samples: list[dict], a: argparse.Namespace) -> dict:
    secs = end["t"] - start["t"]
    rate = lambda s, e: (e - s) / secs  # noqa: E731

    sm, em = start["metrics"], end["metrics"]
    cas = "consensus_cas"
    s = {
        "window_seconds": round(secs, 1),
        "views": a.views,
        "tick_ms": a.tick_ms,
        "nominal_view_writes_per_s": a.views * 1000 / a.tick_ms,
    }

    # Did the work happen: ticks and one view's rows over the window.
    ticks = rate(int(start["ticks_rows"]), int(end["ticks_rows"]))
    s["ticks_per_s"] = round(ticks, 2)
    s["mv0_rows_per_s"] = round(rate(int(start["mv0_rows"]), int(end["mv0_rows"])), 2)
    s["rows_per_tick"] = getattr(a, "rows_per_tick", 10)
    s["view_writes_per_s"] = round(ticks * a.views, 1)

    # Freshness: the histogram's samples recorded in the window.
    def hist(rows):
        return {(o, l): int(c) for o, l, c in rows}

    h0, h1 = hist(start["histogram"]), hist(end["histogram"])
    buckets: dict[str, int] = {}
    for k, c in h1.items():
        d = c - h0.get(k, 0)
        if d:
            buckets[k[1]] = buckets.get(k[1], 0) + d
    total = sum(buckets.values())
    s["lag_histogram"] = dict(sorted(buckets.items(), key=lambda kv: (kv[0] == "null", int(kv[0]) if kv[0] != "null" else 0)))
    s["lag_samples"] = total
    s["lag_samples_expected"] = a.views * round(secs / 60) * 60
    fresh = sum(c for b, c in buckets.items() if b != "null" and int(b) <= 1)
    s["fresh_le_1s"] = round(fresh / total, 4) if total else None
    acc, p99 = 0, None
    for b, c in s["lag_histogram"].items():
        acc += c
        if acc >= 0.99 * total:
            p99 = b
            break
    s["lag_p99_bucket_s"] = p99

    # Consensus: CaS from every Materialize process (the resolver's own
    # manifest writes included), and the client-side append path.
    cas_by = {
        p: rate(by_process(sm, "mz_persist_external_succeeded_count", op=cas).get(p, 0), v)
        for p, v in by_process(em, "mz_persist_external_succeeded_count", op=cas).items()
    }
    s["cas_per_s"] = round(sum(cas_by.values()), 1)
    s["cas_per_s_by_process"] = {p: round(v, 1) for p, v in sorted(cas_by.items())}
    s["cas_bytes_per_s"] = round(
        rate(counter(sm, "mz_persist_external_bytes_count", op=cas), counter(em, "mz_persist_external_bytes_count", op=cas))
    )
    s["cas_failed_per_s"] = round(
        rate(counter(sm, "mz_persist_external_failed_count", op=cas), counter(em, "mz_persist_external_failed_count", op=cas)), 2
    )
    s["cas_mismatch_per_s"] = round(
        rate(counter(sm, "mz_persist_cmd_cas_mismatch_count"), counter(em, "mz_persist_cmd_cas_mismatch_count")), 2
    )
    hs = histogram(sm, "mz_persist_external_op_latency", op=cas)
    he = histogram(em, "mz_persist_external_op_latency", op=cas)
    s["cas_p50_s"] = quantile(hs, he, 0.5)
    s["cas_p99_s"] = quantile(hs, he, 0.99)
    # Materialize's appends only: the resolver's manifest writes are its own.
    caa = dict(cmd="compare_and_append")
    smc = {p: v for p, v in sm.items() if p != "resolver"}
    emc = {p: v for p, v in em.items() if p != "resolver"}
    n = counter(emc, "mz_persist_cmd_succeeded_count", **caa) - counter(smc, "mz_persist_cmd_succeeded_count", **caa)
    t = counter(emc, "mz_persist_cmd_seconds", **caa) - counter(smc, "mz_persist_cmd_seconds", **caa)
    s["compare_and_append_per_s"] = round(n / secs, 1)
    s["compare_and_append_mean_s"] = round(t / n, 5) if n else None

    # CockroachDB.
    sc, ec = {"c": start["crdb"]}, {"c": end["crdb"]}
    for k in ["sql_query_count", "sql_insert_count", "sql_update_count", "sql_select_count",
              "sql_delete_count", "sql_txn_commit_count", "sql_failure_count"]:  # fmt: skip
        s[f"crdb_{k.removesuffix('_count')}_per_s"] = round(rate(counter(sc, k), counter(ec, k)), 1)
    s["crdb_txn_restarts_per_s"] = round(
        sum(rate(counter(sc, n), counter(ec, n)) for n in {m for m, _, _ in parse_metrics(end["crdb"]) if m.startswith("txn_restarts_")}),
        2,
    )
    ls, le_ = histogram(sc, "sql_service_latency"), histogram(ec, "sql_service_latency")
    p = quantile(ls, le_, 0.5)
    s["crdb_sql_service_p50_s"] = p / 1e9 if p else None
    p = quantile(ls, le_, 0.99)
    s["crdb_sql_service_p99_s"] = p / 1e9 if p and p != math.inf else p

    # CPU and memory by role over the window; peak RSS from the samples.
    for role in ["environmentd", "clusterd", "resolver", "crdb"]:
        s[f"{role}_cores"] = round(rate(start["cpu"][role]["cpu_s"], end["cpu"][role]["cpu_s"]), 2)
        s[f"{role}_rss_mb_end"] = round(end["cpu"][role]["rss"] / 2**20)
        peaks = [x.get(f"{role}_rss_mb", 0) for x in samples]
        s[f"{role}_rss_mb_peak"] = round(max(peaks)) if peaks else None

    # Resolver.
    if a.arm != "classic":
        r = lambda n: rate(counter(sm, f"mz_persist_resolver_{n}_total"), counter(em, f"mz_persist_resolver_{n}_total"))  # noqa: E731
        cycles = r("cycles")
        s["resolver_cycles_per_s"] = round(cycles, 1)
        s["resolver_commits_per_s"] = round(r("commits"), 1)
        s["resolver_commits_per_cycle"] = round(r("commits") / cycles, 1) if cycles else None
        s["resolver_manifest_writes_per_s"] = round(r("manifest_writes"), 1)
        s["resolver_lost_writes_per_s"] = round(r("lost_writes"), 2)
        s["resolver_bytes_per_write"] = round(r("encoded_bytes_written") / r("manifest_writes")) if r("manifest_writes") else None
        s["resolver_watch_requests_per_s"] = round(r("watch_requests"), 1)
        s["resolver_fetch_requests_per_s"] = round(r("fetch_requests"), 1)
        s["resolver_groups_start"] = start.get("groups")
        s["resolver_groups_end"] = end.get("groups")
        for name in ["commit", "manifest_write"]:
            hs = histogram(sm, f"mz_persist_resolver_{name}_seconds")
            he = histogram(em, f"mz_persist_resolver_{name}_seconds")
            s[f"resolver_{name}_p50_s"] = quantile(hs, he, 0.5)
            s[f"resolver_{name}_p99_s"] = quantile(hs, he, 0.99)

    # Live freshness from the samples, as a cross-check of the histogram.
    if samples:
        fr = [x["views_fresh_lt_2s"] / x["views"] for x in samples if x.get("views")]
        s["sampled_fresh_lt_2s_mean"] = round(sum(fr) / len(fr), 4) if fr else None
        p50 = [x["p50_lag_s"] for x in samples if x.get("p50_lag_s") is not None]
        s["sampled_p50_lag_s_mean"] = round(sum(p50) / len(p50), 3) if p50 else None
        s["sampled_p99_lag_s_max"] = round(max((x["p99_lag_s"] or 0) for x in samples), 3)
        s["sampled_max_lag_s_max"] = round(max((x["max_lag_s"] or 0) for x in samples), 3)
    return s


def resummarize(out: Path) -> None:
    """Recomputes `summary.json`'s measurements from a results dir's snapshots,
    keeping what only the live run knew (mode check, consensus table, logs)."""
    cfg = argparse.Namespace(**json.loads((out / "config.json").read_text()))
    snaps = []
    for tag in ["start", "end"]:
        with gzip.open(out / f"snapshot-{tag}.json.gz", "rt") as f:
            snaps.append(json.load(f))
    samples = [json.loads(line) for line in (out / "samples.jsonl").read_text().splitlines() if line]
    old = json.loads((out / "summary.json").read_text())
    new = summarize(snaps[0], snaps[1], samples, cfg)
    keep = ["setup_seconds", "mode", "consensus_table", "ok", "error", "exited_early", "logs"]
    new.update({k: old[k] for k in keep if k in old})
    (out / "summary.json").write_text(json.dumps(new, indent=2) + "\n")


def main() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "--resummarize":
        for d in sys.argv[2:]:
            resummarize(Path(d))
        return
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--arm", choices=ARMS, required=True)
    p.add_argument("--views", type=int, required=True)
    p.add_argument("--tick-ms", type=int, required=True)
    p.add_argument(
        "--rows-per-tick",
        type=float,
        default=10,
        help="rows each view writes per tick, on average; 10 is the cloud report's shape",
    )
    p.add_argument("--duration", type=int, default=600, help="seconds, rounded to whole minutes")
    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--clusters", type=int, default=4)
    p.add_argument("--workers", type=int, default=4, help="timely workers per replica")
    p.add_argument("--crdb-cpus", type=int, default=4)
    p.add_argument("--crdb-memory", default="16g")
    p.add_argument("--crdb-cache", default="4GiB")
    p.add_argument("--hydration-timeout", type=int, default=600)
    p.add_argument("--sample-secs", type=float, default=5)
    p.add_argument("--align-slack", type=int, default=15)
    p.add_argument("--name")
    p.add_argument("--results-root", default=str(BENCH / "results"))
    p.add_argument("--target-dir", default=str(ROOT / "target" / "release"))
    p.add_argument("--keep-data", action="store_true")
    Bench(p.parse_args()).go()


if __name__ == "__main__":
    main()
