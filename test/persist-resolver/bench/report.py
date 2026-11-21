#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""PROTOTYPE: tables and charts from bench.py results directories.

Writes `results.md` (one row per run, generated, linked from RESULTS.md) and
one SVG chart per metric under `charts/`. Every number traces to a
`results/<run>/summary.json`. Standard library only.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

BENCH = Path(__file__).resolve().parent
ARM_COLOR = {"classic": "#d1495b", "resolver": "#00798c", "resolver-grouped": "#edae49"}
TICK_DASH = {1000: "6 4", 100: ""}


def load() -> list[dict]:
    runs = []
    for d in sorted((BENCH / "results").iterdir()):
        sf, cf = d / "summary.json", d / "config.json"
        if not (sf.exists() and cf.exists()):
            continue
        s, c = json.loads(sf.read_text()), json.loads(cf.read_text())
        if c.get("exclude"):
            continue
        c.setdefault("sweep", "2")
        c.setdefault("rows_per_tick", 10)
        runs.append({"dir": d.name, "cfg": c, **s})
    return runs


def fmt(v, digits=1) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        if v == math.inf:
            return "inf"
        if abs(v) < 0.1 and v != 0:
            return f"{v:.3g}"
        return f"{v:,.{digits}f}"
    return f"{v:,}" if isinstance(v, int) else str(v)


def ms(v) -> str:
    return "-" if v is None else ("inf" if v == math.inf else f"{v * 1000:,.1f}")


COLUMNS = [
    ("run", lambda r: f"[{r['dir']}](results/{r['dir']})"),
    ("arm", lambda r: r["cfg"]["arm"]),
    ("views", lambda r: r["cfg"]["views"]),
    ("tick ms", lambda r: r["cfg"]["tick_ms"]),
    ("view writes/s", lambda r: fmt(r.get("view_writes_per_s"), 0)),
    ("fresh <=1s", lambda r: fmt(r.get("fresh_le_1s"), 3)),
    ("lag p99 bucket s", lambda r: r.get("lag_p99_bucket_s", "-")),
    ("rows/tick", lambda r: fmt(r["cfg"]["rows_per_tick"], 1)),
    ("appends/s committed / requested", lambda r: f"{fmt(r['committed_appends_per_s'], 0)} / {fmt(r['requested_appends_per_s'], 0)}" if r.get("committed_appends_per_s") is not None else "-"),
    ("CaS/s", lambda r: fmt(r.get("cas_per_s"), 0)),
    ("CaS per committed append", lambda r: fmt(r.get("cas_per_committed_append"), 3)),
    ("CaS MB/s", lambda r: fmt((r.get("cas_bytes_per_s") or 0) / 2**20, 2)),
    ("CaS p50 ms", lambda r: ms(r.get("cas_p50_s"))),
    ("CaS p99 ms", lambda r: ms(r.get("cas_p99_s"))),
    ("append mean ms", lambda r: ms(r.get("compare_and_append_mean_s"))),
    ("CRDB stmts/s", lambda r: fmt(r.get("crdb_sql_query_per_s"), 0)),
    ("CRDB cores", lambda r: fmt(r.get("crdb_cores"), 2)),
    ("CRDB p99 ms", lambda r: ms(r.get("crdb_sql_service_p99_s"))),
    ("consensus rows", lambda r: fmt((r.get("consensus_table") or {}).get("rows"))),
    ("MVCC garbage MB", lambda r: fmt(((r.get("consensus_table") or {}).get("mvcc_garbage_bytes") or 0) / 2**20, 1) if r.get("consensus_table") else "-"),
    ("envd cores", lambda r: fmt(r.get("environmentd_cores"), 2)),
    ("clusterd cores", lambda r: fmt(r.get("clusterd_cores"), 2)),
    ("resolver cores", lambda r: fmt(r.get("resolver_cores"), 2) if r["cfg"]["arm"] == "resolver" else "-"),
    ("resolver RSS MB", lambda r: fmt(r.get("resolver_rss_mb_peak"), 0) if r["cfg"]["arm"] == "resolver" else "-"),
    ("cycles/s", lambda r: fmt(r.get("resolver_cycles_per_s"), 0)),
    ("manifest KB/write", lambda r: fmt(r["resolver_bytes_per_write"] / 1024, 1) if r.get("resolver_bytes_per_write") else "-"),
    ("groups (members)", lambda r: f"{g['groups']} ({g['grouped_members']}/{g['data_shards']})" if (g := r.get("resolver_groups_end")) else "-"),
    ("commits/cycle", lambda r: fmt(r.get("resolver_commits_per_cycle"), 1)),
    ("commit p50 ms", lambda r: ms(r.get("resolver_commit_p50_s"))),
    ("commit p99 ms", lambda r: ms(r.get("resolver_commit_p99_s"))),
    ("mode ok", lambda r: (r.get("mode") or {}).get("ok", "-")),
    ("error", lambda r: r.get("error", "") or ("exited: " + ",".join(r["exited_early"]) if r.get("exited_early") else "")),
]


def table(runs: list[dict]) -> str:
    head = "| " + " | ".join(c for c, _ in COLUMNS) + " |"
    sep = "|" + "---|" * len(COLUMNS)
    rows = ["| " + " | ".join(str(f(r)) for _, f in COLUMNS) + " |" for r in runs]
    return "\n".join([head, sep, *rows])


def svg_chart(title: str, ylabel: str, series: dict[tuple[str, int], list[tuple[float, float]]],
              logy: bool = False, ymax: float | None = None) -> str:  # fmt: skip
    W, H, L, R, T, B = 640, 380, 70, 170, 40, 50
    pts = [p for s in series.values() for p in s if p[1] is not None]
    if not pts:
        return ""
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts if not logy or p[1] > 0]
    x0, x1 = math.log10(max(min(xs), 1)), math.log10(max(xs) * 1.05 + 1)
    if logy:
        y0, y1 = math.log10(min(ys)), math.log10(max(ys))
        y0, y1 = math.floor(y0), math.ceil(y1) if y1 > y0 else y0 + 1
    else:
        y0, y1 = 0.0, ymax if ymax is not None else (max(ys) * 1.1 or 1)

    def px(x):
        return L + (math.log10(max(x, 1)) - x0) / (x1 - x0) * (W - L - R)

    def py(y):
        v = math.log10(y) if logy else y
        return H - B - (v - y0) / (y1 - y0) * (H - T - B)

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="11">',
        f'<rect width="{W}" height="{H}" fill="white"/>',
        f'<text x="{L}" y="20" font-size="14" font-weight="bold">{title}</text>',
        f'<line x1="{L}" y1="{H - B}" x2="{W - R}" y2="{H - B}" stroke="black"/>',
        f'<line x1="{L}" y1="{T}" x2="{L}" y2="{H - B}" stroke="black"/>',
        f'<text x="{(L + W - R) / 2}" y="{H - 10}" text-anchor="middle">view writes/s (log)</text>',
        f'<text x="15" y="{(T + H - B) / 2}" transform="rotate(-90 15 {(T + H - B) / 2})" text-anchor="middle">{ylabel}</text>',
    ]
    for e in range(math.floor(x0), math.ceil(x1) + 1):
        for m in (1, 2, 5):
            x = m * 10**e
            if x0 <= math.log10(x) <= x1:
                out.append(f'<line x1="{px(x):.1f}" y1="{H - B}" x2="{px(x):.1f}" y2="{T}" stroke="#eee"/>')
                out.append(f'<text x="{px(x):.1f}" y="{H - B + 15}" text-anchor="middle">{x:g}</text>')
    if logy:
        ticks = [10**e for e in range(int(y0), int(y1) + 1)]
    else:
        step = 10 ** math.floor(math.log10(y1 / 5)) if y1 > 0 else 1
        step *= 1 if y1 / step <= 6 else (2 if y1 / step <= 12 else 5)
        ticks = [i * step for i in range(int(y1 / step) + 1)]
    for t in ticks:
        out.append(f'<line x1="{L}" y1="{py(t):.1f}" x2="{W - R}" y2="{py(t):.1f}" stroke="#eee"/>')
        out.append(f'<text x="{L - 5}" y="{py(t) + 4:.1f}" text-anchor="end">{t:g}</text>')
    for i, ((arm, tick), s) in enumerate(sorted(series.items())):
        s = sorted(p for p in s if p[1] is not None and (not logy or p[1] > 0))
        if not s:
            continue
        color, dash = ARM_COLOR[arm], TICK_DASH.get(tick, "2 2")
        path = " ".join(f"{'M' if j == 0 else 'L'}{px(x):.1f},{py(y):.1f}" for j, (x, y) in enumerate(s))
        out.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2" stroke-dasharray="{dash}"/>')
        for x, y in s:
            out.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="3" fill="{color}"/>')
        ly = T + 15 + i * 18
        out.append(f'<line x1="{W - R + 10}" y1="{ly}" x2="{W - R + 40}" y2="{ly}" stroke="{color}" stroke-width="2" stroke-dasharray="{dash}"/>')
        out.append(f'<text x="{W - R + 45}" y="{ly + 4}">{arm}, {1000 / tick:g} Hz</text>')
    out.append("</svg>")
    return "\n".join(out) + "\n"


CHARTS = [
    ("freshness", "Freshness: share of lag samples <= 1 s", "share", lambda r: r.get("fresh_le_1s"), False, 1.05),
    ("cas", "Consensus writes (CaS/s, all processes)", "CaS/s", lambda r: r.get("cas_per_s"), True, None),
    ("manifest_bytes", "Resolver manifest bytes per write", "KB", lambda r: (r.get("resolver_bytes_per_write") or 0) / 1024 or None, True, None),
    ("crdb_cpu", "CockroachDB CPU", "cores", lambda r: r.get("crdb_cores"), False, None),
    ("crdb_stmts", "CockroachDB statements/s", "statements/s", lambda r: r.get("crdb_sql_query_per_s"), True, None),
    ("cas_p99", "CaS p99 (bucket upper bound)", "ms", lambda r: (r.get("cas_p99_s") or 0) * 1000 or None, True, None),
    ("append_mean", "compare_and_append mean latency (client side)", "ms", lambda r: (r.get("compare_and_append_mean_s") or 0) * 1000 or None, True, None),
]


def main() -> None:
    runs = [r for r in load() if r.get("view_writes_per_s") is not None or r.get("error")]
    runs.sort(key=lambda r: (r["cfg"]["sweep"], r["cfg"]["rows_per_tick"], r["cfg"]["tick_ms"], r["cfg"]["views"], r["cfg"]["arm"], r["dir"]))
    (BENCH / "charts").mkdir(exist_ok=True)
    # Charts: sweep 2 (one revision, all three arms), the cloud report's shape.
    good = [
        r for r in runs
        if r.get("view_writes_per_s") and (r.get("mode") or {}).get("ok")
        and r["cfg"]["sweep"] == "2" and r["cfg"]["rows_per_tick"] == 10
    ]
    for name, title, ylabel, f, logy, ymax in CHARTS:
        series: dict = {}
        for r in good:
            key = (r["cfg"]["arm"], r["cfg"]["tick_ms"])
            series.setdefault(key, []).append((r["nominal_view_writes_per_s"], f(r)))
        # Repeats at one point: plot their mean; the table has each run.
        merged = {}
        for k, pts in series.items():
            by_x: dict = {}
            for x, y in pts:
                if y is not None:
                    by_x.setdefault(x, []).append(y)
            merged[k] = [(x, sum(v) / len(v)) for x, v in by_x.items()]
        svg = svg_chart(title, ylabel, merged, logy=logy, ymax=ymax)
        if svg:
            (BENCH / "charts" / f"{name}.svg").write_text(svg)
    md = [
        "<!-- Generated by report.py from results/*/summary.json. Do not edit. -->",
        "",
        "# Per-run results",
        "",
    ]
    groups = [
        ("Sweep 2: classic, resolver, resolver-grouped at one revision, 10 rows per tick",
         lambda r: r["cfg"]["sweep"] == "2" and r["cfg"]["rows_per_tick"] == 10),
        ("Sweep 2, sparse variant: 0.1 rows per tick (9 of 10 ticks upper-only)",
         lambda r: r["cfg"]["sweep"] == "2" and r["cfg"]["rows_per_tick"] != 10),
        ("Sweep 1: classic and resolver before upper groups existed",
         lambda r: r["cfg"]["sweep"] == "1"),
    ]
    for title, pred in groups:
        rows = [r for r in runs if pred(r)]
        if rows:
            md += [f"## {title}", "", table(rows), ""]
    (BENCH / "results.md").write_text("\n".join(md))
    print(f"{len(runs)} runs, {len(good)} plotted")


if __name__ == "__main__":
    main()
