# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Analyze a Tempo trace JSON file for DDL benchmark profiling.

Prints a span tree, top spans by self-time, and counts CRDB round-trips
to estimate network-adjusted latency.
"""

import argparse
import base64
import json
import sys

# Span name patterns that represent CRDB round-trips
CRDB_SPAN_PATTERNS = [
    "consensus::compare_and_set",
    "consensus::scan",
    "consensus::head",
    "oracle::write_ts",
    "oracle::apply_write",
    "oracle::read_ts",
    "oracle::get_all_timelines",
]


def decode_id(b64_id):
    """Decode a base64-encoded span/trace ID to hex string."""
    if not b64_id:
        return ""
    try:
        return base64.b64decode(b64_id).hex()
    except Exception:
        return b64_id


def parse_trace(filepath):
    with open(filepath) as f:
        data = json.load(f)
    spans = []
    for batch in data.get("batches", []):
        for scope_spans in batch.get("scopeSpans", []):
            for span in scope_spans.get("spans", []):
                start = int(span["startTimeUnixNano"])
                end = int(span["endTimeUnixNano"])
                attrs = {}
                for a in span.get("attributes", []):
                    v = a.get("value", {})
                    val = (
                        v.get("stringValue")
                        or v.get("intValue")
                        or v.get("boolValue")
                        or v.get("doubleValue")
                        or ""
                    )
                    attrs[a["key"]] = val
                spans.append(
                    {
                        "name": span["name"],
                        "spanId": decode_id(span.get("spanId", "")),
                        "parentSpanId": decode_id(span.get("parentSpanId", "")),
                        "startNs": start,
                        "endNs": end,
                        "durationNs": end - start,
                        "attributes": attrs,
                    }
                )
    return spans


def build_tree(spans):
    by_id = {s["spanId"]: s for s in spans}
    children = {}
    roots = []
    for s in spans:
        pid = s["parentSpanId"]
        if pid and pid in by_id:
            children.setdefault(pid, []).append(s)
        else:
            roots.append(s)
    return roots, children


def fmt_dur(ns):
    ms = ns / 1e6
    if ms >= 1000:
        return f"{ms/1000:.2f}s "
    elif ms >= 1:
        return f"{ms:.1f}ms"
    elif ms >= 0.001:
        return f"{ns/1000:.0f}us"
    else:
        return f"{ns}ns"


def self_time(node, children_map):
    child_time = sum(c["durationNs"] for c in children_map.get(node["spanId"], []))
    return max(0, node["durationNs"] - child_time)


def print_tree(node, children_map, indent=0, min_ms=0.1):
    dur_ms = node["durationNs"] / 1e6
    if dur_ms < min_ms:
        return
    loc = ""
    if "code.file.path" in node["attributes"]:
        loc = f' [{node["attributes"]["code.file.path"]}'
        if "code.line.number" in node["attributes"]:
            loc += f':{node["attributes"]["code.line.number"]}'
        loc += "]"
    st = self_time(node, children_map)
    prefix = "  " * indent
    print(
        f"{prefix}{fmt_dur(node['durationNs']):>10}  (self: {fmt_dur(st):>10})  {node['name']}{loc}"
    )
    for child in sorted(
        children_map.get(node["spanId"], []), key=lambda s: s["startNs"]
    ):
        print_tree(child, children_map, indent + 1, min_ms)


def count_crdb_calls(spans, children_map):
    """Count CRDB round-trip spans and classify as sequential vs parallel."""
    crdb_spans = []
    for s in spans:
        for pattern in CRDB_SPAN_PATTERNS:
            if pattern in s["name"]:
                crdb_spans.append(s)
                break

    if not crdb_spans:
        return [], 0, 0

    # Sort by start time
    crdb_spans.sort(key=lambda s: s["startNs"])

    # Count sequential vs overlapping
    sequential_count = 0
    max_parallel = 0
    if crdb_spans:
        sequential_count = 1
        current_end = crdb_spans[0]["endNs"]
        parallel_group = 1
        for s in crdb_spans[1:]:
            if s["startNs"] >= current_end:
                # Sequential
                sequential_count += 1
                max_parallel = max(max_parallel, parallel_group)
                parallel_group = 1
                current_end = s["endNs"]
            else:
                # Overlapping
                parallel_group += 1
                current_end = max(current_end, s["endNs"])
        max_parallel = max(max_parallel, parallel_group)

    return crdb_spans, sequential_count, max_parallel


def analyze(filepath, label="", crdb_rtt_ms=0):
    spans = parse_trace(filepath)
    if not spans:
        print(f"No spans found in {filepath}")
        return

    roots, children = build_tree(spans)
    if label:
        print(f"\n{'='*80}\n  {label}\n{'='*80}")
    print(f"\nTotal spans: {len(spans)}")
    root_dur = sum(r["durationNs"] for r in roots)
    print(f"Root duration: {fmt_dur(root_dur)}")

    # Top by self-time
    ranked = sorted(
        [(s, self_time(s, children)) for s in spans], key=lambda x: x[1], reverse=True
    )
    print("\nTop 15 spans by self-time:")
    print(f"{'Self Time':>12}  {'Total':>12}  Name")
    print(f"{'-'*12}  {'-'*12}  {'-'*60}")
    for s, st in ranked[:15]:
        print(f"{fmt_dur(st):>12}  {fmt_dur(s['durationNs']):>12}  {s['name']}")

    # CRDB round-trip analysis
    crdb_spans, seq_count, max_par = count_crdb_calls(spans, children)
    total_crdb = len(crdb_spans)
    if total_crdb > 0:
        crdb_local_time = sum(s["durationNs"] for s in crdb_spans)
        print(f"\nCRDB round-trips: {total_crdb} total, ~{seq_count} sequential groups (max {max_par} parallel)")
        print(f"CRDB local time:  {fmt_dur(crdb_local_time)}")

        # Breakdown by type
        by_type = {}
        for s in crdb_spans:
            for pattern in CRDB_SPAN_PATTERNS:
                if pattern in s["name"]:
                    by_type.setdefault(pattern, []).append(s)
                    break
        print(f"\n  {'Type':<35} {'Count':>6} {'Total':>10} {'Avg':>10}")
        print(f"  {'-'*35} {'-'*6} {'-'*10} {'-'*10}")
        for pattern, pattern_spans in sorted(by_type.items(), key=lambda x: -len(x[1])):
            total_ns = sum(s["durationNs"] for s in pattern_spans)
            avg_ns = total_ns // len(pattern_spans) if pattern_spans else 0
            print(f"  {pattern:<35} {len(pattern_spans):>6} {fmt_dur(total_ns):>10} {fmt_dur(avg_ns):>10}")

        if crdb_rtt_ms > 0:
            # Estimate: each sequential group adds one RTT
            estimated_network_ms = seq_count * crdb_rtt_ms
            local_ms = root_dur / 1e6
            estimated_total_ms = local_ms + estimated_network_ms
            print(f"\nNetwork cost estimate (RTT={crdb_rtt_ms}ms):")
            print(f"  Local time:     {local_ms:.1f}ms")
            print(f"  + Network:      {estimated_network_ms:.1f}ms ({seq_count} sequential x {crdb_rtt_ms}ms)")
            print(f"  = Estimated:    {estimated_total_ms:.1f}ms")

    # Span tree
    print(f"\nSpan tree (>= 0.1ms):\n{'-'*80}")
    for root in sorted(roots, key=lambda s: s["startNs"]):
        print_tree(root, children)
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze DDL benchmark traces")
    parser.add_argument("trace_file", help="Path to trace JSON file")
    parser.add_argument("label", nargs="?", default="", help="Label for the trace")
    parser.add_argument("--crdb-rtt", type=float, default=0, help="CRDB RTT in ms for network cost estimate")
    args = parser.parse_args()
    analyze(args.trace_file, args.label, args.crdb_rtt)
