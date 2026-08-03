# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reproducer for SQL-457: SUBSCRIBE emits stale rows under concurrent writes."""

import threading
import time
from collections import Counter, deque

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.util import PropagatingThread

SERVICES = [Materialized()]


def row_values(kk: int) -> str:
    """VALUES tuple for a row keyed by kk, deterministic from the key."""
    dr = (
        "daterange(NULL, NULL)"
        if kk % 5 == 0
        else (
            f"daterange((DATE '2000-01-01' + {kk % 3000} * INTERVAL '1 day')::date,"
            f" (DATE '2000-01-01' + {kk % 3000 + 30} * INTERVAL '1 day')::date)"
        )
    )
    return (
        f"({kk}, LIST[{kk % 100}, {(kk + 1) % 100}], {kk % 32000}, {kk % 60000}, {dr})"
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--writers", default=4, type=int)
    parser.add_argument("--verifiers", default=2, type=int)
    parser.add_argument("--replicas", default=2, type=int)
    parser.add_argument("--history", default=64, type=int)
    # Keep hunting past pure stale-row discrepancies for the "torn" variant
    # (committed rows also missing from the stream), which is the more severe
    # symptom. Falls back to the stale-row discrepancy if none appears in time.
    parser.add_argument("--prefer-torn", action="store_true")
    args = parser.parse_args()

    c.up("materialized")
    # Column types mirror the parallel-workload table that failed (int list,
    # smallint, uint2, daterange); the defect did not show with plain integers.
    c.sql(
        f"""
        DROP TABLE IF EXISTS t CASCADE;
        DROP CLUSTER IF EXISTS c_idx CASCADE;
        DROP CLUSTER IF EXISTS c_noidx CASCADE;
        CREATE CLUSTER c_idx SIZE 'scale=1,workers=2', REPLICATION FACTOR {args.replicas};
        CREATE CLUSTER c_noidx SIZE 'scale=1,workers=2';
        CREATE TABLE t (k bigint, arr int list, s smallint, u uint2, dr daterange);
        INSERT INTO t SELECT g, LIST[g, g+1], g % 32000, g % 60000,
            daterange(
                (DATE '2000-01-01' + g * INTERVAL '1 day')::date,
                (DATE '2001-01-01' + g * INTERVAL '1 day')::date)
            FROM generate_series(1, 200) g;
        CREATE DEFAULT INDEX t_idx IN CLUSTER c_idx ON t;
        """
    )

    deadline = time.time() + args.runtime
    flag = {"stop": False}
    failure: list[str] = []
    next_key = {"k": 1000}
    key_counter_lock = threading.Lock()
    # Guards the write + in-memory model update, so the recorded sequence of
    # key-sets is exact and matches Materialize's commit order. Keys are
    # globally unique, so tracking the SET of present keys is enough to catch
    # extra (stale, not retracted), missing (lost), and duplicated rows without
    # having to model column values or re-read the table on every write. The
    # cheap in-memory replay keeps the write rate high, which the torn variant
    # (missing committed rows, not just stale ones) needs.
    lock = threading.Lock()
    version = {"v": 0}
    present: set[str] = {str(i) for i in range(1, 201)}
    history: deque[tuple[int, frozenset[str]]] = deque(
        [(0, frozenset(present))], maxlen=args.history
    )
    # Pure stale-row discrepancies seen while hunting for the torn variant.
    stale_seen: list[str] = []

    def writer() -> None:
        cur = c.sql_cursor(reuse_connection=False)  # autocommit, so each write commits
        cur.execute(b"SET cluster = c_noidx")
        while not flag["stop"] and time.time() < deadline and not failure:
            with key_counter_lock:
                k = next_key["k"]
                next_key["k"] += 1
            op = k % 10
            r = k % 11
            if op <= 4:
                stmt = "INSERT INTO t VALUES " + row_values(k)
            elif op <= 6:
                stmt = f"UPDATE t SET s = (s + 1) % 32000 WHERE k % 7 = {k % 7}"
            elif op <= 8:
                stmt = f"DELETE FROM t WHERE k % 11 = {r}"
            else:
                stmt = "DELETE FROM t"
            try:
                with lock:
                    cur.execute(stmt.encode())
                    # Replay onto the key-set model in the same commit order.
                    if op <= 4:
                        present.add(str(k))
                    elif op <= 6:
                        pass  # value-only update, key-set unchanged
                    elif op <= 8:
                        present.difference_update(
                            {key for key in present if int(key) % 11 == r}
                        )
                    else:
                        present.clear()
                    version["v"] += 1
                    history.append((version["v"], frozenset(present)))
            except Exception as e:
                if not flag["stop"]:
                    print(f"writer error (ignored): {e}")

    def verifier() -> None:
        while not flag["stop"] and time.time() < deadline and not failure:
            try:
                cur = c.sql_cursor(reuse_connection=False)
                cur.execute(b"SET cluster = c_idx")
                cur.execute("BEGIN")
                # Sample the floor BEFORE declaring: every version <= floor
                # committed before the subscribe started, so its as_of (strict
                # serializable) is >= floor's timestamp and its first settled
                # state is some version >= floor. A too-high floor would falsely
                # fail, so sampling early (a wider window) is the safe side.
                with lock:
                    floor = version["v"]
                cur.execute(
                    "DECLARE cur CURSOR FOR SUBSCRIBE (SELECT * FROM t) WITH (PROGRESS)"
                )
                # Accumulate FULL row tuples: an UPDATE emits a retract of the
                # old row and an insert of the new one at the same timestamp, so
                # keying by the primary key alone would wrongly cancel them.
                state: Counter = Counter()
                pending: dict = {}
                rounds = 0
                while rounds < 15 and not flag["stop"] and not failure:
                    rounds += 1
                    cur.execute("FETCH ALL cur WITH (timeout = '1s')")
                    for row in cur.fetchall():
                        ts, progressed, diff = row[0], row[1], row[2]
                        if not progressed:
                            rowtup = tuple(str(x) for x in row[3:])
                            pending.setdefault(ts, Counter())[rowtup] += int(diff)
                            continue
                        for bts in sorted(t for t in pending if t < ts):
                            for rowtup, d in pending.pop(bts).items():
                                state[rowtup] += d
                            # Net multiplicity per key across its value variants.
                            key_mult: Counter = Counter()
                            for tup, m in state.items():
                                if m:
                                    key_mult[tup[0]] += m
                            bad = {k: m for k, m in key_mult.items() if m < 0 or m > 1}
                            assert not bad, (
                                f"SUBSCRIBE produced key multiplicity not in {{0,1}}"
                                f" at {bts}: {list(bad.items())[:5]}"
                            )
                            present_keys = frozenset(
                                k for k, m in key_mult.items() if m > 0
                            )
                            res = match(present_keys, floor)
                            if isinstance(res, int):
                                floor = res
                                continue
                            # Evicted window or a discrepancy: abandon this
                            # subscribe and start a fresh one either way.
                            rounds = 999
                            if res is not None:
                                ver, extra, missing = res
                                msg = (
                                    f"SUBSCRIBE key-set matches no recorded version"
                                    f" >= {floor}; closest v{ver}: {len(extra)} extra"
                                    f" keys (stale, not retracted), {len(missing)}"
                                    f" missing (committed rows lost from the stream)."
                                    f" extra e.g. {extra[:8]}, missing e.g. {missing[:8]}"
                                )
                                if missing or not args.prefer_torn:
                                    failure.append(msg)  # torn, or fail-fast mode
                                else:
                                    stale_seen.append(msg)  # keep hunting for torn
                            break
                        if failure or rounds == 999:
                            break
                try:
                    cur.execute("ROLLBACK")
                    cur.connection.close()
                except Exception:
                    pass
            except Exception as e:
                if not flag["stop"]:
                    print(f"verifier retry (ignored): {e}")

    def match(present_keys: frozenset, floor: int):
        """Returns the matched version (int) if the subscribe's key-set equals a
        recorded version >= floor, None if the window was evicted (cannot
        verify), or (closest_version, extra_keys, missing_keys) on a real
        discrepancy."""
        with lock:
            snap = list(history)
        oldest = snap[0][0]
        for ver, keys in snap:
            if ver >= floor and keys == present_keys:
                return ver
        if oldest > floor:
            return None  # window evicted, cannot verify this subscribe further
        best = min(snap, key=lambda vk: len(present_keys ^ vk[1]))
        extra = sorted(present_keys - best[1], key=int)
        missing = sorted(best[1] - present_keys, key=int)
        return (best[0], extra, missing)

    threads = [PropagatingThread(target=writer) for _ in range(args.writers)]
    threads += [PropagatingThread(target=verifier) for _ in range(args.verifiers)]
    for t in threads:
        t.start()
    try:
        while time.time() < deadline and not failure:
            time.sleep(2)
    finally:
        flag["stop"] = True
        for t in threads:
            t.join(timeout=15)

    if failure:
        raise AssertionError(failure[0])
    if stale_seen:
        # --prefer-torn: no torn (missing-rows) instance appeared in time, so
        # report the stale-row discrepancy we did find.
        raise AssertionError(
            f"No torn variant seen in {args.runtime}s; reporting a stale-row"
            f" discrepancy instead:\n{stale_seen[-1]}"
        )
    print(
        f"No divergence found in {args.runtime}s. The race is timing dependent;"
        " increase --runtime and retry."
    )
