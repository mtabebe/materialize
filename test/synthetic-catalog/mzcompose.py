# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Demonstrates the synthetic catalog state toolkit, which populates a catalog with fake
objects, history and statistics so a situation can be modelled instead of built.

Four scenes, each asserting what it showed, so the demo is also the toolkit's regression
net:

1. A catalog far larger than any test would build object by object, in seconds.
2. The two costs a large environment pays at boot, told apart: catalog size on its own,
   then the same fleet plus a dataflow per object.
3. A sink stuck `stalled` with a history and statistics behind it, and no sink.
4. Purge, and the catalog back where it started.

The orphaned sink comes after the restarts on purpose. Its history and statistics live
in the running environment, and the handle to them does not survive a boot, so a scene
that showed them first could not clean them up at the end.

`--objects` and `--dataflows` dial the fleets up past the sizes CI can afford.
"""

import time

import requests

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.mz import Mz

SERVICES = [
    CockroachOrPostgresMetadata(),
    Mz(app_password=""),
    Materialized(
        # Restarting is how scene 2 measures boot, so the sanity restart every
        # composition does by default would just double the work.
        sanity_restart=False,
        additional_system_parameter_defaults={
            # The default is a minute, which is a long time to watch a demo for.
            "storage_statistics_interval": "1s",
        },
    ),
]

MZ_SYSTEM = {"x-materialize-user": "mz_system"}
INTERNAL_HTTP_PORT = 6878

# An object id nothing in the catalog uses, so scene 3's sink really has no sink.
ORPHANED_SINK = "u9999"
ORPHANED_SINK_HISTORY = ["starting", "running", "stalled"]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--objects",
        type=int,
        default=5_000,
        help="how many catalog-only objects scene 1 seeds",
    )
    parser.add_argument(
        "--dataflows",
        type=int,
        default=200,
        help="how many dataflows scene 2 adds on top of that catalog",
    )
    args = parser.parse_args()

    c.up("materialized")
    c.sql(
        "ALTER SYSTEM SET enable_synthetic_catalog_state = on",
        port=6877,
        user="mz_system",
    )
    baseline_objects = count(c, "SELECT count(*) FROM mz_objects")

    scene_large_catalog(c, args.objects)
    scene_hydration(c, args.dataflows)
    scene_orphaned_sink(c)
    scene_cleanup(c, baseline_objects, args.dataflows)


def scene_orphaned_sink(c: Composition) -> None:
    """A sink's history and statistics, with no sink."""
    post(
        c,
        "inject-synthetic-history",
        kind="sink",
        object_id=ORPHANED_SINK,
        statuses=ORPHANED_SINK_HISTORY,
        error="upstream broker unreachable",
    )
    post(
        c,
        "inject-synthetic-stats",
        kind="sink",
        object_id=ORPHANED_SINK,
        replica_id="u1",
        messages_staged=1_000_000,
        messages_committed=999_000,
        bytes_staged=4_000_000,
        bytes_committed=3_996_000,
    )

    wait_until(
        c,
        f"SELECT count(*) FROM mz_internal.mz_sink_status_history "
        f"WHERE sink_id = '{ORPHANED_SINK}'",
        len(ORPHANED_SINK_HISTORY),
    )
    wait_until(
        c,
        f"SELECT count(*) FROM mz_internal.mz_sink_statistics WHERE id = '{ORPHANED_SINK}'",
        1,
    )
    assert (
        count(c, f"SELECT count(*) FROM mz_sinks WHERE id = '{ORPHANED_SINK}'") == 0
    ), "the whole point is that there is no sink"
    assert (
        count(
            c,
            "SELECT count(*) FROM mz_internal.mz_storage_shards "
            f"WHERE object_id = '{ORPHANED_SINK}'",
        )
        == 0
    ), "and nothing running behind it"


def scene_large_catalog(c: Composition, objects: int) -> None:
    """A catalog nobody would build for real."""
    started = time.monotonic()
    post(
        c,
        "inject-synthetic-objects",
        kind="table",
        count=objects,
        database="materialize",
        schema="public",
        name_prefix="synthetic",
        columns=4,
        cluster="quickstart",
    )
    print(f"seeded {objects} tables in {time.monotonic() - started:.1f}s")

    assert (
        count(c, "SELECT count(*) FROM mz_tables WHERE name LIKE 'synthetic%'")
        == objects
    )
    # The catalog readers are what a large catalog slows down, so ask one of each shape
    # rather than only counting.
    for query in [
        "SELECT count(*) FROM mz_objects",
        "SELECT count(*) FROM mz_columns",
        "SELECT count(*) FROM mz_internal.mz_object_dependencies",
    ]:
        started = time.monotonic()
        c.sql_query(query)
        print(f"{query} answered in {time.monotonic() - started:.1f}s")


def scene_hydration(c: Composition, dataflows: int) -> None:
    """The two costs of a large environment's boot, told apart."""
    catalog_only = restart_and_time(c)

    # A shipped dataflow needs a real, empty input to hydrate over. Injecting these
    # synthetically needs the offline tool, which has no container image, so the demo
    # creates them directly: at boot they are the same objects either way.
    c.sql("CREATE TABLE hydration_input (a int)")
    for i in range(dataflows):
        c.sql(
            f"CREATE INDEX hydration_idx_{i} IN CLUSTER quickstart ON hydration_input (a)"
        )
    with_dataflows = restart_and_time(c)

    # What the scene asserts is that the dataflows were really shipped, not what they
    # cost. The timings are reported rather than gated: at a fleet size CI can afford,
    # the difference between the two boots sits inside the noise of a boot, so an
    # ordering assertion would be a flake rather than a measurement. Dial --dataflows up
    # to watch the second number pull away from the first.
    installed = count(
        c,
        "SELECT count(*) FROM mz_catalog.mz_cluster_replica_frontiers f "
        "JOIN mz_internal.mz_object_global_ids g ON g.global_id = f.object_id "
        "JOIN mz_objects o ON o.id = g.id "
        "WHERE o.name LIKE 'hydration_idx%'",
    )
    assert (
        installed == dataflows
    ), f"{installed} of {dataflows} dataflows were installed"

    print(
        f"boot on catalog size alone: {catalog_only:.1f}s; "
        f"with {dataflows} shipped dataflows as well: {with_dataflows:.1f}s"
    )


def scene_cleanup(c: Composition, baseline_objects: int, dataflows: int) -> None:
    """Back to where the demo started."""
    report = post(c, "purge-synthetic")
    print(f"purged {report}")
    assert report["objects"] > 0, report

    # Scene 2's dataflow fleet is real, so purge leaves it alone.
    for i in range(dataflows):
        c.sql(f"DROP INDEX hydration_idx_{i}")
    c.sql("DROP TABLE hydration_input")

    assert count(c, "SELECT count(*) FROM mz_objects") == baseline_objects
    wait_until(
        c,
        f"SELECT count(*) FROM mz_internal.mz_sink_status_history "
        f"WHERE sink_id = '{ORPHANED_SINK}'",
        0,
    )
    wait_until(
        c,
        f"SELECT count(*) FROM mz_internal.mz_sink_statistics WHERE id = '{ORPHANED_SINK}'",
        0,
    )


def post(c: Composition, route: str, **request) -> dict:
    """Calls one of the toolkit's internal-HTTP routes."""
    response = requests.post(
        f"http://localhost:{c.port('materialized', INTERNAL_HTTP_PORT)}/api/catalog/{route}",
        headers=MZ_SYSTEM,
        json=request or None,
        timeout=600,
    )
    assert response.status_code == 200, response.text
    return response.json() if response.text else {}


def count(c: Composition, query: str) -> int:
    return c.sql_query(query)[0][0]


def wait_until(c: Composition, query: str, expected: int) -> None:
    """Waits for a count to reach `expected`, which injection reaches asynchronously."""
    deadline = time.monotonic() + 120
    while True:
        actual = count(c, query)
        if actual == expected:
            return
        assert (
            time.monotonic() < deadline
        ), f"{query} stuck at {actual}, want {expected}"
        time.sleep(1)


def restart_and_time(c: Composition) -> float:
    """Restarts twice and times the second, returning seconds to answer a query again.

    The boot right after a catalog changes pays one-time costs, populating the
    expression cache and warming persist, that are larger at these fleet sizes than
    anything being compared. Discarding it leaves both measurements warm.
    """
    for _ in range(2):
        c.kill("materialized")
        started = time.monotonic()
        c.up("materialized")
        c.sql_query("SELECT count(*) FROM mz_objects")
    return time.monotonic() - started
