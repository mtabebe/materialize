# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Exercises the synthetic catalog state toolkit, which populates a catalog with fake
objects, history and statistics so a situation can be modelled without building it.

`scale` seeds a catalog far larger than any test would create object by object, and
checks that ordinary catalog queries still answer.

`hydration` separates the two costs a large environment pays at boot: catalog size,
which the toolkit's metadata-only objects model on their own, and per-dataflow
bootstrap. It reports both so the difference between them is visible. The dataflow fleet
is created with real `CREATE INDEX` over one empty table, which is what a Tier 1
synthetic object becomes at boot anyway; the toolkit's contribution here is the catalog
half, which is the expensive half to build for real.
"""

import time

import requests

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.mz import Mz

SERVICES = [
    CockroachOrPostgresMetadata(),
    Mz(app_password=""),
    # Restarting is how the harness below measures boot, so the sanity restart every
    # composition does by default would just double the work.
    Materialized(sanity_restart=False),
]

# Big enough that catalog size is the dominant cost, small enough for CI.
SCALE_OBJECTS = 5_000
# Both halves of the comparison run at this count, so the only difference between the two
# boots is whether the objects ship a dataflow.
HYDRATION_OBJECTS = 200

MZ_SYSTEM = {"x-materialize-user": "mz_system"}


def inject(c: Composition, **request) -> None:
    """Creates synthetic objects over the internal HTTP port."""
    response = requests.post(
        f"http://localhost:{c.port('materialized', 6878)}"
        "/api/catalog/inject-synthetic-objects",
        headers=MZ_SYSTEM,
        json=request,
        timeout=600,
    )
    assert response.status_code == 200, response.text


def declare_disposable(c: Composition) -> None:
    c.sql(
        "ALTER SYSTEM SET enable_synthetic_catalog_state = on",
        port=6877,
        user="mz_system",
    )


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_scale(c: Composition) -> None:
    c.up("materialized")
    declare_disposable(c)

    started = time.monotonic()
    inject(
        c,
        kind="table",
        count=SCALE_OBJECTS,
        database="materialize",
        schema="public",
        name_prefix="synthetic",
        columns=4,
        cluster="quickstart",
    )
    print(f"seeded {SCALE_OBJECTS} tables in {time.monotonic() - started:.1f}s")

    assert (
        c.sql_query("SELECT count(*) FROM mz_tables WHERE name LIKE 'synthetic%'")[0][0]
        == SCALE_OBJECTS
    )
    # The readers that derive from the catalog are the ones a large catalog slows down,
    # so query one of each shape rather than only counting.
    for query in [
        "SELECT count(*) FROM mz_objects",
        "SELECT count(*) FROM mz_columns",
        "SELECT count(*) FROM mz_internal.mz_object_dependencies",
        "SHOW TABLES",
    ]:
        c.sql_query(query)

    purged = requests.post(
        f"http://localhost:{c.port('materialized', 6878)}/api/catalog/purge-synthetic",
        headers=MZ_SYSTEM,
        timeout=600,
    )
    assert purged.status_code == 200, purged.text
    assert purged.json()["objects"] == SCALE_OBJECTS, purged.text
    assert (
        c.sql_query("SELECT count(*) FROM mz_tables WHERE name LIKE 'synthetic%'")[0][0]
        == 0
    )


def workflow_hydration(c: Composition) -> None:
    """Times a boot with each tier's fleet, so the two costs can be told apart."""
    c.up("materialized")
    declare_disposable(c)

    # Tier 0: catalog rows and nothing else.
    inject(
        c,
        kind="table",
        count=HYDRATION_OBJECTS,
        database="materialize",
        schema="public",
        name_prefix="tier0",
        columns=4,
        cluster="quickstart",
    )
    tier0_seconds = restart_and_time(c)

    # A shipped dataflow needs a real, empty input to hydrate over. Injecting these
    # synthetically needs the offline tool, which has no container image, so the harness
    # creates them directly: at boot they are the same objects either way.
    c.sql(
        "CREATE TABLE hydration_input (a int)",
        port=6877,
        user="mz_system",
    )
    for i in range(HYDRATION_OBJECTS):
        c.sql(
            f"CREATE INDEX hydration_idx_{i} IN CLUSTER quickstart ON hydration_input (a)",
            port=6877,
            user="mz_system",
        )
    tier1_seconds = restart_and_time(c)

    print(
        f"boot with {HYDRATION_OBJECTS} catalog-only objects: {tier0_seconds:.1f}s; "
        f"with {HYDRATION_OBJECTS} shipped dataflows as well: {tier1_seconds:.1f}s"
    )
    assert tier1_seconds > tier0_seconds, (
        "shipping a dataflow per object is supposed to cost more than the catalog row "
        f"alone: {tier1_seconds:.1f}s vs {tier0_seconds:.1f}s"
    )


def restart_and_time(c: Composition) -> float:
    """Restarts and returns how long it took to answer a catalog query again."""
    c.kill("materialized")
    started = time.monotonic()
    c.up("materialized")
    c.sql_query("SELECT count(*) FROM mz_objects")
    return time.monotonic() - started
