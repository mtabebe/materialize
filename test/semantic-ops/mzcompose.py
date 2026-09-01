# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The semantic-operators demo, and an end-to-end test of the same chain.

The chain: a row lands in `tickets_raw`; it shows up in `tickets_pending` because
nothing in the store answers it; the worker's `SUBSCRIBE` delivers it; the worker
calls the provider and inserts the label; `tickets.severity` fills in and the row
leaves the queue; `revenue_at_risk` moves.

Materialize does not make the provider call at any point. A SQL statement is what
causes the call transitively, but the engine process never talks to a provider.
"""

import sys
import time
from pathlib import Path
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

sys.path.append(str(Path(__file__).parent))

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={"enable_semantic_operators": "true"},
    ),
]


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_end_to_end(c: Composition) -> None:
    """Runs the whole chain against the mock provider and asserts each beat."""
    from demo import BODIES, Demo
    from providers import MockProvider
    from worker import Worker

    c.up("materialized")
    url = f"postgres://materialize@localhost:{c.default_port('materialized')}/materialize"

    seed = (Path(__file__).parent / "seed.sql").read_text()
    with c.sql_cursor() as cur:
        for statement in _split(seed):
            cur.execute(statement)

    # Chaotic, because beat 2 only proves something if the model *would* have
    # answered differently the second time.
    worker = Worker(url, "tickets", MockProvider(mode="chaotic", seed=0))
    demo = Demo(url, settle=1.0)

    subscribe = worker.open_subscribe()

    def drain(deadline: float = 30.0) -> None:
        """Runs the worker loop by hand until the queue is empty."""
        until = time.time() + deadline
        while time.time() < until:
            for body in worker.poll(subscribe):
                worker.write(body, worker.enrich(body))
            if demo.pending() == 0:
                return
        raise AssertionError("the pending view never drained")

    # Beat 1: a row is visible with NULL labels before its answer arrives.
    demo.sql("INSERT INTO tickets_raw VALUES (%s, 'open')", BODIES[0])
    assert demo.one("SELECT severity FROM tickets WHERE body = %s", BODIES[0]) is None
    assert demo.pending() == 1
    drain()
    first_label = demo.one("SELECT severity FROM tickets WHERE body = %s", BODIES[0])
    assert first_label is not None
    assert demo.risk()[0] >= 0

    # Beat 2: delete and re-insert. The store holds the answer, so the model is
    # not called again and the label does not drift.
    calls = worker.calls
    demo.sql("DELETE FROM tickets_raw WHERE body = %s", BODIES[0])
    demo.sql("INSERT INTO tickets_raw VALUES (%s, 'open')", BODIES[0])
    drain()
    assert worker.calls == calls, "a re-inserted row paid for a second call"
    assert demo.one("SELECT severity FROM tickets WHERE body = %s", BODIES[0]) == first_label

    # Beat 3: 500 rows with one body are one unit of work.
    calls = worker.calls
    demo.sql(
        "INSERT INTO tickets_raw SELECT %s, 'open' FROM generate_series(1, 500)",
        BODIES[1],
    )
    assert demo.pending() == 1, "the queue is keyed on the input, not on the row"
    drain()
    assert worker.calls == calls + len(worker.specs)

    # Beats 4 and 5: churn on unenriched data moves the views and not the counter.
    calls = worker.calls
    demo.sql("UPDATE accounts SET arr = 1_600_000 WHERE name = 'Acme'")
    demo.sql("UPDATE tickets_raw SET status = 'closed'")
    demo.sql("UPDATE tickets_raw SET status = 'open'")
    drain()
    assert worker.calls == calls, "churn on an unenriched column called the model"

    worker.close()


def workflow_demo(c: Composition) -> None:
    """Brings up Materialize and the worker for a live run of the five beats."""
    c.up("materialized")
    port = c.default_port("materialized")
    url = f"postgres://materialize@localhost:{port}/materialize"

    seed = (Path(__file__).parent / "seed.sql").read_text()
    with c.sql_cursor() as cur:
        for statement in _split(seed):
            cur.execute(statement)

    print(
        dedent(
            f"""
            Materialize is up on port {port}.

              psql "{url}"
              python3 test/semantic-ops/worker.py --url "{url}" --source tickets --mode chaotic
              python3 test/semantic-ops/demo.py --url "{url}"

            Rows are inserted into `tickets_raw`, not `tickets`: the declared name
            belongs to a view over the relation and the store.
            """
        )
    )


def _split(sql: str) -> list[str]:
    """Splits a seed file into statements.

    Comments are stripped *before* splitting, not after: a `;` inside a comment
    would otherwise cut a statement in half and leave two fragments that neither
    parse nor look like comments. Splitting on `;` is then enough, since the seed
    file has no semicolons inside string literals.
    """
    stripped = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return [s.strip() for s in stripped.split(";") if s.strip()]
