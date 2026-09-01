# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The escalation-radar demo: five beats plus a closer.

Each beat is a state change and a number to watch beside it. Three of the five
beats are "this number does not move", which is why the call counter is a table
rather than a log line: the counter and the views sit in one session, so the claim
and the evidence appear together.

Run against a local `environmentd`:

    bin/environmentd --optimized --reset &
    psql -p 6875 -h localhost -U materialize -f test/semantic-ops/seed.sql
    python3 test/semantic-ops/worker.py --source tickets --mode chaotic &
    python3 test/semantic-ops/demo.py

`--mode chaotic` is not a detail. Beat 2 re-inserts a deleted ticket and shows the
label is identical; that only proves something if the model *would* have answered
differently.
"""

from __future__ import annotations

import argparse
import sys
import time

import psycopg

URL = "postgres://materialize@localhost:6875/materialize"

BODIES = [
    "Acme reports the production cluster is down and they cannot ingest",
    "Globex says dashboards are slow since this morning",
    "Initech asked whether we support a new export format",
]


class Demo:
    def __init__(self, url: str, settle: float = 3.0):
        self.conn = psycopg.connect(url, autocommit=True)
        self.settle = settle
        self.beat = 0

    def sql(self, statement: str, *args) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(statement, args or None)
            return cur.fetchall() if cur.description else []

    def one(self, statement: str, *args):
        rows = self.sql(statement, *args)
        return rows[0][0] if rows else None

    def calls(self) -> int:
        return self.one("SELECT count(*) FROM model_calls")

    def pending(self) -> int:
        return self.one("SELECT count(*) FROM tickets_pending")

    def risk(self) -> tuple:
        return self.sql("SELECT open_sev1, arr_at_risk FROM revenue_at_risk")[0]

    def wait_for_labels(self, timeout: float = 30.0) -> None:
        """Blocks until the queue drains, which is when the worker has caught up."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.pending() == 0:
                return
            time.sleep(0.25)
        print("  (timed out waiting for the worker; is it running?)", file=sys.stderr)

    def say(self, title: str) -> None:
        self.beat += 1
        print(f"\n=== Beat {self.beat}: {title}")

    def report(self) -> None:
        sev1, arr = self.risk()
        print(
            f"  pending={self.pending()}  model_calls={self.calls()}"
            f"  open_sev1={sev1}  arr_at_risk={arr}"
        )

    # -- the beats -----------------------------------------------------------

    def beat_one_ticket(self) -> None:
        self.say("one ticket arrives")
        self.sql("INSERT INTO tickets_raw VALUES (%s, 'open')", BODIES[0])
        print("  right after insert, before the worker has answered:")
        self.report()
        print(
            "  ",
            self.sql("SELECT severity, account_name FROM tickets WHERE body = %s", BODIES[0]),
        )
        self.wait_for_labels()
        print("  once the label lands:")
        self.report()
        print(
            "  ",
            self.sql("SELECT severity, account_name FROM tickets WHERE body = %s", BODIES[0]),
        )

    def beat_delete_reinsert(self) -> None:
        self.say("delete the ticket and re-insert it (run the worker chaotic)")
        before_calls = self.calls()
        before_label = self.one("SELECT severity FROM tickets WHERE body = %s", BODIES[0])
        self.sql("DELETE FROM tickets_raw WHERE body = %s", BODIES[0])
        self.sql("INSERT INTO tickets_raw VALUES (%s, 'open')", BODIES[0])
        time.sleep(self.settle)
        after_label = self.one("SELECT severity FROM tickets WHERE body = %s", BODIES[0])
        print(f"  label before={before_label} after={after_label}")
        print(f"  model_calls {before_calls} -> {self.calls()}")
        assert before_label == after_label, "the store did not hold the answer"
        assert before_calls == self.calls(), "the model was called again"
        self.report()

    def beat_five_hundred(self) -> None:
        self.say("500 tickets with one body")
        before = self.calls()
        self.sql(
            "INSERT INTO tickets_raw SELECT %s, 'open' FROM generate_series(1, 500)",
            BODIES[1],
        )
        print(f"  pending rows for 500 tickets: {self.pending()}")
        self.wait_for_labels()
        print(f"  model_calls {before} -> {self.calls()}")
        self.report()

    def beat_arr_updates(self) -> None:
        self.say("stream ARR updates; the labels are untouched")
        before = self.calls()
        for arr in (1_300_000, 1_450_000, 1_600_000):
            self.sql("UPDATE accounts SET arr = %s WHERE name = 'Acme'", arr)
            time.sleep(0.3)
            sev1, at_risk = self.risk()
            print(f"  arr={arr} -> arr_at_risk={at_risk}")
        print(f"  model_calls {before} -> {self.calls()}")

    def beat_status_churn(self) -> None:
        self.say("bulk-update status; the views churn and the counter does not")
        before = self.calls()
        self.sql("UPDATE tickets_raw SET status = 'closed'")
        time.sleep(self.settle)
        self.report()
        self.sql("UPDATE tickets_raw SET status = 'open'")
        time.sleep(self.settle)
        self.report()
        print(f"  model_calls {before} -> {self.calls()}")

    def closer(self) -> None:
        print("\n=== Closer: stop the worker, insert, restart it")
        print("  Stop the worker now, then press enter.")
        input()
        self.sql("INSERT INTO tickets_raw VALUES (%s, 'open')", BODIES[2])
        print("  the view still serves, with NULL labels:")
        print(
            "  ",
            self.sql("SELECT body, severity FROM tickets WHERE body = %s", BODIES[2]),
        )
        print(f"  pending={self.pending()}")
        print(
            "  Restart the worker. SUBSCRIBE snapshots before it tails, so it sees\n"
            "  every outstanding input rather than only what changes next. Press enter."
        )
        input()
        self.wait_for_labels()
        print(
            "  ",
            self.sql("SELECT body, severity FROM tickets WHERE body = %s", BODIES[2]),
        )

    def run(self, interactive: bool) -> None:
        self.beat_one_ticket()
        self.beat_delete_reinsert()
        self.beat_five_hundred()
        self.beat_arr_updates()
        self.beat_status_churn()
        if interactive:
            self.closer()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default=URL)
    p.add_argument(
        "--no-closer",
        action="store_true",
        help="skip the closer, which needs someone to stop and start the worker",
    )
    args = p.parse_args()
    Demo(args.url).run(interactive=not args.no_closer)


if __name__ == "__main__":
    main()
