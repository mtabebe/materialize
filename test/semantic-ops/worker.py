# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The enrichment worker: the half of semantic operators that lives outside Materialize.

`CREATE SOURCE ... ENRICH WITH (...)` expands into six catalog items. Three of them
are this program's interface:

    <source>_ai_spec       what to compute, read once at startup
    <source>_pending       what still needs computing, tailed with SUBSCRIBE
    <source>_ai_store_raw  where answers go

Nothing here is Materialize-specific beyond `SUBSCRIBE`. A third-party service
would run the same loop and POST to a webhook source instead of inserting.

Two properties of `SUBSCRIBE` shape the whole loop and are easy to get wrong:

1. It is a change stream, not a queue. Nothing acks, and nothing redrives. If this
   process takes a row and dies before writing a result, `<source>_pending` has not
   changed, so that row is never re-delivered. **In-process retry is therefore
   mandatory, not a nicety.**

2. It snapshots before it tails. A restarting worker therefore sees every
   outstanding input, which is why the loop self-heals across restarts even though
   it cannot within one.

Act only on `mz_diff > 0`. A retraction means the input was answered, by this
worker or a concurrent one, and paying for it again would be pure waste.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
from typing import Any

import psycopg

from providers import EnrichmentSpec, Provider, ProviderError, make_provider


class Worker:
    def __init__(
        self,
        url: str,
        source: str,
        provider: Provider,
        *,
        fetch_timeout: str = "1s",
        max_retries: int = 4,
        log=sys.stderr,
    ):
        self.url = url
        self.source = source
        self.provider = provider
        self.fetch_timeout = fetch_timeout
        self.max_retries = max_retries
        self.log_file = log
        self.calls = 0
        self.count_calls = True
        self._stop = threading.Event()

        # Derived by the same rule the planner used. The worker is handed the name
        # the user typed and nothing else.
        self.pending_view = f"{source}_pending"
        self.spec_view = f"{source}_ai_spec"
        self.store_table = f"{source}_ai_store_raw"

        # Two connections, deliberately. The tailing one is parked inside
        # `BEGIN; DECLARE c CURSOR ...` for the life of the process and cannot also
        # run the INSERTs, so results go out over a second one.
        self.tail_conn = psycopg.connect(url)
        self.write_conn = psycopg.connect(url, autocommit=True)

        self.specs = self._read_specs()

    # -- setup ---------------------------------------------------------------

    def log(self, msg: str) -> None:
        print(f"[worker] {msg}", file=self.log_file, flush=True)

    def _read_specs(self) -> list[EnrichmentSpec]:
        with self.write_conn.cursor() as cur:
            cur.execute(
                f"SELECT column_name, kind, input_column, prompt, labels, prompt_version"
                f" FROM {_qualify(self.spec_view)} ORDER BY column_name"
            )
            specs = [
                EnrichmentSpec(
                    column_name=r[0],
                    kind=r[1],
                    input_column=r[2],
                    prompt=r[3],
                    labels=list(r[4]) if r[4] is not None else None,
                    prompt_version=r[5],
                )
                for r in cur.fetchall()
            ]
        if not specs:
            raise RuntimeError(f"{self.spec_view} is empty; is this source enriched?")
        versions = {s.prompt_version for s in specs}
        if len(versions) != 1:
            raise RuntimeError(f"mixed prompt versions in {self.spec_view}: {versions}")
        self.prompt_version = versions.pop()
        self.log(
            f"specs: {', '.join(f'{s.column_name}={s.kind}' for s in specs)}"
            f" @ {self.prompt_version}"
        )
        return specs

    # -- reading work --------------------------------------------------------

    def open_subscribe(self) -> psycopg.Cursor:
        cur = self.tail_conn.cursor()
        cur.execute("BEGIN")
        cur.execute(f"DECLARE c CURSOR FOR SUBSCRIBE {_qualify(self.pending_view)}")
        return cur

    def poll(self, cur: psycopg.Cursor) -> list[str]:
        """One FETCH, reduced to the inputs that still want an answer."""
        cur.execute(f"FETCH ALL c WITH (TIMEOUT = '{self.fetch_timeout}')")
        inputs: list[str] = []
        for _ts, diff, input_ in cur.fetchall():
            if diff > 0:
                inputs.append(input_)
        # Dedupe within the batch: one FETCH can span several timestamps, and the
        # same input appearing at two of them would otherwise be two paid calls.
        return list(dict.fromkeys(inputs))

    # -- calling the provider ------------------------------------------------

    def enrich(self, body: str) -> dict[str, Any]:
        """Compute every enriched column for one body.

        One store per source, so one pass here fills every column at once. A
        failure that survives retry becomes an `error` key rather than an
        exception: the input must leave `<source>_pending` either way, or the
        worker livelocks on one bad body.
        """
        out: dict[str, Any] = {}
        for spec in self.specs:
            try:
                out[spec.column_name] = _encode(spec, self._call_with_retry(spec, body))
            except ProviderError as e:
                out[spec.column_name] = None
                out.setdefault("error", {})[spec.column_name] = str(e)
        return out

    def _call_with_retry(self, spec: EnrichmentSpec, body: str) -> Any:
        delay = 0.25
        last: Exception | None = None
        for _attempt in range(self.max_retries):
            try:
                value = self.provider.call(spec, body)
            except ProviderError as e:
                last = e
                # No server-side redrive exists, so backing off here is the only
                # thing standing between a transient 429 and a permanently lost row.
                time.sleep(delay + random.random() * delay)
                delay *= 2
                continue
            self.calls += 1
            self._record_call(spec, body)
            if spec.kind == "classify" and spec.labels and value not in spec.labels:
                # A parse failure, not a transport failure. Retrying cannot help.
                raise ProviderError(f"{value!r} is not one of {spec.labels}")
            return value
        raise ProviderError(f"{self.max_retries} attempts failed: {last}")

    def _record_call(self, spec: EnrichmentSpec, body: str) -> None:
        """Records one paid call, if the demo's counter table exists.

        `model_calls` is part of the demo's schema rather than of the expansion, so
        a worker pointed at a plain enriched relation has nowhere to write. Missing
        the counter is not a reason to stop answering, so the first failure disables
        it for the rest of the run.
        """
        if not self.count_calls:
            return
        try:
            with self.write_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO model_calls (input, column_name, at) VALUES (%s, %s, now())",
                    (body, spec.column_name),
                )
        except psycopg.Error as e:
            self.count_calls = False
            self.log(f"not counting calls: {e}")

    # -- writing results -----------------------------------------------------

    def write(self, body: str, output: dict[str, Any]) -> None:
        """Insert one answer.

        The store is append-only, so a retry that writes the same input twice
        leaves two rows. The generated `<source>_ai_store` view absorbs that with
        `DISTINCT ON (input, prompt_version)`, and `<source>_pending` anti-joins
        against that view rather than this table for exactly that reason.

        The webhook variant of this write is:

            POST /api/webhook/<database>/<schema>/<source>_labels
            {"input": ..., "prompt_version": ..., "output": {...}}

        which is how a third-party service delivers results without ever holding
        database credentials. The demo uses the direct INSERT because it is one
        fewer moving part on stage.
        """
        with self.write_conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {_qualify(self.store_table)}"
                f" (input, prompt_version, output, computed_at)"
                f" VALUES (%s, %s, %s::jsonb, now())",
                (body, self.prompt_version, json.dumps(output)),
            )

    # -- the loop ------------------------------------------------------------

    def run(self) -> None:
        cur = self.open_subscribe()
        self.log(f"tailing {self.pending_view}")
        while not self._stop.is_set():
            try:
                batch = self.poll(cur)
            except psycopg.Error as e:
                self.log(f"subscribe failed, reopening: {e}")
                self.tail_conn.close()
                self.tail_conn = psycopg.connect(self.url)
                cur = self.open_subscribe()
                continue
            for body in batch:
                output = self.enrich(body)
                self.write(body, output)
            if batch:
                self.log(f"answered {len(batch)} input(s), {self.calls} call(s) total")

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        for conn in (self.tail_conn, self.write_conn):
            try:
                conn.close()
            except Exception:
                pass


def _encode(spec: EnrichmentSpec, value: Any) -> Any:
    """Renders one answer the way the generated view expects to read it back.

    The enriched view projects `(output ->> 'col')::<type>`, and `->>` yields text.
    For everything but `embed` the JSON scalar already casts correctly. An embedding
    would arrive as `[0.1, 0.2]`, which is JSON array syntax and not a Postgres array
    literal, so it is written as `{0.1,0.2}` instead.
    """
    if spec.kind == "embed" and isinstance(value, list):
        return "{" + ",".join(repr(float(v)) for v in value) + "}"
    return value


def _qualify(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="postgres://materialize@localhost:6875/materialize")
    p.add_argument("--source", required=True, help="the name given to CREATE SOURCE")
    p.add_argument("--provider", default="mock", choices=["mock", "anthropic"])
    p.add_argument("--mode", default="deterministic", choices=["deterministic", "chaotic"])
    args = p.parse_args()

    worker = Worker(args.url, args.source, make_provider(args.provider, args.mode))
    try:
        worker.run()
    except KeyboardInterrupt:
        pass
    finally:
        worker.close()


if __name__ == "__main__":
    main()
