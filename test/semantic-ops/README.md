# Semantic operators demo

The escalation radar: which of our revenue is sitting behind an open sev1, where
`severity` and the customer's name are read out of the ticket body by a model.

**Materialize makes no model call.** The planner reads an `ENRICH WITH` expression
to learn the input column, the kind, the arguments and the output type, then
rewrites it away. `worker.py` makes the call, holds the credential, and pays for
the tokens. A SQL statement is what causes the call transitively; the engine
process never talks to a provider.

## What `ENRICH WITH` expands into

`CREATE TABLE tickets (body text, status text) ENRICH WITH (severity = ..., account_name = ...)`
becomes six catalog items:

| Name | Kind | Purpose |
|---|---|---|
| `tickets_raw` | table | rows land here |
| `tickets_ai_store_raw` | table | append-only; the worker writes here |
| `tickets_ai_store` | view | `DISTINCT ON` dedup over the store |
| `tickets_pending` | view | the work queue: an anti-join of raw against the store |
| `tickets_ai_spec` | view | what to compute, for the worker to read |
| `tickets` | view | what you query |

**Insert into `tickets_raw`, not `tickets`.** The declared name belongs to the view
that joins the relation to the store, which is how enriched columns exist without
the engine computing anything. That is the one ergonomic wart the design buys.

## Running it

```
bin/environmentd --optimized --reset &
psql -p 6875 -h localhost -U materialize -f test/semantic-ops/seed.sql
python3 test/semantic-ops/worker.py --source tickets --mode chaotic &
python3 test/semantic-ops/demo.py
```

Or through mzcompose, which brings up Materialize and seeds it:

```
bin/mzcompose --find semantic-ops run demo          # interactive
bin/mzcompose --find semantic-ops run end-to-end    # asserted, mock provider
```

`enable_semantic_operators` must be on. It is on by default in every test
configuration and off in production.

## Run the worker chaotic

`--mode chaotic` returns a random label for any body the process has not seen
before. Beat 2 deletes a ticket and re-inserts it and shows the label is identical;
that only proves the store is doing the work if the model *would* have answered
differently. A mock that always returns the same thing makes the store look
decorative.

Beat 1 is the one worth running against a real provider (`--provider anthropic`,
`ANTHROPIC_API_KEY` set), because that is where a real label is persuasive. Beats 2
through 5 prove mechanism, and a live model there only adds flakiness.

## The one thing to know about the worker

`SUBSCRIBE` is a change stream, not a queue. Nothing acks and nothing redrives, so
a row taken by a worker that then dies is not re-delivered: `tickets_pending` has
not changed. In-process retry is mandatory rather than a nicety.

It self-heals across restarts, though, because `SUBSCRIBE` snapshots current
contents before it tails. That is why the closer works: stop the worker, insert
tickets, watch the view serve with NULL labels, restart, watch it catch up.

## Not built

Drop cascade: dropping `tickets_raw` leaves the other five objects behind.
Connections and secrets, spend caps, rate limiting, introspection, and any async
inside Materialize are all out of scope by decision, not by oversight.
