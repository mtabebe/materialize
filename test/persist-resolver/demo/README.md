# Persist resolver demo

PROTOTYPE. A five-minute demo of the persist resolver
(`src/persist-resolver`), with the same steps on classic persist for contrast:

1. Materialize works as usual: a table, a materialized view, `SELECT`, `SUBSCRIBE`.
2. With the resolver, the user collections' shards have no row in consensus. Only
   the catalog, builtin, txns and the resolver's two manifest shards do.
3. The resolver's JSON view shows each data shard's upper, since, read intents
   and batches, and the partition's seqno.
4. On classic persist, every user shard has its own consensus row, and its seqno
   climbs with every write.

## Prerequisites

- CockroachDB on `localhost:26257`, insecure, for consensus and the timestamp
  oracle. `demo.sh up` drops and recreates the `consensus`, `tsoracle` and
  `storage` schemas of database `materialize`.

  ```
  docker run -d --name cockroach -p 127.0.0.1:26257:26257 \
      cockroachdb/cockroach:v23.1.11 start-single-node --insecure
  ```

- `psql`, `jq`, `curl`.
- Dev builds of the three binaries:

  ```
  cargo build --bin environmentd --bin clusterd --bin persist-resolver
  ```

  Set `PROFILE=release` (or `optimized`) to run binaries from another profile's
  target directory. The dev build is quick enough: statements take 0.1-0.3 s and
  `up` takes about 30 s, almost all of it environmentd booting.

`demo.sh up` wipes `mzdata/` in the repository root, as `bin/environmentd --reset`
would. Logs go to `scratch/persist-resolver-demo/`.

## Running it

Bring a mode up before the recording starts, then run one step at a time. Each
step prints the SQL, `curl` or shell command before its output.

```
test/persist-resolver/demo/demo.sh up resolver   # ~30 s; wipes everything first
test/persist-resolver/demo/demo.sh step 1
test/persist-resolver/demo/demo.sh step 2
test/persist-resolver/demo/demo.sh step 3
test/persist-resolver/demo/demo.sh step 4
test/persist-resolver/demo/demo.sh step 5
test/persist-resolver/demo/demo.sh up classic    # stops the resolver run, starts fresh
test/persist-resolver/demo/demo.sh step 1        # ... through step 5 again
test/persist-resolver/demo/demo.sh down
```

`demo.sh all` runs steps 1-5 in a row. `rehearse.sh` runs `up` and `all` for both
modes and fails if a process died, a log has a panic, or the transcript lacks
what the demo shows (carol's row in the SUBSCRIBE, the user shards' consensus
rows or their absence, the resolver's JSON). Rerun it before recording.

Knobs: `TICKS` (step 5's inserts, default 10), `SUBSCRIBE_SECS` (step 2, default 6).

### What to point out

| Step | Resolver | Classic |
|---|---|---|
| 1. Create `accounts` and the MV `balances`, insert three rows | Ordinary SQL. The two user collections are registered with the resolver. | Same SQL. |
| 2. `SELECT`, then `SUBSCRIBE` while another session inserts | The subscription sees the snapshot, then carol's row about two seconds in: reads, including listens served from clusterd, follow the data shards through the resolver. | Same output. |
| 3. Consensus, by shard | 104 shards with rows: 98 builtin, 4 others (catalog, txns), and 2 manifest shards. `accounts` and `balances` have **no consensus row**. | 104 shards: 98 builtin, 4 others, and the 2 user shards, each with its own rows and seqno. |
| 4. The resolver's view | `GET /views`: the partition `p0`, its seqno, both manifest shards and their manifest times, the claim. `GET /views/<shard>`: the table's upper, since, read intents (the storage controller's critical reader with its opaque, a leased reader, ...) and its batches with update and part counts. | No resolver. The table's state is a versioned blob in consensus: one row per seqno, a few hundred bytes each. |
| 5. Insert once a second, watching consensus | `accounts` stays absent from consensus. The partition manifest shard's seqno climbs instead, a few per tick, and the resolver's view seqno and the table's upper advance with it. | `accounts`'s seqno climbs by one per insert. |

Two different seqnos appear in resolver mode. The **view seqno** in `/views`
(`seqno`, `view_seqno=` in step 5) is the sum of the two manifest shards' manifest
times, so it moves once per resolver write. The **consensus seqno** of the
partition manifest shard is persist's own for that shard and also counts state
changes that aren't resolver writes, so it runs ahead. Both move while idle too,
because the table's upper advances about once a second: over 30 idle seconds one
run saw the view seqno go 23 to 56 and the consensus seqno 24 to 62.

### Two-pane layout

Left pane: the presenter runs the steps. Right pane, resolver mode: the
partition's view refreshing, so it moves during step 5.

```
tmux new-session -s demo \; split-window -h \; send-keys \
    "watch -n1 \"curl -s 127.0.0.1:6891/views | jq -c '{seqno, partition_manifest, shards: [.data_shards[] | {shard, upper: .upper[0], batches: (.batches | length)}]}'\"" C-m \; select-pane -L
```

Classic mode, right pane: the user shards' consensus rows.

```
watch -n1 "psql postgres://root@localhost:26257/materialize -c 'SELECT shard, count(*), max(sequence_number) FROM consensus.consensus GROUP BY shard ORDER BY 3 DESC LIMIT 6'"
```

## Known issues

- In a dev build, `FETCH` from a `DECLARE ... CURSOR FOR SUBSCRIBE` overflowed a
  3 MiB tokio worker stack in environmentd and aborted it, in 3 of 3 classic-mode
  tries. The one resolver-mode try didn't crash. The overflowing stack is
  pgwire's `fetch` and the adapter's frontend peek path down to `plan_subscribe`,
  not persist. Step 2 uses `COPY (SUBSCRIBE ...) TO STDOUT` instead, which
  streams rows live in a terminal.
- Only user collections are data shards; the prototype's cuts are listed in its
  plan.

## Captured output

From `rehearse.sh` on a dev build. Shard ids, timestamps and seqnos differ per
run, and the seqno increments in step 5 vary with how writes fall into cycles.

<details><summary>resolver mode</summary>

```

== 1. Create a table and a materialized view, and write to them (resolver)
materialize=> CREATE TABLE accounts (id int, owner text, balance int)
materialize=> CREATE MATERIALIZED VIEW balances AS SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner
materialize=> INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25)

== 2. Read it back: SELECT, and a SUBSCRIBE that sees a new write (resolver)
materialize=> SELECT * FROM balances ORDER BY owner
 owner | total | n 
-------+-------+---
 alice |   125 | 2
 bob   |    50 | 1
(2 rows)

# SUBSCRIBE for 6s in one session; another session inserts a row two seconds in.
materialize=> COPY (SUBSCRIBE balances) TO STDOUT
1795186886859	1	bob	50	1
1795186886859	1	alice	125	2
1795186889198	1	carol	75	1

== 3. What's in consensus (resolver)
consensus=> SELECT count(DISTINCT shard) AS shards, count(*) AS rows FROM consensus.consensus
 shards | rows 
--------+------
    104 | 2713
(1 row)

# Every shard with a consensus row, by kind:
  builtin   98
  manifest  2
  other     4
# ('other': shards Materialize doesn't list by name, such as the catalog and txns shards.)
# The user collections, and the resolver's manifest shards:
  user: accounts             sdab04b58-1b91-4af5-9f04-efd8008dac02    (no consensus row)
  user: balances             sc249e290-86c1-48bc-9e3b-f7509b57f6bd    (no consensus row)
  manifest: control          s00000000-0000-0000-0000-00000000c0de    rows=14  seqno=13
  manifest: partition p0     s39a28e62-bcbd-4f9b-a213-670dcbd84f39    rows=34  seqno=33
# User collections have no consensus row: their state is rows in the manifest shards.

== 4. Ask the resolver for the partition's view (resolver)
$ curl -sf 127.0.0.1:6891/views | jq '{partition, seqno, control_manifest, partition_manifest, claim, data_shards: (.data_shards | length)}'
{
  "partition": "p0",
  "seqno": 32,
  "control_manifest": {
    "shard": "s00000000-0000-0000-0000-00000000c0de",
    "time": 5
  },
  "partition_manifest": {
    "shard": "s39a28e62-bcbd-4f9b-a213-670dcbd84f39",
    "time": 27
  },
  "claim": {
    "resolver": "resolver-4eb78e8a-9051-4d93-9e44-5af59ca7bfd2",
    "token": 1,
    "addr": "http://127.0.0.1:6890"
  },
  "data_shards": 2
}
# The 'accounts' table is data shard sdab04b58-1b91-4af5-9f04-efd8008dac02:
$ curl -sf 127.0.0.1:6891/views/sdab04b58-1b91-4af5-9f04-efd8008dac02
{
  "shard": "sdab04b58-1b91-4af5-9f04-efd8008dac02",
  "finalized": false,
  "upper": [
    1795186889199
  ],
  "since": [
    1795186886162
  ],
  "intents": [
    {
      "reader": "c00000000-1111-2222-3333-444444444444",
      "frontier": [
        1795186886162
      ],
      "opaque": 2
    },
    {
      "reader": "ra47712bf-13e7-4bf9-805d-f4b45032f281",
      "frontier": [
        1795186886162
      ],
      "opaque": null
    }
  ],
  "batches": [
    {
      "lower": [
        1795186886450
      ],
      "upper": [
        1795186886860
      ],
      "since": [
        0
      ],
      "updates": 3,
      "parts": 1
    },
    {
      "lower": [
        1795186886860
      ],
      "upper": [
        1795186889199
      ],
      "since": [
        0
      ],
      "updates": 1,
      "parts": 1
    }
  ]
}

== 5. Watch consensus while writing, 10 inserts one second apart (resolver)
  tick consensus: accounts          consensus: partition manifest resolver: accounts          
  1    absent                       rows=37 seqno=36             view_seqno=34 upper=1795186893915
  2    absent                       rows=40 seqno=39             view_seqno=37 upper=1795186895084
  3    absent                       rows=44 seqno=43             view_seqno=40 upper=1795186896254
  4    absent                       rows=48 seqno=47             view_seqno=44 upper=1795186897424
  5    absent                       rows=51 seqno=50             view_seqno=47 upper=1795186898593
  6    absent                       rows=55 seqno=54             view_seqno=50 upper=1795186899766
  7    absent                       rows=58 seqno=57             view_seqno=53 upper=1795186900939
  8    absent                       rows=62 seqno=61             view_seqno=56 upper=1795186902107
  9    absent                       rows=65 seqno=64             view_seqno=59 upper=1795186903277
  10   absent                       rows=70 seqno=69             view_seqno=63 upper=1795186904446
```

</details>

<details><summary>classic mode</summary>

```

== 1. Create a table and a materialized view, and write to them (classic)
materialize=> CREATE TABLE accounts (id int, owner text, balance int)
materialize=> CREATE MATERIALIZED VIEW balances AS SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner
materialize=> INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25)

== 2. Read it back: SELECT, and a SUBSCRIBE that sees a new write (classic)
materialize=> SELECT * FROM balances ORDER BY owner
 owner | total | n 
-------+-------+---
 alice |   125 | 2
 bob   |    50 | 1
(2 rows)

# SUBSCRIBE for 6s in one session; another session inserts a row two seconds in.
materialize=> COPY (SUBSCRIBE balances) TO STDOUT
1795186935052	1	bob	50	1
1795186935052	1	alice	125	2
1795186937187	1	carol	75	1

== 3. What's in consensus (classic)
consensus=> SELECT count(DISTINCT shard) AS shards, count(*) AS rows FROM consensus.consensus
 shards | rows 
--------+------
    104 | 2721
(1 row)

# Every shard with a consensus row, by kind:
  builtin   98
  other     4
  user      2
# ('other': shards Materialize doesn't list by name, such as the catalog and txns shards.)
# The user collections, and the resolver's manifest shards:
  user: accounts             s98668b4e-5322-451a-9fbe-5ed8d16b0ace    rows=14  seqno=13
  user: balances             s59262881-3260-4b1d-8502-76a5beddeaf5    rows=26  seqno=25
# Each user collection is a persist shard: its own consensus row, its own seqno.

== 4. Where a user collection's state lives (classic)
# Classic persist has no resolver. A shard's state is a versioned blob in consensus:
consensus=> SELECT shard, sequence_number, length(data) AS state_bytes FROM consensus.consensus WHERE shard = 's98668b4e-5322-451a-9fbe-5ed8d16b0ace' ORDER BY sequence_number DESC LIMIT 3
                 shard                 | sequence_number | state_bytes 
---------------------------------------+-----------------+-------------
 s98668b4e-5322-451a-9fbe-5ed8d16b0ace |              13 |         943
 s98668b4e-5322-451a-9fbe-5ed8d16b0ace |              12 |         869
 s98668b4e-5322-451a-9fbe-5ed8d16b0ace |              11 |         235
(3 rows)


== 5. Watch consensus while writing, 10 inserts one second apart (classic)
  tick consensus: accounts         
  1    rows=15 seqno=14            
  2    rows=16 seqno=15            
  3    rows=17 seqno=16            
  4    rows=18 seqno=17            
  5    rows=19 seqno=18            
  6    rows=20 seqno=19            
  7    rows=21 seqno=20            
  8    rows=22 seqno=21            
  9    rows=24 seqno=23            
  10   rows=25 seqno=24            
```

</details>
