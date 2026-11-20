# Persist resolver demo

PROTOTYPE. A hand-run demo of the persist resolver (`src/persist-resolver`),
then the same SQL on classic persist for contrast.

## Why

Persist's cost grows with what it writes down. Every persist shard has its own
consensus key, and every tick of every shard is a compare-and-set on it, so
consensus load grows with objects x replicas x tick rate. Load tests with
thousands of materialized views show the consensus database saturating first:
CaS latency climbs, and freshness falls off with it.

The idea: write down less. A resolver takes over the computation and ordering
for many shards. It settles their races in memory and decides each cycle's
outcome, and many per-shard writes become one batched write. That write goes
to existing persist: a manifest shard is an ordinary persist shard whose rows
are the state changes of many data shards (uppers, batches, read intents).
Persist stays the durable layer it is today, so correctness still comes from
its compare-and-set, and the data shards Materialize reads and writes are views
the resolver builds from those rows. Shards that tick together can even share
one upper row.

The tradeoff is latency for throughput. Requests to the resolver queue while
its previous write to CockroachDB is in flight, so a commit waits for that
write plus its own. In exchange, each write carries the changes of many shards.

## What it shows

1. Materialize works as usual on top of the resolver: tables, materialized
   views, `SELECT`, `SUBSCRIBE`.
2. Only user collections go through the resolver. The opt-in is two settings
   on environmentd, and it applies to new user collections only: builtin,
   catalog and txns shards stay ordinary persist shards, each with its own
   rows in consensus.
3. User collections have no consensus rows. Their state is rows in the
   resolver's two manifest shards.
4. Shards whose uppers move together share one upper row (an upper group).
   With 20 MVs on one cluster, one group's single upper ticks for all of them.
   On classic persist, every MV has its own consensus row and seqno.

Every command shown on camera is below, verbatim and in order. `demo.sh` only
resets, tears down, labels shards, and holds environmentd's long local argument
list. `rehearse.sh` runs this file's blocks (the ones marked `<!-- run: ... -->`)
for both modes and checks the output, so a passing rehearsal means the commands
here work as written.

## Before recording

- CockroachDB on `localhost:26257`, insecure, for consensus and the timestamp
  oracle:

  ```
  docker run -d --name cockroach -p 127.0.0.1:26257:26257 \
      cockroachdb/cockroach:v23.1.11 start-single-node --insecure
  ```

- `psql`, `jq` and `curl` (macOS: `brew install libpq jq` and `brew link --force
  libpq`, since libpq is keg-only), and `timeout` for `rehearse.sh` (macOS:
  `brew install coreutils`).
- Dev builds of the three binaries, from the repository root:

  ```
  cargo build --bin environmentd --bin clusterd --bin persist-resolver
  ```

- Check the box: `test/persist-resolver/demo/rehearse.sh` (about two minutes).
  It ends with everything stopped.
- Four terminal panes, all in the repository root: **1** resolver,
  **2** environmentd, **3** `psql` to Materialize, **4** a shell.

`demo.sh reset` wipes `mzdata/` in the repository root and the `consensus`,
`tsoracle`, `storage` and `demo` schemas of CRDB database `materialize`, as
`bin/environmentd --reset` would. `demo.sh down` stops every environmentd,
clusterd and persist-resolver started from a `target/` directory.

## Resolver run

### 1. Reset (pane 4)

<!-- run: reset -->
```sh
test/persist-resolver/demo/demo.sh reset
```

Empty consensus, empty blob store.

### 2. Start the resolver (pane 1)

<!-- run: resolver -->
```sh
target/debug/persist-resolver \
    --blob-url "file://$PWD/mzdata/persist/blob" \
    --consensus-url 'postgres://root@localhost:26257/materialize?options=--search_path=consensus' \
    --listen-addr 127.0.0.1:6890 \
    --http-listen-addr 127.0.0.1:6891
```

Point at `claimed partition p0 with token 1`: one resolver owns partition `p0`,
whose state lives in two ordinary persist shards (the manifest shards) in the
same consensus and blob store Materialize uses.

### 3. Start environmentd, opted in (pane 2)

<!-- run: environmentd-resolver -->
```sh
MZ_PERSIST_RESOLVER_URL=http://127.0.0.1:6890 \
test/persist-resolver/demo/demo.sh environmentd \
    --system-parameter-default=persist_v2_register_user_shards=true
```

The opt-in is the first and last line: where the resolver is (environmentd and
the clusterd processes it starts), and a system parameter that registers new
**user** collections with it. `demo.sh environmentd` adds only the local
arguments `bin/environmentd` would pass, and logs to
`scratch/persist-resolver-demo/environmentd.log`. Ready in about 30 s.

### 4. SQL (pane 3)

<!-- run: psql -->
```sh
psql -h localhost -p 6875 -U materialize
```

<!-- run: mz-create -->
```sql
CREATE TABLE accounts (id int, owner text, balance int);
INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25);
CREATE MATERIALIZED VIEW balances AS
    SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner;
SELECT * FROM balances ORDER BY owner;
```

Ordinary SQL. The table and the MV are the first two user collections.

<!-- run: mz-subscribe -->
```sql
COPY (SUBSCRIBE balances) TO STDOUT;
```

Then in pane 4:

<!-- run: insert-carol -->
```sh
psql -h localhost -p 6875 -U materialize -c "INSERT INTO accounts VALUES (4, 'carol', 75)"
```

Carol's row streams into pane 3: the MV's writes and the subscription's reads
both go through the resolver. `Ctrl-C` in pane 3 ends the `SUBSCRIBE`.

<!-- run: mz-fanout -->
```sql
SELECT 'CREATE MATERIALIZED VIEW mv_' || lpad(i::text, 2, '0') ||
       ' AS SELECT owner, sum(balance) * ' || i || ' AS total FROM accounts GROUP BY owner'
FROM generate_series(1, 20) AS i \gexec
```

Twenty more MVs on the same cluster, so 22 user collections.

### 5. Consensus, by kind of shard (pane 4)

<!-- run: labels -->
```sh
test/persist-resolver/demo/demo.sh labels
```

Copies each shard's kind and name from Materialize (and the resolver's
manifest shards) into CRDB as `demo.shard_labels`, so plain SQL against
consensus can name them.

<!-- run: crdb -->
```sh
psql postgres://root@localhost:26257/materialize
```

<!-- run: crdb-kinds -->
```sql
SELECT l.kind,
       count(DISTINCT l.shard) AS shards,
       count(DISTINCT c.shard) AS shards_in_consensus,
       count(c.shard) AS consensus_rows
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
GROUP BY l.kind ORDER BY l.kind;
```

`user`: 22 shards, none in consensus. Everything else (`builtin`, `catalog`
which is `mz_catalog_raw`, and `other`: txns, the expression cache and other
shards Materialize doesn't list by name) has its own consensus rows, as today.
So does `resolver-manifest`: the resolver's state is itself two persist shards.

<!-- run: crdb-user-shards -->
```sql
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
```

Every user collection by name, with 0 consensus rows, and the two manifest
shards with theirs. `\q` to leave.

### 6. The resolver's view (pane 4)

<!-- run: views-groups -->
```sh
curl -s 127.0.0.1:6891/views | jq '.upper_groups'
```

One upper group: 21 members (`balances` and the 20 MVs) and one `upper`. It's
one row in the partition manifest, in place of 21 per-shard `Upper` rows.

<!-- run: views-watch -->
```sh
while sleep 1; do
  curl -s 127.0.0.1:6891/views | jq -c '{seqno, groups: [.upper_groups[] | {group, upper: .upper[0], members: (.members | length)}], own_upper_rows: (.data_shards | map(select(.upper_group == null)) | length)}'
done
```

Once a second the group's single `upper` moves for all 21 members together.
`own_upper_rows` is 1: `accounts`, a table, whose data shard upper txn-wal
advances on its own schedule. `seqno` (the resolver's view seqno, one per
resolver write) moves by about 2 per tick, see the numbers below. `Ctrl-C` to
stop.

<!-- run: write-rate -->
```sh
s0=$(curl -s 127.0.0.1:6891/views | jq .seqno); sleep 30
curl -s 127.0.0.1:6891/views | jq -r --argjson s0 "$s0" \
  '"group members: \([.upper_groups[].members | length] | add), resolver writes in 30 s: \(.seqno - $s0)"'
```

How many writes the resolver made to its manifest shards in 30 seconds, for
every user shard together: 2-3 a second, one group advance per tick plus
a little more. A window can catch an occasional burst; run it again if one
looks high. Step 14 runs it again with twice the MVs.

### 7. Snapshot the user shards before writing (pane 4)

<!-- run: snapshot-before -->
```sh
curl -s 127.0.0.1:6891/views | jq -r --slurpfile n scratch/persist-resolver-demo/names.json '
  (.upper_groups[] | "group \(.group): upper \(.upper[0]), \(.members | length) members"),
  (.data_shards | map([$n[0][.shard], (.upper_group // "own"), .upper[0], (.batches | length),
    ([.batches[].updates] | add // 0)]) | sort[] | @tsv)' | tee /tmp/before.txt
```

The group's one upper, then a line per user shard: name, upper group (`own`
for its own `Upper` row), upper, batches, and updates across its batches. The
20 MVs and `balances` are in the group at its upper. `accounts` is on its own
row, behind: txn-wal advances a table's data shard upper only when it applies
a write to it. A snapshot can land between a tick's two resolver writes, with
an early appender or two `own` for that moment. Rerun it if so.

### 8. One shard up close, before a write (pane 4)

<!-- run: shard-id -->
```sh
S=$(psql -h localhost -p 6875 -U materialize -At -c "SELECT s.shard_id FROM mz_internal.mz_storage_shards s JOIN mz_materialized_views m ON s.object_id = m.id WHERE m.name = 'mv_01'"); echo "$S"
```

`mv_01`'s data shard id, kept in `$S`.

<!-- run: shard-logical -->
```sh
curl -s 127.0.0.1:6891/views/$S | jq '{shard, upper, upper_group, since,
  intents: [.intents[] | {reader, frontier}],
  batches: [.batches[] | {lower: .lower[0], upper: .upper[0], since: .since[0], updates}]}'
```

The shard as clients get it from the resolver, the data persist's client
builds today's shard state from: one `upper` (its group's), a `since` (the
earliest read intent), the readers' intents, and its batches with their time
ranges and update counts. The batches leave gaps in time: a tick with no data
advances the upper but writes no batch (next-steps doc, open question 3).

<!-- run: shard-rows -->
```sh
curl -s "127.0.0.1:6891/views/$S?rows" | jq -c '.rows[]'
```

A summary of the rows that shard is made of (schemas elided, batch part keys
only counted). In control: `Shard`, `Schema`, `Assign` (its
partition). In the partition manifest: `Member` (it's in group 0, so it has no
`Upper` row of its own), the group's `Group(0)` row holding the upper, one
`Batch` row per batch, and one `Intent` row per reader.

<!-- run: shard-crdb -->
```sh
psql postgres://root@localhost:26257/materialize -c "SELECT shard, sequence_number, length(data) AS bytes FROM consensus.consensus WHERE shard = '$S'"
P=$(curl -s 127.0.0.1:6891/views | jq -r .partition_manifest.shard)
psql postgres://root@localhost:26257/materialize -c "SELECT shard, count(*) AS rows, max(sequence_number) AS seqno FROM consensus.consensus WHERE shard = '$P' GROUP BY shard"
```

`mv_01` has no rows in CRDB's consensus table: the state above lives only in the
partition manifest shard, whose own consensus rows are the second query. The
shell fills in `$S` and `$P`; typed inside an interactive `psql`, `'$S'` would
be the literal string and match nothing in either mode.

### 9. One INSERT (pane 3)

<!-- run: mz-insert -->
```sql
INSERT INTO accounts VALUES (5, 'dave', 40);
```

### 10. The same shard after the INSERT (pane 4)

<!-- run: shard-logical-again -->
```sh
curl -s 127.0.0.1:6891/views/$S | jq '{shard, upper, upper_group, since,
  intents: [.intents[] | {reader, frontier}],
  batches: [.batches[] | {lower: .lower[0], upper: .upper[0], since: .since[0], updates}]}'
```

<!-- run: shard-rows-again -->
```sh
curl -s "127.0.0.1:6891/views/$S?rows" | jq -c '.rows[]'
```

- The logical view has one more batch, covering the write's time, with the
  MV's changed rows as its updates. Its `upper` moved past it.
- The rows have one new `Batch` row, and `Group(0)` has a new upper.
- `Member` didn't change. The MV's append landed in the same resolver write as
  the group's advance, so the MV never left the group in what was written.

### 11. More writes (pane 3)

<!-- run: mz-writes -->
```sql
UPDATE accounts SET balance = balance + 10 WHERE owner = 'bob';
DELETE FROM accounts WHERE owner = 'carol';
SELECT * FROM balances ORDER BY owner;
SELECT * FROM mv_20 ORDER BY owner;
```

Both MVs show all three writes: dave added, bob up by 10 (by 200 in `mv_20`,
which multiplies by 20), carol gone.

### 12. Every user shard after the writes (pane 4)

<!-- run: snapshot-after -->
```sh
curl -s 127.0.0.1:6891/views | jq -r --slurpfile n scratch/persist-resolver-demo/names.json '
  (.upper_groups[] | "group \(.group): upper \(.upper[0]), \(.members | length) members"),
  (.data_shards | map([$n[0][.shard], (.upper_group // "own"), .upper[0], (.batches | length),
    ([.batches[].updates] | add // 0)]) | sort[] | @tsv)' | tee /tmp/after.txt
```

<!-- run: batch-diff -->
```sh
awk -F '\t' 'NF == 5 && NR == FNR { b[$1] = $0; next } NF == 5 && ($1 in b) { split(b[$1], o, "\t")
  printf "%-9s group %s -> %-4s batches %2s -> %-3s updates %3s -> %s\n", $1, o[2], $2, o[4], $4, o[5], $5 }' /tmp/before.txt /tmp/after.txt
```

- Each write adds a batch to the table and to every MV whose result changed,
  so batch counts go up.
- Compaction sometimes merges a shard's batches, so a count can also go down.
- The MVs stay in the group. An append that carries data advances the upper
  by the same tick an empty one would.
- The table is the exception. txn-wal moves its upper on its own schedule, so
  it usually keeps its own upper row, and briefly joins the group when the
  uppers happen to match.

### 13. Consensus after the writes (pane 4)

<!-- run: crdb-again -->
```sh
psql postgres://root@localhost:26257/materialize
```

<!-- run: crdb-kinds-again -->
```sql
SELECT l.kind,
       count(DISTINCT l.shard) AS shards,
       count(DISTINCT c.shard) AS shards_in_consensus,
       count(c.shard) AS consensus_rows
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
GROUP BY l.kind ORDER BY l.kind;
```

`user` still has 0 consensus rows: the user shards' new state is batches in
blob and rows in the partition manifest. (The table writes also went through
txn-wal's txns shard, counted in `other`, as they do today.)

### 14. Twice the MVs, the same writes (panes 3 and 4)

<!-- run: mz-fanout-more -->
```sql
SELECT 'CREATE MATERIALIZED VIEW mv_' || lpad(i::text, 2, '0') ||
       ' AS SELECT owner, sum(balance) * ' || i || ' AS total FROM accounts GROUP BY owner'
FROM generate_series(21, 40) AS i \gexec
```

<!-- run: write-rate-more -->
```sh
s0=$(curl -s 127.0.0.1:6891/views | jq .seqno); sleep 30
curl -s 127.0.0.1:6891/views | jq -r --argjson s0 "$s0" \
  '"group members: \([.upper_groups[].members | length] | add), resolver writes in 30 s: \(.seqno - $s0)"'
```

- The group grew from 21 to 41 members, and the resolver's write rate stayed
  about the same: every member's upper rides the one group row. (Give the new
  MVs half a minute to settle first; while they hydrate, the rate runs high.)
- That holds for the uppers ticking, which is most of persist's write load
  today. An MV that actually gets new data still adds its own `Batch` row to
  the write, so data changes still cost per shard. The writes just aren't
  separate round trips to CockroachDB anymore.

### 15. Teardown

`Ctrl-C` in panes 1 and 2, then in pane 4:

<!-- run: down -->
```sh
test/persist-resolver/demo/demo.sh down
```

## Classic run, for contrast

The same steps without the resolver. `Ctrl-C` and `\q` out of anything still
running from the resolver run first.

1. Reset (pane 4):

   <!-- run: classic-reset -->
   ```sh
   test/persist-resolver/demo/demo.sh reset
   ```

2. No resolver. environmentd without the opt-in (pane 2):

   <!-- run: environmentd-classic -->
   ```sh
   test/persist-resolver/demo/demo.sh environmentd
   ```

3. SQL (pane 3):

   <!-- run: classic-psql -->
   ```sh
   psql -h localhost -p 6875 -U materialize
   ```

   <!-- run: classic-mz-create -->
   ```sql
   CREATE TABLE accounts (id int, owner text, balance int);
   INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25);
   CREATE MATERIALIZED VIEW balances AS
       SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner;
   SELECT * FROM balances ORDER BY owner;
   ```

   <!-- run: classic-mz-subscribe -->
   ```sql
   COPY (SUBSCRIBE balances) TO STDOUT;
   ```

   Then in pane 4:

   <!-- run: classic-insert-carol -->
   ```sh
   psql -h localhost -p 6875 -U materialize -c "INSERT INTO accounts VALUES (4, 'carol', 75)"
   ```

   Carol's row streams in, as before. `Ctrl-C` in pane 3, then:

   <!-- run: classic-mz-fanout -->
   ```sql
   SELECT 'CREATE MATERIALIZED VIEW mv_' || lpad(i::text, 2, '0') ||
          ' AS SELECT owner, sum(balance) * ' || i || ' AS total FROM accounts GROUP BY owner'
   FROM generate_series(1, 20) AS i \gexec
   ```

   Same results as the resolver run.

4. Consensus by kind (pane 4):

   <!-- run: classic-labels -->
   ```sh
   test/persist-resolver/demo/demo.sh labels
   ```

   <!-- run: classic-crdb -->
   ```sh
   psql postgres://root@localhost:26257/materialize
   ```

   <!-- run: classic-crdb-kinds -->
   ```sql
   SELECT l.kind,
          count(DISTINCT l.shard) AS shards,
          count(DISTINCT c.shard) AS shards_in_consensus,
          count(c.shard) AS consensus_rows
   FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
   GROUP BY l.kind ORDER BY l.kind;
   ```

   <!-- run: classic-crdb-user-shards -->
   ```sql
   SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
   FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
   WHERE l.kind IN ('user', 'resolver-manifest')
   GROUP BY l.name ORDER BY l.name;
   ```

   `user` now has 22 shards in consensus, each with its own rows and seqno.
   Then watch them climb, each on its own:

   <!-- run: crdb-watch -->
   ```sql
   SELECT now()::TIME(0) AS t,
          string_agg(s.seqno::STRING, ' ' ORDER BY s.name) AS seqno_of_each_user_shard
   FROM (SELECT l.name, max(c.sequence_number) AS seqno
         FROM demo.shard_labels l JOIN consensus.consensus c USING (shard)
         WHERE l.kind = 'user' GROUP BY l.name) AS s \watch 1
   ```

   One line per second with 22 seqnos, `accounts` first, then `balances` and
   `mv_01` to `mv_20`: every shard writes its own consensus row each tick.
   (In the resolver run this query prints an empty list.) `Ctrl-C` to stop.

5. Writes (pane 3):

   <!-- run: classic-mz-insert -->
   ```sql
   INSERT INTO accounts VALUES (5, 'dave', 40);
   ```

   <!-- run: classic-mz-writes -->
   ```sql
   UPDATE accounts SET balance = balance + 10 WHERE owner = 'bob';
   DELETE FROM accounts WHERE owner = 'carol';
   SELECT * FROM balances ORDER BY owner;
   SELECT * FROM mv_20 ORDER BY owner;
   ```

   Same results as the resolver run.

6. Each shard's consensus rows again (pane 4, still in `psql`):

   <!-- run: classic-crdb-user-shards-again -->
   ```sql
   SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
   FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
   WHERE l.kind IN ('user', 'resolver-manifest')
   GROUP BY l.name ORDER BY l.name;
   ```

   Every user shard has more consensus rows and a higher seqno. The writes
   added some, but most is the once a second upper advance each shard makes on
   its own row. In the resolver run that is one `Group` row for 21 shards.

7. Teardown: `Ctrl-C` in pane 2, then in pane 4:

   <!-- run: classic-down -->
   ```sh
   test/persist-resolver/demo/demo.sh down
   ```

## Numbers to have in hand

- A tick's appends from one cluster's 21 MV shards reach the resolver over two
  of its writes: 1-2 shards, then the rest. The early ones leave the group for
  their own `Upper` row and rejoin when the group catches up, so the view
  seqno moves by about 2 per tick, and `own_upper_rows` sometimes briefly
  shows 2 or 3.
- The resolver writes while idle: the uppers advance once a second whether or
  not there are inserts. Over 30 idle seconds one earlier run saw the view seqno
  go 23 to 56 (before upper groups).
- `--upper-groups=false` on `persist-resolver` turns grouping off (every shard
  on its own `Upper` row), for comparison.

## Known issues

- In a dev build, `FETCH` from a `DECLARE ... CURSOR FOR SUBSCRIBE` overflowed a
  3 MiB tokio worker stack in environmentd and aborted it, in 3 of 3 classic-mode
  tries. The one resolver-mode try didn't crash. The overflowing stack is
  pgwire's `fetch` and the adapter's frontend peek path down to `plan_subscribe`,
  not persist. The demo uses `COPY (SUBSCRIBE ...) TO STDOUT` instead, which
  streams rows live in a terminal.
- `demo.sh down` matches processes by path, so it also stops an environmentd,
  clusterd or persist-resolver from another checkout's `target/`.

## Captured output

From `rehearse.sh` on a dev build, which runs the blocks above in order. Shard
ids, timestamps and seqnos differ per run.

<details><summary>resolver run</summary>

```

$ # reset
test/persist-resolver/demo/demo.sh reset
reset: mzdata/ and the consensus, tsoracle and storage schemas are empty.

$ # resolver
target/debug/persist-resolver \
    --blob-url "file://$PWD/mzdata/persist/blob" \
    --consensus-url 'postgres://root@localhost:26257/materialize?options=--search_path=consensus' \
    --listen-addr 127.0.0.1:6890 \
    --http-listen-addr 127.0.0.1:6891
grep: scratch/persist-resolver-demo/rehearsal-2/resolver.out: No such file or directory
2026-11-20T18:34:55.164299Z  INFO mz_persist_resolver: resolver resolver-dd2a532f-6662-40ce-b066-351e43de1bcc claimed partition p0 with token 1

$ # environmentd-resolver
MZ_PERSIST_RESOLVER_URL=http://127.0.0.1:6890 \
test/persist-resolver/demo/demo.sh environmentd \
    --system-parameter-default=persist_v2_register_user_shards=true

$ # psql
psql -h localhost -p 6875 -U materialize

$ # mz-create
CREATE TABLE accounts (id int, owner text, balance int);
INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25);
CREATE MATERIALIZED VIEW balances AS
    SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner;
SELECT * FROM balances ORDER BY owner;
CREATE TABLE
INSERT 0 3
CREATE MATERIALIZED VIEW
 owner | total | n
-------+-------+---
 alice |   125 | 2
 bob   |    50 | 1
(2 rows)


$ # mz-subscribe
COPY (SUBSCRIBE balances) TO STDOUT;

$ # insert-carol
psql -h localhost -p 6875 -U materialize -c "INSERT INTO accounts VALUES (4, 'carol', 75)"
INSERT 0 1
1795199724921	1	bob	50	1
1795199724921	1	alice	125	2
1795199727458	1	carol	75	1

$ # mz-fanout
SELECT 'CREATE MATERIALIZED VIEW mv_' || lpad(i::text, 2, '0') ||
       ' AS SELECT owner, sum(balance) * ' || i || ' AS total FROM accounts GROUP BY owner'
FROM generate_series(1, 20) AS i \gexec
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW

$ # labels
test/persist-resolver/demo/demo.sh labels
labels: 97 builtin, 1 catalog, 4 other, 2 resolver-manifest, 22 user

$ # crdb
psql postgres://root@localhost:26257/materialize

$ # crdb-kinds
SELECT l.kind,
       count(DISTINCT l.shard) AS shards,
       count(DISTINCT c.shard) AS shards_in_consensus,
       count(c.shard) AS consensus_rows
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
GROUP BY l.kind ORDER BY l.kind;
       kind        | shards | shards_in_consensus | consensus_rows
-------------------+--------+---------------------+----------------
 builtin           |     97 |                  97 |           3948
 catalog           |      1 |                   1 |            203
 other             |      4 |                   4 |            172
 resolver-manifest |      2 |                   2 |            177
 user              |     22 |                   0 |              0
(5 rows)


$ # crdb-user-shards
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
   name    | consensus_rows | seqno
-----------+----------------+-------
 accounts  |              0 |
 balances  |              0 |
 control   |             58 |    57
 mv_01     |              0 |
 mv_02     |              0 |
 mv_03     |              0 |
 mv_04     |              0 |
 mv_05     |              0 |
 mv_06     |              0 |
 mv_07     |              0 |
 mv_08     |              0 |
 mv_09     |              0 |
 mv_10     |              0 |
 mv_11     |              0 |
 mv_12     |              0 |
 mv_13     |              0 |
 mv_14     |              0 |
 mv_15     |              0 |
 mv_16     |              0 |
 mv_17     |              0 |
 mv_18     |              0 |
 mv_19     |              0 |
 mv_20     |              0 |
 partition |            119 |   247
(24 rows)


$ # views-groups
curl -s 127.0.0.1:6891/views | jq '.upper_groups'
[
  {
    "group": 0,
    "upper": [
      1795199737726
    ],
    "members": [
      "s110b0440-7b5b-44cc-aa7a-0195b6033a89",
      "s288c945e-4cc9-4378-9ee1-791bc09d5c75",
      "s379a378a-51a8-4366-ab1e-a604ed4d502c",
      "s530879db-66bb-4c05-b17c-8db7ec4287a2",
      "s56725e89-e23b-456a-abd5-293cac9cc9c9",
      "s70f21d6d-f33e-4653-9ecc-3fcb945a62b6",
      "s736b688a-d4c1-4c43-bc3b-aedc2885eecc",
      "s7b47da08-cbb3-48d0-84ea-64150d6b203a",
      "s8c2cd18c-6d12-4e00-95ce-eaeaed8d8922",
      "s96620a54-c2d7-4726-9a72-f59fe73deea9",
      "sbc3991a9-a164-47b9-b29b-a4ecac562789",
      "sc096d4e3-9c54-45f8-b918-0315f94ecc0e",
      "sc1d6b2c2-ba20-484b-8fd5-7917a5470019",
      "sc89c2741-b34b-4996-bfd2-eaf905b8ddd8",
      "sd8b01498-f6d4-4d93-9c9d-d8ae2cadc67f",
      "sdde6d44c-71ac-47dd-a826-1708ef514d0c",
      "se359375d-b703-4303-96dd-037b0cd25e89",
      "se4154776-8b32-4d69-8dfe-205e1c0d40be",
      "sec1f8317-58e9-4d84-8ad6-0f3a7c70a2f1",
      "sf2312795-8d43-4793-b5cb-87b3148d437d",
      "sf7feebe3-0c96-44e7-a1f1-75261c490822"
    ]
  }
]

$ # views-watch
while sleep 1; do
  curl -s 127.0.0.1:6891/views | jq -c '{seqno, groups: [.upper_groups[] | {group, upper: .upper[0], members: (.members | length)}], own_upper_rows: (.data_shards | map(select(.upper_group == null)) | length)}'
done
{"seqno":260,"groups":[{"group":0,"upper":1795199738726,"members":21}],"own_upper_rows":1}
{"seqno":262,"groups":[{"group":0,"upper":1795199739726,"members":21}],"own_upper_rows":1}
{"seqno":265,"groups":[{"group":0,"upper":1795199740726,"members":21}],"own_upper_rows":1}
{"seqno":267,"groups":[{"group":0,"upper":1795199741727,"members":21}],"own_upper_rows":1}
{"seqno":270,"groups":[{"group":0,"upper":1795199742727,"members":21}],"own_upper_rows":1}

$ # snapshot-before
curl -s 127.0.0.1:6891/views | jq -r --slurpfile n scratch/persist-resolver-demo/names.json '
  (.upper_groups[] | "group \(.group): upper \(.upper[0]), \(.members | length) members"),
  (.data_shards | map([$n[0][.shard], (.upper_group // "own"), .upper[0], (.batches | length),
    ([.batches[].updates] | add // 0)]) | sort[] | @tsv)' | tee /tmp/before.txt
group 0: upper 1795199743727, 21 members
accounts	own	1795199730001	2	4
balances	0	1795199743727	2	3
mv_01	0	1795199743727	1	3
mv_02	0	1795199743727	1	3
mv_03	0	1795199743727	1	3
mv_04	0	1795199743727	1	3
mv_05	0	1795199743727	1	3
mv_06	0	1795199743727	1	3
mv_07	0	1795199743727	1	3
mv_08	0	1795199743727	1	3
mv_09	0	1795199743727	1	3
mv_10	0	1795199743727	1	3
mv_11	0	1795199743727	1	3
mv_12	0	1795199743727	1	3
mv_13	0	1795199743727	1	3
mv_14	0	1795199743727	1	3
mv_15	0	1795199743727	1	3
mv_16	0	1795199743727	1	3
mv_17	0	1795199743727	1	3
mv_18	0	1795199743727	1	3
mv_19	0	1795199743727	1	3
mv_20	0	1795199743727	1	3

$ # shard-id
S=$(psql -h localhost -p 6875 -U materialize -At -c "SELECT s.shard_id FROM mz_internal.mz_storage_shards s JOIN mz_materialized_views m ON s.object_id = m.id WHERE m.name = 'mv_01'"); echo "$S"
sec1f8317-58e9-4d84-8ad6-0f3a7c70a2f1

$ # shard-logical
curl -s 127.0.0.1:6891/views/$S | jq '{shard, upper, upper_group, since,
  intents: [.intents[] | {reader, frontier}],
  batches: [.batches[] | {lower: .lower[0], upper: .upper[0], since: .since[0], updates}]}'
{
  "shard": "sec1f8317-58e9-4d84-8ad6-0f3a7c70a2f1",
  "upper": [
    1795199743727
  ],
  "upper_group": 0,
  "since": [
    1795199730000
  ],
  "intents": [
    {
      "reader": "c00000000-1111-2222-3333-444444444444",
      "frontier": [
        1795199730000
      ]
    },
    {
      "reader": "r30104bb6-896a-4eeb-a365-e4f738f23323",
      "frontier": [
        1795199730000
      ]
    }
  ],
  "batches": [
    {
      "lower": 1795199730000,
      "upper": 1795199731809,
      "since": 0,
      "updates": 3
    }
  ]
}

$ # shard-rows
curl -s "127.0.0.1:6891/views/$S?rows" | jq -c '.rows[]'
{"manifest":"control","key":"Shard","val":{"finalized":false}}
{"manifest":"control","key":"Schema(0)","val":"..."}
{"manifest":"control","key":"Assign","val":"p0"}
{"manifest":"partition","key":"Group(0)","val":[1795199743727]}
{"manifest":"partition","key":"Member","val":0}
{"manifest":"partition","key":"Batch","val":{"lower":[1795199730000],"upper":[1795199731809],"since":[0],"updates":3,"parts":1}}
{"manifest":"partition","key":"Intent(c00000000-1111-2222-3333-444444444444)","val":{"frontier":[1795199730000],"opaque":2}}
{"manifest":"partition","key":"Intent(r30104bb6-896a-4eeb-a365-e4f738f23323)","val":{"frontier":[1795199730000],"opaque":null}}

$ # mz-insert
INSERT INTO accounts VALUES (5, 'dave', 40);
INSERT 0 1

$ # shard-logical-again
curl -s 127.0.0.1:6891/views/$S | jq '{shard, upper, upper_group, since,
  intents: [.intents[] | {reader, frontier}],
  batches: [.batches[] | {lower: .lower[0], upper: .upper[0], since: .since[0], updates}]}'
{
  "shard": "sec1f8317-58e9-4d84-8ad6-0f3a7c70a2f1",
  "upper": [
    1795199746761
  ],
  "upper_group": 0,
  "since": [
    1795199730000
  ],
  "intents": [
    {
      "reader": "c00000000-1111-2222-3333-444444444444",
      "frontier": [
        1795199730000
      ]
    },
    {
      "reader": "r30104bb6-896a-4eeb-a365-e4f738f23323",
      "frontier": [
        1795199730000
      ]
    }
  ],
  "batches": [
    {
      "lower": 1795199730000,
      "upper": 1795199731809,
      "since": 0,
      "updates": 3
    },
    {
      "lower": 1795199744726,
      "upper": 1795199744759,
      "since": 0,
      "updates": 1
    }
  ]
}

$ # shard-rows-again
curl -s "127.0.0.1:6891/views/$S?rows" | jq -c '.rows[]'
{"manifest":"control","key":"Shard","val":{"finalized":false}}
{"manifest":"control","key":"Schema(0)","val":"..."}
{"manifest":"control","key":"Assign","val":"p0"}
{"manifest":"partition","key":"Group(0)","val":[1795199746761]}
{"manifest":"partition","key":"Member","val":0}
{"manifest":"partition","key":"Batch","val":{"lower":[1795199730000],"upper":[1795199731809],"since":[0],"updates":3,"parts":1}}
{"manifest":"partition","key":"Batch","val":{"lower":[1795199744726],"upper":[1795199744759],"since":[0],"updates":1,"parts":1}}
{"manifest":"partition","key":"Intent(c00000000-1111-2222-3333-444444444444)","val":{"frontier":[1795199730000],"opaque":2}}
{"manifest":"partition","key":"Intent(r30104bb6-896a-4eeb-a365-e4f738f23323)","val":{"frontier":[1795199730000],"opaque":null}}

$ # mz-writes
UPDATE accounts SET balance = balance + 10 WHERE owner = 'bob';
DELETE FROM accounts WHERE owner = 'carol';
SELECT * FROM balances ORDER BY owner;
SELECT * FROM mv_20 ORDER BY owner;
UPDATE 1
DELETE 1
 owner | total | n
-------+-------+---
 alice |   125 | 2
 bob   |    60 | 1
 dave  |    40 | 1
(3 rows)

 owner | total
-------+-------
 alice |  2500
 bob   |  1200
 dave  |   800
(3 rows)


$ # snapshot-after
curl -s 127.0.0.1:6891/views | jq -r --slurpfile n scratch/persist-resolver-demo/names.json '
  (.upper_groups[] | "group \(.group): upper \(.upper[0]), \(.members | length) members"),
  (.data_shards | map([$n[0][.shard], (.upper_group // "own"), .upper[0], (.batches | length),
    ([.batches[].updates] | add // 0)]) | sort[] | @tsv)' | tee /tmp/after.txt
group 0: upper 1795199749761, 21 members
accounts	own	1795199746763	5	8
balances	0	1795199749761	5	7
mv_01	0	1795199749761	4	7
mv_02	0	1795199749761	4	7
mv_03	0	1795199749761	4	7
mv_04	0	1795199749761	4	7
mv_05	0	1795199749761	4	7
mv_06	0	1795199749761	4	7
mv_07	0	1795199749761	4	7
mv_08	0	1795199749761	4	7
mv_09	0	1795199749761	4	7
mv_10	0	1795199749761	4	7
mv_11	0	1795199749761	4	7
mv_12	0	1795199749761	4	7
mv_13	0	1795199749761	4	7
mv_14	0	1795199749761	4	7
mv_15	0	1795199749761	4	7
mv_16	0	1795199749761	4	7
mv_17	0	1795199749761	4	7
mv_18	0	1795199749761	4	7
mv_19	0	1795199749761	4	7
mv_20	0	1795199749761	4	7

$ # batch-diff
awk -F '\t' 'NF == 5 && NR == FNR { b[$1] = $0; next } NF == 5 && ($1 in b) { split(b[$1], o, "\t")
  printf "%-9s group %s -> %-4s batches %2s -> %-3s updates %3s -> %s\n", $1, o[2], $2, o[4], $4, o[5], $5 }' /tmp/before.txt /tmp/after.txt
accounts  group own -> own  batches  2 -> 5   updates   4 -> 8
balances  group 0 -> 0    batches  2 -> 5   updates   3 -> 7
mv_01     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_02     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_03     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_04     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_05     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_06     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_07     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_08     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_09     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_10     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_11     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_12     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_13     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_14     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_15     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_16     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_17     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_18     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_19     group 0 -> 0    batches  1 -> 4   updates   3 -> 7
mv_20     group 0 -> 0    batches  1 -> 4   updates   3 -> 7

$ # crdb-again
psql postgres://root@localhost:26257/materialize

$ # crdb-kinds-again
SELECT l.kind,
       count(DISTINCT l.shard) AS shards,
       count(DISTINCT c.shard) AS shards_in_consensus,
       count(c.shard) AS consensus_rows
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
GROUP BY l.kind ORDER BY l.kind;
       kind        | shards | shards_in_consensus | consensus_rows
-------------------+--------+---------------------+----------------
 builtin           |     97 |                  97 |           5041
 catalog           |      1 |                   1 |            223
 other             |      4 |                   4 |            205
 resolver-manifest |      2 |                   2 |            110
 user              |     22 |                   0 |              0
(5 rows)


$ # down
test/persist-resolver/demo/demo.sh down
down.
```

</details>

<details><summary>classic run</summary>

```

$ # reset
test/persist-resolver/demo/demo.sh reset
reset: mzdata/ and the consensus, tsoracle and storage schemas are empty.

$ # environmentd-classic
test/persist-resolver/demo/demo.sh environmentd

$ # psql
psql -h localhost -p 6875 -U materialize

$ # mz-create
CREATE TABLE accounts (id int, owner text, balance int);
INSERT INTO accounts VALUES (1, 'alice', 100), (2, 'bob', 50), (3, 'alice', 25);
CREATE MATERIALIZED VIEW balances AS
    SELECT owner, sum(balance) AS total, count(*) AS n FROM accounts GROUP BY owner;
SELECT * FROM balances ORDER BY owner;
CREATE TABLE
INSERT 0 3
CREATE MATERIALIZED VIEW
 owner | total | n
-------+-------+---
 alice |   125 | 2
 bob   |    50 | 1
(2 rows)


$ # mz-subscribe
COPY (SUBSCRIBE balances) TO STDOUT;

$ # insert-carol
psql -h localhost -p 6875 -U materialize -c "INSERT INTO accounts VALUES (4, 'carol', 75)"
INSERT 0 1
1795199779134	1	bob	50	1
1795199779134	1	alice	125	2
1795199781280	1	carol	75	1

$ # mz-fanout
SELECT 'CREATE MATERIALIZED VIEW mv_' || lpad(i::text, 2, '0') ||
       ' AS SELECT owner, sum(balance) * ' || i || ' AS total FROM accounts GROUP BY owner'
FROM generate_series(1, 20) AS i \gexec
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW

$ # labels
test/persist-resolver/demo/demo.sh labels
labels: 97 builtin, 1 catalog, 4 other, 22 user

$ # crdb
psql postgres://root@localhost:26257/materialize

$ # crdb-kinds
SELECT l.kind,
       count(DISTINCT l.shard) AS shards,
       count(DISTINCT c.shard) AS shards_in_consensus,
       count(c.shard) AS consensus_rows
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
GROUP BY l.kind ORDER BY l.kind;
  kind   | shards | shards_in_consensus | consensus_rows
---------+--------+---------------------+----------------
 builtin |     97 |                  97 |           3195
 catalog |      1 |                   1 |            199
 other   |      4 |                   4 |            172
 user    |     22 |                  22 |            550
(4 rows)


$ # crdb-user-shards
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
   name   | consensus_rows | seqno
----------+----------------+-------
 accounts |             74 |    73
 balances |             46 |    45
 mv_01    |             31 |    30
 mv_02    |             30 |    29
 mv_03    |             29 |    28
 mv_04    |             28 |    27
 mv_05    |             27 |    26
 mv_06    |             26 |    25
 mv_07    |             25 |    24
 mv_08    |             24 |    23
 mv_09    |             23 |    22
 mv_10    |             22 |    21
 mv_11    |             21 |    20
 mv_12    |             20 |    19
 mv_13    |             19 |    18
 mv_14    |             18 |    17
 mv_15    |             17 |    16
 mv_16    |             16 |    15
 mv_17    |             15 |    14
 mv_18    |             14 |    13
 mv_19    |             13 |    12
 mv_20    |             12 |    11
(22 rows)


$ # crdb-watch
SELECT now()::TIME(0) AS t,
       string_agg(s.seqno::STRING, ' ' ORDER BY s.name) AS seqno_of_each_user_shard
FROM (SELECT l.name, max(c.sequence_number) AS seqno
      FROM demo.shard_labels l JOIN consensus.consensus c USING (shard)
      WHERE l.kind = 'user' GROUP BY l.name) AS s \watch 1
                     Fri Nov 20 18:36:28 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:36:29 | 73 45 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13 12 11
(1 row)

                     Fri Nov 20 18:36:29 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:36:30 | 73 46 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13 12
(1 row)

                     Fri Nov 20 18:36:30 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:36:31 | 73 47 32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13
(1 row)

                     Fri Nov 20 18:36:31 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:36:32 | 73 48 33 32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14
(1 row)



$ # mz-insert
INSERT INTO accounts VALUES (5, 'dave', 40);
INSERT 0 1

$ # mz-writes
UPDATE accounts SET balance = balance + 10 WHERE owner = 'bob';
DELETE FROM accounts WHERE owner = 'carol';
SELECT * FROM balances ORDER BY owner;
SELECT * FROM mv_20 ORDER BY owner;
UPDATE 1
DELETE 1
 owner | total | n
-------+-------+---
 alice |   125 | 2
 bob   |    60 | 1
 dave  |    40 | 1
(3 rows)

 owner | total
-------+-------
 alice |  2500
 bob   |  1200
 dave  |   800
(3 rows)


$ # crdb-user-shards-again
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
   name   | consensus_rows | seqno
----------+----------------+-------
 accounts |             86 |    85
 balances |             58 |    57
 mv_01    |             40 |    39
 mv_02    |             39 |    38
 mv_03    |             38 |    37
 mv_04    |             38 |    37
 mv_05    |             36 |    35
 mv_06    |             35 |    34
 mv_07    |             34 |    33
 mv_08    |             33 |    32
 mv_09    |             32 |    31
 mv_10    |             31 |    30
 mv_11    |             30 |    29
 mv_12    |             29 |    28
 mv_13    |             28 |    27
 mv_14    |             27 |    26
 mv_15    |             26 |    25
 mv_16    |             25 |    24
 mv_17    |             24 |    23
 mv_18    |             23 |    22
 mv_19    |             22 |    21
 mv_20    |             24 |    23
(22 rows)


$ # down
test/persist-resolver/demo/demo.sh down
down.
```

</details>
