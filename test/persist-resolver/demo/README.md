# Persist resolver demo

PROTOTYPE. A hand-run demo of the persist resolver (`src/persist-resolver`),
then the same SQL on classic persist for contrast. What it shows:

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
a write to it.

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

The rows that shard is made of. In control: `Shard`, `Schema`, `Assign` (its
partition). In the partition manifest: `Member` (it's in group 0, so it has no
`Upper` row of its own), the group's `Group(0)` row holding the upper, one
`Batch` row per batch, and one `Intent` row per reader.

### 9. One INSERT (pane 3)

<!-- run: mz-insert -->
```sql
INSERT INTO accounts VALUES (5, 'dave', 40);
```

### 10. The same shard after the INSERT (pane 4)

Rerun the `shard-logical` and `shard-rows` commands from step 8.

The logical view has one more batch, ending at the write's time, with the
MV's changed rows as its updates, and `upper` moved past it. In the rows,
there is one new `Batch` row and `Group(0)` has a new upper. `Member` is
unchanged: the MV's append landed in the same resolver write as the group's
advance, so its move out of the group and back in cancel before anything is
written (in one run, polling these rows every 20 ms across an INSERT never
showed `Member` missing). A member is only stored outside its group when its append
lands in a different resolver write from the rest of the group: the 1-2
early appenders each tick, which shows as `own_upper_rows` above 1 in step 6's
watch.

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
paste /tmp/before.txt /tmp/after.txt | awk -F '\t' 'NF == 10 {
  printf "%-9s group %s -> %-4s batches %2s -> %-3s updates %3s -> %s\n", $1, $2, $7, $4, $9, $5, $10 }'
```

Each write is appended to the table and, at the same tick, to every MV it
changed: batch and update counts go up on all 22 shards. Compaction merges a
shard's batches now and then, so a count can also go down, with the updates
merged into fewer batches. The MVs stay in the group: an append that carries
data moves a member's upper by the same tick as an empty one, so it lands in
the group's ordinary advance. The group's single upper kept ticking (compare
the `group` lines). The table stays `own`, its upper now at its last write. If
its upper equals the group's at a tick, it joins the group and leaves at the
next tick (seen with the 1 s watch from step 6 as 22 members, then 21).

### 13. Consensus after the writes (pane 4)

Rerun the step 5 `crdb-kinds` query in `psql postgres://...`: `user` still has
0 consensus rows. The writes went to batches in blob and rows in the
partition manifest shard only.

### 14. Teardown

`Ctrl-C` in panes 1 and 2, then in pane 4:

<!-- run: down -->
```sh
test/persist-resolver/demo/demo.sh down
```

## Classic run, for contrast

The same steps without the resolver:

1. Reset (pane 4): the `reset` block above.
2. No resolver.
3. environmentd without the opt-in (pane 2):

   <!-- run: environmentd-classic -->
   ```sh
   test/persist-resolver/demo/demo.sh environmentd
   ```

4. The same SQL in pane 3: `psql`, `CREATE ...`, `SUBSCRIBE` with the insert
   from pane 4, and the fan-out. Same results.
5. The same `labels`, `psql postgres://...` and two queries in pane 4. Now
   `user` has 22 shards in consensus, and each MV has its own rows and seqno.
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
   `mv_01` to `mv_20`: every MV writes its own consensus row each tick. (In the
   resolver run this query prints an empty list.) `Ctrl-C` to stop.

6. No resolver view: steps 6 to 10 and 12 have no classic counterpart.
7. Before writing, rerun the `crdb-user-shards` query (pane 4, in
   `psql postgres://...`) for each shard's consensus rows and seqno.
8. The same writes in pane 3 (the `mz-insert` and `mz-writes` blocks), with
   the same results.
9. Rerun `crdb-user-shards`: every user shard has more consensus rows and a
   higher seqno. Each of the writes was a compare-and-set on the table's and
   on each changed MV's own row. Most of the growth, though, is the once a
   second upper advance every shard does on its own row (the `\watch` in
   step 5), which in the resolver run is one group row.
10. Teardown: `Ctrl-C` in pane 2, then the `down` block.

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
2026-11-20T18:22:23.084858Z  INFO mz_persist_resolver: resolver resolver-145f4f29-8052-4028-9c34-9bb0d061578b claimed partition p0 with token 1

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
1795198972049	1	bob	50	1
1795198972049	1	alice	125	2
1795198974450	1	carol	75	1

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
 builtin           |     97 |                  97 |           3849
 catalog           |      1 |                   1 |            204
 other             |      4 |                   4 |            170
 resolver-manifest |      2 |                   2 |            182
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
 control   |             57 |    56
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
 partition |            125 |   253
(24 rows)


$ # views-groups
curl -s 127.0.0.1:6891/views | jq '.upper_groups'
[
  {
    "group": 0,
    "upper": [
      1795198984865
    ],
    "members": [
      "s0fb3f8b6-bff4-45de-84ec-255ceb065572",
      "s10c769af-5c1d-4c7c-86a4-41ece3359884",
      "s2afe27d4-e8b0-49cd-9de0-a7bdf19eea8a",
      "s2e4850b8-e107-4c0f-9c22-befe7985d0bf",
      "s593e09d4-2fd8-4828-834f-ad720b60026a",
      "s6ac9151c-a009-4038-9aad-c0eabc353905",
      "s6cc966d1-d9cb-4c3f-9dad-611f0b9ca453",
      "s73ec1b99-6fac-44ff-bb4f-97284ef99169",
      "s78d150e0-d3f7-4fbb-8b35-4e5648c006ec",
      "s8a80f52b-0af5-497a-889a-fc1a3f6bf139",
      "s8af02543-b730-4a77-b92f-0f403a0f2b74",
      "s927e58d5-6774-43ae-91c5-d8ba7d7c46ed",
      "sa0ffa0a0-766d-49bc-9a6e-7fa5b51b84c1",
      "safee8aa2-ec2c-4a75-a745-4d2ebaa61944",
      "sb92587d8-9b5d-4fbc-985a-bcfd6183dc2e",
      "scef3a53f-8fe3-4ce5-a0cf-88101dcc16d8",
      "sd700496f-6151-4d81-b4c1-9bb06eea7b45",
      "se2a6308e-5106-4d88-9e11-72e1a145f62e",
      "sef32a78c-20a5-4a7d-bed4-a588d32e292c",
      "sf41fc44c-130e-45ec-b84b-a9c83fa3a91c",
      "sf859d568-e96d-4d95-9f97-0e1656f6776f"
    ]
  }
]

$ # views-watch
while sleep 1; do
  curl -s 127.0.0.1:6891/views | jq -c '{seqno, groups: [.upper_groups[] | {group, upper: .upper[0], members: (.members | length)}], own_upper_rows: (.data_shards | map(select(.upper_group == null)) | length)}'
done
{"seqno":267,"groups":[{"group":0,"upper":1795198985866,"members":21}],"own_upper_rows":1}
{"seqno":269,"groups":[{"group":0,"upper":1795198986868,"members":21}],"own_upper_rows":1}
{"seqno":272,"groups":[{"group":0,"upper":1795198987865,"members":21}],"own_upper_rows":1}
{"seqno":274,"groups":[{"group":0,"upper":1795198988866,"members":21}],"own_upper_rows":1}
{"seqno":276,"groups":[{"group":0,"upper":1795198989867,"members":21}],"own_upper_rows":1}

$ # snapshot-before
curl -s 127.0.0.1:6891/views | jq -r --slurpfile n scratch/persist-resolver-demo/names.json '
  (.upper_groups[] | "group \(.group): upper \(.upper[0]), \(.members | length) members"),
  (.data_shards | map([$n[0][.shard], (.upper_group // "own"), .upper[0], (.batches | length),
    ([.batches[].updates] | add // 0)]) | sort[] | @tsv)' | tee /tmp/before.txt
group 0: upper 1795198990866, 21 members
accounts	own	1795198977001	2	4
balances	0	1795198990866	2	3
mv_01	0	1795198990866	1	3
mv_02	0	1795198990866	1	3
mv_03	0	1795198990866	1	3
mv_04	0	1795198990866	1	3
mv_05	0	1795198990866	1	3
mv_06	0	1795198990866	1	3
mv_07	0	1795198990866	1	3
mv_08	0	1795198990866	1	3
mv_09	0	1795198990866	1	3
mv_10	0	1795198990866	1	3
mv_11	0	1795198990866	1	3
mv_12	0	1795198990866	1	3
mv_13	0	1795198990866	1	3
mv_14	0	1795198990866	1	3
mv_15	0	1795198990866	1	3
mv_16	0	1795198990866	1	3
mv_17	0	1795198990866	1	3
mv_18	0	1795198990866	1	3
mv_19	0	1795198990866	1	3
mv_20	0	1795198990866	1	3

$ # shard-id
S=$(psql -h localhost -p 6875 -U materialize -At -c "SELECT s.shard_id FROM mz_internal.mz_storage_shards s JOIN mz_materialized_views m ON s.object_id = m.id WHERE m.name = 'mv_01'"); echo "$S"
s6cc966d1-d9cb-4c3f-9dad-611f0b9ca453

$ # shard-logical
curl -s 127.0.0.1:6891/views/$S | jq '{shard, upper, upper_group, since,
  intents: [.intents[] | {reader, frontier}],
  batches: [.batches[] | {lower: .lower[0], upper: .upper[0], since: .since[0], updates}]}'
{
  "shard": "s6cc966d1-d9cb-4c3f-9dad-611f0b9ca453",
  "upper": [
    1795198990866
  ],
  "upper_group": 0,
  "since": [
    1795198977000
  ],
  "intents": [
    {
      "reader": "c00000000-1111-2222-3333-444444444444",
      "frontier": [
        1795198977000
      ]
    },
    {
      "reader": "r653be9d4-6cf7-4124-ba23-747c7ec207a4",
      "frontier": [
        1795198977000
      ]
    }
  ],
  "batches": [
    {
      "lower": 1795198977000,
      "upper": 1795198978757,
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
{"manifest":"partition","key":"Group(0)","val":[1795198991866]}
{"manifest":"partition","key":"Member","val":0}
{"manifest":"partition","key":"Batch","val":{"lower":[1795198977000],"upper":[1795198978757],"since":[0],"updates":3,"parts":1}}
{"manifest":"partition","key":"Intent(c00000000-1111-2222-3333-444444444444)","val":{"frontier":[1795198977000],"opaque":2}}
{"manifest":"partition","key":"Intent(r653be9d4-6cf7-4124-ba23-747c7ec207a4)","val":{"frontier":[1795198977000],"opaque":null}}

$ # mz-insert
INSERT INTO accounts VALUES (5, 'dave', 40);
INSERT 0 1

$ # shard-logical
curl -s 127.0.0.1:6891/views/$S | jq '{shard, upper, upper_group, since,
  intents: [.intents[] | {reader, frontier}],
  batches: [.batches[] | {lower: .lower[0], upper: .upper[0], since: .since[0], updates}]}'
{
  "shard": "s6cc966d1-d9cb-4c3f-9dad-611f0b9ca453",
  "upper": [
    1795198994036
  ],
  "upper_group": 0,
  "since": [
    1795198977000
  ],
  "intents": [
    {
      "reader": "c00000000-1111-2222-3333-444444444444",
      "frontier": [
        1795198977000
      ]
    },
    {
      "reader": "r653be9d4-6cf7-4124-ba23-747c7ec207a4",
      "frontier": [
        1795198977000
      ]
    }
  ],
  "batches": [
    {
      "lower": 1795198977000,
      "upper": 1795198978757,
      "since": 0,
      "updates": 3
    },
    {
      "lower": 1795198991866,
      "upper": 1795198992035,
      "since": 0,
      "updates": 1
    }
  ]
}

$ # shard-rows
curl -s "127.0.0.1:6891/views/$S?rows" | jq -c '.rows[]'
{"manifest":"control","key":"Shard","val":{"finalized":false}}
{"manifest":"control","key":"Schema(0)","val":"..."}
{"manifest":"control","key":"Assign","val":"p0"}
{"manifest":"partition","key":"Group(0)","val":[1795198994036]}
{"manifest":"partition","key":"Member","val":0}
{"manifest":"partition","key":"Batch","val":{"lower":[1795198977000],"upper":[1795198978757],"since":[0],"updates":3,"parts":1}}
{"manifest":"partition","key":"Batch","val":{"lower":[1795198991866],"upper":[1795198992035],"since":[0],"updates":1,"parts":1}}
{"manifest":"partition","key":"Intent(c00000000-1111-2222-3333-444444444444)","val":{"frontier":[1795198977000],"opaque":2}}
{"manifest":"partition","key":"Intent(r653be9d4-6cf7-4124-ba23-747c7ec207a4)","val":{"frontier":[1795198977000],"opaque":null}}

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
group 0: upper 1795198997036, 21 members
accounts	own	1795198995038	5	8
balances	0	1795198997036	5	7
mv_01	0	1795198997036	4	7
mv_02	0	1795198997036	4	7
mv_03	0	1795198997036	4	7
mv_04	0	1795198997036	4	7
mv_05	0	1795198997036	4	7
mv_06	0	1795198997036	4	7
mv_07	0	1795198997036	4	7
mv_08	0	1795198997036	4	7
mv_09	0	1795198997036	4	7
mv_10	0	1795198997036	4	7
mv_11	0	1795198997036	4	7
mv_12	0	1795198997036	4	7
mv_13	0	1795198997036	4	7
mv_14	0	1795198997036	4	7
mv_15	0	1795198997036	4	7
mv_16	0	1795198997036	4	7
mv_17	0	1795198997036	4	7
mv_18	0	1795198997036	4	7
mv_19	0	1795198997036	4	7
mv_20	0	1795198997036	4	7

$ # batch-diff
paste /tmp/before.txt /tmp/after.txt | awk -F '\t' 'NF == 10 {
  printf "%-9s group %s -> %-4s batches %2s -> %-3s updates %3s -> %s\n", $1, $2, $7, $4, $9, $5, $10 }'
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

$ # crdb-kinds
SELECT l.kind,
       count(DISTINCT l.shard) AS shards,
       count(DISTINCT c.shard) AS shards_in_consensus,
       count(c.shard) AS consensus_rows
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
GROUP BY l.kind ORDER BY l.kind;
       kind        | shards | shards_in_consensus | consensus_rows
-------------------+--------+---------------------+----------------
 builtin           |     97 |                  97 |           5030
 catalog           |      1 |                   1 |            221
 other             |      4 |                   4 |            202
 resolver-manifest |      2 |                   2 |            113
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
1795199026791	1	bob	50	1
1795199026791	1	alice	125	2
1795199028952	1	carol	75	1

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
 builtin |     97 |                  97 |           3259
 catalog |      1 |                   1 |            199
 other   |      4 |                   4 |            175
 user    |     22 |                  22 |            555
(4 rows)


$ # crdb-user-shards
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
   name   | consensus_rows | seqno
----------+----------------+-------
 accounts |             75 |    74
 balances |             48 |    47
 mv_01    |             31 |    30
 mv_02    |             30 |    29
 mv_03    |             29 |    28
 mv_04    |             29 |    28
 mv_05    |             27 |    26
 mv_06    |             26 |    25
 mv_07    |             25 |    24
 mv_08    |             24 |    23
 mv_09    |             23 |    22
 mv_10    |             22 |    21
 mv_11    |             22 |    21
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
                     Fri Nov 20 18:23:55 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:23:56 | 74 47 30 29 28 28 26 25 24 23 22 21 21 19 18 17 16 15 14 13 12 11
(1 row)

                     Fri Nov 20 18:23:56 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:23:57 | 74 48 31 30 29 29 27 26 25 24 23 22 22 20 19 18 17 16 15 14 13 12
(1 row)

                     Fri Nov 20 18:23:57 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:23:58 | 74 49 32 31 30 30 28 27 26 25 24 23 23 21 20 19 18 17 16 15 14 13
(1 row)

                     Fri Nov 20 18:23:58 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:23:59 | 74 50 33 32 31 31 29 28 27 26 25 24 24 22 21 20 19 18 17 16 15 14
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


$ # crdb-user-shards
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
   name   | consensus_rows | seqno
----------+----------------+-------
 accounts |             86 |    85
 balances |             60 |    59
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
 mv_11    |             31 |    30
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
