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

### 7. Teardown

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

6. Teardown: `Ctrl-C` in pane 2, then the `down` block.

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
2026-11-20T18:00:48.095021Z  INFO mz_persist_resolver: resolver resolver-6833d0bb-d0ee-41c0-b90a-2a182a0ecd4e claimed partition p0 with token 1

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
1795197677844	1	bob	50	1
1795197677844	1	alice	125	2
1795197680521	1	carol	75	1

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
 builtin           |     97 |                  97 |           3781
 catalog           |      1 |                   1 |            198
 other             |      4 |                   4 |            165
 resolver-manifest |      2 |                   2 |            185
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
 partition |            127 |   255
(24 rows)


$ # views-groups
curl -s 127.0.0.1:6891/views | jq '.upper_groups'
[
  {
    "group": 0,
    "upper": [
      1795197690312
    ],
    "members": [
      "s25dcb71f-21ce-41b8-8cc2-7c72098e6bb7",
      "s2a0a0530-a93a-44f5-b38d-f85c664b2f6a",
      "s3afd30d6-f606-408f-acc5-1d0cd9ea252a",
      "s3c5144b2-40b9-4348-b0ef-f22aec7565d4",
      "s3c66a2ab-63d3-48fa-8868-7cc54fb8a68c",
      "s4031ce18-7765-4a56-b333-f522c5f9789a",
      "s4691baa3-b5fe-4f7d-9c1e-02c5856f91d4",
      "s67b4bf97-d23f-46d5-9af7-457c2566eb00",
      "s6dd2c152-9488-401f-8fa3-73ec45bca4c2",
      "s71de9458-e28e-48c5-b489-bfcd6df1be0c",
      "s82567f39-772b-4e74-a716-7a089a4660f5",
      "s839d2b0b-38c9-4d20-8466-1e55cb931da3",
      "sb21ba472-54fc-4a6b-abf0-d98ff05e9105",
      "scef09ad4-ff08-439a-8b76-65213a62e60d",
      "scfaa93ff-b40d-4224-b305-353a71c3b629",
      "sd3bf4854-01df-4ecb-bf84-775a2b39331e",
      "sd98127ae-69f3-46c1-80d6-b83106153498",
      "se876a7d8-5a37-499b-9798-d1f6b25fec53",
      "se878c147-9c00-4300-a8a1-236980e2060d",
      "sf7b289e9-6d08-4001-9707-5aa6251c7eb1",
      "sfae3b391-ce0d-4047-be8b-f4604d1203e8"
    ]
  }
]

$ # views-watch
while sleep 1; do
  curl -s 127.0.0.1:6891/views | jq -c '{seqno, groups: [.upper_groups[] | {group, upper: .upper[0], members: (.members | length)}], own_upper_rows: (.data_shards | map(select(.upper_group == null)) | length)}'
done
{"seqno":268,"groups":[{"group":0,"upper":1795197691314,"members":21}],"own_upper_rows":1}
{"seqno":270,"groups":[{"group":0,"upper":1795197692314,"members":21}],"own_upper_rows":1}
{"seqno":272,"groups":[{"group":0,"upper":1795197693314,"members":21}],"own_upper_rows":1}
{"seqno":274,"groups":[{"group":0,"upper":1795197694314,"members":21}],"own_upper_rows":1}
{"seqno":276,"groups":[{"group":0,"upper":1795197695314,"members":21}],"own_upper_rows":1}

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
1795197726473	1	bob	50	1
1795197726473	1	alice	125	2
1795197728624	1	carol	75	1

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
 builtin |     97 |                  97 |           3124
 catalog |      1 |                   1 |            201
 other   |      4 |                   4 |            169
 user    |     22 |                  22 |            551
(4 rows)


$ # crdb-user-shards
SELECT l.name, count(c.shard) AS consensus_rows, max(c.sequence_number) AS seqno
FROM demo.shard_labels l LEFT JOIN consensus.consensus c USING (shard)
WHERE l.kind IN ('user', 'resolver-manifest')
GROUP BY l.name ORDER BY l.name;
   name   | consensus_rows | seqno
----------+----------------+-------
 accounts |             75 |    74
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
                     Fri Nov 20 18:02:15 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:02:16 | 74 45 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13 12 11
(1 row)

                     Fri Nov 20 18:02:16 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:02:17 | 74 46 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13 12
(1 row)

                     Fri Nov 20 18:02:17 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:02:18 | 74 47 32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13
(1 row)

                     Fri Nov 20 18:02:18 2026 (every 1s)

    t     |                     seqno_of_each_user_shard
----------+-------------------------------------------------------------------
 18:02:19 | 74 48 33 32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14
(1 row)



$ # down
test/persist-resolver/demo/demo.sh down
down.
```

</details>
