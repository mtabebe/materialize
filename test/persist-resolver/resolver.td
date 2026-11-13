# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# PROTOTYPE: tables and materialized views on v2 shards served by a resolver.
# Run by test/persist-resolver/run.sh, which starts the resolver and points
# environmentd at it. That script also checks the user shards really are v2.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET persist_v2_register_user_shards = true

> CREATE TABLE t (a int, b text)

> INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')

> CREATE MATERIALIZED VIEW mv AS
  SELECT a % 2 AS parity, count(*) AS n, sum(a) AS total FROM t GROUP BY 1

> SELECT * FROM mv
0 1 2
1 2 4

> INSERT INTO t VALUES (4, 'd')

> UPDATE t SET a = a + 10 WHERE a = 1

> DELETE FROM t WHERE a = 2

> SELECT * FROM t
3 c
4 d
11 a

> SELECT * FROM mv
0 1 4
1 2 14

# A second MV reading the first: an MV's output shard is read back through the
# resolver.
> CREATE MATERIALIZED VIEW mv2 AS SELECT sum(total) AS s FROM mv

> SELECT * FROM mv2
18

> INSERT INTO t SELECT generate_series(100, 199), 'bulk'

> SELECT * FROM mv2
14968

> SELECT count(*) FROM t
103

# SUBSCRIBE runs on a cluster replica (clusterd), so it reads the v2 shards from
# another process than environmentd, which writes the table. New data reaches it
# through a listen on the shard.
$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ postgres-connect name=writer url=postgres://materialize@${testdrive.materialize-sql-addr}

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM t WHERE a >= 1000)

$ postgres-execute connection=writer
INSERT INTO t VALUES (1000, 'x')

> FETCH 1 c WITH (timeout = '30s')
<TIMESTAMP> 1 1000 x

$ postgres-execute connection=writer
INSERT INTO t VALUES (1001, 'y')

> FETCH 1 c WITH (timeout = '30s')
<TIMESTAMP> 1 1001 y

> ROLLBACK

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE mv

> FETCH 2 c WITH (timeout = '30s')
<TIMESTAMP> 1 0 52 8454
<TIMESTAMP> 1 1 53 8515

$ postgres-execute connection=writer
INSERT INTO t VALUES (2000, 'z')

> FETCH 2 c WITH (timeout = '30s')
<TIMESTAMP> -1 0 52 8454
<TIMESTAMP> 1 0 53 10454

> ROLLBACK
