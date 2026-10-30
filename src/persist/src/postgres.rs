// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by Postgres.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::PoolError;
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use futures_util::StreamExt;
use itertools::Itertools;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::url::SensitiveUrl;
use mz_postgres_client::metrics::PostgresClientMetrics;
use mz_postgres_client::{
    Connection, IsolationLevel, PostgresClient, PostgresClientConfig, PostgresClientKnobs,
};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Row, Statement};
use tracing::{info, warn};

use crate::cas_batch::{BatchLimits, CasBackend, CasBatcher, CasOp, RangeId, RangeMap};
use crate::error::Error;
use crate::location::{
    CaSResult, Consensus, ExternalError, ResultStream, SCAN_ALL, SeqNo, VersionedData,
};
use crate::metrics::ConsensusBatchMetrics;
use crate::scan_batch::ScanBatcher;

/// Flag to run PostgreSQL consensus connections under READ COMMITTED isolation instead of SERIALIZABLE.
///
/// The query family used on vanilla Postgres is linearizable under READ COMMITTED isolation and
/// under SERIALIZABLE isolation, and remains linearizable when some connections run under each,
/// so this flag can be flipped freely once every process runs this query family. Every operation
/// of the family is a single statement, and a single-statement transaction takes its snapshot at
/// the same instant under either isolation level, so the only difference between the levels is
/// that SERIALIZABLE may additionally abort a statement with a serialization failure, which the
/// caller retries. The flag only exists to make the upgrade path from older versions safe since
/// multiple query versions co-exist during a 0dt upgrade. See the note on the default value for
/// more information.
///
/// This flag should stay off on CockroachDB. The system will refuse to issue consensus queries if
/// the flag is enabled on CockroachDB.
pub const PG_CONSENSUS_READ_COMMITTED: mz_dyncfg::Config<bool> = mz_dyncfg::Config::new(
    "persist_pg_consensus_read_committed",
    // NOTE: The default value of this flag *MUST* remain *false* for as long as there are users
    // running Materialize versions <=v26.32. Environments running one of these versions will be
    // issuing consensus queries that are only correct in SERIALIZABLE isolation. Therefore this
    // flag must default to off so that during the 0dt deployment that takes an environment from
    // <=v26.32 to >=v26.33 the consensus queries of both families linearize together.
    false,
    "Run consensus connections under READ COMMITTED isolation instead of SERIALIZABLE when targetting
    PostgreSQL backends. This flag must be off when targetting CockroachDB.",
    mz_dyncfg::ParameterScope::Environment,
);

/// Whether `compare_and_set` goes through the batcher in [crate::cas_batch].
///
/// Off runs the single-row CaS queries exactly as before the batcher existed. Batched and
/// single-row statements write identical rows, so processes with different values of this flag
/// can share one consensus table.
pub const CONSENSUS_CAS_BATCH_ENABLED: mz_dyncfg::Config<bool> = mz_dyncfg::Config::new(
    "persist_consensus_cas_batch_enabled",
    false,
    "Coalesce concurrent consensus compare-and-set calls into one statement per CockroachDB range \
    (Materialize).",
    mz_dyncfg::ParameterScope::Environment,
);

// NOTE: every in-flight statement holds a pooled connection, so this times the number of hot
// ranges must stay well below `persist_consensus_connection_pool_max_size`, else callers block
// acquiring connections.
pub(crate) const CONSENSUS_CAS_BATCH_MAX_IN_FLIGHT_PER_RANGE: mz_dyncfg::Config<usize> =
    mz_dyncfg::Config::new(
        "persist_consensus_cas_batch_max_in_flight_per_range",
        4,
        "Batched consensus compare-and-set statements in flight per CockroachDB range before new \
        calls wait to join the next statement (Materialize).",
        mz_dyncfg::ParameterScope::Environment,
    );

pub(crate) const CONSENSUS_CAS_BATCH_MAX_ROWS: mz_dyncfg::Config<usize> = mz_dyncfg::Config::new(
    "persist_consensus_cas_batch_max_rows",
    128,
    "Maximum compare-and-set calls in one batched consensus statement (Materialize).",
    mz_dyncfg::ParameterScope::Environment,
);

pub(crate) const CONSENSUS_CAS_BATCH_MAX_BYTES: mz_dyncfg::Config<usize> = mz_dyncfg::Config::new(
    "persist_consensus_cas_batch_max_bytes",
    1024 * 1024,
    "Maximum total data bytes in one batched consensus statement. A single larger call still goes \
    out alone (Materialize).",
    mz_dyncfg::ParameterScope::Environment,
);

pub(crate) const CONSENSUS_CAS_BATCH_RANGE_REFRESH_INTERVAL: mz_dyncfg::Config<Duration> =
    mz_dyncfg::Config::new(
        "persist_consensus_cas_batch_range_refresh_interval",
        Duration::from_secs(300),
        "How often the consensus compare-and-set batcher re-reads the consensus table's \
        CockroachDB range boundaries (Materialize).",
        mz_dyncfg::ParameterScope::Environment,
    );

/// Whether unlimited `scan`s go through the batcher in [crate::scan_batch].
///
/// A batched scan is a read-only statement over the existing table, so processes with different
/// values of this flag can share one consensus table.
pub const CONSENSUS_SCAN_BATCH_ENABLED: mz_dyncfg::Config<bool> = mz_dyncfg::Config::new(
    "persist_consensus_scan_batch_enabled",
    false,
    "Coalesce concurrent unlimited consensus scans into one statement (Materialize).",
    mz_dyncfg::ParameterScope::Environment,
);

// NOTE: every in-flight statement holds a pooled connection.
pub(crate) const CONSENSUS_SCAN_BATCH_MAX_IN_FLIGHT: mz_dyncfg::Config<usize> =
    mz_dyncfg::Config::new(
        "persist_consensus_scan_batch_max_in_flight",
        4,
        "Batched consensus scan statements in flight per process before new scans wait to join \
        the next statement (Materialize).",
        mz_dyncfg::ParameterScope::Environment,
    );

pub(crate) const CONSENSUS_SCAN_BATCH_MAX_SHARDS: mz_dyncfg::Config<usize> = mz_dyncfg::Config::new(
    "persist_consensus_scan_batch_max_shards",
    128,
    "Maximum shards read by one batched consensus scan statement, at least 8 (Materialize).",
    mz_dyncfg::ParameterScope::Environment,
);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS consensus (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY(shard, sequence_number)
)
";

// These `sql_stats_automatic_collection_enabled` are for the cost-based
// optimizer but all the queries against this table are single-table and very
// carefully tuned to hit the primary index, so the cost-based optimizer doesn't
// really get us anything. OTOH, the background jobs that crdb creates to
// collect these stats fill up the jobs table (slowing down all sorts of
// things).
const CRDB_SCHEMA_OPTIONS: &str = "WITH (sql_stats_automatic_collection_enabled = false)";
// The `consensus` table creates and deletes rows at a high frequency, generating many
// tombstoned rows. If Cockroach's GC interval is set high (the default is 25h) and
// these tombstones accumulate, scanning over the table will take increasingly and
// prohibitively long.
//
// See: https://github.com/MaterializeInc/database-issues/issues/4001
// See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
const CRDB_CONFIGURE_ZONE: &str = "ALTER TABLE consensus CONFIGURE ZONE USING gc.ttlseconds = 600";

/// NOTE: `mz-persist` intentionally does not depend on `mz-postgres-util`.
/// These helpers are the only direct driver-call boundary in this module.
async fn pg_batch_execute(client: &Connection, query: &str) -> Result<(), tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.batch_execute(query).await
}

async fn pg_query_prepared(
    client: &Connection,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query(statement, params).await
}

async fn pg_query_opt_prepared(
    client: &Connection,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query_opt(statement, params).await
}

async fn pg_execute_prepared(
    client: &Connection,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.execute(statement, params).await
}

impl ToSql for SeqNo {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // We can only represent sequence numbers in the range [0, i64::MAX].
        let value = i64::try_from(self.0)?;
        <i64 as ToSql>::to_sql(&value, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <i64 as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for SeqNo {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<SeqNo, Box<dyn std::error::Error + Sync + Send>> {
        let sequence_number = <i64 as FromSql>::from_sql(ty, raw)?;

        // Sanity check that the sequence number we received falls in the
        // [0, i64::MAX] range.
        let sequence_number = u64::try_from(sequence_number)?;
        Ok(SeqNo(sequence_number))
    }

    fn accepts(ty: &Type) -> bool {
        <i64 as FromSql>::accepts(ty)
    }
}

/// Configuration to connect to a Postgres backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct PostgresConsensusConfig {
    url: SensitiveUrl,
    knobs: Arc<dyn PostgresClientKnobs>,
    metrics: PostgresClientMetrics,
    batch_metrics: ConsensusBatchMetrics,
    dyncfg: Arc<ConfigSet>,
}

impl PostgresConsensusConfig {
    const EXTERNAL_TESTS_POSTGRES_URL: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL";

    /// Returns a new [PostgresConsensusConfig] for use in production.
    pub fn new(
        url: &SensitiveUrl,
        knobs: Box<dyn PostgresClientKnobs>,
        metrics: PostgresClientMetrics,
        batch_metrics: ConsensusBatchMetrics,
        dyncfg: Arc<ConfigSet>,
    ) -> Result<Self, Error> {
        Ok(PostgresConsensusConfig {
            url: url.clone(),
            knobs: Arc::from(knobs),
            metrics,
            batch_metrics,
            dyncfg,
        })
    }

    /// Returns a new [PostgresConsensusConfig] for use in unit tests.
    ///
    /// By default, persist tests that use external storage (like Postgres) are
    /// no-ops so that `cargo test` works on new environments without any
    /// configuration. To activate the tests for [PostgresConsensus] set the
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL` environment variable
    /// with a valid connection url [1].
    ///
    /// [1]: https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url
    pub fn new_for_test() -> Result<Option<Self>, Error> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => SensitiveUrl::from_str(&url).map_err(|e| e.to_string())?,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        struct TestConsensusKnobs;
        impl std::fmt::Debug for TestConsensusKnobs {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestConsensusKnobs").finish_non_exhaustive()
            }
        }
        impl PostgresClientKnobs for TestConsensusKnobs {
            fn connection_pool_max_size(&self) -> usize {
                2
            }

            fn connection_pool_max_wait(&self) -> Option<Duration> {
                Some(Duration::from_secs(1))
            }

            fn connection_pool_ttl(&self) -> Duration {
                Duration::MAX
            }
            fn connection_pool_ttl_stagger(&self) -> Duration {
                Duration::MAX
            }
            fn connect_timeout(&self) -> Duration {
                Duration::MAX
            }
            fn tcp_user_timeout(&self) -> Duration {
                Duration::ZERO
            }

            fn keepalives_idle(&self) -> Duration {
                Duration::from_secs(10)
            }

            fn keepalives_interval(&self) -> Duration {
                Duration::from_secs(5)
            }

            fn keepalives_retries(&self) -> u32 {
                5
            }

            fn statement_timeout(&self) -> Duration {
                Duration::ZERO
            }
        }

        let dyncfg = crate::cfg::all_dyn_configs(ConfigSet::default());
        let registry = MetricsRegistry::new();
        let config = PostgresConsensusConfig::new(
            &url,
            Box::new(TestConsensusKnobs),
            PostgresClientMetrics::new(&registry, "mz_persist"),
            ConsensusBatchMetrics::new(&registry),
            Arc::new(dyncfg),
        )?;
        Ok(Some(config))
    }
}

/// What flavor of Postgres are we connected to for consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresMode {
    /// CockroachDB, used in our cloud offering.
    CockroachDB,
    /// Vanilla Postgres, the default for our self-hosted offering.
    Postgres,
}

/// Implementation of [Consensus] over a Postgres database.
pub struct PostgresConsensus {
    client: PostgresConsensusClient,
    cfg: Arc<ConfigSet>,
    batcher: CasBatcher<PostgresCasBackend>,
    scan_batcher: ScanBatcher,
    _range_refresh: Option<AbortOnDropHandle<()>>,
}

/// The connection pool and backend flavor, shared with the batchers.
#[derive(Clone, Debug)]
pub(crate) struct PostgresConsensusClient {
    postgres_client: Arc<PostgresClient>,
    mode: PostgresMode,
}

impl std::fmt::Debug for PostgresConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConsensus").finish_non_exhaustive()
    }
}

impl PostgresConsensus {
    /// Open a Postgres [Consensus] instance with `config`, for the collection
    /// named `shard`.
    pub async fn open(config: PostgresConsensusConfig) -> Result<Self, ExternalError> {
        // don't need to unredact here because we just want to pull out the username
        let pg_config: Config = config.url.to_string().parse()?;
        let role = pg_config.get_user().expect("failed to get PostgreSQL user");
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS consensus AUTHORIZATION {}",
            escape_identifier(role),
        );

        let cfg = Arc::clone(&config.dyncfg);
        let dyncfg = Arc::clone(&config.dyncfg);

        // The resolver runs per connection, so a flag change takes effect as the pool cycles
        // connections. It unconditionally follows the flag: it does not know the backend. The
        // backend-specific safety check lives in `get_connection`, which asserts that CockroachDB
        // connections are SERIALIZABLE. The connection carries the level it was created with, so
        // that assertion is exact rather than a guess about what the resolver returned.
        let client_config = PostgresClientConfig::new(config.url, config.knobs, config.metrics)
            .with_isolation(Arc::new(move || {
                if PG_CONSENSUS_READ_COMMITTED.get(&dyncfg) {
                    IsolationLevel::ReadCommitted
                } else {
                    IsolationLevel::Serializable
                }
            }));
        let postgres_client = PostgresClient::open(client_config)?;

        let client = postgres_client.get_connection().await?;

        let mode = match pg_batch_execute(
            &client,
            &format!(
                "{}; {}{}; {};",
                create_schema, SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
            ),
        )
        .await
        {
            Ok(()) => PostgresMode::CockroachDB,
            Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
                warn!(
                    "unable to ALTER TABLE consensus, this is expected and OK when connecting with a read-only user"
                );
                PostgresMode::CockroachDB
            }
            // Vanilla Postgres doesn't support the Cockroach zone configuration
            // that we attempted, so we use that to determine what mode we're in.
            Err(e)
                if e.code() == Some(&SqlState::INVALID_PARAMETER_VALUE)
                    || e.code() == Some(&SqlState::SYNTAX_ERROR) =>
            {
                info!(
                    "unable to initiate consensus with CRDB params, this is expected and OK when running against Postgres: {:?}",
                    e
                );
                PostgresMode::Postgres
            }
            Err(e) => return Err(e.into()),
        };

        match mode {
            PostgresMode::CockroachDB => {}
            PostgresMode::Postgres => {
                pg_batch_execute(&client, &format!("{}; {};", create_schema, SCHEMA)).await?;
            }
        }

        let consensus_client = PostgresConsensusClient {
            postgres_client: Arc::new(postgres_client),
            mode,
        };
        let ranges = Arc::new(RwLock::new(RangeMap::default()));
        let range_refresh = match mode {
            PostgresMode::CockroachDB => Some(
                mz_ore::task::spawn(
                    || "persist::consensus::cas_batch_range_refresh",
                    refresh_ranges(
                        consensus_client.clone(),
                        Arc::clone(&cfg),
                        Arc::clone(&ranges),
                        config.batch_metrics.clone(),
                    ),
                )
                .abort_on_drop(),
            ),
            PostgresMode::Postgres => None,
        };
        let batcher = CasBatcher::new(
            PostgresCasBackend {
                client: consensus_client.clone(),
                cfg: Arc::clone(&cfg),
                ranges,
            },
            config.batch_metrics.clone(),
        );
        let scan_batcher = ScanBatcher::new(
            consensus_client.clone(),
            Arc::clone(&cfg),
            config.batch_metrics,
        );
        Ok(PostgresConsensus {
            client: consensus_client,
            cfg,
            batcher,
            scan_batcher,
            _range_refresh: range_refresh,
        })
    }

    /// Drops and recreates the `consensus` table in Postgres
    ///
    /// ONLY FOR TESTING
    pub async fn drop_and_recreate(&self) -> Result<(), ExternalError> {
        // this could be a TRUNCATE if we're confident the db won't reuse any state
        let client = self.get_connection().await?;
        pg_batch_execute(&client, "DROP TABLE consensus").await?;
        let crdb_mode = match pg_batch_execute(
            &client,
            &format!("{}{}; {}", SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,),
        )
        .await
        {
            Ok(()) => true,
            Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
                warn!(
                    "unable to ALTER TABLE consensus, this is expected and OK when connecting with a read-only user"
                );
                true
            }
            Err(e)
                if e.code() == Some(&SqlState::INVALID_PARAMETER_VALUE)
                    || e.code() == Some(&SqlState::SYNTAX_ERROR) =>
            {
                info!(
                    "unable to initiate consensus with CRDB params, this is expected and OK when running against Postgres: {:?}",
                    e
                );
                false
            }
            Err(e) => return Err(e.into()),
        };

        if !crdb_mode {
            pg_batch_execute(&client, SCHEMA).await?;
        }
        Ok(())
    }

    async fn get_connection(&self) -> Result<Connection, PoolError> {
        self.client.get_connection().await
    }
}

impl PostgresConsensusClient {
    async fn get_connection(&self) -> Result<Connection, PoolError> {
        let conn = self.postgres_client.get_connection().await?;
        // On CockroachDB we always run the lockless `CRDB_*` queries, which are only linearizable
        // under SERIALIZABLE. The connection records the level it was created with, so we assert on
        // that exact level rather than trusting the isolation flag to be off. The `POSTGRES_*`
        // queries used on vanilla Postgres are correct under any isolation, so that backend needs
        // no check.
        if self.mode == PostgresMode::CockroachDB {
            assert_eq!(
                conn.isolation_level(),
                IsolationLevel::Serializable,
                "consensus on CockroachDB requires SERIALIZABLE isolation, refusing to run \
                 CRDB_* queries under {:?}",
                conn.isolation_level(),
            );
        }
        Ok(conn)
    }

    /// Runs one compare-and-set as its own statement.
    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let expected = new.seqno.previous();

        let result = match expected {
            Some(expected) => {
                /// This query has been written to execute within a single
                /// network round-trip. The insert performance has been tuned
                /// against CockroachDB, ensuring it goes through the fast-path
                /// 1-phase commit of CRDB. Any changes to this query should
                /// confirm an EXPLAIN ANALYZE (VERBOSE) query plan contains
                /// `auto commit`
                static CRDB_CAS_QUERY: &str = "
                    INSERT INTO consensus (shard, sequence_number, data)
                    SELECT $1, $2, $3
                    WHERE (SELECT sequence_number FROM consensus
                        WHERE shard = $1
                        ORDER BY sequence_number DESC LIMIT 1) = $4;
                ";

                // ## Correctness argument
                //
                // The Postgres tuned queries are designed to be correct under READ COMMITTED
                // isolation. In that mode each operation sees its own snapshot of the database and
                // special care is needed to ensure that the observable behavior is linearizable.
                //
                // The whole argument rests on one invariant: the live sequence numbers `>= 0` form
                // a contiguous range with no gaps, whose maximum is the head. Appends only ever
                // extend the head by one and truncation only ever removes a prefix, preserving
                // contiguity. (The `-1` sentinel written at init sits below all appends and never
                // participates in any test here, so we ignore it.) The cases below rely on the
                // following equivalence:
                //
                //        `seqno` is the head iff `seqno` is present and `seqno+1` is absent.
                //
                // A client performs CaS operations at a seqno one above the seqno it has already
                // observed, unless it is initializing the shard for the first time, in which case it
                // does a plain insert. The two scenarios are analyzed separately:
                //
                // 1. CaS for `expected_seqno+1` issued after `expected_seqno` was observed
                //
                // Because `expected_seqno` was observed, the consensus table must have contained a
                // row with it at some point, the `expected_row`.
                //
                // The first operation of the CaS query is to find `expected_row` and lock it with
                // a `FOR KEY SHARE` lock. The `expected_row` may or may not still exist, depending on
                // how outdated the client is. If `expected_seqno` is the current head the row is
                // guaranteed to exist, since truncation never deletes the head. This gives two cases:
                //
                // 1.1. `expected_row` exists
                //
                // The INSERT is the linearization point of this CaS, and it commits a row iff
                // `expected_seqno` is the head at the instant the INSERT runs. The held lock keeps
                // `expected_seqno` present at that instant (see 1.1.3). By contiguity it is then
                // the head iff `expected_seqno+1` is absent. This is exactly what the PRIMARY KEY
                // tests as the INSERT tries to write `expected_seqno+1` (`$2`). So the INSERT
                // writes one row and the call returns Committed (advancing the head to
                // `expected_seqno+1`, the range still contiguous) precisely when `expected_seqno`
                // was the head; otherwise the PK raises `unique_violation` and the call returns
                // ExpectationMismatch with the table unchanged. The remaining obligation is that
                // operations interleaving between the lock and the INSERT cannot break this. There
                // are three cases:
                //
                // 1.1.1 Another CaS has taken its own `expected_row_2` lock but not inserted yet.
                //
                // `FOR KEY SHARE` is shared, so the two locks neither block each other nor wait:
                // locks never serialize appenders, the PRIMARY KEY does. Having only locked, the
                // other CaS has not changed the table, so it does not affect what this INSERT
                // observes. If it is racing for the same head (`expected_row_2 == expected_row`) both
                // appenders attempt to INSERT the same `expected_seqno+1`, and the PK admits exactly
                // one. Whichever INSERT commits first is linearized first and becomes the head; the
                // other then finds `expected_seqno+1` present and is rejected — case 1.1.2 from its
                // side.
                //
                // 1.1.2 Another CaS has performed its `expected_seqno+1` insertion.
                //
                // By contiguity an append always targets head+1, so the only insertion that can
                // affect this one is `expected_seqno+1` itself: nothing above it can be added while
                // `expected_seqno+1` is still absent. If such an insert commits first, then
                // `expected_seqno` is no longer the head, and this INSERT now finds `expected_seqno+1`
                // present and is rejected by the PK → ExpectationMismatch. That is correct: this CaS
                // is linearized after the other appender, and at that point it must fail.
                //
                // 1.1.3 A truncation has happened.
                //
                // A truncation deletes a prefix `[0, cut)` and never the head. Its DELETE takes a
                // `FOR UPDATE`-strength row lock on each row it removes, and that conflicts with the
                // `FOR KEY SHARE` held on `expected_seqno`. So no committed truncation can have
                // removed `expected_seqno` while that lock is held: any truncation that commits in
                // this window has `cut <= expected_seqno` and deletes only rows strictly below
                // `expected_seqno`. That leaves `expected_seqno` present and the presence/absence of
                // `expected_seqno+1` untouched, so it does not change the INSERT's outcome, and
                // removing a low prefix keeps the range contiguous. This is also why, once locked,
                // `expected_row` is still present at INSERT time, closing the "lock found a row, then
                // it was GC'd before the INSERT" hole.
                //
                // 1.2. `expected_row` does not exist
                //
                // The CTE is empty, the INSERT touches zero rows, and the call returns
                // ExpectationMismatch without modifying the table. This is correct: `expected_seqno`
                // was observed, so it was present once, and the only way a present seqno later
                // becomes absent is truncation (inserts never delete). A truncation that removed
                // `expected_seqno` advanced the head strictly above it (it removes a prefix and keeps
                // the head), so `expected_seqno` is not the head and the CaS must fail. This operation
                // linearizes at its locking SELECT, where `expected_seqno` is already gone.
                //
                // 2. CaS that initializes the shard, issued with `expected` = None
                //
                // The init path (see the `None` arm) is a single statement that inserts the `-1`
                // marker and seqno 0, guarded by `NOT EXISTS` over the shard.
                //
                // Against a shard this CaS could be appending to, the existence guard is false and
                // the init affects no rows. Against an empty shard it inserts both rows
                // atomically, and there is no append to interleave with.
                //
                // Concurrent first-time inits block on the uncommitted `-1` marker and the loser
                // is rejected by the PK, so exactly one wins.
                //
                // Truncation does not remove the marker, so a delayed init cannot re-use seqno 0
                // if a gap is left by truncation.
                static POSTGRES_CAS_QUERY: &str = "
                WITH expected_row AS (
                    SELECT sequence_number FROM consensus
                    WHERE shard = $1 AND sequence_number = $4
                    FOR KEY SHARE
                )
                INSERT INTO consensus (shard, sequence_number, data)
                SELECT $1, $2, $3
                FROM expected_row;
                ";

                let q = match self.mode {
                    PostgresMode::CockroachDB => CRDB_CAS_QUERY,
                    PostgresMode::Postgres => POSTGRES_CAS_QUERY,
                };
                let client = self.get_connection().await?;
                let statement = client.prepare_cached(q).await?;
                pg_execute_prepared(
                    &client,
                    &statement,
                    &[&key, &new.seqno, &new.data.as_ref(), &expected],
                )
                .await
            }
            None => {
                match self.mode {
                    PostgresMode::Postgres => {
                        // SUBTLE: This query is designed to be linearizable with respect to:
                        // * the other POSTGRES queries (cas/truncate) present in this file, under
                        //   READ COMMITTED isolation, under SERIALIZABLE isolation, and under a
                        //   mixture of the two
                        // * the CRDB and POSTGRES queries that existed in Materialize versions
                        //   <=v26.30 and run under SERIALIZABLE isolation
                        // * the POSTGRES queries that were introduced in
                        //   d6dff42fd69d1a87edc4ae99e7ea1364201830dc, exist in versions v26.31 and
                        //   v26.32, and run under READ COMMITTED isolation
                        //
                        // The statement inserts two rows, a marker at seqno -1 with no data and
                        // the first record at seqno 0, guarded by `NOT EXISTS` over the shard.
                        // The guard is evaluated against the statement's snapshot. The primary key
                        // check of the INSERT runs against the live index. Truncation never
                        // deletes the marker (see `truncate`), so a shard created by this query
                        // keeps its marker for as long as it exists.
                        //
                        // # Correctness with respect to the current version
                        //
                        // The specification is that the first write succeeds iff the shard is
                        // empty. There are three cases at the instant the INSERT runs:
                        //
                        // * The guard saw the shard non-empty. Non-emptiness is monotone (no
                        //   truncation deletes the head), so the shard is still non-empty and the
                        //   INSERT correctly affects zero rows.
                        // * The guard saw the shard empty and the shard is still empty. Both rows
                        //   are inserted and the shard is created. Concurrent first writers block
                        //   on the uncommitted marker row and then fail on the primary key, so
                        //   exactly one of them commits.
                        // * The guard saw the shard empty but the shard was created in between.
                        //   Every creator running this query family writes the marker, no
                        //   truncation removes it, and this INSERT targets the marker's key, so it
                        //   fails on the primary key regardless of what happened to seqno 0. This
                        //   is why the marker is load-bearing: without it a delayed first writer
                        //   could re-insert seqno 0 into a shard whose seqno 0 had been truncated.
                        //
                        // None of this depends on the isolation level. The guard is stale in the
                        // same way under READ COMMITTED and under SERIALIZABLE (a single-statement
                        // transaction takes its snapshot when the statement starts in both), and
                        // the marker collision closes the window in both. In particular the
                        // argument does not rely on SSI, which matters while the
                        // `persist_pg_consensus_read_committed` flag propagates and some
                        // connections run under each level: SSI only tracks conflicts among
                        // SERIALIZABLE transactions and would not see a READ COMMITTED creator.
                        //
                        // # Correctness with respect to <=v26.30 (SERIALIZABLE only)
                        //
                        // The <=v26.30 first write is `INSERT ... WHERE NOT EXISTS` without a
                        // marker, and the <=v26.30 truncate deletes the marker. Mixed with those
                        // queries this query is correct only when every transaction in the system
                        // is SERIALIZABLE, which is why `persist_pg_consensus_read_committed` must
                        // default to off (see the note on the flag). Under SERIALIZABLE every
                        // interleaving is equivalent to a serial one, and in a serial execution
                        // this statement inserts iff the shard is empty, which agrees with the old
                        // guard. The guard is also a read, so SSI tracks the statement; a blind
                        // insert of the two rows would be serializable even when it resurrects a
                        // truncated seqno 0 on a marker-less shard, and so would not be safe here.
                        //
                        // # Correctness with respect to v26.31/v26.32 (READ COMMITTED)
                        //
                        // Those versions initialize a shard by blindly inserting seqno -1 and
                        // seqno 0 and rely on colliding with the marker of any existing shard.
                        // This query writes the same marker and `truncate` preserves it, so a stale
                        // v26.31 first write against a shard created here fails on the marker, and
                        // a stale first write from here against a shard created by v26.31 fails
                        // the same way.
                        static POSTGRES_INIT_QUERY: &str = "
                        INSERT INTO consensus (shard, sequence_number, data)
                        SELECT v.shard, v.sequence_number, v.data
                        FROM (VALUES ($1::text, -1::bigint, ''::bytea), ($1::text, $2::bigint, $3::bytea))
                            AS v (shard, sequence_number, data)
                        WHERE NOT EXISTS (SELECT * FROM consensus WHERE shard = $1)
                        ";
                        let client = self.get_connection().await?;
                        let statement = client.prepare_cached(POSTGRES_INIT_QUERY).await?;
                        pg_execute_prepared(
                            &client,
                            &statement,
                            &[&key, &new.seqno, &new.data.as_ref()],
                        )
                        .await
                    }
                    PostgresMode::CockroachDB => {
                        static CRDB_INIT_QUERY: &str =
                            "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                        NOT EXISTS (
                            SELECT * FROM consensus WHERE shard = $1
                        )";
                        let client = self.get_connection().await?;
                        let statement = client.prepare_cached(CRDB_INIT_QUERY).await?;
                        pg_execute_prepared(
                            &client,
                            &statement,
                            &[&key, &new.seqno, &new.data.as_ref()],
                        )
                        .await
                    }
                }
            }
        };

        match result {
            Ok(n) if n >= 1 => Ok(CaSResult::Committed),
            Ok(_) => Ok(CaSResult::ExpectationMismatch),
            Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                Ok(CaSResult::ExpectationMismatch)
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Runs [PostgresConsensus]'s batched compare-and-sets.
///
/// On CockroachDB a batch is one statement per range, so that a batch whose rows share a range
/// commits on the one-phase-commit fast path. Vanilla Postgres keeps today's single-row query and
/// concurrency, with batches of one and no in-flight cap, until a multi-row query is proven
/// correct under READ COMMITTED.
#[derive(Debug)]
struct PostgresCasBackend {
    client: PostgresConsensusClient,
    cfg: Arc<ConfigSet>,
    ranges: Arc<RwLock<RangeMap>>,
}

#[async_trait]
impl CasBackend for PostgresCasBackend {
    fn limits(&self) -> BatchLimits {
        match self.client.mode {
            PostgresMode::CockroachDB => BatchLimits {
                max_in_flight: Some(CONSENSUS_CAS_BATCH_MAX_IN_FLIGHT_PER_RANGE.get(&self.cfg)),
                max_rows: CONSENSUS_CAS_BATCH_MAX_ROWS.get(&self.cfg),
                max_bytes: CONSENSUS_CAS_BATCH_MAX_BYTES.get(&self.cfg),
                exclusive_keys: true,
            },
            // A cap would serialize every CaS, and key exclusion would make a second CaS for a
            // key wait where today both race at the database.
            PostgresMode::Postgres => BatchLimits {
                max_in_flight: None,
                max_rows: 1,
                max_bytes: 0,
                exclusive_keys: false,
            },
        }
    }

    fn range(&self, key: &str) -> RangeId {
        match self.client.mode {
            PostgresMode::CockroachDB => self.ranges.read().expect("lock poisoned").range(key),
            PostgresMode::Postgres => None,
        }
    }

    async fn execute(&self, ops: &[CasOp]) -> Result<BTreeSet<String>, ExternalError> {
        match self.client.mode {
            PostgresMode::CockroachDB => self.client.compare_and_set_batch(ops).await,
            PostgresMode::Postgres => {
                // More than one op would need a determinate error to roll back the ops that ran
                // before it.
                let [op] = ops else {
                    panic!("Postgres batches hold one op, got {}", ops.len());
                };
                let result = self.client.compare_and_set(&op.key, op.new.clone()).await?;
                Ok(BTreeSet::from_iter(
                    (result == CaSResult::Committed).then(|| op.key.clone()),
                ))
            }
        }
    }
}

impl PostgresConsensusClient {
    /// Runs the CockroachDB compare-and-sets in `ops` as one statement and returns the keys that
    /// committed. Every op must have an expected seqno, and keys must be distinct.
    async fn compare_and_set_batch(
        &self,
        ops: &[CasOp],
    ) -> Result<BTreeSet<String>, ExternalError> {
        // Per row, this is `CRDB_CAS_QUERY`: under SERIALIZABLE, `seqno` is inserted iff
        // `expected` is the shard's head. `EXISTS = expected AND NOT EXISTS > expected` is that
        // predicate. The obvious `(SELECT ... ORDER BY sequence_number DESC LIMIT 1) = expected`
        // gets decorrelated into a lookup join that reads every live row of each shard, data
        // included. This form reads one row per op. `ON CONFLICT DO NOTHING` turns the primary
        // key collision with a racing writer into a row missing from `RETURNING`, so a key is
        // returned iff its row committed.
        //
        // Any change to this query should confirm with EXPLAIN ANALYZE (VERBOSE) that the plan
        // contains `auto commit` and only lookup joins on `consensus_pkey`, and that rows within
        // one range still increment the `txn.commits1PC` metric.
        static CRDB_CAS_BATCH_QUERY: &str = "
            INSERT INTO consensus (shard, sequence_number, data)
            SELECT v.shard, v.seqno, v.data
            FROM unnest($1::text[], $2::bigint[], $3::bytea[], $4::bigint[])
                AS v (shard, seqno, data, expected)
            WHERE EXISTS (SELECT 1 FROM consensus c
                    WHERE c.shard = v.shard AND c.sequence_number = v.expected)
                AND NOT EXISTS (SELECT 1 FROM consensus c
                    WHERE c.shard = v.shard AND c.sequence_number > v.expected)
            ORDER BY v.shard
            ON CONFLICT DO NOTHING
            RETURNING shard
        ";
        assert_eq!(self.mode, PostgresMode::CockroachDB);
        // `RETURNING shard` can't tell two ops for one key apart.
        assert!(
            ops.iter().map(|op| &op.key).all_unique(),
            "batched CaS keys must be distinct"
        );
        let shards: Vec<&str> = ops.iter().map(|op| op.key.as_str()).collect();
        let seqnos: Vec<SeqNo> = ops.iter().map(|op| op.new.seqno).collect();
        let data: Vec<&[u8]> = ops.iter().map(|op| op.new.data.as_ref()).collect();
        let expected: Vec<SeqNo> = ops
            .iter()
            .map(|op| {
                op.new
                    .seqno
                    .previous()
                    .expect("batched CaS has an expected seqno")
            })
            .collect();
        let rows = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(CRDB_CAS_BATCH_QUERY).await?;
            pg_query_prepared(&client, &statement, &[&shards, &seqnos, &data, &expected]).await?
        };
        rows.iter().map(|row| Ok(row.try_get("shard")?)).collect()
    }

    /// Returns every version of each shard at or after its seqno, in the order of `scans`.
    /// Shards must be distinct.
    pub(crate) async fn scan_batch(
        &self,
        scans: &[(&str, SeqNo)],
    ) -> Result<Vec<Vec<VersionedData>>, ExternalError> {
        // A per-scan `LIMIT` would need a `LATERAL` subquery, which CockroachDB runs as an apply
        // join that plans the subquery once per shard. Without it the plan is one lookup join on
        // `consensus_pkey`. Check any change to this query with `EXPLAIN` on CockroachDB.
        static SCAN_BATCH_QUERY: &str = "
            SELECT c.shard, c.sequence_number, c.data
            FROM unnest($1::text[], $2::bigint[]) AS v (shard, from_seqno)
            JOIN consensus c ON c.shard = v.shard AND c.sequence_number >= v.from_seqno
            ORDER BY c.shard, c.sequence_number
        ";
        let (shards, froms): (Vec<&str>, Vec<SeqNo>) = scans.iter().copied().unzip();
        let rows = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(SCAN_BATCH_QUERY).await?;
            pg_query_prepared(&client, &statement, &[&shards, &froms]).await?
        };
        let mut results: BTreeMap<String, Vec<VersionedData>> = BTreeMap::new();
        for row in rows {
            let shard: String = row.try_get("shard")?;
            let seqno: SeqNo = row.try_get("sequence_number")?;
            let data: Vec<u8> = row.try_get("data")?;
            results.entry(shard).or_default().push(VersionedData {
                seqno,
                data: Bytes::from(data),
            });
        }
        Ok(shards
            .iter()
            .map(|shard| results.remove(*shard).unwrap_or_default())
            .collect())
    }

    async fn range_map(&self) -> Result<RangeMap, ExternalError> {
        let q = "SELECT start_key FROM [SHOW RANGES FROM TABLE consensus]";
        let rows = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_query_prepared(&client, &statement, &[]).await?
        };
        let start_keys = rows
            .iter()
            .map(|row| row.try_get::<_, Option<String>>("start_key"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RangeMap::from_start_keys(start_keys)?)
    }
}

/// Keeps `ranges` in step with the consensus table's CockroachDB range boundaries while batching
/// is enabled, refreshing within a second of batching turning on.
///
/// A stale or empty map costs batches their one-phase commit, never correctness, so failures keep
/// the previous map. When the role may not run `SHOW RANGES`, the table stays one range.
async fn refresh_ranges(
    client: PostgresConsensusClient,
    cfg: Arc<ConfigSet>,
    ranges: Arc<RwLock<RangeMap>>,
    metrics: ConsensusBatchMetrics,
) {
    // NOTE: until the first refresh, every key maps to one range, so the whole process runs at
    // most `persist_consensus_cas_batch_max_in_flight_per_range` batched statements.
    let mut last_attempt: Option<Instant> = None;
    loop {
        if !CONSENSUS_CAS_BATCH_ENABLED.get(&cfg) {
            last_attempt = None;
        } else if last_attempt.map_or(true, |t| {
            t.elapsed() >= CONSENSUS_CAS_BATCH_RANGE_REFRESH_INTERVAL.get(&cfg)
        }) {
            last_attempt = Some(Instant::now());
            match client.range_map().await {
                Ok(map) => {
                    metrics.ranges.set(u64::cast_from(map.len()));
                    *ranges.write().expect("lock poisoned") = map;
                }
                Err(err) => {
                    metrics.range_refresh_failures.inc();
                    warn!("failed to refresh consensus range boundaries: {:#}", err);
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[async_trait]
impl Consensus for PostgresConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let q = "SELECT DISTINCT shard FROM consensus";

        Box::pin(try_stream! {
            // NB: it's important that we hang on to this client for the lifetime of the stream,
            // to avoid returning it to the pool prematurely.
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            let params: &[String] = &[];
            let mut rows = Box::pin(client.query_raw(&statement, params).await?);
            while let Some(row) = rows.next().await {
                let shard: String = row?.try_get("shard")?;
                yield shard;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1";
        let row = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_query_opt_prepared(&client, &statement, &[&key]).await?
        };
        let row = match row {
            None => return Ok(None),
            Some(row) => row,
        };

        let seqno: SeqNo = row.try_get("sequence_number")?;

        let data: Vec<u8> = row.try_get("data")?;
        Ok(Some(VersionedData {
            seqno,
            data: Bytes::from(data),
        }))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        // Init (no expected seqno) stays on the single-row path.
        if new.seqno.previous().is_some() && CONSENSUS_CAS_BATCH_ENABLED.get(&self.cfg) {
            return self.batcher.compare_and_set(key, new).await;
        }
        self.client.compare_and_set(key, new).await
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        if limit == SCAN_ALL && CONSENSUS_SCAN_BATCH_ENABLED.get(&self.cfg) {
            return self.scan_batcher.scan(key, from).await;
        }
        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 AND sequence_number >= $2
             ORDER BY sequence_number ASC LIMIT $3";
        let Ok(limit) = i64::try_from(limit) else {
            return Err(ExternalError::from(anyhow!(
                "limit must be [0, i64::MAX]. was: {:?}",
                limit
            )));
        };
        let rows = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_query_prepared(&client, &statement, &[&key, &from, &limit]).await?
        };
        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let seqno: SeqNo = row.try_get("sequence_number")?;
            let data: Vec<u8> = row.try_get("data")?;
            results.push(VersionedData {
                seqno,
                data: Bytes::from(data),
            });
        }
        Ok(results)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        // The `sequence_number >= 0` clause preserves the seqno `-1` marker that the first write
        // inserts. The marker is a truncation-proof "already initialized" row that a delayed first
        // write collides with (see `compare_and_set`), so it must outlive every truncation. The
        // clause is a no-op for shards that have no marker, since all of their seqnos are already
        // >= 0.
        static TRUNCATE_QUERY: &str = "
        DELETE FROM consensus
        WHERE shard = $1 AND sequence_number >= 0 AND sequence_number < $2 AND
        EXISTS (
            SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
        )
        ";

        let result = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(TRUNCATE_QUERY).await?;
            pg_execute_prepared(&client, &statement, &[&key, &seqno]).await?
        };
        if result == 0 {
            // We weren't able to successfully truncate any rows inspect head to
            // determine whether the request was valid and there were no records in
            // the provided range, or the request was invalid because it would have
            // also deleted head.

            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(key).await?;
            if current.map_or(true, |data| data.seqno < seqno) {
                return Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )));
            }
        }

        Ok(Some(usize::cast_from(result)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_dyncfg::ConfigUpdates;
    use mz_ore::assert_err;
    use mz_ore::cast::CastLossy;
    use tracing::info;
    use uuid::Uuid;

    use crate::location::tests::consensus_impl_test;

    use super::*;

    /// A test config with CaS and scan batching on and at most `max_in_flight` statements of each
    /// in flight (per range, for CaS).
    fn batching_config(max_in_flight: usize) -> PostgresConsensusConfig {
        let config = PostgresConsensusConfig::new_for_test()
            .expect("valid config")
            .expect("postgres url is set");
        let mut updates = ConfigUpdates::default();
        updates.add(&CONSENSUS_CAS_BATCH_ENABLED, true);
        updates.add(&CONSENSUS_CAS_BATCH_MAX_IN_FLIGHT_PER_RANGE, max_in_flight);
        updates.add(&CONSENSUS_SCAN_BATCH_ENABLED, true);
        updates.add(&CONSENSUS_SCAN_BATCH_MAX_IN_FLIGHT, max_in_flight);
        updates.apply(&config.dyncfg);
        config
    }

    /// Batches against live range boundaries, then races concurrent writers through the batcher,
    /// alone and mixed with writers that use the single-row path.
    async fn cas_batch_ranges_and_races() -> Result<(), ExternalError> {
        let config = batching_config(1);
        let consensus = PostgresConsensus::open(config.clone()).await?;
        if consensus.client.mode == PostgresMode::CockroachDB {
            // Split inside the random shard keys, so the races below span two ranges.
            let client = consensus.get_connection().await?;
            pg_batch_execute(&client, "ALTER TABLE consensus SPLIT AT VALUES ('8')").await?;
            drop(client);
            let map = consensus.client.range_map().await?;
            assert!(map.len() >= 2, "{map:?}");
            assert_ne!(map.range("7fff"), Some("8".to_owned()));
            assert_eq!(map.range("8000"), Some("8".to_owned()));
        }
        // Reopen so that the refresh task's first pass sees the split. Dropping the first
        // instance aborts its refresh task, which shares the `ranges` gauge.
        drop(consensus);
        let batched = Arc::new(PostgresConsensus::open(config.clone()).await?);
        race_cas(&[Arc::clone(&batched)], 8, 3, 5).await?;
        let batch_size = &config.batch_metrics.batch_size;
        match batched.client.mode {
            PostgresMode::CockroachDB => {
                assert!(config.batch_metrics.ranges.get() >= 2);
                assert!(
                    batch_size.get_sample_sum() > f64::cast_lossy(batch_size.get_sample_count()),
                    "some batch should hold more than one call"
                );
            }
            PostgresMode::Postgres => assert_eq!(
                batch_size.get_sample_sum(),
                f64::cast_lossy(batch_size.get_sample_count())
            ),
        }

        // Batched and single-row writers share shards, as old and new binaries do mid-upgrade.
        let single_row_config = PostgresConsensusConfig::new_for_test()?.expect("url is set");
        let single_row = Arc::new(PostgresConsensus::open(single_row_config).await?);
        race_cas(&[batched, single_row], 8, 4, 5).await
    }

    /// Batched scans return what single-shard scans return: for shards with several versions,
    /// one version, no versions after `from`, and no rows at all, and with a truncated prefix.
    async fn scan_batch_matches_scan() -> Result<(), ExternalError> {
        let config = batching_config(1);
        let batched = Arc::new(PostgresConsensus::open(config.clone()).await?);
        let single =
            PostgresConsensus::open(PostgresConsensusConfig::new_for_test()?.unwrap()).await?;
        let mut scans = Vec::new();
        for versions in [0u64, 1, 3, 5] {
            let key = Uuid::new_v4().to_string();
            for seqno in 0..versions {
                let new = VersionedData {
                    seqno: SeqNo(seqno),
                    data: Bytes::from(format!("{key}-{seqno}")),
                };
                assert_eq!(
                    single.compare_and_set(&key, new).await?,
                    CaSResult::Committed
                );
            }
            if versions == 5 {
                single.truncate(&key, SeqNo(2)).await?;
            }
            for from in [0, 1, 3, 9] {
                scans.push((key.clone(), SeqNo(from)));
            }
        }
        let mut expected = Vec::new();
        for (key, from) in &scans {
            expected.push(single.scan(key, *from, SCAN_ALL).await?);
        }

        // Concurrent scans through the batcher, several per shard.
        let handles: Vec<_> = scans
            .iter()
            .map(|(key, from)| {
                let batched = Arc::clone(&batched);
                let (key, from) = (key.clone(), *from);
                mz_ore::task::spawn(|| "scan", async move {
                    batched.scan(&key, from, SCAN_ALL).await
                })
            })
            .collect();
        let mut actual = Vec::new();
        for handle in handles {
            actual.push(handle.await?);
        }
        assert_eq!(actual, expected);
        let statements = config.batch_metrics.scan_batch_size.get_sample_count();
        assert!(
            statements > 0 && statements < u64::cast_from(scans.len()),
            "{statements} statements for {} scans",
            scans.len()
        );

        // Limited scans keep the single-shard query.
        let (key, _) = scans.last().unwrap();
        assert_eq!(
            batched.scan(key, SeqNo(0), 1).await?,
            single.scan(key, SeqNo(0), 1).await?
        );
        assert_eq!(
            config.batch_metrics.scan_batch_size.get_sample_count(),
            statements
        );
        Ok(())
    }

    /// Races `racers` writers on each of `shards` new shards, spread over `instances`, until every
    /// head is `target`. Checks that each seqno has exactly one winner and that the log holds the
    /// winners' data without gaps.
    async fn race_cas(
        instances: &[Arc<PostgresConsensus>],
        shards: usize,
        racers: usize,
        target: u64,
    ) -> Result<(), ExternalError> {
        let keys: Vec<String> = (0..shards).map(|_| Uuid::new_v4().to_string()).collect();
        for key in &keys {
            let init = VersionedData {
                seqno: SeqNo(0),
                data: Bytes::from("init"),
            };
            assert_eq!(
                instances[0].compare_and_set(key, init).await?,
                CaSResult::Committed
            );
        }
        let mut handles = Vec::new();
        for key in &keys {
            for racer in 0..racers {
                let consensus = Arc::clone(&instances[racer % instances.len()]);
                let key = key.clone();
                handles.push(mz_ore::task::spawn(|| "race_cas", async move {
                    let mut wins = Vec::new();
                    let mut head = SeqNo(0);
                    while head.0 < target {
                        let new = VersionedData {
                            seqno: head.next(),
                            data: Bytes::from(racer.to_string()),
                        };
                        // Determinate errors (serialization failures) committed nothing and are
                        // retried, as `Machine` does.
                        match consensus.compare_and_set(&key, new).await {
                            Ok(CaSResult::Committed) => {
                                head = head.next();
                                wins.push(head);
                            }
                            Ok(CaSResult::ExpectationMismatch) => loop {
                                match consensus.head(&key).await {
                                    Ok(data) => {
                                        head = data.expect("initialized").seqno;
                                        break;
                                    }
                                    Err(ExternalError::Determinate(_)) => {}
                                    Err(err) => return Err(err),
                                }
                            },
                            Err(ExternalError::Determinate(_)) => {}
                            Err(err) => return Err(err),
                        }
                    }
                    Ok::<_, ExternalError>((key, racer, wins))
                }));
            }
        }
        let mut winners = BTreeMap::new();
        for handle in handles {
            let (key, racer, wins) = handle.await?;
            for seqno in wins {
                let prev = winners.insert((key.clone(), seqno), racer);
                assert_eq!(prev, None, "two winners of {key} at {seqno:?}");
            }
        }
        for key in &keys {
            let log = instances[0].scan(key, SeqNo(0), SCAN_ALL).await?;
            let seqnos: Vec<_> = log.iter().map(|d| d.seqno).collect();
            assert_eq!(seqnos, (0..=target).map(SeqNo).collect::<Vec<_>>());
            for d in &log[1..] {
                let winner = winners[&(key.clone(), d.seqno)];
                assert_eq!(d.data, Bytes::from(winner.to_string()));
            }
        }
        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn postgres_consensus() -> Result<(), ExternalError> {
        let config = match PostgresConsensusConfig::new_for_test()? {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };

        consensus_impl_test(|| PostgresConsensus::open(config.clone())).await?;

        // On a Postgres backend the default run above already exercises the tuned queries under
        // SERIALIZABLE. Re-run the contract with READ COMMITTED to also cover that isolation. On a
        // CockroachDB backend the flag makes `get_connection` panic, so skip it there.
        // `consensus_impl_test` asserts on `list_keys`, so it needs a clean table: the previous run
        // leaves shards behind, so drop and recreate the table before running it again.
        let read_committed_config =
            PostgresConsensusConfig::new_for_test()?.expect("postgres url was set above");
        {
            let mut updates = ConfigUpdates::default();
            updates.add(&PG_CONSENSUS_READ_COMMITTED, true);
            updates.apply(&read_committed_config.dyncfg);
        }
        let consensus = PostgresConsensus::open(read_committed_config.clone()).await?;
        if consensus.client.mode == PostgresMode::Postgres {
            consensus.drop_and_recreate().await?;
            consensus_impl_test(|| PostgresConsensus::open(read_committed_config.clone())).await?;
        }

        // The contract again with batching on. It is sequential, so every batch has one call.
        let batch_config = batching_config(1);
        PostgresConsensus::open(batch_config.clone())
            .await?
            .drop_and_recreate()
            .await?;
        consensus_impl_test(|| PostgresConsensus::open(batch_config.clone())).await?;

        cas_batch_ranges_and_races().await?;
        scan_batch_matches_scan().await?;

        // and now verify the implementation-specific `drop_and_recreate` works as intended
        let consensus = PostgresConsensus::open(config.clone()).await?;
        let key = Uuid::new_v4().to_string();
        let state = VersionedData {
            seqno: SeqNo(0),
            data: Bytes::from("abc"),
        };

        assert_eq!(
            consensus.compare_and_set(&key, state.clone()).await,
            Ok(CaSResult::Committed),
        );

        assert_eq!(consensus.head(&key).await, Ok(Some(state.clone())));

        consensus.drop_and_recreate().await?;

        assert_eq!(consensus.head(&key).await, Ok(None));

        // This should be a separate postgres_consensus_blocking test, but nextest makes it
        // difficult since we can't specify that both tests touch the consensus table and thus
        // interfere with each other.
        let config = match PostgresConsensusConfig::new_for_test()? {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };

        let consensus: PostgresConsensus = PostgresConsensus::open(config.clone()).await?;
        // Max size in test is 2... let's saturate the pool.
        let _conn1 = consensus.get_connection().await?;
        let _conn2 = consensus.get_connection().await?;

        // And finally, we should see the next connect time out.
        let conn3 = consensus.get_connection().await;

        assert_err!(conn3);

        Ok(())
    }
}
