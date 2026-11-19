// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Blob reaper prototype: deletes blobs nothing will ever use.
//!
//! Machinery (walker, journal, deleter) and rules are kept apart. The machinery lists one unit
//! of the key space, hands it to the [Rules] that own the unit, and acts on the verdicts. It never
//! parses a key or reads shard state. The rules never list, touch the journal, or delete.
//!
//! A candidate is deleted once its group's clock, kept in the journal shard, is `min_age` old.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use mz_persist::location::Blob;
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use prost::Message;
use semver::Version;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tokio::sync::Mutex;

use crate::internal::metrics::Metrics;
use crate::internal::paths::{BlobKey, ParsedBlobKey, PartialBlobKey, WriterKey};
use crate::internal::state::{BatchPart, HollowBlobRef, HollowRunRef, ProtoRollup, RunPart};
use crate::internal::state_versions::StateVersions;
use crate::lease::{LeaseShard, LeaseState};
use crate::read::ReadHandle;
use crate::write::WriteHandle;
use crate::{Diagnostics, PersistClient, ShardId};

/// Why a key is reported and never deleted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Reason {
    Unparseable,
    /// Case A: rollups of live shards.
    Rollup,
    /// `w<uuid>` writer keys carry no version.
    LegacyWriter,
    /// Case A rule 3: the writer is not older than the shard's recorded version.
    WriterNotOlder,
    /// Case A rule 2 can't be checked without the txns shard.
    NoTxnsShard,
    /// A referenced hollow run is missing, so its children can't be told from orphans.
    MissingHollowRun,
    /// Case B needs the oldest running build.
    NoFloor,
    /// Case B: some writer of the shard is not older than the oldest running build.
    WriterNotBelowFloor,
    NoLeaseShard,
    LeaseNoRow,
    LeaseUnknownState,
}

/// A candidate's group: the candidates that share one clock. Opaque to the machinery.
pub type GroupKey = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Keep,
    Report(Reason),
    Candidate(GroupKey),
}

#[derive(Debug, Clone)]
pub struct UnitListing {
    pub unit: String,
    pub keys: Vec<(String, u64)>,
}

/// The verdicts for one object (a shard, or a lease) of a unit.
#[derive(Debug, Clone, Default)]
pub struct ObjectVerdicts {
    pub object: String,
    pub per_key: Vec<(String, u64, Verdict)>,
}

#[derive(Debug, Clone, Default)]
pub struct UnitVerdicts {
    pub objects: Vec<ObjectVerdicts>,
    /// Set when the results look wrong enough to stop all deletes.
    pub tripwire: Option<String>,
}

#[async_trait]
pub trait Rules: Send + Sync {
    /// The units this rule set owns, in walk order. Each is also the listing prefix.
    fn units(&self) -> Vec<String>;
    /// Judges every key of one unit.
    async fn judge(&self, unit: &UnitListing) -> UnitVerdicts;
}

fn hex_units(prefix: &str) -> Vec<String> {
    (0..4096).map(|i| format!("{prefix}{i:03x}")).collect()
}

/// Parts the txns shard names in unapplied txns (case A rule 2).
#[async_trait]
pub trait PendingParts: Send + Sync {
    /// `None` if `shard` is not a txn-wal data shard. Otherwise the full blob keys its unapplied
    /// txns name, and the txns shard's recorded version.
    async fn pending(&self, shard: ShardId) -> Option<(BTreeSet<String>, Version)>;
}

/// Rules for `s<id>/`: case A (shard in consensus) and case B (not in consensus).
pub struct ShardRules {
    state_versions: Arc<StateVersions>,
    blob: Arc<dyn Blob>,
    metrics: Arc<Metrics>,
    pending: Option<Arc<dyn PendingParts>>,
    floor: Option<Version>,
    tripwire_threshold: usize,
}

impl std::fmt::Debug for ShardRules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRules")
            .field("floor", &self.floor)
            .finish_non_exhaustive()
    }
}

/// What case A needs from a shard's live states.
#[derive(Debug)]
pub struct ShardReach {
    pub referenced: BTreeSet<String>,
    pub version: Version,
    pub tombstone: bool,
    pub missing_run: bool,
}

impl ShardRules {
    pub fn new(
        client: &PersistClient,
        pending: Option<Arc<dyn PendingParts>>,
        floor: Option<Version>,
        tripwire_threshold: usize,
    ) -> Self {
        let state_versions = Arc::new(StateVersions::new(
            client.cfg.clone(),
            Arc::clone(&client.consensus),
            Arc::clone(&client.blob),
            Arc::clone(&client.metrics),
        ));
        ShardRules {
            state_versions,
            blob: Arc::clone(&client.blob),
            metrics: Arc::clone(&client.metrics),
            pending,
            floor,
            tripwire_threshold,
        }
    }

    /// Every key any live state of `shard` names, hollow runs expanded. `None` if the shard has
    /// no consensus entry.
    pub async fn reach(&self, shard: ShardId) -> Option<ShardReach> {
        let iter = self
            .state_versions
            .fetch_all_live_states::<u64>(shard)
            .await?;
        let mut iter = iter.check_ts_codec().expect("u64 shard");
        let mut referenced = BTreeSet::new();
        let mut runs: Vec<HollowRunRef<u64>> = Vec::new();
        fn collect(
            shard: ShardId,
            part: &RunPart<u64>,
            referenced: &mut BTreeSet<String>,
            runs: &mut Vec<HollowRunRef<u64>>,
        ) {
            match part {
                RunPart::Single(BatchPart::Hollow(p)) => {
                    referenced.insert(p.key.complete(&shard).to_string());
                }
                RunPart::Single(BatchPart::Inline { .. }) => {}
                RunPart::Many(r) => runs.push(r.clone()),
            }
        }
        while let Some(_) = iter.next(|diff| {
            for blob in diff.referenced_blobs() {
                match blob {
                    HollowBlobRef::Batch(b) => {
                        for part in b.parts.iter() {
                            collect(shard, part, &mut referenced, &mut runs);
                        }
                    }
                    HollowBlobRef::Rollup(r) => {
                        referenced.insert(r.key.complete(&shard).to_string());
                    }
                }
            }
        }) {}
        let mut missing_run = false;
        while let Some(run) = runs.pop() {
            referenced.insert(run.key.complete(&shard).to_string());
            match run.get(shard, &*self.blob, &self.metrics).await {
                Some(r) => {
                    for part in r.parts.iter() {
                        collect(shard, part, &mut referenced, &mut runs);
                    }
                }
                None => missing_run = true,
            }
        }
        let state = iter.state();
        Some(ShardReach {
            referenced,
            version: state.collections.version.clone(),
            tombstone: state.collections.is_tombstone(),
            missing_run,
        })
    }

    async fn case_a(
        &self,
        shard: ShardId,
        reach: ShardReach,
        keys: Vec<(String, u64, PartialBlobKey)>,
    ) -> Vec<(String, u64, Verdict)> {
        let pending = match &self.pending {
            Some(p) => Some(p.pending(shard).await),
            None => None,
        };
        let mut floor = WriterKey::for_version(&reach.version);
        if let Some(Some((_, txns_version))) = &pending {
            floor = std::cmp::min(floor, WriterKey::for_version(txns_version));
        }
        keys.into_iter()
            .map(|(key, size, parsed)| {
                let v = if reach.referenced.contains(&key) {
                    Verdict::Keep
                } else if reach.missing_run {
                    Verdict::Report(Reason::MissingHollowRun)
                } else {
                    match parsed {
                        PartialBlobKey::Rollup(..) => Verdict::Report(Reason::Rollup),
                        PartialBlobKey::Batch(WriterKey::Id(_), _) => {
                            Verdict::Report(Reason::LegacyWriter)
                        }
                        PartialBlobKey::Batch(w @ WriterKey::Version(_), _) => {
                            if !(reach.tombstone || w < floor) {
                                Verdict::Report(Reason::WriterNotOlder)
                            } else {
                                match &pending {
                                    None => Verdict::Report(Reason::NoTxnsShard),
                                    Some(Some((names, _))) if names.contains(&key) => Verdict::Keep,
                                    Some(_) => Verdict::Candidate(format!("{shard}/{w}")),
                                }
                            }
                        }
                    }
                };
                (key, size, v)
            })
            .collect()
    }

    /// Case B. Returns the verdicts and whether the shard has a rollup.
    async fn case_b(
        &self,
        shard: ShardId,
        keys: Vec<(String, u64, PartialBlobKey)>,
    ) -> (Vec<(String, u64, Verdict)>, bool) {
        let has_rollup = keys
            .iter()
            .any(|(_, _, p)| matches!(p, PartialBlobKey::Rollup(..)));
        let Some(floor) = &self.floor else {
            let v = keys
                .into_iter()
                .map(|(k, s, _)| (k, s, Verdict::Report(Reason::NoFloor)))
                .collect();
            return (v, has_rollup);
        };
        let floor = WriterKey::for_version(floor);
        let mut all_gone = true;
        for (key, _, parsed) in &keys {
            let writer = match parsed {
                PartialBlobKey::Batch(w, _) => Some(w.clone()),
                PartialBlobKey::Rollup(..) => self.rollup_writer(key).await,
            };
            match writer {
                Some(w @ WriterKey::Version(_)) if w < floor => {}
                // A rollup deleted since the listing has no writer to wait for.
                None => {}
                Some(_) => all_gone = false,
            }
        }
        let verdict = if all_gone {
            Verdict::Candidate(format!("{shard}/absent"))
        } else {
            Verdict::Report(Reason::WriterNotBelowFloor)
        };
        let v = keys
            .into_iter()
            .map(|(k, s, _)| (k, s, verdict.clone()))
            .collect();
        (v, has_rollup)
    }

    /// The writer version of a rollup blob, read from the proto without the state decoder (which
    /// halts on state newer than this build).
    async fn rollup_writer(&self, key: &str) -> Option<WriterKey> {
        let buf = self.blob.get(key).await.expect("blob get")?;
        let proto = ProtoRollup::decode(buf.into_contiguous().as_slice()).expect("valid rollup");
        let version = Version::parse(&proto.applier_version).expect("valid version");
        Some(WriterKey::for_version(&version))
    }
}

#[async_trait]
impl Rules for ShardRules {
    fn units(&self) -> Vec<String> {
        hex_units("s")
    }

    async fn judge(&self, unit: &UnitListing) -> UnitVerdicts {
        let mut by_shard: BTreeMap<ShardId, Vec<(String, u64, PartialBlobKey)>> = BTreeMap::new();
        let mut unparseable = Vec::new();
        for (key, size) in &unit.keys {
            match BlobKey::parse_ids(key) {
                Ok((shard, partial)) => {
                    by_shard
                        .entry(shard)
                        .or_default()
                        .push((key.clone(), *size, partial))
                }
                Err(_) => {
                    unparseable.push((key.clone(), *size, Verdict::Report(Reason::Unparseable)))
                }
            }
        }
        let mut out = UnitVerdicts::default();
        if !unparseable.is_empty() {
            out.objects.push(ObjectVerdicts {
                object: "unparseable".into(),
                per_key: unparseable,
            });
        }
        let mut absent_with_rollup = 0;
        for (shard, keys) in by_shard {
            let per_key = match self.reach(shard).await {
                Some(reach) => self.case_a(shard, reach, keys).await,
                None => {
                    let (v, has_rollup) = self.case_b(shard, keys).await;
                    if has_rollup {
                        absent_with_rollup += 1;
                    }
                    v
                }
            };
            out.objects.push(ObjectVerdicts {
                object: shard.to_string(),
                per_key,
            });
        }
        if absent_with_rollup > self.tripwire_threshold {
            out.tripwire = Some(format!(
                "{}: {absent_with_rollup} shards with rollups but no consensus entry (threshold {})",
                unit.unit, self.tripwire_threshold
            ));
        }
        out
    }
}

/// Rules for `leased/`. They act only on the `state` an envd wrote to the lease shard.
#[derive(Debug)]
pub struct LeasedRules {
    lease_shard: Option<Mutex<LeaseShard>>,
}

impl LeasedRules {
    pub fn new(lease_shard: Option<LeaseShard>) -> Self {
        LeasedRules {
            lease_shard: lease_shard.map(Mutex::new),
        }
    }
}

#[async_trait]
impl Rules for LeasedRules {
    fn units(&self) -> Vec<String> {
        hex_units("leased/l")
    }

    async fn judge(&self, unit: &UnitListing) -> UnitVerdicts {
        let rows = match &self.lease_shard {
            Some(s) => Some(s.lock().await.snapshot().await),
            None => None,
        };
        let mut by_lease: BTreeMap<String, Vec<(String, u64, Verdict)>> = BTreeMap::new();
        for (key, size) in &unit.keys {
            let (object, v) = match BlobKey::parse(key) {
                Ok(ParsedBlobKey::Leased { lease, .. }) => {
                    let object = format!("leased/{lease}");
                    let v = match rows.as_ref().map(|r| r.get(&lease)) {
                        None => Verdict::Report(Reason::NoLeaseShard),
                        Some(None) => Verdict::Report(Reason::LeaseNoRow),
                        Some(Some(row)) => match row.state {
                            LeaseState::Expired => Verdict::Candidate(object.clone()),
                            LeaseState::Live => Verdict::Keep,
                            LeaseState::Unknown(_) => Verdict::Report(Reason::LeaseUnknownState),
                        },
                    };
                    (object, v)
                }
                _ => ("unparseable".into(), Verdict::Report(Reason::Unparseable)),
            };
            by_lease
                .entry(object)
                .or_default()
                .push((key.clone(), *size, v));
        }
        UnitVerdicts {
            objects: by_lease
                .into_iter()
                .map(|(object, per_key)| ObjectVerdicts { object, per_key })
                .collect(),
            tripwire: None,
        }
    }
}

/// A journal row. Rows are key-only. A change is a retraction plus an insert.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JournalRow {
    Cursor {
        unit: String,
    },
    Clock {
        unit: String,
        object: String,
        group: GroupKey,
        first_seen_ms: u64,
    },
}

#[derive(Debug, Clone)]
pub struct JournalState {
    pub upper: u64,
    pub rows: BTreeSet<JournalRow>,
}

impl JournalState {
    pub fn cursor(&self) -> Option<&str> {
        self.rows.iter().find_map(|r| match r {
            JournalRow::Cursor { unit } => Some(unit.as_str()),
            _ => None,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct LostRace;

/// The journal shard. PROTOTYPE: like the lease shard, it holds its initial since forever.
#[derive(Debug)]
pub struct Journal {
    write: WriteHandle<String, (), u64, i64>,
    read: ReadHandle<String, (), u64, i64>,
}

impl Journal {
    pub async fn open(client: &PersistClient, shard_id: ShardId) -> Self {
        let (mut write, read) = client
            .open::<String, (), u64, i64>(
                shard_id,
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("blob reaper journal"),
                false,
            )
            .await
            .expect("journal codecs never change");
        let _ = write
            .compare_and_append_batch(
                &mut [],
                Antichain::from_elem(0),
                Antichain::from_elem(1),
                true,
            )
            .await
            .expect("valid usage");
        Journal { write, read }
    }

    pub async fn load(&mut self) -> JournalState {
        let upper = *self
            .write
            .fetch_recent_upper()
            .await
            .as_option()
            .expect("journal is never closed");
        let updates = self
            .read
            .snapshot_and_fetch(Antichain::from_elem(upper - 1))
            .await
            .expect("journal since is never advanced");
        let mut rows = BTreeSet::new();
        for ((k, ()), _t, d) in updates {
            assert_eq!(d, 1);
            rows.insert(serde_json::from_str(&k).expect("valid journal row"));
        }
        JournalState { upper, rows }
    }

    pub async fn commit(
        &mut self,
        upper: u64,
        retract: &[JournalRow],
        insert: &[JournalRow],
    ) -> Result<(), LostRace> {
        let enc = |r: &JournalRow| serde_json::to_string(r).expect("serializable");
        let updates: Vec<_> = retract
            .iter()
            .map(|r| ((enc(r), ()), upper, -1i64))
            .chain(insert.iter().map(|r| ((enc(r), ()), upper, 1)))
            .collect();
        self.write
            .compare_and_append(
                updates,
                Antichain::from_elem(upper),
                Antichain::from_elem(upper + 1),
            )
            .await
            .expect("valid usage")
            .map_err(|_| LostRace)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VisitOpts {
    pub commit: bool,
    pub min_age_ms: u64,
    pub now_ms: u64,
}

/// Restricts a visit to one object (`--shard`). The cursor doesn't move.
#[derive(Debug, Clone)]
pub struct ObjectFilter {
    pub prefix: String,
    pub object: String,
}

#[derive(Debug)]
pub enum VisitError {
    Tripwire(String),
    LostRace,
}

/// Everything one visit decided, before it acts.
#[derive(Debug, Clone)]
pub struct VisitPlan {
    pub unit: String,
    pub verdicts: Vec<ObjectVerdicts>,
    pub to_delete: Vec<String>,
    pub upper: u64,
    pub retract: Vec<JournalRow>,
    pub insert: Vec<JournalRow>,
}

/// Lists and judges `unit` and decides deletes and journal changes. Changes nothing.
pub async fn plan_visit(
    unit: &str,
    next_unit: Option<&str>,
    filter: Option<&ObjectFilter>,
    rules: &dyn Rules,
    journal: &mut Journal,
    blob: &dyn Blob,
    opts: VisitOpts,
) -> Result<VisitPlan, VisitError> {
    let prefix = filter.map_or(unit, |f| f.prefix.as_str());
    let mut keys = Vec::new();
    blob.list_keys_and_metadata(prefix, &mut |m| {
        keys.push((m.key.to_owned(), m.size_in_bytes))
    })
    .await
    .expect("list");
    let verdicts = rules
        .judge(&UnitListing {
            unit: unit.to_owned(),
            keys,
        })
        .await;
    if let Some(msg) = verdicts.tripwire {
        return Err(VisitError::Tripwire(msg));
    }

    let state = journal.load().await;
    let in_scope = |u: &str, o: &str| u == unit && filter.map_or(true, |f| f.object == o);
    let mut clocks: BTreeMap<(String, GroupKey), (u64, bool)> = BTreeMap::new();
    let mut retract = Vec::new();
    let mut insert = Vec::new();
    for row in &state.rows {
        if let JournalRow::Clock {
            unit: u,
            object,
            group,
            first_seen_ms,
        } = row
        {
            if in_scope(u, object) {
                clocks.insert((object.clone(), group.clone()), (*first_seen_ms, false));
            }
        }
    }
    let mut to_delete = Vec::new();
    for obj in &verdicts.objects {
        for (key, _, v) in &obj.per_key {
            let Verdict::Candidate(group) = v else {
                continue;
            };
            let k = (obj.object.clone(), group.clone());
            match clocks.get_mut(&k) {
                Some((first_seen, used)) => {
                    *used = true;
                    if opts.now_ms.saturating_sub(*first_seen) >= opts.min_age_ms {
                        to_delete.push(key.clone());
                    }
                }
                None => {
                    clocks.insert(k.clone(), (opts.now_ms, true));
                    insert.push(JournalRow::Clock {
                        unit: unit.to_owned(),
                        object: k.0,
                        group: k.1,
                        first_seen_ms: opts.now_ms,
                    });
                }
            }
        }
    }
    for ((object, group), (first_seen_ms, used)) in clocks {
        let is_new = insert.iter().any(|r| matches!(r, JournalRow::Clock { object: o, group: g, .. } if *o == object && *g == group));
        if !used && !is_new {
            retract.push(JournalRow::Clock {
                unit: unit.to_owned(),
                object,
                group,
                first_seen_ms,
            });
        }
    }
    if filter.is_none() {
        if let Some(next) = next_unit {
            if let Some(cur) = state.cursor() {
                retract.push(JournalRow::Cursor {
                    unit: cur.to_owned(),
                });
            }
            insert.push(JournalRow::Cursor {
                unit: next.to_owned(),
            });
        }
    }
    Ok(VisitPlan {
        unit: unit.to_owned(),
        verdicts: verdicts.objects,
        to_delete,
        upper: state.upper,
        retract,
        insert,
    })
}

/// Deletes (if `commit`), then appends the journal changes. The deletes happen first: a crash
/// between them leaves the journal as it was, and the next visit deletes again.
pub async fn apply_visit(
    plan: &VisitPlan,
    journal: &mut Journal,
    data_blob: &dyn Blob,
    commit: bool,
) -> Result<Vec<String>, VisitError> {
    let mut deleted = Vec::new();
    if commit {
        let results: Vec<_> = futures::stream::iter(plan.to_delete.iter())
            .map(|k| async move { (k, data_blob.delete(k).await) })
            .buffer_unordered(16)
            .collect()
            .await;
        for (k, r) in results {
            r.expect("delete");
            deleted.push(k.clone());
        }
    }
    journal
        .commit(plan.upper, &plan.retract, &plan.insert)
        .await
        .map_err(|LostRace| VisitError::LostRace)?;
    deleted.sort();
    Ok(deleted)
}

/// One visit: plan, then apply.
pub async fn visit(
    unit: &str,
    next_unit: Option<&str>,
    filter: Option<&ObjectFilter>,
    rules: &dyn Rules,
    journal: &mut Journal,
    blob: &dyn Blob,
    opts: VisitOpts,
) -> Result<(VisitPlan, Vec<String>), VisitError> {
    let plan = plan_visit(unit, next_unit, filter, rules, journal, blob, opts).await?;
    let deleted = apply_visit(&plan, journal, blob, opts.commit).await?;
    Ok((plan, deleted))
}

/// Runs one lap over every unit of `rule_sets`, starting at the journal's cursor.
pub async fn lap(
    rule_sets: &[&dyn Rules],
    journal: &mut Journal,
    blob: &dyn Blob,
    opts: VisitOpts,
) -> Result<Vec<VisitPlan>, VisitError> {
    let units: Vec<(String, usize)> = rule_sets
        .iter()
        .enumerate()
        .flat_map(|(i, r)| r.units().into_iter().map(move |u| (u, i)))
        .collect();
    let start = journal.load().await;
    let start_idx = start
        .cursor()
        .and_then(|c| units.iter().position(|(u, _)| u == c))
        .unwrap_or(0);
    let mut plans = Vec::new();
    for step in 0..units.len() {
        let (unit, rules) = &units[(start_idx + step) % units.len()];
        let next = &units[(start_idx + step + 1) % units.len()].0;
        let (plan, _) = visit(
            unit,
            Some(next),
            None,
            rule_sets[*rules],
            journal,
            blob,
            opts,
        )
        .await?;
        if plan.verdicts.iter().any(|o| !o.per_key.is_empty()) {
            plans.push(plan);
        }
    }
    Ok(plans)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_persist::location::SeqNo;

    use super::*;
    use crate::batch::{
        BLOB_TARGET_SIZE, INLINE_WRITES_SINGLE_MAX_BYTES, INLINE_WRITES_TOTAL_MAX_BYTES,
        MAX_RUN_LEN, MAX_RUNS,
    };
    use crate::cache::PersistClientCache;
    use crate::critical::Opaque;
    use crate::internal::paths::{KeyRoot, PartId, PartialBatchKey, PartialRollupKey, RollupId};
    use crate::lease::LeaseShard;
    use crate::{BlobLeaseId, PersistLocation};

    const V26: Version = Version::new(26, 1, 0);
    const V27: Version = Version::new(27, 1, 0);
    const V28: Version = Version::new(28, 1, 0);
    const DAY_MS: u64 = 24 * 60 * 60 * 1000;

    fn cache() -> PersistClientCache {
        let cache = PersistClientCache::new_no_metrics();
        cache.cfg.set_config(&BLOB_TARGET_SIZE, 0);
        cache.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        cache.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
        cache
    }

    async fn open(cache: &mut PersistClientCache, v: &Version) -> PersistClient {
        cache.cfg.build_version = v.clone();
        cache.open(PersistLocation::new_in_mem()).await.unwrap()
    }

    async fn keys_under(blob: &dyn Blob, prefix: &str) -> BTreeSet<String> {
        let mut keys = BTreeSet::new();
        blob.list_keys_and_metadata(prefix, &mut |m| {
            keys.insert(m.key.to_owned());
        })
        .await
        .unwrap();
        keys
    }

    async fn judge_prefix(rules: &dyn Rules, blob: &dyn Blob, prefix: &str) -> UnitVerdicts {
        let mut keys = Vec::new();
        blob.list_keys_and_metadata(prefix, &mut |m| {
            keys.push((m.key.to_owned(), m.size_in_bytes))
        })
        .await
        .unwrap();
        rules
            .judge(&UnitListing {
                unit: prefix.to_owned(),
                keys,
            })
            .await
    }

    async fn verdicts(
        rules: &dyn Rules,
        blob: &dyn Blob,
        prefix: &str,
    ) -> BTreeMap<String, Verdict> {
        judge_prefix(rules, blob, prefix)
            .await
            .objects
            .into_iter()
            .flat_map(|o| o.per_key.into_iter().map(|(k, _, v)| (k, v)))
            .collect()
    }

    struct Pending(BTreeSet<String>, Version);
    #[async_trait]
    impl PendingParts for Pending {
        async fn pending(&self, _shard: ShardId) -> Option<(BTreeSet<String>, Version)> {
            Some((self.0.clone(), self.1.clone()))
        }
    }
    struct NotTxnWal;
    #[async_trait]
    impl PendingParts for NotTxnWal {
        async fn pending(&self, _shard: ShardId) -> Option<(BTreeSet<String>, Version)> {
            None
        }
    }

    fn rules(client: &PersistClient, floor: Option<Version>) -> ShardRules {
        ShardRules::new(client, Some(Arc::new(NotTxnWal)), floor, 10)
    }

    type W = WriteHandle<String, String, u64, i64>;

    /// Appends one row and builds (but never appends) an orphan batch. Returns the orphan's keys
    /// and its wire form.
    async fn shard_with_orphan(
        client: &PersistClient,
        shard: ShardId,
    ) -> (W, BTreeSet<String>, crate::batch::ProtoBatch) {
        let (mut w, r) = client.expect_open::<String, String, u64, i64>(shard).await;
        r.expire().await;
        w.expect_compare_and_append(&[(("a".into(), "a".into()), 0, 1)], 0, 1)
            .await;
        let before = keys_under(&*client.blob, &shard.to_string()).await;
        let orphan = w
            .expect_batch(&[(("o".into(), "o".into()), 10, 1)], 10, 11)
            .await
            .into_transmittable_batch();
        let after = keys_under(&*client.blob, &shard.to_string()).await;
        let orphan_keys: BTreeSet<_> = after.difference(&before).cloned().collect();
        assert!(!orphan_keys.is_empty());
        (w, orphan_keys, orphan)
    }

    // R2 tests 1 and 6: rule 3 waits for an explicit version upgrade, and a cross-version append
    // can then link a part the rules already called a candidate.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn case_a_version_gate_and_cross_version_append() {
        let mut cache = cache();
        let c26 = open(&mut cache, &V26).await;
        let shard = ShardId::new();
        let (w, orphan_keys, orphan) = shard_with_orphan(&c26, shard).await;
        let prefix = shard.to_string();

        let v = verdicts(&rules(&c26, None), &*c26.blob, &prefix).await;
        for k in &orphan_keys {
            assert_eq!(v[k], Verdict::Report(Reason::WriterNotOlder), "{k}");
        }
        assert!(v.values().all(|v| !matches!(v, Verdict::Candidate(_))));
        w.expire().await;

        // Opening and writing at 27 doesn't move the shard's version, so nothing changes.
        let c27 = open(&mut cache, &V27).await;
        let (mut w27, r27) = c27.expect_open::<String, String, u64, i64>(shard).await;
        r27.expire().await;
        w27.expect_compare_and_append(&[(("b".into(), "b".into()), 1, 1)], 1, 2)
            .await;
        let v = verdicts(&rules(&c27, None), &*c27.blob, &prefix).await;
        for k in &orphan_keys {
            assert_eq!(v[k], Verdict::Report(Reason::WriterNotOlder), "{k}");
        }

        // An explicit upgrade does.
        c27.upgrade_version::<String, String, u64, i64>(shard, Diagnostics::for_tests())
            .await
            .unwrap();
        let v = verdicts(&rules(&c27, None), &*c27.blob, &prefix).await;
        for k in &orphan_keys {
            assert_eq!(
                v[k],
                Verdict::Candidate(format!("{shard}/{}", WriterKey::for_version(&V26))),
                "{k}"
            );
        }
        // Without the txns shard, the same keys only report.
        let no_txns = ShardRules::new(&c27, None, None, 10);
        let v = verdicts(&no_txns, &*c27.blob, &prefix).await;
        for k in &orphan_keys {
            assert_eq!(v[k], Verdict::Report(Reason::NoTxnsShard));
        }

        // The reaper deletes the candidates (as if `min_age` had passed)...
        for k in &orphan_keys {
            c27.blob.delete(k).await.unwrap();
        }
        // ...and a 27 writer handed the 26 writer's batch appends it anyway.
        w27.compare_and_append_batch(
            &mut [],
            Antichain::from_elem(2),
            Antichain::from_elem(10),
            true,
        )
        .await
        .unwrap()
        .unwrap();
        let mut batch = w27.batch_from_transmittable_batch(orphan);
        let res = w27
            .compare_and_append_batch(
                &mut [&mut batch],
                Antichain::from_elem(10),
                Antichain::from_elem(11),
                true,
            )
            .await
            .expect("valid usage");
        assert_eq!(res, Ok(()), "the cross-version append is accepted");
        let reach = rules(&c27, None).reach(shard).await.unwrap();
        let present = keys_under(&*c27.blob, &prefix).await;
        let dangling: Vec<_> = reach.referenced.difference(&present).collect();
        eprintln!(
            "state now names {} deleted blobs: {dangling:?}",
            dangling.len()
        );
        assert!(
            orphan_keys
                .iter()
                .all(|k| reach.referenced.contains(k) && !present.contains(k)),
            "state names the deleted orphan parts"
        );
        w27.expire().await;
    }

    // R2 test 2: a tombstoned shard's leftovers are candidates at the same version.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn case_a_tombstone() {
        let mut cache = cache();
        let c = open(&mut cache, &V26).await;
        let shard = ShardId::new();
        let (mut w, orphan_keys, _orphan) = shard_with_orphan(&c, shard).await;
        w.advance_upper(&Antichain::new()).await;
        let mut since = c
            .open_critical_since::<String, String, u64, i64>(
                shard,
                PersistClient::CONTROLLER_CRITICAL_SINCE,
                Opaque::encode(&0u64),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        let epoch = since.opaque().clone();
        since
            .compare_and_downgrade_since(&epoch, (&epoch, &Antichain::new()))
            .await
            .unwrap();
        w.expire().await;
        c.finalize_shard::<String, String, u64, i64>(shard, Diagnostics::for_tests())
            .await
            .unwrap();

        let reach = rules(&c, None).reach(shard).await.unwrap();
        assert!(reach.tombstone);
        let v = verdicts(&rules(&c, None), &*c.blob, &shard.to_string()).await;
        for k in &orphan_keys {
            assert!(matches!(&v[k], Verdict::Candidate(_)), "{k}: {:?}", v[k]);
        }
        eprintln!("tombstone verdicts: {v:?}");
    }

    // R2 tests 4 and 5: parts reachable only through nested hollow runs are kept, and a part the
    // txns shard names is kept.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn case_a_hollow_runs_and_pending() {
        let mut cache = cache();
        cache.cfg.set_config(&MAX_RUN_LEN, 2);
        cache.cfg.set_config(&MAX_RUNS, 2);
        let c = open(&mut cache, &V26).await;
        let shard = ShardId::new();
        let (mut w, r) = c.expect_open::<String, String, u64, i64>(shard).await;
        r.expire().await;
        let mut b = w.builder(Antichain::from_elem(0));
        for i in 0..20 {
            b.add(&format!("{i:02}"), &"v".to_owned(), &0, &1)
                .await
                .unwrap();
        }
        let mut batch = b.finish(Antichain::from_elem(1)).await.unwrap();
        assert!(
            batch
                .batch
                .parts
                .iter()
                .any(|p| matches!(p, RunPart::Many(_)))
        );
        // Keys the batch names directly: its single parts and its top-level run blobs.
        let direct: BTreeSet<String> = batch
            .batch
            .parts
            .iter()
            .map(|p| match p {
                RunPart::Single(BatchPart::Hollow(x)) => x.key.complete(&shard).to_string(),
                RunPart::Many(r) => r.key.complete(&shard).to_string(),
                RunPart::Single(BatchPart::Inline { .. }) => unreachable!(),
            })
            .collect();
        w.compare_and_append_batch(
            &mut [&mut batch],
            Antichain::from_elem(0),
            Antichain::from_elem(1),
            true,
        )
        .await
        .unwrap()
        .unwrap();
        let v = verdicts(&rules(&c, Some(V28)), &*c.blob, &shard.to_string()).await;
        let reach = rules(&c, None).reach(shard).await.unwrap();
        let via_runs: Vec<_> = reach
            .referenced
            .difference(&direct)
            .filter(|k| k.contains("/n"))
            .collect();
        assert!(via_runs.len() >= 10, "{via_runs:?}");
        for k in &via_runs {
            assert_eq!(v[*k], Verdict::Keep, "{k}");
        }
        // NOTE: the builder's run merges (`max_runs`) left their inputs behind, unreferenced. At
        // the shard's own version they only report.
        let leftovers = v
            .values()
            .filter(|x| **x == Verdict::Report(Reason::WriterNotOlder))
            .count();
        eprintln!(
            "shard batch: {} keys kept via runs, {leftovers} builder merge leftovers",
            via_runs.len()
        );

        // Pending: an orphan the txns shard names is kept even once the version moves.
        let before = keys_under(&*c.blob, &shard.to_string()).await;
        let _orphan = w
            .expect_batch(&[(("o".into(), "o".into()), 1, 1)], 1, 2)
            .await
            .into_transmittable_batch();
        let orphan_keys: BTreeSet<_> = keys_under(&*c.blob, &shard.to_string())
            .await
            .difference(&before)
            .cloned()
            .collect();
        w.expire().await;
        let c27 = open(&mut cache, &V27).await;
        c27.upgrade_version::<String, String, u64, i64>(shard, Diagnostics::for_tests())
            .await
            .unwrap();
        let named = ShardRules::new(
            &c27,
            Some(Arc::new(Pending(orphan_keys.clone(), V27))),
            None,
            10,
        );
        let v = verdicts(&named, &*c27.blob, &shard.to_string()).await;
        for k in &orphan_keys {
            assert_eq!(v[k], Verdict::Keep, "{k}");
        }
        // The txns shard's own version caps rule 3: still at 26, nothing is older.
        let capped = ShardRules::new(
            &c27,
            Some(Arc::new(Pending(BTreeSet::new(), V26))),
            None,
            10,
        );
        let v = verdicts(&capped, &*c27.blob, &shard.to_string()).await;
        for k in &orphan_keys {
            assert_eq!(v[k], Verdict::Report(Reason::WriterNotOlder), "{k}");
        }
    }

    fn fake_part(shard: ShardId, v: &Version) -> String {
        PartialBatchKey::new(&WriterKey::for_version(v), &PartId::new())
            .complete(&shard)
            .to_string()
    }

    fn fake_rollup(shard: ShardId) -> String {
        format!(
            "{shard}/{}",
            PartialRollupKey::new(SeqNo(1), &RollupId::new())
        )
    }

    fn rollup_bytes(v: &Version) -> bytes::Bytes {
        bytes::Bytes::from(
            ProtoRollup {
                applier_version: v.to_string(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    fn shard_in_unit(unit_hex: &str) -> ShardId {
        let s = ShardId::new().to_string();
        format!("s{unit_hex}{}", &s[4..]).parse().unwrap()
    }

    // R3 tests 1 to 3.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn case_b_floor_rollups_and_tripwire() {
        let mut cache = cache();
        let c = open(&mut cache, &V27).await;
        // A fake stash: parts at 26, no consensus entry.
        let stash = ShardId::new();
        for _ in 0..3 {
            c.blob
                .set(&fake_part(stash, &V26), bytes::Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        let p = stash.to_string();
        let absent = Verdict::Candidate(format!("{stash}/absent"));
        let all = |v: &BTreeMap<String, Verdict>, want: &Verdict| {
            assert_eq!(v.len(), 3);
            assert!(v.values().all(|x| x == want), "{v:?}");
        };
        all(
            &verdicts(&rules(&c, Some(V27)), &*c.blob, &p).await,
            &absent,
        );
        all(
            &verdicts(&rules(&c, None), &*c.blob, &p).await,
            &Verdict::Report(Reason::NoFloor),
        );
        all(
            &verdicts(&rules(&c, Some(V26)), &*c.blob, &p).await,
            &Verdict::Report(Reason::WriterNotBelowFloor),
        );

        // A crashed creation: a rollup written by 27 and no consensus entry. Not a candidate
        // while 27 may still be running, even though its parts are old.
        let created = ShardId::new();
        c.blob
            .set(&fake_rollup(created), rollup_bytes(&V27))
            .await
            .unwrap();
        c.blob
            .set(&fake_part(created, &V26), bytes::Bytes::from_static(b"x"))
            .await
            .unwrap();
        let p = created.to_string();
        let v = verdicts(&rules(&c, Some(V27)), &*c.blob, &p).await;
        assert!(
            v.values()
                .all(|x| *x == Verdict::Report(Reason::WriterNotBelowFloor)),
            "{v:?}"
        );
        let v = verdicts(&rules(&c, Some(V28)), &*c.blob, &p).await;
        assert!(
            v.values().all(|x| matches!(x, Verdict::Candidate(_))),
            "{v:?}"
        );

        // Tripwire: more absent-with-rollup shards in a unit than the threshold.
        for _ in 0..3 {
            let s = shard_in_unit("abc");
            c.blob
                .set(&fake_rollup(s), rollup_bytes(&V26))
                .await
                .unwrap();
        }
        let tight = ShardRules::new(&c, Some(Arc::new(NotTxnWal)), Some(V28), 2);
        assert!(
            judge_prefix(&tight, &*c.blob, "sabc")
                .await
                .tripwire
                .is_some()
        );
        let loose = ShardRules::new(&c, Some(Arc::new(NotTxnWal)), Some(V28), 3);
        assert!(
            judge_prefix(&loose, &*c.blob, "sabc")
                .await
                .tripwire
                .is_none()
        );
    }

    // R4 tests.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn journal_basics() {
        let mut cache = cache();
        let c = open(&mut cache, &V27).await;
        let id = ShardId::new();
        let mut a = Journal::open(&c, id).await;
        let mut b = Journal::open(&c, id).await;
        let st = a.load().await;
        assert_eq!(st.cursor(), None);
        assert!(st.rows.is_empty());
        let row = |u: &str| JournalRow::Cursor { unit: u.into() };
        let up = st.upper;
        assert_eq!(a.commit(up, &[], &[row("s001")]).await, Ok(()));
        assert_eq!(b.commit(up, &[], &[row("s002")]).await, Err(LostRace));
        assert_eq!(b.load().await.cursor(), Some("s001"));
    }

    /// Writes a fake stash of `n` parts at 26 into the unit `unit_hex`.
    async fn stash_in(c: &PersistClient, unit_hex: &str, n: usize) -> (ShardId, BTreeSet<String>) {
        let s = shard_in_unit(unit_hex);
        let mut keys = BTreeSet::new();
        for _ in 0..n {
            let k = fake_part(s, &V26);
            c.blob
                .set(&k, bytes::Bytes::from_static(b"x"))
                .await
                .unwrap();
            keys.insert(k);
        }
        (s, keys)
    }

    fn opts(commit: bool, now_ms: u64) -> VisitOpts {
        VisitOpts {
            commit,
            min_age_ms: 7 * DAY_MS,
            now_ms,
        }
    }

    fn clocks(st: &JournalState) -> Vec<(String, u64)> {
        st.rows
            .iter()
            .filter_map(|r| match r {
                JournalRow::Clock {
                    group,
                    first_seen_ms,
                    ..
                } => Some((group.clone(), *first_seen_ms)),
                _ => None,
            })
            .collect()
    }

    // R5 tests 1, 2 and 5.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn walker_report_commit_and_first_sight() {
        let mut cache = cache();
        let c = open(&mut cache, &V27).await;
        let r = rules(&c, Some(V27));
        let (stash, keys) = stash_in(&c, "123", 3).await;
        let mut j = Journal::open(&c, ShardId::new()).await;
        let t0 = 1_000 * DAY_MS;
        let unit = "s123";

        // 1. Report-only writes clocks and deletes nothing, even once aged.
        let (plan, deleted) = visit(
            unit,
            Some("s124"),
            None,
            &r,
            &mut j,
            &*c.blob,
            opts(false, t0),
        )
        .await
        .unwrap();
        assert!(deleted.is_empty() && plan.to_delete.is_empty());
        assert_eq!(
            clocks(&j.load().await),
            vec![(format!("{stash}/absent"), t0)]
        );
        let (plan, deleted) = visit(
            unit,
            Some("s124"),
            None,
            &r,
            &mut j,
            &*c.blob,
            opts(false, t0 + 8 * DAY_MS),
        )
        .await
        .unwrap();
        assert_eq!(plan.to_delete.len(), 3);
        assert!(deleted.is_empty());
        assert_eq!(keys_under(&*c.blob, unit).await, keys);
        assert_eq!(j.load().await.cursor(), Some("s124"));

        // 2. Committed: nothing before min_age, everything at it, then the clock goes.
        let (_, deleted) = visit(
            unit,
            None,
            None,
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS - 1),
        )
        .await
        .unwrap();
        assert!(deleted.is_empty());
        let (_, deleted) = visit(
            unit,
            None,
            None,
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS),
        )
        .await
        .unwrap();
        assert_eq!(deleted.into_iter().collect::<BTreeSet<_>>(), keys);
        assert!(keys_under(&*c.blob, unit).await.is_empty());
        assert_eq!(
            clocks(&j.load().await).len(),
            1,
            "kept: the group had candidates"
        );

        // 5. A key that becomes a candidate in an aged group is deleted on first sight.
        let late = fake_part(stash, &V26);
        c.blob
            .set(&late, bytes::Bytes::from_static(b"x"))
            .await
            .unwrap();
        let (_, deleted) = visit(
            unit,
            None,
            None,
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS + 1),
        )
        .await
        .unwrap();
        assert_eq!(deleted, vec![late], "deleted with no wait of its own");

        let (_, deleted) = visit(
            unit,
            None,
            None,
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS + 2),
        )
        .await
        .unwrap();
        assert!(deleted.is_empty());
        assert!(
            clocks(&j.load().await).is_empty(),
            "dropped once no candidates remain"
        );
    }

    // R5 tests 3 and 6: two visits plan against the same journal upper, both delete, one loses
    // the append. The deleted set equals one visit's, and a rerun is clean.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn walker_racing_visits_and_crash_before_append() {
        let mut cache = cache();
        let c = open(&mut cache, &V27).await;
        let r = rules(&c, Some(V27));
        let (_stash, keys) = stash_in(&c, "456", 4).await;
        let jid = ShardId::new();
        let mut ja = Journal::open(&c, jid).await;
        let mut jb = Journal::open(&c, jid).await;
        let unit = "s456";
        let t0 = 1_000 * DAY_MS;
        visit(unit, None, None, &r, &mut ja, &*c.blob, opts(true, t0))
            .await
            .unwrap();

        let t1 = t0 + 7 * DAY_MS;
        let pa = plan_visit(
            unit,
            Some("s457"),
            None,
            &r,
            &mut ja,
            &*c.blob,
            opts(true, t1),
        )
        .await
        .unwrap();
        let pb = plan_visit(
            unit,
            Some("s457"),
            None,
            &r,
            &mut jb,
            &*c.blob,
            opts(true, t1),
        )
        .await
        .unwrap();
        assert_eq!(pa.upper, pb.upper);
        let da = apply_visit(&pa, &mut ja, &*c.blob, true).await.unwrap();
        // B is the "crash between deletes and the append": its deletes ran, its append fails.
        let db = apply_visit(&pb, &mut jb, &*c.blob, true).await;
        assert!(matches!(db, Err(VisitError::LostRace)));
        assert_eq!(da.into_iter().collect::<BTreeSet<_>>(), keys);
        assert!(keys_under(&*c.blob, unit).await.is_empty());

        // The rerun deletes nothing new and commits.
        let (_, deleted) = visit(unit, None, None, &r, &mut jb, &*c.blob, opts(true, t1 + 1))
            .await
            .unwrap();
        assert!(deleted.is_empty());
    }

    // R5 test 4: a tripwire stops the visit before the journal or the blob changes.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn walker_tripwire() {
        let mut cache = cache();
        let c = open(&mut cache, &V27).await;
        let (_stash, keys) = stash_in(&c, "789", 2).await;
        let s = shard_in_unit("789");
        c.blob
            .set(&fake_rollup(s), rollup_bytes(&V26))
            .await
            .unwrap();
        let r = ShardRules::new(&c, Some(Arc::new(NotTxnWal)), Some(V27), 0);
        let mut j = Journal::open(&c, ShardId::new()).await;
        let before = j.load().await.upper;
        let res = visit("s789", None, None, &r, &mut j, &*c.blob, opts(true, 0)).await;
        assert!(matches!(res, Err(VisitError::Tripwire(_))), "{res:?}");
        assert_eq!(j.load().await.upper, before);
        assert!(keys.iter().all(|k| {
            futures::executor::block_on(c.blob.get(k))
                .unwrap()
                .is_some()
        }));
    }

    fn leased_unit(lease: BlobLeaseId) -> String {
        format!("leased/{}", &lease.to_string()[..4])
    }

    async fn leased_part(c: &PersistClient, lease: BlobLeaseId) -> String {
        let k = KeyRoot::Leased {
            lease,
            object: uuid::Uuid::new_v4(),
        }
        .complete(&PartialBatchKey::new(
            &WriterKey::for_version(&V27),
            &PartId::new(),
        ))
        .to_string();
        c.blob
            .set(&k, bytes::Bytes::from_static(b"x"))
            .await
            .unwrap();
        k
    }

    // R6: expired, live and row-less leases; then the writer-race footnote in both orderings.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn leased_rules_and_late_writer() {
        let mut cache = cache();
        let c = open(&mut cache, &V27).await;
        let lease_shard = ShardId::new();
        let mut ls = LeaseShard::open(&c, lease_shard).await;
        let (expired, live, norow) = (BlobLeaseId::new(), BlobLeaseId::new(), BlobLeaseId::new());
        ls.create(expired, Duration::from_secs(1), "t")
            .await
            .unwrap();
        ls.create(live, Duration::from_secs(1), "t").await.unwrap();
        ls.expire(expired, 0).await.unwrap();
        let mut keys = BTreeMap::new();
        for l in [expired, live, norow] {
            keys.insert(l, vec![leased_part(&c, l).await, leased_part(&c, l).await]);
        }
        let r = LeasedRules::new(Some(LeaseShard::open(&c, lease_shard).await));
        for (l, want) in [
            (expired, Verdict::Candidate(format!("leased/{expired}"))),
            (live, Verdict::Keep),
            (norow, Verdict::Report(Reason::LeaseNoRow)),
        ] {
            let v = verdicts(&r, &*c.blob, &format!("leased/{l}/")).await;
            assert_eq!(v.len(), 2);
            assert!(v.values().all(|x| *x == want), "{l}: {v:?}");
        }
        let none = LeasedRules::new(None);
        let v = verdicts(&none, &*c.blob, "leased/").await;
        assert!(
            v.values()
                .all(|x| *x == Verdict::Report(Reason::NoLeaseShard))
        );

        // Only the expired lease's keys go, after min_age.
        let mut j = Journal::open(&c, ShardId::new()).await;
        let t0 = 1_000 * DAY_MS;
        let unit = leased_unit(expired);
        let f = ObjectFilter {
            prefix: format!("leased/{expired}/"),
            object: format!("leased/{expired}"),
        };
        visit(&unit, None, Some(&f), &r, &mut j, &*c.blob, opts(true, t0))
            .await
            .unwrap();
        let (_, deleted) = visit(
            &unit,
            None,
            Some(&f),
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS),
        )
        .await
        .unwrap();
        assert_eq!(deleted.len(), 2);
        let mut left = keys_under(&*c.blob, "leased/").await;
        for l in [live, norow] {
            for k in &keys[&l] {
                assert!(left.remove(k));
            }
        }
        assert!(left.is_empty());

        // Late writer, ordering (a): lands while the group's clock is still aged. Deleted on
        // first sight, mid-upload if it were a multi-part batch.
        let late_a = leased_part(&c, expired).await;
        let (_, deleted) = visit(
            &unit,
            None,
            Some(&f),
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS + 1),
        )
        .await
        .unwrap();
        assert_eq!(deleted, vec![late_a]);

        // Ordering (b): lands after a visit found the prefix empty and dropped the clock. Waits a
        // fresh min_age.
        visit(
            &unit,
            None,
            Some(&f),
            &r,
            &mut j,
            &*c.blob,
            opts(true, t0 + 7 * DAY_MS + 2),
        )
        .await
        .unwrap();
        let late_b = leased_part(&c, expired).await;
        let t2 = t0 + 7 * DAY_MS + 3;
        let (_, deleted) = visit(&unit, None, Some(&f), &r, &mut j, &*c.blob, opts(true, t2))
            .await
            .unwrap();
        assert!(deleted.is_empty());
        let (_, deleted) = visit(
            &unit,
            None,
            Some(&f),
            &r,
            &mut j,
            &*c.blob,
            opts(true, t2 + 7 * DAY_MS),
        )
        .await
        .unwrap();
        assert_eq!(deleted, vec![late_b]);
    }
}
