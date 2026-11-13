// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The resolver's in-memory view of its manifests, and how ops become rows.
//!
//! A view is the manifests' live rows, one value per key. Ops are checked
//! against it and applied to it in place: a commit whose op fails a
//! precondition has its earlier ops' rows undone, so a commit lands whole or not
//! at all. The rows a commit changed are what the resolver writes to the
//! manifests.

use std::collections::BTreeMap;

use mz_persist_client::v2::{
    BatchId, Frontier, Manifest, ManifestKey, OpKind, PartitionId, ProtoClaim, ProtoFrontier,
    ProtoHollowBatch, ProtoIntent, ProtoOp, ProtoShardMeta, ProtoShardView, SENTINEL_READER,
    ValKind, frontier, op_shard, proto_frontier,
};
use mz_persist_types::ShardId;
use mz_persist_types::schema::SchemaId;
use timely::PartialOrder;
use timely::progress::Antichain;

/// One change to a manifest row.
pub type RowDiff = (ManifestKey, ValKind, i64);

/// Why a commit didn't apply.
#[derive(Clone, Debug, PartialEq)]
pub enum CommitError {
    /// A precondition failed: the caller's view was stale.
    Rejected(String),
    /// The ops were malformed.
    Invalid(String),
}

/// The live rows of every manifest the resolver follows, and each manifest's
/// time.
#[derive(Clone, Debug, Default)]
pub struct View {
    pub rows: BTreeMap<ManifestKey, ValKind>,
    /// The upper of the control manifest: the rows reflect its writes before it.
    pub control_time: u64,
    /// The upper of the partition manifest.
    pub partition_time: u64,
    /// The partition the partition rows belong to.
    pub partition: PartitionId,
    /// The view seqno at which each shard's rows last changed, for watches.
    pub changed_at: BTreeMap<ShardId, u64>,
}

impl View {
    pub fn new(partition: PartitionId) -> Self {
        View {
            partition,
            ..Default::default()
        }
    }

    /// Applies one row change. Panics if it doesn't keep one value per key, which
    /// would mean the manifest was written wrongly.
    pub fn apply(&mut self, key: ManifestKey, val: ValKind, diff: i64) {
        match diff {
            1 => {
                let prev = self.rows.insert(key.clone(), val);
                assert!(prev.is_none(), "two live values for {key:?}");
            }
            -1 => {
                let prev = self.rows.remove(&key);
                assert_eq!(
                    prev.as_ref(),
                    Some(&val),
                    "retracted a value not live for {key:?}"
                );
            }
            d => panic!("unexpected diff {d} for {key:?}"),
        }
    }

    fn set(&mut self, out: &mut Vec<RowDiff>, key: ManifestKey, val: Option<ValKind>) {
        if let Some(old) = self.rows.get(&key).cloned() {
            self.apply(key.clone(), old.clone(), -1);
            out.push((key.clone(), old, -1));
        }
        if let Some(val) = val {
            self.apply(key.clone(), val.clone(), 1);
            out.push((key, val, 1));
        }
    }

    fn undo(&mut self, diffs: &[RowDiff]) {
        for (k, v, d) in diffs.iter().rev() {
            self.apply(k.clone(), v.clone(), -d);
        }
    }

    pub fn shard_meta(&self, shard: &ShardId) -> Option<ProtoShardMeta> {
        match self.rows.get(&ManifestKey::Shard(*shard)) {
            Some(ValKind::Shard(m)) => Some(m.clone()),
            _ => None,
        }
    }

    pub fn upper(&self, shard: &ShardId) -> Option<Frontier> {
        match self.rows.get(&ManifestKey::Upper(*shard)) {
            Some(ValKind::Upper(u)) => Some(frontier(&Some(u.clone()))),
            _ => None,
        }
    }

    /// The control manifest time the partition's rows are consistent with.
    pub fn control_at(&self) -> Option<u64> {
        match self.rows.get(&ManifestKey::ControlAt) {
            Some(ValKind::ControlAt(c)) => Some(*c),
            _ => None,
        }
    }

    /// Records that the partition is consistent with control as of the control
    /// write at manifest time `c`.
    pub fn set_control_at(&mut self, c: u64) -> Vec<RowDiff> {
        let mut out = Vec::new();
        self.set(
            &mut out,
            ManifestKey::ControlAt,
            Some(ValKind::ControlAt(c)),
        );
        out
    }

    pub fn claim(&self) -> Option<ProtoClaim> {
        match self.rows.get(&ManifestKey::Claim) {
            Some(ValKind::Claim(c)) => Some(c.clone()),
            _ => None,
        }
    }

    fn shard_rows<'a>(
        &'a self,
        shard: &ShardId,
        start: ManifestKey,
    ) -> impl Iterator<Item = (&'a ManifestKey, &'a ValKind)> + 'a {
        let shard = *shard;
        let kind = std::mem::discriminant(&start);
        self.rows.range(start..).take_while(move |(k, _)| {
            std::mem::discriminant(*k) == kind && k.shard() == Some(shard)
        })
    }

    pub fn batches(&self, shard: &ShardId) -> BTreeMap<BatchId, ProtoHollowBatch> {
        let start = ManifestKey::Batch(
            *shard,
            BatchId {
                lower: vec![],
                upper: vec![],
            },
        );
        self.shard_rows(shard, start)
            .filter_map(|(k, v)| match (k, v) {
                (ManifestKey::Batch(_, id), ValKind::Batch(b)) => Some((id.clone(), b.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn intents(&self, shard: &ShardId) -> BTreeMap<String, ProtoIntent> {
        let start = ManifestKey::Intent(*shard, String::new());
        self.shard_rows(shard, start)
            .filter_map(|(k, v)| match (k, v) {
                (ManifestKey::Intent(_, r), ValKind::Intent(i)) => Some((r.clone(), i.clone())),
                _ => None,
            })
            .collect()
    }

    /// The earliest frontier any intent needs readable, or None with no intents.
    pub fn readable(&self, shard: &ShardId) -> Option<Frontier> {
        self.intents(shard)
            .values()
            .map(|i| frontier(&i.frontier))
            .reduce(|a, b| meet(&a, &b))
    }

    /// The seqno a client sees for any shard's view: it advances whenever either
    /// manifest is written.
    pub fn seqno(&self) -> u64 {
        self.control_time + self.partition_time
    }

    pub fn shard_view(&self, shard: &ShardId) -> Option<ProtoShardView> {
        let meta = self.shard_meta(shard)?;
        let schemas = self
            .rows
            .range(ManifestKey::Schema(*shard, SchemaId(0))..)
            .take_while(|(k, _)| matches!(k, ManifestKey::Schema(s, _) if s == shard))
            .filter_map(|(k, v)| match (k, v) {
                (ManifestKey::Schema(_, id), ValKind::Schema(s)) => {
                    Some((u64::try_from(id.0).expect("fits"), s.clone()))
                }
                _ => None,
            })
            .collect();
        Some(ProtoShardView {
            shard: shard.to_string(),
            seqno: self.seqno(),
            meta: Some(meta),
            schemas,
            upper: self.upper(shard).map(|u| proto_frontier(&u)),
            batches: self.batches(shard).into_values().collect(),
            intents: self.intents(shard),
        })
    }

    /// Checks `ops` against the view and applies them, all or none. Returns the
    /// row changes, to be written to the manifests.
    pub fn commit(&mut self, ops: &[ProtoOp]) -> Result<Vec<RowDiff>, CommitError> {
        let mut diffs = Vec::new();
        for op in ops {
            if let Err(err) = self.apply_op(op, &mut diffs) {
                self.undo(&diffs);
                return Err(err);
            }
        }
        Ok(diffs)
    }

    fn apply_op(&mut self, op: &ProtoOp, out: &mut Vec<RowDiff>) -> Result<(), CommitError> {
        let shard = op_shard(op).map_err(CommitError::Invalid)?;
        let kind = op
            .kind
            .as_ref()
            .ok_or_else(|| CommitError::Invalid("op without a kind".into()))?;
        let rejected = |msg: String| Err(CommitError::Rejected(msg));

        if let OpKind::Register(r) = kind {
            if self.shard_meta(&shard).is_some() {
                return rejected(format!("{shard} is already registered"));
            }
            let meta = ProtoShardMeta {
                codecs: r.codecs.clone(),
                finalized: false,
            };
            self.set(out, ManifestKey::Shard(shard), Some(ValKind::Shard(meta)));
            self.set(
                out,
                ManifestKey::Assign(shard),
                Some(ValKind::Assign(self.partition.clone())),
            );
            let min = ProtoFrontier { elements: vec![0] };
            self.set(
                out,
                ManifestKey::Upper(shard),
                Some(ValKind::Upper(min.clone())),
            );
            // Every data shard has a since from the start, as a new persist shard
            // does.
            let sentinel = ProtoIntent {
                frontier: Some(min),
                opaque_codec: None,
                opaque: None,
            };
            self.set(
                out,
                ManifestKey::Intent(shard, SENTINEL_READER.to_owned()),
                Some(ValKind::Intent(sentinel)),
            );
            return Ok(());
        }

        let Some(meta) = self.shard_meta(&shard) else {
            return Err(CommitError::Invalid(format!("{shard} isn't registered")));
        };
        if meta.finalized {
            return Err(CommitError::Invalid(format!("{shard} is finalized")));
        }
        match kind {
            OpKind::Register(_) => unreachable!("handled above"),
            OpKind::Evolve(e) => {
                let id = SchemaId(usize::try_from(e.schema_id).expect("fits"));
                let key = ManifestKey::Schema(shard, id);
                if self.rows.contains_key(&key) {
                    return rejected(format!("{shard} already has schema {id}"));
                }
                let schema = e.schema.clone().unwrap_or_default();
                self.set(out, key, Some(ValKind::Schema(schema)));
            }
            OpKind::Finalize(_) => {
                let readers: Vec<_> = self
                    .intents(&shard)
                    .into_keys()
                    .filter(|r| r != SENTINEL_READER)
                    .collect();
                if !readers.is_empty() {
                    return rejected(format!("finalize {shard} with read intents {readers:?}"));
                }
                let keys: Vec<_> = self
                    .rows
                    .keys()
                    .filter(|k| k.manifest() == Manifest::Partition && k.shard() == Some(shard))
                    .cloned()
                    .collect();
                for key in keys {
                    self.set(out, key, None);
                }
                let meta = ProtoShardMeta {
                    finalized: true,
                    ..meta
                };
                self.set(out, ManifestKey::Shard(shard), Some(ValKind::Shard(meta)));
            }
            OpKind::Append(a) => {
                let expected = frontier(&a.expected);
                let new = frontier(&a.new);
                let upper = self.upper(&shard).unwrap_or_default();
                if upper != expected {
                    return rejected(format!("{shard} upper is {upper:?}, expected {expected:?}"));
                }
                if !PartialOrder::less_equal(&expected, &new) {
                    return Err(CommitError::Invalid(format!(
                        "append to {shard} from {expected:?} to {new:?}"
                    )));
                }
                if let Some(batch) = a.batch.as_ref().filter(|b| !BatchId::is_empty_batch(b)) {
                    let id = BatchId::of(batch);
                    if id.lower != expected.elements() || id.upper != new.elements() {
                        return Err(CommitError::Invalid(format!(
                            "batch {id:?} doesn't span [{expected:?}, {new:?})"
                        )));
                    }
                    self.set(
                        out,
                        ManifestKey::Batch(shard, id),
                        Some(ValKind::Batch(batch.clone())),
                    );
                }
                if expected != new {
                    self.set(
                        out,
                        ManifestKey::Upper(shard),
                        Some(ValKind::Upper(proto_frontier(&new))),
                    );
                }
            }
            OpKind::ReadIntent(r) => {
                let key = ManifestKey::Intent(shard, r.reader.clone());
                let current = match self.rows.get(&key) {
                    Some(ValKind::Intent(i)) => Some(i.clone()),
                    _ => None,
                };
                if current != r.expected {
                    return rejected(format!(
                        "{shard} intent {} is {current:?}, expected {:?}",
                        r.reader, r.expected
                    ));
                }
                let new = r.new.clone().unwrap_or_default();
                if current.is_none() {
                    if let Some(readable) = self.readable(&shard) {
                        let f = frontier(&new.frontier);
                        if !PartialOrder::less_equal(&readable, &f) {
                            return rejected(format!(
                                "{shard} intent at {f:?} is before readable {readable:?}"
                            ));
                        }
                    }
                }
                self.set(out, key, Some(ValKind::Intent(new)));
                let sentinel = ManifestKey::Intent(shard, SENTINEL_READER.to_owned());
                if r.reader != SENTINEL_READER && self.rows.contains_key(&sentinel) {
                    self.set(out, sentinel, None);
                }
            }
            OpKind::DropIntent(d) => {
                let key = ManifestKey::Intent(shard, d.reader.clone());
                let current = match self.rows.get(&key) {
                    Some(ValKind::Intent(i)) => Some(i.clone()),
                    _ => None,
                };
                if current != d.expected {
                    return rejected(format!(
                        "{shard} intent {} is {current:?}, expected {:?}",
                        d.reader, d.expected
                    ));
                }
                let dropped = current.expect("checked");
                self.set(out, key, None);
                if self.intents(&shard).is_empty() {
                    let sentinel = ProtoIntent {
                        frontier: dropped.frontier,
                        opaque_codec: None,
                        opaque: None,
                    };
                    self.set(
                        out,
                        ManifestKey::Intent(shard, SENTINEL_READER.to_owned()),
                        Some(ValKind::Intent(sentinel)),
                    );
                }
            }
            OpKind::Compact(c) => {
                // A removed batch is named by its time range alone. Batches of a
                // shard never overlap, so the range picks out the batch the
                // compactor read, or one that already replaced it with the same
                // contents.
                for batch in c.remove.iter() {
                    let key = ManifestKey::Batch(shard, BatchId::of(batch));
                    if !self.rows.contains_key(&key) {
                        return rejected(format!("{key:?} is gone"));
                    }
                    self.set(out, key, None);
                }
                for batch in c.add.iter() {
                    let key = ManifestKey::Batch(shard, BatchId::of(batch));
                    self.set(out, key, Some(ValKind::Batch(batch.clone())));
                }
            }
        }
        Ok(())
    }

    /// Rows that break the manifests' invariants. Empty in a consistent view.
    pub fn invariant_violations(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (key, val) in self.rows.iter() {
            match (key, val) {
                (ManifestKey::Shard(s), ValKind::Shard(m)) if !m.finalized => {
                    if self.upper(s).is_none() {
                        out.push(format!("registered shard {s} has no upper"));
                    }
                }
                (ManifestKey::Batch(s, id), _) => {
                    if self.shard_meta(s).is_none() {
                        out.push(format!("batch {id:?} of unregistered shard {s}"));
                    }
                    let upper = self.upper(s).unwrap_or_default();
                    let batch_upper = Antichain::from(id.upper.clone());
                    if !PartialOrder::less_equal(&batch_upper, &upper) {
                        out.push(format!("batch {id:?} of {s} ends past upper {upper:?}"));
                    }
                }
                (ManifestKey::Intent(s, r), _) if self.shard_meta(s).is_none() => {
                    out.push(format!("intent {r} of unregistered shard {s}"));
                }
                _ => {}
            }
        }
        out
    }
}

/// The meet of two totally ordered frontiers: the earlier one.
fn meet(a: &Frontier, b: &Frontier) -> Frontier {
    if PartialOrder::less_equal(a, b) {
        a.clone()
    } else {
        b.clone()
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_client::v2::{
        ProtoAppend, ProtoCodecs, ProtoCompact, ProtoDropIntent, ProtoEmpty, ProtoReadIntent,
        ProtoRegister, ProtoU64Antichain,
    };
    use mz_persist_client::v2::{ProtoHollowBatch, ProtoOp};

    use super::*;

    fn op(shard: ShardId, kind: OpKind) -> ProtoOp {
        ProtoOp {
            shard: shard.to_string(),
            kind: Some(kind),
        }
    }

    fn register(s: ShardId) -> ProtoOp {
        op(
            s,
            OpKind::Register(ProtoRegister {
                codecs: Some(ProtoCodecs::default()),
            }),
        )
    }

    fn f(t: u64) -> ProtoFrontier {
        ProtoFrontier { elements: vec![t] }
    }

    fn batch(lower: u64, upper: u64, len: u64) -> ProtoHollowBatch {
        let a = |t: u64| ProtoU64Antichain {
            elements: vec![i64::try_from(t).expect("fits")],
        };
        ProtoHollowBatch {
            desc: Some(mz_persist_client::v2::ProtoU64Description {
                lower: Some(a(lower)),
                upper: Some(a(upper)),
                since: Some(a(0)),
            }),
            len,
            ..Default::default()
        }
    }

    fn append(s: ShardId, from: u64, to: u64, data: bool) -> ProtoOp {
        op(
            s,
            OpKind::Append(ProtoAppend {
                batch: data.then(|| batch(from, to, 1)),
                expected: Some(f(from)),
                new: Some(f(to)),
            }),
        )
    }

    fn intent(t: u64) -> ProtoIntent {
        ProtoIntent {
            frontier: Some(f(t)),
            opaque_codec: None,
            opaque: None,
        }
    }

    fn read_intent(s: ShardId, r: &str, expected: Option<u64>, new: u64) -> ProtoOp {
        op(
            s,
            OpKind::ReadIntent(ProtoReadIntent {
                reader: r.into(),
                expected: expected.map(intent),
                new: Some(intent(new)),
            }),
        )
    }

    fn view_with(shards: &[ShardId]) -> View {
        let mut view = View::new("p0".into());
        for s in shards {
            view.commit(&[register(*s)]).expect("register");
        }
        view
    }

    #[mz_ore::test]
    fn register_writes_control_and_partition_rows() {
        let s = ShardId::new();
        let mut view = View::new("p0".into());
        let diffs = view.commit(&[register(s)]).expect("register");
        let manifests: Vec<_> = diffs.iter().map(|(k, _, _)| k.manifest()).collect();
        assert_eq!(
            manifests,
            vec![
                Manifest::Control,
                Manifest::Control,
                Manifest::Partition,
                Manifest::Partition
            ]
        );
        // A new data shard is readable from the minimum time.
        assert_eq!(view.readable(&s), Some(Antichain::from_elem(0)));
        assert_eq!(view.upper(&s), Some(Antichain::from_elem(0)));
        assert!(matches!(
            view.commit(&[register(s)]),
            Err(CommitError::Rejected(_))
        ));
        assert!(view.invariant_violations().is_empty());
    }

    #[mz_ore::test]
    fn append_checks_expected_upper() {
        let s = ShardId::new();
        let mut view = view_with(&[s]);
        let diffs = view.commit(&[append(s, 0, 5, true)]).expect("append");
        // +Batch, -Upper(0), +Upper(5).
        assert_eq!(diffs.len(), 3);
        assert_eq!(view.batches(&s).len(), 1);
        assert_eq!(view.upper(&s), Some(Antichain::from_elem(5)));
        // A second append from the same upper loses.
        assert!(matches!(
            view.commit(&[append(s, 0, 7, true)]),
            Err(CommitError::Rejected(_))
        ));
        // An empty append only moves the upper: no batch row, so a gap.
        view.commit(&[append(s, 5, 9, false)])
            .expect("empty append");
        assert_eq!(view.batches(&s).len(), 1);
        assert_eq!(view.upper(&s), Some(Antichain::from_elem(9)));
        assert!(view.invariant_violations().is_empty());
    }

    #[mz_ore::test]
    fn multi_shard_commit_is_all_or_nothing() {
        let (a, b) = (ShardId::new(), ShardId::new());
        let mut view = view_with(&[a, b]);
        view.commit(&[append(b, 0, 3, false)]).expect("advance b");
        let rows_before = view.rows.clone();
        // a's append is fine, b's expected upper is stale: neither applies.
        let res = view.commit(&[append(a, 0, 10, true), append(b, 0, 10, true)]);
        assert!(matches!(res, Err(CommitError::Rejected(_))));
        assert_eq!(view.rows, rows_before);
        // Both right: both apply.
        view.commit(&[append(a, 0, 10, true), append(b, 3, 10, true)])
            .expect("commit");
        assert_eq!(view.upper(&a), view.upper(&b));
    }

    #[mz_ore::test]
    fn intents_and_sentinel() {
        let s = ShardId::new();
        let mut view = view_with(&[s]);
        view.commit(&[read_intent(s, "r1", None, 0)]).expect("r1");
        view.commit(&[read_intent(s, "r1", Some(0), 4)])
            .expect("downgrade");
        // A stale expected value loses, like a since handle's opaque mismatch.
        assert!(matches!(
            view.commit(&[read_intent(s, "r1", Some(0), 5)]),
            Err(CommitError::Rejected(_))
        ));
        // A new intent before what's readable loses.
        assert!(matches!(
            view.commit(&[read_intent(s, "r2", None, 2)]),
            Err(CommitError::Rejected(_))
        ));
        view.commit(&[read_intent(s, "r2", None, 4)]).expect("r2");
        assert_eq!(view.readable(&s), Some(Antichain::from_elem(4)));

        let drop = |r: &str| {
            op(
                s,
                OpKind::DropIntent(ProtoDropIntent {
                    reader: r.into(),
                    expected: Some(intent(4)),
                }),
            )
        };
        view.commit(&[drop("r1")]).expect("drop r1");
        assert!(!view.intents(&s).contains_key(SENTINEL_READER));
        view.commit(&[drop("r2")]).expect("drop r2");
        // The last drop leaves a sentinel at its frontier, which the next intent
        // replaces.
        assert_eq!(
            view.intents(&s).keys().collect::<Vec<_>>(),
            vec![SENTINEL_READER]
        );
        assert_eq!(view.readable(&s), Some(Antichain::from_elem(4)));
        view.commit(&[read_intent(s, "r3", None, 6)]).expect("r3");
        assert_eq!(view.intents(&s).keys().collect::<Vec<_>>(), vec!["r3"]);
    }

    #[mz_ore::test]
    fn compaction_replaces_batches() {
        let s = ShardId::new();
        let mut view = view_with(&[s]);
        view.commit(&[append(s, 0, 2, true)]).expect("append");
        view.commit(&[append(s, 2, 4, true)]).expect("append");
        let compact = |remove, add| op(s, OpKind::Compact(ProtoCompact { remove, add }));
        view.commit(&[compact(
            vec![batch(0, 2, 1), batch(2, 4, 1)],
            vec![batch(0, 4, 2)],
        )])
        .expect("compact");
        assert_eq!(view.batches(&s).len(), 1);
        // A compaction of batches that are already gone loses.
        assert!(matches!(
            view.commit(&[compact(vec![batch(0, 2, 1)], vec![batch(0, 4, 2)])]),
            Err(CommitError::Rejected(_))
        ));
        assert!(view.invariant_violations().is_empty());
    }

    #[mz_ore::test]
    fn finalize_keeps_a_tombstone() {
        let s = ShardId::new();
        let mut view = view_with(&[s]);
        view.commit(&[append(s, 0, 2, true), read_intent(s, "r1", None, 0)])
            .expect("append");
        let finalize = op(s, OpKind::Finalize(ProtoEmpty {}));
        // A reader's intent blocks it.
        assert!(matches!(
            view.commit(std::slice::from_ref(&finalize)),
            Err(CommitError::Rejected(_))
        ));
        let drop = op(
            s,
            OpKind::DropIntent(ProtoDropIntent {
                reader: "r1".into(),
                expected: Some(intent(0)),
            }),
        );
        // Only the sentinel left: it goes.
        view.commit(&[drop, finalize]).expect("finalize");
        assert!(view.shard_meta(&s).expect("still registered").finalized);
        assert!(view.batches(&s).is_empty());
        assert!(view.intents(&s).is_empty());
        assert!(view.upper(&s).is_none());
        assert!(view.invariant_violations().is_empty());
    }

    #[mz_ore::test]
    fn view_rows_round_trip_through_encoding() {
        let s = ShardId::new();
        let mut view = view_with(&[s]);
        view.commit(&[append(s, 0, 2, true), read_intent(s, "r1", None, 0)])
            .expect("commit");
        let mut rebuilt = View::new("p0".into());
        for (k, v) in view.rows.iter() {
            let k = ManifestKey::decode(&k.encode()).expect("key");
            let v = mz_persist_client::v2::decode_val(&mz_persist_client::v2::encode_val(v))
                .expect("val");
            rebuilt.apply(k, v, 1);
        }
        assert_eq!(rebuilt.rows, view.rows);
        assert_eq!(rebuilt.shard_view(&s), view.shard_view(&s));
    }
}
