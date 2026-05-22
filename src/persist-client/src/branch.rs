// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Phase 0 spike: validate the persist primitives needed for Branch.
//!
//! Branch represents a forked table as `(source_shard, branch_ts, delta_shard)`:
//! - `source_shard`: original shard, read frozen at `branch_ts`
//! - `delta_shard`: initially empty, receives all local writes at ts > `branch_ts`
//!
//! Reads: `consolidate(source.snapshot(as_of=branch_ts) + delta.snapshot(as_of=T_read))`
//! Conflict detection: intersect `source.changes(branch_ts, merge_ts]` and
//!   `delta.changes(branch_ts, merge_ts]` on primary key.

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use differential_dataflow::consolidation::consolidate_updates;
    use mz_dyncfg::ConfigUpdates;
    use timely::progress::Antichain;

    use crate::critical::{CriticalReaderId, Opaque};
    use crate::tests::new_test_client;
    use crate::{Diagnostics, ShardId};

    // Helpers

    fn key_set_from_diff(diff: &[((String, String), u64, i64)]) -> BTreeSet<String> {
        diff.iter()
            .map(|((k, _v), _t, _d)| k.clone())
            .collect()
    }

    fn current_state(
        updates: &[((String, String), u64, i64)],
        as_of: u64,
    ) -> Vec<(String, String)> {
        use std::collections::BTreeMap;
        let mut counts: BTreeMap<(String, String), i64> = BTreeMap::new();
        for ((k, v), t, d) in updates {
            if *t <= as_of {
                *counts.entry((k.clone(), v.clone())).or_insert(0) += d;
            }
        }
        counts
            .into_iter()
            .filter(|(_, d)| *d > 0)
            .map(|(kv, _)| kv)
            .collect()
    }

    /// Validates two-shard read merge: write data to source shard A up to
    /// branch_ts, write diverging data to delta shard B at ts > branch_ts,
    /// and confirm the merged read is correct.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn two_shard_read_merge(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let branch_ts: u64 = 10;
        let read_ts: u64 = 20;

        // Source shard: has k1=source_v1 (ts 1) and k2=source_v2 (ts 5).
        // Source also has upstream changes after branch: k2 updated (ts 15).
        let (mut write_a, mut read_a) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // Delta shard: local writes at ts > branch_ts.
        let (mut write_b, mut read_b) = client
            .expect_open::<String, String, u64, i64>(ShardId::new())
            .await;

        // Write source data up through and past branch_ts.
        // Pre-branch: k1=source_v1, k2=source_v2.
        // Post-branch: k2 gets upstream update.
        write_a
            .expect_append(
                &[
                    (("k1".to_owned(), "source_v1".to_owned()), 1u64, 1i64),
                    (("k2".to_owned(), "source_v2".to_owned()), 5u64, 1i64),
                    // upstream change after branch_ts: k2 value changes
                    (("k2".to_owned(), "source_v2".to_owned()), 15u64, -1i64),
                    (("k2".to_owned(), "upstream_v2".to_owned()), 15u64, 1i64),
                ],
                vec![0u64],
                vec![read_ts + 1],
            )
            .await;

        // Write local (clone) data to delta shard: all at ts > branch_ts.
        // k3 is a new local insert; k1 is locally updated.
        write_b
            .expect_append(
                &[
                    (("k3".to_owned(), "local_v3".to_owned()), 12u64, 1i64),
                    (("k1".to_owned(), "source_v1".to_owned()), 12u64, -1i64),
                    (("k1".to_owned(), "local_v1_updated".to_owned()), 12u64, 1i64),
                ],
                vec![0u64],
                vec![read_ts + 1],
            )
            .await;

        // Two-shard read: source frozen at branch_ts, delta at read_ts.
        let snapshot_a = read_a
            .snapshot_and_fetch(Antichain::from_elem(branch_ts))
            .await
            .expect("source snapshot must be readable at branch_ts");

        let snapshot_b = read_b
            .snapshot_and_fetch(Antichain::from_elem(read_ts))
            .await
            .expect("delta snapshot must be readable at read_ts");

        // Merge: concatenate then consolidate.
        let mut merged = snapshot_a;
        merged.extend(snapshot_b);
        consolidate_updates(&mut merged);

        // The merged state at read_ts should be:
        //   k1 = local_v1_updated (source had source_v1, clone retracted + re-inserted)
        //   k2 = source_v2        (source shard frozen at branch_ts=10, before upstream update)
        //   k3 = local_v3         (new local insert)
        let state = current_state(&merged, read_ts);
        let mut state_sorted = state.clone();
        state_sorted.sort();

        let expected: Vec<(String, String)> = vec![
            ("k1".to_owned(), "local_v1_updated".to_owned()),
            ("k2".to_owned(), "source_v2".to_owned()),
            ("k3".to_owned(), "local_v3".to_owned()),
        ];

        assert_eq!(state_sorted, expected, "merged clone state must be correct");

        // Upstream change to k2 (at ts 15) must NOT be visible in the clone,
        // because the source is frozen at branch_ts=10.
        assert!(
            !state_sorted
                .iter()
                .any(|(k, v)| k == "k2" && v == "upstream_v2"),
            "clone must not see upstream changes after branch_ts"
        );
    }

    /// Validates conflict detection: read changes from both shards over
    /// (branch_ts, merge_ts], intersect on primary key, check all conflict
    /// cases from the design doc.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn conflict_detection(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let branch_ts: u64 = 10;
        let merge_ts: u64 = 30;

        let shard_a = ShardId::new();
        let shard_b = ShardId::new();

        // Write source (upstream) changes after branch_ts.
        // k1: upstream update — will conflict with local update to k1.
        // k4: upstream insert — no conflict (not in local diff).
        // k5: upstream delete — will conflict with local insert of k5.
        {
            let (mut write_a, _) = client
                .expect_open::<String, String, u64, i64>(shard_a)
                .await;
            write_a
                .expect_append(
                    &[
                        // pre-branch data
                        (("k1".to_owned(), "v1_orig".to_owned()), 1u64, 1i64),
                        (("k5".to_owned(), "v5_orig".to_owned()), 1u64, 1i64),
                        // upstream changes after branch
                        (("k1".to_owned(), "v1_orig".to_owned()), 15u64, -1i64),
                        (("k1".to_owned(), "v1_upstream".to_owned()), 15u64, 1i64),
                        (("k4".to_owned(), "v4_upstream".to_owned()), 15u64, 1i64),
                        (("k5".to_owned(), "v5_orig".to_owned()), 15u64, -1i64),
                    ],
                    vec![0u64],
                    vec![merge_ts + 1],
                )
                .await;
        }

        // Write local (clone) changes to delta shard after branch_ts.
        // k1: local update — CONFLICT
        // k2: local insert, no upstream activity — NO conflict
        // k3: local insert, no upstream activity — NO conflict
        // k5: local insert of a key deleted upstream — CONFLICT
        {
            let (mut write_b, _) = client
                .expect_open::<String, String, u64, i64>(shard_b)
                .await;
            write_b
                .expect_append(
                    &[
                        (("k1".to_owned(), "v1_orig".to_owned()), 12u64, -1i64),
                        (("k1".to_owned(), "v1_local".to_owned()), 12u64, 1i64),
                        (("k2".to_owned(), "v2_local".to_owned()), 12u64, 1i64),
                        (("k3".to_owned(), "v3_local".to_owned()), 12u64, 1i64),
                        (("k5".to_owned(), "v5_local".to_owned()), 12u64, 1i64),
                    ],
                    vec![0u64],
                    vec![merge_ts + 1],
                )
                .await;
        }

        // Read upstream diff: source.changes(branch_ts, merge_ts].
        let upstream_diff = {
            let (_, read_a) = client
                .expect_open::<String, String, u64, i64>(shard_a)
                .await;
            // listen(as_of=branch_ts) gives events at ts > branch_ts.
            let mut listen = read_a
                .listen(Antichain::from_elem(branch_ts))
                .await
                .expect("listen must succeed");
            let (changes, _) = listen.read_until(&merge_ts).await;
            changes
        };

        // Read local diff: delta.changes(branch_ts, merge_ts].
        let local_diff = {
            let (_, read_b) = client
                .expect_open::<String, String, u64, i64>(shard_b)
                .await;
            let mut listen = read_b
                .listen(Antichain::from_elem(branch_ts))
                .await
                .expect("listen must succeed");
            let (changes, _) = listen.read_until(&merge_ts).await;
            changes
        };

        // Conflict detection: intersect on primary key.
        let upstream_keys = key_set_from_diff(&upstream_diff);
        let local_keys = key_set_from_diff(&local_diff);
        let conflicts: BTreeSet<String> = upstream_keys
            .intersection(&local_keys)
            .cloned()
            .collect();

        // k1 and k5 both changed in both local and upstream → conflict.
        // k2, k3 only changed locally → no conflict.
        // k4 only changed upstream → no conflict.
        assert!(conflicts.contains("k1"), "k1 must conflict (both updated)");
        assert!(
            conflicts.contains("k5"),
            "k5 must conflict (deleted upstream, inserted locally)"
        );
        assert!(!conflicts.contains("k2"), "k2 must not conflict (local only)");
        assert!(!conflicts.contains("k3"), "k3 must not conflict (local only)");
        assert!(!conflicts.contains("k4"), "k4 must not conflict (upstream only)");
        assert_eq!(conflicts.len(), 2, "exactly 2 conflicts expected");
    }

    /// Validates that a critical read hold placed at branch_ts prevents the
    /// since frontier from advancing past branch_ts while the hold is active.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn read_hold_prevents_compaction(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let branch_ts: u64 = 50;

        let shard_id = ShardId::new();

        // Write some data to the shard.
        {
            let (mut write, _) = client
                .expect_open::<String, String, u64, i64>(shard_id)
                .await;
            write
                .expect_append(
                    &[(("k1".to_owned(), "v1".to_owned()), 1u64, 1i64)],
                    vec![0u64],
                    vec![100u64],
                )
                .await;
        }

        // Place a critical read hold at branch_ts.
        let hold_reader_id = CriticalReaderId::new();
        let initial_opaque = Opaque::encode(&0u64);
        let hold_opaque = Opaque::encode(&1u64);

        let mut hold = client
            .open_critical_since::<String, String, u64, i64>(
                shard_id,
                hold_reader_id.clone(),
                initial_opaque.clone(),
                Diagnostics::for_tests(),
            )
            .await
            .expect("codec mismatch");

        // Advance the hold's since to branch_ts (this is the "place hold" operation).
        hold.compare_and_downgrade_since(
            &initial_opaque,
            (&hold_opaque, &Antichain::from_elem(branch_ts)),
        )
        .await
        .expect("opaque token must match (compare_and_downgrade_since)");

        assert_eq!(
            hold.since(),
            &Antichain::from_elem(branch_ts),
            "hold since must be at branch_ts"
        );

        // With the hold in place, we can still read a snapshot at branch_ts.
        let (_, mut read) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        let snapshot = read
            .snapshot_and_fetch(Antichain::from_elem(branch_ts))
            .await;
        assert!(
            snapshot.is_ok(),
            "snapshot at branch_ts must succeed while hold is active: {:?}",
            snapshot.err()
        );

        // Attempting to read BEFORE branch_ts should fail because the hold
        // has advanced the since to branch_ts (data before that can be GC'd).
        // Note: in a test environment without forced compaction this may still
        // succeed; the important invariant is that the SinceHandle holds the
        // since frontier, not that GC has actually fired.
        let snapshot_before = read
            .snapshot_and_fetch(Antichain::from_elem(branch_ts - 1))
            .await;

        // The hold is at branch_ts, so reads at branch_ts - 1 may fail.
        // This is informational in tests where GC hasn't fired.
        let _ = snapshot_before; // either outcome is acceptable for the spike

        // Re-open the critical since handle (simulating a coordinator restart)
        // to verify the hold persists across handle re-creation.
        let hold2 = client
            .open_critical_since::<String, String, u64, i64>(
                shard_id,
                hold_reader_id,
                hold_opaque.clone(),
                Diagnostics::for_tests(),
            )
            .await
            .expect("codec mismatch");

        assert_eq!(
            hold2.since(),
            &Antichain::from_elem(branch_ts),
            "hold since must survive handle re-creation (coordinator restart)"
        );
    }

    /// Validates non-conflicting (clean) cases: local inserts for new keys
    /// and upstream inserts for different keys do not produce conflicts.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn clean_merge_no_conflicts(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let branch_ts: u64 = 10;
        let merge_ts: u64 = 20;

        let shard_a = ShardId::new();
        let shard_b = ShardId::new();

        // Upstream: only adds new key k_upstream, doesn't touch k_local.
        {
            let (mut write_a, _) = client
                .expect_open::<String, String, u64, i64>(shard_a)
                .await;
            write_a
                .expect_append(
                    &[(
                        ("k_upstream".to_owned(), "v_upstream".to_owned()),
                        15u64,
                        1i64,
                    )],
                    vec![0u64],
                    vec![merge_ts + 1],
                )
                .await;
        }

        // Local: only adds new key k_local, doesn't touch k_upstream.
        {
            let (mut write_b, _) = client
                .expect_open::<String, String, u64, i64>(shard_b)
                .await;
            write_b
                .expect_append(
                    &[(("k_local".to_owned(), "v_local".to_owned()), 12u64, 1i64)],
                    vec![0u64],
                    vec![merge_ts + 1],
                )
                .await;
        }

        let upstream_diff = {
            let (_, read_a) = client
                .expect_open::<String, String, u64, i64>(shard_a)
                .await;
            let mut listen = read_a
                .listen(Antichain::from_elem(branch_ts))
                .await
                .expect("listen must succeed");
            let (changes, _) = listen.read_until(&merge_ts).await;
            changes
        };

        let local_diff = {
            let (_, read_b) = client
                .expect_open::<String, String, u64, i64>(shard_b)
                .await;
            let mut listen = read_b
                .listen(Antichain::from_elem(branch_ts))
                .await
                .expect("listen must succeed");
            let (changes, _) = listen.read_until(&merge_ts).await;
            changes
        };

        let upstream_keys = key_set_from_diff(&upstream_diff);
        let local_keys = key_set_from_diff(&local_diff);
        let conflicts: BTreeSet<String> = upstream_keys
            .intersection(&local_keys)
            .cloned()
            .collect();

        assert!(
            conflicts.is_empty(),
            "non-overlapping changes must produce no conflicts: {:?}",
            conflicts
        );
    }
}
