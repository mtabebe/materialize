// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The key encoding a checkpoint shard is written in.
//!
//! A checkpoint is stored keyed by the arrangement's key, so restoring it is a
//! merge of sorted runs into a `Builder` rather than a sort. That property is
//! the whole reason for a bespoke shard: the unkeyed shape `persist_source`
//! reads for free would restore no more cheaply than arranging the input does.
//!
//! It only holds if the shard's order is the arrangement's order, and by
//! default it is not. Materialize orders rows **by length first**, then
//! lexicographically ([`mz_repr::RowRef`]'s `Ord`, which is also how an
//! arrangement's cursor walks its keys). Persist orders a byte-blob key
//! lexicographically with no length term. On rows of unequal length the two
//! disagree, so a checkpoint read back from persist would arrive out of
//! arrangement order and restore would pay the sort it exists to avoid.
//!
//! Prefixing the length restores the agreement exactly, since comparing a
//! big-endian length before the bytes *is* Materialize's comparison. Encoding
//! the key this way is what lets the shard be read back as a sorted run.

use mz_ore::cast::CastFrom;
use mz_repr::{Row, RowRef};

/// The length prefix, wide enough for any row and fixed so that comparing two
/// prefixes compares two lengths.
const LEN: usize = size_of::<u64>();

/// Encodes a row as a shard key whose lexicographic order is the arrangement's.
pub fn encode(row: &RowRef) -> Vec<u8> {
    let data = row.data();
    let mut encoded = Vec::with_capacity(LEN + data.len());
    encoded.extend_from_slice(&u64::cast_from(data.len()).to_be_bytes());
    encoded.extend_from_slice(data);
    encoded
}

/// Decodes a row encoded by [`encode`].
///
/// # Safety
///
/// The caller must have written `encoded` with [`encode`], from a valid row.
/// Row data is not self-validating, so this inherits
/// [`Row::from_bytes_unchecked`]'s contract.
pub unsafe fn decode(encoded: &[u8]) -> Row {
    let data = &encoded[LEN..];
    // SAFETY: the caller guarantees these bytes came from a row.
    unsafe { Row::from_bytes_unchecked(data) }
}

#[cfg(test)]
mod tests {
    use mz_repr::Datum;

    use super::*;

    /// The rows are chosen so that plain lexicographic order and Materialize's
    /// disagree: a one-datum integer packs shorter than a two-datum row, but
    /// can lead with a larger tag byte.
    fn rows() -> Vec<Row> {
        vec![
            Row::default(),
            Row::pack_slice(&[Datum::Int64(1)]),
            Row::pack_slice(&[Datum::Int64(-1)]),
            Row::pack_slice(&[Datum::String("a")]),
            Row::pack_slice(&[Datum::String("zzzzzzzzzzzz")]),
            Row::pack_slice(&[Datum::Null]),
            Row::pack_slice(&[Datum::Int64(1), Datum::Int64(2)]),
            Row::pack_slice(&[Datum::String("a"), Datum::String("b")]),
        ]
    }

    #[mz_ore::test]
    fn the_encoding_orders_rows_the_way_an_arrangement_does() {
        for left in rows() {
            for right in rows() {
                assert_eq!(
                    encode(&left).cmp(&encode(&right)),
                    left.cmp(&right),
                    "{left:?} vs {right:?}"
                );
            }
        }
    }

    /// Without the length prefix the orders come apart, which is the reason the
    /// prefix is there.
    #[mz_ore::test]
    fn the_raw_bytes_do_not() {
        let disagreements = rows()
            .iter()
            .flat_map(|left| rows().into_iter().map(move |right| (left.clone(), right)))
            .filter(|(left, right)| left.data().cmp(right.data()) != left.cmp(right))
            .count();
        assert!(disagreements > 0);
    }

    #[mz_ore::test]
    fn encoding_round_trips() {
        for row in rows() {
            // SAFETY: the bytes come from `encode` over a valid row.
            assert_eq!(unsafe { decode(&encode(&row)) }, row);
        }
    }
}
