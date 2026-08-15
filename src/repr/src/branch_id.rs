// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use anyhow::{Error, anyhow};
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

const BRANCH_CHAR: char = 'b';

/// The identifier for a branch.
///
/// Branches are always user-created, so unlike the other catalog ids there is
/// no system variant.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct BranchId(pub u64);

impl FromStr for BranchId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || anyhow!("couldn't parse branch id '{s}'");
        match s.chars().next() {
            Some(BRANCH_CHAR) if s.len() >= 2 => Ok(Self(s[1..].parse().map_err(|_| err())?)),
            _ => Err(err()),
        }
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{BRANCH_CHAR}{}", self.0)
    }
}

#[mz_ore::test]
fn test_branch_id_parsing() {
    let s = "b42";
    let branch_id: BranchId = s.parse().unwrap();
    assert_eq!(BranchId(42), branch_id);
    assert_eq!(s, branch_id.to_string());

    mz_ore::assert_err!("u23".parse::<BranchId>());
    mz_ore::assert_err!("b".parse::<BranchId>());
    mz_ore::assert_err!("".parse::<BranchId>());
}
