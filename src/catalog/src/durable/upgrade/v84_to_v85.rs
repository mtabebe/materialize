// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! No-op migration from v84 to v85.
//!
//! v85 adds the `BranchDescriptor` collection, which starts empty; no existing
//! rows need to be changed.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v84 as v84;
use crate::durable::upgrade::objects_v85 as v85;

pub fn upgrade(
    snapshot: Vec<v84::StateUpdateKind>,
) -> Vec<MigrationAction<v84::StateUpdateKind, v85::StateUpdateKind>> {
    let _ = snapshot;
    Vec::new()
}
