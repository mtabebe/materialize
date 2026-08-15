// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v91 as v91;
use crate::durable::upgrade::objects_v92 as v92;

/// Adds the `BranchDescriptor` collection.
///
/// Purely additive: no existing record's encoding changes, so nothing is
/// rewritten and the migration produces no actions.
pub fn upgrade(
    _snapshot: Vec<v91::StateUpdateKind>,
) -> Vec<MigrationAction<v91::StateUpdateKind, v92::StateUpdateKind>> {
    Vec::new()
}
