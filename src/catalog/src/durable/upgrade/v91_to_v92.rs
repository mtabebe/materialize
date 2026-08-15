// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v91 as v91;
use crate::durable::upgrade::objects_v92 as v92;

crate::json_compatible!(v91::ItemKey with v92::ItemKey);
crate::json_compatible!(v91::SchemaId with v92::SchemaId);
crate::json_compatible!(v91::RoleId with v92::RoleId);
crate::json_compatible!(v91::MzAclItem with v92::MzAclItem);
crate::json_compatible!(v91::CatalogItem with v92::CatalogItem);
crate::json_compatible!(v91::GlobalId with v92::GlobalId);
crate::json_compatible!(v91::ItemVersion with v92::ItemVersion);

/// Adds the `BranchDescriptor` collection, and the `branch_id` field on items
/// that distinguishes a branch's substitute for an object from the object
/// itself.
///
/// The new collection is additive, so no existing record moves for it. `Item`
/// records gained a field, so their stored JSON is no longer readable as the
/// v92 type and every such record is rewritten.
///
/// NOTE: the explicit rewrite matters even though serde would default the
/// missing field to `None` on read. A later edit to an item retracts the record
/// by writing its v92 encoding (with `branch_id: None`) at diff -1. Without the
/// backfill, the stored record lacks the field, so the retraction doesn't match
/// it and the collection is left with negative multiplicity.
pub fn upgrade(
    snapshot: Vec<v91::StateUpdateKind>,
) -> Vec<MigrationAction<v91::StateUpdateKind, v92::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
            v91::StateUpdateKind::Item(old_item) => {
                let new_item = migrate_item(old_item.clone());
                migrations.push(MigrationAction::Update(
                    v91::StateUpdateKind::Item(old_item),
                    v92::StateUpdateKind::Item(new_item),
                ));
            }
            _ => {}
        }
    }
    migrations
}

fn migrate_item(old: v91::Item) -> v92::Item {
    let v91::Item { key, value } = old;
    v92::Item {
        key: JsonCompatible::convert(&key),
        value: v92::ItemValue {
            schema_id: JsonCompatible::convert(&value.schema_id),
            name: value.name,
            definition: JsonCompatible::convert(&value.definition),
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            oid: value.oid,
            global_id: JsonCompatible::convert(&value.global_id),
            extra_versions: value
                .extra_versions
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            ephemeral_owner_session: value.ephemeral_owner_session,
            branch_id: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::v91_to_v92::upgrade;
    use crate::durable::upgrade::{objects_v91 as v91, objects_v92 as v92};

    fn item(id: u64) -> v91::Item {
        v91::Item {
            key: v91::ItemKey {
                gid: v91::CatalogItemId::User(id),
            },
            value: v91::ItemValue {
                schema_id: v91::SchemaId::User(1),
                name: format!("item{id}"),
                definition: v91::CatalogItem::V1(v91::CatalogItemV1 {
                    create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
                }),
                owner_id: v91::RoleId::User(1),
                privileges: Vec::new(),
                oid: 20_001,
                global_id: v91::GlobalId::User(id),
                extra_versions: Vec::new(),
                ephemeral_owner_session: None,
            },
        }
    }

    #[mz_ore::test]
    fn backfills_items_as_none() {
        let migrations = upgrade(vec![v91::StateUpdateKind::Item(item(1))]);
        assert_eq!(migrations.len(), 1);

        let MigrationAction::Update(_, v92::StateUpdateKind::Item(item)) = &migrations[0] else {
            panic!("expected an item update");
        };
        assert_eq!(item.value.branch_id, None);
    }
}
