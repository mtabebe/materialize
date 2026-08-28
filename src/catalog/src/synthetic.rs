// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared pieces of the synthetic catalog state toolkit, which populates a catalog
//! with fake objects, history, and statistics so a situation can be modelled without
//! building it for real.
//!
//! Everything the toolkit injects is owned by the [`MZ_SYNTHETIC_ROLE_ID`] role. That
//! owner is the durable marker: it rides along in the durable `owner_id`, survives
//! renames, and is what listing and purging filter on.
//!
//! Injection writes fake state into the real catalog, so it must never run against an
//! environment anyone cares about. Two independent gates protect it: unsafe mode
//! (`SystemVars::allow_unsafe`), which says "this is a debug build", and
//! [`require_disposable_env`], which says "this specific environment is throwaway".
//! Both are required.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use mz_controller_types::ClusterId;
use mz_ore::collections::HashSet;
use mz_repr::CatalogItemId;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::ObjectType;
use mz_sql::names::SchemaId;
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYNTHETIC_ROLE_ID;
use mz_sql::session::vars::{ENABLE_SYNTHETIC_CATALOG_STATE, SystemVars, VarInput};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{Ident, UnresolvedItemName};

use crate::durable::Transaction;
use crate::memory::objects::CatalogEntry;

/// How much of a real object's machinery a synthetic object pays for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectsTier {
    /// Catalog metadata and nothing else: no storage collection is registered and no
    /// dataflow is planned or shipped, at injection time or on any later boot. Models
    /// the cost of catalog size alone.
    MetadataOnly,
    /// A real, empty storage collection and a real dataflow over empty inputs. Models
    /// per-dataflow bootstrap cost, and needs running clusters and replicas.
    ShippedOverEmpty,
}

/// Whether an object was injected by the toolkit, and so is safe to purge.
pub fn is_synthetic(owner_id: RoleId) -> bool {
    owner_id == MZ_SYNTHETIC_ROLE_ID
}

/// Errors unless this environment has been declared disposable.
pub fn require_disposable_env(vars: &SystemVars) -> Result<(), anyhow::Error> {
    if !vars.enable_synthetic_catalog_state() {
        return Err(not_disposable());
    }
    Ok(())
}

/// Errors unless this environment has been declared disposable, reading the setting
/// out of the durable catalog.
///
/// The offline path has no [`SystemVars`] to consult: the environment it is about to
/// write to is down.
pub fn require_disposable_env_durable(tx: &Transaction) -> Result<(), anyhow::Error> {
    // Reuse the variable's own parser rather than reading the stored string, so this
    // agrees with `ALTER SYSTEM SET` on what counts as set.
    let name = ENABLE_SYNTHETIC_CATALOG_STATE.flag.name.as_str();
    let mut vars = SystemVars::new();
    if let Some(config) = tx.get_system_configurations().find(|c| c.name == name) {
        vars.set(name, VarInput::Flat(&config.value))?;
    }
    require_disposable_env(&vars)
}

fn not_disposable() -> anyhow::Error {
    anyhow!(
        "synthetic catalog state is only available in disposable environments; set the {} \
         system variable to confirm this environment's catalog can be destroyed",
        ENABLE_SYNTHETIC_CATALOG_STATE.flag.name.as_str()
    )
}

/// Whether the boot path must treat `entry` as catalog metadata and nothing else.
///
/// The three bootstrap loops that give an item real effects (storage registration,
/// dataflow planning, dataflow instantiation) each skip such an entry. Every synthetic
/// object is [`EffectsTier::MetadataOnly`] today; a tier that ships real effects will
/// need a way to tell the two apart in the durable row.
pub fn is_metadata_only(entry: &CatalogEntry) -> bool {
    is_synthetic(*entry.owner_id())
}

/// The self-contained shapes the generator can synthesize.
///
/// Each one plans without resolving a name, so a generated object cannot brick a boot
/// by referencing something that is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntheticItemKind {
    Table,
    View,
    MaterializedView,
}

impl SyntheticItemKind {
    fn object_type(&self) -> ObjectType {
        match self {
            SyntheticItemKind::Table => ObjectType::Table,
            SyntheticItemKind::View => ObjectType::View,
            SyntheticItemKind::MaterializedView => ObjectType::MaterializedView,
        }
    }
}

impl fmt::Display for SyntheticItemKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SyntheticItemKind::Table => "table",
            SyntheticItemKind::View => "view",
            SyntheticItemKind::MaterializedView => "materialized-view",
        };
        f.write_str(s)
    }
}

impl FromStr for SyntheticItemKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "table" => Ok(SyntheticItemKind::Table),
            "view" => Ok(SyntheticItemKind::View),
            "materialized-view" => Ok(SyntheticItemKind::MaterializedView),
            other => bail!("unknown synthetic item type {other}"),
        }
    }
}

/// A batch of synthetic objects to write into the durable catalog.
#[derive(Debug, Clone)]
pub struct GenerateSpec {
    pub kind: SyntheticItemKind,
    pub count: u64,
    /// The schema the objects are inserted into. `database_name` and `schema_name` must
    /// name this same schema: they are what the rendered `create_sql` says, and the two
    /// disagreeing leaves the catalog with an item whose SQL names a different schema.
    pub schema_id: SchemaId,
    pub database_name: String,
    pub schema_name: String,
    /// Prefixes every generated object name. The allocated id is appended, so a second
    /// batch with the same prefix cannot collide with the first.
    pub name_prefix: String,
    pub columns: usize,
    /// Required for [`SyntheticItemKind::MaterializedView`], ignored otherwise.
    pub cluster_id: Option<ClusterId>,
}

/// Writes `spec.count` synthetic Tier 0 objects into `tx`, owned by `mz_synthetic`.
///
/// The objects only appear once the environment next boots and re-plans the durable
/// rows, so the environment must be down. Ids come from the shared user-item allocator
/// rather than being picked by hand: the allocator never scans for what is in use, it
/// trusts its stored `next_id`, so an id written around it gets handed out again to a
/// later real `CREATE`.
pub fn generate_objects(
    tx: &mut Transaction,
    spec: &GenerateSpec,
) -> Result<Vec<CatalogItemId>, anyhow::Error> {
    if spec.columns == 0 {
        bail!("a synthetic object needs at least one column");
    }
    if spec.kind == SyntheticItemKind::MaterializedView && spec.cluster_id.is_none() {
        bail!("a synthetic materialized view needs a cluster");
    }

    let privileges = vec![rbac::owner_privilege(
        spec.kind.object_type(),
        MZ_SYNTHETIC_ROLE_ID,
    )];
    let temporary_oids = HashSet::new();

    let ids = tx.allocate_user_item_ids(spec.count)?;
    let mut item_ids = Vec::with_capacity(ids.len());
    for (item_id, global_id) in ids {
        let name = format!("{}_{}", spec.name_prefix, item_id);
        tx.insert_user_item(
            item_id,
            global_id,
            spec.schema_id,
            &name,
            render_create_sql(spec, &name)?,
            MZ_SYNTHETIC_ROLE_ID,
            privileges.clone(),
            &temporary_oids,
            BTreeMap::new(),
            None,
        )?;
        item_ids.push(item_id);
    }
    Ok(item_ids)
}

/// The literal, type pairs generated columns cycle through.
const COLUMN_TYPES: &[(&str, &str)] = &[
    ("integer", "0"),
    ("text", "''"),
    ("boolean", "false"),
    ("double precision", "0"),
];

fn render_create_sql(spec: &GenerateSpec, item: &str) -> Result<String, anyhow::Error> {
    let name = UnresolvedItemName(vec![
        Ident::new(&spec.database_name)?,
        Ident::new(&spec.schema_name)?,
        Ident::new(item)?,
    ])
    .to_ast_string_stable();

    let column = |i: usize| COLUMN_TYPES[i % COLUMN_TYPES.len()];
    // Every literal is cast, so the select list has no unknown-typed column for the
    // planner to reject.
    let select_list = || {
        (0..spec.columns)
            .map(|i| {
                let (ty, lit) = column(i);
                format!("CAST({lit} AS {ty}) AS c{i}")
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    Ok(match spec.kind {
        SyntheticItemKind::Table => {
            let defs = (0..spec.columns)
                .map(|i| format!("c{i} {}", column(i).0))
                .collect::<Vec<_>>()
                .join(", ");
            format!("CREATE TABLE {name} ({defs})")
        }
        SyntheticItemKind::View => format!("CREATE VIEW {name} AS SELECT {}", select_list()),
        SyntheticItemKind::MaterializedView => {
            let cluster = spec
                .cluster_id
                .ok_or_else(|| anyhow!("a synthetic materialized view needs a cluster"))?;
            format!(
                "CREATE MATERIALIZED VIEW {name} IN CLUSTER [{cluster}] AS SELECT {}",
                select_list()
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use mz_sql::session::vars::VarInput;

    use super::*;

    #[mz_ore::test]
    fn test_disposable_env_gate() {
        let mut vars = SystemVars::new();
        assert!(require_disposable_env(&vars).is_err());

        vars.set("enable_synthetic_catalog_state", VarInput::Flat("on"))
            .expect("flag exists and takes a boolean");
        assert!(require_disposable_env(&vars).is_ok());
    }
}
