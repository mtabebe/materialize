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

use anyhow::bail;
use mz_repr::role_id::RoleId;
use mz_sql::session::user::MZ_SYNTHETIC_ROLE_ID;
use mz_sql::session::vars::SystemVars;

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
        bail!(
            "synthetic catalog state is only available in disposable environments; \
             set the enable_synthetic_catalog_state system variable to confirm this \
             environment's catalog can be destroyed"
        );
    }
    Ok(())
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
