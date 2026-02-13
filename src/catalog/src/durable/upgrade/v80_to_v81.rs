// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v80 as v80;
use crate::durable::upgrade::objects_v81 as v81;

/// Migration from v80 to v81: adds `sealed` field to ClusterConfig.
///
/// The `sealed` field is a new boolean field that defaults to `false`.
/// All existing clusters will have `sealed = false` after this migration.
pub fn upgrade(
    snapshot: Vec<v80::StateUpdateKind>,
) -> Vec<MigrationAction<v80::StateUpdateKind, v81::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|old| {
            let new: v81::StateUpdateKind = match old.clone() {
                v80::StateUpdateKind::Cluster(cluster) => {
                    v81::StateUpdateKind::Cluster(v81::Cluster {
                        key: v81::ClusterKey {
                            id: upgrade_cluster_id(cluster.key.id),
                        },
                        value: v81::ClusterValue {
                            name: cluster.value.name,
                            owner_id: upgrade_role_id(cluster.value.owner_id),
                            privileges: cluster
                                .value
                                .privileges
                                .into_iter()
                                .map(upgrade_mz_acl_item)
                                .collect(),
                            config: v81::ClusterConfig {
                                workload_class: cluster.value.config.workload_class,
                                variant: upgrade_cluster_variant(cluster.value.config.variant),
                                // New field: default to false (unsealed)
                                sealed: false,
                            },
                        },
                    })
                }
                // All other types are JSON-compatible between v80 and v81
                _ => return None,
            };
            Some(MigrationAction::Update(old, new))
        })
        .collect()
}

fn upgrade_cluster_id(id: v80::ClusterId) -> v81::ClusterId {
    match id {
        v80::ClusterId::System(id) => v81::ClusterId::System(id),
        v80::ClusterId::User(id) => v81::ClusterId::User(id),
    }
}

fn upgrade_role_id(id: v80::RoleId) -> v81::RoleId {
    match id {
        v80::RoleId::System(id) => v81::RoleId::System(id),
        v80::RoleId::User(id) => v81::RoleId::User(id),
        v80::RoleId::Public => v81::RoleId::Public,
        v80::RoleId::Predefined(id) => v81::RoleId::Predefined(id),
    }
}

fn upgrade_mz_acl_item(item: v80::MzAclItem) -> v81::MzAclItem {
    v81::MzAclItem {
        grantee: upgrade_role_id(item.grantee),
        grantor: upgrade_role_id(item.grantor),
        acl_mode: v81::AclMode {
            bitflags: item.acl_mode.bitflags,
        },
    }
}

fn upgrade_cluster_variant(variant: v80::ClusterVariant) -> v81::ClusterVariant {
    match variant {
        v80::ClusterVariant::Unmanaged => v81::ClusterVariant::Unmanaged,
        v80::ClusterVariant::Managed(m) => v81::ClusterVariant::Managed(v81::ManagedCluster {
            size: m.size,
            replication_factor: m.replication_factor,
            availability_zones: m.availability_zones,
            logging: v81::ReplicaLogging {
                log_logging: m.logging.log_logging,
                interval: m.logging.interval.map(|d| v81::Duration {
                    secs: d.secs,
                    nanos: d.nanos,
                }),
            },
            optimizer_feature_overrides: m
                .optimizer_feature_overrides
                .into_iter()
                .map(|o| v81::OptimizerFeatureOverride {
                    name: o.name,
                    value: o.value,
                })
                .collect(),
            schedule: match m.schedule {
                v80::ClusterSchedule::Manual => v81::ClusterSchedule::Manual,
                v80::ClusterSchedule::Refresh(r) => {
                    v81::ClusterSchedule::Refresh(v81::ClusterScheduleRefreshOptions {
                        rehydration_time_estimate: v81::Duration {
                            secs: r.rehydration_time_estimate.secs,
                            nanos: r.rehydration_time_estimate.nanos,
                        },
                    })
                }
            },
        }),
    }
}
