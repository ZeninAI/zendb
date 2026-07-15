//! Local planning for declarative device-hosted operators.

use zendb_types::{
    DeviceCapabilitySummary, Hlc, OperatorDesiredState, OperatorEffect, OperatorId, OperatorLease,
    OperatorSpec,
};

/// Inputs a reconciler reads at one local point in time. These are already
/// resolved CRDT control values; this planner does not do network I/O.
#[derive(Debug)]
pub struct ReconcileSnapshot {
    pub specs: Vec<OperatorSpec>,
    pub device: DeviceCapabilitySummary,
    pub leases: Vec<OperatorLease>,
    pub now: Hlc,
}

/// Local actions produced from declarative desired state. Applying a claim or
/// renewal is the workspace control-plane's responsibility.
#[derive(Debug)]
pub enum ReconcileAction {
    Start {
        spec: OperatorSpec,
        lease: Option<OperatorLease>,
    },
    Stop {
        operator_id: OperatorId,
        reason: StopReason,
    },
    ClaimLease {
        operator_id: OperatorId,
    },
    RenewLease(OperatorLease),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    StoppedBySpec,
    Ineligible,
    LeaseHeldElsewhere,
}

/// Plan a single reconciliation pass.
///
/// A local-only operator starts on every eligible device. A shared-writing
/// operator starts only while this device holds the resolved non-expired lease;
/// an eligible device requests a claim when the lease is absent or expired.
/// This is intentionally advisory under a partition. Output fencing, not this
/// decision alone, protects consumers from a stale worker.
pub fn plan_reconciliation(snapshot: &ReconcileSnapshot) -> Vec<ReconcileAction> {
    let mut actions = Vec::new();
    for spec in &snapshot.specs {
        if spec.desired_state != OperatorDesiredState::Running || !snapshot.device.supports(spec) {
            actions.push(ReconcileAction::Stop {
                operator_id: spec.operator_id.clone(),
                reason: if spec.desired_state == OperatorDesiredState::Running {
                    StopReason::Ineligible
                } else {
                    StopReason::StoppedBySpec
                },
            });
            continue;
        }

        if spec.effect == OperatorEffect::LocalOnly {
            actions.push(ReconcileAction::Start {
                spec: spec.clone(),
                lease: None,
            });
            continue;
        }

        let lease = snapshot
            .leases
            .iter()
            .find(|lease| lease.operator_id == spec.operator_id);
        match lease {
            Some(lease)
                if lease.holder_device_id == snapshot.device.device_id
                    && !lease.is_expired_at(snapshot.now) =>
            {
                actions.push(ReconcileAction::Start {
                    spec: spec.clone(),
                    lease: Some(lease.clone()),
                });
                actions.push(ReconcileAction::RenewLease(lease.clone()));
            }
            Some(lease) if lease.is_expired_at(snapshot.now) => {
                actions.push(ReconcileAction::ClaimLease {
                    operator_id: spec.operator_id.clone(),
                });
            }
            Some(_) => actions.push(ReconcileAction::Stop {
                operator_id: spec.operator_id.clone(),
                reason: StopReason::LeaseHeldElsewhere,
            }),
            None => actions.push(ReconcileAction::ClaimLease {
                operator_id: spec.operator_id.clone(),
            }),
        }
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use zendb_types::{CapabilityId, DeviceId, OperatorSource};

    fn clock(value: u64, device: DeviceId) -> Hlc {
        Hlc::with_device_id(value, 0, device).unwrap()
    }

    #[test]
    fn shared_operator_starts_only_for_the_current_lease_holder() {
        let device = DeviceId::from_bytes([1; 16]);
        let spec = OperatorSpec {
            operator_id: "index".into(),
            name: "index".into(),
            desired_state: OperatorDesiredState::Running,
            source: OperatorSource::Native {
                type_name: "index".into(),
                api_version: 1,
            },
            config: vec![],
            inputs: vec![],
            required_capabilities: BTreeSet::from([CapabilityId::from("search")]),
            effect: OperatorEffect::WriteShared,
            output_table: Some("derived/index".into()),
        };
        let snapshot = ReconcileSnapshot {
            specs: vec![spec],
            device: DeviceCapabilitySummary {
                device_id: device,
                capabilities: BTreeSet::from([CapabilityId::from("search")]),
                can_contribute: true,
            },
            leases: vec![OperatorLease {
                operator_id: "index".into(),
                holder_device_id: device,
                fence: clock(1, device),
                expires_at: clock(10, device),
            }],
            now: clock(5, device),
        };
        let actions = plan_reconciliation(&snapshot);
        assert!(matches!(
            actions.first(),
            Some(ReconcileAction::Start { .. })
        ));
    }
}
