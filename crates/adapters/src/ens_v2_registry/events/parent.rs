use anyhow::Result;
use serde_json::json;

use super::RegistryObservationContext;
use super::topology::refresh_registry_suffixes;
use crate::ens_v2_registry::{
    constants::{EVENT_KIND_PARENT_CHANGED, ZERO_ADDRESS},
    normalized::normalized_event,
    types::{CurrentParentClaim, ObservationRef},
    util::null_if_zero_address,
};

pub(super) fn apply_parent_updated(
    parent: String,
    label: String,
    sender: String,
    reference: ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    let registry_address = reference.emitting_address.clone();
    let previous_claim = context
        .current_parent_claim_by_registry
        .get(&registry_address)
        .cloned();
    let previous_registry_name = (!context.root_registry_addresses.contains(&registry_address))
        .then(|| {
            context
                .registry_suffix_by_address
                .get(&registry_address)
                .cloned()
        })
        .flatten();

    // `setParent` is child-initiated and replaces both parts of the child's
    // current claim. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L171 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L175 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L176 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L177 @ ens_v2@ccaeb58)
    if parent == ZERO_ADDRESS || label.is_empty() || label.contains('.') {
        context
            .current_parent_claim_by_registry
            .remove(&registry_address);
    } else {
        context.current_parent_claim_by_registry.insert(
            registry_address.clone(),
            CurrentParentClaim {
                parent: parent.clone(),
                label: label.clone(),
            },
        );
    }
    // Canonical lookup accepts a child-side claim only when the claimed
    // parent's CURRENT `getSubregistry(label)` pointer leads back to that child.
    // (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L82 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L86 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L87 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L88 @ ens_v2@ccaeb58)
    // `getSubregistry` returns zero at and after the label's expiry. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L251 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L253 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L625 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L626 @ ens_v2@ccaeb58)
    refresh_registry_suffixes(
        &reference,
        "ParentUpdated",
        Some(&registry_address),
        context,
    )?;
    let registry_name = (!context.root_registry_addresses.contains(&registry_address))
        .then(|| {
            context
                .registry_suffix_by_address
                .get(&registry_address)
                .cloned()
        })
        .flatten();
    context.graph_events.push(normalized_event(
        &reference,
        None,
        None,
        EVENT_KIND_PARENT_CHANGED,
        json!({
            "parent": previous_claim.as_ref().map(|claim| claim.parent.as_str()),
            "label": previous_claim.as_ref().map(|claim| claim.label.as_str()),
            "registry_name": previous_registry_name,
        }),
        json!({
            "source_event": "ParentUpdated",
            "parent": null_if_zero_address(&parent),
            "label": label,
            "registry_name": registry_name,
            "sender": sender,
            "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
            "parent_contract_instance_id": context.registry_contract_by_address
                .get(&parent)
                .map(ToString::to_string),
        }),
        format!("parent-updated:{}", reference.emitting_address),
    ));
    Ok(())
}
