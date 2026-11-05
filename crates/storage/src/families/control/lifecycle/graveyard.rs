//! A name whose current ENSv1 registry record the admitted Graveyard holds: its ENSv1
//! registration has ended, whatever its lease, binding or schedule says.
//!
//! The Graveyard is the ENSv1 `.eth` controller the ENSv2 launch leaves in place. Anyone may call
//! its `clear`. For a `.eth` second-level name it registers the name to itself through the
//! BaseRegistrar, which only succeeds once the lease is past its expiry and grace, with a
//! far-future expiry that is no lease of anyone's. That takes the registry record and burns the
//! lapsed token. A subname of a name it holds is cleared by giving the Graveyard its record. No
//! ENSv1 controller can register the name again, and the Graveyard never transfers what it holds.
//! So the name is served `released` with no expiry (`expires_at_reason: released`), no owner,
//! manager or registrant, released at the block of the Graveyard's registry write, and with the
//! holder it had before the claim as `lapsed_registration.owner`
//! (docs/upstream.md).
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L98-L102 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L104 @ ens_v1@91c966f)
use serde_json::{Map, Value, json};

use super::NameFacts;
use crate::families::control::registry::{OwnerEvent, ZERO_ADDRESS};

/// The admitted Graveyard's registry write that holds the name's current ENSv1 record: the node's
/// newest current-registry owner write, when it names the Graveyard. An ENSv2 name has its own
/// registry, so a label registered in ENSv2 after the claim serves that registration instead.
pub(super) fn claim(facts: &NameFacts) -> Option<&OwnerEvent> {
    if facts.input.selection.authority_arm.as_deref() != Some("ens_v1") {
        return None;
    }
    facts
        .registry_node
        .as_ref()?
        .latest_transfer()
        .filter(|write| write.graveyard_held())
}

/// Serve a Graveyard-held name's registration as ended. `registrant` is the lease holder the
/// registration laterals found, before any mask: the name's holder when the Graveyard claimed it.
pub(super) fn apply(
    facts: &NameFacts,
    registrant: Option<String>,
    registration: &mut Map<String, Value>,
    control: &mut Map<String, Value>,
) {
    let Some(write) = claim(facts) else {
        return;
    };
    let released_at = facts
        .block_seconds
        .get(&write.position.block_number)
        .map_or(Value::Null, |seconds| json!(seconds));
    let holder = registrant.or_else(|| prior_holder(facts, write));
    for field in [
        "expiry",
        "ens_v1_expiry",
        "grace_ends_at",
        "registrant",
        "lifecycle_recompose_at",
    ] {
        registration.insert(field.into(), Value::Null);
    }
    registration.insert("status".into(), json!("released"));
    registration.insert("lifecycle_status".into(), json!("released"));
    registration.insert("expires_at_reason".into(), json!("released"));
    registration.insert("released_at".into(), released_at.clone());
    registration.insert(
        "lapsed_registration".into(),
        json!({"owner": holder, "released_at": released_at, "release_kind": "expired"}),
    );
    for field in ["owner", "registry_owner", "registrant", "expiry"] {
        if let Some(value) = control.get_mut(field) {
            *value = Value::Null;
        }
    }
}

/// The holder of a name with no lease holder: the owner its newest registry write before the
/// claim reported, the owner a registry-only name serves. A NameWrapper-selected name's registry
/// owner is the NameWrapper, never a holder.
fn prior_holder(facts: &NameFacts, claim: &OwnerEvent) -> Option<String> {
    let wrapped = facts.candidates.iter().any(|candidate| {
        candidate.is_wrapper()
            && facts.input.selection.surface_binding_id.as_deref()
                == Some(candidate.surface_binding_id.as_str())
    });
    if wrapped {
        return None;
    }
    facts
        .registry_node
        .as_ref()
        .into_iter()
        .flat_map(|node| node.owner_events.iter())
        .filter(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.source_family == "ens_v1_registry_l1"
                && event.position < claim.position
        })
        .max_by(|left, right| left.position.cmp(&right.position))
        .and_then(OwnerEvent::reported_owner)
        .filter(|owner| owner != ZERO_ADDRESS)
}

#[cfg(test)]
#[path = "graveyard_tests.rs"]
mod tests;
