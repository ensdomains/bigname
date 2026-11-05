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
use crate::families::control::{
    position::Position,
    registry::{OwnerEvent, ZERO_ADDRESS},
    rows::{BindingCandidate, LifecycleEvent},
};

/// The admitted Graveyard's registry write that holds the name's current ENSv1 record: the node's
/// newest current-registry owner write, when it names the Graveyard. An ENSv2 name has its own
/// registry, so a label registered in ENSv2 after the claim serves that registration instead. A
/// migrated name's record is the Graveyard's from its migration, which `clear` never claims, so
/// that write is no claim even where the name falls back to its ENSv1 arm.
pub(super) fn claim(facts: &NameFacts) -> Option<&OwnerEvent> {
    if facts.input.selection.authority_arm.as_deref() != Some("ens_v1") || facts.migration.is_some()
    {
        return None;
    }
    facts
        .registry_node
        .as_ref()?
        .latest_transfer()
        .filter(|write| write.graveyard_held())
}

/// Serve a Graveyard-held name's registration as ended. The lapsed owner is the holder as it
/// stood just before the Graveyard's write. `registrant` is the holder the registration laterals
/// found, before any mask, at `trace.registrant_position`. It is that holder when it was found
/// before the write and is not zero or the Graveyard, which a wrapped subname's token reaches as
/// it is cleared. A token moved after a holder's own write to the Graveyard (ruling 4) names a
/// later holder, so the lease's token history before the write decides, then the registry's.
pub(super) fn apply(
    facts: &NameFacts,
    registrant: Option<String>,
    trace: &Map<String, Value>,
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
    let graveyard = write.owner_getter.as_deref();
    let found_before = trace
        .get("registrant_position")
        .and_then(|position| {
            Some((
                position.get("block_number")?.as_i64()?,
                position.get("transaction_index").and_then(Value::as_i64),
                position.get("log_index").and_then(Value::as_i64),
            ))
        })
        .is_none_or(|position| position < place(&write.position));
    let holder = registrant
        .filter(|holder| {
            found_before && holder != ZERO_ADDRESS && Some(holder.as_str()) != graveyard
        })
        .or_else(|| lease_holder(facts, write, graveyard))
        .or_else(|| prior_holder(facts, write, graveyard));
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

/// The holder the name's BaseRegistrar token history names last before the claim's write: a
/// grant's registrant or a transfer's recipient, skipping a transfer to zero or the Graveyard. A
/// token in the NameWrapper names no holder here: its wrapped token's history decides
/// ([`prior_holder`]).
fn lease_holder(facts: &NameFacts, claim: &OwnerEvent, graveyard: Option<&str>) -> Option<String> {
    let recipient = |event: &LifecycleEvent| match event.event_kind.as_str() {
        "RegistrationGranted" => event.registrant.clone(),
        "TokenControlTransferred" => event.to_address.clone(),
        _ => None,
    };
    let holder = facts
        .events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && before(&event.position, claim)
                && super::staged_as_own(facts, event)
                && recipient(event).is_some_and(|holder| {
                    !holder.eq_ignore_ascii_case(ZERO_ADDRESS)
                        && graveyard.is_none_or(|graveyard| !holder.eq_ignore_ascii_case(graveyard))
                })
        })
        .max_by(|left, right| left.position.cmp(&right.position))
        .and_then(recipient)?;
    let name_wrapper = facts.candidates.iter().any(|candidate| {
        candidate.authority_kind.as_deref() == Some("wrapper")
            && candidate
                .emitting_address
                .as_deref()
                .is_some_and(|wrapper| wrapper.eq_ignore_ascii_case(&holder))
    });
    (!name_wrapper).then_some(holder)
}

/// The holder of a name with no other holder, from before the claim's write: the owner its
/// newest registry write reported, the owner a registry-only name serves. When that owner is the
/// NameWrapper, the name's registry record was wrapped until the claim, and the holder is the
/// token holder its NameWrapper transfers name last ([`wrapped_holder`]).
fn prior_holder(facts: &NameFacts, claim: &OwnerEvent, graveyard: Option<&str>) -> Option<String> {
    let owner = facts
        .registry_node
        .as_ref()
        .into_iter()
        .flat_map(|node| node.owner_events.iter())
        .filter(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.source_family == "ens_v1_registry_l1"
                && before(&event.position, claim)
        })
        .max_by(|left, right| left.position.cmp(&right.position))
        .and_then(OwnerEvent::reported_owner)
        .filter(|owner| owner != ZERO_ADDRESS)?;
    let wrappers: Vec<&BindingCandidate> = facts
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.authority_kind.as_deref() == Some("wrapper")
                && candidate.emitting_address.is_some()
        })
        .collect();
    if wrappers
        .iter()
        .any(|wrapper| wrapper.emitting_address.as_deref() == Some(owner.as_str()))
    {
        return wrapped_holder(facts, claim, graveyard, &wrappers);
    }
    Some(owner)
}

/// The wrapped token's holder its NameWrapper transfers name last before the claim's write: the
/// wrap's or a later transfer's recipient, skipping a transfer to the Graveyard (and to zero or
/// the NameWrapper, which no transfer reaches). A cleared wrapped subname's registry write names
/// the Graveyard, which ends the NameWrapper authority and selects a registry-only binding
/// (adapters `wrapper_custody.rs`), whose registration laterals read no NameWrapper event. A
/// re-parent moves the token to the Graveyard in the claim's own transaction, before its write.
fn wrapped_holder(
    facts: &NameFacts,
    claim: &OwnerEvent,
    graveyard: Option<&str>,
    wrappers: &[&BindingCandidate],
) -> Option<String> {
    let names = |holder: &str, address: Option<&str>| {
        address.is_some_and(|address| address.eq_ignore_ascii_case(holder))
    };
    facts
        .events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
                && wrappers.iter().any(|wrapper| {
                    event.resource_id.as_deref() == Some(wrapper.resource_id.as_str())
                })
                && before(&event.position, claim)
                && event.to_address.as_deref().is_some_and(|holder| {
                    !names(holder, Some(ZERO_ADDRESS))
                        && !names(holder, graveyard)
                        && !wrappers
                            .iter()
                            .any(|wrapper| names(holder, wrapper.emitting_address.as_deref()))
                })
        })
        .max_by(|left, right| left.position.cmp(&right.position))
        .and_then(|event| event.to_address.clone())
}

/// A position's place in the chain: block, transaction and log.
fn place(position: &Position) -> (i64, Option<i64>, Option<i64>) {
    (
        position.block_number,
        position.transaction_index,
        position.log_index,
    )
}

/// Whether `position` lies before the claim's write. Earlier writes of the claim's own
/// transaction count: a holder can set the record and call `clear` in one transaction. The
/// Graveyard's own custody steps there, a re-parent's write naming the NameWrapper and its token
/// move to the Graveyard, name no holder and are passed over by the holder rules.
fn before(position: &Position, claim: &OwnerEvent) -> bool {
    place(position) < place(&claim.position)
}

#[cfg(test)]
#[path = "graveyard_tests.rs"]
mod tests;
