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
use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use super::NameFacts;
use crate::families::control::{
    position::Position,
    registry::{OwnerEvent, ZERO_ADDRESS},
    rows::{BindingCandidate, LifecycleEvent},
};

/// The admitted Graveyard's registry write that holds the name's current ENSv1 record: when the
/// node's newest current-registry owner write names the Graveyard, the first write of that
/// trailing run of Graveyard writes ([`first_held`]). An ENSv2 name has its own registry, so a
/// label registered in ENSv2 after the claim serves that registration instead. A migrated name's
/// record is the Graveyard's from its migration, which `clear` never claims, so that write is no
/// claim even where the name falls back to its ENSv1 arm.
pub(super) fn claim(facts: &NameFacts) -> Option<&OwnerEvent> {
    if facts.input.selection.authority_arm.as_deref() != Some("ens_v1") || facts.migration.is_some()
    {
        return None;
    }
    first_held(facts)
}

/// The write that gave the Graveyard the node's record, when it still holds it: the first of the
/// trailing run of current-registry owner writes naming it. `clear` writes a subname of a name it
/// holds again each time it runs, for that subname or any name below it, and a holder's own write
/// to the Graveyard can precede a clear. Two Graveyard writes are one hold when the Graveyard
/// controls the record without a break between them ([`carries`]): then the registration ended at
/// the earlier one. The claim is the first write of the run, and its holder is the one before it.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_mainnet_20261002@4f47cbcc)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
fn first_held(facts: &NameFacts) -> Option<&OwnerEvent> {
    let node = facts.registry_node.as_ref()?;
    let registry = node.latest_transfer()?.source_family.as_str();
    let writes: Vec<&OwnerEvent> = node
        .owner_events
        .iter()
        .filter(|event| {
            event.event_kind == "AuthorityTransferred" && event.source_family == registry
        })
        .map(|event| (&event.position, event))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    let mut claim = writes.len().checked_sub(1)?;
    if !writes[claim].graveyard_held() {
        return None;
    }
    while let Some(earlier) = (0..claim)
        .rev()
        .find(|index| writes[*index].graveyard_held())
    {
        if !carries(facts, &writes[earlier + 1..claim], writes[claim]) {
            break;
        }
        claim = earlier;
    }
    Some(writes[claim])
}

/// Whether the Graveyard controlled the record without a break from its earlier write to its
/// write `next`, given the registry writes `between` them. The invariant: a Graveyard hold carries
/// across those writes only when no one else controlled the record in between. The record's
/// controller is its registry owner, or, while the NameWrapper owns it, the wrapped token's
/// holder. So the hold carries when every write between names one of the name's NameWrappers,
/// and from the first of them to `next` every transfer of the name's wrapped token goes to the
/// Graveyard, with at least one. Each wrap mints on a resource with its own NameWrapper binding
/// (adapters `wrapper.rs`), and the adapter records no burn and no transfer of a token whose
/// NameWrapper authority has ended (adapters `wrapper_custody.rs`). Any write to anyone else (a `reclaim`,
/// a parent's `setSubnodeOwner`) ends the hold, and so does any wrapped token transfer to anyone
/// else, even one the same transaction reverses. The holder is then the one before `next`. A
/// BaseRegistrar token move writes no record and controls nothing until a `reclaim` writes it, so
/// it neither ends nor carries the hold.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L630 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L927-L948 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
fn carries(facts: &NameFacts, between: &[&OwnerEvent], next: &OwnerEvent) -> bool {
    let Some(first) = between.first() else {
        return true;
    };
    let Some(graveyard) = next.owner_getter.as_deref() else {
        return false;
    };
    if !between.iter().all(|write| {
        write
            .reported_owner()
            .is_some_and(|owner| name_wrapper(facts, &owner))
    }) {
        return false;
    }
    let wrappers = name_wrappers(facts);
    let (from, to) = (place(&first.position), place(&next.position));
    let mut to_graveyard = false;
    for event in facts.events.iter().filter(|event| {
        event.source_family == "ens_v1_wrapper_l1"
            && event.event_kind == "TokenControlTransferred"
            && wrappers
                .iter()
                .any(|wrapper| event.resource_id.as_deref() == Some(wrapper.resource_id.as_str()))
            && (from..to).contains(&place(&event.position))
    }) {
        if !event
            .to_address
            .as_deref()
            .is_some_and(|holder| holder.eq_ignore_ascii_case(graveyard))
        {
            return false;
        }
        to_graveyard = true;
    }
    to_graveyard
}

/// Whether `address` is one of the name's NameWrappers.
fn name_wrapper(facts: &NameFacts, address: &str) -> bool {
    name_wrappers(facts).iter().any(|wrapper| {
        wrapper
            .emitting_address
            .as_deref()
            .is_some_and(|emitter| emitter.eq_ignore_ascii_case(address))
    })
}

/// The name's NameWrapper candidates that record their NameWrapper's address.
fn name_wrappers(facts: &NameFacts) -> Vec<&BindingCandidate> {
    facts
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.authority_kind.as_deref() == Some("wrapper")
                && candidate.emitting_address.is_some()
        })
        .collect()
}

/// Serve a Graveyard-held name's registration as ended. The lapsed owner is the holder as it
/// stood just before the Graveyard's write. `registrant` is the holder the registration laterals
/// found, before any mask, at `trace.registrant_position`. It is that holder when it was found
/// before the write and is not zero or the Graveyard, which a wrapped subname's token reaches as
/// it is cleared. A token moved after a holder's own write to the Graveyard (ruling 4) names a
/// later holder, so the lease's token history before the write decides, then the registry's. The
/// NameWrapper is never the holder: a wrapped `.eth` name handed to a registry-only binding at
/// the claim keeps its lease's transfer into the NameWrapper in the laterals, and its holder is
/// the wrapped token's ([`prior_holder`]).
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
            found_before
                && holder != ZERO_ADDRESS
                && Some(holder.as_str()) != graveyard
                && !name_wrapper(facts, holder)
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
    let holder = lease_recipient(facts, claim, graveyard, false)?;
    (!name_wrapper(facts, &holder)).then_some(holder)
}

/// The lease's last grant registrant or transfer recipient before the claim's write, skipping
/// zero and the Graveyard, and with `past_wrapper` the NameWrapper too.
fn lease_recipient(
    facts: &NameFacts,
    claim: &OwnerEvent,
    graveyard: Option<&str>,
    past_wrapper: bool,
) -> Option<String> {
    let recipient = |event: &LifecycleEvent| match event.event_kind.as_str() {
        "RegistrationGranted" => event.registrant.clone(),
        "TokenControlTransferred" => event.to_address.clone(),
        _ => None,
    };
    facts
        .events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && before(&event.position, claim)
                && super::staged_as_own(facts, event)
                && recipient(event).is_some_and(|holder| {
                    !holder.eq_ignore_ascii_case(ZERO_ADDRESS)
                        && graveyard.is_none_or(|graveyard| !holder.eq_ignore_ascii_case(graveyard))
                        && !(past_wrapper && name_wrapper(facts, &holder))
                })
        })
        .max_by(|left, right| left.position.cmp(&right.position))
        .and_then(recipient)
}

/// The holder of a name with no other holder, from before the claim's write: the owner its
/// newest registry write reported, the owner a registry-only name serves. When that owner is the
/// NameWrapper, the name's registry record was wrapped until the claim, and the holder is the
/// token holder its NameWrapper transfers name last ([`wrapped_holder`]). When the wrap went to
/// the Graveyard ([`handed_over`]), it is whoever handed the name over.
fn prior_holder(facts: &NameFacts, claim: &OwnerEvent, graveyard: Option<&str>) -> Option<String> {
    let mut writes: Vec<&OwnerEvent> = facts
        .registry_node
        .as_ref()
        .into_iter()
        .flat_map(|node| node.owner_events.iter())
        .filter(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.source_family == "ens_v1_registry_l1"
                && before(&event.position, claim)
        })
        .collect();
    writes.sort_by(|left, right| left.position.cmp(&right.position));
    let owner = writes
        .last()
        .and_then(|write| write.reported_owner())
        .filter(|owner| owner != ZERO_ADDRESS)?;
    if name_wrapper(facts, &owner) {
        let run = writes
            .iter()
            .rev()
            .take_while(|write| {
                write
                    .reported_owner()
                    .is_some_and(|owner| name_wrapper(facts, &owner))
            })
            .count();
        let since = writes.len() - run;
        let wrappers = name_wrappers(facts);
        return wrapped_holder(facts, claim, &writes[since].position, graveyard, &wrappers)
            .or_else(|| {
                if !handed_over(facts, claim, &writes[since].position, graveyard, &wrappers) {
                    return None;
                }
                // The run's wrapped token went only to the Graveyard: whoever held the name
                // before the wrap handed it over, as a token sent to the Graveyard leaves its
                // sender. That is the lease's last holder outside the NameWrapper, else the
                // registry owner before the run.
                lease_recipient(facts, claim, graveyard, true).or_else(|| {
                    since
                        .checked_sub(1)
                        .and_then(|before| writes[before].reported_owner())
                        .filter(|owner| owner != ZERO_ADDRESS && !name_wrapper(facts, owner))
                })
            });
    }
    Some(owner)
}

/// Whether the NameWrapper's run of registry writes from `since` moved the name's wrapped token to
/// the Graveyard before the claim's write: a wrap to the Graveyard. A bare registry write to the
/// NameWrapper wraps nothing, and its writer lost control at it.
fn handed_over(
    facts: &NameFacts,
    claim: &OwnerEvent,
    since: &Position,
    graveyard: Option<&str>,
    wrappers: &[&BindingCandidate],
) -> bool {
    let Some(graveyard) = graveyard else {
        return false;
    };
    facts.events.iter().any(|event| {
        event.source_family == "ens_v1_wrapper_l1"
            && event.event_kind == "TokenControlTransferred"
            && wrappers
                .iter()
                .any(|wrapper| event.resource_id.as_deref() == Some(wrapper.resource_id.as_str()))
            && before(&event.position, claim)
            && (
                event.position.block_number,
                event.position.transaction_index,
            ) >= (since.block_number, since.transaction_index)
            && event
                .to_address
                .as_deref()
                .is_some_and(|holder| holder.eq_ignore_ascii_case(graveyard))
    })
}

/// The wrapped token's holder its NameWrapper transfers name last before the claim's write: the
/// wrap's or a later transfer's recipient, skipping a transfer to the Graveyard (and to zero or
/// the NameWrapper, which no transfer reaches). A cleared wrapped subname's registry write names
/// the Graveyard, which ends the NameWrapper authority and selects a registry-only binding
/// (adapters `wrapper_custody.rs`), whose registration laterals read no NameWrapper event. A
/// re-parent moves the token to the Graveyard in the claim's own transaction, before its write.
/// Only transfers from the transaction of `since`, the first of the NameWrapper's trailing run of
/// registry writes before the claim, count: an earlier wrap's holder lost control when that wrap
/// ended, and a later write to the NameWrapper with no wrap of its own revives no one's control.
fn wrapped_holder(
    facts: &NameFacts,
    claim: &OwnerEvent,
    since: &Position,
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
                && (
                    event.position.block_number,
                    event.position.transaction_index,
                ) >= (since.block_number, since.transaction_index)
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
