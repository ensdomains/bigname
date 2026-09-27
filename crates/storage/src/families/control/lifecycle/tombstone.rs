//! The deciding fact of a released ENSv2 tombstone (TYR-36 step 6, de24ff32). When nothing
//! ENSv2 is open for a name and the latest lifecycle fact of the registration it was last bound
//! to is a release, authority selection keeps the name on that registration's tombstone and
//! records the fact that decided it (name_authority/build.sql:48-271, :584-587); name_current
//! serves that fact, its kind and payload, on the tombstone's resource, instead of the ordinary
//! registration fold (build.sql:349-364). The fact can be a release written without a name, or
//! the end of a later reservation of the name with no resource or another one.
use anyhow::Result;
use serde_json::Value;

use super::{Clock, NameFacts, membership::without_expired};
use crate::families::control::{
    position::Position,
    rows::{BindingCandidate, LifecycleEvent},
};

/// The fact that decided a released tombstone, and the tombstone's resource.
pub(super) struct Tombstone<'a> {
    pub(super) event: &'a LifecycleEvent,
    pub(super) resource: String,
}

/// The kinds a registration's lifecycle facts are (name_authority/build.sql:90-93).
const LIFECYCLE: [&str; 4] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "RegistrationReserved",
];

/// The released tombstone's deciding fact, or none when the name is not one: the selected arm is
/// not exactly ens_v2 (name_authority/build.sql:584-587), an ENSv2 binding of the name is open at
/// the publication (:267-271), or the latest lifecycle fact is not a release (:266). A
/// reservation whose expiry cannot be compared exactly fails the read
/// (`membership::expired_when_written`).
pub(super) fn deciding_fact<'a>(
    facts: &'a NameFacts,
    clock: &Clock,
) -> Result<Option<Tombstone<'a>>> {
    if facts.input.selection.authority_arm.as_deref() != Some("ens_v2") {
        return Ok(None);
    }
    let bindings: Vec<&BindingCandidate> = facts
        .candidates
        .iter()
        .filter(|candidate| candidate.authority_arm == "ens_v2")
        .collect();
    if bindings
        .iter()
        .any(|binding| binding.open_at(clock.timestamp_seconds))
    {
        return Ok(None);
    }
    // The binding the name was last bound to (:48-56).
    let bound = bindings.into_iter().max_by(|left, right| {
        (
            left.block_number,
            left.transaction_index.unwrap_or(-1),
            left.log_index.unwrap_or(-1),
        )
            .cmp(&(
                right.block_number,
                right.transaction_index.unwrap_or(-1),
                right.log_index.unwrap_or(-1),
            ))
            .then_with(|| left.surface_binding_id.cmp(&right.surface_binding_id))
    });
    let Some(bound) = bound else {
        return Ok(None);
    };
    let resource = bound.resource_id.as_str();
    let name = facts.input.logical_name_id.as_str();
    let named = |event: &LifecycleEvent| event.original_logical_name_id.as_deref() == Some(name);
    let facts_of_the_name = facts
        .events
        .iter()
        .filter(|event| event.is_v2_family())
        .filter(|event| {
            let on_bound = event.resource_id.as_deref() == Some(resource);
            // The registration's own facts, and a release of it written without a name (:68-93).
            let own = on_bound
                && LIFECYCLE.contains(&event.event_kind.as_str())
                && (named(event)
                    || (event.original_logical_name_id.is_none()
                        && event.event_kind == "RegistrationReleased"));
            // A reservation of the name on any other resource or none, and the end of the name's
            // current reservation (:95-210).
            let reservation = named(event)
                && !on_bound
                && match event.event_kind.as_str() {
                    "RegistrationReserved" => true,
                    "RegistrationReleased" if event.resource_id.is_none() => {
                        ends_resourceless_reservation(facts, event)
                    }
                    "RegistrationReleased" => ends_reservation_on_its_resource(facts, event),
                    _ => false,
                };
            own || reservation
        });
    // A reservation expired when written is never live and takes no part (:212-239).
    let latest = without_expired(facts, facts_of_the_name)?
        .into_iter()
        .max_by(|left, right| facts.order.name_membership(&left.position, &right.position));
    Ok(latest
        .filter(|latest| latest.event_kind == "RegistrationReleased")
        .map(|latest| Tombstone {
            event: latest,
            resource: resource.to_owned(),
        }))
}

/// Whether `witness` is before `event` as the reservation-end rule compares them: its block,
/// transaction and log, a missing one read as -1, before the release's, a missing one read as the
/// end of its block (name_authority/build.sql:156-161, :198-203).
fn before(witness: &Position, event: &Position) -> bool {
    witness.bound()
        < (
            event.block_number,
            event.transaction_index.unwrap_or(i64::MAX),
            event.log_index.unwrap_or(i64::MAX),
        )
}

/// The latest of `witnesses` before `event` in the name-membership order.
fn latest_before<'a>(
    facts: &'a NameFacts,
    event: &LifecycleEvent,
    witnesses: impl Fn(&LifecycleEvent) -> bool,
) -> Option<&'a LifecycleEvent> {
    facts
        .events
        .iter()
        .filter(|witness| {
            witness.is_v2_family()
                && witnesses(witness)
                && before(&witness.position, &event.position)
        })
        .max_by(|left, right| facts.order.name_membership(&left.position, &right.position))
}

/// The registry identifier and token id of a resource-less event, from its triple key
/// (`[name, registry identifier, token id]`); none when either is empty.
fn entry(event: &LifecycleEvent) -> Option<(String, String)> {
    if event.state_kind != "triple" {
        return None;
    }
    let key: Vec<String> = serde_json::from_str::<Value>(&event.state_key)
        .ok()?
        .as_array()?
        .iter()
        .map(|part| part.as_str().unwrap_or_default().to_owned())
        .collect();
    match key.as_slice() {
        [_, registry, token] if !registry.is_empty() && !token.is_empty() => {
            Some((registry.clone(), token.clone()))
        }
        _ => None,
    }
}

/// A named release without a resource ends the name's current reservation when the latest earlier
/// fact among the name's reservations and registrations is a reservation of the same registry
/// instance and token id (name_authority/build.sql:139-167). The families key a resource-less
/// event by its registry identifier and token id, so the two compare by that key. A reservation
/// with its own resource has no such key here and is never a match: Interpret gives a
/// reservation its own resource only at token version zero, while a release without a resource
/// ends a versioned token, so their token ids differ (tests/issue_503/reservation_resource.rs).
fn ends_resourceless_reservation(facts: &NameFacts, event: &LifecycleEvent) -> bool {
    let name = event.original_logical_name_id.as_deref();
    let Some(ended) = entry(event) else {
        return false;
    };
    latest_before(facts, event, |witness| {
        witness.original_logical_name_id.as_deref() == name
            && matches!(
                witness.event_kind.as_str(),
                "RegistrationReserved" | "RegistrationGranted"
            )
    })
    .is_some_and(|witness| {
        witness.event_kind == "RegistrationReserved" && entry(witness).as_ref() == Some(&ended)
    })
}

/// A named release on a resource other than the tombstone's ends the name's current reservation
/// when the latest earlier fact among the name's reservations, on any resource, and the grants
/// on the release's resource, of any name, is a reservation on that resource
/// (name_authority/build.sql:182-209).
fn ends_reservation_on_its_resource(facts: &NameFacts, event: &LifecycleEvent) -> bool {
    let name = event.original_logical_name_id.as_deref();
    let resource = event.resource_id.as_deref();
    latest_before(facts, event, |witness| {
        (witness.event_kind == "RegistrationReserved"
            && witness.original_logical_name_id.as_deref() == name)
            || (witness.event_kind == "RegistrationGranted"
                && witness.resource_id.as_deref() == resource)
    })
    .is_some_and(|witness| {
        witness.event_kind == "RegistrationReserved" && witness.resource_id.as_deref() == resource
    })
}
