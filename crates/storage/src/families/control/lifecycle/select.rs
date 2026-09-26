//! The ENSv2 registration candidate of one name (build.sql:319-348): one candidate per lifecycle
//! key from the name's membership of the key, then the cross-key preference, then the
//! released-elsewhere exclusion.
use serde_json::{Map, Value, json};

use super::{
    NameFacts,
    membership::{foreign_named, merged_for},
    served::{Selected, Tagged},
    view::{self, Candidate},
};
use crate::families::control::rows::LifecycleEvent;

/// The ENSv2 registration candidate (build.sql:319-348): one candidate per lifecycle key from
/// the merged maxima, then the cross-key preference, then the released-elsewhere exclusion.
pub(super) fn select_v2<'a>(
    facts: &'a NameFacts,
    tagged: &[Tagged<'a>],
    binding_resource: Option<&str>,
    trace: &mut Map<String, Value>,
) -> Selected<'a> {
    let name = facts.input.logical_name_id.as_str();
    let mut keys: Vec<String> = tagged
        .iter()
        .filter(|tagged| {
            tagged.event.is_v2_family()
                && tagged.event.original_logical_name_id.as_deref() == Some(name)
        })
        .filter_map(|tagged| tagged.key.clone())
        .collect();
    keys.sort();
    keys.dedup();
    // Events on the name's keys emitted for another name (a topology rebind): left out of the
    // name's membership (membership.rs) and traced.
    let foreign: Vec<String> = facts
        .events
        .iter()
        .filter(|event| keys.iter().any(|key| foreign_named(event, key, name)))
        .map(|event| event.position.event_identity.clone())
        .collect();
    if !foreign.is_empty() {
        trace.insert("foreign_named_members".into(), json!(foreign));
    }
    let mut candidates: Vec<(String, Candidate, &'a LifecycleEvent)> = Vec::new();
    for key in keys {
        let view = merged_for(facts, &key, name);
        let Some(candidate) = view::candidate(&view, &facts.order) else {
            continue;
        };
        // The candidate event itself, for its payload: the name's own event or an unnamed one on
        // the key. The interpreter's path-expiry release names the resource and no name
        // (adapters v2_registry/expiry.rs:58-59), so it is the key's candidate when it is the
        // latest, and the name is served released: an expired or released ENSv2 registration
        // stays ENSv2 and is served unregistered (Tate's ruling on TYR-36 step 3). On chain a
        // registration is over once its expiry has passed: the registry then reports no owner
        // and no resolver for it, and unregistering sets the expiry to the current time.
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L36 @ ens_v2@a971bd64)
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L206 @ ens_v2@a971bd64)
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L313-L316 @ ens_v2@a971bd64)
        // Today's name-scoped fold (build.sql:322) never sees the unnamed release; it is served
        // only as a released tombstone's deciding fact (build.sql:349-364, `tombstone.rs`), and
        // otherwise the harness records the served-side bug. Another name's event is never the
        // candidate: the membership leaves it out, and one found here is traced and the key
        // skipped.
        let event = tagged.iter().find(|tagged| {
            tagged.event.position.event_identity == candidate.position.event_identity
                && tagged.key.as_deref() == Some(key.as_str())
        });
        if let Some(foreign) = event.filter(|tagged| {
            tagged
                .event
                .original_logical_name_id
                .as_deref()
                .is_some_and(|emitted| emitted != name)
        }) {
            trace
                .entry("foreign_named_candidate")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("an array")
                .push(json!(foreign.event.position.event_identity));
            continue;
        }
        match event {
            Some(tagged) => candidates.push((key, candidate, tagged.event)),
            None => {
                trace
                    .entry("candidate_without_event")
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .expect("an array")
                    .push(json!(candidate.position.event_identity));
            }
        }
    }
    trace.insert(
        "key_candidates".into(),
        json!(
            candidates
                .iter()
                .map(|(key, candidate, _)| json!({
                    "key": key, "kind": format!("{:?}", candidate.kind),
                    "event": candidate.position.event_identity,
                }))
                .collect::<Vec<_>>()
        ),
    );
    let winner = view::preferred(
        candidates
            .iter()
            .map(|(key, candidate, _)| (key.as_str(), candidate)),
        binding_resource,
        &facts.order,
    );
    let Some((key, candidate, event)) = winner.map(|index| &candidates[index]) else {
        return Selected::none();
    };
    let released_elsewhere = candidate.event_kind == "RegistrationReleased"
        && binding_resource.is_some()
        && event.resource_id.as_deref() != binding_resource;
    if released_elsewhere {
        return Selected::none();
    }
    Selected {
        event: Some(event),
        lifecycle_key: Some(key.clone()),
        resource: event.resource_id.clone(),
        released: false,
    }
}
