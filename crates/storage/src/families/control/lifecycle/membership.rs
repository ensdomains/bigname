//! A name's lifecycle membership of an ENSv2 key. The F2a key state of a resource counts every
//! event on the resource (design:40, decoder rule 1), which is the name's membership as long as
//! the resource carries one name's history. A subregistry rebind moves a resource from one name
//! to another in one raw log: the adapter emits a named RegistrationReleased for the previous
//! name and a named RegistrationGranted for the current one on the same resource
//! (crates/adapters/src/schema_v2/protocol/v2_registry/topology.rs:65-84, :363-370). A name's
//! membership is its own events and the unnamed ones, such as the interpreter's path-expiry
//! release, never another name's; when the key holds another name's events the read folds the
//! key's retained events without them instead of using the stored maxima.
use super::{NameFacts, view};
use crate::families::control::{
    position::EventOrder,
    rows::{LifecycleEvent, Mark, Maxima},
};

/// The membership maxima of one lifecycle key folded from retained events in `order`'s
/// membership order, the fold of step 2's reducer (crates/project/src/families/lifecycle.rs
/// :388-497). Every mark keeps its event's own position. `last_revival` is kept for a resource
/// key only.
pub fn maxima_of<'a>(
    events: impl IntoIterator<Item = &'a LifecycleEvent>,
    resource: bool,
    order: &EventOrder,
) -> Maxima {
    let mut own: Vec<&LifecycleEvent> = events.into_iter().collect();
    own.sort_by(|left, right| order.membership(&left.position, &right.position));
    let mark = |event: &LifecycleEvent| {
        Some(Mark {
            position: event.position.clone(),
            detail: serde_json::json!({"kind": event.event_kind}),
        })
    };
    let mut maxima = Maxima::default();
    for event in own {
        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                maxima.last_grant = mark(event);
                maxima.last_active = mark(event);
            }
            "RegistrationReserved" => {
                maxima.last_reservation = mark(event);
                maxima.last_active = mark(event);
            }
            "RegistrationReleased" => {
                maxima.last_release_any = mark(event);
                if event.is_path_expiry() {
                    maxima.last_path_expiry = mark(event);
                } else {
                    maxima.last_explicit_release = mark(event);
                }
            }
            "RegistrationRenewed" => {
                if resource
                    && event.revived_from_expiry == Some(true)
                    && maxima.last_path_expiry.is_some()
                {
                    maxima.last_revival = mark(event);
                }
                maxima.last_renewal = mark(event);
            }
            "ExpiryChanged" => maxima.last_expiry_changed = mark(event),
            _ => {}
        }
    }
    maxima
}

/// Whether `event` sits on resource key `key` and was emitted for a name other than `name`.
pub(super) fn foreign_named(event: &LifecycleEvent, key: &str, name: &str) -> bool {
    event.state_kind == "resource"
        && event.state_key == key
        && event
            .original_logical_name_id
            .as_deref()
            .is_some_and(|emitted| emitted != name)
}

/// The retained events of one ENSv2 lifecycle key that are `name`'s membership: the resource's
/// events without another name's, and the null-resource events of every triple whose association
/// targets the key, or of an unassociated triple whose own key it is.
pub(super) fn members<'a>(facts: &'a NameFacts, key: &str, name: &str) -> Vec<&'a LifecycleEvent> {
    let triples: Vec<String> = facts
        .triples
        .iter()
        .filter(|triple| match &triple.target {
            Some(target) => target == key,
            None => triple.unassociated_key().as_deref() == Some(key),
        })
        .map(|triple| triple.state_key())
        .collect();
    facts
        .events
        .iter()
        .filter(|event| match event.state_kind.as_str() {
            "resource" => event.state_key == key && !foreign_named(event, key, name),
            "triple" => triples.contains(&event.state_key),
            _ => false,
        })
        .collect()
}

/// The merged view of one ENSv2 lifecycle key for `name`: the resource's key state, or its
/// retained events without another name's when it holds any, merged with the summaries of the
/// triples associated with it; or an unassociated triple's summary alone. In the harness's
/// same-block counterfactual the view is the members folded in that order instead.
pub(super) fn merged_for(facts: &NameFacts, key: &str, name: &str) -> view::MergedView {
    if facts.order != EventOrder::Canonical {
        let folded = maxima_of(members(facts, key, name), true, &facts.order);
        return view::merged_view(Some(&folded), []);
    }
    let associated = facts
        .triples
        .iter()
        .filter(|triple| triple.target.as_deref() == Some(key))
        .map(|triple| &triple.maxima);
    if let Some(state) = facts.key_states.get(key) {
        if facts
            .events
            .iter()
            .any(|event| foreign_named(event, key, name))
        {
            let own = maxima_of(
                facts.events.iter().filter(|event| {
                    event.state_kind == "resource"
                        && event.state_key == key
                        && !foreign_named(event, key, name)
                }),
                true,
                &EventOrder::Canonical,
            );
            return view::merged_view(Some(&own), associated);
        }
        return view::merged_view(Some(state), associated);
    }
    let unassociated = facts
        .triples
        .iter()
        .filter(|triple| {
            triple.target.is_none() && triple.unassociated_key().as_deref() == Some(key)
        })
        .map(|triple| &triple.maxima);
    view::merged_view(None, associated.chain(unassociated))
}
