//! The served registration and control values of one name, from its loaded facts. Each function
//! restates one lateral of name_current/build.sql over the retained events; every "latest" is
//! the latest under the canonical order.
use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use super::{
    Clock, NameFacts, ShadowName,
    admission::{Authority, Probe, StagedName},
    laterals::{
        authority_context, control_owner, expiry_candidate, format_utc, latest_event_kind,
        registered_at, registrant, registrar_resource,
    },
    view::{self, Candidate, MergedView},
};
use crate::families::control::{
    position::Position,
    rows::LifecycleEvent,
    wrapper::{effective_wrapper, servable_expiry},
};

/// A retained event with what the read decided about it.
pub(super) struct Tagged<'a> {
    pub(super) event: &'a LifecycleEvent,
    pub(super) staged: StagedName,
    pub(super) admitted: bool,
    /// The ENSv2 lifecycle key (v2_lifecycle_events.sql:10-23), for an ENSv2-family event.
    pub(super) key: Option<String>,
}

/// The registration the name serves (build.sql:319-348).
pub(super) struct Selected<'a> {
    pub(super) event: Option<&'a LifecycleEvent>,
    pub(super) lifecycle_key: Option<String>,
}

impl Selected<'_> {
    pub(super) fn kind(&self) -> Option<&str> {
        self.event.map(|event| event.event_kind.as_str())
    }
}

pub(super) fn latest<T>(
    items: impl IntoIterator<Item = T>,
    position: impl for<'b> Fn(&'b T) -> &'b Position,
) -> Option<T> {
    items
        .into_iter()
        .max_by(|left, right| position(left).cmp(position(right)))
}

fn opt_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |text| Value::String(text.to_owned()))
}

pub(super) fn evaluate(facts: &NameFacts, clock: &Clock) -> ShadowName {
    let input = &facts.input;
    let selection = &input.selection;
    let is_v2 = selection.is_v2();
    let binding = selection.surface_binding_id.as_deref().and_then(|id| {
        facts
            .candidates
            .iter()
            .find(|candidate| candidate.surface_binding_id == id)
    });
    let binding_resource = binding.map(|binding| binding.resource_id.as_str());
    let authority = Authority {
        name: &input.logical_name_id,
        selection,
        candidates: &facts.candidates,
        binding,
        wrapper_modifier: selection
            .resource_id
            .as_deref()
            .and_then(|resource| facts.wrappers.get(resource))
            .is_some_and(|wrapper| wrapper.has_modifier),
        events: &facts.events,
    };
    let triple_targets: BTreeMap<String, Option<String>> = facts
        .triples
        .iter()
        .map(|triple| {
            (
                triple.state_key(),
                triple.target.clone().or_else(|| triple.unassociated_key()),
            )
        })
        .collect();
    let tagged: Vec<Tagged<'_>> = facts
        .events
        .iter()
        .map(|event| {
            let linked = authority.wrapper_linked(event);
            let key = event
                .is_v2_family()
                .then(|| match event.state_kind.as_str() {
                    "triple" => triple_targets.get(&event.state_key).cloned().flatten(),
                    _ => event.resource_id.clone(),
                });
            Tagged {
                event,
                staged: authority.staged_name(event),
                admitted: authority.admits(&Probe::of(event, linked)),
                key: key.flatten(),
            }
        })
        .collect();

    let mut trace = Map::new();
    let selected = if is_v2 {
        select_v2(facts, &tagged, binding_resource, &mut trace)
    } else {
        Selected {
            event: latest(
                tagged.iter().filter(|tagged| {
                    tagged.staged == StagedName::Ours
                        && tagged.admitted
                        && matches!(
                            tagged.event.event_kind.as_str(),
                            "RegistrationGranted"
                                | "RegistrationRenewed"
                                | "RegistrationReleased"
                                | "RegistrationReserved"
                        )
                }),
                |tagged| &tagged.event.position,
            )
            .map(|tagged| tagged.event),
            lifecycle_key: None,
        }
    };
    let has_lifecycle = is_v2 && selected.event.is_some();
    let selected_resource = selected
        .event
        .and_then(|event| event.resource_id.as_deref());
    let mismatch = has_lifecycle && selected_resource != binding_resource;
    let event_resource = if has_lifecycle {
        selected_resource
    } else {
        binding_resource
    };
    let selected_key = selected
        .lifecycle_key
        .clone()
        .or_else(|| event_resource.map(str::to_owned));
    trace.insert("is_v2".into(), json!(is_v2));
    trace.insert(
        "selected_event".into(),
        opt_text(
            selected
                .event
                .map(|event| event.position.event_identity.as_str()),
        ),
    );
    trace.insert("selected_key".into(), opt_text(selected_key.as_deref()));
    trace.insert("identity_mismatch".into(), json!(mismatch));
    // A selected release emitted without a name: the path-expiry release the interpreter
    // synthesises. The harness reads these to check that the shadow serves the release.
    let unnamed_release = selected.event.filter(|event| {
        event.original_logical_name_id.is_none() && event.event_kind == "RegistrationReleased"
    });
    trace.insert(
        "selected_unnamed_path_expiry".into(),
        json!(unnamed_release.is_some_and(LifecycleEvent::is_path_expiry)),
    );
    if let Some(release) = unnamed_release {
        trace.insert("selected_released_at".into(), release.released_at.clone());
        trace.insert("selected_expiry".into(), release.expiry.clone());
    }
    trace.insert(
        "admitted".into(),
        json!(
            tagged
                .iter()
                .filter(|tagged| tagged.admitted && tagged.staged == StagedName::Ours)
                .map(|tagged| tagged.event.position.event_identity.clone())
                .collect::<Vec<_>>()
        ),
    );

    // The summary laterals' input: the name's admitted events, restricted to the selected
    // lifecycle key for an ENSv2 selection (build.sql:386, :397, :448, :507, :642, :666).
    let in_scope: Vec<&Tagged<'_>> = tagged
        .iter()
        .filter(|tagged| {
            tagged.staged == StagedName::Ours
                && tagged.admitted
                && (!is_v2
                    || (tagged.event.is_v2_family()
                        && tagged.key.is_some()
                        && tagged.key == selected_key))
        })
        .collect();

    let grant = latest(
        in_scope
            .iter()
            .filter(|tagged| tagged.event.event_kind == "RegistrationGranted"),
        |tagged| &tagged.event.position,
    )
    .map(|tagged| tagged.event);
    let registered_at = grant.map_or(Value::Null, |grant| registered_at(facts, grant));

    let wrapper_row = event_resource.and_then(|resource| facts.wrappers.get(resource));
    let effective = wrapper_row.map(|row| effective_wrapper(row, clock.timestamp_seconds));
    let owner_lapsed = effective
        .as_ref()
        .is_some_and(|wrapper| wrapper.owner_lapsed);
    // A wrapped ENSv1 name with no registrar lease expires with its NameWrapper entry
    // (build.sql:47-53, :120-123).
    let wrapper_fallback = wrapper_row
        .filter(|row| row.wrapper_state.is_some() && !is_v2)
        .and_then(servable_expiry);

    let expiry_seconds = expiry_candidate(&in_scope);
    trace.insert("expiry_candidate".into(), json!(expiry_seconds));
    let selected_expiry = || {
        selected
            .event
            .map_or(Value::Null, |event| event.expiry.clone())
    };
    let registration_expiry = if mismatch {
        selected_expiry()
    } else if let Some(seconds) = expiry_seconds {
        json!(seconds)
    } else if is_v2 {
        selected_expiry()
    } else {
        wrapper_fallback.map_or(Value::Null, |seconds| json!(seconds))
    };

    let registrant = registrant(
        &authority,
        &tagged,
        &in_scope,
        is_v2,
        selected_key.as_deref(),
    );
    let context = authority_context(facts, &authority, &in_scope, is_v2, selected_key.as_deref());
    trace.insert("authority_key_stored".into(), json!(context.key_stored));
    trace.insert("authority_context_event".into(), context.event.clone());
    let latest_event_kind =
        latest_event_kind(facts, &selected, &in_scope, is_v2, selected_key.as_deref());

    let selected_kind = selected.kind();
    let released_at = selected
        .event
        .map_or(Value::Null, |event| event.released_at.clone());
    let status = if selection.ownerless_registry {
        json!("unregistered")
    } else {
        match selected_kind {
            Some("RegistrationReleased") => json!("released"),
            Some("RegistrationReserved") => json!("reserved"),
            Some("RegistrationGranted" | "RegistrationRenewed") => json!("active"),
            _ if binding_resource.is_some() => json!("active"),
            _ => Value::Null,
        }
    };
    let resource_id = if is_v2 {
        Value::Null
    } else {
        opt_text(registrar_resource(facts, selected_resource))
    };
    let mut registration = Map::new();
    registration.insert("status".into(), status);
    registration.insert("authority_kind".into(), context.kind);
    registration.insert("authority_key".into(), context.key);
    registration.insert("resource_id".into(), resource_id);
    registration.insert(
        "registrant".into(),
        if owner_lapsed {
            Value::Null
        } else {
            opt_text(registrant.as_deref())
        },
    );
    registration.insert("expiry".into(), registration_expiry);
    registration.insert("registered_at".into(), registered_at);
    registration.insert("released_at".into(), released_at.clone());
    registration.insert(
        "latest_event_kind".into(),
        opt_text(latest_event_kind.as_deref()),
    );
    let v2_release = selected_kind == Some("RegistrationReleased")
        && selection.authority_arm.as_deref() == Some("ens_v2");
    if selection.ownerless_registry {
        for field in ["authority_kind", "authority_key", "registrant", "expiry"] {
            registration.insert(field.into(), Value::Null);
        }
    } else if selection.released_tombstone {
        registration.insert("authority_kind".into(), Value::Null);
        registration.insert("authority_key".into(), Value::Null);
        registration.insert("registrant".into(), Value::Null);
        registration.insert(
            "lapsed_registration".into(),
            json!({"registrant": registrant, "released_at": released_at}),
        );
    } else if v2_release {
        registration.insert("authority_kind".into(), Value::Null);
        registration.insert("authority_key".into(), Value::Null);
        registration.insert("registrant".into(), Value::Null);
        let path = selected
            .event
            .is_some_and(|event| event.source_event.as_deref() == Some("RegistryPathExpired"));
        if !path {
            registration.insert("expiry".into(), Value::Null);
        }
    }

    let control_unsupported = event_resource
        .and_then(|resource| facts.resource_authority_kinds.get(resource))
        .map(String::as_str)
        .or_else(|| grant.map(|grant| grant.authority_kind.as_str()))
        .is_some_and(|kind| matches!(kind, "wrapper" | "name_wrapper"));
    let mut control = Map::new();
    if selection.ownerless_registry || selection.released_tombstone || v2_release {
        control.insert("status".into(), json!("unregistered"));
    } else if control_unsupported {
        control.insert("status".into(), json!("unsupported"));
        control.insert(
            "unsupported_reason".into(),
            json!("ENSv1 wrapper effective control is not yet projected"),
        );
    } else {
        let status = if selected_kind == Some("RegistrationReserved") {
            selected.event.and_then(|event| event.status.clone())
        } else {
            latest(
                in_scope
                    .iter()
                    .filter(|tagged| tagged.event.status.is_some()),
                |tagged| &tagged.event.position,
            )
            .and_then(|tagged| tagged.event.status.clone())
            .or_else(|| selected.event.and_then(|event| event.status.clone()))
        };
        let seconds = expiry_seconds.or(wrapper_fallback);
        control.insert("status".into(), opt_text(status.as_deref()));
        control.insert(
            "expiry".into(),
            seconds
                .filter(|seconds| (0..=253_402_300_799).contains(seconds))
                .map_or(Value::Null, |seconds| json!(format_utc(seconds))),
        );
        control.insert(
            "registrant".into(),
            if owner_lapsed {
                Value::Null
            } else {
                opt_text(registrant.as_deref())
            },
        );
        let (owner, kind) =
            control_owner(facts, &authority, &in_scope, is_v2, selected_key.as_deref());
        control.insert("registry_owner".into(), opt_text(owner.as_deref()));
        control.insert("latest_event_kind".into(), opt_text(kind.as_deref()));
    }
    ShadowName {
        registration,
        control,
        trace,
    }
}

/// The ENSv2 registration candidate (build.sql:319-348): one candidate per lifecycle key from
/// the merged maxima, then the cross-key preference, then the released-elsewhere exclusion.
fn select_v2<'a>(
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
    // Events on the name's keys that were not emitted for the name: the key state counts them,
    // today's name-scoped membership does not.
    let foreign: Vec<String> = tagged
        .iter()
        .filter(|tagged| {
            tagged.event.is_v2_family()
                && tagged.event.original_logical_name_id.as_deref() != Some(name)
                && tagged
                    .key
                    .as_ref()
                    .is_some_and(|key| keys.binary_search(key).is_ok())
        })
        .map(|tagged| tagged.event.position.event_identity.clone())
        .collect();
    if !foreign.is_empty() {
        trace.insert("foreign_members".into(), json!(foreign));
    }
    let mut candidates: Vec<(String, Candidate, &'a LifecycleEvent)> = Vec::new();
    for key in keys {
        let view = merged_for(facts, &key);
        let Some(candidate) = view::candidate(&view) else {
            continue;
        };
        // The candidate event itself, for its payload, looked up by the key and not by the name:
        // the key state counts every event on the resource (design:40, decoder rule 1), so the
        // interpreter's path-expiry release, which names the resource and no name
        // (adapters v2_registry/expiry.rs:58-59), is the key's candidate when it is the latest.
        // An expired or released ENSv2 registration stays ENSv2 and is served unregistered; it
        // never falls back to an ENSv1 lease (Tate's ruling on TYR-36 step 3). On chain a
        // registration is over once its expiry has passed, and unregistering sets the expiry to
        // the current time.
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L36 @ ens_v2@a971bd64)
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L206 @ ens_v2@a971bd64)
        // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
        // Today's name-scoped membership (build.sql:322) never sees the release, a served-side
        // bug the harness records.
        let event = tagged.iter().find(|tagged| {
            tagged.event.position.event_identity == candidate.position.event_identity
                && tagged.key.as_deref() == Some(key.as_str())
        });
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
    );
    let Some((key, candidate, event)) = winner.map(|index| &candidates[index]) else {
        return Selected {
            event: None,
            lifecycle_key: None,
        };
    };
    let released_elsewhere = candidate.event_kind == "RegistrationReleased"
        && binding_resource.is_some()
        && event.resource_id.as_deref() != binding_resource;
    if released_elsewhere {
        return Selected {
            event: None,
            lifecycle_key: None,
        };
    }
    Selected {
        event: Some(event),
        lifecycle_key: Some(key.clone()),
    }
}

/// The merged view of one ENSv2 lifecycle key: a resource's key state with the summaries of the
/// triples associated with it, or an unassociated triple's summary alone.
pub(super) fn merged_for(facts: &NameFacts, key: &str) -> MergedView {
    let associated = facts
        .triples
        .iter()
        .filter(|triple| triple.target.as_deref() == Some(key))
        .map(|triple| &triple.maxima);
    if let Some(state) = facts.key_states.get(key) {
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
