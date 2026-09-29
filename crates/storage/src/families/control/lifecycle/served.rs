//! The served registration and control values of one name, from its loaded facts. Each function
//! derives one part of the composed registration or control block from the retained events;
//! every "latest" is
//! the latest under the facts' order, the canonical order in every read.
use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::{Map, Value, json};

use super::{
    Clock, NameFacts, ShadowName,
    admission::{Authority, Probe, StagedName},
    control::{control_owner, served_owner},
    expiry::{choose, classify_expiry, grace_ends_at, live_entry},
    laterals::{
        authority_context, expiry_candidate, latest_event_kind, registered_at, registrant,
        registrar_resource,
    },
    select::select_v2,
    tombstone::deciding_fact,
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
    /// The ENSv2 lifecycle key, for an ENSv2-family event.
    pub(super) key: Option<String>,
}

/// The registration the name serves: the event whose kind and payload it
/// serves, its lifecycle key, the resource it serves the registration on, which for a released
/// tombstone is the tombstone's and not the deciding event's own, and whether it is a released
/// tombstone's deciding fact (`is_released_v2`).
pub(super) struct Selected<'a> {
    pub(super) event: Option<&'a LifecycleEvent>,
    pub(super) lifecycle_key: Option<String>,
    pub(super) resource: Option<String>,
    pub(super) released: bool,
}

impl Selected<'_> {
    pub(super) fn none() -> Self {
        Self {
            event: None,
            lifecycle_key: None,
            resource: None,
            released: false,
        }
    }

    pub(super) fn kind(&self) -> Option<&str> {
        self.event.map(|event| event.event_kind.as_str())
    }
}

/// The latest item in the canonical order.
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

/// The admission inputs of one name: its selection, binding candidates, selected binding,
/// wrapper modifier and retained events.
pub(super) fn authority_of(facts: &NameFacts) -> Authority<'_> {
    let selection = &facts.input.selection;
    Authority {
        name: &facts.input.logical_name_id,
        selection,
        candidates: &facts.candidates,
        lease_candidates: &facts.lease_candidates,
        binding: selection.surface_binding_id.as_deref().and_then(|id| {
            facts
                .candidates
                .iter()
                .find(|candidate| candidate.surface_binding_id == id)
        }),
        wrapper_modifier: selection
            .resource_id
            .as_deref()
            .and_then(|resource| facts.wrappers.get(resource))
            .is_some_and(|wrapper| wrapper.has_modifier),
        events: &facts.events,
    }
}

/// Every retained event of the name with its staged name, admission and ENSv2 lifecycle key.
fn tag<'a>(facts: &'a NameFacts, authority: &Authority<'a>) -> Vec<Tagged<'a>> {
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
    facts
        .events
        .iter()
        .map(|event| {
            let key = event
                .is_v2_family()
                .then(|| match event.state_kind.as_str() {
                    "triple" => triple_targets.get(&event.state_key).cloned().flatten(),
                    _ => event.resource_id.clone(),
                });
            Tagged {
                event,
                staged: authority.staged_name(event),
                admitted: authority.admits(&Probe::of(event)),
                key: key.flatten(),
            }
        })
        .collect()
}

/// The name's live ENSv2 registry entry (`expiry::live_entry`), whatever arm is selected.
pub(super) fn live_ens_v2_entry(facts: &NameFacts) -> Result<Option<String>> {
    let authority = authority_of(facts);
    let tagged = tag(facts, &authority);
    Ok(live_entry(facts, &tagged)?.map(|entry| entry.key))
}

pub(super) fn evaluate(facts: &NameFacts, clock: &Clock) -> Result<ShadowName> {
    let input = &facts.input;
    let selection = &input.selection;
    let is_v2 = selection.is_v2();
    let authority = authority_of(facts);
    let binding = authority.binding;
    let binding_resource = binding.map(|binding| binding.resource_id.as_str());
    let tagged = tag(facts, &authority);

    let mut trace = Map::new();
    // A released ENSv2 tombstone serves the fact that decided it, on the tombstone's resource;
    // every other ENSv2 name the registration fold.
    let tombstone = deciding_fact(facts, clock)?;
    trace.insert("released_v2".into(), json!(tombstone.is_some()));
    let selected = if let Some(tombstone) = tombstone {
        Selected {
            event: Some(tombstone.event),
            lifecycle_key: Some(tombstone.resource.clone()),
            resource: Some(tombstone.resource),
            released: true,
        }
    } else if is_v2 {
        select_v2(facts, &tagged, binding_resource, &mut trace)?
    } else {
        let event = latest(
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
        .map(|tagged| tagged.event);
        Selected {
            event,
            lifecycle_key: None,
            resource: event.and_then(|event| event.resource_id.clone()),
            released: false,
        }
    };
    let has_lifecycle = is_v2 && selected.event.is_some();
    let selected_resource = selected.resource.as_deref();
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
    trace.insert("event_resource".into(), opt_text(event_resource));
    trace.insert("selected_kind".into(), opt_text(selected.kind()));
    // A release emitted without a name that the registration fold selected: the path-expiry
    // release the interpreter synthesises. The trace records whether one was selected, and its
    // times. A released tombstone's deciding fact is not one.
    let unnamed_release = selected.event.filter(|event| {
        !selected.released
            && event.original_logical_name_id.is_none()
            && event.event_kind == "RegistrationReleased"
    });
    trace.insert(
        "selected_unnamed_path_expiry".into(),
        json!(unnamed_release.is_some_and(LifecycleEvent::is_path_expiry)),
    );
    if let Some(release) = unnamed_release {
        trace.insert("selected_released_at".into(), release.released_at.clone());
        trace.insert("selected_expiry".into(), release.expiry.clone());
    }
    let staged: Map<String, Value> = facts
        .events
        .iter()
        .filter_map(|event| {
            let pass = authority.staged_by(event)?;
            Some((
                event.position.event_identity.clone(),
                json!(format!("{pass:?}")),
            ))
        })
        .collect();
    if !staged.is_empty() {
        trace.insert("staged".into(), Value::Object(staged));
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
    // lifecycle key for an ENSv2 selection.
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
    // A wrapped ENSv1 name with no registrar lease expires with its NameWrapper entry.
    let wrapper_fallback = wrapper_row
        .filter(|row| row.wrapper_state.is_some() && !is_v2)
        .and_then(servable_expiry);

    let selected_kind = selected.kind();
    let expiry_seconds = expiry_candidate(&in_scope);
    trace.insert("expiry_candidate".into(), json!(expiry_seconds));
    let selected_expiry = || {
        selected
            .event
            .map_or(Value::Null, |event| event.expiry.clone())
    };
    // The registration expiry, branch by branch. First, an ENSv2
    // path-expiry release serves its own expiry, and the expiry lateral only when the release
    // carries none: a renewal after the path was cut is written without a
    // name, so the name's expiry rows can be older than the release. Then, with an identity
    // mismatch, the selected event's own expiry. Otherwise the expiry lateral
    // (`expiry_candidate`: the latest admitted grant of the name on the selected key, or its
    // latest admitted renewal, release or ExpiryChanged with a numeric expiry), else the selected
    // ENSv2 event's own expiry, else for ENSv1 the NameWrapper expiry.
    let path_release = is_v2
        && selected_kind == Some("RegistrationReleased")
        && selected
            .event
            .is_some_and(|event| event.source_event.as_deref() == Some("RegistryPathExpired"));
    let registration_expiry = if path_release {
        match selected_expiry() {
            Value::Null => expiry_seconds.map_or(Value::Null, |seconds| json!(seconds)),
            own => own,
        }
    } else if mismatch {
        selected_expiry()
    } else if let Some(seconds) = expiry_seconds {
        json!(seconds)
    } else if is_v2 {
        selected_expiry()
    } else {
        wrapper_fallback.map_or(Value::Null, |seconds| json!(seconds))
    };
    // The expiry and renewal grace the name serves: after the Universal Resolver cutover a live
    // ENSv2 entry's, whatever arm holds authority (`expiry::choose`).
    let entry = live_entry(facts, &tagged)?;
    let (registration_expiry, grace) = choose(
        facts,
        is_v2,
        registration_expiry,
        entry.as_ref(),
        &mut trace,
    );

    let registrant = registrant(
        &authority,
        &tagged,
        &in_scope,
        is_v2,
        selected_key.as_deref(),
    );
    let (registrant, registrant_position) = match registrant {
        Some((registrant, position)) => (
            Some(registrant),
            json!({
                "block_number": position.block_number,
                "transaction_index": position.transaction_index,
                "log_index": position.log_index,
                "event_identity": position.event_identity,
            }),
        ),
        None => (None, Value::Null),
    };
    trace.insert("registrant_position".into(), registrant_position);
    let context = authority_context(facts, &authority, &in_scope, is_v2, selected_key.as_deref());
    trace.insert("authority_context_event".into(), context.event.clone());
    trace.insert("authority_context_kind".into(), context.kind.clone());
    let latest_event_kind =
        latest_event_kind(facts, &selected, &in_scope, is_v2, selected_key.as_deref())?;

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
    // One resolved arm decides both the selection and its presentation: a missing arm reads as
    // ENSv2, so a release it selects is presented as an ENSv2 release. Comparing the raw arm
    // with 'ens_v2' instead would not present it, because a missing arm fails that comparison;
    // the trace records that difference under its own cause.
    let v2_release = selected_kind == Some("RegistrationReleased") && is_v2;
    let mut unreleased = None;
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
            json!({"registrant": registrant, "released_at": released_at,
                   "release_kind": "expired"}),
        );
    } else if v2_release {
        unreleased = Some(registration.clone());
        registration.insert("authority_kind".into(), Value::Null);
        registration.insert("authority_key".into(), Value::Null);
        registration.insert("registrant".into(), Value::Null);
        let path = selected
            .event
            .is_some_and(|event| event.source_event.as_deref() == Some("RegistryPathExpired"));
        if !path {
            registration.insert("expiry".into(), Value::Null);
            registration.insert("expires_at_reason".into(), json!("released"));
        }
        // The registry token's holder when the registration ended (TYR-63): the latest grant,
        // transfer or release registrant on the key. A path-expired registration stays
        // renewable by it through the grace period; an unregistered one does not.
        registration.insert(
            "lapsed_registration".into(),
            json!({"registrant": registrant, "released_at": released_at,
                   "held_through": "registry",
                   "release_kind": if path { "expired" } else { "unregistered" }}),
        );
    }

    classify_expiry(
        &mut registration,
        !is_v2 && expiry_seconds.is_none() && wrapper_fallback.is_some(),
        selected
            .event
            .is_some_and(|event| event.source_family == "ens_v2_root_l1"),
        &facts.input.namehash,
    )?;

    registration.insert(
        "grace_ends_at".into(),
        grace_ends_at(registration.get("expiry"), grace),
    );

    let (folded, owner_kind) =
        control_owner(facts, &authority, &in_scope, is_v2, selected_key.as_deref());
    // An unwrapped ENSv1 or Basenames registration on its lease or registry record has a
    // registry owner on chain; `served_owner` never serves it as absent.
    let owner_required = !is_v2
        && matches!(
            selection.authority_arm.as_deref(),
            Some("ens_v1" | "basenames")
        )
        && registration.get("status") == Some(&json!("active"))
        && matches!(
            registration.get("authority_kind").and_then(Value::as_str),
            Some("registrar" | "registry_only")
        );
    let serves_control =
        !(selection.ownerless_registry || selection.released_tombstone || v2_release);
    let owner = served_owner(facts, folded, owner_required && serves_control)?;
    // A wrapper grant's control is built like any other grant's.
    let live_control = || {
        let mut control = Map::new();
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
        control.insert("status".into(), opt_text(status.as_deref()));
        control.insert(
            "expiry".into(),
            registration.get("expiry").cloned().unwrap_or(Value::Null),
        );
        control.insert(
            "registrant".into(),
            if owner_lapsed {
                Value::Null
            } else {
                opt_text(registrant.as_deref())
            },
        );
        // An expired NameWrapper entry with PARENT_CANNOT_CONTROL burned has no owner
        // (`owner_lapsed`), as it has no registrant.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
        control.insert(
            "registry_owner".into(),
            if owner_lapsed {
                Value::Null
            } else {
                opt_text(owner.as_deref())
            },
        );
        control.insert("latest_event_kind".into(), opt_text(owner_kind.as_deref()));
        control
    };
    let control = if serves_control {
        live_control()
    } else {
        Map::from_iter([("status".to_owned(), json!("unregistered"))])
    };
    // With no selected arm, a presentation that compared the raw arm would not clear the
    // release; the trace records what that would have served.
    if let Some(unreleased) = unreleased.filter(|_| selection.authority_arm.is_none()) {
        trace.insert(
            "raw_arm_presentation".into(),
            json!({"registration": unreleased, "control": live_control()}),
        );
    }
    Ok(ShadowName {
        registration,
        control,
        trace,
    })
}
