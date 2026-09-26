//! The summary laterals of name_current/build.sql, each restated over the admitted retained
//! events of the name (or of the selected ENSv2 lifecycle key) and ordered by the facts'
//! order, the canonical order in every read.
use serde_json::{Value, json};

use super::{
    NameFacts,
    admission::{Authority, Probe, REGISTRAR, StagedName},
    membership::{expired_when_written, members, merged_for},
    served::{Selected, Tagged, latest},
};
use crate::families::control::{
    position::{EventOrder, Position},
    rows::{BindingCandidate, LifecycleEvent, Mark},
};

/// The five kinds `latest_event_kind` reads (build.sql:58-63).
const FIVE_KINDS: [&str; 5] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "RegistrationReserved",
    "ExpiryChanged",
];

/// The registration time of the latest admitted grant: its block's timestamp, or a registrar
/// snapshot's own registration time (build.sql:374-380).
pub(super) fn registered_at(facts: &NameFacts, grant: &LifecycleEvent) -> Value {
    let snapshot = grant.source_family == REGISTRAR
        && grant.state_derived == Some(true)
        && grant.surface_materialization == Some(true)
        && grant.registrar_surface_snapshot == Some(true);
    if snapshot {
        return grant
            .original_registered_at
            .and_then(|seconds| facts.snapshot_timestamps.get(&seconds).cloned())
            .unwrap_or(Value::Null);
    }
    facts
        .block_timestamps
        .get(&grant.position.block_number)
        .cloned()
        .unwrap_or(Value::Null)
}

/// The expiry lateral (build.sql:496-533): the latest admitted grant, or renewal, release or
/// ExpiryChanged with a JSON-number expiry, leaving out the wrapper's ExpiryChanged; its
/// converted seconds, null for a grant without a numeric expiry.
pub(super) fn expiry_candidate(order: &EventOrder, in_scope: &[&Tagged<'_>]) -> Option<i64> {
    latest(
        order,
        in_scope.iter().filter(|tagged| {
            let event = tagged.event;
            let wrapper_expiry = event.event_kind == "ExpiryChanged"
                && (event.source_family == "ens_v1_wrapper_l1"
                    || (event.source_family == REGISTRAR
                        && event.source_event.as_deref() == Some("NameRenewed")
                        && event.authority_kind == "wrapper"));
            matches!(
                event.event_kind.as_str(),
                "RegistrationGranted"
                    | "RegistrationRenewed"
                    | "RegistrationReleased"
                    | "ExpiryChanged"
            ) && !wrapper_expiry
                && (event.event_kind == "RegistrationGranted" || event.numeric_expiry())
        }),
        |tagged| &tagged.event.position,
    )
    .and_then(|tagged| tagged.event.expiry_seconds)
}

/// The registrant (build.sql:440-495 over registration_events.sql): the latest admitted grant,
/// release or transfer, plus the registry-only lease's transfers after the handoff, without the
/// custody transfer into the wrapper and without a registrar release of a lease a wrapper of the
/// name stands for; a release names its before-state registrant.
pub(super) fn registrant(
    order: &EventOrder,
    authority: &Authority<'_>,
    tagged: &[Tagged<'_>],
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> Option<String> {
    let selection = authority.selection;
    let handoff = authority.registry_only_binding().filter(|_| {
        selection.unsupported_reason.is_none()
            && selection.authority_arm.as_deref() == Some("ens_v1")
    });
    let after_handoff = |tagged: &&Tagged<'_>| {
        let event = tagged.event;
        !tagged.admitted
            && tagged.staged == StagedName::Ours
            && event.event_kind == "TokenControlTransferred"
            && event.source_family == REGISTRAR
            && event.authority_kind == "registrar"
            && handoff.is_some_and(|binding| {
                let (block, transaction, log, _) = binding.order();
                binding.lease_resource_id.is_some()
                    && binding.lease_resource_id == event.resource_id
                    && event.position.bound() > (block, transaction, log)
            })
            && (!is_v2 || (event.is_v2_family() && tagged.key.as_deref() == selected_key))
    };
    let admitted_grants: Vec<&LifecycleEvent> = tagged
        .iter()
        .filter(|tagged| {
            tagged.admitted
                && tagged.event.event_kind == "RegistrationGranted"
                && tagged.event.source_family == REGISTRAR
        })
        .map(|tagged| tagged.event)
        .collect();
    let custody = |event: &LifecycleEvent| {
        event.event_kind == "TokenControlTransferred"
            && event.source_family == REGISTRAR
            && authority.candidates.iter().any(|wrapper| {
                wrapper.is_wrapper()
                    && authority.admits_wrapper_binding(wrapper)
                    && wrapper.transaction_hash.is_some()
                    && wrapper.transaction_hash == event.transaction_hash
                    && wrapper.wrapped_registrar_resource_id == event.resource_id
                    && event.to_address.is_some()
                    && event.to_address == wrapper.emitting_address
                    && admitted_grants.iter().any(|grant| {
                        grant.resource_id == event.resource_id
                            && grant.transaction_hash != wrapper.transaction_hash
                    })
            })
    };
    let released_under_wrapper = |event: &LifecycleEvent| {
        event.event_kind == "RegistrationReleased"
            && event.source_family == REGISTRAR
            && authority.candidates.iter().any(|wrapper| {
                wrapper.is_wrapper()
                    && (wrapper.wrapped_registrar_resource_id.is_some()
                        && wrapper.wrapped_registrar_resource_id == event.resource_id
                        || authority.events.iter().any(|registration| {
                            registration.resource_id == event.resource_id
                                && registration.source_family == REGISTRAR
                                && registration.event_kind == "RegistrationGranted"
                                && authority.staged_name(registration) == StagedName::Ours
                                && registration.transaction_hash.is_some()
                                && registration.transaction_hash == wrapper.transaction_hash
                        }))
            })
    };
    let value = |event: &LifecycleEvent| match event.event_kind.as_str() {
        "TokenControlTransferred" => event.to_address.clone(),
        "RegistrationReleased" => event.before_registrant.clone(),
        _ => event.registrant.clone(),
    };
    let candidates = in_scope
        .iter()
        .copied()
        .filter(|tagged| {
            matches!(
                tagged.event.event_kind.as_str(),
                "RegistrationGranted" | "RegistrationReleased" | "TokenControlTransferred"
            )
        })
        .chain(tagged.iter().filter(after_handoff))
        .filter(|tagged| {
            !custody(tagged.event)
                && !released_under_wrapper(tagged.event)
                && value(tagged.event).is_some()
        });
    latest(order, candidates, |tagged| &tagged.event.position)
        .and_then(|tagged| value(tagged.event))
}

/// The authority kind and key the registration serves (build.sql:393-420).
pub(super) struct AuthorityContext {
    pub(super) kind: Value,
    pub(super) key: Value,
    /// The identity of the winning event, for the harness.
    pub(super) event: Value,
}

/// The name's state-derived registry-only SurfaceBounds the admission holds, as their binding
/// candidates (the SurfaceBound arm of build.sql:396-399 and :664-665).
pub(super) fn admitted_registry_only<'a>(
    facts: &'a NameFacts,
    authority: &Authority<'_>,
    is_v2: bool,
    selected_key: Option<&str>,
) -> Vec<(&'a Position, &'a BindingCandidate)> {
    facts
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.state_derived == Some(true)
                && candidate.authority_kind.as_deref() == Some("registry_only")
                && (!is_v2 || Some(candidate.resource_id.as_str()) == selected_key)
        })
        .filter_map(|candidate| {
            let position = candidate.surface_bound_position.as_ref()?;
            authority
                .admits(&Probe {
                    event_kind: "SurfaceBound",
                    source_family: "registry_only_binding",
                    resource_id: Some(&candidate.resource_id),
                    authority_kind: "registry_only",
                    position,
                    transaction_hash: None,
                    to_address: None,
                    namehash: None,
                })
                .then_some((position, candidate))
        })
        .collect()
}

/// The latest admitted grant, AuthorityEpochChanged or state-derived registry-only SurfaceBound
/// (build.sql:393-420), with the authority kind and key its after-state carries: the retained
/// grant's columns, F1's latest AuthorityEpochChanged per arm, the binding candidate's
/// SurfaceBound. A successor lease granted under a registry-only binding's handoff names the
/// registration, not the authority, and is left out (build.sql:405-416); step 2 folds the
/// successor as the handoff's lease (`lease_resource_id`, name_authority/stage.rs:60, :81-119).
pub(super) fn authority_context(
    facts: &NameFacts,
    authority: &Authority<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> AuthorityContext {
    let successor_lease = |event: &LifecycleEvent| {
        authority.binding.is_some_and(|binding| {
            binding.registry_only
                && binding.lease_resource_id.is_some()
                && binding.lease_resource_id == event.resource_id
                && binding.lease_resource_id != binding.predecessor_resource_id
        })
    };
    let mut found: Vec<(Position, Value, Value)> = in_scope
        .iter()
        .filter(|tagged| {
            tagged.event.event_kind == "RegistrationGranted" && !successor_lease(tagged.event)
        })
        .map(|tagged| {
            (
                tagged.event.position.clone(),
                json!(tagged.event.authority_kind_raw),
                json!(tagged.event.authority_key),
            )
        })
        .collect();
    for epoch in admitted_epochs(facts, authority, is_v2, selected_key) {
        found.push((epoch.position, epoch.kind, epoch.key));
    }
    for (position, candidate) in admitted_registry_only(facts, authority, is_v2, selected_key) {
        found.push((
            position.clone(),
            json!("registry_only"),
            json!(candidate.authority_key),
        ));
    }
    match latest(&facts.order, found, |(position, _, _)| position) {
        Some((position, kind, key)) => AuthorityContext {
            kind,
            key,
            event: json!(position.event_identity),
        },
        None => AuthorityContext {
            kind: Value::Null,
            key: Value::Null,
            event: Value::Null,
        },
    }
}

/// One admitted AuthorityEpochChanged: its position, and the authority kind, key and control
/// owner its after-state carries.
pub(super) struct Epoch {
    pub(super) position: Position,
    pub(super) kind: Value,
    pub(super) key: Value,
    pub(super) owner: Option<String>,
}

/// The name's AuthorityEpochChanged events the admission holds, from F1's latest per arm
/// (`project_name_state.authority_start_positions`). F1 keeps one epoch per arm, so an older
/// admitted epoch behind a later one of the same arm is not seen.
pub(super) fn admitted_epochs(
    facts: &NameFacts,
    authority: &Authority<'_>,
    is_v2: bool,
    selected_key: Option<&str>,
) -> Vec<Epoch> {
    let mut found = Vec::new();
    let Some(starts) = facts.authority_starts.as_object() else {
        return found;
    };
    for (arm, start) in starts {
        let Some(position) = Position::from_json(start) else {
            continue;
        };
        let resource = start.get("resource_id").and_then(Value::as_str);
        if is_v2 && (arm != "ens_v2" || resource != selected_key) {
            continue;
        }
        // A stand-in family of the arm, so the null-resource rule reads the right arm.
        let family = format!("{arm}_epoch");
        let probe = Probe {
            event_kind: "AuthorityEpochChanged",
            source_family: &family,
            resource_id: resource,
            authority_kind: "registrar",
            position: &position,
            transaction_hash: None,
            to_address: None,
            namehash: None,
        };
        if authority.admits(&probe) {
            let member = |name: &str| start.get(name).cloned().unwrap_or(Value::Null);
            found.push(Epoch {
                kind: member("authority_kind"),
                key: member("authority_key"),
                owner: start
                    .get("owner")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                position,
            });
        }
    }
    found
}

/// `latest_event_kind`, the CASE of build.sql:67-73 in order: a selected reservation serves its
/// own kind; so does a released tombstone's deciding fact (`is_released_v2`); another ENSv2
/// selection serves the latest of the five kinds in the selected key's membership, else its own
/// kind; an ENSv1 one the latest admitted of the five kinds, else its own.
pub(super) fn latest_event_kind(
    facts: &NameFacts,
    selected: &Selected<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> Option<String> {
    if selected.kind() == Some("RegistrationReserved") || selected.released {
        return selected.kind().map(str::to_owned);
    }
    let found = if is_v2 {
        selected_key.and_then(|key| {
            let name = &facts.input.logical_name_id;
            if facts.order != EventOrder::Canonical {
                // The counterfactual reads the key's members themselves in today's lateral order
                // (build.sql:366-371), not the maxima folded in its membership order.
                return latest(
                    &facts.order,
                    members(facts, key, name).into_iter().filter(|event| {
                        FIVE_KINDS.contains(&event.event_kind.as_str())
                            && !expired_when_written(facts, event)
                    }),
                    |event| &event.position,
                )
                .map(|event| event.event_kind.clone());
            }
            let view = merged_for(facts, key, name);
            let marks: [(&Option<Mark>, &str); 5] = [
                (&view.last_grant, "RegistrationGranted"),
                (&view.last_renewal, "RegistrationRenewed"),
                (&view.last_release_any, "RegistrationReleased"),
                (&view.last_reservation, "RegistrationReserved"),
                (&view.last_expiry_changed, "ExpiryChanged"),
            ];
            latest(
                &facts.order,
                marks
                    .into_iter()
                    .filter_map(|(mark, kind)| mark.as_ref().map(|mark| (mark, kind))),
                |(mark, _)| &mark.position,
            )
            .map(|(_, kind)| kind.to_owned())
        })
    } else {
        latest(
            &facts.order,
            in_scope
                .iter()
                .filter(|tagged| FIVE_KINDS.contains(&tagged.event.event_kind.as_str())),
            |tagged| &tagged.event.position,
        )
        .map(|tagged| tagged.event.event_kind.clone())
    };
    found.or_else(|| selected.kind().map(str::to_owned))
}

/// The registration's registrar lease: the lease the latest NameWrapper SurfaceBound on the
/// selected event's resource recorded, else that resource (build.sql:348-360). Today's builder
/// takes that SurfaceBound by block and generated id, so the candidates compare by their
/// SurfaceBound positions in the read's order: the canonical order in a read, block and
/// generated id in the same-block counterfactual. A candidate without a SurfaceBound position
/// stands at its own place with the identity `binding:<id>`, as step 2 places it
/// (families/identity.rs `binding_position`).
pub(super) fn registrar_resource<'a>(
    facts: &'a NameFacts,
    resource: Option<&'a str>,
) -> Option<&'a str> {
    let resource = resource?;
    let wrapped = facts
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.is_wrapper()
                && candidate.resource_id == resource
                && candidate.wrapped_registrar_resource_id.is_some()
        })
        .map(|candidate| {
            let position = candidate
                .surface_bound_position
                .clone()
                .unwrap_or_else(|| Position {
                    block_number: candidate.block_number,
                    transaction_index: candidate.transaction_index,
                    log_index: candidate.log_index,
                    event_identity: format!("binding:{}", candidate.surface_binding_id),
                });
            (position, candidate)
        })
        .max_by(|(left, _), (right, _)| facts.order.membership(left, right))
        .and_then(|(_, candidate)| candidate.wrapped_registrar_resource_id.as_deref());
    Some(wrapped.unwrap_or(resource))
}

/// `to_char(to_timestamp(seconds) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')` for
/// seconds in 0..=253402300799.
pub(crate) fn format_utc(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let rest = seconds.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let day_of_year = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

#[cfg(test)]
mod tests {
    use super::format_utc;

    #[test]
    fn utc_formatting_matches_to_char() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc(1_800_000_000), "2027-01-15T08:00:00Z");
        assert_eq!(format_utc(253_402_300_799), "9999-12-31T23:59:59Z");
        assert_eq!(format_utc(951_782_400), "2000-02-29T00:00:00Z");
    }
}
