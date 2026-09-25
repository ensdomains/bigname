//! The summary laterals of name_current/build.sql, each restated over the admitted retained
//! events of the name (or of the selected ENSv2 lifecycle key) and ordered by the canonical
//! order.
use serde_json::{Value, json};

use super::{
    NameFacts,
    admission::{Authority, Probe, REGISTRAR, StagedName},
    served::{Selected, Tagged, latest, merged_for},
};
use crate::families::control::{
    position::Position,
    rows::{LifecycleEvent, Mark},
};

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
pub(super) fn expiry_candidate(in_scope: &[&Tagged<'_>]) -> Option<i64> {
    latest(
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
    latest(candidates, |tagged| &tagged.event.position).and_then(|tagged| value(tagged.event))
}

/// The authority kind and key the registration serves (build.sql:393-420).
pub(super) struct AuthorityContext {
    pub(super) kind: Value,
    pub(super) key: Value,
    /// Whether the family place that would hold the winning event's authority key exists: the
    /// retained row's `authority_key` column or the `last_grant` member for a grant, the start
    /// position's member for an AuthorityEpochChanged, the binding candidate's column for a
    /// registry-only SurfaceBound. It says whether the place exists, not whether the value is
    /// null. Step 2 at b218b2fc has none of the three, so it is false there.
    pub(super) key_stored: bool,
    /// The identity of the winning event, for the harness.
    pub(super) event: Value,
}

/// The latest admitted grant, AuthorityEpochChanged or state-derived registry-only SurfaceBound
/// (build.sql:393-420). Grants read their retained authority kind; the AuthorityEpochChanged is
/// F1's latest per arm and the SurfaceBound is the binding candidate. A successor lease granted
/// under a registry-only binding's handoff names the registration, not the authority, and is
/// left out (build.sql:405-416). Step 2 at b218b2fc writes the predecessor as the lease
/// (crates/project/src/families/identity.rs:378-383) where the served fold takes the successor
/// (name_authority/stage.rs:60, :81-119), so this exclusion cannot fire until step 2 folds the
/// successor lease.
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
    let mut found: Vec<(Position, Value, Option<Value>)> = in_scope
        .iter()
        .filter(|tagged| {
            tagged.event.event_kind == "RegistrationGranted" && !successor_lease(tagged.event)
        })
        .map(|tagged| {
            (
                tagged.event.position.clone(),
                json!(tagged.event.authority_kind),
                grant_authority_key(facts, tagged.event),
            )
        })
        .collect();
    for (position, kind, key) in admitted_epochs(facts, authority, is_v2, selected_key) {
        found.push((position, kind, key));
    }
    for candidate in &facts.candidates {
        if candidate.state_derived != Some(true)
            || candidate.authority_kind.as_deref() != Some("registry_only")
        {
            continue;
        }
        let Some(position) = &candidate.surface_bound_position else {
            continue;
        };
        if is_v2 && Some(candidate.resource_id.as_str()) != selected_key {
            continue;
        }
        let probe = Probe {
            event_kind: "SurfaceBound",
            source_family: "registry_only_binding",
            resource_id: Some(&candidate.resource_id),
            authority_kind: "registry_only",
            position,
            transaction_hash: None,
            to_address: None,
            namehash: None,
            wrapper_linked: false,
        };
        if authority.admits(&probe) {
            let key = candidate
                .authority_key_stored
                .then(|| json!(candidate.authority_key));
            found.push((position.clone(), json!("registry_only"), key));
        }
    }
    match latest(found, |(position, _, _)| position) {
        Some((position, kind, key)) => AuthorityContext {
            kind,
            key_stored: key.is_some(),
            key: key.unwrap_or(Value::Null),
            event: json!(position.event_identity),
        },
        None => AuthorityContext {
            kind: Value::Null,
            key: Value::Null,
            key_stored: true,
            event: Value::Null,
        },
    }
}

/// A retained grant's authority key: its own column, else the `last_grant` member of the key
/// state or triple summary that holds the same grant; None when neither place exists.
fn grant_authority_key(facts: &NameFacts, grant: &LifecycleEvent) -> Option<Value> {
    if grant.authority_key_stored {
        return Some(json!(grant.authority_key));
    }
    facts
        .key_states
        .values()
        .chain(facts.triples.iter().map(|triple| &triple.maxima))
        .filter_map(|maxima| maxima.last_grant.as_ref())
        .find(|mark| mark.position == grant.position)
        .and_then(|mark| mark.detail.get("authority_key").cloned())
}

/// The name's AuthorityEpochChanged events the admission holds, from F1's latest per arm
/// (`project_name_state.authority_start_positions`): position, authority kind and, when F1 keeps
/// it, authority key. F1 keeps one epoch per arm, so an older admitted epoch behind a later one
/// of the same arm is not seen.
pub(super) fn admitted_epochs(
    facts: &NameFacts,
    authority: &Authority<'_>,
    is_v2: bool,
    selected_key: Option<&str>,
) -> Vec<(Position, Value, Option<Value>)> {
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
            wrapper_linked: false,
        };
        if authority.admits(&probe) {
            found.push((
                position.clone(),
                start.get("authority_kind").cloned().unwrap_or(Value::Null),
                start.get("authority_key").cloned(),
            ));
        }
    }
    found
}

/// `latest_event_kind` (build.sql:58-63): a selected reservation serves its own kind; an ENSv2
/// selection serves the latest of the five kinds in the selected key's membership; an ENSv1 one
/// the latest admitted of the five kinds.
pub(super) fn latest_event_kind(
    facts: &NameFacts,
    selected: &Selected<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> Option<String> {
    if selected.kind() == Some("RegistrationReserved") {
        return selected.kind().map(str::to_owned);
    }
    let found = if is_v2 {
        selected_key.and_then(|key| {
            let view = merged_for(facts, key);
            let marks: [(&Option<Mark>, &str); 5] = [
                (&view.last_grant, "RegistrationGranted"),
                (&view.last_renewal, "RegistrationRenewed"),
                (&view.last_release_any, "RegistrationReleased"),
                (&view.last_reservation, "RegistrationReserved"),
                (&view.last_expiry_changed, "ExpiryChanged"),
            ];
            latest(
                marks
                    .into_iter()
                    .filter_map(|(mark, kind)| mark.as_ref().map(|mark| (mark, kind))),
                |(mark, _)| &mark.position,
            )
            .map(|(_, kind)| kind.to_owned())
        })
    } else {
        latest(
            in_scope.iter().filter(|tagged| {
                matches!(
                    tagged.event.event_kind.as_str(),
                    "RegistrationGranted"
                        | "RegistrationRenewed"
                        | "RegistrationReleased"
                        | "RegistrationReserved"
                        | "ExpiryChanged"
                )
            }),
            |tagged| &tagged.event.position,
        )
        .map(|tagged| tagged.event.event_kind.clone())
    };
    found.or_else(|| selected.kind().map(str::to_owned))
}

/// The registration's registrar lease: the lease a NameWrapper SurfaceBound on the selected
/// event's resource recorded, else that resource (build.sql:348-360).
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
        .max_by(|left, right| left.order().cmp(&right.order()))
        .and_then(|candidate| candidate.wrapped_registrar_resource_id.as_deref());
    Some(wrapped.unwrap_or(resource))
}

/// The control block's registry owner and latest kind (build.sql:649-694), from what the
/// families keep: the latest admitted ENSv2 transfer or registrar snapshot grant, and the F2c
/// node's latest owner for an ENSv1 or Basenames name. F2c keeps the owner without the position
/// of the AuthorityTransferred that set it, and no family keeps an AuthorityEpochChanged's owner,
/// so this part is an approximation the harness reports separately.
pub(super) fn control_owner(
    facts: &NameFacts,
    authority: &Authority<'_>,
    in_scope: &[&Tagged<'_>],
    is_v2: bool,
    selected_key: Option<&str>,
) -> (Option<String>, Option<String>) {
    let mut owners: Vec<(Position, Option<String>, &str)> = Vec::new();
    let mut kinds: Vec<(Position, &str)> = Vec::new();
    for tagged in in_scope {
        let event = tagged.event;
        let masked = event.owner_word_unmasked == Some(true);
        if event.event_kind == "TokenControlTransferred" {
            kinds.push((event.position.clone(), "TokenControlTransferred"));
            if is_v2 {
                let owner = if masked {
                    None
                } else {
                    event.to_address.clone()
                };
                owners.push((event.position.clone(), owner, "TokenControlTransferred"));
            }
        }
        if event.event_kind == "RegistrationGranted"
            && event.state_derived == Some(true)
            && event.registrar_surface_snapshot == Some(true)
        {
            let owner = if masked {
                None
            } else {
                event.owner_getter.clone()
            };
            owners.push((event.position.clone(), owner, "RegistrationGranted"));
        }
    }
    if !is_v2
        && let Some(node) = &facts.registry_node
        && let Some(position) = &node.position
    {
        let owner = if node.owner_word_unmasked == Some(true) {
            None
        } else {
            node.registry_owner.clone().or_else(|| node.owner.clone())
        };
        owners.push((position.clone(), owner, "AuthorityTransferred"));
        kinds.push((position.clone(), "AuthorityTransferred"));
    }
    // An AuthorityEpochChanged decides the kind; F1 keeps no owner for it.
    for (position, _, _) in admitted_epochs(facts, authority, is_v2, selected_key) {
        kinds.push((position, "AuthorityEpochChanged"));
    }
    let owner = latest(owners, |(position, _, _)| position).and_then(|(_, owner, _)| owner);
    let kind = latest(kinds, |(position, _)| position).map(|(_, kind)| kind.to_owned());
    (owner, kind)
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
