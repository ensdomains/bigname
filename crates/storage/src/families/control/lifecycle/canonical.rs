//! Registration schedules are retained independently of current control and resolution.
//! Only canonical lifecycle evidence changes a selected schedule; crossing its expiry never
//! switches a post-cutover name back to a different registration's dates.
use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use super::{Clock, NameFacts, NamePlace, admission::StagedName, expiry::Grace, served::Tagged};
use crate::{UnixSeconds, families::control::rows::LifecycleEvent};

struct Instance<'a> {
    origin: &'a LifecycleEvent,
    latest: &'a LifecycleEvent,
    expiry: Value,
    terminal: Option<&'a LifecycleEvent>,
    reserved: bool,
    attached: bool,
}

/// Retain one instance per admitted registry/token/resource key. The origin is distinct from the
/// public registration handle. Unnamed changes belong only while the key still names this name;
/// another name's association cannot renew or terminate its predecessor's retained instance.
fn v2_instance<'a>(
    facts: &NameFacts,
    tagged: &[Tagged<'a>],
    clock: &Clock,
) -> Option<Instance<'a>> {
    let mut keys: BTreeMap<&str, Vec<&Tagged<'a>>> = BTreeMap::new();
    for item in tagged.iter().filter(|item| item.event.is_v2_family()) {
        if let Some(key) = item.key.as_deref() {
            keys.entry(key).or_default().push(item);
        }
    }
    let mut instances = Vec::new();
    for (key, mut events) in keys {
        events.sort_by(|a, b| a.event.position.cmp(&b.event.position));
        let mut current: Option<Instance<'a>> = None;
        for item in events {
            let event = item.event;
            if item.staged == StagedName::Other {
                if let Some(instance) = &mut current {
                    instance.attached = false;
                }
                continue;
            }
            let own = item.staged == StagedName::Ours;
            let kind = event.event_kind.as_str();
            let starts = own && matches!(kind, "RegistrationGranted" | "RegistrationReserved");
            if starts {
                let claim = kind == "RegistrationGranted"
                    && current
                        .as_ref()
                        .is_some_and(|instance| instance.reserved && instance.terminal.is_none());
                let explicit_origin = matches!(
                    event.source_event.as_deref(),
                    Some("LabelRegistered" | "LabelReserved")
                ) && event.derived_from.as_deref() != Some("registry_state");
                // Topology reassertions can emit a reserved/granted snapshot after renewal or
                // token regeneration. Only an actual label allocation starts a new instance.
                if current.is_none() || (!claim && explicit_origin) {
                    current = Some(Instance {
                        origin: event,
                        latest: event,
                        expiry: event.expiry.clone(),
                        terminal: None,
                        reserved: kind == "RegistrationReserved",
                        attached: true,
                    });
                    continue;
                }
            }
            let Some(instance) = &mut current else {
                continue;
            };
            if !instance.attached && !starts {
                continue;
            }
            if starts {
                instance.attached = true;
                instance.reserved = kind == "RegistrationReserved";
            }
            instance.latest = event;
            match kind {
                "RegistrationReleased"
                    if event.source_event.as_deref() != Some("RegistryPathExpired") =>
                {
                    instance.terminal = Some(event);
                }
                "RegistrationGranted"
                | "RegistrationReserved"
                | "RegistrationRenewed"
                | "ExpiryChanged" => {
                    // LabelUnregistered writes a shorter registry expiry, but that is terminal
                    // evidence, not the ended registration's previously scheduled deadline.
                    if event.source_event.as_deref() != Some("LabelUnregistered")
                        && !event.expiry.is_null()
                    {
                        instance.expiry = event.expiry.clone();
                    }
                    if starts || event.revived_from_expiry == Some(true) {
                        instance.terminal = None;
                    }
                }
                _ => {}
            }
        }
        if let Some(instance) = current {
            instances.push((key, instance));
        }
    }
    let binding = facts
        .input
        .selection
        .resource_id
        .as_deref()
        .filter(|resource| {
            facts.candidates.iter().any(|candidate| {
                candidate.resource_id == *resource && candidate.open_at(clock.timestamp_seconds)
            })
        });
    instances
        .into_iter()
        .max_by(|(a_key, a), (b_key, b)| {
            (binding == Some(*a_key) && a.terminal.is_none())
                .cmp(&(binding == Some(*b_key) && b.terminal.is_none()))
                .then_with(|| a.origin.position.cmp(&b.origin.position))
                .then_with(|| a.latest.position.cmp(&b.latest.position))
        })
        .map(|(_, instance)| instance)
}

#[derive(Clone, Copy)]
enum Boundary {
    Exclusive,
    InclusiveGrace,
    InclusiveExpiry,
}

/// One pure policy result, evaluated only at the publication timestamp. Exact arithmetic includes
/// uint64 expiries beyond the achievable chain clock; those need no i64 invalidation timer.
fn schedule(
    expiry: Option<UnixSeconds>,
    grace: Grace,
    boundary: Boundary,
    terminal: bool,
    allocated: bool,
    clock: i64,
) -> (&'static str, Option<i64>) {
    if terminal {
        return ("released", None);
    }
    if !allocated {
        return ("unregistered", None);
    }
    let Some(expiry) = expiry else {
        return ("active", None);
    };
    let now = UnixSeconds::from_seconds(i128::from(clock)).expect("i64 clock fits");
    let end = expiry
        .checked_add_seconds(grace.seconds())
        .expect("uint64 expiry plus grace fits");
    let start = match boundary {
        Boundary::InclusiveExpiry => expiry.checked_add_seconds(1).unwrap(),
        _ => expiry,
    };
    let release = match boundary {
        Boundary::InclusiveGrace => end.checked_add_seconds(1).unwrap(),
        Boundary::InclusiveExpiry => start,
        _ => end,
    };
    let next = |date: UnixSeconds| i64::try_from(date.unix_timestamp()).ok();
    if now < start {
        ("active", next(start))
    } else if now < release {
        ("expired", next(release))
    } else {
        ("released", None)
    }
}

pub(super) fn apply(
    facts: &NameFacts,
    tagged: &[Tagged<'_>],
    clock: &Clock,
    registration: &mut Map<String, Value>,
    trace: &mut Map<String, Value>,
) -> anyhow::Result<()> {
    let canonical = v2_instance(facts, tagged, clock)
        .filter(|_| facts.input.selection.is_v2() || facts.resolution_cutover);
    let mut terminal = false;
    // An authority-kind hint or a binding alone is not an observed allocation. During a
    // historical rebuild a later authority can already have a resource identity while its
    // first grant is still above this publication. Wrapper/registry-only custody has its own
    // allocation evidence; registrar schedules require a selected lifecycle fact.
    let selected_event = trace.get("selected_event").and_then(Value::as_str);
    let allocation_evidence = tagged.iter().any(|item| {
        Some(item.event.position.event_identity.as_str()) == selected_event
            && matches!(
                item.event.event_kind.as_str(),
                "RegistrationGranted"
                    | "RegistrationReserved"
                    | "RegistrationRenewed"
                    | "RegistrationReleased"
            )
    }) || registration.get("authority_kind").and_then(Value::as_str)
        == Some("registry_only")
        || registration.get("authority_kind").and_then(Value::as_str) == Some("wrapper")
            && facts
                .input
                .selection
                .resource_id
                .as_deref()
                .and_then(|resource| facts.wrappers.get(resource))
                .is_some_and(|wrapper| wrapper.wrapper_state.is_some());
    let mut allocated = allocation_evidence
        && registration
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status != "unregistered");
    let wrapper_only = registration.get("ens_v1_expiry").is_none_or(Value::is_null)
        && registration.get("authority_kind").and_then(Value::as_str) == Some("wrapper");
    let (grace, boundary) = if let Some(instance) = canonical {
        terminal = instance.terminal.is_some();
        allocated = true;
        // A replacement reservation, including one already expired when emitted, does not
        // inherit a former holder from the older ENSv2 tombstone selected for control.
        let selected = trace.get("selected_event").and_then(Value::as_str);
        if tagged.iter().any(|item| {
            Some(item.event.position.event_identity.as_str()) == selected
                && item.event.is_v2_family()
                && item.event.position < instance.origin.position
        }) {
            registration.remove("lapsed_registration");
        }
        registration.insert("expiry".into(), instance.expiry);
        registration.remove("expires_at_reason");
        super::expiry::classify_expiry(
            registration,
            false,
            instance.origin.source_family == "ens_v2_root_l1",
            &facts.input.namehash,
        )?;
        trace.insert(
            "canonical_registration_origin".into(),
            json!({
                "state_key": instance.origin.state_key,
                "event_identity": instance.origin.position.event_identity,
                "source_family": instance.origin.source_family,
                "terminal_event": instance.terminal.map(|event| &event.position.event_identity),
            }),
        );
        if let Some(event) = instance.terminal
            && event.source_event.as_deref() == Some("LabelUnregistered")
        {
            let released_at = facts.block_seconds.get(&event.position.block_number);
            // Preserve independently established historical holder metadata, if any.
            let lapsed = registration
                .entry("lapsed_registration")
                .or_insert_with(|| json!({}));
            if let Some(lapsed) = lapsed.as_object_mut() {
                lapsed.insert("release_kind".into(), json!("unregistered"));
                lapsed.insert("released_at".into(), json!(released_at));
            }
        }
        let grace = if facts.input.place == NamePlace::EthSecondLevel
            && super::policy::ens_v2_grace(facts, instance.origin)
        {
            Grace::EnsV2
        } else {
            Grace::None
        };
        (grace, Boundary::Exclusive)
    } else if wrapper_only {
        (Grace::None, Boundary::InclusiveExpiry)
    } else if registration.get("authority_kind").and_then(Value::as_str) == Some("ens_v2_registry")
    {
        (Grace::None, Boundary::Exclusive)
    } else {
        match facts.input.place {
            NamePlace::EthSecondLevel => (Grace::EnsV1, Boundary::InclusiveGrace),
            NamePlace::BasenamesSecondLevel => (Grace::Basenames, Boundary::InclusiveGrace),
            _ => (Grace::None, Boundary::Exclusive),
        }
    };
    let expiry = registration.get("expiry").and_then(UnixSeconds::from_json);
    // Absence is not a perpetual-registration policy. Registry-only allocations have no
    // expiry concept; explicit contract sentinels were classified above. Everything else
    // needs an actual date, including a retained ended registration.
    let no_expiry = registration
        .get("expires_at_reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| matches!(reason, "no_expiry" | "not_set"))
        || registration.get("authority_kind").and_then(Value::as_str) == Some("registry_only")
        || facts.candidates.iter().any(|candidate| {
            candidate.registry_only
                && Some(candidate.resource_id.as_str())
                    == facts.input.selection.resource_id.as_deref()
        });
    anyhow::ensure!(
        !allocated || expiry.is_some() || no_expiry,
        "allocated registration is missing its required expiry: {}",
        facts.input.logical_name_id
    );
    registration.insert(
        "grace_ends_at".into(),
        super::expiry::grace_ends_at(registration.get("expiry"), grace),
    );
    let (status, next) = schedule(
        expiry,
        grace,
        boundary,
        terminal,
        allocated,
        clock.timestamp_seconds,
    );
    registration.insert("lifecycle_status".into(), json!(status));
    registration.insert("lifecycle_recompose_at".into(), json!(next));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protocol_equality_and_full_uint64_schedules_are_exact() {
        let e = UnixSeconds::from_seconds(100).unwrap();
        for (grace, boundary, g, equal) in [
            (Grace::EnsV1, Boundary::InclusiveGrace, 7_776_100, "expired"),
            (
                Grace::Basenames,
                Boundary::InclusiveGrace,
                7_776_100,
                "expired",
            ),
            (Grace::EnsV2, Boundary::Exclusive, 2_419_300, "released"),
        ] {
            for (t, expected) in [
                (99, "active"),
                (100, "expired"),
                (101, "expired"),
                (g - 1, "expired"),
                (g, equal),
                (g + 1, "released"),
            ] {
                assert_eq!(
                    schedule(Some(e), grace, boundary, false, true, t).0,
                    expected
                );
            }
        }
        for (boundary, equal) in [
            (Boundary::Exclusive, "released"),
            (Boundary::InclusiveExpiry, "active"),
        ] {
            assert_eq!(
                schedule(Some(e), Grace::None, boundary, false, true, 100).0,
                equal
            );
            assert_eq!(
                schedule(Some(e), Grace::None, boundary, false, true, 101).0,
                "released"
            );
        }
        assert_eq!(
            schedule(
                UnixSeconds::from_seconds(u64::MAX as i128),
                Grace::EnsV2,
                Boundary::Exclusive,
                false,
                true,
                i64::MAX
            ),
            ("active", None)
        );
        assert_eq!(
            schedule(None, Grace::None, Boundary::Exclusive, false, true, 100),
            ("active", None)
        );
        assert_eq!(
            schedule(None, Grace::None, Boundary::Exclusive, false, false, 100),
            ("unregistered", None)
        );
        assert_eq!(
            schedule(Some(e), Grace::EnsV2, Boundary::Exclusive, true, true, 0),
            ("released", None)
        );
    }
}
