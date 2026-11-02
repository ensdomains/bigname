//! A reservation already expired when written (`membership::expired_when_written`) is never live.
//! When it is the name's own label allocation it ends the instance on its key, so the reservation
//! it replaced no longer dates the name. The replacement keeps the key's position but dates
//! nothing, until a later live allocation or a revival of its entry. A registry-state snapshot that
//! is never live changes nothing.
use serde_json::{Value, json};

use super::{Instance, StagedName, Tagged, v2_instance};
use crate::families::control::rows::LifecycleEvent;

const KEY: &str = "ens-v2:0xregistry:1:resource";
const NAME: &str = "ens:0xname";

fn event(
    identity: &str,
    block: i64,
    kind: &str,
    name: Option<&str>,
    after: Value,
) -> LifecycleEvent {
    let mut row = json!({
        "state_kind": "resource",
        "state_key": KEY,
        "block_number": block,
        "transaction_index": 0,
        "log_index": 0,
        "event_identity": identity,
        "event_kind": kind,
        "original_logical_name_id": name,
        "decoded_logical_name_id": name,
        "source_family": "ens_v2_registry_l1",
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(after.as_object().expect("after is an object").clone());
    LifecycleEvent::from_row(&row).expect("lifecycle row decodes")
}

fn reserved(identity: &str, block: i64, expiry: i64) -> LifecycleEvent {
    event(
        identity,
        block,
        "RegistrationReserved",
        Some(NAME),
        json!({"source_event": "LabelReserved", "expiry": expiry}),
    )
}

/// The instance `v2_instance` selects for `NAME` over events on their own keys, with `never_live`
/// naming the identities of the reservations already expired when written.
fn select<'a>(events: &'a [(LifecycleEvent, &str)], never_live: &[&str]) -> Option<Instance<'a>> {
    let tagged: Vec<Tagged<'_>> = events
        .iter()
        .map(|(event, key)| Tagged {
            event,
            staged: match event.original_logical_name_id.as_deref() {
                Some(NAME) => StagedName::Ours,
                Some(_) => StagedName::Other,
                None => StagedName::Unnamed,
            },
            admitted: true,
            key: Some((*key).to_owned()),
        })
        .collect();
    v2_instance(&tagged, |event| {
        never_live.contains(&event.position.event_identity.as_str())
    })
}

/// `events` on `KEY`.
fn on_key(events: &[LifecycleEvent]) -> Vec<(LifecycleEvent, &'static str)> {
    events.iter().map(|event| (event.clone(), KEY)).collect()
}

/// The expiry of the instance that dates `NAME`, with all `events` on `KEY`. A dateless instance
/// dates nothing, as `apply` treats it.
fn fold(events: &[LifecycleEvent], never_live: &[&str]) -> Option<Value> {
    let events = on_key(events);
    select(&events, never_live)
        .filter(|instance| !instance.dateless)
        .map(|instance| instance.expiry)
}

/// The origin identity, expiry and dateless flag of the instance `v2_instance` selects.
fn selected(
    events: &[(LifecycleEvent, &str)],
    never_live: &[&str],
) -> Option<(String, Value, bool)> {
    select(events, never_live).map(|instance| {
        (
            instance.origin.position.event_identity.clone(),
            instance.expiry,
            instance.dateless,
        )
    })
}

#[test]
fn a_never_live_label_reservation_ends_the_instance_it_replaces() {
    let events = [reserved("live", 1, 100), reserved("replacement", 3, 2)];
    assert_eq!(fold(&events, &[]), Some(json!(2)));
    assert_eq!(
        fold(&events, &["replacement"]),
        None,
        "the replacement supplies no dates and the live reservation it replaced none either"
    );
}

#[test]
fn a_never_live_registry_state_snapshot_changes_nothing() {
    let snapshot = event(
        "snapshot",
        3,
        "RegistrationReserved",
        Some(NAME),
        json!({"source_event": "ExpiryUpdated", "derived_from": "registry_state", "expiry": 2}),
    );
    let events = [reserved("live", 1, 100), snapshot];
    assert_eq!(fold(&events, &["snapshot"]), Some(json!(100)));
}

#[test]
fn another_names_never_live_reservation_ends_nothing() {
    let other = event(
        "other",
        3,
        "RegistrationReserved",
        Some("ens:0xother"),
        json!({"source_event": "LabelReserved", "expiry": 2}),
    );
    let events = [reserved("live", 1, 100), other];
    assert_eq!(fold(&events, &["other"]), Some(json!(100)));
}

#[test]
fn a_live_reservation_after_the_never_live_one_opens_a_new_instance() {
    let events = [
        reserved("live", 1, 100),
        reserved("replacement", 3, 2),
        reserved("again", 5, 500),
    ];
    assert_eq!(fold(&events, &["replacement"]), Some(json!(500)));
}

/// The ENSv2 registry `B` the root moved `.eth` to. `KEY` is the earlier registry `A`.
const B_KEY: &str = "ens-v2:0xregistry-b:1:resource";

fn revival(
    identity: &str,
    block: i64,
    kind: &str,
    name: Option<&str>,
    expiry: i64,
) -> LifecycleEvent {
    event(
        identity,
        block,
        kind,
        name,
        json!({"source_event": "ExpiryUpdated", "revived_from_expiry": true, "expiry": expiry}),
    )
}

#[test]
fn a_never_live_replacement_outranks_an_older_keys_release() {
    let moved = event(
        "a-moved",
        2,
        "RegistrationReleased",
        Some(NAME),
        json!({"source_event": "SubregistryUpdated",
               "terminal_reason": "registry_name_binding_changed"}),
    );
    let events = [
        (reserved("a", 1, 100), KEY),
        (moved, KEY),
        (reserved("b", 3, 100), B_KEY),
        (reserved("replacement", 5, 2), B_KEY),
    ];
    let winner = select(&events, &["replacement"]).expect("an instance is selected");
    assert_eq!(
        winner.origin.position.event_identity, "replacement",
        "the replacement keeps B's position over A's older release"
    );
    assert!(winner.dateless && winner.terminal.is_none() && winner.expiry.is_null());
}

#[test]
fn a_live_start_over_a_dateless_instance_opens_a_new_one() {
    let snapshot = event(
        "snapshot",
        5,
        "RegistrationReserved",
        Some(NAME),
        json!({"source_event": "ExpiryUpdated", "derived_from": "registry_state", "expiry": 500}),
    );
    let registered = event(
        "registered",
        5,
        "RegistrationGranted",
        Some(NAME),
        json!({"source_event": "LabelRegistered", "expiry": 600}),
    );
    for (start, expiry) in [(snapshot, 500), (registered, 600)] {
        let identity = start.position.event_identity.clone();
        let events = on_key(&[
            reserved("live", 1, 100),
            reserved("replacement", 3, 2),
            start,
        ]);
        assert_eq!(
            selected(&events, &["replacement"]),
            Some((identity, json!(expiry), false))
        );
    }
}

#[test]
fn a_revival_dates_a_dateless_instance_again() {
    let named = revival("revived", 5, "ExpiryChanged", Some(NAME), 500);
    let unnamed = revival("renewed", 5, "RegistrationRenewed", None, 600);
    for (change, expiry) in [(named, 500), (unnamed, 600)] {
        let events = on_key(&[
            reserved("live", 1, 100),
            reserved("replacement", 3, 2),
            change,
        ]);
        assert_eq!(
            selected(&events, &["replacement"]),
            Some(("replacement".to_owned(), json!(expiry), false))
        );
    }
}

#[test]
fn a_change_without_revival_leaves_the_instance_dateless() {
    let passed = event(
        "passed",
        5,
        "ExpiryChanged",
        Some(NAME),
        json!({"source_event": "ExpiryUpdated", "revived_from_expiry": false, "expiry": 3}),
    );
    let events = [
        reserved("live", 1, 100),
        reserved("replacement", 3, 2),
        passed,
    ];
    assert_eq!(fold(&events, &["replacement"]), None);
}

#[test]
fn a_registry_state_start_over_a_detached_dateless_instance_opens_a_new_one() {
    let other = event(
        "other",
        4,
        "RegistrationGranted",
        Some("ens:0xother"),
        json!({"source_event": "LabelRegistered", "expiry": 300}),
    );
    let snapshot = event(
        "snapshot",
        5,
        "RegistrationReserved",
        Some(NAME),
        json!({"source_event": "ExpiryUpdated", "derived_from": "registry_state", "expiry": 500}),
    );
    let events = on_key(&[
        reserved("live", 1, 100),
        reserved("replacement", 3, 2),
        other,
        snapshot,
    ]);
    assert_eq!(
        selected(&events, &["replacement"]),
        Some(("snapshot".to_owned(), json!(500), false)),
        "a snapshot opens no instance over a dated one, so only the dateless rule opens this one"
    );
}
