//! A reservation already expired when written (`membership::expired_when_written`) is never live.
//! When it is the name's own label allocation it ends the instance on its key, so the reservation
//! it replaced no longer dates the name. A registry-state snapshot that is never live changes
//! nothing.
use serde_json::{Value, json};

use super::{StagedName, Tagged, v2_instance};
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

/// The instance `v2_instance` keeps for `NAME`, with `never_live` naming the identities of the
/// reservations already expired when written.
fn fold(events: &[LifecycleEvent], never_live: &[&str]) -> Option<Value> {
    let tagged: Vec<Tagged<'_>> = events
        .iter()
        .map(|event| Tagged {
            event,
            staged: match event.original_logical_name_id.as_deref() {
                Some(NAME) => StagedName::Ours,
                Some(_) => StagedName::Other,
                None => StagedName::Unnamed,
            },
            admitted: true,
            key: Some(KEY.to_owned()),
        })
        .collect();
    v2_instance(&tagged, |event| {
        never_live.contains(&event.position.event_identity.as_str())
    })
    .map(|instance| instance.expiry)
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
