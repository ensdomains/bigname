//! A block-boundary move writes the old path's release and the new path's grant on one key with
//! no transaction or log index. Their identities then decide the order, and the grant sorts
//! first. The release ends the old name's path. It must not detach the instance the grant just
//! started, or the new name ignores every later unnamed change on the key.
use serde_json::{Value, json};

use super::{Instance, StagedName, Tagged, v2_instance};
use crate::families::control::rows::LifecycleEvent;

const KEY: &str = "ens-v2:0xregistry:1:resource";
const BOUNDARY: &str = "ens_v2_registry_resource_surface:1:1:0xboundary";

fn event(
    identity: &str,
    block: i64,
    indexed: bool,
    kind: &str,
    name: Option<&str>,
    after: Value,
) -> LifecycleEvent {
    let mut row = json!({
        "state_kind": "registration",
        "state_key": KEY,
        "block_number": block,
        "transaction_index": if indexed { json!(0) } else { Value::Null },
        "log_index": if indexed { json!(0) } else { Value::Null },
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

/// The registry's history on the key: registered under the old name, then moved at the block-10
/// boundary, then one later change at block 11.
fn history(later: LifecycleEvent) -> Vec<LifecycleEvent> {
    vec![
        event(
            "ens_v2_registry_resource_surface:1:1:0xregistered:0:RegistrationGranted:0",
            1,
            true,
            "RegistrationGranted",
            Some("old"),
            json!({"source_event": "LabelRegistered", "expiry": 100}),
        ),
        event(
            &format!("{BOUNDARY}:RegistrationReleased:expiry:0xparent:7:0"),
            10,
            false,
            "RegistrationReleased",
            Some("old"),
            json!({
                "source_event": "RegistryPathExpired",
                "derived_from": "interpreter_state",
                "terminal_reason": "registry_name_binding_expired",
                "expiry": 100,
            }),
        ),
        event(
            &format!("{BOUNDARY}:RegistrationGranted:topology:0xregistry:1:0"),
            10,
            false,
            "RegistrationGranted",
            Some("new"),
            json!({
                "source_event": "RegistryPathExpired",
                "derived_from": "registry_state",
                "expiry": 100,
            }),
        ),
        later,
    ]
}

fn fold<'a>(events: &'a [LifecycleEvent], name: &str) -> Instance<'a> {
    let tagged: Vec<Tagged<'a>> = events
        .iter()
        .map(|event| Tagged {
            event,
            staged: match event.original_logical_name_id.as_deref() {
                Some(own) if own == name => StagedName::Ours,
                Some(_) => StagedName::Other,
                None => StagedName::Unnamed,
            },
            admitted: true,
            key: Some(KEY.to_owned()),
        })
        .collect();
    v2_instance(&tagged).expect("the key has an instance for the name")
}

#[test]
fn a_boundary_move_lets_a_later_renewal_date_the_moved_name() {
    let renewal = || {
        event(
            "ens_v2_registry_resource_surface:1:1:0xrenewed:0:RegistrationRenewed:0",
            11,
            true,
            "RegistrationRenewed",
            None,
            json!({"source_event": "ExpiryUpdated", "expiry": 500}),
        )
    };
    let events = history(renewal());
    // The identities alone order the two boundary events, and the grant sorts first.
    assert!(events[2].position < events[1].position);
    let moved = fold(&events, "new");
    assert_eq!(
        moved.expiry,
        json!(500),
        "the moved name follows the renewal"
    );
    assert!(moved.terminal.is_none());
    // The grant that moved the key detached the old name, so the renewal is not its own.
    let old = fold(&events, "old");
    assert_eq!(old.expiry, json!(100));
}

#[test]
fn a_boundary_move_lets_a_later_unregistration_release_the_moved_name() {
    let unregistration = event(
        "ens_v2_registry_resource_surface:1:1:0xunregistered:0:RegistrationReleased:0",
        11,
        true,
        "RegistrationReleased",
        None,
        json!({"source_event": "LabelUnregistered", "expiry": 11}),
    );
    let events = history(unregistration);
    let moved = fold(&events, "new");
    assert_eq!(
        moved
            .terminal
            .and_then(|event| event.source_event.as_deref()),
        Some("LabelUnregistered"),
        "the unregistration releases the moved name"
    );
    assert!(fold(&events, "old").terminal.is_none());
}
