use serde_json::{Value, json};

use super::{Space, derive};
use crate::families::input::{BlockEvent, Position, order};

const RESOURCE: &str = "00000000-0000-0000-0000-00000000000a";

fn event(kind: &str, family: &str, after: Value) -> BlockEvent {
    BlockEvent {
        normalized_event_id: 1,
        position: Position {
            block_number: 10,
            transaction_index: Some(0),
            log_index: Some(0),
            event_identity: "fixture:1".to_owned(),
        },
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: kind.to_owned(),
        source_family: family.to_owned(),
        source_manifest_id: None,
        transaction_hash: Some("0xtx".to_owned()),
        before: json!({}),
        after,
        raw_fact_ref: json!({"emitting_address": "0x00000000000000000000000000000000000000A4"}),
    }
}

#[test]
fn a_resource_pointer_without_a_name_owns_its_node_and_resource_but_no_name() {
    let mut pointer = event(
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"node": "0xAB", "resolver": "0x00000000000000000000000000000000000000B1"}),
    );
    pointer.resource_id = Some(RESOURCE.to_owned());
    let keys = derive(&[pointer]);
    assert!(keys.contains(Space::RegistryPointer, &["ens", "0xab"]));
    assert!(keys.contains(Space::Resource, &[RESOURCE]));
    assert!(keys.contains(
        Space::Resolver,
        &["0x00000000000000000000000000000000000000b1"]
    ));
    assert_eq!(
        keys.of(Space::Name).count(),
        0,
        "no name key without a name"
    );
}

#[test]
fn a_named_lifecycle_event_without_a_resource_owns_the_name_and_its_triple() {
    let mut release = event(
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"source_event": "RegistryPathExpired", "token_id": "7",
               "registry_contract_instance_id": "00000000-0000-0000-0000-000000000001"}),
    );
    release.logical_name_id = Some("ens:0xname".to_owned());
    let keys = derive(&[release]);
    assert!(keys.contains(Space::Name, &["ens", "ens:0xname"]));
    assert!(keys.contains(
        Space::Triple,
        &["ens:0xname", "00000000-0000-0000-0000-000000000001", "7"]
    ));
    assert_eq!(keys.of(Space::Resource).count(), 0);
}

#[test]
fn a_child_addressed_subregistry_change_owns_the_child_and_the_parent_node() {
    let edge = event(
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({"node": "0xPARENT", "child_node": "0xCHILD", "labelhash": "0x01"}),
    );
    let keys = derive(&[edge]);
    assert!(keys.contains(Space::ChildEdge, &["ens", "0xchild"]));
    assert!(keys.contains(Space::RegistryNode, &["ens", "0xchild"]));
    assert!(keys.contains(Space::RegistryNode, &["ens", "0xparent"]));
}

#[test]
fn a_before_only_resolver_and_node_are_owned_like_the_after_ones() {
    let mut pointer = event(
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"node": "0xnew", "resolver": "0x00000000000000000000000000000000000000b2"}),
    );
    pointer.before =
        json!({"node": "0xold", "resolver": "0x00000000000000000000000000000000000000b1"});
    let keys = derive(&[pointer]);
    for resolver in [
        "0x00000000000000000000000000000000000000b1",
        "0x00000000000000000000000000000000000000b2",
    ] {
        assert!(keys.contains(Space::Resolver, &[resolver]));
    }
    assert!(keys.contains(Space::RegistryPointer, &["ens", "0xold"]));
    assert!(keys.contains(Space::RegistryPointer, &["ens", "0xnew"]));
}

#[test]
fn a_reverse_change_owns_both_its_before_and_after_tuples() {
    let mut reverse = event(
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({"address": "0xAA", "coin_type": "60", "namespace": "ens"}),
    );
    reverse.before = json!({"address": "0xBB", "coin_type": "60", "namespace": "ens"});
    let keys = derive(&[reverse]);
    assert!(keys.contains(Space::ReverseTuple, &["0xaa", "60", "ens"]));
    assert!(keys.contains(Space::ReverseTuple, &["0xbb", "60", "ens"]));
    assert_eq!(keys.of(Space::ReverseTuple).count(), 2);
}

#[test]
fn a_record_write_owns_its_storage_key_whether_or_not_it_is_named() {
    let mut named = event(
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({"node": "0xN", "resolver": "0xR", "record_key": "text:url"}),
    );
    named.logical_name_id = Some("ens:0xname".to_owned());
    let linked = event(
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({"resolver": "0xR", "storage_model": "resolver_record_id",
               "resolver_record_id": "5", "record_key": "text:url"}),
    );
    let keys = derive(&[named, linked]);
    assert!(keys.contains(Space::NodeRecord, &["0xr", "0xn"]));
    assert!(keys.contains(Space::RecordId, &["0xr", "5"]));
}

fn synthesised(identity: &str, id: i64) -> BlockEvent {
    let mut release = event("RegistrationReleased", "ens_v2_registry_l1", json!({}));
    release.normalized_event_id = id;
    release.transaction_hash = None;
    release.position.transaction_index = None;
    release.position.log_index = None;
    release.position.event_identity = identity.to_owned();
    release
}

#[test]
fn synthesised_events_sort_first_and_among_themselves_by_their_identity_text() {
    let mut logged = event("RegistrationGranted", "ens_v2_registry_l1", json!({}));
    logged.normalized_event_id = 1;
    logged.position.event_identity = "a:log".to_owned();
    let mut events = vec![
        logged,
        synthesised("ens_v2:1:c:0xb:RegistrationReleased:expiry:r:1:9", 2),
        synthesised("ens_v2:1:c:0xb:RegistrationReleased:expiry:r:1:10", 3),
    ];
    assert_eq!(order(&mut events), 0);
    let identities = events
        .iter()
        .map(|event| event.position.event_identity.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        identities,
        [
            "ens_v2:1:c:0xb:RegistrationReleased:expiry:r:1:10",
            "ens_v2:1:c:0xb:RegistrationReleased:expiry:r:1:9",
            "a:log",
        ]
    );
}

#[test]
fn one_event_delivered_twice_under_different_generated_ids_is_applied_once() {
    let first = event("RegistrationGranted", "ens_v2_registry_l1", json!({}));
    let mut second = first.clone();
    second.normalized_event_id = 2;
    let mut events = vec![second, first];
    assert_eq!(order(&mut events), 0, "identical deliveries are no anomaly");
    assert_eq!(events.len(), 1);
}

#[test]
fn disagreeing_deliveries_of_one_identity_keep_the_first_in_order_and_count_an_anomaly() {
    let mut early = event("RegistrationGranted", "ens_v2_registry_l1", json!({"expiry": 1}));
    early.normalized_event_id = 9;
    let mut late = early.clone();
    late.normalized_event_id = 1;
    late.position.log_index = Some(5);
    late.after = json!({"expiry": 2});
    let mut between = event("ExpiryChanged", "ens_v2_registry_l1", json!({}));
    between.position.log_index = Some(3);
    between.position.event_identity = "fixture:between".to_owned();
    // Same position, different payload: the choice follows the payload text, not read order.
    let mut twin = early.clone();
    twin.after = json!({"expiry": 0});
    let mut events = vec![late, between.clone(), early.clone(), twin];
    assert_eq!(order(&mut events), 2);
    let kept = events
        .iter()
        .map(|event| (event.position.event_identity.as_str(), event.after.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        kept,
        [("fixture:1", json!({"expiry": 0})), ("fixture:between", json!({}))],
        "the generated ids 9 and 1 play no part"
    );
}
