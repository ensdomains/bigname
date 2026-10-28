//! The implied lease (`admission::ImpliedLease`) of a `.eth` second-level name whose selected
//! ENSv1 authority records no lease: which names have one, and which resource it is.
use std::{collections::BTreeMap, sync::Arc};

use serde_json::{Value, json};

use super::{
    AuthoritySelection, NameFacts, NameInput, NamePlace,
    admission::{Authority, Probe},
};
use crate::families::control::{
    registry::RegistryNode,
    rows::{BindingCandidate, LifecycleEvent},
};

const NAME: &str = "ens:0x01";
const REGISTRY_ONLY: &str = "00000000-0000-0000-0000-0000000000a0";
const LEASE: &str = "00000000-0000-0000-0000-0000000000b1";
const LATER_LEASE: &str = "00000000-0000-0000-0000-0000000000b2";
const HANDOFF: i64 = 10;

fn event(kind: &str, block: i64, resource: &str, extra: Value) -> LifecycleEvent {
    let mut row = json!({
        "state_kind": "resource",
        "state_key": resource,
        "block_number": block,
        "transaction_index": 0,
        "log_index": 1,
        "event_identity": format!("{kind}:{block}:{resource}"),
        "event_kind": kind,
        "original_logical_name_id": NAME,
        "decoded_logical_name_id": NAME,
        "resource_id": resource,
        "source_family": "ens_v1_registrar_l1",
        "authority_kind": "registrar",
        "expiry": 2_000_000_000_i64,
        "expiry_seconds": 2_000_000_000_i64,
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    LifecycleEvent::from_row(&row).expect("lifecycle row decodes")
}

/// A renewal through a controller, with the grant the adapter derives beside the first one.
fn renewed(block: i64, resource: &str) -> Vec<LifecycleEvent> {
    vec![
        event(
            "RegistrationGranted",
            block,
            resource,
            json!({"source_event": "NameRenewed"}),
        ),
        event(
            "RegistrationRenewed",
            block,
            resource,
            json!({"source_event": "NameRenewed"}),
        ),
    ]
}

fn binding(resource: &str, extra: Value) -> BindingCandidate {
    let mut row = json!({
        "surface_binding_id": "binding-1",
        "logical_name_id": NAME,
        "authority_arm": "ens_v1",
        "resource_id": resource,
        "block_number": HANDOFF,
        "transaction_index": 0,
        "log_index": 0,
        "registry_only": true,
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    BindingCandidate::from_row(&row).expect("candidate decodes")
}

struct Name {
    arm: &'static str,
    place: NamePlace,
    binding: Option<BindingCandidate>,
    ownerless: bool,
    unsupported: Option<&'static str>,
    handoff: Option<i64>,
    events: Vec<LifecycleEvent>,
}

impl Name {
    /// A migrated `.eth` name on its registry-only handoff, with no lease recorded.
    fn migrated(events: Vec<LifecycleEvent>) -> Self {
        Self {
            arm: "ens_v1",
            place: NamePlace::EthSecondLevel,
            binding: Some(binding(REGISTRY_ONLY, json!({}))),
            ownerless: false,
            unsupported: None,
            handoff: Some(HANDOFF),
            events,
        }
    }

    fn facts(self) -> NameFacts {
        let selection = AuthoritySelection {
            authority_arm: Some(self.arm.to_owned()),
            surface_binding_id: self
                .binding
                .as_ref()
                .map(|binding| binding.surface_binding_id.clone()),
            resource_id: self
                .binding
                .as_ref()
                .map(|binding| binding.resource_id.clone()),
            unsupported_reason: self.unsupported.map(str::to_owned),
            ownerless_registry: self.ownerless,
            ..AuthoritySelection::default()
        };
        NameFacts {
            input: NameInput {
                logical_name_id: NAME.to_owned(),
                namehash: "0x01".to_owned(),
                selection,
                place: self.place,
            },
            candidates: self.binding.into_iter().collect(),
            lease_candidates: Vec::new(),
            key_states: BTreeMap::new(),
            triples: Vec::new(),
            events: self.events,
            wrappers: BTreeMap::new(),
            block_timestamps: Arc::new(BTreeMap::new()),
            block_seconds: Arc::new(BTreeMap::new()),
            snapshot_timestamps: Arc::new(BTreeMap::new()),
            authority_starts: Value::Null,
            migration: None,
            registry_node: Some(RegistryNode {
                namespace: "ens".to_owned(),
                node: "0x01".to_owned(),
                first_current_record_block: self.handoff,
                ..RegistryNode::default()
            }),
            resolution_cutover: false,
            ens_v2_root: None,
            grace_registries: Arc::new(Vec::new()),
        }
    }
}

fn implied(name: Name) -> Option<String> {
    let facts = name.facts();
    Authority::of(&facts)
        .implied_lease
        .map(|lease| lease.resource.to_owned())
}

#[test]
fn a_migrated_name_renewed_after_its_handoff_implies_the_renewed_lease() {
    assert_eq!(
        implied(Name::migrated(renewed(12, LEASE))),
        Some(LEASE.to_owned())
    );
}

#[test]
fn a_release_alone_implies_its_lease() {
    let release = event("RegistrationReleased", 13, LEASE, json!({}));
    assert_eq!(
        implied(Name::migrated(vec![release])),
        Some(LEASE.to_owned())
    );
}

#[test]
fn the_latest_lease_wins_and_an_older_lease_is_not_admitted() {
    let mut events = renewed(12, LEASE);
    events.push(event("RegistrationReleased", 13, LEASE, json!({})));
    events.extend(renewed(14, LATER_LEASE));
    let facts = Name::migrated(events).facts();
    let authority = Authority::of(&facts);
    assert_eq!(authority.implied_resource(), Some(LATER_LEASE));
    let older = &facts.events[2];
    assert!(!authority.admits(&Probe::of(older)));
    assert!(authority.admits(&Probe::of(&facts.events[3])));
}

#[test]
fn no_registrar_event_implies_no_lease() {
    assert_eq!(implied(Name::migrated(Vec::new())), None);
}

#[test]
fn events_before_the_handoff_or_state_derived_imply_no_lease() {
    assert_eq!(implied(Name::migrated(renewed(9, LEASE))), None);
    let snapshot = event(
        "RegistrationGranted",
        12,
        LEASE,
        json!({"state_derived": true}),
    );
    assert_eq!(implied(Name::migrated(vec![snapshot])), None);
}

#[test]
fn another_names_event_implies_no_lease() {
    let other = event(
        "RegistrationRenewed",
        12,
        LEASE,
        json!({"original_logical_name_id": "ens:0x02"}),
    );
    assert_eq!(implied(Name::migrated(vec![other])), None);
}

#[test]
fn without_a_handoff_block_any_own_event_implies_the_lease() {
    let mut name = Name::migrated(renewed(9, LEASE));
    name.handoff = None;
    assert_eq!(implied(name), Some(LEASE.to_owned()));
}

#[test]
fn only_an_ens_v1_eth_second_level_name_has_an_implied_lease() {
    for place in [
        NamePlace::BasenamesSecondLevel,
        NamePlace::BelowEthSecondLevel("ens:0x02".to_owned()),
        NamePlace::Other,
    ] {
        let mut name = Name::migrated(renewed(12, LEASE));
        name.place = place.clone();
        assert_eq!(implied(name), None, "{place:?}");
    }
    let mut ens_v2 = Name::migrated(renewed(12, LEASE));
    ens_v2.arm = "ens_v2";
    assert_eq!(implied(ens_v2), None);
    let mut unsupported = Name::migrated(renewed(12, LEASE));
    unsupported.unsupported = Some("current_authority_not_projected");
    assert_eq!(implied(unsupported), None);
}

#[test]
fn a_binding_that_records_a_lease_or_is_not_registry_only_has_no_implied_lease() {
    // A lease recorded by a successor grant or a wrapper callback is the lease.
    let mut recorded = Name::migrated(renewed(12, LEASE));
    recorded.binding = Some(binding(
        REGISTRY_ONLY,
        json!({"lease_resource_id": LEASE, "predecessor_resource_id": LEASE}),
    ));
    assert_eq!(implied(recorded), None);
    // A lease recorded as the predecessor's registry resource carries no registrar row.
    let mut registry_lease = Name::migrated(renewed(12, LEASE));
    registry_lease.binding = Some(binding(
        REGISTRY_ONLY,
        json!({"lease_resource_id": REGISTRY_ONLY, "predecessor_resource_id": REGISTRY_ONLY}),
    ));
    assert_eq!(implied(registry_lease), Some(LEASE.to_owned()));
    // A registrar binding (or a wrapper's) selects its own lease.
    let mut registrar = Name::migrated(renewed(12, LEASE));
    registrar.binding = Some(binding(LEASE, json!({"registry_only": false})));
    assert_eq!(implied(registrar), None);
}

#[test]
fn a_name_with_no_binding_has_no_implied_lease_even_on_an_ownerless_record() {
    // The ownerless-registry profile is unregistered whatever its lease (docs/projections.md).
    for ownerless in [true, false] {
        let mut name = Name::migrated(renewed(12, LEASE));
        name.binding = None;
        name.ownerless = ownerless;
        assert_eq!(implied(name), None, "ownerless {ownerless}");
    }
}

#[test]
fn the_implied_lease_admits_its_four_lifecycle_kinds_from_the_handoff_on() {
    let mut events = renewed(12, LEASE);
    events.push(event("ExpiryChanged", 12, LEASE, json!({})));
    events.push(event("TokenControlTransferred", 13, LEASE, json!({})));
    events.push(event(
        "ExpiryChanged",
        13,
        LEASE,
        json!({"authority_kind": "wrapper"}),
    ));
    let facts = Name::migrated(events).facts();
    let authority = Authority::of(&facts);
    let admitted: Vec<bool> = facts
        .events
        .iter()
        .map(|event| authority.admits(&Probe::of(event)))
        .collect();
    assert_eq!(admitted, vec![true, true, true, false, false]);
}
