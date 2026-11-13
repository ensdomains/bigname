//! A `.eth` second-level name only the 2017 registry recorded, of which bigname holds no
//! BaseRegistrar row: the composed read of the name, then every gate flipped alone.
use std::{collections::BTreeMap, sync::Arc};

use serde_json::{Map, Value, json};

use super::{AuthoritySelection, Clock, NameFacts, NameInput, NamePlace, ShadowName, evaluate};
use crate::families::control::{
    position::Position,
    registry::{OwnerEvent, RegistryNode},
    rows::{BindingCandidate, LifecycleEvent},
};

const NAME: &str = "ens:0x01";
const OTHER_NAME: &str = "ens:0x02";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const RESOURCE: &str = "00000000-0000-0000-0000-0000000000a0";
const LEASE: &str = "00000000-0000-0000-0000-0000000000b1";
const CLOCK: Clock = Clock {
    block_number: 100,
    timestamp_seconds: 1_900_000_000,
};

/// A row of `family` on `resource`, emitted for `name`.
fn event(
    kind: &str,
    family: &str,
    resource: &str,
    name: Option<&str>,
    extra: Value,
) -> LifecycleEvent {
    let mut row = json!({
        "state_kind": "resource",
        "state_key": resource,
        "block_number": 20,
        "transaction_index": 0,
        "log_index": 1,
        "event_identity": format!("{kind}:{family}:{resource}"),
        "event_kind": kind,
        "original_logical_name_id": name,
        "decoded_logical_name_id": name,
        "resource_id": resource,
        "source_family": family,
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    LifecycleEvent::from_row(&row).expect("lifecycle row decodes")
}

fn registrar_row(kind: &str, name: Option<&str>) -> LifecycleEvent {
    event(
        kind,
        "ens_v1_registrar_l1",
        LEASE,
        name,
        json!({"authority_kind": "registrar"}),
    )
}

fn binding(extra: Value) -> BindingCandidate {
    let mut row = json!({
        "surface_binding_id": "binding-1",
        "logical_name_id": NAME,
        "authority_arm": "ens_v1",
        "resource_id": RESOURCE,
        "block_number": 5,
        "transaction_index": 0,
        "log_index": 0,
        "registry_only": true,
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    BindingCandidate::from_row(&row).expect("candidate decodes")
}

/// The 2017 registry's owner write of the node.
fn old_write() -> OwnerEvent {
    OwnerEvent {
        position: Position {
            block_number: 5,
            transaction_index: Some(0),
            log_index: Some(2),
            event_identity: "old-write".to_owned(),
        },
        transaction_hash: None,
        logical_name_id: Some(NAME.to_owned()),
        resource_id: Some(RESOURCE.to_owned()),
        event_kind: "AuthorityTransferred".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        authority_kind: None,
        owner: Some(OWNER.to_owned()),
        registry_owner: Some(OWNER.to_owned()),
        owner_word_unmasked: None,
        owner_getter: Some(OWNER.to_owned()),
        owner_getter_reason: None,
    }
}

struct Name {
    arm: &'static str,
    place: NamePlace,
    binding: Option<BindingCandidate>,
    ownerless: bool,
    unsupported: Option<&'static str>,
    has_old_record: bool,
    first_current_record_block: Option<i64>,
    events: Vec<LifecycleEvent>,
}

/// The input class: one registry-only binding, the 2017 registry's record and nothing else.
fn never_registered() -> Name {
    Name {
        arm: "ens_v1",
        place: NamePlace::EthSecondLevel,
        binding: Some(binding(json!({}))),
        ownerless: false,
        unsupported: None,
        has_old_record: true,
        first_current_record_block: None,
        events: Vec::new(),
    }
}

impl Name {
    fn read(self) -> ShadowName {
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
        let facts = NameFacts {
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
                has_old_record: self.has_old_record,
                first_current_record_block: self.first_current_record_block,
                owner_events: vec![old_write()],
            }),
            resolution_cutover: false,
            ens_v2_root: None,
            grace_registries: Arc::new(Vec::new()),
        };
        evaluate(&facts, &CLOCK).expect("the name composes")
    }
}

fn assert_no_registration(shadow: &ShadowName) {
    assert_eq!(shadow.trace.get("never_registered"), Some(&json!(true)));
    assert_eq!(shadow.registration["status"], "unregistered");
    assert_eq!(shadow.registration["lifecycle_status"], "unregistered");
    for field in ["authority_kind", "authority_key", "registrant", "expiry"] {
        assert_eq!(shadow.registration[field], Value::Null, "{field}");
    }
    assert_eq!(
        shadow.control,
        Map::from_iter([("status".to_owned(), json!("unregistered"))])
    );
}

/// The gate kept the name out: it keeps the status it composes with no gate at all.
fn assert_kept_out(shadow: &ShadowName, status: &str) {
    assert_eq!(shadow.trace.get("never_registered"), None);
    assert_eq!(shadow.registration["status"], status);
}

#[test]
fn a_name_only_the_2017_registry_recorded_serves_no_registration() {
    assert_no_registration(&never_registered().read());
}

#[test]
fn another_names_or_an_unattached_registrar_row_changes_nothing() {
    for name in [Some(OTHER_NAME), None] {
        let mut input = never_registered();
        input.events = vec![registrar_row("RegistrationGranted", name)];
        assert_no_registration(&input.read());
    }
}

#[test]
fn another_arm_is_kept_out() {
    let mut input = never_registered();
    input.arm = "ens_v2";
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_name_below_the_second_level_is_kept_out() {
    let mut input = never_registered();
    input.place = NamePlace::BelowEthSecondLevel("ens:0x03".to_owned());
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_basenames_second_level_name_is_kept_out() {
    let mut input = never_registered();
    input.arm = "basenames";
    input.place = NamePlace::BasenamesSecondLevel;
    assert_kept_out(&input.read(), "active");
}

#[test]
fn an_unsupported_row_is_kept_out() {
    let mut input = never_registered();
    input.unsupported = Some("current_authority_not_projected");
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_binding_that_is_not_registry_only_is_kept_out() {
    let mut input = never_registered();
    input.binding = Some(binding(
        json!({"registry_only": false, "authority_kind": "wrapper"}),
    ));
    assert_kept_out(&input.read(), "active");
}

#[test]
fn an_implied_lease_is_kept_out() {
    let mut input = never_registered();
    input.events = vec![event(
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        LEASE,
        Some(NAME),
        json!({"authority_kind": "registrar", "expiry": 2_000_000_000_i64,
               "expiry_seconds": 2_000_000_000_i64, "source_event": "NameRenewed"}),
    )];
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_current_registry_record_is_kept_out() {
    let mut input = never_registered();
    input.first_current_record_block = Some(6);
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_node_with_no_2017_record_is_kept_out() {
    let mut input = never_registered();
    input.has_old_record = false;
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_token_transfer_of_the_name_is_kept_out() {
    let mut input = never_registered();
    input.events = vec![registrar_row("TokenControlTransferred", Some(NAME))];
    assert_kept_out(&input.read(), "active");
}

#[test]
fn a_selected_grant_of_another_family_is_kept_out() {
    let mut input = never_registered();
    input.events = vec![event(
        "RegistrationGranted",
        "ens_v1_wrapper_l1",
        RESOURCE,
        Some(NAME),
        json!({"authority_kind": "wrapper", "expiry": 2_000_000_000_i64,
               "expiry_seconds": 2_000_000_000_i64}),
    )];
    assert_kept_out(&input.read(), "active");
}

#[test]
fn the_ownerless_profile_keeps_its_own_path() {
    let mut input = never_registered();
    input.binding = None;
    input.ownerless = true;
    assert_kept_out(&input.read(), "unregistered");
}
