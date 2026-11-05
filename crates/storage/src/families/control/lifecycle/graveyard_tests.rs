//! A name whose newest current-registry write names the admitted Graveyard serves its ENSv1
//! registration as ended: `released`, no expiry (`expires_at_reason: released`), no owner,
//! manager or registrant, released at the claim's block, with its holder before the claim as the
//! lapsed owner. A name the Graveyard does not hold composes as it did. Each input class is one
//! test. A claim is only reachable for a lease past its expiry and grace, so the classes below
//! are the only claim inputs (`Graveyard.sol` `_clear`, BaseRegistrar `available`).
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde_json::{Map, Value, json};

use super::super::{
    AuthoritySelection, Clock, NameFacts, NameInput, NamePlace, ShadowName, evaluate,
};
use crate::families::{
    control::{
        position::Position,
        registry::{OwnerEvent, RegistryNode, ZERO_ADDRESS},
        rows::{BindingCandidate, LifecycleEvent, WrapperRow},
    },
    name::selection::select,
};

const NAME: &str = "ens:0x01";
const HOLDER: &str = "0x00000000000000000000000000000000000000aa";
const NEXT_OWNER: &str = "0x00000000000000000000000000000000000000bb";
const NAME_WRAPPER: &str = "0x00000000000000000000000000000000000000cc";
const GRAVEYARD: &str = "0x00000000000000000000000000000000000000dd";
const REGISTRY_ONLY: &str = "00000000-0000-0000-0000-0000000000a0";
const LEASE: &str = "00000000-0000-0000-0000-0000000000b1";
const WRAPPED: &str = "00000000-0000-0000-0000-0000000000c1";
const ENS_V2: &str = "00000000-0000-0000-0000-0000000000d1";
/// The first current-registry record.
const HANDOFF: i64 = 5;
/// The Graveyard's claim, and its block time.
const CLAIM_BLOCK: i64 = 60;
const CLAIM_TIME: i64 = 1_800_000_000;
/// A lease expiry long past its 90-day grace at the clock.
const LAPSED_EXPIRY: i64 = 1_600_000_000;
/// The 2020 registrar migration's lease expiry.
const MIGRATION_EXPIRY: i64 = 1_590_000_000;
const LIVE_EXPIRY: i64 = 2_000_000_000;
const CLOCK: Clock = Clock {
    block_number: 100,
    timestamp_seconds: 1_900_000_000,
};

fn event(kind: &str, family: &str, resource: &str, block: i64, extra: Value) -> LifecycleEvent {
    let mut row = json!({
        "state_kind": "resource",
        "state_key": resource,
        "block_number": block,
        "transaction_index": 0,
        "log_index": 1,
        "event_identity": format!("{kind}:{family}:{block}"),
        "event_kind": kind,
        "original_logical_name_id": NAME,
        "decoded_logical_name_id": NAME,
        "resource_id": resource,
        "source_family": family,
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    LifecycleEvent::from_row(&row).expect("lifecycle row decodes")
}

/// A BaseRegistrar lease of `HOLDER` until `expiry`, granted at `block`.
fn lease_grant(block: i64, expiry: i64) -> LifecycleEvent {
    event(
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        LEASE,
        block,
        json!({"authority_kind": "registrar", "source_event": "NameRegistered",
               "registrant": HOLDER, "expiry": expiry, "expiry_seconds": expiry}),
    )
}

/// The release Interpret records at the first block past the lease's grace.
fn lease_release(block: i64, expiry: i64) -> LifecycleEvent {
    event(
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        LEASE,
        block,
        json!({"authority_kind": "registrar", "source_event": "RegistrationReleased",
               "before_registrant": HOLDER, "released_at": expiry + 7_776_001,
               "expiry": expiry, "expiry_seconds": expiry}),
    )
}

fn binding(resource: &str, block: i64, extra: Value) -> BindingCandidate {
    let mut row = json!({
        "surface_binding_id": format!("binding-{resource}"),
        "logical_name_id": NAME,
        "authority_arm": "ens_v1",
        "resource_id": resource,
        "block_number": block,
        "transaction_index": 0,
        "log_index": 0,
        "registry_only": true,
        "authority_kind": "registry_only",
    });
    row.as_object_mut()
        .expect("row is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    BindingCandidate::from_row(&row).expect("candidate decodes")
}

/// The registry-only binding a current-registry record opens at `block`, with the state-derived
/// SurfaceBound that gives the name its `registry_only` authority.
fn registry_only(block: i64) -> BindingCandidate {
    binding(
        REGISTRY_ONLY,
        block,
        json!({"state_derived": true,
               "surface_bound_position": {"block_number": block, "transaction_index": 0,
                                          "log_index": 0, "event_identity": "surface-bound"}}),
    )
}

fn registrar_binding() -> BindingCandidate {
    binding(
        LEASE,
        8,
        json!({"registry_only": false, "authority_kind": "registrar"}),
    )
}

/// A NameWrapper binding of the wrapped token, which holds the lease `LEASE`.
fn wrapper_binding() -> BindingCandidate {
    binding(
        WRAPPED,
        8,
        json!({"registry_only": false, "authority_kind": "wrapper",
               "emitting_address": NAME_WRAPPER, "wrapped_registrar_resource_id": LEASE,
               "node": "0x01", "transaction_hash": "0xwrap"}),
    )
}

/// A current-registry owner write of the node to `owner` at `block`.
fn write(block: i64, resource: &str, owner: &str, reason: Option<&str>) -> OwnerEvent {
    OwnerEvent {
        position: Position {
            block_number: block,
            transaction_index: Some(0),
            log_index: Some(2),
            event_identity: format!("write:{block}"),
        },
        transaction_hash: None,
        logical_name_id: Some(NAME.to_owned()),
        resource_id: Some(resource.to_owned()),
        event_kind: "AuthorityTransferred".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        authority_kind: None,
        owner: Some(owner.to_owned()),
        registry_owner: Some(owner.to_owned()),
        owner_word_unmasked: None,
        owner_getter: Some(owner.to_owned()),
        owner_getter_reason: reason.map(str::to_owned),
    }
}

/// The admitted Graveyard's claim write of the node.
fn claim(resource: &str) -> OwnerEvent {
    write(CLAIM_BLOCK, resource, GRAVEYARD, Some("graveyard"))
}

struct Name {
    arm: &'static str,
    place: NamePlace,
    binding: BindingCandidate,
    events: Vec<LifecycleEvent>,
    writes: Vec<OwnerEvent>,
    wrappers: Vec<WrapperRow>,
    has_old_record: bool,
    first_current_record_block: Option<i64>,
    migration: Option<Position>,
}

impl Name {
    fn new(binding: BindingCandidate, events: Vec<LifecycleEvent>) -> Self {
        let resource = binding.resource_id.clone();
        Self {
            arm: "ens_v1",
            place: NamePlace::EthSecondLevel,
            binding,
            events,
            writes: vec![write(HANDOFF, &resource, HOLDER, None)],
            wrappers: Vec::new(),
            has_old_record: false,
            first_current_record_block: Some(HANDOFF),
            migration: None,
        }
    }

    /// The input with the Graveyard's claim write appended.
    fn claimed(mut self) -> Self {
        let resource = self.binding.resource_id.clone();
        self.writes.push(claim(&resource));
        self
    }

    fn read(&self) -> ShadowName {
        evaluate(&self.facts(), &CLOCK).expect("the name composes")
    }

    /// The name's facts, with the input selection of its one binding on its arm.
    fn facts(&self) -> NameFacts {
        let selection = AuthoritySelection {
            authority_arm: Some(self.arm.to_owned()),
            surface_binding_id: Some(self.binding.surface_binding_id.clone()),
            resource_id: Some(self.binding.resource_id.clone()),
            ..AuthoritySelection::default()
        };
        NameFacts {
            input: NameInput {
                logical_name_id: NAME.to_owned(),
                namehash: "0x01".to_owned(),
                selection,
                place: self.place.clone(),
            },
            candidates: vec![self.binding.clone()],
            lease_candidates: Vec::new(),
            key_states: BTreeMap::new(),
            triples: Vec::new(),
            events: self.events.clone(),
            wrappers: self
                .wrappers
                .iter()
                .map(|row| (row.resource_id.clone(), row.clone()))
                .collect(),
            block_timestamps: Arc::new(BTreeMap::new()),
            block_seconds: Arc::new(BTreeMap::from([(CLAIM_BLOCK, CLAIM_TIME)])),
            snapshot_timestamps: Arc::new(BTreeMap::new()),
            authority_starts: Value::Null,
            migration: self.migration.clone(),
            registry_node: Some(RegistryNode {
                namespace: "ens".to_owned(),
                node: "0x01".to_owned(),
                has_old_record: self.has_old_record,
                first_current_record_block: self.first_current_record_block,
                owner_events: self.writes.clone(),
            }),
            resolution_cutover: false,
            ens_v2_root: None,
            grace_registries: Arc::new(Vec::new()),
        }
    }
}

/// The composed reader's path: select the authority from the candidates and history alone
/// (`name/selection.rs`), then compose. Returns the selected arm and the composition.
fn selected(mut facts: NameFacts) -> (Option<String>, ShadowName) {
    facts.input.selection = AuthoritySelection::default();
    let readable: BTreeSet<String> = facts
        .candidates
        .iter()
        .map(|candidate| candidate.resource_id.clone())
        .collect();
    let decided = select(&facts, &CLOCK, None, None, &readable).expect("the name selects");
    facts.input.selection = decided.selection;
    let arm = facts.input.selection.authority_arm.clone();
    (
        arm,
        evaluate(&facts, &CLOCK).expect("the selected name composes"),
    )
}

fn field(map: &Map<String, Value>, key: &str) -> Value {
    map.get(key).cloned().unwrap_or(Value::Null)
}

/// The Graveyard-held shape, with `holder` as the lapsed owner, released at the claim.
fn assert_held(shadow: &ShadowName, holder: Option<&str>) {
    assert_held_at(shadow, holder, CLAIM_TIME);
}

/// The Graveyard-held shape, with `holder` as the lapsed owner, released at `released_at`.
fn assert_held_at(shadow: &ShadowName, holder: Option<&str>, released_at: i64) {
    let registration = &shadow.registration;
    assert_eq!(
        field(registration, "lifecycle_status"),
        "released",
        "{shadow:#?}"
    );
    assert_eq!(field(registration, "status"), "released", "{shadow:#?}");
    assert_eq!(field(registration, "expiry"), Value::Null, "{shadow:#?}");
    assert_eq!(
        field(registration, "expires_at_reason"),
        "released",
        "{shadow:#?}"
    );
    for absent in ["grace_ends_at", "ens_v1_expiry", "registrant"] {
        assert_eq!(
            field(registration, absent),
            Value::Null,
            "{absent}: {shadow:#?}"
        );
    }
    assert_eq!(
        field(registration, "released_at"),
        json!(released_at),
        "{shadow:#?}"
    );
    assert_eq!(
        field(registration, "lapsed_registration"),
        json!({"owner": holder, "released_at": released_at, "release_kind": "expired"}),
        "{shadow:#?}"
    );
    for absent in ["owner", "registry_owner", "registrant", "expiry"] {
        assert_eq!(
            field(&shadow.control, absent),
            Value::Null,
            "{absent}: {shadow:#?}"
        );
    }
}

/// The Graveyard rule did not apply.
fn assert_not_held(shadow: &ShadowName) {
    assert_ne!(
        shadow.registration.get("expires_at_reason"),
        Some(&json!("released")),
        "{shadow:#?}"
    );
}

/// Row 1, release-line shape: a 2020-migrated name with no lease row, on its registry-only
/// binding, serves `active` with its registry owner until the claim.
#[test]
fn a_migrated_name_with_no_lease_row_is_released_by_the_claim() {
    let input = || Name::new(registry_only(HANDOFF), Vec::new());
    let unclaimed = input().read();
    assert_eq!(field(&unclaimed.registration, "lifecycle_status"), "active");
    assert_eq!(field(&unclaimed.control, "owner"), HOLDER);
    assert_held(&input().claimed().read(), Some(HOLDER));
}

/// Row 1, reconcile shape: the 2020 mint's lease, past grace, is released by its schedule with
/// its expiry until the claim.
#[test]
fn a_migrated_name_with_its_2020_lease_is_released_with_no_expiry() {
    let input = || {
        Name::new(
            registry_only(HANDOFF),
            vec![lease_grant(12, MIGRATION_EXPIRY)],
        )
    };
    let unclaimed = input().read();
    assert!(
        unclaimed.trace.contains_key("implied_lease"),
        "{unclaimed:#?}"
    );
    assert_eq!(
        field(&unclaimed.registration, "lifecycle_status"),
        "released"
    );
    assert_ne!(field(&unclaimed.registration, "expiry"), Value::Null);
    assert_held(&input().claimed().read(), Some(HOLDER));
}

/// Row 2: a controller-registered lease, released past grace before the claim, loses its
/// expiry, and its release time is the claim's.
#[test]
fn a_released_named_lease_is_released_at_the_claim_with_no_expiry() {
    let input = || {
        Name::new(
            registrar_binding(),
            vec![
                lease_grant(8, LAPSED_EXPIRY),
                lease_release(40, LAPSED_EXPIRY),
            ],
        )
    };
    let unclaimed = input().read();
    assert_eq!(
        field(&unclaimed.registration, "lifecycle_status"),
        "released"
    );
    assert_ne!(field(&unclaimed.registration, "expiry"), Value::Null);
    assert_held(&input().claimed().read(), Some(HOLDER));
}

/// Row 3: a record cleared to zero before the claim, then claimed: the claim gives the node a
/// registry-only binding again, and nobody held it when it was claimed.
#[test]
fn a_record_cleared_before_the_claim_is_released_with_no_holder() {
    let mut input = Name::new(registry_only(CLAIM_BLOCK), Vec::new());
    input
        .writes
        .push(write(20, REGISTRY_ONLY, ZERO_ADDRESS, Some("literal_zero")));
    assert_held(&input.claimed().read(), None);
}

/// Rows 4, 5 and 10: a name the ENSv2 arm decides is untouched by a Graveyard-held ENSv1 record.
fn assert_ens_v2_untouched(grant_block: i64, migration: Option<Position>) {
    let input = || {
        let mut input = Name::new(
            binding(
                ENS_V2,
                grant_block,
                json!({"authority_arm": "ens_v2",
                                                 "registry_only": false,
                                                 "authority_kind": "ens_v2_registry"}),
            ),
            vec![event(
                "RegistrationGranted",
                "ens_v2_registry_l1",
                ENS_V2,
                grant_block,
                json!({"authority_kind": "ens_v2_registry", "source_event": "LabelRegistered",
                       "registrant": NEXT_OWNER, "expiry": LIVE_EXPIRY,
                       "expiry_seconds": LIVE_EXPIRY}),
            )],
        );
        input.arm = "ens_v2";
        input.writes = vec![write(HANDOFF, REGISTRY_ONLY, HOLDER, None)];
        input.migration = migration.clone();
        input
    };
    let unclaimed = input().read();
    let mut claimed = input();
    claimed.writes.push(claim(REGISTRY_ONLY));
    let claimed = claimed.read();
    assert_not_held(&claimed);
    assert_eq!(claimed, unclaimed);
}

/// Row 4: a label registered in ENSv2 before the claim.
#[test]
fn an_ens_v2_registration_before_the_claim_is_untouched() {
    assert_ens_v2_untouched(40, None);
}

/// Row 10: a label registered in ENSv2 after the claim, which selection gives the ENSv2 arm.
#[test]
fn an_ens_v2_registration_after_the_claim_is_untouched() {
    assert_ens_v2_untouched(70, None);
}

/// Row 5: a migrated name. The migration hands its ENSv1 record to the Graveyard, which then
/// never claims it (`_clear` returns before `register`), and the name is on the ENSv2 arm.
#[test]
fn a_migrated_name_whose_record_the_graveyard_holds_is_untouched() {
    let migration = Position {
        block_number: CLAIM_BLOCK,
        transaction_index: Some(0),
        log_index: Some(3),
        event_identity: "migration".to_owned(),
    };
    assert_ens_v2_untouched(CLAIM_BLOCK, Some(migration));
}

fn wrapped(state: &str, fuses: i64) -> Name {
    let mut input = Name::new(
        wrapper_binding(),
        vec![event(
            "RegistrationGranted",
            "ens_v1_wrapper_l1",
            WRAPPED,
            8,
            json!({"authority_kind": "wrapper", "registrant": HOLDER,
                   "transaction_hash": "0xwrap",
                   "expiry": LAPSED_EXPIRY + 7_776_000,
                   "expiry_seconds": LAPSED_EXPIRY + 7_776_000}),
        )],
    );
    input.writes = vec![write(8, WRAPPED, NAME_WRAPPER, None)];
    input.wrappers = vec![
        WrapperRow::from_row(&json!({
            "resource_id": WRAPPED,
            "logical_name_id": NAME,
            "wrapper_state": state,
            "fuses": fuses,
            "expiry_seconds": (LAPSED_EXPIRY + 7_776_000).to_string(),
            "expiry_position": {"block_number": 8},
            "lifecycle_unwrapped": false,
        }))
        .expect("wrapper row decodes"),
    ];
    input
}

/// Row 8: a wrapped name past its NameWrapper expiry, claimed. The lapsed owner is the wrapped
/// token's holder, never the NameWrapper.
#[test]
fn a_wrapped_name_is_released_with_its_token_holder_as_the_lapsed_owner() {
    // PARENT_CANNOT_CONTROL | CANNOT_UNWRAP is not burned; wrapETH2LD burns PARENT_CANNOT_CONTROL.
    assert_held(
        &wrapped("emancipated", 0x10000).claimed().read(),
        Some(HOLDER),
    );
}

/// Row 9: a locked wrapped name. Past expiry NameWrapper reads its fuses as zero, so it is
/// claimable like any other.
#[test]
fn a_locked_wrapped_name_is_released_with_its_token_holder_as_the_lapsed_owner() {
    assert_held(&wrapped("locked", 0x10001).claimed().read(), Some(HOLDER));
}

/// Row 11: a subname the Graveyard cleared with `setSubnodeRecord`.
#[test]
fn a_cleared_subname_is_released() {
    let input = || {
        let mut input = Name::new(registry_only(HANDOFF), Vec::new());
        input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
        input
    };
    let unclaimed = input().read();
    assert_eq!(field(&unclaimed.registration, "lifecycle_status"), "active");
    assert_eq!(field(&unclaimed.control, "owner"), HOLDER);
    assert_held(&input().claimed().read(), Some(HOLDER));
}

/// Row 11, wrapped: a wrapped subname whose token survives the clear, under a parent the
/// Graveyard holds: the Graveyard takes the registry record with `setSubnodeRecord`.
#[test]
fn a_cleared_wrapped_subname_is_released() {
    let mut input = wrapped("wrapped", 0);
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    assert_held(&input.claimed().read(), Some(HOLDER));
}

/// Row 11, re-parented (ruling 3's case): a wrapped subname without `PARENT_CANNOT_CONTROL` under
/// a locked parent. In one transaction the Graveyard has the NameWrapper re-parent the token to
/// it, which writes the registry record to the NameWrapper and moves the token to the Graveyard,
/// then unwraps it to itself. Neither the Graveyard nor the NameWrapper is a holder. The holder
/// is the recipient of the NameWrapper's last transfer before the claim's write that is not the
/// Graveyard: the wrap's.
#[test]
fn a_re_parented_wrapped_subname_is_released_with_its_token_holder() {
    let mut input = wrapped("wrapped", 0);
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    input.events.push(wrapper_transfer(8, HOLDER));
    input.events.push(event(
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        WRAPPED,
        CLAIM_BLOCK,
        json!({"authority_kind": "wrapper", "source_event": "TransferSingle",
               "to_address": GRAVEYARD}),
    ));
    let mut re_parent = write(CLAIM_BLOCK, WRAPPED, NAME_WRAPPER, None);
    re_parent.position.log_index = Some(0);
    re_parent.position.event_identity = "write:re-parent".to_owned();
    input.writes.push(re_parent);
    assert_held(&input.claimed().read(), Some(HOLDER));
}

/// A live token sent to the Graveyard, later claimed: the Graveyard is not the lapsed holder, the
/// registry owner the holder left behind is.
#[test]
fn a_token_sent_to_the_graveyard_then_claimed_keeps_its_holder() {
    let input = Name::new(
        registrar_binding(),
        vec![
            lease_grant(8, LAPSED_EXPIRY),
            event(
                "TokenControlTransferred",
                "ens_v1_registrar_l1",
                LEASE,
                50,
                json!({"authority_kind": "registrar", "source_event": "Transfer",
                       "to_address": GRAVEYARD}),
            ),
        ],
    );
    assert_held(&input.claimed().read(), Some(HOLDER));
}

/// Row 5 on the ENSv1 arm: a migrated name whose selection falls back to its ENSv1 binding. Its
/// record is the Graveyard's from the migration, which no claim follows.
#[test]
fn a_migrated_name_on_its_ens_v1_arm_is_untouched() {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new()).claimed();
    input.migration = Some(Position {
        block_number: CLAIM_BLOCK,
        transaction_index: Some(0),
        log_index: Some(3),
        event_identity: "migration".to_owned(),
    });
    assert_not_held(&input.read());
}

/// Row 12: a name only the 2017 registry recorded was never minted, so the Graveyard cannot
/// claim it. Its one owner write stands for the 2017 registry's record. With no Graveyard write
/// the rule does not apply.
#[test]
fn a_never_registered_name_is_untouched() {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new());
    input.has_old_record = true;
    input.first_current_record_block = None;
    assert_not_held(&input.read());
}

/// Row 13: a live lease never claimed.
#[test]
fn a_live_lease_is_untouched() {
    let shadow = Name::new(registrar_binding(), vec![lease_grant(8, LIVE_EXPIRY)]).read();
    assert_not_held(&shadow);
    assert_eq!(field(&shadow.registration, "lifecycle_status"), "active");
    assert_eq!(field(&shadow.control, "owner"), HOLDER);
}

/// Row 14: a lease lapsed past grace and not yet claimed keeps its expiry: the null expiry is
/// the Graveyard's alone.
#[test]
fn a_lapsed_lease_not_yet_claimed_keeps_its_expiry() {
    let shadow = Name::new(
        registrar_binding(),
        vec![
            lease_grant(8, LAPSED_EXPIRY),
            lease_release(40, LAPSED_EXPIRY),
        ],
    )
    .read();
    assert_not_held(&shadow);
    assert_eq!(field(&shadow.registration, "lifecycle_status"), "released");
    assert_ne!(field(&shadow.registration, "expiry"), Value::Null);
    assert_ne!(field(&shadow.registration, "ens_v1_expiry"), Value::Null);
}

/// Row 15: a live token its holder sent to the Graveyard keeps its lease: the rule keys on the
/// registry write, not on token custody.
#[test]
fn a_live_token_sent_to_the_graveyard_is_untouched() {
    let shadow = Name::new(
        registrar_binding(),
        vec![
            lease_grant(8, LIVE_EXPIRY),
            event(
                "TokenControlTransferred",
                "ens_v1_registrar_l1",
                LEASE,
                50,
                json!({"authority_kind": "registrar", "source_event": "Transfer",
                       "to_address": GRAVEYARD}),
            ),
        ],
    )
    .read();
    assert_not_held(&shadow);
    assert_eq!(field(&shadow.registration, "lifecycle_status"), "active");
    assert_ne!(field(&shadow.registration, "expiry"), Value::Null);
}

/// Row 16: a write naming a Graveyard the migration manifest does not admit, or written before
/// its declared start, carries no Graveyard reason and is an ordinary registry owner.
#[test]
fn an_undeclared_graveyard_write_is_an_ordinary_owner() {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new());
    input
        .writes
        .push(write(CLAIM_BLOCK, REGISTRY_ONLY, GRAVEYARD, None));
    let shadow = input.read();
    assert_not_held(&shadow);
    assert_eq!(field(&shadow.registration, "lifecycle_status"), "active");
    assert_eq!(field(&shadow.control, "owner"), GRAVEYARD);
}

/// Ruling 4: a live holder's `setOwner` of the registry record to the Graveyard, not a claim.
/// `clear` cannot claim a live lease, so the write is the holder's own. The record is the
/// Graveyard's, which never transfers it, and the name is served released, with the lease's
/// holder as the lapsed owner, until a newer registry write (the token holder's `reclaim`).
#[test]
fn a_set_owner_to_the_graveyard_by_a_live_holder_not_a_claim_serves_released() {
    let input = || Name::new(registrar_binding(), vec![lease_grant(8, LIVE_EXPIRY)]);
    let unclaimed = input().read();
    assert_eq!(field(&unclaimed.registration, "lifecycle_status"), "active");
    assert_held(&input().claimed().read(), Some(HOLDER));
    let mut reclaimed = input().claimed();
    reclaimed
        .writes
        .push(write(CLAIM_BLOCK + 1, LEASE, HOLDER, None));
    assert_not_held(&reclaimed.read());
    assert_eq!(
        field(&reclaimed.read().registration, "lifecycle_status"),
        "active"
    );
}

/// A claim the rule did not reach: the registration fields it writes are as the unclaimed name's.
fn assert_same_registration(claimed: &ShadowName, unclaimed: &ShadowName) {
    assert_not_held(claimed);
    for key in [
        "status",
        "lifecycle_status",
        "expiry",
        "expires_at_reason",
        "released_at",
        "lapsed_registration",
    ] {
        assert_eq!(
            field(&claimed.registration, key),
            field(&unclaimed.registration, key),
            "{key}: {claimed:#?}"
        );
    }
}

/// The registry-only binding of `registry_only`, open from before the clock: the binding the
/// composed reader selects.
fn open_registry_only() -> BindingCandidate {
    let mut candidate = registry_only(HANDOFF);
    candidate.active_from_seconds = Some(1_500_000_000.0);
    candidate
}

/// R4 row A: a Graveyard-held name whose one candidate is an open ENSv1 binding, with no other
/// authority. Selection gives it the ENSv1 arm, so the rule serves it released.
#[test]
fn a_held_name_with_one_open_ens_v1_candidate_is_selected_and_released() {
    let input = || Name::new(open_registry_only(), Vec::new());
    let (arm, unclaimed) = selected(input().facts());
    assert_eq!(arm.as_deref(), Some("ens_v1"));
    assert_eq!(field(&unclaimed.registration, "lifecycle_status"), "active");
    let (arm, claimed) = selected(input().claimed().facts());
    assert_eq!(arm.as_deref(), Some("ens_v1"));
    assert_held(&claimed, Some(HOLDER));
}

/// R4 row A', the staging summary's reading of the two names: an ENSv1 candidate whose binding
/// is not open at the clock, and no arm from history. Selection gives no arm, so the rule does
/// not reach the name and the claim changes nothing.
#[test]
fn a_held_name_whose_one_candidate_is_not_open_has_no_arm_and_is_untouched() {
    let input = || Name::new(registry_only(HANDOFF), Vec::new());
    let (arm, unclaimed) = selected(input().facts());
    assert!(arm.is_none(), "{arm:?}");
    let (arm, claimed) = selected(input().claimed().facts());
    assert!(arm.is_none(), "{arm:?}");
    assert_same_registration(&claimed, &unclaimed);
}

/// R4 row B: a Graveyard-held name with no candidate. Selection gives no arm, so the rule does
/// not reach the name and the claim changes nothing.
#[test]
fn a_held_name_with_no_candidate_has_no_arm_and_is_untouched() {
    let without_candidates = |name: Name| {
        let mut facts = name.facts();
        facts.candidates.clear();
        facts
    };
    let (arm, unclaimed) = selected(without_candidates(Name::new(
        registry_only(HANDOFF),
        Vec::new(),
    )));
    assert!(arm.is_none(), "{arm:?}");
    let (arm, claimed) = selected(without_candidates(
        Name::new(registry_only(HANDOFF), Vec::new()).claimed(),
    ));
    assert!(arm.is_none(), "{arm:?}");
    assert_same_registration(&claimed, &unclaimed);
}

/// A NameWrapper transfer of the wrapped token `WRAPPED` to `to`, the shape the NameWrapper's
/// `NameWrapped` and transfers take (adapters `wrapper.rs`).
fn wrapper_transfer(block: i64, to: &str) -> LifecycleEvent {
    event(
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        WRAPPED,
        block,
        json!({"authority_kind": "wrapper", "source_event": "TransferSingle", "to_address": to}),
    )
}

/// A wrapped subname with the registry-only binding a registry write naming someone other than
/// the NameWrapper opened at `block`, at time `opened`
/// (adapters `wrapper_custody.rs`, `keeps_authority`), and the registry's NameWrapper record
/// until then. Selection picks the registry-only binding, as in production.
fn handed_off(
    block: i64,
    opened: i64,
    registry_only_flag: bool,
    transfers: &[(i64, &str)],
) -> Name {
    let mut input = wrapped("wrapped", 0);
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    input.events.push(wrapper_transfer(8, HOLDER));
    for (at, to) in transfers {
        input.events.push(wrapper_transfer(*at, to));
    }
    let mut registry_only = binding(
        REGISTRY_ONLY,
        block,
        json!({"state_derived": true, "registry_only": registry_only_flag,
               "predecessor_resource_id": WRAPPED,
               "surface_bound_position": {"block_number": block, "transaction_index": 0,
                                          "log_index": 2, "event_identity": "surface-bound"}}),
    );
    registry_only.active_from_seconds = Some(opened as f64);
    input.binding = registry_only;
    input
}

/// The composed reader's selection over the NameWrapper binding, closed at `closed`, and the
/// registry-only binding.
fn select_handed_off(input: Name, closed: i64) -> (Option<String>, ShadowName) {
    let mut wrapper = wrapper_binding();
    wrapper.active_from_seconds = Some(1_500_000_000.0);
    wrapper.active_to_seconds = Some(closed as f64);
    let mut facts = input.facts();
    facts.candidates = vec![wrapper, facts.candidates[0].clone()];
    selected(facts)
}

/// Row 11, handed off (Codex P2 on #1390): a wrapped subname whose token survives, cleared by the
/// Graveyard's `setSubnodeRecord`. The write names the Graveyard, not the NameWrapper, so it ends
/// the NameWrapper authority and opens a registry-only binding at the claim. The lapsed owner is
/// the recipient of the NameWrapper's last transfer before the claim, never the NameWrapper or
/// the Graveyard: the wrap's, a later holder's, or, for a token its holder sent to the Graveyard
/// before the claim, the sender's. Each registry-only flag a handoff binding can carry.
#[test]
fn a_cleared_wrapped_subname_handed_to_a_registry_only_binding_keeps_its_token_holder() {
    let cases: [(&[(i64, &str)], &str); 3] = [
        (&[], HOLDER),
        (&[(30, NEXT_OWNER)], NEXT_OWNER),
        (&[(40, GRAVEYARD)], HOLDER),
    ];
    for flag in [true, false] {
        for (transfers, holder) in cases {
            let input = handed_off(CLAIM_BLOCK, CLAIM_TIME, flag, transfers).claimed();
            let (arm, shadow) = select_handed_off(input, CLAIM_TIME);
            assert_eq!(arm.as_deref(), Some("ens_v1"), "registry_only {flag}");
            assert_held(&shadow, Some(holder));
        }
    }
}

/// Codex P2 on #1390: a wrapped `.eth` name past its NameWrapper expiry, claimed. The claim's
/// registry write names the Graveyard, which ends the NameWrapper authority and selects a
/// registry-only binding. The lease's BaseRegistrar history is retained, including its transfer
/// into the NameWrapper at the wrap, which the registration laterals keep as the last transfer.
/// The lapsed owner is the wrapped token's holder, never the NameWrapper. Each registry-only flag
/// a handoff binding can carry: the `true` flag implies the lease, whose transfers the laterals
/// keep; the `false` flag admits none of them and guards the registry fallback.
#[test]
fn a_claimed_wrapped_eth_name_handed_to_a_registry_only_binding_keeps_its_token_holder() {
    for flag in [true, false] {
        let mut input = handed_off(CLAIM_BLOCK, CLAIM_TIME, flag, &[]);
        input.place = NamePlace::EthSecondLevel;
        let mut wrap = event(
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            LEASE,
            8,
            json!({"authority_kind": "registrar", "source_event": "Transfer",
                   "to_address": NAME_WRAPPER, "transaction_hash": "0xwrap"}),
        );
        wrap.position.log_index = Some(0);
        input
            .events
            .splice(0..0, [lease_grant(7, LAPSED_EXPIRY), wrap]);
        let (arm, shadow) = select_handed_off(input.claimed(), CLAIM_TIME);
        assert_eq!(arm.as_deref(), Some("ens_v1"), "registry_only {flag}");
        assert_held(&shadow, Some(HOLDER));
    }
}

/// A wrapped subname unwrapped to `NEXT_OWNER` (or its record overwritten by the parent's
/// `setSubnodeOwner`, which leaves the same facts), then cleared: the registry record was not the
/// NameWrapper's when the Graveyard claimed it, so the holder is the owner its last registry write
/// named, not the wrap's recipient.
#[test]
fn an_unwrapped_subname_cleared_later_keeps_its_registry_owner() {
    const UNWRAP_BLOCK: i64 = 20;
    const UNWRAP_TIME: i64 = 1_700_000_000;
    for flag in [true, false] {
        let mut input = handed_off(UNWRAP_BLOCK, UNWRAP_TIME, flag, &[]);
        input
            .writes
            .push(write(UNWRAP_BLOCK, REGISTRY_ONLY, NEXT_OWNER, None));
        let (arm, shadow) = select_handed_off(input.claimed(), UNWRAP_TIME);
        assert_eq!(arm.as_deref(), Some("ens_v1"), "registry_only {flag}");
        assert_held(&shadow, Some(NEXT_OWNER));
    }
}

/// tayy-i on #1390, ruling 4 followed by a token move: a live lease's holder sets the registry
/// record to the Graveyard, then transfers the BaseRegistrar token without `reclaim`. The
/// registration ended at the write, so the lapsed owner is the holder then, not the later
/// recipient the registration laterals name.
#[test]
fn a_token_moved_after_its_holders_own_graveyard_write_keeps_the_holder_at_the_write() {
    let mut input = Name::new(registrar_binding(), vec![lease_grant(8, LIVE_EXPIRY)]).claimed();
    input.events.push(event(
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        LEASE,
        70,
        json!({"authority_kind": "registrar", "source_event": "Transfer",
               "to_address": NEXT_OWNER}),
    ));
    assert_held(&input.read(), Some(HOLDER));
}

/// tayy-i on #1390: in one transaction the record's owner sets it to `NEXT_OWNER` and then
/// calls `clear`. The holder is the owner that earlier write of the claim's own transaction
/// named.
#[test]
fn a_record_set_earlier_in_the_claims_transaction_names_the_holder() {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new());
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    let mut set_owner = write(CLAIM_BLOCK, REGISTRY_ONLY, NEXT_OWNER, None);
    set_owner.position.log_index = Some(1);
    set_owner.position.event_identity = "write:set-owner".to_owned();
    input.writes.push(set_owner);
    assert_held(&input.claimed().read(), Some(NEXT_OWNER));
}

/// `clear` writes a subname of a name it holds again each time it runs, for that subname or any
/// name below it (`Graveyard.sol` `_clear`, the owned-parent branch). The registration ended at
/// the first Graveyard write: `released_at` is its block time and the lapsed owner is the holder
/// before it, never the Graveyard.
#[test]
fn a_repeated_graveyard_write_keeps_the_first_claim_and_its_holder() {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new()).claimed();
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    input
        .writes
        .push(write(80, REGISTRY_ONLY, GRAVEYARD, Some("graveyard")));
    assert_held(&input.read(), Some(HOLDER));
}

/// A guard: the `.eth` claim's BaseRegistrar burn of the lapsed token and its mint to the Graveyard
/// come before the registry write in the claim's transaction. Interpret records no transfer to or
/// from zero, so no such rows arise; if a transfer to zero or the Graveyard were recorded there,
/// neither would name a holder, and the lapsed owner stays the lease's holder before them.
#[test]
fn a_claimed_lease_burned_and_minted_to_the_graveyard_keeps_its_holder() {
    let transfer = |log: i64, to: &str| {
        let mut row = event(
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            LEASE,
            CLAIM_BLOCK,
            json!({"authority_kind": "registrar", "source_event": "Transfer", "to_address": to}),
        );
        row.position.log_index = Some(log);
        row.position.event_identity = format!("transfer:{log}");
        row
    };
    let input = Name::new(
        registrar_binding(),
        vec![
            lease_grant(8, LAPSED_EXPIRY),
            lease_release(40, LAPSED_EXPIRY),
            transfer(0, ZERO_ADDRESS),
            transfer(1, GRAVEYARD),
        ],
    );
    assert_held(&input.claimed().read(), Some(HOLDER));
}

/// The resource of a re-parented subname's later wrap: each wrap is a new NameWrapper resource
/// with its own binding (adapters `wrapper.rs`, `authority_key`).
const REWRAPPED: &str = "00000000-0000-0000-0000-0000000000c2";

/// A re-parented subname, claimed at `CLAIM_BLOCK`, then cleared again at `RECLAIM_BLOCK`. Each
/// re-parent writes the record to the NameWrapper, mints the wrapped token to the Graveyard and
/// writes the record to the Graveyard, in one transaction; the later one wraps under
/// `REWRAPPED`.
fn re_parented_again() -> NameFacts {
    let mut input = wrapped("wrapped", 0);
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    input.events.push(wrapper_transfer(8, HOLDER));
    let mut first = write(CLAIM_BLOCK, WRAPPED, NAME_WRAPPER, None);
    first.position.log_index = Some(0);
    first.position.event_identity = "write:re-parent".to_owned();
    input.writes.push(first);
    input.events.push(wrapper_transfer(CLAIM_BLOCK, GRAVEYARD));
    let mut input = input.claimed();
    let mut again = write(RECLAIM_BLOCK, REWRAPPED, NAME_WRAPPER, None);
    again.position.log_index = Some(0);
    again.position.event_identity = "write:re-parent-again".to_owned();
    input.writes.push(again);
    let mut mint = wrapper_transfer(RECLAIM_BLOCK, GRAVEYARD);
    mint.resource_id = Some(REWRAPPED.to_owned());
    input.events.push(mint);
    let mut repeat = write(RECLAIM_BLOCK, REWRAPPED, GRAVEYARD, Some("graveyard"));
    repeat.position.event_identity = "write:repeat".to_owned();
    input.writes.push(repeat);
    let mut facts = input.facts();
    facts.candidates.push(binding(
        REWRAPPED,
        RECLAIM_BLOCK,
        json!({"registry_only": false, "authority_kind": "wrapper",
               "emitting_address": NAME_WRAPPER, "node": "0x01",
               "transaction_hash": "0xrewrap"}),
    ));
    facts.block_seconds = Arc::new(BTreeMap::from([
        (CLAIM_BLOCK, CLAIM_TIME),
        (RECLAIM_BLOCK, RECLAIM_TIME),
    ]));
    facts
}

/// A re-parented subname cleared again: the later `clear` re-parents it once more under a new
/// wrapped resource. That custody write does not end the Graveyard's hold: `released_at` stays at
/// the first claim and the holder is the one before it.
#[test]
fn a_re_parented_subname_cleared_again_keeps_the_first_claim_and_its_holder() {
    let facts = re_parented_again();
    assert_held(
        &evaluate(&facts, &CLOCK).expect("the name composes"),
        Some(HOLDER),
    );
}

/// The block of a token holder's own write to the Graveyard after a hold, and its block time.
const RECLAIM_BLOCK: i64 = 90;
const RECLAIM_TIME: i64 = 1_850_000_000;

/// Ruling 4's hold: a live lease's holder writes the registry record to the Graveyard at the
/// claim block, then transfers the BaseRegistrar token to `NEXT_OWNER` at block 70.
fn held_then_moved() -> Name {
    let mut input = Name::new(registrar_binding(), vec![lease_grant(8, LIVE_EXPIRY)]).claimed();
    input.events.push(event(
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        LEASE,
        70,
        json!({"authority_kind": "registrar", "source_event": "Transfer",
               "to_address": NEXT_OWNER}),
    ));
    input
}

/// A registry write of the node to `owner` at `RECLAIM_BLOCK`, at `transaction` and `log`.
fn reclaim_block_write(
    transaction: i64,
    log: i64,
    owner: &str,
    reason: Option<&str>,
) -> OwnerEvent {
    let mut write = write(RECLAIM_BLOCK, LEASE, owner, reason);
    write.position.transaction_index = Some(transaction);
    write.position.log_index = Some(log);
    write.position.event_identity = format!("write:{RECLAIM_BLOCK}:{transaction}:{log}");
    write
}

/// Compose with the block times of the claim and of `RECLAIM_BLOCK`, and `extra` candidates.
fn read_at_reclaim(input: &Name, extra: Vec<BindingCandidate>) -> ShadowName {
    let mut facts = input.facts();
    facts.candidates.extend(extra);
    facts.block_seconds = Arc::new(BTreeMap::from([
        (CLAIM_BLOCK, CLAIM_TIME),
        (RECLAIM_BLOCK, RECLAIM_TIME),
    ]));
    evaluate(&facts, &CLOCK).expect("the name composes")
}

/// tayy-i on #1390: after ruling 4's hold, the token's new holder (a contract) calls `reclaim` and
/// then writes the record to the Graveyard in one transaction. The reclaim ended the earlier hold,
/// so its write between the two Graveyard writes ends the run: the registration ended at the later
/// write, and its holder is the reclaimer.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
#[test]
fn a_reclaim_and_graveyard_write_in_one_transaction_start_a_new_hold() {
    let mut input = held_then_moved();
    input
        .writes
        .push(reclaim_block_write(0, 0, NEXT_OWNER, None));
    input
        .writes
        .push(reclaim_block_write(0, 2, GRAVEYARD, Some("graveyard")));
    assert_held_at(
        &read_at_reclaim(&input, Vec::new()),
        Some(NEXT_OWNER),
        RECLAIM_TIME,
    );
}

/// The same with the reclaim and the Graveyard write in two transactions of one block.
#[test]
fn a_reclaim_then_a_graveyard_write_in_another_transaction_start_a_new_hold() {
    let mut input = held_then_moved();
    input
        .writes
        .push(reclaim_block_write(0, 0, NEXT_OWNER, None));
    input
        .writes
        .push(reclaim_block_write(1, 0, GRAVEYARD, Some("graveyard")));
    assert_held_at(
        &read_at_reclaim(&input, Vec::new()),
        Some(NEXT_OWNER),
        RECLAIM_TIME,
    );
}

/// A BaseRegistrar transfer of the lease at `RECLAIM_BLOCK`, at `log`, carrying the later wrap's
/// transaction.
fn lease_transfer_at_reclaim(log: i64, to: &str) -> LifecycleEvent {
    let mut transfer = event(
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        LEASE,
        RECLAIM_BLOCK,
        json!({"authority_kind": "registrar", "source_event": "Transfer",
               "to_address": to, "transaction_hash": "0xrewrap"}),
    );
    transfer.position.log_index = Some(log);
    transfer.position.event_identity = format!("lease-transfer:{log}");
    transfer
}

/// After ruling 4's hold, the token's new holder wraps the name with itself as the wrapped owner
/// (`wrapETH2LD`: the BaseRegistrar token moves into the NameWrapper, whose `reclaim` writes the
/// record to it, and the wrapped token is minted to the holder), then unwraps it with the
/// Graveyard as the registry owner (`unwrapETH2LD`: the record is written to the Graveyard and the
/// BaseRegistrar token is returned), in one transaction. The wrapped token went to the holder, so
/// the hold ends: the holder is the wrapped token's.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L277 @ ens_v1@91c966f)
#[test]
fn a_holders_wrap_and_unwrap_to_the_graveyard_in_one_transaction_start_a_new_hold() {
    let mut input = held_then_moved();
    input
        .events
        .push(lease_transfer_at_reclaim(0, NAME_WRAPPER));
    input
        .writes
        .push(reclaim_block_write(0, 1, NAME_WRAPPER, None));
    let mut mint = wrapper_transfer(RECLAIM_BLOCK, NEXT_OWNER);
    mint.resource_id = Some(REWRAPPED.to_owned());
    mint.position.log_index = Some(2);
    input.events.push(mint);
    input
        .writes
        .push(reclaim_block_write(0, 4, GRAVEYARD, Some("graveyard")));
    input.events.push(lease_transfer_at_reclaim(5, NEXT_OWNER));
    assert_held_at(
        &read_at_reclaim(&input, vec![rewrap_binding(RECLAIM_BLOCK)]),
        Some(NEXT_OWNER),
        RECLAIM_TIME,
    );
}

/// A later Graveyard write in another transaction, and its block time.
const LATER_BLOCK: i64 = 95;
const LATER_TIME: i64 = 1_870_000_000;

/// A subname whose holder `HOLDER` wrote its unwrapped record to the Graveyard at the claim block,
/// then the registry `writes` and the name's wrapped token `transfers` (on `REWRAPPED`, whose
/// NameWrapper binding the name has), each at (block, transaction, log). A write to the Graveyard
/// carries its reason.
fn held_subname(
    writes: &[(i64, i64, i64, &str)],
    transfers: &[(i64, i64, i64, &str)],
) -> NameFacts {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new()).claimed();
    input.place = NamePlace::BelowEthSecondLevel("ens:0x02".to_owned());
    for &(block, transaction, log, owner) in writes {
        let reason = (owner == GRAVEYARD).then_some("graveyard");
        let mut registry = write(block, REGISTRY_ONLY, owner, reason);
        registry.position.transaction_index = Some(transaction);
        registry.position.log_index = Some(log);
        registry.position.event_identity = format!("write:{block}:{transaction}:{log}");
        input.writes.push(registry);
    }
    for &(block, transaction, log, to) in transfers {
        let mut transfer = wrapper_transfer(block, to);
        transfer.resource_id = Some(REWRAPPED.to_owned());
        transfer.position.transaction_index = Some(transaction);
        transfer.position.log_index = Some(log);
        transfer.position.event_identity = format!("transfer:{block}:{transaction}:{log}");
        input.events.push(transfer);
    }
    let mut facts = input.facts();
    facts.candidates.push(binding(
        REWRAPPED,
        RECLAIM_BLOCK,
        json!({"registry_only": false, "authority_kind": "wrapper",
               "emitting_address": NAME_WRAPPER, "node": "0x01",
               "transaction_hash": "0xrewrap"}),
    ));
    facts.block_seconds = Arc::new(BTreeMap::from([
        (CLAIM_BLOCK, CLAIM_TIME),
        (RECLAIM_BLOCK, RECLAIM_TIME),
        (LATER_BLOCK, LATER_TIME),
    ]));
    facts
}

fn compose(facts: &NameFacts) -> ShadowName {
    evaluate(facts, &CLOCK).expect("the name composes")
}

/// tayy-i on #1390: in one transaction the wrapped parent's owner `NEXT_OWNER` has the NameWrapper
/// write the held child's record to itself and mint the token to the Graveyard
/// (`setSubnodeRecord`), moves the token to itself (`setSubnodeOwner` on a wrapped child writes no
/// record), and unwraps it to the Graveyard, which burns it (the adapter records no burn). The
/// transfer out of the Graveyard
/// ends the hold, even though the transaction started with a mint to it: the holder is the one
/// before the later write.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L585 @ ens_v1@91c966f)
#[test]
fn a_held_child_minted_to_the_graveyard_then_taken_out_starts_a_new_hold() {
    let facts = held_subname(
        &[
            (RECLAIM_BLOCK, 0, 0, NAME_WRAPPER),
            (RECLAIM_BLOCK, 0, 6, GRAVEYARD),
        ],
        &[
            (RECLAIM_BLOCK, 0, 1, GRAVEYARD),
            (RECLAIM_BLOCK, 0, 3, NEXT_OWNER),
        ],
    );
    assert_held_at(&compose(&facts), Some(NEXT_OWNER), RECLAIM_TIME);
}

/// In one transaction the wrapped parent's owner wraps the held child to itself, sends the token
/// to the Graveyard, and, as the parent's registry owner after unwrapping the parent, writes the
/// child's record to the Graveyard. It controlled the record in between, so the hold ends there
/// even though the token ends with the Graveyard.
#[test]
fn a_held_child_taken_out_then_sent_back_to_the_graveyard_starts_a_new_hold() {
    let facts = held_subname(
        &[
            (RECLAIM_BLOCK, 0, 0, NAME_WRAPPER),
            (RECLAIM_BLOCK, 0, 6, GRAVEYARD),
        ],
        &[
            (RECLAIM_BLOCK, 0, 1, NEXT_OWNER),
            (RECLAIM_BLOCK, 0, 3, GRAVEYARD),
        ],
    );
    assert_held_at(&compose(&facts), Some(NEXT_OWNER), RECLAIM_TIME);
}

/// The NameWrapper writes the held child's record to itself and mints the token to the Graveyard
/// in one transaction, and the record is written to the Graveyard in a later one. No one else
/// controlled it in between, so the hold carries: released at the first write, with its holder.
#[test]
fn a_held_child_minted_to_the_graveyard_and_written_back_later_keeps_the_first_claim() {
    let facts = held_subname(
        &[
            (RECLAIM_BLOCK, 0, 0, NAME_WRAPPER),
            (LATER_BLOCK, 0, 2, GRAVEYARD),
        ],
        &[(RECLAIM_BLOCK, 0, 1, GRAVEYARD)],
    );
    assert_held(&compose(&facts), Some(HOLDER));
}

/// A registry write to the NameWrapper with no wrapped token transfer before the Graveyard's
/// write: a parent's `setSubnodeOwner` to the NameWrapper, which wraps nothing. The Graveyard did
/// not control the record in between, so the hold ends: released at the later write, with no
/// wrapped holder.
#[test]
fn a_name_wrapper_write_with_no_wrapped_transfer_starts_a_new_hold() {
    let facts = held_subname(
        &[
            (RECLAIM_BLOCK, 0, 0, NAME_WRAPPER),
            (RECLAIM_BLOCK, 0, 2, GRAVEYARD),
        ],
        &[],
    );
    assert_held_at(&compose(&facts), None, RECLAIM_TIME);
}

/// After ruling 4's hold, the token's new holder wraps the `.eth` name with the Graveyard as the
/// wrapped owner (`wrapETH2LD`): the BaseRegistrar token moves into the NameWrapper, whose
/// `reclaim` writes the record to it, and the wrapped token is minted to the Graveyard. Once the
/// lease is past its expiry and grace, the Graveyard's claim writes the record to it again. No
/// one else controlled the record in between, so the hold carries from the holder's own write.
/// The composition reads no expiry here, so the fixture keeps the lease's.
#[test]
fn a_eth_name_wrapped_to_the_graveyard_then_claimed_keeps_the_first_claim() {
    let mut input = held_then_moved();
    let mut custody = event(
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        LEASE,
        80,
        json!({"authority_kind": "registrar", "source_event": "Transfer",
               "to_address": NAME_WRAPPER, "transaction_hash": "0xrewrap"}),
    );
    custody.position.log_index = Some(0);
    input.events.push(custody);
    let mut wrap = write(80, LEASE, NAME_WRAPPER, None);
    wrap.position.log_index = Some(1);
    input.writes.push(wrap);
    let mut mint = wrapper_transfer(80, GRAVEYARD);
    mint.resource_id = Some(REWRAPPED.to_owned());
    mint.position.log_index = Some(3);
    input.events.push(mint);
    input
        .writes
        .push(write(LATER_BLOCK, LEASE, GRAVEYARD, Some("graveyard")));
    assert_held(
        &read_at_reclaim(&input, vec![rewrap_binding(80)]),
        Some(HOLDER),
    );
}

/// After ruling 4's hold and the token's move, its new holder calls `reclaim` to the Graveyard.
/// The record was already the Graveyard's and the token controlled nothing, so there is no write
/// between the two Graveyard writes: the hold carries from the holder's own write.
#[test]
fn a_reclaim_to_the_graveyard_after_a_token_move_keeps_the_first_claim() {
    let mut input = held_then_moved();
    input
        .writes
        .push(reclaim_block_write(0, 2, GRAVEYARD, Some("graveyard")));
    assert_held(&read_at_reclaim(&input, Vec::new()), Some(HOLDER));
}

/// Holder recovery after an old wrap: the subname was wrapped to `HOLDER` and unwrapped to
/// `NEXT_OWNER`, who wrote it to the Graveyard. A parent's `setSubnodeOwner` writes it to the
/// NameWrapper, wrapping nothing, and then to the Graveyard. The hold ends at the later write,
/// and the old wrap's holder, who lost control at the unwrap, is no holder: released at the later
/// write with none.
#[test]
fn an_old_wraps_holder_is_not_the_holder_after_a_bare_name_wrapper_write() {
    let mut facts = held_subname(
        &[
            (8, 0, 2, NAME_WRAPPER),
            (20, 0, 2, NEXT_OWNER),
            (80, 0, 0, NAME_WRAPPER),
            (RECLAIM_BLOCK, 0, 2, GRAVEYARD),
        ],
        &[(8, 0, 3, HOLDER)],
    );
    facts.block_seconds = Arc::new(BTreeMap::from([
        (CLAIM_BLOCK, CLAIM_TIME),
        (RECLAIM_BLOCK, RECLAIM_TIME),
    ]));
    assert_held_at(&compose(&facts), None, RECLAIM_TIME);
}

/// A NameWrapper binding of a later wrap at `block`, under `REWRAPPED`, of the lease `LEASE`.
fn rewrap_binding(block: i64) -> BindingCandidate {
    binding(
        REWRAPPED,
        block,
        json!({"registry_only": false, "authority_kind": "wrapper",
               "emitting_address": NAME_WRAPPER, "wrapped_registrar_resource_id": LEASE,
               "node": "0x01", "transaction_hash": "0xrewrap"}),
    )
}

/// After ruling 4's hold and the token's move, its new holder `NEXT_OWNER` calls `reclaim`, which
/// ends the hold, then wraps the `.eth` name with the Graveyard as the wrapped owner
/// (`wrapETH2LD`): the BaseRegistrar token moves into the NameWrapper, whose `reclaim` writes the
/// record to it, and the wrapped token is minted to the Graveyard. The Graveyard's claim later
/// writes the record to it. The wrapper handed the name to the Graveyard, as a token sent to the
/// Graveyard leaves its sender: the holder is the lease's last holder outside the NameWrapper.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L277 @ ens_v1@91c966f)
#[test]
fn a_reclaim_then_a_wrap_to_the_graveyard_then_a_claim_names_the_reclaimer() {
    let mut input = held_then_moved();
    input.writes.push(write(80, LEASE, NEXT_OWNER, None));
    let mut custody = event(
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        LEASE,
        85,
        json!({"authority_kind": "registrar", "source_event": "Transfer",
               "to_address": NAME_WRAPPER, "transaction_hash": "0xrewrap"}),
    );
    custody.position.log_index = Some(0);
    input.events.push(custody);
    let mut wrap = write(85, LEASE, NAME_WRAPPER, None);
    wrap.position.log_index = Some(1);
    input.writes.push(wrap);
    let mut mint = wrapper_transfer(85, GRAVEYARD);
    mint.resource_id = Some(REWRAPPED.to_owned());
    mint.position.log_index = Some(3);
    input.events.push(mint);
    input
        .writes
        .push(reclaim_block_write(0, 2, GRAVEYARD, Some("graveyard")));
    assert_held_at(
        &read_at_reclaim(&input, vec![rewrap_binding(85)]),
        Some(NEXT_OWNER),
        RECLAIM_TIME,
    );
}

/// A subname whose record `NEXT_OWNER` holds (written by its parent at block 20), then the
/// registry `writes` and the wrapped token `transfers` on `REWRAPPED`, as in [`held_subname`],
/// with no Graveyard write before them.
fn owned_subname(
    writes: &[(i64, i64, i64, &str)],
    transfers: &[(i64, i64, i64, &str)],
) -> NameFacts {
    let mut facts = held_subname(writes, transfers);
    let node = facts.registry_node.as_mut().expect("the name has a node");
    node.owner_events
        .retain(|event| event.position.block_number != CLAIM_BLOCK);
    node.owner_events
        .push(write(20, REGISTRY_ONLY, NEXT_OWNER, None));
    facts
}

/// The holder of a subname wraps it with the Graveyard as the wrapped owner (`wrap` writes the
/// record to the NameWrapper and mints the token to the Graveyard), and the Graveyard, holding the
/// parent, clears it with `setSubnodeRecord`. The holder handed the name to the Graveyard, so it
/// is the lapsed owner.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L375 @ ens_v1@91c966f)
#[test]
fn a_subname_its_holder_wrapped_to_the_graveyard_names_that_holder() {
    let facts = owned_subname(
        &[(80, 0, 2, NAME_WRAPPER), (RECLAIM_BLOCK, 0, 2, GRAVEYARD)],
        &[(80, 0, 3, GRAVEYARD)],
    );
    assert_held_at(&compose(&facts), Some(NEXT_OWNER), RECLAIM_TIME);
}

/// The Graveyard's first re-parent of an unwrapped subname under a locked parent it holds: in one
/// transaction the NameWrapper's `setSubnodeRecord` writes the record to the NameWrapper and mints
/// the token to the Graveyard, and `unwrap` writes the record to the Graveyard. The record's owner
/// before the re-parent is the lapsed owner.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
#[test]
fn an_unwrapped_subname_re_parented_by_the_graveyard_names_its_registry_owner() {
    let facts = owned_subname(
        &[
            (RECLAIM_BLOCK, 0, 0, NAME_WRAPPER),
            (RECLAIM_BLOCK, 0, 4, GRAVEYARD),
        ],
        &[(RECLAIM_BLOCK, 0, 1, GRAVEYARD)],
    );
    assert_held_at(&compose(&facts), Some(NEXT_OWNER), RECLAIM_TIME);
}

/// The holder of a subname writes its record to the NameWrapper with `setOwner`, which wraps
/// nothing, and the Graveyard later clears it. The holder lost control at that write and no
/// wrapped token reached the Graveyard, so there is no lapsed owner.
#[test]
fn a_bare_name_wrapper_write_by_the_holder_names_no_lapsed_owner() {
    let facts = owned_subname(
        &[(80, 0, 2, NAME_WRAPPER), (RECLAIM_BLOCK, 0, 2, GRAVEYARD)],
        &[],
    );
    assert_held_at(&compose(&facts), None, RECLAIM_TIME);
}
