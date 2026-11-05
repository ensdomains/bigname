//! A name whose newest current-registry write names the admitted Graveyard serves its ENSv1
//! registration as ended: `released`, no expiry (`expires_at_reason: released`), no owner,
//! manager or registrant, released at the Graveyard's write that began its control, with the
//! holder who controlled the name before it as the lapsed owner. A name the Graveyard does not
//! hold composes as it did. `clear` only claims a `.eth` lease past its expiry and grace
//! (`Graveyard.sol` `_clear`, BaseRegistrar `available`). A live holder can also write its own
//! record to the Graveyard.
//!
//! The custody rows below each drive one sequence of the events the adapters emit: registry owner
//! writes, NameWrapper mints and transfers (adapters `wrapper.rs`, `wrapper/transfer.rs`) and
//! BaseRegistrar grants and transfers (adapters `registrar/token_transfer.rs`). The adapters
//! record no NameWrapper burn, no transfer of a token whose NameWrapper authority a registry write
//! ended, no BaseRegistrar transfer to or from zero, and no registry write that names the owner the
//! record already has (adapters `registry.rs`), so no row has one. A registry `NewOwner` also
//! leaves a `SubregistryChanged` row at its own log, even when its owner is unchanged. The fold
//! reads those rows only between a write and a mint, so a row carries one only where a `NewOwner`
//! whose write the adapter dropped sits. Each row is two tests: `<row>_serves` checks the served
//! fields and `<row>_trace` checks the controller after each step of the pass over the name's
//! custody events, written out by hand.
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
const MANAGER: &str = "0x00000000000000000000000000000000000000ee";
const PARENT_OWNER: &str = "0x00000000000000000000000000000000000000ff";
const REGISTRY_ONLY: &str = "00000000-0000-0000-0000-0000000000a0";
const LEASE: &str = "00000000-0000-0000-0000-0000000000b1";
/// The resources of a name's first and second wrap: each wrap is a new NameWrapper resource with
/// its own candidate (adapters `wrapper.rs`, `authority_key`).
const WRAPPED: &str = "00000000-0000-0000-0000-0000000000c1";
const REWRAPPED: &str = "00000000-0000-0000-0000-0000000000c2";
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
/// The BaseRegistrar's grace period.
const GRACE: i64 = 7_776_000;
const CLOCK: Clock = Clock {
    block_number: 100,
    timestamp_seconds: 1_900_000_000,
};
/// The controller the trace names for the Graveyard.
const IN_GRAVEYARD: Option<&str> = Some("graveyard");

/// The block time of `block` in the custody rows, on the line through the claim and the clock.
fn time(block: i64) -> i64 {
    1_650_000_000 + block * 2_500_000
}

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
               "before_registrant": HOLDER, "released_at": expiry + GRACE + 1,
               "expiry": expiry, "expiry_seconds": expiry}),
    )
}

fn binding(resource: &str, block: i64, extra: Value) -> BindingCandidate {
    let mut row = json!({
        "surface_binding_id": format!("binding-{resource}-{block}"),
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
        HANDOFF,
        json!({"registry_only": false, "authority_kind": "registrar"}),
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
            wrappers: BTreeMap::new(),
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

/// The composed reader's selection: the authority chosen from the candidates and history alone
/// (`name/selection.rs`).
fn with_selection(mut facts: NameFacts) -> NameFacts {
    facts.input.selection = AuthoritySelection::default();
    let readable: BTreeSet<String> = facts
        .candidates
        .iter()
        .map(|candidate| candidate.resource_id.clone())
        .collect();
    let decided = select(&facts, &CLOCK, None, None, &readable).expect("the name selects");
    facts.input.selection = decided.selection;
    facts
}

/// Select, then compose. Returns the selected arm and the composition.
fn selected(facts: NameFacts) -> (Option<String>, ShadowName) {
    let facts = with_selection(facts);
    (
        facts.input.selection.authority_arm.clone(),
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

/// A 2020-migrated name with no lease row, on its registry-only binding, serves `active` with
/// its registry owner until the claim. The claim is the plain `.eth` claim: `clear` registers the
/// lapsed name to the Graveyard, whose registry write is the only row it leaves.
#[test]
fn a_migrated_name_with_no_lease_row_is_released_by_the_claim() {
    let input = || Name::new(registry_only(HANDOFF), Vec::new());
    let unclaimed = input().read();
    assert_eq!(field(&unclaimed.registration, "lifecycle_status"), "active");
    assert_eq!(field(&unclaimed.control, "owner"), HOLDER);
    assert_held(&input().claimed().read(), Some(HOLDER));
}

/// The 2020 mint's lease, past grace, is released by its schedule with its expiry until the
/// claim.
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

/// A controller-registered lease, released past grace before the claim, loses its expiry, and
/// its release time is the claim's.
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

/// A registry-only record cleared to zero before the claim, then claimed: nobody held it when it
/// was claimed.
#[test]
fn a_record_cleared_before_the_claim_is_released_with_no_holder() {
    let mut input = Name::new(registry_only(CLAIM_BLOCK), Vec::new());
    input
        .writes
        .push(write(20, REGISTRY_ONLY, ZERO_ADDRESS, Some("literal_zero")));
    assert_held(&input.claimed().read(), None);
}

/// A name the ENSv2 arm decides is untouched by a Graveyard-held ENSv1 record.
fn assert_ens_v2_untouched(grant_block: i64, migration: Option<Position>) {
    let input = || {
        let mut input = Name::new(
            binding(
                ENS_V2,
                grant_block,
                json!({"authority_arm": "ens_v2", "registry_only": false,
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

/// A label registered in ENSv2 before the claim.
#[test]
fn an_ens_v2_registration_before_the_claim_is_untouched() {
    assert_ens_v2_untouched(40, None);
}

/// A label registered in ENSv2 after the claim, which selection gives the ENSv2 arm.
#[test]
fn an_ens_v2_registration_after_the_claim_is_untouched() {
    assert_ens_v2_untouched(70, None);
}

/// A migrated name. The migration hands its ENSv1 record to the Graveyard, which then never
/// claims it (`_clear` returns before `register`), and the name is on the ENSv2 arm.
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

/// A migrated name whose selection falls back to its ENSv1 binding. Its record is the
/// Graveyard's from the migration, which no claim follows.
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

/// A name only the 2017 registry recorded was never minted, so the Graveyard cannot claim it.
/// Its one owner write stands for the 2017 registry's record. With no Graveyard write the rule
/// does not apply.
#[test]
fn a_never_registered_name_is_untouched() {
    let mut input = Name::new(registry_only(HANDOFF), Vec::new());
    input.has_old_record = true;
    input.first_current_record_block = None;
    assert_not_held(&input.read());
}

/// A live lease never claimed.
#[test]
fn a_live_lease_is_untouched() {
    let shadow = Name::new(registrar_binding(), vec![lease_grant(8, LIVE_EXPIRY)]).read();
    assert_not_held(&shadow);
    assert_eq!(field(&shadow.registration, "lifecycle_status"), "active");
    assert_eq!(field(&shadow.control, "owner"), HOLDER);
}

/// A lease lapsed past grace and not yet claimed keeps its expiry: the null expiry is the
/// Graveyard's alone.
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

/// A live token its holder sent to the Graveyard keeps its lease: the rule keys on the registry
/// write, not on token custody.
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

/// A write naming a Graveyard the migration manifest does not admit, or written before its
/// declared start, carries no Graveyard reason and is an ordinary registry owner.
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

/// A live holder's `setOwner` of the registry record to the Graveyard is not a claim: `clear`
/// cannot claim a live lease, so the write is the holder's own. The record is the Graveyard's,
/// which never transfers it, and the name is served released, with the lease's holder as the
/// lapsed owner, until a newer registry write (the token holder's `reclaim`).
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

/// A Graveyard-held name whose one candidate is an open ENSv1 binding, with no other authority.
/// Selection gives it the ENSv1 arm, so the rule serves it released.
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

/// A Graveyard-held name whose one ENSv1 candidate is not open at the clock, with no arm from
/// history. Selection gives no arm, so the rule does not reach the name and the claim changes
/// nothing.
#[test]
fn a_held_name_whose_one_candidate_is_not_open_has_no_arm_and_is_untouched() {
    let input = || Name::new(registry_only(HANDOFF), Vec::new());
    let (arm, unclaimed) = selected(input().facts());
    assert!(arm.is_none(), "{arm:?}");
    let (arm, claimed) = selected(input().claimed().facts());
    assert!(arm.is_none(), "{arm:?}");
    assert_same_registration(&claimed, &unclaimed);
}

/// A Graveyard-held name with no candidate. Selection gives no arm, so the rule does not reach
/// the name and the claim changes nothing.
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

/// One custody event of a row, at (block, transaction, log).
#[derive(Clone, Copy)]
enum Step {
    /// A current-registry owner write naming the owner.
    Write(i64, i64, i64, &'static str),
    /// A NameWrapper wrap minting the token of the resource to the recipient (`NameWrapped`).
    Mint(i64, i64, i64, &'static str, &'static str),
    /// A NameWrapper transfer of the token of the resource to the recipient.
    Transfer(i64, i64, i64, &'static str, &'static str),
    /// A BaseRegistrar transfer of the `.eth` token to the recipient.
    Lease(i64, i64, i64, &'static str),
    /// A BaseRegistrar `NameRegistered` grant to the registrant, until the history's expiry.
    Grant(i64, i64, i64, &'static str),
    /// A BaseRegistrar `NameRegistered` grant to the registrant, until the expiry given.
    GrantUntil(i64, i64, i64, &'static str, i64),
    /// A registry `NewOwner` whose owner is unchanged: the adapter records only its
    /// `SubregistryChanged`.
    Subregistry(i64, i64, i64, &'static str),
}

impl Step {
    fn block(self) -> i64 {
        match self {
            Self::Write(block, ..)
            | Self::Mint(block, ..)
            | Self::Transfer(block, ..)
            | Self::Lease(block, ..)
            | Self::Grant(block, ..)
            | Self::GrantUntil(block, ..)
            | Self::Subregistry(block, ..) => block,
        }
    }
}

fn at(block: i64, transaction: i64, log: i64) -> Position {
    Position {
        block_number: block,
        transaction_index: Some(transaction),
        log_index: Some(log),
        event_identity: format!("{block}:{transaction}:{log}"),
    }
}

/// A row's history: where the name sits, its lease's expiry, and the block whose registry write
/// ended its last live wrap and opened the registry-only binding the composed reader selects, if
/// one did.
struct History {
    place: NamePlace,
    expiry: i64,
    handoff: Option<i64>,
}

/// A `.eth` second-level name on its BaseRegistrar binding.
fn eth(expiry: i64) -> History {
    History {
        place: NamePlace::EthSecondLevel,
        expiry,
        handoff: None,
    }
}

/// A subname of a `.eth` name, on its registry-only binding.
fn subname() -> History {
    History {
        place: NamePlace::BelowEthSecondLevel("ens:0x02".to_owned()),
        expiry: 0,
        handoff: None,
    }
}

impl History {
    /// The same history, selected over its closed NameWrapper candidates and the registry-only
    /// binding the registry write at `block` opened. That binding is the handoff Project marks
    /// registry-only, with the last NameWrapper candidate as its predecessor (crates/project
    /// `families/identity.rs`, `handoff`).
    fn handed_off_at(mut self, block: i64) -> Self {
        self.handoff = Some(block);
        self
    }

    fn facts(&self, steps: &[Step]) -> NameFacts {
        let two_ld = self.place == NamePlace::EthSecondLevel;
        let mut events = Vec::new();
        let mut writes = Vec::new();
        let mut wraps: Vec<(&str, i64)> = Vec::new();
        for &step in steps {
            match step {
                Step::Write(block, transaction, log, owner) => {
                    let reason = match owner {
                        GRAVEYARD => Some("graveyard"),
                        ZERO_ADDRESS => Some("literal_zero"),
                        _ => None,
                    };
                    let resource = if two_ld { LEASE } else { REGISTRY_ONLY };
                    let mut row = write(block, resource, owner, reason);
                    row.position = at(block, transaction, log);
                    writes.push(row);
                }
                Step::Mint(block, transaction, log, resource, to)
                | Step::Transfer(block, transaction, log, resource, to) => {
                    let source = if matches!(step, Step::Mint(..)) {
                        wraps.push((resource, block));
                        "NameWrapped"
                    } else {
                        "TransferSingle"
                    };
                    let mut row = event(
                        "TokenControlTransferred",
                        "ens_v1_wrapper_l1",
                        resource,
                        block,
                        json!({"authority_kind": "wrapper", "source_event": source,
                               "to_address": to}),
                    );
                    row.position = at(block, transaction, log);
                    events.push(row);
                }
                Step::Lease(block, transaction, log, to) => {
                    let mut row = event(
                        "TokenControlTransferred",
                        "ens_v1_registrar_l1",
                        LEASE,
                        block,
                        json!({"authority_kind": "registrar", "source_event": "Transfer",
                               "to_address": to}),
                    );
                    row.position = at(block, transaction, log);
                    events.push(row);
                }
                Step::Grant(block, transaction, log, registrant)
                | Step::GrantUntil(block, transaction, log, registrant, _) => {
                    let expiry = match step {
                        Step::GrantUntil(.., expiry) => expiry,
                        _ => self.expiry,
                    };
                    let mut row = event(
                        "RegistrationGranted",
                        "ens_v1_registrar_l1",
                        LEASE,
                        block,
                        json!({"authority_kind": "registrar", "source_event": "NameRegistered",
                               "registrant": registrant, "expiry": expiry,
                               "expiry_seconds": expiry}),
                    );
                    row.position = at(block, transaction, log);
                    events.push(row);
                }
                Step::Subregistry(block, transaction, log, owner) => {
                    let resource = if two_ld { LEASE } else { REGISTRY_ONLY };
                    let mut row = write(block, resource, owner, None);
                    row.event_kind = "SubregistryChanged".to_owned();
                    row.position = at(block, transaction, log);
                    writes.push(row);
                }
            }
        }
        let mut wrappers: Vec<BindingCandidate> = wraps
            .iter()
            .map(|&(resource, block)| {
                let mut candidate = binding(
                    resource,
                    block,
                    json!({"registry_only": false, "authority_kind": "wrapper",
                           "emitting_address": NAME_WRAPPER, "node": "0x01",
                           "transaction_hash": format!("0xwrap{block}")}),
                );
                if two_ld {
                    candidate.wrapped_registrar_resource_id = Some(LEASE.to_owned());
                }
                candidate
            })
            .collect();
        let main = match self.handoff {
            Some(block) => {
                for wrapper in &mut wrappers {
                    wrapper.active_from_seconds = Some(1_500_000_000.0);
                    wrapper.active_to_seconds = Some(time(block) as f64);
                }
                let predecessor = wraps.last().map(|&(resource, _)| resource);
                let mut handoff = binding(
                    REGISTRY_ONLY,
                    block,
                    json!({"state_derived": true, "registry_only": true,
                           "predecessor_resource_id": predecessor,
                           "surface_bound_position": {"block_number": block,
                                                      "transaction_index": 0, "log_index": 2,
                                                      "event_identity": "surface-bound"}}),
                );
                handoff.active_from_seconds = Some(time(block) as f64);
                handoff
            }
            None if two_ld => registrar_binding(),
            None => registry_only(HANDOFF),
        };
        let mut name = Name::new(main, events);
        name.place = self.place.clone();
        name.first_current_record_block = writes.first().map(|write| write.position.block_number);
        name.writes = writes;
        let mut facts = name.facts();
        let blocks: BTreeSet<i64> = steps.iter().map(|step| step.block()).collect();
        facts.block_seconds = Arc::new(
            blocks
                .into_iter()
                .map(|block| (block, time(block)))
                .collect(),
        );
        if self.handoff.is_some() {
            facts.candidates.splice(0..0, wrappers);
            with_selection(facts)
        } else {
            facts.candidates.extend(wrappers);
            facts
        }
    }
}

/// One custody row: the facts, the controller after each step, the lapsed holder and the block
/// whose time is `released_at`.
struct Row {
    facts: NameFacts,
    trace: Vec<(i64, i64, i64, Option<&'static str>)>,
    holder: Option<&'static str>,
    released_at: i64,
}

fn row(
    history: History,
    steps: &[Step],
    trace: &[(i64, i64, i64, Option<&'static str>)],
    holder: Option<&'static str>,
    released_block: i64,
) -> Row {
    Row {
        facts: history.facts(steps),
        trace: trace.to_vec(),
        holder,
        released_at: time(released_block),
    }
}

impl Row {
    fn serves(&self) {
        assert_eq!(
            self.facts.input.selection.authority_arm.as_deref(),
            Some("ens_v1")
        );
        let shadow = evaluate(&self.facts, &CLOCK).expect("the name composes");
        assert_held_at(&shadow, self.holder, self.released_at);
    }

    fn controller_trace(&self) {
        let actual: Vec<(i64, i64, i64, Option<String>)> = super::trace(&self.facts)
            .into_iter()
            .map(|(position, controller)| {
                (
                    position.block_number,
                    position.transaction_index.unwrap_or(-1),
                    position.log_index.unwrap_or(-1),
                    controller,
                )
            })
            .collect();
        let expected: Vec<(i64, i64, i64, Option<String>)> = self
            .trace
            .iter()
            .map(|&(block, transaction, log, controller)| {
                (block, transaction, log, controller.map(str::to_owned))
            })
            .collect();
        assert_eq!(actual, expected);
    }
}

/// Each row function gets its two tests: `<row>_serves` and `<row>_trace`.
macro_rules! custody_rows {
    ($($row:ident: $serves:ident, $trace:ident;)*) => {$(
        #[test]
        fn $serves() {
            $row().serves();
        }

        #[test]
        fn $trace() {
            $row().controller_trace();
        }
    )*};
}

use Step::{Grant, GrantUntil, Lease, Mint, Subregistry, Transfer, Write};

/// A lease that lapses past its grace between block 52 and the claim at block 60.
const CLAIMABLE_EXPIRY: i64 = 1_780_000_000;

/// The `.eth` claim: the lease lapses past grace and `clear` registers the name to the Graveyard,
/// whose BaseRegistrar burn and mint leave no rows and whose registry write is the claim.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L157-L164 @ ens_v2_mainnet_20261002@4f47cbcc)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
fn a_lapsed_eth_name_is_released_at_the_claim_to_its_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// The registry owner clears the record to zero (`setOwner`), then the claim: no one controlled
/// the name when the Graveyard took it, so there is no lapsed holder, whatever the lease says.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
fn a_lapsed_eth_name_cleared_to_zero_has_no_lapsed_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Write(40, 0, 2, ZERO_ADDRESS),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (40, 0, 2, None),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        None,
        60,
    )
}

/// A `.eth` name wrapped with `wrapETH2LD`: the BaseRegistrar token moves into the NameWrapper,
/// its `reclaim` writes the record to it and the wrapped token is minted. Past its expiry the
/// wrapped name reads no fuses, emancipated or locked alike, so the claim registers it, which
/// ends the NameWrapper authority and hands the name to a registry-only binding. The NameWrapper
/// is never the holder: the wrapped token's holder is.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L148-L164 @ ens_v2_mainnet_20261002@4f47cbcc)
fn a_wrapped_eth_name_is_released_to_its_wrapped_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY).handed_off_at(60),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(8, 0, 0, NAME_WRAPPER),
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (8, 0, 0, Some(HOLDER)),
            (8, 0, 1, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// A `.eth` name registered wrapped (`registerAndWrapETH2LD`): the BaseRegistrar writes the
/// record to the NameWrapper and grants it the lease before the NameWrapper mints the wrapped
/// token, in one transaction, so the write and the mint are one wrap step. Then the claim.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L303 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
fn a_name_registered_wrapped_is_released_to_its_wrapped_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY).handed_off_at(60),
        &[
            Write(5, 0, 1, NAME_WRAPPER),
            Grant(5, 0, 2, NAME_WRAPPER),
            Mint(5, 0, 4, WRAPPED, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// The live holder writes the record to the Graveyard (`setOwner`), then transfers the
/// BaseRegistrar token. The token controls nothing until a `reclaim`, so the Graveyard's control
/// carries and the holder is the one at the write.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
const HOLDERS_OWN_WRITE: [Step; 4] = [
    Write(5, 0, 1, HOLDER),
    Grant(5, 0, 2, HOLDER),
    Write(60, 0, 2, GRAVEYARD),
    Lease(70, 0, 1, NEXT_OWNER),
];
const HOLDERS_OWN_WRITE_TRACE: [(i64, i64, i64, Option<&str>); 4] = [
    (5, 0, 1, Some(HOLDER)),
    (5, 0, 2, Some(HOLDER)),
    (60, 0, 2, IN_GRAVEYARD),
    (70, 0, 1, IN_GRAVEYARD),
];

fn then<T: Copy>(first: &[T], rest: &[T]) -> Vec<T> {
    first.iter().chain(rest).copied().collect()
}

fn a_token_moved_after_its_holders_own_graveyard_write_keeps_the_holder_at_the_write() -> Row {
    row(
        eth(LIVE_EXPIRY),
        &HOLDERS_OWN_WRITE,
        &HOLDERS_OWN_WRITE_TRACE,
        Some(HOLDER),
        60,
    )
}

/// After that, the token's new holder, a contract, calls `reclaim` and then writes the record to
/// the Graveyard in one transaction. It controlled the record in between, so the Graveyard's
/// control restarts at the later write, with the reclaimer as the holder.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
fn a_reclaim_and_graveyard_write_in_one_transaction_start_a_new_hold() -> Row {
    row(
        eth(LIVE_EXPIRY),
        &then(
            &HOLDERS_OWN_WRITE,
            &[Write(90, 0, 0, NEXT_OWNER), Write(90, 0, 2, GRAVEYARD)],
        ),
        &then(
            &HOLDERS_OWN_WRITE_TRACE,
            &[(90, 0, 0, Some(NEXT_OWNER)), (90, 0, 2, IN_GRAVEYARD)],
        ),
        Some(NEXT_OWNER),
        90,
    )
}

/// The same with the reclaim and the Graveyard write in two transactions.
fn a_reclaim_then_a_graveyard_write_in_another_transaction_start_a_new_hold() -> Row {
    row(
        eth(LIVE_EXPIRY),
        &then(
            &HOLDERS_OWN_WRITE,
            &[Write(90, 0, 0, NEXT_OWNER), Write(90, 1, 0, GRAVEYARD)],
        ),
        &then(
            &HOLDERS_OWN_WRITE_TRACE,
            &[(90, 0, 0, Some(NEXT_OWNER)), (90, 1, 0, IN_GRAVEYARD)],
        ),
        Some(NEXT_OWNER),
        90,
    )
}

/// The token's new holder wraps the name to itself (`wrapETH2LD`) and unwraps it with the
/// Graveyard as the registry owner (`unwrapETH2LD`: `_unwrap` writes the record, then the
/// BaseRegistrar token is returned), in one transaction. The wrapped token's holder controlled
/// the name in between.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
fn a_holders_wrap_and_unwrap_to_the_graveyard_in_one_transaction_start_a_new_hold() -> Row {
    row(
        eth(LIVE_EXPIRY),
        &then(
            &HOLDERS_OWN_WRITE,
            &[
                Lease(90, 0, 0, NAME_WRAPPER),
                Write(90, 0, 1, NAME_WRAPPER),
                Mint(90, 0, 2, REWRAPPED, NEXT_OWNER),
                Write(90, 0, 4, GRAVEYARD),
                Lease(90, 0, 5, NEXT_OWNER),
            ],
        ),
        &then(
            &HOLDERS_OWN_WRITE_TRACE,
            &[
                (90, 0, 0, IN_GRAVEYARD),
                (90, 0, 1, Some(NEXT_OWNER)),
                (90, 0, 4, IN_GRAVEYARD),
                (90, 0, 5, IN_GRAVEYARD),
            ],
        ),
        Some(NEXT_OWNER),
        90,
    )
}

/// The token's new holder wraps the name with the Graveyard as the wrapped owner
/// (`wrapETH2LD`; the Graveyard is an ERC1155 holder) while the lease is live, and the Graveyard
/// claims it once it is past its expiry and grace. No one else controlled it in between.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
fn a_eth_name_wrapped_to_the_graveyard_then_claimed_keeps_the_first_claim() -> Row {
    row(
        eth(1_860_000_000),
        &then(
            &HOLDERS_OWN_WRITE,
            &[
                Lease(80, 0, 0, NAME_WRAPPER),
                Write(80, 0, 1, NAME_WRAPPER),
                Mint(80, 0, 2, REWRAPPED, GRAVEYARD),
                Write(95, 0, 2, GRAVEYARD),
            ],
        ),
        &then(
            &HOLDERS_OWN_WRITE_TRACE,
            &[
                (80, 0, 0, IN_GRAVEYARD),
                (80, 0, 1, IN_GRAVEYARD),
                (95, 0, 2, IN_GRAVEYARD),
            ],
        ),
        Some(HOLDER),
        60,
    )
}

/// The token's new holder calls `reclaim`, then wraps the name with the Graveyard as the wrapped
/// owner, and the Graveyard claims it once lapsed. The reclaimer handed it to the Graveyard.
fn a_reclaim_then_a_wrap_to_the_graveyard_then_a_claim_names_the_reclaimer() -> Row {
    row(
        eth(1_865_000_000),
        &then(
            &HOLDERS_OWN_WRITE,
            &[
                Write(80, 0, 2, NEXT_OWNER),
                Lease(85, 0, 0, NAME_WRAPPER),
                Write(85, 0, 1, NAME_WRAPPER),
                Mint(85, 0, 2, REWRAPPED, GRAVEYARD),
                Write(90, 0, 2, GRAVEYARD),
            ],
        ),
        &then(
            &HOLDERS_OWN_WRITE_TRACE,
            &[
                (80, 0, 2, Some(NEXT_OWNER)),
                (85, 0, 0, Some(NEXT_OWNER)),
                (85, 0, 1, IN_GRAVEYARD),
                (90, 0, 2, IN_GRAVEYARD),
            ],
        ),
        Some(NEXT_OWNER),
        90,
    )
}

/// The holder sets the record to `NEXT_OWNER` and calls `clear` in one transaction, keeping the
/// BaseRegistrar token. The lapsed holder of a `.eth` name is the token's holder when someone
/// controlled the name at the claim.
fn a_record_set_in_the_claims_transaction_names_the_token_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Write(60, 0, 0, NEXT_OWNER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 0, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// The holder sends the live token to the Graveyard (an ERC721 holder), which leaves its sender
/// the holder, then the claim once lapsed.
fn a_token_sent_to_the_graveyard_then_claimed_keeps_its_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(50, 0, 1, GRAVEYARD),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (50, 0, 1, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// The holder transfers the token without `reclaim`, and the lease lapses in the new holder's
/// hands. The token's holder is the lapsed holder.
fn a_token_moved_without_reclaim_names_its_new_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(30, 0, 1, NEXT_OWNER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (30, 0, 1, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// The holder transfers the token without `reclaim`. Its new holder wraps the name with the
/// Graveyard as the wrapped owner (`wrapETH2LD` takes the token from its holder, the
/// BaseRegistrar's `reclaim` writes the record to the NameWrapper and the wrapped token is
/// minted), and the Graveyard claims it once lapsed. The wrap moved the token into custody, so
/// the lease's holder is still the one who wrapped it.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
fn a_token_moved_without_reclaim_then_wrapped_to_the_graveyard_names_its_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY).handed_off_at(60),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(30, 0, 1, NEXT_OWNER),
            Lease(40, 0, 0, NAME_WRAPPER),
            Write(40, 0, 1, NAME_WRAPPER),
            Mint(40, 0, 2, WRAPPED, GRAVEYARD),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (30, 0, 1, Some(HOLDER)),
            (40, 0, 0, Some(HOLDER)),
            (40, 0, 1, IN_GRAVEYARD),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// The holder sets the registry owner to a manager and keeps the token. The token's holder is
/// the lapsed holder.
fn a_manager_set_record_names_the_token_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Write(30, 0, 2, MANAGER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (30, 0, 2, Some(MANAGER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// A wrapped `.eth` name whose wrapped token was transferred while unexpired, then claimed.
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L138-L152 @ ens_v1@91c966f)
fn a_wrapped_eth_name_transferred_is_released_to_its_new_wrapped_holder() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY).handed_off_at(60),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(8, 0, 0, NAME_WRAPPER),
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Transfer(30, 0, 1, WRAPPED, NEXT_OWNER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (8, 0, 0, Some(HOLDER)),
            (8, 0, 1, Some(HOLDER)),
            (30, 0, 1, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// A wrapped `.eth` name unwrapped to `NEXT_OWNER` (`unwrapETH2LD`: the record is written, then
/// the BaseRegistrar token is returned), whose lease then lapses, then claimed.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
fn a_wrapped_eth_name_unwrapped_is_released_to_the_unwrapper() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(8, 0, 0, NAME_WRAPPER),
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Write(40, 0, 1, NEXT_OWNER),
            Lease(40, 0, 2, NEXT_OWNER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (8, 0, 0, Some(HOLDER)),
            (8, 0, 1, Some(HOLDER)),
            (40, 0, 1, Some(NEXT_OWNER)),
            (40, 0, 2, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// A wrapped `.eth` name whose wrapped token was transferred, then unwrapped by its new holder with
/// the Graveyard as the registrant (`unwrapETH2LD` writes the record to the new holder, then sends
/// the BaseRegistrar token to the Graveyard, an ERC721 holder), then claimed once lapsed. The
/// wrapped token's last holder, who unwrapped it, is the lease's holder.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
fn a_wrapped_eth_name_unwrapped_to_the_graveyard_names_the_unwrapper() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(8, 0, 0, NAME_WRAPPER),
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Transfer(30, 0, 1, WRAPPED, NEXT_OWNER),
            Write(40, 0, 1, NEXT_OWNER),
            Lease(40, 0, 2, GRAVEYARD),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (8, 0, 0, Some(HOLDER)),
            (8, 0, 1, Some(HOLDER)),
            (30, 0, 1, Some(NEXT_OWNER)),
            (40, 0, 1, Some(NEXT_OWNER)),
            (40, 0, 2, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// The same unwrap with a third address as the registry owner (`unwrapETH2LD` takes the
/// controller and the registrant apart): the lease's holder is still the wrapped token's last
/// holder, ahead of the controller the unwrap wrote.
fn a_wrapped_eth_name_unwrapped_to_the_graveyard_for_another_controller_names_the_unwrapper() -> Row
{
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, HOLDER),
            Grant(5, 0, 2, HOLDER),
            Lease(8, 0, 0, NAME_WRAPPER),
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Transfer(30, 0, 1, WRAPPED, NEXT_OWNER),
            Write(40, 0, 1, MANAGER),
            Lease(40, 0, 2, GRAVEYARD),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(HOLDER)),
            (5, 0, 2, Some(HOLDER)),
            (8, 0, 0, Some(HOLDER)),
            (8, 0, 1, Some(HOLDER)),
            (30, 0, 1, Some(NEXT_OWNER)),
            (40, 0, 1, Some(MANAGER)),
            (40, 0, 2, Some(MANAGER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// A lapsed lease registered again, wrapped, by a new registrant (`registerAndWrapETH2LD`), who
/// unwraps it with the Graveyard as the registrant, then the claim once that lease lapses. The
/// earlier registration's holder is not the holder.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L303 @ ens_v1@91c966f)
fn a_name_registered_wrapped_again_then_unwrapped_to_the_graveyard_names_its_registrant() -> Row {
    row(
        eth(CLAIMABLE_EXPIRY),
        &[
            Write(5, 0, 1, MANAGER),
            GrantUntil(5, 0, 2, MANAGER, 1_700_000_000),
            Write(30, 0, 1, NAME_WRAPPER),
            Grant(30, 0, 2, NAME_WRAPPER),
            Mint(30, 0, 4, WRAPPED, NEXT_OWNER),
            Write(40, 0, 1, NEXT_OWNER),
            Lease(40, 0, 2, GRAVEYARD),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 1, Some(MANAGER)),
            (5, 0, 2, Some(MANAGER)),
            (30, 0, 1, Some(NEXT_OWNER)),
            (30, 0, 2, Some(NEXT_OWNER)),
            (40, 0, 1, Some(NEXT_OWNER)),
            (40, 0, 2, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// A subname the Graveyard cleared with `setSubnodeRecord` under a parent it holds.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L170-L172 @ ens_v2_mainnet_20261002@4f47cbcc)
fn a_cleared_subname_is_released_to_its_owner() -> Row {
    row(
        subname(),
        &[Write(5, 0, 2, HOLDER), Write(60, 0, 2, GRAVEYARD)],
        &[(5, 0, 2, Some(HOLDER)), (60, 0, 2, IN_GRAVEYARD)],
        Some(HOLDER),
        60,
    )
}

/// The wrap of a subname (`wrap`: the record is written to the NameWrapper, then the token is
/// minted).
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L375 @ ens_v1@91c966f)
const WRAP: [Step; 2] = [Write(8, 0, 1, NAME_WRAPPER), Mint(8, 0, 2, WRAPPED, HOLDER)];

/// A wrapped subname cleared with `setSubnodeRecord`: the write names the Graveyard, which ends
/// the NameWrapper authority (the token survives) and hands the name to a registry-only binding.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1080 @ ens_v1@91c966f)
fn a_cleared_wrapped_subname_is_released_to_its_wrapped_holder() -> Row {
    row(
        subname().handed_off_at(60),
        &then(&WRAP, &[Write(60, 0, 2, GRAVEYARD)]),
        &[(8, 0, 1, Some(HOLDER)), (60, 0, 2, IN_GRAVEYARD)],
        Some(HOLDER),
        60,
    )
}

/// The same when the wrap was emancipated and its expiry passed before the clear. Past its
/// expiry NameWrapper reads an emancipated or locked token's owner as zero and lets the parent
/// take the name, and it emits no event at the expiry. The wrapper adapter records the state
/// and expiry only from `NameWrapped`, `FusesSet` and `ExpiryExtended`. The last wrapped holder
/// is the holder, as a lapsed lease's holder is for a claimed `.eth` name.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L701-L730 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
/// (crates/adapters/src/schema_v2/protocol/v1/wrapper.rs, `wrapper_state` and `interpret`)
fn an_expired_emancipated_subname_cleared_later_names_its_last_wrapped_holder() -> Row {
    let mut row = row(
        subname().handed_off_at(60),
        &then(&WRAP, &[Write(60, 0, 2, GRAVEYARD)]),
        &[(8, 0, 1, Some(HOLDER)), (60, 0, 2, IN_GRAVEYARD)],
        Some(HOLDER),
        60,
    );
    row.facts.wrappers.insert(
        WRAPPED.to_owned(),
        WrapperRow {
            resource_id: WRAPPED.to_owned(),
            logical_name_id: Some(NAME.to_owned()),
            wrapper_state: Some("emancipated".to_owned()),
            fuses: Some(1 << 16),
            has_modifier: true,
            expiry_seconds: Some(time(30).to_string()),
            has_expiry: true,
            lifecycle_unwrapped: Some(false),
        },
    );
    row
}

/// The same after a transfer of the wrapped token.
fn a_cleared_wrapped_subname_is_released_to_its_new_wrapped_holder() -> Row {
    row(
        subname().handed_off_at(60),
        &then(
            &WRAP,
            &[
                Transfer(30, 0, 1, WRAPPED, NEXT_OWNER),
                Write(60, 0, 2, GRAVEYARD),
            ],
        ),
        &[
            (8, 0, 1, Some(HOLDER)),
            (30, 0, 1, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// The holder sends the wrapped token to the Graveyard (an ERC1155 holder) while the NameWrapper
/// owns the record. The Graveyard controls the name from then on, but nothing is released until
/// a Graveyard registry write, which is the claim. The sender is the holder.
fn a_wrapped_subname_sent_to_the_graveyard_is_released_at_the_clear_to_its_sender() -> Row {
    row(
        subname().handed_off_at(60),
        &then(
            &WRAP,
            &[
                Transfer(40, 0, 1, WRAPPED, GRAVEYARD),
                Write(60, 0, 2, GRAVEYARD),
            ],
        ),
        &[
            (8, 0, 1, Some(HOLDER)),
            (40, 0, 1, IN_GRAVEYARD),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// The Graveyard's re-parent of a wrapped subname without `PARENT_CANNOT_CONTROL` under a locked
/// parent it holds: the NameWrapper's `setSubnodeRecord` on the wrapped child writes the record
/// to the NameWrapper, which already owns it, and transfers the token to the Graveyard, then
/// `unwrap` burns it and writes the record to the Graveyard. The wrap's holder is the lapsed
/// holder.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L621-L628 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L415 @ ens_v1@91c966f)
const RE_PARENT: [Step; 5] = [
    Write(8, 0, 1, NAME_WRAPPER),
    Mint(8, 0, 2, WRAPPED, HOLDER),
    Subregistry(60, 0, 0, NAME_WRAPPER),
    Transfer(60, 0, 1, WRAPPED, GRAVEYARD),
    Write(60, 0, 3, GRAVEYARD),
];
const RE_PARENT_TRACE: [(i64, i64, i64, Option<&str>); 3] = [
    (8, 0, 1, Some(HOLDER)),
    (60, 0, 1, IN_GRAVEYARD),
    (60, 0, 3, IN_GRAVEYARD),
];

fn a_re_parented_wrapped_subname_is_released_to_its_wrapped_holder() -> Row {
    row(
        subname().handed_off_at(60),
        &RE_PARENT,
        &RE_PARENT_TRACE,
        Some(HOLDER),
        60,
    )
}

/// The same re-parent of an unwrapped child: `setSubnodeRecord` writes the record to the
/// NameWrapper and mints the token to the Graveyard, then `unwrap`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L611-L619 @ ens_v1@91c966f)
fn an_unwrapped_subname_re_parented_by_the_graveyard_is_released_to_its_owner() -> Row {
    row(
        subname(),
        &[
            Write(20, 0, 2, NEXT_OWNER),
            Write(90, 0, 0, NAME_WRAPPER),
            Mint(90, 0, 1, REWRAPPED, GRAVEYARD),
            Write(90, 0, 4, GRAVEYARD),
        ],
        &[
            (20, 0, 2, Some(NEXT_OWNER)),
            (90, 0, 0, IN_GRAVEYARD),
            (90, 0, 4, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        90,
    )
}

/// A later `clear` re-parents the cleared subname again under a new wrap. The Graveyard's
/// control carries.
fn a_re_parented_subname_cleared_again_keeps_the_first_claim_and_its_holder() -> Row {
    row(
        subname().handed_off_at(90),
        &then(
            &RE_PARENT,
            &[
                Write(90, 0, 0, NAME_WRAPPER),
                Mint(90, 0, 1, REWRAPPED, GRAVEYARD),
                Write(90, 0, 3, GRAVEYARD),
            ],
        ),
        &then(
            &RE_PARENT_TRACE,
            &[(90, 0, 0, IN_GRAVEYARD), (90, 0, 3, IN_GRAVEYARD)],
        ),
        Some(HOLDER),
        60,
    )
}

/// A subname the Graveyard holds under a wrapped parent `NEXT_OWNER` owns. In one transaction
/// `NEXT_OWNER` has the NameWrapper write the record to itself and mint the token to the
/// Graveyard (`setSubnodeRecord`), moves the token to itself (`setSubnodeOwner` on the now
/// wrapped child, which writes no record), and unwraps it to the Graveyard. `NEXT_OWNER`
/// controlled it in between.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L585 @ ens_v1@91c966f)
fn a_held_child_minted_to_the_graveyard_then_taken_out_starts_a_new_hold() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
            Write(90, 0, 0, NAME_WRAPPER),
            Mint(90, 0, 1, REWRAPPED, GRAVEYARD),
            Transfer(90, 0, 3, REWRAPPED, NEXT_OWNER),
            Write(90, 0, 6, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
            (90, 0, 0, IN_GRAVEYARD),
            (90, 0, 3, Some(NEXT_OWNER)),
            (90, 0, 6, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        90,
    )
}

/// The wrapped parent's owner wraps the held child to itself (`setSubnodeOwner`), sends the
/// token to the Graveyard, then, owning the unwrapped parent, writes the child's record to the
/// Graveyard through the registry.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
fn a_held_child_taken_out_then_sent_back_to_the_graveyard_starts_a_new_hold() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
            Write(90, 0, 0, NAME_WRAPPER),
            Mint(90, 0, 1, REWRAPPED, NEXT_OWNER),
            Transfer(90, 0, 3, REWRAPPED, GRAVEYARD),
            Write(90, 0, 6, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
            (90, 0, 0, Some(NEXT_OWNER)),
            (90, 0, 3, IN_GRAVEYARD),
            (90, 0, 6, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        90,
    )
}

/// The first two steps of a re-parent in one transaction (the record written to the NameWrapper
/// and the token minted to the Graveyard), the write back to the Graveyard in a later one.
fn a_held_child_minted_to_the_graveyard_and_written_back_later_keeps_the_first_claim() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
            Write(90, 0, 0, NAME_WRAPPER),
            Mint(90, 0, 1, REWRAPPED, GRAVEYARD),
            Write(95, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
            (90, 0, 0, IN_GRAVEYARD),
            (95, 0, 2, IN_GRAVEYARD),
        ],
        Some(HOLDER),
        60,
    )
}

/// A wrapped subname unwrapped to `NEXT_OWNER` (`unwrap`: the token is burned, then the record
/// written), which `NEXT_OWNER` writes to the Graveyard.
const UNWRAPPED_THEN_HELD: [Step; 4] = [
    Write(8, 0, 1, NAME_WRAPPER),
    Mint(8, 0, 2, WRAPPED, HOLDER),
    Write(20, 0, 2, NEXT_OWNER),
    Write(60, 0, 2, GRAVEYARD),
];
const UNWRAPPED_THEN_HELD_TRACE: [(i64, i64, i64, Option<&str>); 3] = [
    (8, 0, 1, Some(HOLDER)),
    (20, 0, 2, Some(NEXT_OWNER)),
    (60, 0, 2, IN_GRAVEYARD),
];

/// Then the parent writes the held child's record to the NameWrapper through the registry
/// (`setSubnodeOwner`), which wraps nothing, and to the Graveyard, in one transaction. No one
/// controlled the name in between.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
fn a_name_wrapper_write_with_no_wrap_starts_a_new_hold_with_no_holder() -> Row {
    row(
        subname(),
        &then(
            &UNWRAPPED_THEN_HELD,
            &[Write(90, 0, 0, NAME_WRAPPER), Write(90, 0, 2, GRAVEYARD)],
        ),
        &then(
            &UNWRAPPED_THEN_HELD_TRACE,
            &[(90, 0, 0, None), (90, 0, 2, IN_GRAVEYARD)],
        ),
        None,
        90,
    )
}

/// The same with the bare write and the clear in two transactions: the old wrap's holder lost
/// control at the unwrap and is no holder.
fn an_old_wraps_holder_is_not_the_holder_after_a_bare_name_wrapper_write() -> Row {
    row(
        subname(),
        &then(
            &UNWRAPPED_THEN_HELD,
            &[Write(80, 0, 2, NAME_WRAPPER), Write(90, 0, 2, GRAVEYARD)],
        ),
        &then(
            &UNWRAPPED_THEN_HELD_TRACE,
            &[(80, 0, 2, None), (90, 0, 2, IN_GRAVEYARD)],
        ),
        None,
        90,
    )
}

/// A wrapped subname unwrapped to `NEXT_OWNER`, who writes the record to the NameWrapper with
/// `setOwner` in the same transaction, then the clear. The old wrap's holder is not revived.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L415 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
fn an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder() -> Row {
    row(
        subname(),
        &[
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Write(80, 0, 0, NEXT_OWNER),
            Write(80, 0, 1, NAME_WRAPPER),
            Write(90, 0, 2, GRAVEYARD),
        ],
        &[
            (8, 0, 1, Some(HOLDER)),
            (80, 0, 0, Some(NEXT_OWNER)),
            (80, 0, 1, None),
            (90, 0, 2, IN_GRAVEYARD),
        ],
        None,
        90,
    )
}

/// In one transaction the record's owner, a contract, wraps the subname to itself (`wrap`),
/// unwraps it to itself (`unwrap`: the token is burned, then the record written), and writes the
/// record to the NameWrapper with `setOwner`, which wraps nothing. Then the clear. The wrap ended
/// at the unwrap, so its holder is not revived by the later write in the same transaction.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L375 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L415 @ ens_v1@91c966f)
fn a_wrap_an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(80, 0, 0, NAME_WRAPPER),
            Mint(80, 0, 1, WRAPPED, HOLDER),
            Write(80, 0, 3, HOLDER),
            Write(80, 0, 4, NAME_WRAPPER),
            Write(90, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (80, 0, 0, Some(HOLDER)),
            (80, 0, 3, Some(HOLDER)),
            (80, 0, 4, None),
            (90, 0, 2, IN_GRAVEYARD),
        ],
        None,
        90,
    )
}

/// The owner writes the subname's record to the NameWrapper with `setOwner`, which wraps
/// nothing. Later the Graveyard's re-parent finds no wrapped token, writes the record to the
/// NameWrapper, which already owns it, mints the token to itself and unwraps it. The mint is the
/// re-parent's first row. The owner lost control at its bare write, so there is no holder.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L611-L619 @ ens_v1@91c966f)
fn a_bare_name_wrapper_write_then_a_re_parent_names_no_holder() -> Row {
    row(
        subname(),
        &[
            Write(20, 0, 2, HOLDER),
            Write(80, 0, 2, NAME_WRAPPER),
            Subregistry(90, 0, 0, NAME_WRAPPER),
            Mint(90, 0, 1, REWRAPPED, GRAVEYARD),
            Write(90, 0, 4, GRAVEYARD),
        ],
        &[
            (20, 0, 2, Some(HOLDER)),
            (80, 0, 2, None),
            (90, 0, 1, IN_GRAVEYARD),
            (90, 0, 4, IN_GRAVEYARD),
        ],
        None,
        90,
    )
}

/// In one transaction the subname's owner, a contract, writes its record to the NameWrapper with
/// `setOwner`, which wraps nothing, and calls `clear` under a locked parent the Graveyard holds.
/// The re-parent's `setSubnodeRecord` writes the record to the NameWrapper again, which leaves only
/// a `SubregistryChanged`, then mints the token to the Graveyard and unwraps it. The mint is the
/// re-parent's, not the owner's write's: the owner lost control at its bare write.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L611-L619 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L58 @ ens_v1@91c966f)
fn an_owners_bare_name_wrapper_write_and_a_clear_in_one_transaction_name_no_holder() -> Row {
    row(
        subname(),
        &[
            Write(20, 0, 2, HOLDER),
            Write(90, 0, 0, NAME_WRAPPER),
            Subregistry(90, 0, 1, NAME_WRAPPER),
            Mint(90, 0, 2, REWRAPPED, GRAVEYARD),
            Write(90, 0, 4, GRAVEYARD),
        ],
        &[
            (20, 0, 2, Some(HOLDER)),
            (90, 0, 0, None),
            (90, 0, 2, IN_GRAVEYARD),
            (90, 0, 4, IN_GRAVEYARD),
        ],
        None,
        90,
    )
}

/// The owner wraps the subname with the Graveyard as the wrapped owner (`wrap`), and the
/// Graveyard, holding the parent, clears it. The owner handed it to the Graveyard.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L375 @ ens_v1@91c966f)
fn a_subname_its_holder_wrapped_to_the_graveyard_names_that_holder() -> Row {
    row(
        subname(),
        &[
            Write(20, 0, 2, NEXT_OWNER),
            Write(80, 0, 2, NAME_WRAPPER),
            Mint(80, 0, 3, REWRAPPED, GRAVEYARD),
            Write(90, 0, 2, GRAVEYARD),
        ],
        &[
            (20, 0, 2, Some(NEXT_OWNER)),
            (80, 0, 2, IN_GRAVEYARD),
            (90, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        90,
    )
}

/// A once-wrapped subname's owner writes its record to the NameWrapper with `setOwner`, which
/// wraps nothing, and the Graveyard clears it. No one controlled it at the clear.
fn a_bare_name_wrapper_write_by_the_owner_names_no_holder() -> Row {
    row(
        subname(),
        &[
            Write(8, 0, 1, NAME_WRAPPER),
            Mint(8, 0, 2, WRAPPED, HOLDER),
            Write(20, 0, 2, NEXT_OWNER),
            Write(80, 0, 2, NAME_WRAPPER),
            Write(90, 0, 2, GRAVEYARD),
        ],
        &[
            (8, 0, 1, Some(HOLDER)),
            (20, 0, 2, Some(NEXT_OWNER)),
            (80, 0, 2, None),
            (90, 0, 2, IN_GRAVEYARD),
        ],
        None,
        90,
    )
}

/// The owner sets the record to `NEXT_OWNER` and calls `clear` in one transaction.
fn a_record_set_earlier_in_the_claims_transaction_names_the_holder() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(60, 0, 0, NEXT_OWNER),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 0, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// A wrapped subname unwrapped to `NEXT_OWNER` (or its record overwritten by the parent's
/// `setSubnodeOwner`, which leaves the same rows), then cleared.
fn an_unwrapped_subname_cleared_later_keeps_its_registry_owner() -> Row {
    row(
        subname().handed_off_at(20),
        &UNWRAPPED_THEN_HELD,
        &UNWRAPPED_THEN_HELD_TRACE,
        Some(NEXT_OWNER),
        60,
    )
}

/// The parent's owner retakes the held child (`setSubnodeOwner`) and surrenders it to the
/// Graveyard in one transaction. The parent's owner controlled it in between, so the Graveyard's
/// control restarts.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
fn a_parent_retaking_and_surrendering_a_held_child_starts_a_new_hold() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(60, 0, 2, GRAVEYARD),
            Write(90, 0, 0, PARENT_OWNER),
            Write(90, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (60, 0, 2, IN_GRAVEYARD),
            (90, 0, 0, Some(PARENT_OWNER)),
            (90, 0, 2, IN_GRAVEYARD),
        ],
        Some(PARENT_OWNER),
        90,
    )
}

/// The parent deletes the wrapped child (`setSubnodeOwner` to zero: `_updateName` unwraps it to
/// zero, which burns the token and writes the record to zero), then the clear. No one controlled
/// it at the clear.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L927-L948 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
fn a_deleted_wrapped_subname_cleared_later_names_no_holder() -> Row {
    row(
        subname(),
        &then(
            &WRAP,
            &[Write(30, 0, 2, ZERO_ADDRESS), Write(60, 0, 2, GRAVEYARD)],
        ),
        &[
            (8, 0, 1, Some(HOLDER)),
            (30, 0, 2, None),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        None,
        60,
    )
}

/// The parent overwrites a wrapped child's record (`setSubnodeOwner` through the registry), which
/// ends the wrap and leaves its token stale, then writes it to the NameWrapper. On chain the stale
/// token is live again (`_isWrapped` reads the registry owner), and the Graveyard's re-parent
/// transfers it to the Graveyard. The adapter records no transfer of a token whose NameWrapper
/// authority a registry write ended (adapters `state.rs` `transfer_v1_wrapper_owner`,
/// `wrapper/transfer.rs`), so that transfer has no row and the record names no one before the
/// clear. This is a known limit (docs/upstream.md): the served holder is none, where on chain the
/// stale token's holder controlled the name.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1080 @ ens_v1@91c966f)
fn a_stale_wrap_revived_by_a_bare_name_wrapper_write_names_no_holder() -> Row {
    row(
        subname(),
        &then(
            &WRAP,
            &[
                Write(20, 0, 2, PARENT_OWNER),
                Write(40, 0, 2, NAME_WRAPPER),
                Subregistry(60, 0, 0, NAME_WRAPPER),
                Write(60, 0, 3, GRAVEYARD),
            ],
        ),
        &[
            (8, 0, 1, Some(HOLDER)),
            (20, 0, 2, Some(PARENT_OWNER)),
            (40, 0, 2, None),
            (60, 0, 3, IN_GRAVEYARD),
        ],
        None,
        60,
    )
}

/// The parent moves a wrapped child to `NEXT_OWNER` through the NameWrapper (`setSubnodeRecord`
/// on the wrapped child rewrites the record the NameWrapper already owns, then `_updateName`
/// transfers the token), then the clear.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L621-L628 @ ens_v1@91c966f)
fn a_wrapped_child_moved_by_its_parent_is_released_to_the_new_holder() -> Row {
    row(
        subname().handed_off_at(60),
        &then(
            &WRAP,
            &[
                Subregistry(30, 0, 1, NAME_WRAPPER),
                Transfer(30, 0, 2, WRAPPED, NEXT_OWNER),
                Write(60, 0, 2, GRAVEYARD),
            ],
        ),
        &[
            (8, 0, 1, Some(HOLDER)),
            (30, 0, 2, Some(NEXT_OWNER)),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        Some(NEXT_OWNER),
        60,
    )
}

/// The parent clears the child to zero through the registry, then the clear.
fn a_subname_cleared_to_zero_names_no_holder() -> Row {
    row(
        subname(),
        &[
            Write(5, 0, 2, HOLDER),
            Write(30, 0, 2, ZERO_ADDRESS),
            Write(60, 0, 2, GRAVEYARD),
        ],
        &[
            (5, 0, 2, Some(HOLDER)),
            (30, 0, 2, None),
            (60, 0, 2, IN_GRAVEYARD),
        ],
        None,
        60,
    )
}

custody_rows! {
    an_expired_emancipated_subname_cleared_later_names_its_last_wrapped_holder: an_expired_emancipated_subname_cleared_later_names_its_last_wrapped_holder_serves, an_expired_emancipated_subname_cleared_later_names_its_last_wrapped_holder_trace;
    a_wrapped_eth_name_unwrapped_to_the_graveyard_for_another_controller_names_the_unwrapper: a_wrapped_eth_name_unwrapped_to_the_graveyard_for_another_controller_names_the_unwrapper_serves, a_wrapped_eth_name_unwrapped_to_the_graveyard_for_another_controller_names_the_unwrapper_trace;
    an_owners_bare_name_wrapper_write_and_a_clear_in_one_transaction_name_no_holder: an_owners_bare_name_wrapper_write_and_a_clear_in_one_transaction_name_no_holder_serves, an_owners_bare_name_wrapper_write_and_a_clear_in_one_transaction_name_no_holder_trace;
    a_wrapped_eth_name_unwrapped_to_the_graveyard_names_the_unwrapper: a_wrapped_eth_name_unwrapped_to_the_graveyard_names_the_unwrapper_serves, a_wrapped_eth_name_unwrapped_to_the_graveyard_names_the_unwrapper_trace;
    a_name_registered_wrapped_again_then_unwrapped_to_the_graveyard_names_its_registrant: a_name_registered_wrapped_again_then_unwrapped_to_the_graveyard_names_its_registrant_serves, a_name_registered_wrapped_again_then_unwrapped_to_the_graveyard_names_its_registrant_trace;
    a_lapsed_eth_name_is_released_at_the_claim_to_its_holder: a_lapsed_eth_name_is_released_at_the_claim_to_its_holder_serves, a_lapsed_eth_name_is_released_at_the_claim_to_its_holder_trace;
    a_lapsed_eth_name_cleared_to_zero_has_no_lapsed_holder: a_lapsed_eth_name_cleared_to_zero_has_no_lapsed_holder_serves, a_lapsed_eth_name_cleared_to_zero_has_no_lapsed_holder_trace;
    a_wrapped_eth_name_is_released_to_its_wrapped_holder: a_wrapped_eth_name_is_released_to_its_wrapped_holder_serves, a_wrapped_eth_name_is_released_to_its_wrapped_holder_trace;
    a_name_registered_wrapped_is_released_to_its_wrapped_holder: a_name_registered_wrapped_is_released_to_its_wrapped_holder_serves, a_name_registered_wrapped_is_released_to_its_wrapped_holder_trace;
    a_token_moved_after_its_holders_own_graveyard_write_keeps_the_holder_at_the_write: a_token_moved_after_its_holders_own_graveyard_write_keeps_the_holder_at_the_write_serves, a_token_moved_after_its_holders_own_graveyard_write_keeps_the_holder_at_the_write_trace;
    a_reclaim_and_graveyard_write_in_one_transaction_start_a_new_hold: a_reclaim_and_graveyard_write_in_one_transaction_start_a_new_hold_serves, a_reclaim_and_graveyard_write_in_one_transaction_start_a_new_hold_trace;
    a_reclaim_then_a_graveyard_write_in_another_transaction_start_a_new_hold: a_reclaim_then_a_graveyard_write_in_another_transaction_start_a_new_hold_serves, a_reclaim_then_a_graveyard_write_in_another_transaction_start_a_new_hold_trace;
    a_holders_wrap_and_unwrap_to_the_graveyard_in_one_transaction_start_a_new_hold: a_holders_wrap_and_unwrap_to_the_graveyard_in_one_transaction_start_a_new_hold_serves, a_holders_wrap_and_unwrap_to_the_graveyard_in_one_transaction_start_a_new_hold_trace;
    a_eth_name_wrapped_to_the_graveyard_then_claimed_keeps_the_first_claim: a_eth_name_wrapped_to_the_graveyard_then_claimed_keeps_the_first_claim_serves, a_eth_name_wrapped_to_the_graveyard_then_claimed_keeps_the_first_claim_trace;
    a_reclaim_then_a_wrap_to_the_graveyard_then_a_claim_names_the_reclaimer: a_reclaim_then_a_wrap_to_the_graveyard_then_a_claim_names_the_reclaimer_serves, a_reclaim_then_a_wrap_to_the_graveyard_then_a_claim_names_the_reclaimer_trace;
    a_record_set_in_the_claims_transaction_names_the_token_holder: a_record_set_in_the_claims_transaction_names_the_token_holder_serves, a_record_set_in_the_claims_transaction_names_the_token_holder_trace;
    a_token_sent_to_the_graveyard_then_claimed_keeps_its_holder: a_token_sent_to_the_graveyard_then_claimed_keeps_its_holder_serves, a_token_sent_to_the_graveyard_then_claimed_keeps_its_holder_trace;
    a_token_moved_without_reclaim_names_its_new_holder: a_token_moved_without_reclaim_names_its_new_holder_serves, a_token_moved_without_reclaim_names_its_new_holder_trace;
    a_token_moved_without_reclaim_then_wrapped_to_the_graveyard_names_its_holder: a_token_moved_without_reclaim_then_wrapped_to_the_graveyard_names_its_holder_serves, a_token_moved_without_reclaim_then_wrapped_to_the_graveyard_names_its_holder_trace;
    a_manager_set_record_names_the_token_holder: a_manager_set_record_names_the_token_holder_serves, a_manager_set_record_names_the_token_holder_trace;
    a_wrapped_eth_name_transferred_is_released_to_its_new_wrapped_holder: a_wrapped_eth_name_transferred_is_released_to_its_new_wrapped_holder_serves, a_wrapped_eth_name_transferred_is_released_to_its_new_wrapped_holder_trace;
    a_wrapped_eth_name_unwrapped_is_released_to_the_unwrapper: a_wrapped_eth_name_unwrapped_is_released_to_the_unwrapper_serves, a_wrapped_eth_name_unwrapped_is_released_to_the_unwrapper_trace;
    a_cleared_subname_is_released_to_its_owner: a_cleared_subname_is_released_to_its_owner_serves, a_cleared_subname_is_released_to_its_owner_trace;
    a_cleared_wrapped_subname_is_released_to_its_wrapped_holder: a_cleared_wrapped_subname_is_released_to_its_wrapped_holder_serves, a_cleared_wrapped_subname_is_released_to_its_wrapped_holder_trace;
    a_cleared_wrapped_subname_is_released_to_its_new_wrapped_holder: a_cleared_wrapped_subname_is_released_to_its_new_wrapped_holder_serves, a_cleared_wrapped_subname_is_released_to_its_new_wrapped_holder_trace;
    a_wrapped_subname_sent_to_the_graveyard_is_released_at_the_clear_to_its_sender: a_wrapped_subname_sent_to_the_graveyard_is_released_at_the_clear_to_its_sender_serves, a_wrapped_subname_sent_to_the_graveyard_is_released_at_the_clear_to_its_sender_trace;
    a_re_parented_wrapped_subname_is_released_to_its_wrapped_holder: a_re_parented_wrapped_subname_is_released_to_its_wrapped_holder_serves, a_re_parented_wrapped_subname_is_released_to_its_wrapped_holder_trace;
    an_unwrapped_subname_re_parented_by_the_graveyard_is_released_to_its_owner: an_unwrapped_subname_re_parented_by_the_graveyard_is_released_to_its_owner_serves, an_unwrapped_subname_re_parented_by_the_graveyard_is_released_to_its_owner_trace;
    a_re_parented_subname_cleared_again_keeps_the_first_claim_and_its_holder: a_re_parented_subname_cleared_again_keeps_the_first_claim_and_its_holder_serves, a_re_parented_subname_cleared_again_keeps_the_first_claim_and_its_holder_trace;
    a_held_child_minted_to_the_graveyard_then_taken_out_starts_a_new_hold: a_held_child_minted_to_the_graveyard_then_taken_out_starts_a_new_hold_serves, a_held_child_minted_to_the_graveyard_then_taken_out_starts_a_new_hold_trace;
    a_held_child_taken_out_then_sent_back_to_the_graveyard_starts_a_new_hold: a_held_child_taken_out_then_sent_back_to_the_graveyard_starts_a_new_hold_serves, a_held_child_taken_out_then_sent_back_to_the_graveyard_starts_a_new_hold_trace;
    a_held_child_minted_to_the_graveyard_and_written_back_later_keeps_the_first_claim: a_held_child_minted_to_the_graveyard_and_written_back_later_keeps_the_first_claim_serves, a_held_child_minted_to_the_graveyard_and_written_back_later_keeps_the_first_claim_trace;
    a_name_wrapper_write_with_no_wrap_starts_a_new_hold_with_no_holder: a_name_wrapper_write_with_no_wrap_starts_a_new_hold_with_no_holder_serves, a_name_wrapper_write_with_no_wrap_starts_a_new_hold_with_no_holder_trace;
    an_old_wraps_holder_is_not_the_holder_after_a_bare_name_wrapper_write: an_old_wraps_holder_is_not_the_holder_after_a_bare_name_wrapper_write_serves, an_old_wraps_holder_is_not_the_holder_after_a_bare_name_wrapper_write_trace;
    an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder: an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder_serves, an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder_trace;
    a_wrap_an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder: a_wrap_an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder_serves, a_wrap_an_unwrap_and_a_bare_name_wrapper_write_in_one_transaction_name_no_holder_trace;
    a_bare_name_wrapper_write_then_a_re_parent_names_no_holder: a_bare_name_wrapper_write_then_a_re_parent_names_no_holder_serves, a_bare_name_wrapper_write_then_a_re_parent_names_no_holder_trace;
    a_subname_its_holder_wrapped_to_the_graveyard_names_that_holder: a_subname_its_holder_wrapped_to_the_graveyard_names_that_holder_serves, a_subname_its_holder_wrapped_to_the_graveyard_names_that_holder_trace;
    a_bare_name_wrapper_write_by_the_owner_names_no_holder: a_bare_name_wrapper_write_by_the_owner_names_no_holder_serves, a_bare_name_wrapper_write_by_the_owner_names_no_holder_trace;
    a_record_set_earlier_in_the_claims_transaction_names_the_holder: a_record_set_earlier_in_the_claims_transaction_names_the_holder_serves, a_record_set_earlier_in_the_claims_transaction_names_the_holder_trace;
    an_unwrapped_subname_cleared_later_keeps_its_registry_owner: an_unwrapped_subname_cleared_later_keeps_its_registry_owner_serves, an_unwrapped_subname_cleared_later_keeps_its_registry_owner_trace;
    a_parent_retaking_and_surrendering_a_held_child_starts_a_new_hold: a_parent_retaking_and_surrendering_a_held_child_starts_a_new_hold_serves, a_parent_retaking_and_surrendering_a_held_child_starts_a_new_hold_trace;
    a_deleted_wrapped_subname_cleared_later_names_no_holder: a_deleted_wrapped_subname_cleared_later_names_no_holder_serves, a_deleted_wrapped_subname_cleared_later_names_no_holder_trace;
    a_stale_wrap_revived_by_a_bare_name_wrapper_write_names_no_holder: a_stale_wrap_revived_by_a_bare_name_wrapper_write_names_no_holder_serves, a_stale_wrap_revived_by_a_bare_name_wrapper_write_names_no_holder_trace;
    a_wrapped_child_moved_by_its_parent_is_released_to_the_new_holder: a_wrapped_child_moved_by_its_parent_is_released_to_the_new_holder_serves, a_wrapped_child_moved_by_its_parent_is_released_to_the_new_holder_trace;
    a_subname_cleared_to_zero_names_no_holder: a_subname_cleared_to_zero_names_no_holder_serves, a_subname_cleared_to_zero_names_no_holder_trace;
}

/// A wrapped subname moved and unwrapped and never written to the Graveyard is not held. The
/// pass over its custody events finds no claim, and the read skips the pass, since only a
/// registry write sets the record and its latest one names an ordinary owner.
#[test]
fn a_name_never_written_to_the_graveyard_skips_the_pass_and_is_untouched() {
    let facts = subname().handed_off_at(40).facts(&then(
        &WRAP,
        &[
            Transfer(30, 0, 1, WRAPPED, NEXT_OWNER),
            Write(40, 0, 1, MANAGER),
        ],
    ));
    assert!(super::fold(&facts).is_some_and(|fold| fold.claim.is_none()));
    assert!(super::claim(&facts).is_none());
    assert_not_held(&evaluate(&facts, &CLOCK).expect("the name composes"));
}
