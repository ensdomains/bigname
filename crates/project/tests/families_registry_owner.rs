//! The served registry owner and registration of an ENSv1 name through registry ownership
//! changes, composed from the families. Each fixture writes the facts the ENSv1 adapters emit for
//! the Sepolia histories it is named after, in separate blocks: a registrar lease whose registry
//! record moves to another owner (a registry-only binding) and back to the lease by a registrar
//! token transfer (gymbaja.eth, TYR-88), and a registry-only binding followed, after a reclaim that
//! opens no binding, by another registry-only binding (howdy.eth, TYR-87). A registered name whose
//! registry owner the families cannot produce is an integrity failure and is not published.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::{ErrorKind, families::FamilyMode};
use bigname_storage::families::name::load_family_name;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const NAME_WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const V1_WRAPPER: &str = "ens_v1_wrapper_l1";
const ETH_NODE: &str = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b2";
const CAROL: &str = "0x00000000000000000000000000000000000000c3";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_REGISTRY: &str = "ens_v1_registry_l1";
const EXPIRY: i64 = 2_100_000_000;

fn node() -> String {
    format!("0x{:064x}", 0x61_u64)
}

fn name() -> String {
    format!("ens:{}", node())
}

fn lease() -> String {
    uuid(1)
}

fn registry_only() -> String {
    uuid(2)
}

async fn write(
    fixture: &Fixture,
    block: i64,
    log: i64,
    kind: &str,
    family: &str,
    resource: &str,
    after: Value,
) -> Result<()> {
    let emitter = match family {
        V1_REGISTRAR => REGISTRAR,
        V1_WRAPPER => NAME_WRAPPER,
        _ => REGISTRY,
    };
    let name = name();
    fixture
        .write(
            block,
            log,
            kind,
            family,
            Some(&name),
            Some(resource),
            after,
            emitter,
        )
        .await?;
    Ok(())
}

/// A registration at `block`: the registry NewOwner the controller's `register` writes for the
/// registrant, then the registrar's grant on the lease with its SurfaceBound (the registry owner
/// it read as `owner_getter`), epoch and expiry, the lease binding closing at `closed_at`.
async fn registered(
    fixture: &Fixture,
    block: i64,
    registrant: &str,
    closed_at: Option<i64>,
) -> Result<()> {
    fixture
        .binding(&uuid(100), &name(), &lease(), "ens_v1", block, 2, closed_at)
        .await?;
    write(
        fixture,
        block,
        1,
        "AuthorityTransferred",
        V1_REGISTRY,
        &lease(),
        json!({"source_event": "NewOwner", "node": ETH_NODE, "child_node": node(),
               "owner": registrant, "owner_getter": registrant, "emitter_role": "registry"}),
    )
    .await?;
    let grant = json!({"authority_kind": "registrar", "authority_key": "lease",
                       "namehash": node(), "registrant": registrant, "expiry": EXPIRY,
                       "source_event": "NameRegistered"});
    let mut bound = grant.clone();
    bound["owner_getter"] = json!(registrant);
    bound["binding_kind"] = json!("declared_registry_path");
    bound["registry_contract"] = json!(REGISTRY);
    write(
        fixture,
        block,
        2,
        "SurfaceBound",
        V1_REGISTRAR,
        &lease(),
        bound,
    )
    .await?;
    write(
        fixture,
        block,
        2,
        "AuthorityEpochChanged",
        V1_REGISTRAR,
        &lease(),
        grant.clone(),
    )
    .await?;
    let mut granted = grant.clone();
    granted["status"] = json!("registered");
    write(
        fixture,
        block,
        2,
        "RegistrationGranted",
        V1_REGISTRAR,
        &lease(),
        granted,
    )
    .await?;
    write(
        fixture,
        block,
        2,
        "ExpiryChanged",
        V1_REGISTRAR,
        &lease(),
        grant,
    )
    .await?;
    Ok(())
}

/// A registry `Transfer` of the name's node to `owner` at `block`: the registry-only binding it
/// opens (`binding`, closing at `closed_at`) with its SurfaceBound, epoch and transfer.
async fn registry_transfer(
    fixture: &Fixture,
    binding: u32,
    block: i64,
    owner: &str,
    closed_at: Option<i64>,
) -> Result<()> {
    registry_transfer_at(fixture, binding, block, 1, owner, closed_at).await
}

/// [`registry_transfer`] at log `log` of `block`.
async fn registry_transfer_at(
    fixture: &Fixture,
    binding: u32,
    block: i64,
    log: i64,
    owner: &str,
    closed_at: Option<i64>,
) -> Result<()> {
    fixture
        .binding(
            &uuid(binding),
            &name(),
            &registry_only(),
            "ens_v1",
            block,
            log,
            closed_at,
        )
        .await?;
    let after = json!({"source_event": "Transfer", "node": node(), "owner": owner,
                       "owner_getter": owner, "emitter_role": "registry",
                       "authority_kind": "registry_only", "authority_key": "registry-only"});
    let mut bound = after.clone();
    bound["binding_kind"] = json!("declared_registry_path");
    bound["registry_contract"] = json!(REGISTRY);
    for (kind, after) in [
        ("SurfaceBound", bound),
        ("AuthorityEpochChanged", after.clone()),
        ("AuthorityTransferred", after),
    ] {
        write(
            fixture,
            block,
            log,
            kind,
            V1_REGISTRY,
            &registry_only(),
            after,
        )
        .await?;
    }
    Ok(())
}

/// The registry's read-anchor resource of the name's node: the adapter anchors a zero-equivalent
/// registry owner write there while the registrar lease stays the selected authority, so the
/// families do not admit it for the name.
fn read_anchor() -> String {
    uuid(3)
}

/// A registry write at `block`/`log` that leaves `owner(node)` answering zero while the lease
/// stays selected: the AuthorityTransferred the adapter anchors on the read anchor, with the raw
/// `owner` word and the getter view (zero) and its reason.
async fn zero_equivalent_transfer(
    fixture: &Fixture,
    block: i64,
    log: i64,
    source_event: &str,
    owner: &str,
    reason: &str,
) -> Result<()> {
    let mut after = json!({"source_event": source_event, "node": node(), "owner": owner,
                           "owner_getter": ZERO, "owner_getter_reason": reason,
                           "emitter_role": "registry"});
    if source_event == "NewOwner" {
        after["node"] = json!(ETH_NODE);
        after["child_node"] = json!(node());
    }
    write(
        fixture,
        block,
        log,
        "AuthorityTransferred",
        V1_REGISTRY,
        &read_anchor(),
        after,
    )
    .await
}

/// The composed name's registration and control fields the tests compare.
async fn served(fixture: &Fixture) -> Result<Value> {
    let row = load_family_name(&fixture.pool, &name())
        .await?
        .ok_or_else(|| anyhow::anyhow!("no composed row"))?;
    let summary = &row.declared_summary;
    Ok(json!({
        "status": summary["registration"]["status"],
        "authority_kind": summary["registration"]["authority_kind"],
        "registered": !summary["registration"]["registered_at"].is_null(),
        "expiry": summary["registration"]["expiry"],
        "owner": summary["control"]["registry_owner"],
    }))
}

#[tokio::test]
async fn a_registrar_transfer_back_to_the_lease_keeps_the_owner_the_registry_transfer_set()
-> Result<()> {
    let fixture = Fixture::new("families_registry_owner_back_to_lease", 20).await?;
    registered(&fixture, 10, ALICE, Some(11)).await?;
    registry_transfer(&fixture, 101, 11, BOB, Some(12)).await?;
    // The registrar token moves to BOB: the lease is bound again. The registrar adapter's
    // SurfaceBound reads the registry owner (BOB) as its owner getter; the epoch and the token
    // transfer say nothing about the registry owner.
    fixture
        .binding(&uuid(102), &name(), &lease(), "ens_v1", 12, 1, None)
        .await?;
    write(
        &fixture,
        12,
        1,
        "SurfaceBound",
        V1_REGISTRAR,
        &lease(),
        json!({"authority_kind": "registrar", "authority_key": "lease", "owner_getter": BOB,
               "source_event": "Transfer", "binding_kind": "declared_registry_path",
               "registry_contract": REGISTRY}),
    )
    .await?;
    write(
        &fixture,
        12,
        1,
        "TokenControlTransferred",
        V1_REGISTRAR,
        &lease(),
        json!({"to": BOB, "namehash": node(), "source_event": "Transfer"}),
    )
    .await?;
    write(
        &fixture,
        12,
        1,
        "AuthorityEpochChanged",
        V1_REGISTRAR,
        &lease(),
        json!({"authority_kind": "registrar", "authority_key": "lease",
               "source_event": "Transfer"}),
    )
    .await?;

    fixture.apply(10, FamilyMode::Normal).await?;
    ensure!(served(&fixture).await?["owner"] == json!(ALICE));
    fixture.apply(11, FamilyMode::Normal).await?;
    let at_transfer = served(&fixture).await?;
    ensure!(
        at_transfer["owner"] == json!(BOB),
        "the registry transfer: {at_transfer}"
    );
    fixture.apply(12, FamilyMode::Normal).await?;
    let back = served(&fixture).await?;
    ensure!(
        back == json!({"status": "active", "authority_kind": "registrar", "registered": true,
                       "expiry": EXPIRY, "owner": BOB}),
        "the registrar transfer back to the lease lost the registry owner: {back}"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn an_explicit_zero_registry_owner_is_served_as_zero() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_zero", 20).await?;
    registered(&fixture, 10, ALICE, None).await?;
    // The registrant sets the registry owner to zero; the lease stays bound.
    write(
        &fixture,
        11,
        1,
        "AuthorityTransferred",
        V1_REGISTRY,
        &lease(),
        json!({"source_event": "Transfer", "node": node(), "owner": ZERO, "owner_getter": ZERO,
               "emitter_role": "registry"}),
    )
    .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let row = served(&fixture).await?;
    ensure!(
        row["owner"] == json!(ZERO) && row["status"] == json!("active"),
        "{row}"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_second_registry_only_binding_keeps_the_registration_of_the_first() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_chained", 20).await?;
    registered(&fixture, 10, ALICE, Some(11)).await?;
    registry_transfer(&fixture, 101, 11, BOB, Some(12)).await?;
    // A `reclaim` writes a registry NewOwner on the lease; the adapter closes the registry-only
    // binding and opens none (howdy.eth at Sepolia block 10184681).
    let reclaim = json!({"source_event": "NewOwner", "node": ETH_NODE, "child_node": node(),
                         "owner": ALICE, "owner_getter": ALICE, "emitter_role": "registry",
                         "authority_kind": "registrar", "authority_key": "lease"});
    for kind in ["AuthorityTransferred", "AuthorityEpochChanged"] {
        write(
            &fixture,
            12,
            1,
            kind,
            V1_REGISTRY,
            &lease(),
            reclaim.clone(),
        )
        .await?;
    }
    registry_transfer(&fixture, 102, 13, CAROL, None).await?;

    fixture.apply(11, FamilyMode::Normal).await?;
    let first = served(&fixture).await?;
    ensure!(
        first
            == json!({"status": "active", "authority_kind": "registry_only", "registered": true,
                       "expiry": EXPIRY, "owner": BOB}),
        "the first registry-only binding: {first}"
    );
    fixture.apply(13, FamilyMode::Normal).await?;
    let second = served(&fixture).await?;
    ensure!(
        second
            == json!({"status": "active", "authority_kind": "registry_only", "registered": true,
                       "expiry": EXPIRY, "owner": CAROL}),
        "the second registry-only binding lost the registration: {second}"
    );
    let handoffs: Vec<Value> = fixture
        .rows("project_binding_candidate")
        .await?
        .into_iter()
        .filter(|row| row["registry_only"] == json!(true))
        .map(|row| json!([row["predecessor_resource_id"], row["lease_resource_id"]]))
        .collect();
    ensure!(
        handoffs == vec![json!([lease(), lease()]), json!([lease(), lease()])],
        "both registry-only bindings stand for the lease: {handoffs:?}"
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn a_registered_name_without_a_registry_record_serves_the_zero_owner() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_no_record", 20).await?;
    // A grant with no registry write (`registerOnly`) and no owner getter: the registry answers
    // zero for a node it holds no record of.
    fixture
        .binding(&uuid(100), &name(), &lease(), "ens_v1", 10, 2, None)
        .await?;
    let grant = json!({"authority_kind": "registrar", "authority_key": "lease",
                       "namehash": node(), "registrant": ALICE, "expiry": EXPIRY});
    write(
        &fixture,
        10,
        2,
        "SurfaceBound",
        V1_REGISTRAR,
        &lease(),
        grant.clone(),
    )
    .await?;
    let mut granted = grant.clone();
    granted["status"] = json!("registered");
    write(
        &fixture,
        10,
        2,
        "RegistrationGranted",
        V1_REGISTRAR,
        &lease(),
        granted,
    )
    .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    let row = served(&fixture).await?;
    ensure!(
        row["owner"] == json!(ZERO) && row["status"] == json!("active"),
        "{row}"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_registered_name_whose_registry_record_has_no_owner_is_not_published() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_missing", 20).await?;
    fixture
        .binding(&uuid(100), &name(), &lease(), "ens_v1", 10, 2, None)
        .await?;
    let grant = json!({"authority_kind": "registrar", "authority_key": "lease",
                       "namehash": node(), "registrant": ALICE, "expiry": EXPIRY});
    write(
        &fixture,
        10,
        2,
        "SurfaceBound",
        V1_REGISTRAR,
        &lease(),
        grant.clone(),
    )
    .await?;
    let mut granted = grant.clone();
    granted["status"] = json!("registered");
    write(
        &fixture,
        10,
        2,
        "RegistrationGranted",
        V1_REGISTRAR,
        &lease(),
        granted,
    )
    .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    let marker = fixture.marker().await?.0;
    // The node has a registry record (a subregistry change) but no owner-setting event the
    // families kept: the owner it has on chain is unknown.
    write(
        &fixture,
        11,
        1,
        "SubregistryChanged",
        V1_REGISTRY,
        &lease(),
        json!({"node": node(), "emitter_role": "registry"}),
    )
    .await?;
    let error = fixture
        .apply(11, FamilyMode::Normal)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("the block with an ownerless record was published"))?;
    ensure!(
        error.kind() == ErrorKind::DataIntegrity
            && error
                .to_string()
                .contains("no registry owner the families can serve"),
        "{error:?}"
    );
    ensure!(
        fixture.marker().await?.0 == marker,
        "the failed block moved the marker"
    );
    fixture.cleanup().await
}

/// `setOwner(node, 0)` by the registrant while the lease stays selected: the adapter anchors the
/// transfer on the registry read anchor, which the families do not admit for the name, and the
/// lease binding's SurfaceBound still records the earlier owner. The node's newer registry owner
/// supersedes that snapshot, in a later block and later in the registration's own transaction.
#[tokio::test]
async fn a_registry_clear_on_the_read_anchor_supersedes_the_lease_binding_snapshot() -> Result<()> {
    for (label, block, log) in [("later_block", 11, 1), ("same_transaction", 10, 5)] {
        let fixture = Fixture::new(&format!("families_registry_owner_clear_{label}"), 20).await?;
        registered(&fixture, 10, ALICE, None).await?;
        zero_equivalent_transfer(&fixture, block, log, "Transfer", ZERO, "literal_zero").await?;
        fixture.apply(10, FamilyMode::Normal).await?;
        fixture.apply(11, FamilyMode::Normal).await?;
        let row = served(&fixture).await?;
        ensure!(
            row == json!({"status": "active", "authority_kind": "registrar", "registered": true,
                          "expiry": EXPIRY, "owner": ZERO}),
            "{label}: the registry clear lost to the binding snapshot: {row}"
        );
        fixture.assert_undo_restores(11).await?;
        fixture.assert_rebuild_equal(11).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

/// `reclaim(id, registry)` on a lease with no other owner fact: the registrar's NewOwner names
/// the current registry itself, which `owner(node)` reads as zero. The fallback serves the
/// event's getter view, not the raw registry address.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
#[tokio::test]
async fn a_reclaim_to_the_registry_itself_serves_the_zero_owner() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_registry_self", 20).await?;
    fixture
        .binding(&uuid(100), &name(), &lease(), "ens_v1", 10, 2, None)
        .await?;
    let grant = json!({"authority_kind": "registrar", "authority_key": "lease",
                       "namehash": node(), "registrant": ALICE, "expiry": EXPIRY});
    write(
        &fixture,
        10,
        2,
        "SurfaceBound",
        V1_REGISTRAR,
        &lease(),
        grant.clone(),
    )
    .await?;
    let mut granted = grant.clone();
    granted["status"] = json!("registered");
    write(
        &fixture,
        10,
        2,
        "RegistrationGranted",
        V1_REGISTRAR,
        &lease(),
        granted,
    )
    .await?;
    zero_equivalent_transfer(&fixture, 11, 1, "NewOwner", REGISTRY, "registry_self").await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let row = served(&fixture).await?;
    ensure!(
        row["owner"] == json!(ZERO) && row["status"] == json!("active"),
        "the registry's own address was served as the owner: {row}"
    );
    fixture.cleanup().await
}

/// A successor grant earlier in the block of the second registry-only binding. The first
/// registry-only binding stands for the released lease; in one later block a `registerOnly`
/// grant on another resource comes first, then a registry transfer opens the second
/// registry-only binding, which carries the first one's handoff over. Both then stand for the
/// successor lease and the registration is the successor's, applied block by block, after an
/// undo, and in a ranged rebuild.
#[tokio::test]
async fn a_successor_grant_earlier_in_the_block_reaches_the_inherited_handoff() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_successor_same_block", 20).await?;
    let successor = uuid(4);
    let renewed = EXPIRY + 1_000_000;
    registered(&fixture, 10, ALICE, Some(11)).await?;
    registry_transfer(&fixture, 101, 11, BOB, Some(13)).await?;
    write(
        &fixture,
        12,
        1,
        "RegistrationReleased",
        V1_REGISTRAR,
        &lease(),
        json!({"namehash": node(), "source_event": "RegistrationReleased"}),
    )
    .await?;
    let grant = json!({"authority_kind": "registrar", "authority_key": "successor",
                       "namehash": node(), "registrant": CAROL, "expiry": renewed,
                       "source_event": "NameRegistered", "status": "registered"});
    for kind in ["RegistrationGranted", "ExpiryChanged"] {
        write(
            &fixture,
            13,
            1,
            kind,
            V1_REGISTRAR,
            &successor,
            grant.clone(),
        )
        .await?;
    }
    registry_transfer_at(&fixture, 102, 13, 5, ALICE, None).await?;

    fixture.apply(13, FamilyMode::Normal).await?;
    let mut leases: Vec<Value> = fixture
        .rows("project_binding_candidate")
        .await?
        .into_iter()
        .filter(|row| row["registry_only"] == json!(true))
        .map(|row| json!([row["surface_binding_id"], row["lease_resource_id"]]))
        .collect();
    leases.sort_by_key(Value::to_string);
    ensure!(
        leases
            == vec![
                json!([uuid(101), successor.clone()]),
                json!([uuid(102), successor.clone()])
            ],
        "both registry-only bindings stand for the successor lease: {leases:?}"
    );
    let row = served(&fixture).await?;
    ensure!(
        row == json!({"status": "active", "authority_kind": "registry_only", "registered": true,
                      "expiry": renewed, "owner": ALICE}),
        "the second registry-only binding reads the successor's registration: {row}"
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

/// Clear, reclaim, clear: the registrant clears the registry owner (a zero-equivalent write the
/// adapter anchors on the read anchor), reclaims it through the registrar (a registry NewOwner
/// the adapter links to the lease, which the families admit), then clears it again on the read
/// anchor. The reclaim is an actual owner write, not a snapshot, and the newer clear still sets
/// `owner(node)` to zero.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
#[tokio::test]
async fn a_clear_after_an_admitted_reclaim_serves_the_zero_owner() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_clear_reclaim_clear", 20).await?;
    registered(&fixture, 10, ALICE, None).await?;
    zero_equivalent_transfer(&fixture, 11, 1, "Transfer", ZERO, "literal_zero").await?;
    write(
        &fixture,
        12,
        1,
        "AuthorityTransferred",
        V1_REGISTRY,
        &lease(),
        json!({"source_event": "NewOwner", "node": ETH_NODE, "child_node": node(),
               "owner": ALICE, "owner_getter": ALICE, "emitter_role": "registry"}),
    )
    .await?;
    zero_equivalent_transfer(&fixture, 13, 1, "Transfer", ZERO, "literal_zero").await?;
    let mut served_at = Vec::new();
    for block in 11..=13 {
        fixture.apply(block, FamilyMode::Normal).await?;
        served_at.push(served(&fixture).await?["owner"].clone());
    }
    ensure!(
        served_at == vec![json!(ZERO), json!(ALICE), json!(ZERO)],
        "clear, reclaim, clear: {served_at:?}"
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

/// `unwrapETH2LD(label, ALICE, registry)` on a wrapped `.eth` name: the NameWrapper writes the
/// registry itself as the node's owner (which `owner(node)` reads as zero), emits `NameUnwrapped`
/// with that raw address, and then hands the registrar token to ALICE. The unwrap's authority
/// epoch reactivates the lease with the raw address as its owner; the served owner is the
/// registry write's getter view, zero.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
#[tokio::test]
async fn an_unwrap_to_the_registry_itself_serves_the_zero_owner() -> Result<()> {
    let fixture = Fixture::new("families_registry_owner_unwrap_registry_self", 20).await?;
    let wrapper = uuid(5);
    registered(&fixture, 10, ALICE, Some(11)).await?;
    // wrapETH2LD: the registrar reclaims the node for the NameWrapper, which binds its token.
    write(
        &fixture,
        11,
        1,
        "AuthorityTransferred",
        V1_REGISTRY,
        &lease(),
        json!({"source_event": "NewOwner", "node": ETH_NODE, "child_node": node(),
               "owner": NAME_WRAPPER, "owner_getter": NAME_WRAPPER, "emitter_role": "registry"}),
    )
    .await?;
    fixture
        .binding(&uuid(110), &name(), &wrapper, "ens_v1", 11, 3, Some(12))
        .await?;
    let wrapped = json!({"source_event": "NameWrapped", "node": node(), "owner": BOB,
                         "fuses": 0, "wrapper_state": "wrapped", "expiry": EXPIRY,
                         "authority_kind": "wrapper", "authority_key": "wrapper:key",
                         "surface_known": true, "binding_kind": "declared_registry_path"});
    for kind in ["SurfaceBound", "AuthorityEpochChanged"] {
        write(&fixture, 11, 3, kind, V1_WRAPPER, &wrapper, wrapped.clone()).await?;
    }
    // unwrapETH2LD: ens.setOwner(node, registry), then NameUnwrapped(node, registry), then the
    // registrar token to ALICE.
    zero_equivalent_transfer(&fixture, 12, 1, "Transfer", REGISTRY, "registry_self").await?;
    fixture
        .binding(&uuid(111), &name(), &lease(), "ens_v1", 12, 2, None)
        .await?;
    let unwrapped = json!({"source_event": "NameUnwrapped", "node": node(), "owner": REGISTRY,
                           "authority_kind": "registrar", "authority_key": "lease",
                           "binding_kind": "declared_registry_path",
                           "registry_contract": REGISTRY});
    for kind in ["SurfaceBound", "AuthorityEpochChanged"] {
        write(
            &fixture,
            12,
            2,
            kind,
            V1_WRAPPER,
            &lease(),
            unwrapped.clone(),
        )
        .await?;
    }
    write(
        &fixture,
        12,
        3,
        "TokenControlTransferred",
        V1_REGISTRAR,
        &lease(),
        json!({"from": NAME_WRAPPER, "to": ALICE, "namehash": node(),
               "source_event": "Transfer"}),
    )
    .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let wrapped_row = served(&fixture).await?;
    ensure!(
        wrapped_row["owner"] == json!(BOB),
        "a wrapped name serves the wrapper holder: {wrapped_row}"
    );
    fixture.apply(12, FamilyMode::Normal).await?;
    let row = served(&fixture).await?;
    ensure!(
        row == json!({"status": "active", "authority_kind": "registrar", "registered": true,
                      "expiry": EXPIRY, "owner": ZERO}),
        "the unwrap served the registry's own address: {row}"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
