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
    let emitter = if family == V1_REGISTRAR {
        REGISTRAR
    } else {
        REGISTRY
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
    fixture
        .binding(
            &uuid(binding),
            &name(),
            &registry_only(),
            "ens_v1",
            block,
            1,
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
            1,
            kind,
            V1_REGISTRY,
            &registry_only(),
            after,
        )
        .await?;
    }
    Ok(())
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
