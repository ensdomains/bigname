//! The owner an ENSv2 registration names. A PermissionedRegistry registration emits
//! LabelRegistered, mints the token with an ERC1155 TransferSingle from the zero address and
//! emits TokenResource; the adapter skips the mint and carries the owner on the AuthorityTransferred
//! it writes with the TokenResource grant (adapters protocol/v2_registry/transfer.rs,
//! `token_resource`). The composed control block takes the latest of that transfer, a later
//! ERC1155 transfer and an AuthorityEpochChanged on the selected lifecycle, so a name that was
//! registered and never transferred still has its owner.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L466-L471 @ ens_v2@a971bd64)
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Context, Result};
use bigname_project::families::FamilyMode;
use bigname_storage::{NameCurrentRow, families::name::load_family_name};
use serde_json::{Value, json};
use support::{Fixture, uuid};

const V2_REGISTRY: &str = "ens_v2_registry_l1";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const NAME: &str = "ens:0x0000000000000000000000000000000000000000000000000000000000000001";

fn instance() -> String {
    uuid(0x901)
}

/// One registration as the adapter writes it at `block`: LabelRegistered's grant at log 0 (its
/// resource still pending, so it is kept under the registry and token), then the TokenResource
/// log's SurfaceBound, grant, AuthorityTransferred and ExpiryChanged at log 2, and the name's
/// ENSv2 binding to `resource`, closed at `closed_at`.
async fn register(
    fixture: &Fixture,
    block: i64,
    token: &str,
    resource: &str,
    binding: &str,
    owner: &str,
    closed_at: Option<i64>,
) -> Result<()> {
    let instance = instance();
    fixture
        .binding(binding, NAME, resource, "ens_v2", block, 2, closed_at)
        .await?;
    fixture
        .write(
            block,
            0,
            "RegistrationGranted",
            V2_REGISTRY,
            Some(NAME),
            None,
            json!({"source_event": "LabelRegistered", "registrant": owner,
                   "expiry": 2_000_000_000u64, "token_id": token, "resource_pending": true,
                   "status": "registered", "registry_contract_instance_id": instance}),
            REGISTRY,
        )
        .await?;
    let linked = json!({"source_event": "TokenResource", "token_id": token,
                        "current_token_id": token, "upstream_resource": token});
    let mut bound = linked.clone();
    bound["binding_kind"] = json!("declared_registry_path");
    bound["surface_binding_id"] = json!(binding);
    fixture
        .write(
            block,
            2,
            "SurfaceBound",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            bound,
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "RegistrationGranted",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "LabelRegistered", "registrant": owner,
                   "expiry": 2_000_000_000u64, "token_id": token, "current_token_id": token,
                   "upstream_resource": token, "status": "registered",
                   "authority_kind": "ens_v2_registry",
                   "authority_key": format!("ens-v2-registry:test:{instance}:{token}"),
                   "resource_pending": false, "registry_contract_instance_id": instance}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "AuthorityTransferred",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "LabelRegistered", "token_id": token,
                   "current_token_id": token, "upstream_resource": token, "owner": owner}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "ExpiryChanged",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "LabelRegistered", "token_id": token,
                   "current_token_id": token, "upstream_resource": token,
                   "expiry": 2_000_000_000u64}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

/// An ERC1155 TransferSingle of the token from `from` to `to` at `block`, as the adapter writes it.
async fn transfer(
    fixture: &Fixture,
    block: i64,
    token: &str,
    resource: &str,
    from: &str,
    to: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            0,
            "TokenControlTransferred",
            V2_REGISTRY,
            Some(NAME),
            Some(resource),
            json!({"source_event": "TransferSingle", "operator": from, "from": from, "to": to,
                   "amount": "1", "token_id": token, "upstream_resource": token}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

async fn composed(fixture: &Fixture) -> Result<NameCurrentRow> {
    load_family_name(&fixture.pool, NAME)
        .await?
        .context("the ENSv2 name composes")
}

/// The served control owner and kind with the registration's status, registrant and kind.
fn summary(row: &NameCurrentRow) -> Value {
    let declared = &row.declared_summary;
    json!({
        "status": declared["registration"]["status"],
        "authority_kind": declared["registration"]["authority_kind"],
        "registrant": declared["registration"]["registrant"],
        "registry_owner": declared["control"]["registry_owner"],
        "control_kind": declared["control"]["latest_event_kind"],
    })
}

#[tokio::test]
async fn a_registered_name_never_transferred_is_owned_by_its_registrant() -> Result<()> {
    let fixture = Fixture::new("families_v2_owner_registered", 20).await?;
    register(&fixture, 10, "0x100", &uuid(1), &uuid(100), ALICE, None).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(
        summary(&composed(&fixture).await?),
        json!({"status": "active", "authority_kind": "ens_v2_registry", "registrant": ALICE,
               "registry_owner": ALICE, "control_kind": "AuthorityTransferred"}),
        "the registration's AuthorityTransferred names the owner"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn a_later_erc1155_transfer_wins_over_the_registrations_owner() -> Result<()> {
    let fixture = Fixture::new("families_v2_owner_transferred", 20).await?;
    let resource = uuid(1);
    register(&fixture, 10, "0x100", &resource, &uuid(100), ALICE, None).await?;
    transfer(&fixture, 11, "0x100", &resource, ALICE, BOB).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(
        summary(&composed(&fixture).await?),
        json!({"status": "active", "authority_kind": "ens_v2_registry", "registrant": BOB,
               "registry_owner": BOB, "control_kind": "TokenControlTransferred"}),
        "the transfer after the registration names the owner"
    );
    fixture.cleanup().await
}

/// PermissionedRegistry `unregister` burns the token and moves its access-control version, so a
/// registration after it links a new resource. The old registration's transfers stay on the old
/// resource and never give the new one an owner.
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L206 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455-L459 @ ens_v2@a971bd64)
#[tokio::test]
async fn a_registration_after_release_is_owned_by_its_new_registrant() -> Result<()> {
    let fixture = Fixture::new("families_v2_owner_reregistered", 20).await?;
    let (first, second) = (uuid(1), uuid(2));
    register(&fixture, 10, "0x100", &first, &uuid(100), ALICE, Some(13)).await?;
    transfer(&fixture, 11, "0x100", &first, ALICE, BOB).await?;
    fixture
        .write(
            13,
            0,
            "RegistrationReleased",
            V2_REGISTRY,
            Some(NAME),
            Some(&first),
            json!({"source_event": "LabelUnregistered", "sender": BOB, "token_id": "0x100",
                   "status": "released", "registry_contract_instance_id": instance()}),
            REGISTRY,
        )
        .await?;
    fixture.apply(14, FamilyMode::Normal).await?;
    let released = composed(&fixture).await?;
    assert_eq!(
        released.declared_summary["control"],
        json!({"status": "unregistered"}),
        "the released name has no owner: {:#}",
        released.declared_summary
    );
    register(&fixture, 15, "0x101", &second, &uuid(101), CAROL, None).await?;
    fixture.apply(16, FamilyMode::Normal).await?;
    assert_eq!(
        summary(&composed(&fixture).await?),
        json!({"status": "active", "authority_kind": "ens_v2_registry", "registrant": CAROL,
               "registry_owner": CAROL, "control_kind": "AuthorityTransferred"}),
        "the new registration's owner, not the earlier registration's transferee"
    );
    fixture.assert_undo_restores(16).await?;
    fixture.assert_rebuild_equal(16).await?;
    fixture.cleanup().await
}
