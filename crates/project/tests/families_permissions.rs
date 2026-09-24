//! F2b, F8 and F9 through the family loop: wrapper state and expiry, grants with revocations
//! and the admin aggregate, account approvals with an explicit `false`. Each case undoes its last
//! block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const WRAPPER: &str = "0x00000000000000000000000000000000000000d4";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b2";

fn columns(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

#[tokio::test]
async fn wrapper_state_keeps_the_latest_fuses_and_the_latest_wrapper_expiry() -> Result<()> {
    let fixture = Fixture::new("families_wrapper_state", 20).await?;
    let resource = uuid(1);
    fixture
        .write(
            10,
            1,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&resource),
            json!({"fuses": 65537, "wrapper_state": "locked", "expiry": 2000}),
            WRAPPER,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&resource),
            json!({"expiry": 3000, "source_event": "ExpiryExtended"}),
            WRAPPER,
        )
        .await?;
    // A registrar renewal that is not the wrapper's does not move the wrapper expiry.
    fixture
        .write(
            12,
            1,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            None,
            Some(&resource),
            json!({"expiry": 9000, "source_event": "NameRenewed", "authority_kind": "registrar"}),
            WRAPPER,
        )
        .await?;
    fixture
        .write(
            12,
            2,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            None,
            Some(&resource),
            json!({"fuses": 0, "wrapper_state": "unknown"}),
            WRAPPER,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_wrapper_state").await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        columns(&rows[0], &["wrapper_state", "fuses", "expiry_seconds"]),
        json!({"wrapper_state": null, "fuses": 0, "expiry_seconds": 3000})
    );
    assert_eq!(rows[0]["expiry_position"]["block_number"], json!(11));
    assert_eq!(rows[0]["wrapper_state_position"]["block_number"], json!(12));
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn grants_keep_revocations_and_the_admin_aggregate_follows_each_holder() -> Result<()> {
    let fixture = Fixture::new("families_grants", 20).await?;
    let resource = uuid(2);
    let registry_scope =
        json!({"kind": "registry", "chain_id": "ethereum-sepolia", "registry_address": REGISTRY});
    let grant = |subject: &str, powers: Value, revoked: bool| {
        let mut after = json!({"subject": subject, "scope": registry_scope, "effective_powers": powers,
                               "grant_source": {"kind": "raw_log"}, "inheritance_path": [],
                               "transfer_behavior": "stays"});
        if revoked {
            after["revocation_source"] = json!({"kind": "raw_log"});
        }
        after
    };
    fixture
        .write(
            10,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(ALICE, json!(["admin_roles", "set_resolver"]), false),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(BOB, json!(["can_transfer_admin"]), false),
            REGISTRY,
        )
        .await?;
    // Alice's grant is revoked: the grant row stays, her admin entry goes.
    fixture
        .write(
            11,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(ALICE, json!([]), true),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;

    let mut grants = fixture.rows("project_grant").await?;
    grants.sort_by_key(|row| row["subject"].to_string());
    assert_eq!(
        grants
            .iter()
            .map(|row| columns(
                row,
                &[
                    "subject",
                    "scope",
                    "scope_kind",
                    "revoked",
                    "effective_powers"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"subject": ALICE, "scope": "registry", "scope_kind": "registry", "revoked": true, "effective_powers": []}),
            json!({"subject": BOB, "scope": "registry", "scope_kind": "registry", "revoked": false, "effective_powers": ["can_transfer_admin"]}),
        ]
    );
    assert_eq!(grants[1]["transfer_behavior"], json!({"mode": "stays"}));
    let aggregate = fixture.rows("project_resource_admin_aggregate").await?;
    assert_eq!(
        aggregate
            .iter()
            .map(|row| row["admin_powers"].clone())
            .collect::<Vec<_>>(),
        vec![json!({format!("{BOB}|registry"): ["can_transfer_admin"]})]
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;

    // Bob's revocation empties the aggregate, which then has no fact left and goes.
    fixture
        .write(
            12,
            1,
            "PermissionChanged",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            grant(BOB, json!([]), true),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    assert!(
        fixture
            .rows("project_resource_admin_aggregate")
            .await?
            .is_empty()
    );
    assert_eq!(fixture.rows("project_grant").await?.len(), 2);
    fixture.assert_undo_restores(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn approvals_keep_an_explicit_false() -> Result<()> {
    let fixture = Fixture::new("families_approvals", 20).await?;
    let approval = |approved: bool| {
        json!({"subject": BOB, "relation_kind": "operator", "approved": approved,
               "scope": {"kind": "account", "authority_kind": "registry",
                         "authority_contract": REGISTRY.to_uppercase().replace("0X", "0x"), "owner": ALICE},
               "effective_powers": if approved { json!(["registry_control"]) } else { json!([]) },
               "inheritance_path": [], "transfer_behavior": {"mode": "owner_scoped"}})
    };
    fixture
        .write(
            10,
            1,
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            None,
            None,
            approval(true),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            None,
            None,
            approval(false),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let rows = fixture.rows("project_account_approval").await?;
    assert_eq!(
        rows.iter()
            .map(|row| columns(
                row,
                &[
                    "authority_kind",
                    "authority_contract",
                    "owner",
                    "subject",
                    "approved"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"authority_kind": "registry", "authority_contract": REGISTRY, "owner": ALICE,
                    "subject": BOB, "approved": false})
        ]
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}
