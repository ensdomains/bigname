//! Registry shadow reads (TYR-36 step 3, docs/glossary.md "Shadow read"): the F2c node rows give
//! an ENSv1 name's registry generation, handoff block and ownerless profile, and the registry
//! binding observations give each resource's served registry owner, contract and clear. Each
//! case publishes with the production batch, follows it with the families, and compares every
//! served name and resource with its family read; it also pins the served value it was written
//! for.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use serde_json::{Value, json};
use shadow_support::publish_and_compare;
use support::{Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_REGISTRY: &str = "ens_v1_registry_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// Name 1 bound at block 9 to `lease` under arm ens_v1, the SurfaceBound naming its registry and
/// the registry owner the getter read, then granted at 10.
async fn bound(fixture: &Fixture, lease: &str) -> Result<()> {
    fixture
        .binding(&uuid(100), &name(1), lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// A registry AuthorityTransferred of name 1's node from the old or the current registry.
async fn transferred(fixture: &Fixture, block: i64, owner: &str, role: &str) -> Result<()> {
    fixture
        .write(
            block,
            1,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            None,
            json!({"node": node(1), "owner": owner, "owner_getter": owner, "emitter_role": role}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

async fn summary(fixture: &Fixture, resource: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('registry_owner', registry_owner,
                    'registry_contract', registry_contract,
                    'clear_event_id', provenance -> 'registry_binding_clear_event_id')
         FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_optional(&fixture.pool)
    .await?)
}

async fn selection(fixture: &Fixture) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(name(1))
    .fetch_one(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn a_surface_binding_serves_its_registry_owner() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_bound", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(
        summary(&fixture, &lease).await?,
        Some(
            json!({"registry_owner": OWNER, "registry_contract": REGISTRY,
                    "clear_event_id": null})
        )
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_surface_unbinding_clears_the_registry_owner_with_its_event() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_unbound", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    fixture
        .write(
            14,
            1,
            "SurfaceUnbound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"registry_contract": REGISTRY}),
            REGISTRAR,
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let unbound: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
    )
    .bind("SurfaceUnbound:14:1")
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        summary(&fixture, &lease).await?,
        Some(json!({"registry_owner": null, "registry_contract": null,
                    "clear_event_id": unbound}))
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn the_old_registry_then_the_current_one_move_the_generation() -> Result<()> {
    for current in [false, true] {
        let fixture = Fixture::new(
            &format!("families_shadow_registry_generation_{current}"),
            20,
        )
        .await?;
        bound(&fixture, &uuid(1)).await?;
        transferred(&fixture, 11, OWNER, "registry_old").await?;
        if current {
            transferred(&fixture, 13, OWNER, "registry").await?;
        }
        let report = publish_and_compare(&fixture, 16).await?;
        shadow_support::assert_counts(&report, &[], &[]);
        let selection = selection(&fixture).await?;
        if current {
            assert_eq!(selection["registry_generation"], json!("current"));
            assert_eq!(selection["registry_handoff_block_number"], json!(13));
        } else {
            assert_eq!(selection["registry_generation"], json!("old"));
            assert_eq!(selection["registry_handoff_block_number"], Value::Null);
        }
        fixture.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_zero_registry_owner_keeps_its_getter_facts() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_zero", 20).await?;
    bound(&fixture, &uuid(1)).await?;
    transferred(&fixture, 11, ZERO, "registry").await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let selection = selection(&fixture).await?;
    assert_eq!(selection["registry_generation"], json!("current"));
    assert_eq!(selection["registry_handoff_block_number"], json!(11));
    assert_eq!(
        selection["ownerless_registry"],
        Value::Null,
        "the lease still holds the name"
    );
    fixture.cleanup().await
}
