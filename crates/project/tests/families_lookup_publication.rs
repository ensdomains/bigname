//! The explicit lookup publication APIs share the ordinary name and record selectors. These
//! inputs are admitted normalized events driven through Project; no lookup state is seeded.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::families::{
    lookup::{LookupInventoryDependency, compose_lookup_inventories_at, compose_lookup_names_at},
    name::{load_family_name, load_family_publication},
    records::{FamilyAttribution, load_family_record_inventory_detail},
};
use serde_json::json;
use support::{CHAIN, Fixture, uuid};
use uuid::Uuid;

#[tokio::test]
async fn shared_lookup_publication_matches_name_selection_and_inventory() -> Result<()> {
    let fixture = Fixture::new("lookup_publication_parity", 20).await?;
    let name = format!("ens:0x{:064x}", 1);
    let resource = uuid(1);
    let resolver = "0x00000000000000000000000000000000000000a1";
    fixture
        .binding(&uuid(100), &name, &resource, "ens_v1", 9, 0, None)
        .await?;
    fixture.write(10, 1, "RegistrationGranted", "ens_v1_registrar_l1", Some(&name), Some(&resource),
        json!({"authority_kind":"registrar", "status":"registered", "registrant":"0x00000000000000000000000000000000000000aa", "expiry":2_000_000_000u64}),
        "0x00000000000000000000000000000000000000e3").await?;
    fixture
        .write(
            10,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name),
            Some(&resource),
            json!({"node":format!("0x{:064x}",1), "resolver":resolver}),
            "0x00000000000000000000000000000000000000e1",
        )
        .await?;
    fixture.write(10, 3, "RecordChanged", "ens_v1_resolver_l1", Some(&name), Some(&resource),
        json!({"node":format!("0x{:064x}",1), "record_key":"text:arbitrary", "record_family":"text", "selector_key":"arbitrary", "value":"one", "source_event":"TextChanged"}), resolver).await?;
    fixture.write(10, 4, "RecordChanged", "ens_v1_resolver_l1", Some(&name), Some(&resource),
        json!({"node":format!("0x{:064x}",1), "record_key":"text:unchanged", "record_family":"text", "selector_key":"unchanged", "value":"kept", "source_event":"TextChanged"}), resolver).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let publication = load_family_publication(&fixture.pool, CHAIN)
        .await?
        .expect("published");
    let ordinary = load_family_name(&fixture.pool, &name)
        .await?
        .expect("served");
    let resource: Uuid = resource.parse()?;
    let inventory = load_family_record_inventory_detail(
        &fixture.pool,
        CHAIN,
        resource,
        FamilyAttribution::Omit,
    )
    .await?
    .expect("inventory");
    let mut conn = fixture.pool.acquire().await?;
    let names =
        compose_lookup_names_at(&mut conn, &publication, std::slice::from_ref(&name)).await?;
    let selected = names[&name].core.as_ref().expect("lookup name");
    assert_eq!(selected.resource_id, ordinary.resource_id);
    assert_eq!(
        selected.record_serving_resource_id,
        ordinary.record_serving_resource_id()
    );
    assert_eq!(
        selected.declared_summary["registration"],
        ordinary.declared_summary["registration"]
    );
    assert_eq!(
        selected.declared_summary["resolver"],
        ordinary.declared_summary["resolver"]
    );
    let composed = compose_lookup_inventories_at(&mut conn, &publication, &[resource]).await?;
    let actual = composed[&resource]
        .inventory
        .as_ref()
        .expect("lookup inventory");
    assert_eq!(actual.row, inventory.row);
    ensure!(composed[&resource].records.contains_key("text:arbitrary"));
    ensure!(
        composed[&resource]
            .dependencies
            .contains(&LookupInventoryDependency::Link {
                resolver_address: resolver.into(),
                node: bigname_storage::families::records::DEFAULT_RECORD_NODE.into(),
            }),
        "missing default links must remain dependencies"
    );
    let missing = Uuid::from_u128(999);
    let absent = compose_lookup_inventories_at(&mut conn, &publication, &[missing]).await?;
    assert!(absent[&missing].inventory.is_none());
    ensure!(
        absent[&missing]
            .dependencies
            .contains(&LookupInventoryDependency::ResourcePointer {
                resource_id: missing
            })
    );
    drop(conn);
    let before = fixture.rows("project_lookup_record").await?;
    ensure!(
        before.len() == 2,
        "Project publishes both selected keys: {before:?}"
    );
    let names_before = fixture.rows("project_lookup_name").await?;
    let dependencies_before = fixture.rows("project_lookup_dependency").await?;
    fixture.write(13, 1, "RecordChanged", "ens_v1_resolver_l1", Some(&name), Some(&resource.to_string()),
        json!({"node":format!("0x{:064x}",1), "record_key":"text:arbitrary", "record_family":"text", "selector_key":"arbitrary", "value":"two", "source_event":"TextChanged"}), resolver).await?;
    fixture.assert_undo_restores(13).await?;
    assert_eq!(
        fixture.rows("project_lookup_name").await?,
        names_before,
        "value-only writes must not rewrite name cores"
    );
    assert_eq!(
        fixture.rows("project_lookup_dependency").await?,
        dependencies_before,
        "value-only writes keep selection dependencies"
    );
    let changed_keys: Vec<String> = sqlx::query_scalar(
        "SELECT key::jsonb ->> 2 FROM project_family_undo
        WHERE chain_id=$1 AND block_number=13 AND family='project_lookup_record'",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        changed_keys,
        ["text:arbitrary"],
        "only the changed selected value is journalled"
    );
    let unchanged = |rows: Vec<serde_json::Value>| {
        rows.into_iter()
            .find(|row| row["record_key"] == "text:unchanged")
            .expect("unchanged key")
    };
    assert_eq!(
        unchanged(fixture.rows("project_lookup_record").await?),
        unchanged(before)
    );
    fixture.apply(14, FamilyMode::Normal).await?;
    let unrelated: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
        WHERE chain_id=$1 AND block_number=14 AND family LIKE 'project_lookup_%'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        unrelated, 0,
        "unrelated publication does not rewrite lookup components"
    );
    fixture.assert_rebuild_equal(14).await?;
    fixture.cleanup().await
}
