//! The registry pointer (F4) and the record inventory a resolver holds for the pointer's node.
//! The inventory is kept by resolver and node, so moving the pointer away, and undoing the move,
//! leaves what the resolver holds for the node as it was.
mod families_support;

use anyhow::{Context, Result};
use bigname_project::families::{self, FamilyMode};
use bigname_storage::families::records::FamilyRecordInventory;
use families_support::{CHAIN, Fixture, hash, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";

fn node() -> String {
    format!("0x{:064x}", 1)
}

fn name() -> String {
    format!("ens:{}", node())
}

/// An active ENSv1 resolver manifest from block 1 that declares R1 a public resolver, so R1 has
/// a supported classification.
async fn declare_r1(fixture: &Fixture) -> Result<()> {
    let payload = json!({"contracts": [{"address": R1, "role": "public_resolver"}]});
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/ens_v1_resolver_l1.yaml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ($1, 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1, $2, $3, 1, $4,
                 'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', 'active', 'manifest_payload', $5::jsonb),
                 '{}'::jsonb)",
    )
    .bind(format!("manifest:{id}:1"))
    .bind(id)
    .bind(CHAIN)
    .bind(hash(1))
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// What R1 holds for the node, as the resolver-anchored records route reads it.
async fn held_by_r1(fixture: &Fixture) -> Result<Value> {
    let mut conn = fixture.pool.acquire().await?;
    let inventory =
        FamilyRecordInventory::load_resolver_node_on(&mut conn, CHAIN, R1, "ens", &name(), &node())
            .await?
            .context("R1 has a classification")?;
    Ok(json!({
        "entries": inventory.row.entries,
        "selectors": inventory.row.selectors,
        "coverage": inventory.row.coverage,
    }))
}

async fn pointer(fixture: &Fixture) -> Result<Value> {
    let rows = fixture.rows("project_registry_pointer").await?;
    anyhow::ensure!(rows.len() == 1, "one pointer row: {rows:?}");
    Ok(rows[0]["resolver_address"].clone())
}

#[tokio::test]
async fn undo_restores_the_pointer_and_the_inventory_is_untouched() -> Result<()> {
    let fixture = Fixture::new("families_registry_pointer_undo", 20).await?;
    declare_r1(&fixture).await?;
    let resource = uuid(1);
    let set = |resolver: &str| json!({"node": node(), "resolver": resolver});
    fixture
        .write(
            10,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name()),
            Some(&resource),
            set(R1),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            json!({"node": node(), "record_key": "text:url", "record_family": "text",
                   "selector_key": "url", "value": "https://r1.example",
                   "source_event": "TextChanged"}),
            R1,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name()),
            Some(&resource),
            set(R2),
            REGISTRY,
        )
        .await?;

    fixture.apply(11, FamilyMode::Normal).await?;
    assert_eq!(pointer(&fixture).await?, json!(R1));
    let held = held_by_r1(&fixture).await?;
    assert!(
        held["entries"].to_string().contains("https://r1.example"),
        "{held:#}"
    );
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(pointer(&fixture).await?, json!(R2));
    assert_eq!(held_by_r1(&fixture).await?, held, "after the pointer moved");
    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    assert_eq!(undone, 1);
    assert_eq!(pointer(&fixture).await?, json!(R1));
    assert_eq!(
        held_by_r1(&fixture).await?,
        held,
        "after the move was undone"
    );

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    assert_eq!(held_by_r1(&fixture).await?, held, "after a rebuild");
    fixture.cleanup().await
}
