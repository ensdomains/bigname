//! Rebuild planning takes the declaration start blocks from the manifest history the run
//! captured, the same history population classifies under, so an update that lands between the
//! two cannot give classification a start block the work list omitted.
use anyhow::Result;
use serde_json::json;

use super::{
    guard_tests::{CHAIN, database},
    input, manifests,
};

async fn blockless_update(pool: &sqlx::PgPool, start_block: i64) -> Result<()> {
    let payload = json!({"contracts": [
        {"address": "0x00000000000000000000000000000000000000a1", "role": "public_resolver",
         "start_block": start_block}
    ]});
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/resolver.yaml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ('manifest:' || $1, 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1, $1,
                 $2, 'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', 'active', 'manifest_payload', $3::jsonb),
                 '{}'::jsonb)",
    )
    .bind(id)
    .bind(CHAIN)
    .bind(&payload)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn the_work_list_takes_declaration_starts_from_the_captured_history() -> Result<()> {
    let (database, pool) = database().await?;
    let captured = manifests::History::read(&pool, CHAIN, 12).await?;
    // A blockless update lands after the run captured its history.
    blockless_update(&pool, 7).await?;
    let blocks = input::work_blocks(&pool, CHAIN, 1, 12, &captured).await?;
    assert!(
        !blocks.contains(&7),
        "the captured history declares nothing, so block 7 is no work: {blocks:?}"
    );
    // A history read after the update has the declaration, and its start block is work.
    let later = manifests::History::read(&pool, CHAIN, 12).await?;
    let blocks = input::work_blocks(&pool, CHAIN, 1, 12, &later).await?;
    assert_eq!(blocks, [7]);
    database.cleanup().await?;
    Ok(())
}
