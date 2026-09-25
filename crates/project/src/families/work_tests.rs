//! Rebuild planning takes the declaration start blocks from the manifest history the run
//! captured, the same history population classifies under, so an update that lands between the
//! two cannot give classification a start block the work list omitted.
use anyhow::Result;
use serde_json::json;

use super::{
    FamilyOptions, block,
    guard_tests::{CHAIN, NO_INTERPRET, database},
    input, manifests, marker,
};

/// One blockless manifest update declaring a resolver at each of `start_blocks`.
async fn blockless_update(pool: &sqlx::PgPool, start_blocks: &[i64]) -> Result<()> {
    let contracts: Vec<_> = (1..)
        .zip(start_blocks)
        .map(|(n, start_block)| {
            json!({"address": format!("0x{n:040x}"), "role": "public_resolver",
                   "start_block": start_block})
        })
        .collect();
    let payload = json!({ "contracts": contracts });
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

// The update lands after the history is captured and before the work list is built, so this
// shows the work list reads no manifests of its own; the window after the work list is the next
// test.
#[tokio::test]
async fn the_work_list_takes_declaration_starts_from_the_captured_history() -> Result<()> {
    let (database, pool) = database().await?;
    let captured = manifests::History::read(&pool, CHAIN, 12).await?;
    // A blockless update lands after the run captured its history.
    blockless_update(&pool, &[7]).await?;
    let blocks = input::work_blocks(&pool, CHAIN, 1, 12, &captured, i64::MAX).await?;
    assert!(
        !blocks.contains(&7),
        "the captured history declares nothing, so block 7 is no work: {blocks:?}"
    );
    // A history read after the update has the declaration, and its start block is work.
    let later = manifests::History::read(&pool, CHAIN, 12).await?;
    let blocks = input::work_blocks(&pool, CHAIN, 1, 12, &later, i64::MAX).await?;
    assert_eq!(blocks, [7]);
    database.cleanup().await?;
    Ok(())
}

// The planning-to-population window itself: the work list is materialized, the update lands,
// and the population step then applies under the history the run captured. Neither the work list
// nor the block's classification sees the update; a block applied under a history read afterwards
// does.
#[tokio::test]
async fn an_update_after_the_work_list_is_invisible_to_the_blocks_populated_under_it() -> Result<()>
{
    let (database, pool) = database().await?;
    let captured = manifests::History::read(&pool, CHAIN, 11).await?;
    let blocks = input::work_blocks(&pool, CHAIN, 1, 11, &captured, i64::MAX).await?;
    assert!(
        blocks.is_empty(),
        "nothing is declared or active: {blocks:?}"
    );
    blockless_update(&pool, &[7]).await?;
    // Population visits the work list and then the target, as `populate` does.
    let options = FamilyOptions::new("work");
    let populate = |number: i64, history: &manifests::History| {
        let history = history.clone();
        let (pool, options) = (&pool, &options);
        async move {
            let family = marker::read(pool, CHAIN).await?;
            let plan = block::Plan {
                predecessor: family.current.as_ref(),
                sequence: family.sequence,
                contiguous: false,
                bootstrap: false,
                revision: &NO_INTERPRET,
                role: block::Role::Follow,
                manifests: &history,
            };
            block::apply(pool, CHAIN, number, &plan, options).await?;
            anyhow::Ok(marker::read(pool, CHAIN).await?.admission_manifests)
        }
    };
    let admitted = populate(11, &captured).await?;
    assert_eq!(
        admitted.as_deref(),
        Some(captured.at(11).key.as_str()),
        "block 11 classified under the captured history"
    );
    let later = manifests::History::read(&pool, CHAIN, 12).await?;
    assert_ne!(
        later.at(11).key,
        captured.at(11).key,
        "the update changes the active set at block 11"
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 11, &later, i64::MAX).await?,
        [7]
    );
    let admitted = populate(12, &later).await?;
    assert_eq!(
        admitted.as_deref(),
        Some(later.at(12).key.as_str()),
        "a block applied under a fresh history classifies under the update"
    );
    database.cleanup().await?;
    Ok(())
}

// A run reads only the next chunk of its budget: the lowest `limit` work blocks.
#[tokio::test]
async fn the_work_list_returns_the_lowest_blocks_up_to_its_limit() -> Result<()> {
    let (database, pool) = database().await?;
    blockless_update(&pool, &[7, 3, 5]).await?;
    let history = manifests::History::read(&pool, CHAIN, 12).await?;
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, i64::MAX).await?,
        [3, 5, 7]
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, 2).await?,
        [3, 5]
    );
    database.cleanup().await?;
    Ok(())
}
