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

// The window after the work list: the work list is materialized, the update lands, and a block is
// applied through `block::apply` with a hand-built plan carrying the captured history, the
// contract the driver's population relies on. The work list omits the update and the block records
// the captured active-set key; a block applied under a history read afterwards records the updated
// key. No resolver is seeded, so this shows which history reaches the block, not a classification
// value.
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
                head: None,
            };
            block::apply(pool, CHAIN, number, &plan, options, &mut Default::default()).await?;
            anyhow::Ok(marker::read(pool, CHAIN).await?.admission_manifests)
        }
    };
    let admitted = populate(11, &captured).await?;
    assert_eq!(
        admitted.as_deref(),
        Some(captured.at(11).key.as_str()),
        "block 11 records the captured active-set key"
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
        "a block applied under a fresh history records the updated active-set key"
    );
    database.cleanup().await?;
    Ok(())
}

/// Manifest `id`'s update to `status`: its catalog row, and a blockless SourceManifestUpdated
/// event carrying `payload` while it is active, as manifest sync writes them.
async fn root_update(
    pool: &sqlx::PgPool,
    id: Option<i64>,
    status: &str,
    payload: serde_json::Value,
) -> Result<i64> {
    let id: i64 = match id {
        Some(id) => {
            sqlx::query("UPDATE manifest_versions SET rollout_status = $2 WHERE manifest_id = $1")
                .bind(id)
                .bind(status)
                .execute(pool)
                .await?;
            id
        }
        None => {
            sqlx::query_scalar(
                "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', 'ens_v2_root_l1', $1, 'fixture-' || $3::text, $2, 'fixture',
                         'fixture/root-' || $3::text || '.toml', $3)
                 RETURNING manifest_id",
            )
            .bind(CHAIN)
            .bind(status)
            .bind(&payload)
            .fetch_one(pool)
            .await?
        }
    };
    let active = status == "active";
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ('root-manifest:' || $1 || ':' || $3, 'ens', 'SourceManifestUpdated',
                 'ens_v2_root_l1', 1, $1, $2, 'manifest_sync', 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', $3::text,
                                    'manifest_payload', CASE WHEN $4 THEN $5::jsonb END),
                 '{}'::jsonb)",
    )
    .bind(id)
    .bind(CHAIN)
    .bind(status)
    .bind(active)
    .bind(&payload)
    .execute(pool)
    .await?;
    Ok(id)
}

fn root(address: &str, start_block: i64) -> serde_json::Value {
    json!({"contracts": [{"role": "root_registry", "address": address,
                          "start_block": start_block}]})
}

// The run captures a set admitting root A. A manifest sync then deprecates A and admits root B
// before the block publishes. The block composes and stamps the captured admission: its marker
// records A and A's start block beside the captured set key. A block applied under a history
// read after the sync records B.
#[tokio::test]
async fn a_manifest_sync_after_capture_leaves_the_block_on_the_captured_admission() -> Result<()> {
    let (database, pool) = database().await?;
    let first = root_update(&pool, None, "active", root("0xAAAA", 3)).await?;
    let captured = manifests::History::read(&pool, CHAIN, 11).await?;
    root_update(&pool, Some(first), "deprecated", root("0xAAAA", 3)).await?;
    root_update(&pool, None, "active", root("0xBBBB", 5)).await?;
    let options = FamilyOptions::new("admission");
    let apply = |number: i64, history: manifests::History| {
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
                head: None,
            };
            block::apply(pool, CHAIN, number, &plan, options, &mut Default::default()).await?;
            let family = marker::read(pool, CHAIN).await?;
            anyhow::Ok((family.admission, family.admission_manifests))
        }
    };
    let (admission, key) = apply(11, captured.clone()).await?;
    assert_eq!(
        admission
            .as_ref()
            .map(|a| (a.root_registry.as_str(), a.since_block)),
        Some(("0xaaaa", Some(3))),
        "block 11 stamps the captured root and start block"
    );
    assert_eq!(key.as_deref(), Some(captured.at(11).key.as_str()));
    let later = manifests::History::read(&pool, CHAIN, 12).await?;
    let (admission, key) = apply(12, later.clone()).await?;
    assert_eq!(
        admission
            .as_ref()
            .map(|a| (a.root_registry.as_str(), a.since_block)),
        Some(("0xbbbb", Some(5))),
        "a block applied under a fresh history stamps the synced root"
    );
    assert_eq!(key.as_deref(), Some(later.at(12).key.as_str()));
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

// Each source of the work list stops at `limit` blocks of its own, and the list is still the
// lowest `limit` blocks across all of them. Events fill blocks 4 to 10, three to a block, more
// blocks than the limit; declarations start at 2, below every event, and at 11. A source that
// stopped at `limit` rows rather than `limit` blocks would give the events one block.
#[tokio::test]
async fn each_work_source_stops_at_the_limit_and_the_lowest_blocks_win_across_them() -> Result<()> {
    let (database, pool) = database().await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, before_state,
             after_state, raw_fact_ref)
         SELECT 'work:' || block || ':' || log, 'ens', 'PreimageObserved', 'ens_v1_registry_l1',
                1, $1, block, '0x' || lpad(to_hex(block), 64, '0'), '0xfeed', 0, log,
                'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb, '{}'::jsonb,
                '{}'::jsonb
         FROM generate_series(4, 10) block, generate_series(0, 2) log",
    )
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    blockless_update(&pool, &[2, 11]).await?;
    let history = manifests::History::read(&pool, CHAIN, 12).await?;
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, i64::MAX).await?,
        [2, 4, 5, 6, 7, 8, 9, 10, 11]
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, 3).await?,
        [2, 4, 5]
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 5, 12, &history, 3).await?,
        [5, 6, 7]
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 9, 12, &history, 3).await?,
        [9, 10, 11]
    );
    database.cleanup().await?;
    Ok(())
}

// The two resolver edge sources return distinct readable boundaries up to the limit. Lineage
// below block 3 is gone, so the edge boundaries 1 and 2 have no block to apply, and two edges
// stop at 6; a source that spent its limit on unreadable blocks or on repeated rows would miss 7.
// The address boundary 3 comes from three rows (R1 from 3, R2 to 3 through both of its edges)
// and 1 is below the lineage, so the address source must still reach 5.
#[tokio::test]
async fn the_resolver_edge_sources_return_distinct_readable_boundaries_up_to_the_limit()
-> Result<()> {
    let (database, pool) = database().await?;
    sqlx::query("DELETE FROM chain_lineage WHERE chain_id = $1 AND block_number < 3")
        .bind(CHAIN)
        .execute(&pool)
        .await?;
    let [registry, r1, r2] =
        [0xa0, 0xa1, 0xa2].map(|n| format!("00000000-0000-0000-0000-{n:012x}"));
    for instance in [&registry, &r1, &r2] {
        sqlx::query(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ($1::uuid, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(&pool)
        .await?;
    }
    for (to, from_block, to_block) in [(&r1, 1_i64, Some(6_i64)), (&r2, 2, Some(6)), (&r2, 7, None)]
    {
        sqlx::query(
            "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
                 to_contract_instance_id, discovery_source, admission_basis,
                 active_from_block_number, active_from_block_hash, active_to_block_number,
                 active_to_block_hash, canonicality_state)
             VALUES ($1, 'resolver', $2::uuid, $3::uuid, 'NewResolver', 'fixture', $4,
                     '0x' || lpad(to_hex($4::bigint), 64, '0'), $5,
                     '0x' || lpad(to_hex($5::bigint), 64, '0'), 'canonical')",
        )
        .bind(CHAIN)
        .bind(&registry)
        .bind(to)
        .bind(from_block)
        .bind(to_block)
        .execute(&pool)
        .await?;
    }
    let history = manifests::History::read(&pool, CHAIN, 12).await?;
    for limit in [2, i64::MAX] {
        assert_eq!(
            input::work_blocks(&pool, CHAIN, 1, 12, &history, limit).await?,
            [6, 7]
        );
    }
    for (instance, from_block, to_block) in [(&r1, 3_i64, 5_i64), (&r2, 1, 3)] {
        sqlx::query(
            "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
                 active_from_block_number, active_to_block_number)
             VALUES ($1::uuid, $2, $3, $4, $5)",
        )
        .bind(instance)
        .bind(CHAIN)
        .bind(format!("0x{:0>40}", &instance[24..]))
        .bind(from_block)
        .bind(to_block)
        .execute(&pool)
        .await?;
    }
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, 1).await?,
        [3]
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, 2).await?,
        [3, 5]
    );
    assert_eq!(
        input::work_blocks(&pool, CHAIN, 1, 12, &history, i64::MAX).await?,
        [3, 5, 6, 7]
    );
    database.cleanup().await?;
    Ok(())
}
