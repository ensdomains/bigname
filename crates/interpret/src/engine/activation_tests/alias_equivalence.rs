use super::equivalence::{first_json_difference, install_stage_capture, semantic_end_state};
use super::*;
use bigname_adapters::schema_v2::seam::{
    ADMISSION_DISCOVERY_EDGE_KINDS, PREIMAGE_OBSERVATION_EVENT_KIND,
};

const RESOLVER: &str = "0x0000000000000000000000000000000000000529";

#[tokio::test]
async fn alias_observed_name_survives_cold_resume() -> TestResult {
    let whole = database("interpret_alias_whole_equivalence").await?;
    let resumed = database("interpret_alias_resume_equivalence").await?;
    seed_alias_corpus(whole.pool()).await?;
    seed_alias_corpus(resumed.pool()).await?;

    Engine::new(whole.pool().clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    Engine::new(resumed.pool().clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: SETUP_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Engine::new(resumed.pool().clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: Some(Marker {
                number: SETUP_BLOCK,
                hash: block_hash(SETUP_BLOCK),
            }),
            mode: RunMode::Normal,
        })
        .await?;

    install_stage_capture(whole.pool()).await?;
    install_stage_capture(resumed.pool()).await?;
    assert_expected_alias_rows(whole.pool()).await?;
    assert_expected_alias_rows(resumed.pool()).await?;
    let whole_events = normalized_events_snapshot(whole.pool()).await?;
    let resumed_events = normalized_events_snapshot(resumed.pool()).await?;
    assert!(
        whole_events == resumed_events,
        "whole-pass and cold-resume normalized events differ: {}",
        first_json_difference(&whole_events, &resumed_events, "normalized_events"),
    );

    whole.cleanup().await?;
    resumed.cleanup().await?;
    Ok(())
}

async fn assert_expected_alias_rows(pool: &PgPool) -> TestResult {
    let logical_name_id = format!("ens:{:#x}", eth_namehash(keccak256(b"alias")));
    let alias_preimages: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = $3
           AND logical_name_id = $2
           AND after_state ->> 'source_event' = 'AliasChanged'",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .bind(PREIMAGE_OBSERVATION_EVENT_KIND)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        alias_preimages, 1,
        "expected the alias preimage observation"
    );

    let attributed_records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'
           AND logical_name_id = $2 AND resource_id IS NULL",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        attributed_records, 1,
        "expected one resource-less alias-attributed record"
    );
    Ok(())
}

async fn normalized_events_snapshot(pool: &PgPool) -> TestResult<serde_json::Value> {
    Ok(semantic_end_state(pool)
        .await?
        .into_iter()
        .find_map(|(table, rows)| (table == "normalized_events").then_some(rows))
        .expect("normalized-events semantic snapshot"))
}

async fn seed_alias_corpus(pool: &PgPool) -> TestResult {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    seed_lineage(pool).await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_v2_resolver_l1'
           AND rollout_status = 'active'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let instance_id = Uuid::from_u128(529);
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(instance_id)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(instance_id)
    .bind(CHAIN)
    .bind(RESOLVER)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         ) VALUES ($1, $2, 'contract', 'issue-529-resolver', $3, $4, $5, 'none', 0)",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(instance_id)
    .bind(RESOLVER)
    .bind(ADMISSION_DISCOVERY_EDGE_KINDS[0])
    .execute(pool)
    .await?;

    let from_name = b"\x05alias\x03eth\0".to_vec();
    let to_name = b"\x06target\x03eth\0".to_vec();
    let node = eth_namehash(keccak256(b"alias"));
    insert_transaction(pool, SETUP_BLOCK, RESOLVER).await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        0,
        RESOLVER,
        AliasChanged {
            indexedFromName: keccak256(&from_name),
            indexedToName: keccak256(&to_name),
            fromName: from_name.into(),
            toName: to_name.into(),
        }
        .encode_log_data(),
    )
    .await?;
    insert_transaction(pool, PREDECESSOR_BLOCK, RESOLVER).await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        0,
        RESOLVER,
        AddressChanged {
            node,
            coinType: U256::from(60_u64),
            newAddress: vec![0x52, 0x9].into(),
        }
        .encode_log_data(),
    )
    .await?;
    Ok(())
}
