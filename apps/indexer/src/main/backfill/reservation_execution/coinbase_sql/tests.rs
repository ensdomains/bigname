use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
};
use sqlx::types::Uuid;

use super::*;
use crate::backfill::{BackfillAdapterSyncMode, HistoricalLogValidationFilter};

fn source_plan_for_family(source_family: &str) -> WatchedSourceSelectorPlan {
    let address = "0x1111111111111111111111111111111111111111";
    WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: Some(source_family.to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets: vec![WatchedBackfillTarget {
            source_family: source_family.to_owned(),
            contract_instance_id: Uuid::from_u128(1),
            address: address.to_owned(),
            effective_from_block: 1,
            effective_to_block: 8_192,
        }],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        },
    }
}

fn registry_source_plan() -> WatchedSourceSelectorPlan {
    source_plan_for_family(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
}

fn registry_topic_plan() -> BackfillTopicPlan {
    BackfillTopicPlan::new(
        BTreeMap::from([(
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY.to_owned(),
            vec!["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned()],
        )]),
        BTreeMap::from([(
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY.to_owned(),
            vec!["NewOwner(bytes32,bytes32,address)".to_owned()],
        )]),
        BTreeSet::new(),
    )
}

fn provider_log(block_hash: &str, block_number: i64) -> ProviderLog {
    ProviderLog {
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_owned(),
        transaction_index: 0,
        log_index: 0,
        address: "0x0000000000000000000000000000000000000001".to_owned(),
        topics: Vec::new(),
        data: "0x".to_owned(),
    }
}

fn coinbase_sql_config_with_max(max_window_blocks: i64) -> CoinbaseSqlBackfillConfig {
    CoinbaseSqlBackfillConfig {
        initial_window_blocks: 8_192,
        max_window_blocks,
        page_limit: 50_000,
        sql_char_limit: 10_000,
        query_timeout_secs: 30,
        rate_limit_qps: 5,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    }
}

#[test]
fn registry_coinbase_sql_scan_all_forces_raw_only_adapter_sync() {
    let mut source_plan = registry_source_plan();
    source_plan.selector_kind = WatchedSourceSelectorKind::SourceFamily;

    assert_eq!(
        effective_coinbase_sql_adapter_sync_mode(
            &source_plan,
            &registry_topic_plan(),
            BackfillAdapterSyncMode::Auto,
        ),
        BackfillAdapterSyncMode::RawOnly
    );
    assert_eq!(
        effective_coinbase_sql_adapter_sync_mode(
            &source_plan,
            &registry_topic_plan(),
            BackfillAdapterSyncMode::Inline,
        ),
        BackfillAdapterSyncMode::RawOnly
    );
}

#[test]
fn registrar_coinbase_sql_forces_raw_only_adapter_sync() {
    let source_plan = source_plan_for_family(BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY);
    assert_eq!(
        effective_coinbase_sql_adapter_sync_mode(
            &source_plan,
            &registry_topic_plan(),
            BackfillAdapterSyncMode::Auto,
        ),
        BackfillAdapterSyncMode::RawOnly
    );
    assert_eq!(
        effective_coinbase_sql_adapter_sync_mode(
            &source_plan,
            &registry_topic_plan(),
            BackfillAdapterSyncMode::Inline,
        ),
        BackfillAdapterSyncMode::RawOnly
    );
}

#[test]
fn non_authority_coinbase_sql_keeps_hash_pinned_adapter_sync_mode() {
    let source_plan = source_plan_for_family("basenames_base_primary");
    assert_eq!(
        effective_coinbase_sql_adapter_sync_mode(
            &source_plan,
            &registry_topic_plan(),
            BackfillAdapterSyncMode::Auto,
        ),
        BackfillAdapterSyncMode::Inline
    );
    assert_eq!(
        effective_coinbase_sql_adapter_sync_mode(
            &source_plan,
            &registry_topic_plan(),
            BackfillAdapterSyncMode::RawOnly,
        ),
        BackfillAdapterSyncMode::RawOnly
    );
}

#[test]
fn sparse_coinbase_sql_window_growth_stays_below_practical_query_memory_ceiling() {
    let config = coinbase_sql_config_with_max(131_072);
    assert_eq!(
        next_coinbase_sql_window_blocks(65_536, &config, 500),
        MAX_COINBASE_SQL_PRACTICAL_WINDOW_BLOCKS
    );
}

#[test]
fn coinbase_sql_window_growth_still_honors_lower_configured_max() {
    let mut config = coinbase_sql_config_with_max(16_384);
    assert_eq!(next_coinbase_sql_window_blocks(8_192, &config, 500), 16_384);
    config.page_limit = 5_000;
    assert_eq!(next_coinbase_sql_window_blocks(8_192, &config, 0), 16_384);
    assert_eq!(
        next_coinbase_sql_window_blocks(8_192, &config, 2_500),
        4_096
    );
}

#[test]
fn coinbase_sql_window_growth_downsizes_near_page_cap() {
    let config = coinbase_sql_config_with_max(131_072);
    assert_eq!(
        next_coinbase_sql_window_blocks(65_536, &config, 5_000),
        32_768
    );
}

#[test]
fn sample_validation_allows_large_decoded_registry_scan_all_payloads() -> Result<()> {
    let source_plan = registry_source_plan();
    let payload = HistoricalLogPayload {
        logs_need_validation_provider_payload: false,
        logs_filtered_by_selected_target_index: false,
        validation_filters: vec![HistoricalLogValidationFilter {
            from_block: 1,
            to_block: 8_192,
            addresses: Vec::new(),
            topic0s: vec![
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ],
        }],
        ..Default::default()
    };
    let decoded_payload_log_limit = coinbase_sql_sample_decoded_payload_log_limit(
        &source_plan,
        &payload,
        payload.logs_need_validation_provider_payload,
    );

    assert_eq!(
        decoded_payload_log_limit,
        MAX_COINBASE_SQL_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS
    );
    ensure_coinbase_sql_sample_validation_size(
        BackfillBlockRange::new(1, 8_192)?,
        19_894,
        4_070,
        false,
        decoded_payload_log_limit,
    )
}

#[test]
fn sample_validation_allows_moderate_decoded_registrar_address_payloads() -> Result<()> {
    let source_plan = source_plan_for_family(BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY);
    let payload = HistoricalLogPayload {
        logs_need_validation_provider_payload: false,
        logs_filtered_by_selected_target_index: false,
        validation_filters: vec![HistoricalLogValidationFilter {
            from_block: 1,
            to_block: 8_192,
            addresses: vec!["0x1111111111111111111111111111111111111111".to_owned()],
            topic0s: vec![
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ],
        }],
        ..Default::default()
    };
    let decoded_payload_log_limit = coinbase_sql_sample_decoded_payload_log_limit(
        &source_plan,
        &payload,
        payload.logs_need_validation_provider_payload,
    );

    assert_eq!(
        decoded_payload_log_limit,
        MAX_COINBASE_SQL_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS
    );
    ensure_coinbase_sql_sample_validation_size(
        BackfillBlockRange::new(1, 4_096)?,
        9_604,
        2_131,
        false,
        decoded_payload_log_limit,
    )?;
    let error = ensure_coinbase_sql_sample_validation_size(
        BackfillBlockRange::new(1, 8_192)?,
        MAX_COINBASE_SQL_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS + 1,
        3_548,
        false,
        decoded_payload_log_limit,
    )
    .expect_err("large registrar payloads should still retry smaller");
    assert!(
        format!("{error:#}").contains("decoded SQL materialization"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn registry_coinbase_sql_scan_all_allows_mid_identity_range_start() -> Result<()> {
    let mut source_plan = registry_source_plan();
    source_plan.selector_kind = WatchedSourceSelectorKind::SourceFamily;
    ensure_coinbase_sql_registry_range_start_is_replay_safe(
        &source_plan,
        &registry_topic_plan(),
        BackfillBlockRange::new(2, 8_192)?,
    )
}

#[test]
fn registry_coinbase_sql_rejects_mid_identity_range_start_without_scan_all_topics() -> Result<()> {
    let mut source_plan = registry_source_plan();
    source_plan.selector_kind = WatchedSourceSelectorKind::SourceFamily;
    let topic_plan = BackfillTopicPlan::new(BTreeMap::new(), BTreeMap::new(), BTreeSet::new());
    let error = ensure_coinbase_sql_registry_range_start_is_replay_safe(
        &source_plan,
        &topic_plan,
        BackfillBlockRange::new(2, 8_192)?,
    )
    .expect_err("registry source-family backfills must not resume across identity drift");

    assert!(
        format!("{error:#}").contains("possible source-identity drift"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn sample_validation_keeps_default_decoded_payload_limit_for_non_basenames() -> Result<()> {
    let source_plan = source_plan_for_family("ens_v1_registry_l1");
    let payload = HistoricalLogPayload {
        logs_need_validation_provider_payload: false,
        validation_filters: vec![HistoricalLogValidationFilter {
            from_block: 1,
            to_block: 8_192,
            addresses: vec!["0x1111111111111111111111111111111111111111".to_owned()],
            topic0s: vec![
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ],
        }],
        ..Default::default()
    };
    let decoded_payload_log_limit = coinbase_sql_sample_decoded_payload_log_limit(
        &source_plan,
        &payload,
        payload.logs_need_validation_provider_payload,
    );

    assert_eq!(
        decoded_payload_log_limit,
        MAX_COINBASE_SQL_SAMPLE_DECODED_PAYLOAD_LOGS
    );
    let error = ensure_coinbase_sql_sample_validation_size(
        BackfillBlockRange::new(1, 8_192)?,
        MAX_COINBASE_SQL_SAMPLE_DECODED_PAYLOAD_LOGS + 1,
        1,
        false,
        decoded_payload_log_limit,
    )
    .expect_err("address-filtered decoded payloads should keep the default retry guard");
    assert!(
        format!("{error:#}").contains("decoded SQL materialization"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn sample_validation_accepts_matching_coinbase_sql_log_block_hash() -> Result<()> {
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let logs_by_block = BTreeMap::from([(42, vec![provider_log(block_hash, 42)])]);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 42,
        block_hash: block_hash.to_owned(),
    }];

    ensure_coinbase_sql_logs_match_resolved_blocks(&logs_by_block, &resolved_blocks)
}

#[test]
fn sample_validation_rejects_mismatched_coinbase_sql_log_block_hash() {
    let logs_by_block = BTreeMap::from([(
        42,
        vec![provider_log(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            42,
        )],
    )]);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 42,
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    }];

    let error = ensure_coinbase_sql_logs_match_resolved_blocks(&logs_by_block, &resolved_blocks)
        .expect_err("mismatched Coinbase SQL block hash must fail");
    assert!(
        format!("{error:#}").contains("validation provider resolved"),
        "unexpected error: {error:#}"
    );
}
