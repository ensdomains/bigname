use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::*;
use crate::backfill::{BackfillAdapterSyncMode, CoinbaseSqlValidationMode};
use crate::reconciliation::HeaderAuditMode;

#[tokio::test]
async fn coinbase_sql_job_honors_retention_generation_idempotency_scope() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_coinbase_generation_scope"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for Coinbase SQL generation-scope test",
    )
    .await?;
    let chain = "base-mainnet";
    let source_plan = WatchedSourceSelectorPlan {
        chain: chain.to_owned(),
        selector_kind: WatchedSourceSelectorKind::SourceFamily,
        source_family: Some("basenames_base_registry".to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets: vec![WatchedBackfillTarget {
            source_family: "basenames_base_registry".to_owned(),
            contract_instance_id: Uuid::from_u128(1),
            address: "0x0000000000000000000000000000000000000001".to_owned(),
            effective_from_block: 10,
            effective_to_block: 20,
        }],
        watched_chain_plan: WatchedChainPlan {
            chain: chain.to_owned(),
            addresses: vec!["0x0000000000000000000000000000000000000001".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        },
    };
    let config = BackfillJobRunConfig {
        deployment_profile: "mainnet".to_owned(),
        idempotency_key: "coinbase-generation-scoped".to_owned(),
        scope_idempotency_to_raw_log_retention_generation: true,
        range: BackfillBlockRange::new(10, 20)?,
        lease_owner: "test".to_owned(),
        lease_token: "test-token".to_owned(),
        lease_expires_at: OffsetDateTime::now_utc(),
        hash_pinned_chunk_blocks: DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        adapter_sync_mode: BackfillAdapterSyncMode::RawOnly,
        header_audit_mode: HeaderAuditMode::Minimal,
    };
    let coinbase_config = CoinbaseSqlBackfillConfig {
        initial_window_blocks: 1_000,
        max_window_blocks: 1_000,
        page_limit: 1_000,
        sql_char_limit: 10_000,
        query_timeout_secs: 30,
        rate_limit_qps: 1,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    };
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::new(),
        BTreeMap::from([(
            "basenames_base_registry".to_owned(),
            vec!["NewOwner(bytes32,bytes32,address)".to_owned()],
        )]),
        BTreeSet::new(),
    );

    let first = create_coinbase_sql_backfill_job(
        database.pool(),
        &source_plan,
        &config,
        &coinbase_config,
        &topic_plan,
    )
    .await?;
    assert_eq!(first.job.raw_log_retention_generation, 0);

    sqlx::query(
        "UPDATE raw_log_staging_input_revisions SET retention_generation = 1 WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let second = create_coinbase_sql_backfill_job(
        database.pool(),
        &source_plan,
        &config,
        &coinbase_config,
        &topic_plan,
    )
    .await?;
    assert_eq!(second.job.raw_log_retention_generation, 1);
    assert_ne!(first.job.backfill_job_id, second.job.backfill_job_id);
    assert!(
        first
            .job
            .idempotency_key
            .ends_with(":raw_log_retention_generation=0")
    );
    assert!(
        second
            .job
            .idempotency_key
            .ends_with(":raw_log_retention_generation=1")
    );

    database.cleanup().await
}
