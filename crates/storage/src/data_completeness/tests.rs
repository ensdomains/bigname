use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::load_data_completeness;

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_storage_data_completeness_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for data_completeness tests")
            .admin_connect_context("failed to connect admin pool for data_completeness tests")
            .pool_connect_context("failed to connect data_completeness test pool"),
        &crate::MIGRATOR,
        "failed to apply migrations for data_completeness tests",
    )
    .await
}

/// Two non-orphaned lineage rows at one block height are a canonicality violation the
/// distinct-block-number count cannot see; the dedicated CTE must count the height.
#[tokio::test]
async fn duplicate_canonical_heights_are_counted() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xaa', 100, now(), 'canonical'::canonicality_state),
            ('ethereum-sepolia', '0xbb', 100, now(), 'canonical'::canonicality_state),
            ('ethereum-sepolia', '0xcc', 101, now(), 'canonical'::canonicality_state)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let chain = read
        .chains
        .iter()
        .find(|chain| chain.chain_id == "ethereum-sepolia")
        .expect("chain row");
    assert_eq!(chain.duplicate_canonical_height_count, 1);
    // Distinct block numbers are 100 and 101, so the span-based contiguity count is blind.
    assert_eq!(chain.lineage_canonical_block_count, 2);

    database.cleanup().await
}

/// Orphaned events and NULL-chain events must not satisfy the per-chain content check; the
/// NULL rows are counted separately as a data-integrity signal.
#[tokio::test]
async fn orphaned_and_null_chain_events_excluded_from_counts() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family,
             manifest_version, derivation_kind, chain_id, canonicality_state)
        VALUES
            ('e1', 'ens', 'k', 'sf', 1, 'd', 'ethereum-sepolia', 'canonical'::canonicality_state),
            ('e2', 'ens', 'k', 'sf', 1, 'd', 'ethereum-sepolia', 'orphaned'::canonicality_state),
            ('e3', 'ens', 'k', 'sf', 1, 'd', NULL, 'canonical'::canonicality_state)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let ens = read
        .normalized_event_counts
        .iter()
        .find(|entry| entry.chain_id == "ethereum-sepolia" && entry.namespace == "ens");
    assert_eq!(ens.map(|entry| entry.count), Some(1));
    assert_eq!(read.normalized_events_null_chain_id_count, 1);

    database.cleanup().await
}

/// The cursor read must carry `next_block_number` so the evaluator can detect a rewind where
/// `next` dropped below `target` while `last_completed` stayed high.
#[tokio::test]
async fn rewound_cursor_next_below_target_is_loaded() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors
            (deployment_profile, chain_id, cursor_kind, range_start_block_number,
             next_block_number, target_block_number, last_completed_block_number)
        VALUES
            ('sepolia', 'ethereum-sepolia', 'raw_fact_normalized_events', 0, 500, 1000, 1000)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let cursor = read
        .replay_cursors
        .iter()
        .find(|cursor| cursor.cursor_kind == "raw_fact_normalized_events")
        .expect("cursor");
    assert_eq!(cursor.next_block_number, Some(500));
    assert_eq!(cursor.target_block_number, Some(1000));
    assert_eq!(cursor.last_completed_block_number, Some(1000));

    database.cleanup().await
}

/// Only active manifest versions form the expected content set; a manifest-declared chain with
/// no checkpoint or lineage row still appears so the evaluator can gate it.
#[tokio::test]
async fn active_manifest_chain_namespaces_are_loaded() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'sf', 'ethereum-sepolia', 'e', 'active', 'n', 'f1', '{}'::jsonb),
            (1, 'basenames', 'sf', 'base-mainnet', 'e', 'deprecated', 'n', 'f2', '{}'::jsonb)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.manifest_chain_namespaces.len(), 1);
    assert_eq!(read.manifest_chain_namespaces[0].chain, "ethereum-sepolia");
    assert_eq!(read.manifest_chain_namespaces[0].namespace, "ens");
    // The declared chain has no checkpoint/lineage row, so it is absent from the storage chains.
    assert!(
        read.chains
            .iter()
            .all(|chain| chain.chain_id != "ethereum-sepolia")
    );

    database.cleanup().await
}

/// A migrated database has the deferred projection indexes; the read reports them present.
#[tokio::test]
async fn deferred_projection_indexes_present_on_migrated_database() -> Result<()> {
    let database = test_database().await?;
    let read = load_data_completeness(database.pool()).await?;
    assert_eq!(
        read.present_deferred_projection_indexes.len(),
        super::DEFERRED_NORMALIZED_EVENT_INDEXES.len()
    );

    database.cleanup().await
}
