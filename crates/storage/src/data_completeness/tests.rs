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

/// Only active manifest versions that declare normalized-event outputs form the expected content
/// set. An active execution/transport manifest with no event ABI (the checked-in
/// `basenames_execution`/`basenames_l1_compat` shape) is excluded, so a complete database is not
/// required to have events for a namespace no active manifest emits. A qualifying manifest-
/// declared chain with no checkpoint or lineage row still appears so the evaluator can gate it.
#[tokio::test]
async fn active_event_producing_manifest_chain_namespaces_are_loaded() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia', 'e', 'active', 'n', 'f1',
             '{"abi": {"events": [{"name": "NewOwner", "normalized_events": ["AuthorityTransferred"]}]}}'::jsonb),
            (1, 'basenames', 'basenames_execution', 'ethereum-mainnet', 'e', 'active', 'n', 'f2',
             '{"abi": {"events": [{"name": "x", "normalized_events": []}]}}'::jsonb),
            (1, 'basenames', 'basenames_l1_compat', 'ethereum-mainnet', 'e', 'active', 'n', 'f3',
             '{}'::jsonb),
            (1, 'ens', 'ens_v2_registry_l1', 'base-mainnet', 'e', 'deprecated', 'n', 'f4',
             '{"abi": {"events": [{"name": "NewOwner", "normalized_events": ["AuthorityTransferred"]}]}}'::jsonb)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    // Only the active ens manifest that declares normalized-event outputs is expected.
    assert_eq!(read.manifest_chain_namespaces.len(), 1);
    assert_eq!(read.manifest_chain_namespaces[0].chain, "ethereum-sepolia");
    assert_eq!(read.manifest_chain_namespaces[0].namespace, "ens");
    // The active execution/compat manifests declare no normalized-event outputs, so
    // (ethereum-mainnet, basenames) is not required to have events.
    assert!(
        read.manifest_chain_namespaces
            .iter()
            .all(|entry| entry.chain != "ethereum-mainnet")
    );
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

/// Observed code addresses collapse case-insensitively and carry the highest non-orphaned
/// observation block, so coverage can require an observation within a target's active range and
/// an orphaned observation does not raise the block.
#[tokio::test]
async fn observed_code_addresses_carry_max_non_orphaned_block() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO raw_code_hashes
            (chain_id, block_hash, block_number, contract_address, code_hash,
             code_byte_length, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xa', 100, '0xABCD', 'h', 1, 'canonical'::canonicality_state),
            ('ethereum-sepolia', '0xb', 250, '0xabcd', 'h', 1, 'canonical'::canonicality_state),
            ('ethereum-sepolia', '0xc', 500, '0xabcd', 'h', 1, 'orphaned'::canonicality_state)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let observed = read
        .observed_code_addresses
        .iter()
        .find(|entry| entry.chain_id == "ethereum-sepolia" && entry.address == "0xabcd")
        .expect("observed address");
    // The mixed-case rows collapse to one address; the orphaned row at 500 is excluded, so the
    // highest non-orphaned observation is 250.
    assert_eq!(observed.observed_block_number, 250);

    database.cleanup().await
}

/// A canonical branch with one row per height whose parent hashes do not link is disconnected.
/// The distinct-block-number span count cannot see it, so the parent-linkage count must.
#[tokio::test]
async fn disconnected_canonical_parent_is_counted() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0x100', NULL,      100, now(), 'canonical'::canonicality_state),
            ('ethereum-sepolia', '0x101', '0x100',   101, now(), 'canonical'::canonicality_state),
            ('ethereum-sepolia', '0x102', '0xbroken', 102, now(), 'canonical'::canonicality_state)
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
    // Height 102's parent points to no canonical row at 101 (whose hash is 0x101). Height 100 is
    // the floor (no predecessor) and 101 links to 100, so only 102 is counted.
    assert_eq!(chain.disconnected_canonical_parent_count, 1);
    // The span is contiguous by height: three distinct block numbers, no duplicate heights.
    assert_eq!(chain.lineage_canonical_block_count, 3);
    assert_eq!(chain.duplicate_canonical_height_count, 0);

    database.cleanup().await
}

/// A `CREATE INDEX CONCURRENTLY` that fails on a uniqueness violation leaves an invalid index
/// that `pg_indexes` still lists. The read must require `indisvalid`, so a replay-critical index
/// left unusable by a failed rebuild is not reported present.
#[tokio::test]
async fn invalid_deferred_index_is_not_reported_present() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    // Duplicate namespaces make a unique concurrent build fail; the failure leaves an invalid
    // index of the same name behind, which the naive pg_indexes lookup would still list.
    sqlx::query(
        r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family,
             manifest_version, derivation_kind, chain_id, canonicality_state)
        VALUES
            ('e1', 'dup', 'k', 'sf', 1, 'd', 'ethereum-sepolia', 'canonical'::canonicality_state),
            ('e2', 'dup', 'k', 'sf', 1, 'd', 'ethereum-sepolia', 'canonical'::canonicality_state)
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP INDEX normalized_events_namespace_idx")
        .execute(pool)
        .await?;
    let build = sqlx::query(
        "CREATE UNIQUE INDEX CONCURRENTLY normalized_events_namespace_idx \
         ON normalized_events (namespace)",
    )
    .execute(pool)
    .await;
    assert!(
        build.is_err(),
        "the concurrent unique build should fail on the duplicate namespace rows"
    );

    let read = load_data_completeness(pool).await?;
    assert!(
        !read
            .present_deferred_projection_indexes
            .iter()
            .any(|name| name == "normalized_events_namespace_idx"),
        "an invalid index must not be reported present"
    );

    database.cleanup().await
}

/// An active manifest-declared instance whose only `contract_instance_addresses` row is
/// deactivated is dropped from the watch view (which reads the address from a live row), so it is
/// surfaced here; a sibling instance with a live address is not.
#[tokio::test]
async fn manifest_declared_target_without_live_address_is_surfaced() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia', 'e', 'active', 'n', 'f1', '{}'::jsonb)
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES
            ('11111111-1111-1111-1111-111111111111'::uuid, 'ethereum-sepolia', 'contract'),
            ('22222222-2222-2222-2222-222222222222'::uuid, 'ethereum-sepolia', 'contract')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances
            (manifest_id, declaration_kind, declaration_name, contract_instance_id,
             declared_address, role)
        VALUES
            ((SELECT manifest_id FROM manifest_versions LIMIT 1), 'contract', 'registry',
             '11111111-1111-1111-1111-111111111111'::uuid, '0xLIVE', 'registry'),
            ((SELECT manifest_id FROM manifest_versions LIMIT 1), 'contract', 'resolver',
             '22222222-2222-2222-2222-222222222222'::uuid, '0xGONE', 'resolver')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address, deactivated_at)
        VALUES
            ('11111111-1111-1111-1111-111111111111'::uuid, 'ethereum-sepolia', '0xLIVE', NULL),
            ('22222222-2222-2222-2222-222222222222'::uuid, 'ethereum-sepolia', '0xGONE', now())
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    // Only the instance whose sole address row is deactivated is surfaced.
    assert_eq!(read.manifest_declared_targets_missing_address.len(), 1);
    let target = &read.manifest_declared_targets_missing_address[0];
    assert_eq!(target.chain, "ethereum-sepolia");
    assert_eq!(target.address, "0xgone");
    assert_eq!(target.source_family, "ens_v2_registry_l1");

    database.cleanup().await
}
