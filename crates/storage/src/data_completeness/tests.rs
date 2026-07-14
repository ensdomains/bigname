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

/// A complete set of heights is not a connected branch when a child does not point to the
/// canonical hash at the preceding height.
#[tokio::test]
async fn disconnected_canonical_parents_are_counted() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xaa', '0x99', 100, now(), 'canonical'),
            ('ethereum-sepolia', '0xbb', '0xdead', 101, now(), 'canonical')
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
    assert_eq!(chain.lineage_canonical_block_count, 2);
    assert_eq!(chain.disconnected_canonical_parent_count, 1);

    database.cleanup().await
}

/// Code coverage needs the observation height so the evaluator can reject observations from
/// before a target's inclusive active start.
#[tokio::test]
async fn latest_code_observation_block_is_loaded() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO raw_code_hashes
            (chain_id, block_hash, block_number, contract_address, code_hash,
             code_byte_length, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xaa', 10, '0xabc', '0x01', 1, 'canonical'),
            ('ethereum-sepolia', '0xbb', 20, '0xAbC', '0x02', 1, 'canonical'),
            ('ethereum-sepolia', '0xcc', 30, '0xabc', '0x03', 1, 'orphaned')
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.observed_code_addresses.len(), 1);
    assert_eq!(
        read.observed_code_addresses[0].max_observed_block_number,
        20
    );

    database.cleanup().await
}

/// Direct manifest declarations are loaded without depending on the materialized
/// `contract_instance_addresses` row.
#[tokio::test]
async fn manifest_declared_target_is_loaded_without_address_row() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia', 'e', 'active',
             'n', 'f', '{"contracts":[{"role":"registry","address":"0xAbC","start_block":42}]}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    let contract_instance_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000042")?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, 'ethereum-sepolia', 'registry')
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances
            (manifest_id, declaration_kind, declaration_name, contract_instance_id,
             declared_address, role, proxy_kind)
        VALUES ($1, 'contract', 'registry', $2, '0xAbC', 'registry', 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.manifest_declared_targets.len(), 1);
    assert_eq!(read.manifest_declared_targets[0].address, "0xabc");
    assert_eq!(
        read.manifest_declared_targets[0].active_from_block_number,
        Some(42)
    );

    // The materialized watch row may start later, but direct manifest history authority stays
    // at the payload's declared start.
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address, active_from_block_number)
        VALUES ($1, 'ethereum-sepolia', '0xAbC', 900)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert_eq!(
        read.manifest_declared_targets[0].active_from_block_number,
        Some(42)
    );
    assert!(read.manifest_declared_targets_missing_address.is_empty());

    database.cleanup().await
}

/// A live address row only satisfies a manifest declaration when both its chain and address
/// match. Deactivated and mismatched rows remain explicit authority gaps.
#[tokio::test]
async fn manifest_declared_targets_require_matching_live_address_rows() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia', 'e', 'active',
             'n', 'f', '{}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES
            ('11111111-1111-1111-1111-111111111111', 'ethereum-sepolia', 'contract'),
            ('22222222-2222-2222-2222-222222222222', 'ethereum-sepolia', 'contract'),
            ('33333333-3333-3333-3333-333333333333', 'ethereum-sepolia', 'contract'),
            ('44444444-4444-4444-4444-444444444444', 'ethereum-sepolia', 'contract')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances
            (manifest_id, declaration_kind, declaration_name, contract_instance_id,
             declared_address, role, proxy_kind)
        VALUES
            ($1, 'contract', 'exact',
             '11111111-1111-1111-1111-111111111111', '0xEXACT', 'exact', 'none'),
            ($1, 'contract', 'deactivated',
             '22222222-2222-2222-2222-222222222222', '0xGONE', 'deactivated', 'none'),
            ($1, 'contract', 'wrong_address',
             '33333333-3333-3333-3333-333333333333', '0xDECLARED', 'wrong_address', 'none'),
            ($1, 'contract', 'wrong_chain',
             '44444444-4444-4444-4444-444444444444', '0xCHAIN', 'wrong_chain', 'none')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address, deactivated_at)
        VALUES
            ('11111111-1111-1111-1111-111111111111', 'ethereum-sepolia', '0xexact', NULL),
            ('22222222-2222-2222-2222-222222222222', 'ethereum-sepolia', '0xgone', now()),
            ('33333333-3333-3333-3333-333333333333', 'ethereum-sepolia', '0xOTHER', NULL),
            ('44444444-4444-4444-4444-444444444444', 'base-sepolia', '0xchain', NULL)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let missing = read
        .manifest_declared_targets_missing_address
        .iter()
        .map(|target| target.address.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        missing,
        ["0xchain", "0xdeclared", "0xgone"].into_iter().collect()
    );
    assert!(!missing.contains("0xexact"));

    database.cleanup().await
}

/// Only active manifests that declare normalized adapter output form content expectations,
/// and residual rows from a deprecated manifest ID do not count for the active identity.
#[tokio::test]
async fn active_event_sources_are_counted_by_exact_manifest_identity() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let active_event_manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (2, 'ens', 'registry', 'ethereum-sepolia', 'e', 'active', 'n', 'active',
             '{"abi":{"events":[{"normalized_events":["ResolverChanged"]}]}}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    let deprecated_manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'registry', 'ethereum-sepolia', 'e', 'deprecated', 'n', 'old',
             '{"abi":{"events":[{"normalized_events":["ResolverChanged"]}]}}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'basenames', 'basenames_execution', 'ethereum-mainnet', 'e',
             'active', 'n', 'metadata', '{}'::jsonb)
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, derivation_kind, canonicality_state)
        VALUES
            ('stale', 'ens', 'ResolverChanged', 'registry', 1, $1,
             'ethereum-sepolia', 'raw_log', 'canonical')
        "#,
    )
    .bind(deprecated_manifest_id)
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.active_manifest_event_sources.len(), 1);
    let source = &read.active_manifest_event_sources[0];
    assert_eq!(source.manifest_id, active_event_manifest_id);
    assert_eq!(source.normalized_event_count, 0);

    database.cleanup().await
}

/// Orphaned events and NULL-chain events must not satisfy the per-chain content check; the
/// NULL rows are counted separately as a data-integrity signal.
#[tokio::test]
async fn orphaned_and_null_chain_events_excluded_from_counts() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'sf', 'ethereum-sepolia', 'e', 'active', 'n', 'f',
             '{"abi":{"events":[{"normalized_events":["ResolverChanged"]}]}}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, derivation_kind, chain_id,
             canonicality_state)
        VALUES
            ('e1', 'ens', 'ResolverChanged', 'sf', 1, $1, 'd',
             'ethereum-sepolia', 'canonical'::canonicality_state),
            ('e2', 'ens', 'ResolverChanged', 'sf', 1, $1, 'd',
             'ethereum-sepolia', 'orphaned'::canonicality_state),
            ('e3', 'ens', 'ResolverChanged', 'sf', 1, $1, 'd',
             NULL, 'canonical'::canonicality_state)
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.active_manifest_event_sources.len(), 1);
    assert_eq!(
        read.active_manifest_event_sources[0].normalized_event_count,
        1
    );
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

/// Projection replay inspection carries both the writer's completed target and the target a
/// bootstrap would request now, including the chain-checkpoint side of that maximum.
#[tokio::test]
async fn projection_replay_marker_and_required_target_are_loaded() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors
            (deployment_profile, chain_id, cursor_kind, range_start_block_number,
             next_block_number, target_block_number)
        VALUES
            ('sepolia', 'ethereum-sepolia', 'raw_fact_normalized_events', 0, 121, 120)
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints
            (chain_id, canonical_block_hash, canonical_block_number,
             safe_block_hash, safe_block_number)
        VALUES ('ethereum-sepolia', '0x100', 100, '0x140', 140)
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status
            (projection, replay_version, completed_normalized_target_block,
             requested_key_count, upserted_row_count, deleted_row_count)
        VALUES ('name_current', 6, 130, 0, 0, 0)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.projection_replay_required_target_block, Some(140));
    assert_eq!(read.projection_replay_markers.len(), 1);
    assert_eq!(
        read.projection_replay_markers[0].completed_normalized_target_block,
        Some(130)
    );

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

/// A failed concurrent build leaves a named `pg_indexes` entry with `indisvalid = false`.
/// The completeness read must not report that unusable index as present.
#[tokio::test]
async fn invalid_deferred_projection_index_is_not_present() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query("DROP INDEX normalized_events_namespace_idx")
        .execute(pool)
        .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family,
             manifest_version, derivation_kind)
        VALUES
            ('invalid-index-1', 'ens', 'k', 'sf', 1, 'd'),
            ('invalid-index-2', 'ens', 'k', 'sf', 1, 'd')
        "#,
    )
    .execute(pool)
    .await?;

    let build_error = sqlx::query(
        "CREATE UNIQUE INDEX CONCURRENTLY normalized_events_namespace_idx \
         ON normalized_events (namespace)",
    )
    .execute(pool)
    .await;
    assert!(
        build_error.is_err(),
        "duplicate rows must fail the unique build"
    );

    let catalog_state = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT index_state.indisvalid
        FROM pg_index index_state
        WHERE index_state.indexrelid = 'normalized_events_namespace_idx'::regclass
        "#,
    )
    .fetch_one(pool)
    .await?;
    assert!(
        !catalog_state,
        "failed build must leave an invalid catalog entry"
    );

    let read = load_data_completeness(pool).await?;
    assert!(
        !read
            .present_deferred_projection_indexes
            .contains(&"normalized_events_namespace_idx".to_owned())
    );

    database.cleanup().await
}
