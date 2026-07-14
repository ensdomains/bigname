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
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xaa', 10, now(), 'canonical'),
            ('ethereum-sepolia', '0xbb', 20, now(), 'canonical'),
            ('ethereum-sepolia', '0xee', 50, now(), 'orphaned')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_code_hashes
            (chain_id, block_hash, block_number, contract_address, code_hash,
             code_byte_length, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xaa', 10, '0xabc', '0x01', 1, 'canonical'),
            ('ethereum-sepolia', '0xbb', 20, '0xAbC', '0x02', 1, 'canonical'),
            ('ethereum-sepolia', '0xcc', 30, '0xabc', '0x03', 1, 'orphaned'),
            ('ethereum-sepolia', '0xdd', 40, '0xabc', '0x04', 1, 'canonical'),
            ('ethereum-sepolia', '0xee', 50, '0xabc', '0x05', 1, 'canonical')
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
/// match and remain open. Deactivated, closed, and mismatched rows remain explicit authority gaps.
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
             'n', 'f', '{"contracts":[
                 {"role":"exact","address":"0xEXACT"},
                 {"role":"deactivated","address":"0xGONE"},
                 {"role":"closed","address":"0xCLOSED"},
                 {"role":"wrong_address","address":"0xDECLARED"},
                 {"role":"wrong_chain","address":"0xCHAIN"}
             ]}'::jsonb)
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
            ('55555555-5555-5555-5555-555555555555', 'ethereum-sepolia', 'contract'),
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
            ($1, 'contract', 'closed',
             '55555555-5555-5555-5555-555555555555', '0xCLOSED', 'closed', 'none'),
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
            (contract_instance_id, chain_id, address, deactivated_at, active_to_block_number)
        VALUES
            ('11111111-1111-1111-1111-111111111111', 'ethereum-sepolia', '0xexact', NULL, NULL),
            ('22222222-2222-2222-2222-222222222222', 'ethereum-sepolia', '0xgone', now(), NULL),
            ('55555555-5555-5555-5555-555555555555', 'ethereum-sepolia', '0xclosed', NULL, 99),
            ('33333333-3333-3333-3333-333333333333', 'ethereum-sepolia', '0xOTHER', NULL, NULL),
            ('44444444-4444-4444-4444-444444444444', 'base-sepolia', '0xchain', NULL, NULL)
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
        ["0xchain", "0xclosed", "0xdeclared", "0xgone"]
            .into_iter()
            .collect()
    );
    assert!(!missing.contains("0xexact"));

    database.cleanup().await
}

/// The manifest payload is authority for direct declarations. Losing a materialized
/// `manifest_contract_instances` child must leave the payload target in both the coverage
/// universe and the explicit materialization-gap report.
#[tokio::test]
async fn manifest_payload_target_survives_missing_contract_instance_row() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia', 'e', 'active',
             'n', 'f', '{
                 "roots":[{"name":"root","address":"0xROOT","start_block":7}],
                 "contracts":[{"role":"registry","address":"0xREGISTRY","start_block":11}]
             }'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ('55555555-5555-5555-5555-555555555555', 'ethereum-sepolia', 'root')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances
            (manifest_id, declaration_kind, declaration_name, contract_instance_id,
             declared_address)
        VALUES
            ($1, 'root', 'root',
             '55555555-5555-5555-5555-555555555555', '0xROOT')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address)
        VALUES
            ('55555555-5555-5555-5555-555555555555', 'ethereum-sepolia', '0xROOT')
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let targets = read
        .manifest_declared_targets
        .iter()
        .map(|target| (target.address.as_str(), target.active_from_block_number))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        targets,
        [("0xregistry", Some(11)), ("0xroot", Some(7))]
            .into_iter()
            .collect()
    );
    assert_eq!(read.manifest_declared_targets_missing_address.len(), 1);
    assert_eq!(
        read.manifest_declared_targets_missing_address[0].address,
        "0xregistry"
    );

    database.cleanup().await
}

/// A manifest-declared proxy implementation is a separate admitted and watched instance. Its
/// payload address must remain a direct coverage target and require its own live address row.
#[tokio::test]
async fn manifest_proxy_implementation_is_a_declared_target() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia', 'e', 'active',
             'n', 'f', '{"contracts":[{
                 "role":"registry",
                 "address":"0xPROXY",
                 "proxy_kind":"uups",
                 "implementation":"0xIMPLEMENTATION",
                 "start_block":17
             }]}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES
            ('66666666-6666-6666-6666-666666666666', 'ethereum-sepolia', 'contract'),
            ('77777777-7777-7777-7777-777777777777', 'ethereum-sepolia', 'contract')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances
            (manifest_id, declaration_kind, declaration_name, contract_instance_id,
             declared_address, role, proxy_kind, implementation_contract_instance_id,
             declared_implementation_address)
        VALUES
            ($1, 'contract', 'registry',
             '66666666-6666-6666-6666-666666666666', '0xPROXY', 'registry', 'uups',
             '77777777-7777-7777-7777-777777777777', '0xIMPLEMENTATION')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address)
        VALUES
            ('66666666-6666-6666-6666-666666666666', 'ethereum-sepolia', '0xPROXY')
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    let targets = read
        .manifest_declared_targets
        .iter()
        .map(|target| (target.address.as_str(), target.active_from_block_number))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        targets,
        [("0ximplementation", Some(17)), ("0xproxy", Some(17))]
            .into_iter()
            .collect()
    );
    assert_eq!(read.manifest_declared_targets_missing_address.len(), 1);
    assert_eq!(
        read.manifest_declared_targets_missing_address[0].address,
        "0ximplementation"
    );

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address)
        VALUES
            ('77777777-7777-7777-7777-777777777777',
             'ethereum-sepolia', '0xIMPLEMENTATION')
        "#,
    )
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert!(read.manifest_declared_targets_missing_address.is_empty());
    assert_eq!(read.manifest_proxy_implementations_missing_edge.len(), 1);
    assert_eq!(
        read.manifest_proxy_implementations_missing_edge[0].address,
        "0ximplementation"
    );

    sqlx::query(
        r#"
        INSERT INTO discovery_edges
            (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, source_manifest_id, admission)
        VALUES
            ('ethereum-sepolia', 'proxy_implementation',
             '66666666-6666-6666-6666-666666666666',
             '77777777-7777-7777-7777-777777777777',
             'manifest_declared_proxy', $1, 'manifest_declared')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert!(read.manifest_proxy_implementations_missing_edge.is_empty());

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET active_to_block_number = 99
        WHERE source_manifest_id = $1
          AND edge_kind = 'proxy_implementation'
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert_eq!(read.manifest_proxy_implementations_missing_edge.len(), 1);
    assert_eq!(
        read.manifest_proxy_implementations_missing_edge[0].address,
        "0ximplementation"
    );

    database.cleanup().await
}

/// An open discovery edge remains current watch authority only while its target also has an open
/// address. Bounded edges are retained history and must not create current-authority failures.
#[tokio::test]
async fn discovery_target_without_live_address_is_surfaced() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'registry', 'ethereum-sepolia', 'ens_v2_sepolia_dev',
             'active', 'n', 'f', '{}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES
            ('88888888-8888-8888-8888-888888888888', 'ethereum-sepolia', 'contract'),
            ('99999999-9999-9999-9999-999999999999', 'ethereum-sepolia', 'contract')
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges
            (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, source_manifest_id, admission)
        VALUES
            ('ethereum-sepolia', 'subregistry',
             '88888888-8888-8888-8888-888888888888',
             '99999999-9999-9999-9999-999999999999',
             'registry_event', $1, 'reachable_from_root')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.discovery_targets_missing_address.len(), 1);
    assert_eq!(
        read.discovery_targets_missing_address[0].contract_instance_id,
        uuid::Uuid::parse_str("99999999-9999-9999-9999-999999999999")?
    );

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address, active_to_block_number)
        VALUES
            ('99999999-9999-9999-9999-999999999999', 'ethereum-sepolia', '0xDISCOVERED', 99)
        "#,
    )
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert_eq!(read.discovery_targets_missing_address.len(), 1);

    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = NULL
        WHERE contract_instance_id = '99999999-9999-9999-9999-999999999999'
        "#,
    )
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert!(read.discovery_targets_missing_address.is_empty());

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET active_to_block_number = 100
        WHERE source_manifest_id = $1
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = 100
        WHERE contract_instance_id = '99999999-9999-9999-9999-999999999999'
        "#,
    )
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert!(read.discovery_targets_missing_address.is_empty());

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

/// Active normalized content must retain its exact canonical lineage anchors so reorg repair
/// remains possible after restore, including when minimal retention compacted raw-log staging.
#[tokio::test]
async fn active_event_sources_report_missing_canonical_lineage() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'registry', 'ethereum-sepolia', 'ens_v2_sepolia_dev',
             'active', 'n', 'active',
             '{"abi":{"events":[{"normalized_events":["ResolverChanged"]}]}}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, block_number, block_hash, transaction_hash,
             log_index, raw_fact_ref, derivation_kind, canonicality_state)
        VALUES
            ('active-event', 'ens', 'ResolverChanged', 'registry', 1, $1,
             'ethereum-sepolia', 42, '0xBLOCK', '0xTX', 3,
             '{"kind":"raw_log"}'::jsonb, 'raw_log', 'canonical'),
            ('boundary-event', 'ens', 'ResolverChanged', 'registry', 1, $1,
             'ethereum-sepolia', 42, '0xBLOCK', NULL, NULL,
             '{"kind":"raw_block"}'::jsonb, 'synthetic_boundary', 'canonical')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(
        read.active_manifest_event_sources[0].normalized_events_missing_canonical_lineage_count,
        2
    );
    assert_eq!(
        read.active_manifest_event_sources[0].normalized_event_count,
        2
    );

    sqlx::query(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        VALUES
            ('ethereum-sepolia', '0xBLOCK', 42, now(), 'orphaned')
        "#,
    )
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert_eq!(
        read.active_manifest_event_sources[0].normalized_events_missing_canonical_lineage_count,
        2
    );

    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'
        WHERE chain_id = 'ethereum-sepolia'
          AND block_hash = '0xBLOCK'
        "#,
    )
    .execute(pool)
    .await?;
    let read = load_data_completeness(pool).await?;
    assert_eq!(
        read.active_manifest_event_sources[0].normalized_events_missing_canonical_lineage_count,
        0
    );

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

/// Projection replay inspection carries both the writer's completed target and the global target
/// a bootstrap would request now. Per-chain foreign-state advisories do not scope this marker:
/// automatic projection replay uses the same unscoped checkpoint maximum.
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
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'registry', 'ethereum-sepolia', 'e', 'active', 'n', 'active',
             '{}'::jsonb)
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints
            (chain_id, canonical_block_hash, canonical_block_number,
             safe_block_hash, safe_block_number)
        VALUES
            ('ethereum-sepolia', '0x100', 100, '0x140', 140),
            ('retired-chain', '0x200', 200, NULL, NULL)
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
    assert_eq!(read.projection_replay_required_target_block, Some(200));
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

/// Replay cursor selection uses the deployment profile implied by the active manifest corpus,
/// matching the indexer's replay-admission writer boundary.
#[tokio::test]
async fn active_manifest_deployment_profile_is_inferred() -> Result<()> {
    let database = test_database().await?;
    let pool = database.pool();
    sqlx::query(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'ens', 'registry', 'ethereum-sepolia', 'ens_v2_sepolia_dev',
             'active', 'n', 'f', '{}'::jsonb)
        "#,
    )
    .execute(pool)
    .await?;

    let read = load_data_completeness(pool).await?;
    assert_eq!(read.active_deployment_profile.as_deref(), Some("sepolia"));

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
