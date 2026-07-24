use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::{load_live_adapter_source_scope, load_live_adapter_target_block_number};

#[tokio::test]
async fn live_adapter_target_ignores_orphaned_selected_blocks() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "indexer_live_adapter_canonical_target",
    ))
    .await?;
    sqlx::raw_sql(
        r#"
        CREATE TYPE canonicality_state AS ENUM (
            'observed', 'canonical', 'safe', 'finalized', 'orphaned'
        );
        CREATE TABLE chain_lineage (
            chain_id TEXT NOT NULL,
            block_hash TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            canonicality_state canonicality_state NOT NULL,
            PRIMARY KEY (chain_id, block_hash)
        );
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id, block_hash, block_number, canonicality_state
        )
        VALUES
            ('testnet', '0xcanonical', 10, 'canonical'),
            ('testnet', '0xorphaned', 20, 'orphaned')
        "#,
    )
    .execute(database.pool())
    .await?;

    assert_eq!(
        load_live_adapter_target_block_number(
            database.pool(),
            "testnet",
            &["0xcanonical".to_owned(), "0xorphaned".to_owned()],
        )
        .await?,
        10
    );
    let error = load_live_adapter_target_block_number(
        database.pool(),
        "testnet",
        &["0xorphaned".to_owned()],
    )
    .await
    .expect_err("an orphan-only selection must not produce a live adapter target");
    assert!(
        error
            .to_string()
            .contains("live adapter block-hash selection is empty"),
        "unexpected orphan-only target error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn live_adapter_scope_includes_bounded_deactivated_discovery_history() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_live_adapter_retired_scope"),
        &bigname_storage::MIGRATOR,
        "failed to migrate retired live-adapter scope database",
    )
    .await?;
    let chain = "testnet";
    let boundary_hash = "0xblock10";
    let after_boundary_hash = "0xblock11";
    let root_address = "0x00000000000000000000000000000000000000a1";
    let bounded_address = "0x00000000000000000000000000000000000000b1";
    let retracted_address = "0x00000000000000000000000000000000000000c1";
    let root_id = uuid::Uuid::from_u128(0x1610);
    let bounded_id = uuid::Uuid::from_u128(0x1611);
    let retracted_id = uuid::Uuid::from_u128(0x1612);
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (
            1,
            'ens',
            'ens_v2_registry_l1',
            $1,
            'scope-test',
            'active',
            'test',
            'test/retired-scope.toml',
            $2::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .bind(chain)
    .bind(serde_json::to_string(
        &ens_v2_registry_scope_test_manifest(chain),
    )?)
    .fetch_one(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind
        )
        VALUES
            ($1, $4, 'registry'),
            ($2, $4, 'registry'),
            ($3, $4, 'registry')
        "#,
    )
    .bind(root_id)
    .bind(bounded_id)
    .bind(retracted_id)
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            source_manifest_id
        )
        VALUES ($1, $2, $3, 0, $4)
        "#,
    )
    .bind(root_id)
    .bind(chain)
    .bind(root_address)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', 'registry', $2, $3, 'registry', 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(root_id)
    .bind(root_address)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            admitted_at,
            deactivated_at,
            active_from_block_number,
            active_from_block_hash,
            active_to_block_number,
            active_to_block_hash,
            source_manifest_id
        )
        VALUES
            ($1, $3, $4, now(), now(), 5, '0xblock5', NULL, NULL, $6),
            ($2, $3, $5, now(), now(), 5, '0xblock5', NULL, NULL, $6)
        "#,
    )
    .bind(bounded_id)
    .bind(retracted_id)
    .bind(chain)
    .bind(bounded_address)
    .bind(retracted_address)
    .bind(manifest_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            admitted_at,
            deactivated_at,
            active_from_block_number,
            active_from_block_hash,
            active_to_block_number,
            active_to_block_hash,
            provenance
        )
        VALUES
            (
                $1, 'subregistry', $2, $3, 'test', $5, 'admitted',
                now(), now(), 5, '0xblock5', 10, $6, '{}'::JSONB
            ),
            (
                $1, 'subregistry', $2, $4, 'test', $5, 'admitted',
                now(), now(), 5, '0xblock5', NULL, NULL, '{}'::JSONB
            )
        "#,
    )
    .bind(chain)
    .bind(root_id)
    .bind(bounded_id)
    .bind(retracted_id)
    .bind(manifest_id)
    .bind(boundary_hash)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            parent_hash,
            canonicality_state
        )
        VALUES
            ($1, $2, 10, now(), NULL, 'safe'),
            ($1, $3, 11, now(), $2, 'canonical')
        "#,
    )
    .bind(chain)
    .bind(boundary_hash)
    .bind(after_boundary_hash)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            canonicality_state
        )
        VALUES
            ($1, $2, 10, '0xbounded', 0, 0, $3, 'canonical'),
            ($1, $2, 10, '0xretracted', 0, 1, $4, 'canonical'),
            ($1, $5, 11, '0xafter-boundary', 0, 0, $3, 'canonical')
        "#,
    )
    .bind(chain)
    .bind(boundary_hash)
    .bind(bounded_address)
    .bind(retracted_address)
    .bind(after_boundary_hash)
    .execute(database.pool())
    .await?;
    let parent_updated_topic = format!(
        "0x{}",
        alloy_primitives::hex::encode(alloy_primitives::keccak256(
            b"ParentUpdated(address,string,address)"
        ))
    );
    let parent_topic = format!("0x{:0>64}", root_address.trim_start_matches("0x"));
    let sender_topic = format!("0x{:0>64}", "0000000000000000000000000000000000000dad");
    let mut parent_updated_data = vec![0_u8; 96];
    parent_updated_data[31] = 32;
    parent_updated_data[63] = 5;
    parent_updated_data[64..69].copy_from_slice(b"child");
    sqlx::query(
        r#"
        UPDATE raw_logs
        SET topics = $1,
            data = $2
        WHERE chain_id = $3
          AND transaction_hash = '0xbounded'
        "#,
    )
    .bind(vec![parent_updated_topic, parent_topic, sender_topic])
    .bind(parent_updated_data)
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, 0)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation = retention_generation,
            proven_discovery_admission_epoch = (
                SELECT epoch
                FROM discovery_admission_epochs
                WHERE chain_id = $1
            ),
            proven_through_block = 10
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let boundary_scope =
        load_live_adapter_source_scope(database.pool(), chain, &[boundary_hash.to_owned()]).await?;
    assert_eq!(
        boundary_scope,
        vec![(
            "ens_v2_registry_l1".to_owned(),
            bounded_address.to_owned(),
            10,
            10,
        )],
        "bounded retired history must remain replayable while unbounded retractions stay excluded"
    );
    assert!(
        load_live_adapter_source_scope(database.pool(), chain, &[after_boundary_hash.to_owned()],)
            .await?
            .is_empty(),
        "a bounded retired target must stop contributing scope after its terminal block"
    );
    let summary =
        bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
            database.pool(),
            chain,
            &[boundary_hash.to_owned()],
            &boundary_scope,
        )
        .await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_normalized_event_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            r#"
            SELECT after_state ->> 'registry_name'
            FROM normalized_events
            WHERE event_kind = 'ParentChanged'
              AND block_hash = $1
            "#,
        )
        .bind(boundary_hash)
        .fetch_one(database.pool())
        .await?,
        Some("child.eth".to_owned()),
        "rewind derivation must process the retired discovered emitter at its boundary"
    );

    database.cleanup().await
}

fn ens_v2_registry_scope_test_manifest(chain: &str) -> serde_json::Value {
    serde_json::json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain,
        "deployment_epoch": "scope-test",
        "rollout_status": "active",
        "normalizer_version": "test",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {
            "events": [
                {
                    "name": "LabelRegistered",
                    "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)"
                },
                {
                    "name": "LabelReserved",
                    "fragment": "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)"
                },
                {
                    "name": "LabelReserved",
                    "fragment": "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint256 expiry, address indexed sender)"
                },
                {
                    "name": "LabelUnregistered",
                    "fragment": "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)"
                },
                {
                    "name": "ExpiryUpdated",
                    "fragment": "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)"
                },
                {
                    "name": "SubregistryUpdated",
                    "fragment": "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)"
                },
                {
                    "name": "ResolverUpdated",
                    "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)"
                },
                {
                    "name": "TokenResource",
                    "fragment": "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)"
                },
                {
                    "name": "TokenRegenerated",
                    "fragment": "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)"
                },
                {
                    "name": "ParentUpdated",
                    "fragment": "event ParentUpdated(address indexed parent, string label, address indexed sender)"
                },
                {
                    "name": "TransferSingle",
                    "fragment": "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)"
                },
                {
                    "name": "TransferBatch",
                    "fragment": "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)"
                },
            ]
        }
    })
}
