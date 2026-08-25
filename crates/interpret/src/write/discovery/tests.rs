use bigname_adapters::schema_v2::{BatchOutput, ContractAddress, ContractInstance};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;
use crate::{BatchRequest, Engine, RunMode};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn proxy_upgrade_discovery_preserves_omitted_manifest_floor_without_repair() -> TestResult {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "interpret_discovery_omitted_manifest_floor",
    ))
    .await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
        include_str!("../../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../../schema-v2/baseline/12_project_generation_failures.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = load_repository(&manifest_root)?;
    sync_schema_v2_repository(database.pool(), &repository).await?;

    // Production proxy and implementation addresses:
    // (upstream: .refs/basenames/README.md:L36 @ basenames@1809bbc)
    // (upstream: .refs/basenames/README.md:L37 @ basenames@1809bbc).
    let proxy_address = "0xa7d2607c6bd39ae9521e514026cbb078405ab322";
    let declared_address = "0x9ad14968093c5e8c2a8cc86f6868cfee8c659717";
    let (source_manifest_id, initial_floor): (i64, Option<i64>) = sqlx::query_as(
        "SELECT source_manifest_id, active_from_block_number
         FROM contract_instance_addresses
         WHERE chain_id = 'base-mainnet' AND lower(address) = $1
           AND deactivated_at IS NULL",
    )
    .bind(declared_address)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(initial_floor, None);

    let discovery_block = 42;
    let block_hash = "base-mainnet-block-42";
    let transaction_hash = "base-mainnet-transaction-42";
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('base-mainnet', $1, $2, to_timestamp($2), 'canonical')",
    )
    .bind(block_hash)
    .bind(discovery_block)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         ) VALUES ('base-mainnet', $1, $2, $3, 0, $4, $5)",
    )
    .bind(block_hash)
    .bind(discovery_block)
    .bind(transaction_hash)
    .bind("0x0000000000000000000000000000000000000001")
    .bind(proxy_address)
    .execute(database.pool())
    .await?;
    // ERC-1967 Upgraded(address):
    // (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/interfaces/IERC1967.sol:L13 @ basenames@1809bbc).
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics
         ) VALUES ('base-mainnet', $1, $2, $3, 0, 0, $4, $5)",
    )
    .bind(block_hash)
    .bind(discovery_block)
    .bind(transaction_hash)
    .bind(proxy_address)
    .bind(vec![
        "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b".to_owned(),
        "0x0000000000000000000000009ad14968093c5e8c2a8cc86f6868cfee8c659717".to_owned(),
    ])
    .execute(database.pool())
    .await?;
    Engine::new(database.pool().clone())
        .run_batch(BatchRequest {
            chain_id: "base-mainnet".to_owned(),
            from_block: discovery_block,
            to_block: discovery_block,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let discovery_only_instance_id = Uuid::from_u128(547);
    let discovery_only_address = "0x0000000000000000000000000000000000000547";
    let output = BatchOutput {
        contract_instances: vec![ContractInstance {
            contract_instance_id: discovery_only_instance_id,
            chain_id: "base-mainnet".to_owned(),
            contract_kind: "contract".to_owned(),
            provenance: json!({"discovered_at": discovery_block}),
        }],
        contract_addresses: vec![ContractAddress {
            contract_instance_id: discovery_only_instance_id,
            chain_id: "base-mainnet".to_owned(),
            address: discovery_only_address.to_owned(),
            active_from_block_number: discovery_block,
            active_from_block_hash: block_hash.to_owned(),
            source_manifest_id,
            provenance: json!({"discovered_at": discovery_block}),
        }],
        ..BatchOutput::default()
    };
    let mut transaction = database.pool().begin().await?;
    write(&mut transaction, &output, false).await?;
    transaction.commit().await?;

    let refreshed_floor: Option<i64> = sqlx::query_scalar(
        "SELECT active_from_block_number FROM contract_instance_addresses
         WHERE chain_id = 'base-mainnet' AND lower(address) = $1
           AND deactivated_at IS NULL",
    )
    .bind(declared_address)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(refreshed_floor, None);
    let discovery_only_floor: Option<i64> = sqlx::query_scalar(
        "SELECT active_from_block_number FROM contract_instance_addresses
         WHERE chain_id = 'base-mainnet' AND lower(address) = $1
           AND deactivated_at IS NULL",
    )
    .bind(discovery_only_address)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(discovery_only_floor, Some(discovery_block));

    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number
         ) VALUES ('base-mainnet', 'rpc', 'rpc', 'new_signature_range', 0, $1, $2)",
    )
    .bind(discovery_block + 1)
    .bind(discovery_block)
    .execute(database.pool())
    .await?;
    for (phase, hash) in [
        ("ingest", None),
        ("interpret", Some("stable-interpret")),
        ("project", Some("stable-project")),
    ] {
        sqlx::query(
            "INSERT INTO chain_phase_state (
                 chain_id, phase_name, phase_status, current_block_number,
                 current_block_hash, target_block_number, target_block_hash,
                 input_content_hash, started_at, finished_at
             ) VALUES ('base-mainnet', $1, 'completed', $2, $3, $2, $3, $4, now(), now())",
        )
        .bind(phase)
        .bind(discovery_block)
        .bind("base-mainnet-block-42")
        .bind(hash)
        .execute(database.pool())
        .await?;
    }

    sync_schema_v2_repository(database.pool(), &repository).await?;
    let required_ingest: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = 'base-mainnet' AND phase_name = 'ingest'",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(!required_ingest);
    let derived_hashes: Vec<String> = sqlx::query_scalar(
        "SELECT input_content_hash FROM chain_phase_state
         WHERE chain_id = 'base-mainnet' AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(derived_hashes, ["stable-interpret", "stable-project"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn values_boundary_and_idempotent_replay_persist_every_contract_instance() -> TestResult {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "interpret_contract_instances_values_boundary",
    ))
    .await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    let output = BatchOutput {
        contract_instances: (0_u128..501)
            .map(|index| ContractInstance {
                contract_instance_id: Uuid::from_u128(index + 1),
                chain_id: "batch-test".to_owned(),
                contract_kind: if index == 0 { "root" } else { "contract" }.to_owned(),
                provenance: json!({"row": index}),
            })
            .collect(),
        ..BatchOutput::default()
    };
    let mut transaction = database.pool().begin().await?;
    write(&mut transaction, &output, false).await?;
    transaction.commit().await?;
    let mut replay = database.pool().begin().await?;
    write(&mut replay, &output, false).await?;
    replay.commit().await?;

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM contract_instances")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(count, 501);
    let last: (Uuid, String, serde_json::Value) = sqlx::query_as(
        "SELECT contract_instance_id, contract_kind, provenance
         FROM contract_instances ORDER BY contract_instance_id DESC LIMIT 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        last,
        (
            Uuid::from_u128(501),
            "contract".to_owned(),
            json!({"row": 500})
        ),
    );
    database.cleanup().await?;
    Ok(())
}
