use bigname_adapters::schema_v2::{
    BatchOutput, MigrationCandidateEffect, MigrationDiscoveryAssociation, MigrationEventAssociation,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn values_boundary_and_idempotent_replay_preserve_migration_rows() -> TestResult {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "interpret_migration_values_boundary",
    ))
    .await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('batch-test', '0x01', 1, to_timestamp(1), 'canonical')",
    )
    .execute(database.pool())
    .await?;
    let contract_instance_id = Uuid::from_u128(1);
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind, provenance
         ) VALUES ($1, 'batch-test', 'contract', '{}')",
    )
    .bind(contract_instance_id)
    .execute(database.pool())
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', 'batch-test', 'batch-test', 'fixture', 'active',
                   'test', 'batch-test.toml', '{}')
         RETURNING manifest_id",
    )
    .fetch_one(database.pool())
    .await?;
    let event_associations = (0..501)
        .map(|index| MigrationEventAssociation {
            event_identity: format!("event-{index:03}"),
            migration_correlation_id: format!("correlation-{index:03}"),
            correlation_kind: "authority_transition".to_owned(),
            evidence_refs: json!([]),
            chain_id: "batch-test".to_owned(),
            block_number: 1,
            block_hash: "0x01".to_owned(),
            transaction_hash: format!("0xtx{index:03}"),
            transaction_index: index,
            log_index: index,
            canonicality_state: "canonical".to_owned(),
            consumer_visibility: "candidate".to_owned(),
        })
        .collect::<Vec<_>>();
    let discovery_associations = (0..501)
        .map(|index| MigrationDiscoveryAssociation {
            logical_edge_identity: format!("edge-{index:03}"),
            migration_correlation_id: format!("correlation-{index:03}"),
            registry_contract_instance_id: contract_instance_id,
            registry_address: format!("0x{index:040x}"),
            source_manifest_id: manifest_id,
            evidence_refs: json!([]),
            chain_id: "batch-test".to_owned(),
            block_number: 1,
            block_hash: "0x01".to_owned(),
            transaction_hash: format!("0xtx{index:03}"),
            transaction_index: index,
            log_index: index,
            canonicality_state: "canonical".to_owned(),
            consumer_visibility: "candidate".to_owned(),
        })
        .collect::<Vec<_>>();
    let effects = |prefix: &str, correlation_kind: &str| {
        (0..501)
            .map(|index| MigrationCandidateEffect {
                effect_identity: format!("{prefix}-{index:03}"),
                migration_correlation_ids: vec![format!("correlation-{index:03}")],
                correlation_kind: correlation_kind.to_owned(),
                effect_kind: "test_effect".to_owned(),
                proposed_effect: json!({"row": index}),
                evidence_refs: json!([]),
                chain_id: "batch-test".to_owned(),
                block_number: 1,
                block_hash: "0x01".to_owned(),
                transaction_hash: format!("0xtx{index:03}"),
                transaction_index: index,
                log_index: index,
                canonicality_state: "canonical".to_owned(),
                consumer_visibility: "candidate".to_owned(),
            })
            .collect::<Vec<_>>()
    };
    let output = BatchOutput {
        migration_event_associations: event_associations,
        migration_discovery_associations: discovery_associations,
        migration_candidate_identity_effects: effects("identity", "authority_transition"),
        migration_candidate_discovery_effects: effects("discovery", "discovery_test"),
        ..BatchOutput::default()
    };
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'test-hash', true)")
        .execute(&mut *transaction)
        .await?;
    write(&mut transaction, &output).await?;
    transaction.commit().await?;
    let mut replay = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'test-hash', true)")
        .execute(&mut *replay)
        .await?;
    write(&mut replay, &output).await?;
    replay.commit().await?;

    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM migration_event_associations),
             (SELECT count(*) FROM migration_discovery_associations),
             (SELECT count(*) FROM migration_candidate_identity_effects),
             (SELECT count(*) FROM migration_candidate_discovery_effects)",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(counts, (501, 501, 501, 501));
    database.cleanup().await?;
    Ok(())
}
