use bigname_adapters::schema_v2::{
    BatchOutput, MigrationCandidateEffect, MigrationDiscoveryAssociation, MigrationEventAssociation,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Fixture {
    database: TestDatabase,
    contract_instance_id: Uuid,
    manifest_id: i64,
}

async fn fixture(name: &str) -> TestResult<Fixture> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
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
    let manifest_id = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', 'batch-test', 'batch-test', 'fixture', 'active',
                   'test', 'batch-test.toml', '{}')
         RETURNING manifest_id",
    )
    .fetch_one(database.pool())
    .await?;
    Ok(Fixture {
        database,
        contract_instance_id,
        manifest_id,
    })
}

fn event(index: i64) -> MigrationEventAssociation {
    MigrationEventAssociation {
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
    }
}

fn discovery(
    index: i64,
    contract_instance_id: Uuid,
    manifest_id: i64,
) -> MigrationDiscoveryAssociation {
    MigrationDiscoveryAssociation {
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
    }
}

fn effect(prefix: &str, correlation_kind: &str, index: i64) -> MigrationCandidateEffect {
    MigrationCandidateEffect {
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
    }
}

async fn write_output(database: &TestDatabase, output: &BatchOutput) -> TestResult {
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'test-hash', true)")
        .execute(&mut *transaction)
        .await?;
    write(&mut transaction, output).await?;
    transaction.commit().await?;
    Ok(())
}

async fn assert_conflict(
    database: &TestDatabase,
    table: &str,
    seed: BatchOutput,
    attempted: BatchOutput,
    expected_row: &str,
) -> TestResult {
    write_output(database, &seed).await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'test-hash', true)")
        .execute(&mut *transaction)
        .await?;
    let error = write(&mut transaction, &attempted)
        .await
        .expect_err("divergent stored migration evidence must fail");
    transaction.rollback().await?;
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    let message = error.to_string();
    let expected_message = match table {
        "migration_event_associations" => "migration event associations are already bound",
        "migration_discovery_associations" => "migration discovery associations are already bound",
        _ => "migration candidate effects",
    };
    assert!(message.contains(expected_message), "{message}");
    assert!(message.contains(expected_row), "{message}");
    let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(database.pool())
        .await?;
    assert_eq!(count, 1, "the submitted prefix and suffix must roll back");
    Ok(())
}

#[tokio::test]
async fn values_boundary_and_idempotent_replay_preserve_migration_rows() -> TestResult {
    let Fixture {
        database,
        contract_instance_id,
        manifest_id,
    } = fixture("interpret_migration_values_boundary").await?;
    let output = BatchOutput {
        migration_event_associations: (0..501).map(event).collect(),
        migration_discovery_associations: (0..501)
            .map(|index| discovery(index, contract_instance_id, manifest_id))
            .collect(),
        migration_candidate_identity_effects: (0..501)
            .map(|index| effect("identity", "authority_transition", index))
            .collect(),
        migration_candidate_discovery_effects: (0..501)
            .map(|index| effect("discovery", "discovery_test", index))
            .collect(),
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

macro_rules! conflict_test {
    ($name:ident, $database:literal, $table:literal, $field:ident, $row:expr, $conflict:expr, $expected:literal) => {
        #[tokio::test]
        async fn $name() -> TestResult {
            let fixture = fixture($database).await?;
            let row = $row;
            assert_conflict(
                &fixture.database,
                $table,
                BatchOutput {
                    $field: vec![row(&fixture, 1)],
                    ..BatchOutput::default()
                },
                BatchOutput {
                    $field: vec![row(&fixture, 0), ($conflict)(&fixture), row(&fixture, 2)],
                    ..BatchOutput::default()
                },
                $expected,
            )
            .await?;
            fixture.database.cleanup().await?;
            Ok(())
        }
    };
}

conflict_test!(
    conflicting_event_association_identifies_row_and_rolls_back,
    "interpret_migration_event_conflict",
    "migration_event_associations",
    migration_event_associations,
    |_: &Fixture, index| event(index),
    |_: &Fixture| {
        let mut row = event(1);
        row.evidence_refs = json!(["different"]);
        row
    },
    "1=(event-001, correlation-001)"
);
conflict_test!(
    conflicting_discovery_association_identifies_row_and_rolls_back,
    "interpret_migration_discovery_conflict",
    "migration_discovery_associations",
    migration_discovery_associations,
    |fixture: &Fixture, index| discovery(index, fixture.contract_instance_id, fixture.manifest_id),
    |fixture: &Fixture| {
        let mut row = discovery(1, fixture.contract_instance_id, fixture.manifest_id);
        row.evidence_refs = json!(["different"]);
        row
    },
    "1=(edge-001, correlation-001)"
);
conflict_test!(
    conflicting_identity_effect_identifies_row_and_rolls_back,
    "interpret_migration_identity_effect_conflict",
    "migration_candidate_identity_effects",
    migration_candidate_identity_effects,
    |_: &Fixture, index| effect("identity", "authority_transition", index),
    |_: &Fixture| {
        let mut row = effect("identity", "authority_transition", 1);
        row.proposed_effect = json!({"different": true});
        row
    },
    "1=identity-001"
);
conflict_test!(
    conflicting_discovery_effect_identifies_row_and_rolls_back,
    "interpret_migration_discovery_effect_conflict",
    "migration_candidate_discovery_effects",
    migration_candidate_discovery_effects,
    |_: &Fixture, index| effect("discovery", "discovery_test", index),
    |_: &Fixture| {
        let mut row = effect("discovery", "discovery_test", 1);
        row.proposed_effect = json!({"different": true});
        row
    },
    "1=discovery-001"
);
