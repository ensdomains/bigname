use bigname_adapters::schema_v2::{BatchOutput, LabelPreimage, NameSurface};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
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
    Ok(database)
}

async fn write_output(database: &TestDatabase, output: &BatchOutput) -> TestResult {
    let mut transaction = database.pool().begin().await?;
    write(&mut transaction, output).await?;
    transaction.commit().await?;
    Ok(())
}

fn observed_preimage(
    labelhash: &str,
    raw_label: &[u8],
    priority: i32,
    provenance: serde_json::Value,
) -> LabelPreimage {
    LabelPreimage {
        labelhash: labelhash.to_owned(),
        raw_label: raw_label.to_vec(),
        decoded_label: std::str::from_utf8(raw_label).ok().map(str::to_owned),
        normalizer_version: "test".to_owned(),
        normalized_under_version: true,
        normalization_error: None,
        source_kind: format!("source-{priority}"),
        source_priority: priority,
        provenance,
    }
}

fn surface(logical_name_id: &str, raw_name: &str) -> NameSurface {
    let namehash = logical_name_id
        .strip_prefix("ens:")
        .expect("test logical IDs use the ENS namespace");
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        raw_name: raw_name.to_owned(),
        raw_labels: vec![raw_name.to_owned()],
        dns_encoded_name: raw_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: vec![format!("label:{raw_name}")],
        normalizer_version: "test".to_owned(),
        visibility_state: "active".to_owned(),
        normalization_errors: json!([]),
        deactivation_reason: None,
        deactivated_at: None,
        chain_id: "batch-test".to_owned(),
        block_hash: "0x01".to_owned(),
        block_number: 1,
        provenance: json!({"raw_name": raw_name}),
        canonicality_state: "canonical".to_owned(),
    }
}

#[tokio::test]
async fn conflicting_preimage_identifies_row_and_rolls_back_prefix() -> TestResult {
    let database = database("interpret_preimage_conflict_rollback").await?;
    write_output(
        &database,
        &BatchOutput {
            label_preimages: vec![observed_preimage(
                "conflict",
                b"stored",
                5,
                json!({"stored": true}),
            )],
            ..BatchOutput::default()
        },
    )
    .await?;
    let output = BatchOutput {
        label_preimages: vec![
            observed_preimage("prefix", b"prefix", 1, json!({})),
            observed_preimage("conflict", b"different", 5, json!({})),
            observed_preimage("suffix", b"suffix", 1, json!({})),
        ],
        ..BatchOutput::default()
    };

    let mut transaction = database.pool().begin().await?;
    let error = write(&mut transaction, &output)
        .await
        .expect_err("the divergent stored preimage must fail");
    transaction.rollback().await?;
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("label hashes are already bound"));
    assert!(error.to_string().contains("1=conflict"));
    let hashes =
        sqlx::query_scalar::<_, String>("SELECT labelhash FROM label_preimages ORDER BY labelhash")
            .fetch_all(database.pool())
            .await?;
    assert_eq!(hashes, vec!["conflict"]);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn conflicting_surface_identifies_row_and_rolls_back_prefix() -> TestResult {
    let database = database("interpret_surface_conflict_rollback").await?;
    write_output(
        &database,
        &BatchOutput {
            name_surfaces: vec![surface("ens:conflict", "stored.eth")],
            ..BatchOutput::default()
        },
    )
    .await?;
    let output = BatchOutput {
        name_surfaces: vec![
            surface("ens:prefix", "prefix.eth"),
            surface("ens:conflict", "different.eth"),
            surface("ens:suffix", "suffix.eth"),
        ],
        ..BatchOutput::default()
    };

    let mut transaction = database.pool().begin().await?;
    let error = write(&mut transaction, &output)
        .await
        .expect_err("the divergent stored surface must fail");
    transaction.rollback().await?;
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("logical name IDs are already bound")
    );
    assert!(error.to_string().contains("1=ens:conflict"));
    let ids = sqlx::query_scalar::<_, String>(
        "SELECT logical_name_id FROM name_surfaces ORDER BY logical_name_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(ids, vec!["ens:conflict"]);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn canonical_reobservation_refreshes_stale_orphaned_normalization_state() -> TestResult {
    let database = database("interpret_surface_orphan_replacement").await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('batch-test', '0x00', 1, to_timestamp(1), 'orphaned')",
    )
    .execute(database.pool())
    .await?;
    let mut orphaned = surface("ens:reobserved", "same.eth");
    orphaned.block_hash = "0x00".to_owned();
    orphaned.normalizer_version = "old".to_owned();
    orphaned.canonicality_state = "orphaned".to_owned();
    write_output(
        &database,
        &BatchOutput {
            name_surfaces: vec![orphaned],
            ..BatchOutput::default()
        },
    )
    .await?;

    write_output(
        &database,
        &BatchOutput {
            name_surfaces: vec![surface("ens:reobserved", "same.eth")],
            ..BatchOutput::default()
        },
    )
    .await?;
    let row: (String, String, String, String) = sqlx::query_as(
        "SELECT raw_name, normalizer_version, visibility_state, canonicality_state::text
         FROM name_surfaces WHERE logical_name_id = 'ens:reobserved'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        row,
        (
            "same.eth".into(),
            "test".into(),
            "active".into(),
            "canonical".into()
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn tied_preimage_priority_preserves_last_occurrence() -> TestResult {
    let database = database("interpret_preimage_occurrence_order").await?;
    write_output(
        &database,
        &BatchOutput {
            label_preimages: vec![
                observed_preimage("tie", b"same", 5, json!({"occurrence": "first"})),
                observed_preimage("tie", b"same", 3, json!({"occurrence": "middle"})),
                observed_preimage("tie", b"same", 5, json!({"occurrence": "last"})),
            ],
            ..BatchOutput::default()
        },
    )
    .await?;

    let stored: (i32, serde_json::Value) =
        sqlx::query_as("SELECT source_priority, provenance FROM label_preimages")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored, (5, json!({"occurrence": "last"})));
    database.cleanup().await?;
    Ok(())
}
