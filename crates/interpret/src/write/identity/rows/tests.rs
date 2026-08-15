use bigname_adapters::schema_v2::{BatchOutput, Resource, TokenLineage};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for sql in [
        include_str!("../../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../../schema-v2/baseline/03_identity.sql"),
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

fn output(rows: u128) -> BatchOutput {
    let token_lineages = (0..rows)
        .map(|index| TokenLineage {
            token_lineage_id: Uuid::from_u128(index + 1),
            chain_id: "batch-test".to_owned(),
            block_hash: "0x01".to_owned(),
            block_number: 1,
            provenance: json!({"row": index}),
            canonicality_state: "canonical".to_owned(),
        })
        .collect::<Vec<_>>();
    let resources = (0..rows)
        .map(|index| Resource {
            resource_id: Uuid::from_u128(10_000 + index),
            token_lineage_id: Some(Uuid::from_u128(index + 1)),
            chain_id: "batch-test".to_owned(),
            block_hash: "0x01".to_owned(),
            block_number: 1,
            provenance: json!({"row": index}),
            canonicality_state: "canonical".to_owned(),
        })
        .collect::<Vec<_>>();
    BatchOutput {
        token_lineages,
        resources,
        ..BatchOutput::default()
    }
}

#[tokio::test]
async fn values_boundary_duplicate_keys_and_replay_preserve_identity_rows() -> TestResult {
    let database = database("interpret_identity_rows_values_boundary").await?;
    let mut submitted = output(501);
    submitted
        .token_lineages
        .push(submitted.token_lineages[0].clone());
    submitted.resources.push(submitted.resources[0].clone());
    let mut transaction = database.pool().begin().await?;
    write(&mut transaction, &submitted).await?;
    transaction.commit().await?;
    let mut replay = database.pool().begin().await?;
    write(&mut replay, &submitted).await?;
    replay.commit().await?;

    let lineage_count: i64 = sqlx::query_scalar("SELECT count(*) FROM token_lineages")
        .fetch_one(database.pool())
        .await?;
    let resource_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resources")
        .fetch_one(database.pool())
        .await?;
    assert_eq!((lineage_count, resource_count), (501, 501));
    let last: (Uuid, Option<Uuid>, serde_json::Value) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id, provenance
         FROM resources ORDER BY resource_id DESC LIMIT 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        last,
        (
            Uuid::from_u128(10_500),
            Some(Uuid::from_u128(501)),
            json!({"row": 500}),
        )
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn conflicting_lineage_identifies_row_and_rolls_back_prefix() -> TestResult {
    let database = database("interpret_identity_rows_conflict_rollback").await?;
    let conflicting_id = Uuid::from_u128(2);
    sqlx::query(
        r#"INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number,
             provenance, canonicality_state
         ) VALUES ($1, 'batch-test', '0x01', 1, '{"existing":true}', 'canonical')"#,
    )
    .bind(conflicting_id)
    .execute(database.pool())
    .await?;

    let submitted = output(3);
    let mut transaction = database.pool().begin().await?;
    let error = write(&mut transaction, &submitted)
        .await
        .expect_err("the divergent stored lineage must fail")
        .to_string();
    transaction.rollback().await?;

    assert!(
        error.contains("token lineages are already bound"),
        "{error}"
    );
    assert!(error.contains(&format!("1={conflicting_id}")), "{error}");
    let lineage_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT token_lineage_id FROM token_lineages ORDER BY token_lineage_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(lineage_ids, vec![conflicting_id]);

    database.cleanup().await?;
    Ok(())
}
