use bigname_adapters::schema_v2::{BatchOutput, ContractInstance};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

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
