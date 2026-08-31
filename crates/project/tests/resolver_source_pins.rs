use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn classification_role_prefers_latest_declared_address_epoch() -> TestResult {
    let source = include_str!("../src/builders/resolver.rs")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let order_start = source
        .find("ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC")
        .expect("classification ordering");
    let order_end = source[order_start..]
        .find("LIMIT 1")
        .expect("classification limit");
    let production_order = &source[order_start..order_start + order_end];
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "project_resolver_classification_order",
    ))
    .await?;
    let query = format!(
        "SELECT declaration ->> 'role'
         FROM jsonb_array_elements($1::jsonb -> 'contracts') WITH ORDINALITY
              declarations(declaration, declaration_ordinality)
         WHERE lower(declaration ->> 'address') = $2
           AND (declaration ->> 'start_block' IS NULL
                OR (declaration ->> 'start_block')::bigint <= $3)
         {production_order}
         LIMIT 1"
    );
    let selected: String = sqlx::query_scalar(&query)
        .bind(json!({"contracts": [
            {"address": "0x01", "role": "old", "start_block": 10},
            {"address": "0x01", "role": "same_start_first", "start_block": 20},
            {"address": "0x01", "role": "same_start_last", "start_block": 20},
            {"address": "0x01", "role": "future", "start_block": 30}
        ]}))
        .bind("0x01")
        .bind(20_i64)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(selected, "same_start_last");
    database.cleanup().await?;
    Ok(())
}
