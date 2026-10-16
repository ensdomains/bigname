use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const ADDRESS: &str = "0x00000000000000000000000000000000000000ab";

async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for sql in [
        include_str!("../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../storage/schema/baseline/03_identity.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    for id in [1, 2] {
        sqlx::query("INSERT INTO contract_instances(contract_instance_id,chain_id,contract_kind) VALUES($1,'cache-test','contract')")
            .bind(Uuid::from_u128(id)).execute(database.pool()).await?;
    }
    Ok(database)
}

fn observation(block: i64, step: i64) -> ContractAddress {
    ContractAddress {
        contract_instance_id: Uuid::from_u128(1),
        chain_id: "cache-test".into(),
        address: ADDRESS.into(),
        active_from_block_number: block,
        active_from_block_hash: format!("block-{block}-step-{step}"),
        source_manifest_id: step,
        provenance: json!({"source":"raw_log","step":step}),
    }
}

async fn seed_active(database: &TestDatabase, start: Option<i64>, provenance: Value) -> TestResult {
    sqlx::query("INSERT INTO contract_instance_addresses(contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id,provenance) VALUES($1,'cache-test',$2,$3,CASE WHEN $3::bigint IS NULL THEN NULL ELSE 'original-hash' END,99,$4)")
        .bind(Uuid::from_u128(1)).bind(ADDRESS).bind(start).bind(provenance)
        .execute(database.pool()).await?;
    Ok(())
}

async fn active(
    database: &TestDatabase,
) -> TestResult<(Option<i64>, Option<String>, Option<i64>, Value)> {
    Ok(sqlx::query_as("SELECT active_from_block_number,active_from_block_hash,source_manifest_id,provenance FROM contract_instance_addresses WHERE contract_instance_id=$1 AND deactivated_at IS NULL")
        .bind(Uuid::from_u128(1)).fetch_one(database.pool()).await?)
}

#[tokio::test]
async fn repeated_case_variants_keep_first_minimum_hash_and_last_provenance() -> TestResult {
    let db = database("interpret_address_cache_case_order").await?;
    seed_active(&db, Some(20), json!({"source":"raw_log"})).await?;
    let mut second = observation(10, 2);
    second.address = ADDRESS.to_ascii_uppercase();
    let mut fourth = observation(10, 4);
    fourth.address = ADDRESS.to_ascii_uppercase();
    let mut tx = db.pool().begin().await?;
    write(
        &mut tx,
        &[observation(15, 1), second, observation(25, 3), fourth],
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        active(&db).await?,
        (
            Some(10),
            Some("block-10-step-2".into()),
            Some(4),
            json!({"source":"raw_log","step":4})
        )
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn manifest_noops_do_not_rewrite_rows_or_hide_invalid_json() -> TestResult {
    let db = database("interpret_address_cache_manifest_noop").await?;
    seed_active(
        &db,
        None,
        json!({"source":"manifest_declaration","kept":true}),
    )
    .await?;
    let before: String = sqlx::query_scalar("SELECT ctid::text FROM contract_instance_addresses")
        .fetch_one(db.pool())
        .await?;
    let observations: Vec<_> = (0..1000).map(|step| observation(20 + step, step)).collect();
    let mut tx = db.pool().begin().await?;
    write(&mut tx, &observations).await?;
    tx.commit().await?;
    let after: String = sqlx::query_scalar("SELECT ctid::text FROM contract_instance_addresses")
        .fetch_one(db.pool())
        .await?;
    assert_eq!(
        before, after,
        "unchanged manifest rows should not incur UPDATEs"
    );
    assert_eq!(
        active(&db).await?,
        (
            None,
            None,
            Some(99),
            json!({"source":"manifest_declaration","kept":true})
        )
    );

    for provenance in [
        json!({"bad":"nul\0value"}),
        json!({"nested":{"bad\0key":true}}),
    ] {
        let mut invalid = observation(30, 2);
        invalid.provenance = provenance;
        let mut tx = db.pool().begin().await?;
        let error = write(&mut tx, &[observation(20, 1), invalid])
            .await
            .expect_err("PostgreSQL rejects the provenance as jsonb input");
        assert_eq!(error.kind(), crate::ErrorKind::Transient);
        tx.rollback().await?;
    }
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn inserted_rows_keep_original_cross_key_conflict_behavior() -> TestResult {
    let db = database("interpret_address_cache_insert_conflicts").await?;
    let mut other_instance = observation(30, 2);
    other_instance.contract_instance_id = Uuid::from_u128(2);
    let mut tx = db.pool().begin().await?;
    write(
        &mut tx,
        &[
            observation(20, 1),
            other_instance.clone(),
            observation(10, 3),
            other_instance,
        ],
    )
    .await?;
    tx.commit().await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM contract_instance_addresses")
        .fetch_one(db.pool())
        .await?;
    assert_eq!(
        count, 1,
        "the first instance retains an already claimed address"
    );
    assert_eq!(
        active(&db).await?,
        (
            Some(10),
            Some("block-10-step-3".into()),
            Some(3),
            json!({"source":"raw_log","step":3})
        )
    );
    let mut competing_address = observation(40, 4);
    competing_address.address = "0x00000000000000000000000000000000000000cd".into();
    let mut tx = db.pool().begin().await?;
    assert!(
        write(&mut tx, &[observation(10, 3), competing_address])
            .await
            .is_err(),
        "one active instance at two addresses must still violate the SQL constraint"
    );
    tx.rollback().await?;
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn repeated_observations_respect_retirement_and_manifest_provenance() -> TestResult {
    let db = database("interpret_address_cache_retirement").await?;
    sqlx::query("INSERT INTO contract_instance_addresses(contract_instance_id,chain_id,address,active_from_block_number,active_to_block_number,provenance,admitted_at,deactivated_at) VALUES($1,'cache-test',$2,2,9,'{\"source\":\"manifest_declaration\"}',now()-interval '2 hours',now()-interval '1 hour')")
        .bind(Uuid::from_u128(1)).bind(ADDRESS).execute(db.pool()).await?;
    seed_active(&db, Some(20), json!({"source":"raw_log","step":0})).await?;
    let retired: Value = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM contract_instance_addresses t WHERE deactivated_at IS NOT NULL",
    )
    .fetch_one(db.pool())
    .await?;
    let mut frozen = observation(15, 2);
    frozen.provenance = json!({"source":"manifest_proxy_implementation","step":2});
    let mut tx = db.pool().begin().await?;
    write(
        &mut tx,
        &[
            observation(5, 1),
            frozen,
            observation(7, 3),
            observation(11, 4),
        ],
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        active(&db).await?,
        (
            Some(11),
            Some("block-11-step-4".into()),
            Some(2),
            json!({"source":"manifest_proxy_implementation","step":2})
        )
    );
    let after: Value = sqlx::query_scalar(
        "SELECT to_jsonb(t) FROM contract_instance_addresses t WHERE deactivated_at IS NOT NULL",
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(retired, after);
    db.cleanup().await?;
    Ok(())
}

#[test]
fn skipped_bindings_are_limited_to_simple_exact_json_inputs() {
    assert!(flat_jsonb_input(
        &json!({"source":"raw_log","n":u64::MAX,"nil":null,"bool":true})
    ));
    for value in [
        json!({"value":1.5}),
        json!({"nested":{}}),
        json!({"array":[]}),
        json!({"bad\0key":1}),
        json!({"value":"bad\0value"}),
        json!(null),
    ] {
        assert!(!flat_jsonb_input(&value));
    }
    assert!(bound(10, Some(i64::MAX)).is_err());
}
