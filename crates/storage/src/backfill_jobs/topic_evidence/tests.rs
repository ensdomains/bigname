use std::collections::BTreeMap;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

use super::*;

const CHAIN: &str = "test-chain";
const FAMILY: &str = "test-family";
const ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const CURRENT_TOPIC: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

async fn database(name: &str) -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new(name),
        &crate::MIGRATOR,
        "failed to migrate topic-evidence test database",
    )
    .await
}

async fn insert_job_with_fact(
    database: &TestDatabase,
    identity: Value,
    job_from: i64,
    job_to: i64,
    fact_from: i64,
    fact_to: i64,
) -> Result<i64> {
    let job_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status,
            completed_at
        )
        VALUES ('test', $1, $2, 'test', $3, $4,
                md5(random()::TEXT || clock_timestamp()::TEXT),
                'completed'::backfill_lifecycle_status, now())
        RETURNING backfill_job_id
        "#,
    )
    .bind(CHAIN)
    .bind(identity)
    .bind(job_from)
    .bind(job_to)
    .fetch_one(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO backfill_coverage_facts (
            backfill_job_id,
            chain_id,
            source_family,
            scope,
            address,
            covered_from_block,
            covered_to_block,
            derivation
        )
        VALUES ($1, $2, $3, 'address', $4, $5, $6, 'job_completion')
        "#,
    )
    .bind(job_id)
    .bind(CHAIN)
    .bind(FAMILY)
    .bind(ADDRESS)
    .bind(fact_from)
    .bind(fact_to)
    .execute(database.pool())
    .await?;
    Ok(job_id)
}

fn requirement() -> BackfillTopicCoverageRequirement {
    BackfillTopicCoverageRequirement {
        source_family: FAMILY.to_owned(),
        address: ADDRESS.to_owned(),
        required_from_block: 1,
        required_to_block: 10,
    }
}

fn inverted_requirement() -> BackfillTopicCoverageRequirement {
    BackfillTopicCoverageRequirement {
        source_family: FAMILY.to_owned(),
        address: ADDRESS.to_owned(),
        required_from_block: 12,
        required_to_block: 10,
    }
}

fn current_topics() -> BTreeMap<String, Vec<String>> {
    BTreeMap::from([(FAMILY.to_owned(), vec![CURRENT_TOPIC.to_owned()])])
}

async fn violations(database: &TestDatabase) -> Result<Vec<BackfillTopicCoverageViolation>> {
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await?;
    materialize_completed_backfill_topic_evidence(
        transaction.as_mut(),
        CHAIN,
        1,
        10,
        &current_topics(),
        None,
    )
    .await?;
    let violations =
        find_backfill_topic_coverage_violations(transaction.as_mut(), CHAIN, &[requirement()], 1)
            .await?;
    transaction.rollback().await?;
    Ok(violations)
}

#[tokio::test]
async fn inverted_requirement_is_skipped_without_masking_valid_violation() -> Result<()> {
    let database = database("topic_evidence_mixed_inverted_requirement").await?;
    let stale_job = insert_job_with_fact(
        &database,
        json!({
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {FAMILY: []}
            }
        }),
        1,
        20,
        1,
        20,
    )
    .await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await?;
    materialize_completed_backfill_topic_evidence(
        transaction.as_mut(),
        CHAIN,
        1,
        10,
        &current_topics(),
        None,
    )
    .await?;
    let requirements = vec![inverted_requirement(), requirement()];
    let found =
        find_backfill_topic_coverage_violations(transaction.as_mut(), CHAIN, &requirements, 2)
            .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].backfill_job_id, stale_job);
    assert_eq!(found[0].required_from_block, 1);
    assert_eq!(found[0].required_to_block, 10);
    transaction.rollback().await?;
    database.cleanup().await
}

#[tokio::test]
async fn all_inverted_requirements_are_satisfied_without_querying() -> Result<()> {
    let database = database("topic_evidence_all_inverted_requirements").await?;
    let mut connection = database.pool().acquire().await?;
    assert!(
        find_backfill_topic_coverage_violations(
            connection.as_mut(),
            CHAIN,
            &[inverted_requirement()],
            1,
        )
        .await?
        .is_empty()
    );
    assert_eq!(
        materialize_completed_backfill_topic_evidence(
            connection.as_mut(),
            CHAIN,
            12,
            10,
            &current_topics(),
            None,
        )
        .await?,
        0
    );
    drop(connection);
    database.cleanup().await
}

#[tokio::test]
async fn relied_on_topic_filtered_family_missing_from_map_is_stale() -> Result<()> {
    let database = database("topic_evidence_missing_family_key").await?;
    let job_id = insert_job_with_fact(
        &database,
        json!({
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {"another-family": [CURRENT_TOPIC]},
                "source_families_without_topics": []
            }
        }),
        1,
        10,
        1,
        10,
    )
    .await?;
    let found = violations(&database).await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].backfill_job_id, job_id);
    assert_eq!(found[0].persisted_topic_count, None);
    database.cleanup().await
}

#[tokio::test]
async fn out_of_parent_range_current_fact_cannot_replace_valid_stale_fact() -> Result<()> {
    let database = database("topic_evidence_out_of_parent_range").await?;
    let stale_job = insert_job_with_fact(
        &database,
        json!({
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {FAMILY: []}
            }
        }),
        1,
        10,
        1,
        10,
    )
    .await?;
    insert_job_with_fact(
        &database,
        json!({
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {FAMILY: [CURRENT_TOPIC]}
            }
        }),
        1,
        5,
        1,
        10,
    )
    .await?;
    let found = violations(&database).await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].backfill_job_id, stale_job);
    database.cleanup().await
}

#[tokio::test]
async fn high_job_history_is_materialized_once_and_returns_bounded_rows() -> Result<()> {
    let database = database("topic_evidence_high_job_history").await?;
    sqlx::query(
        r#"
        WITH inserted_jobs AS (
            INSERT INTO backfill_jobs (
                deployment_profile,
                chain_id,
                source_identity,
                scan_mode,
                range_start_block_number,
                range_end_block_number,
                idempotency_key,
                status,
                completed_at
            )
            SELECT
                'test',
                $1,
                jsonb_build_object(
                    'coinbase_sql_topic_plan',
                    jsonb_build_object(
                        'topic0s_by_source_family',
                        jsonb_build_object($2, '[]'::JSONB)
                    ),
                    'selected_targets',
                    jsonb_build_array(repeat('x', 8192))
                ),
                'test',
                1,
                10,
                'history-' || history,
                'completed'::backfill_lifecycle_status,
                now()
            FROM generate_series(1, 600) AS history
            RETURNING backfill_job_id
        )
        INSERT INTO backfill_coverage_facts (
            backfill_job_id,
            chain_id,
            source_family,
            scope,
            address,
            covered_from_block,
            covered_to_block,
            derivation
        )
        SELECT backfill_job_id, $1, $2, 'address', $3, 1, 10, 'job_completion'
        FROM inserted_jobs
        "#,
    )
    .bind(CHAIN)
    .bind(FAMILY)
    .bind(ADDRESS)
    .execute(database.pool())
    .await?;

    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await?;
    assert_eq!(
        materialize_completed_backfill_topic_evidence(
            transaction.as_mut(),
            CHAIN,
            1,
            10,
            &current_topics(),
            None,
        )
        .await?,
        600
    );
    // Mutating the large job payloads after materialization cannot affect page
    // reads: those reads use only the compact transaction-local status table.
    sqlx::query("UPDATE backfill_jobs SET source_identity = '{}'::JSONB WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(transaction.as_mut())
        .await?;
    let first =
        find_backfill_topic_coverage_violations(transaction.as_mut(), CHAIN, &[requirement()], 1)
            .await?;
    let second =
        find_backfill_topic_coverage_violations(transaction.as_mut(), CHAIN, &[requirement()], 1)
            .await?;
    assert_eq!(first.len(), 1);
    assert_eq!(second, first);
    transaction.rollback().await?;
    database.cleanup().await
}
