use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

#[tokio::test]
async fn generation_bound_proof_pages_more_than_256_requirements() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("topic_evidence_generation_bound_paging"),
        &bigname_storage::MIGRATOR,
        "failed to migrate generation-bound topic paging test",
    )
    .await?;
    let requirements = (0..600)
        .map(|index| RequiredWatchedTuple {
            source_family: "test-family".to_owned(),
            address: format!("0x{index:040x}"),
            required_from_block: 1,
            required_to_block: 10,
        })
        .collect::<Vec<_>>();
    let uncovered = find_uncovered_generation_bound_coverage_with_current_topics(
        database.pool(),
        "test-chain",
        &BTreeMap::new(),
        &requirements,
        0,
        20,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    assert_eq!(uncovered.len(), 20);
    database.cleanup().await
}

#[tokio::test]
async fn generation_bound_proof_skips_empty_requirement_windows() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("topic_evidence_skips_empty_requirement_windows"),
        &bigname_storage::MIGRATOR,
        "failed to migrate empty topic requirement test",
    )
    .await?;
    let valid = RequiredWatchedTuple {
        source_family: "test-family".to_owned(),
        address: "0x0000000000000000000000000000000000000001".to_owned(),
        required_from_block: 1,
        required_to_block: 10,
    };
    let inverted = RequiredWatchedTuple {
        source_family: "test-family".to_owned(),
        address: "0x0000000000000000000000000000000000000002".to_owned(),
        required_from_block: 11,
        required_to_block: 10,
    };
    let uncovered = find_uncovered_generation_bound_coverage_with_current_topics(
        database.pool(),
        "test-chain",
        &BTreeMap::new(),
        &[inverted, valid.clone()],
        0,
        20,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    assert_eq!(
        uncovered,
        vec![UncoveredWatchedTuple {
            source_family: valid.source_family,
            address: valid.address,
            required_from_block: valid.required_from_block,
            required_to_block: valid.required_to_block,
        }]
    );
    database.cleanup().await
}

#[tokio::test]
async fn generation_bound_proof_returns_stale_topic_coverage_as_uncovered() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("topic_evidence_generation_bound_stale"),
        &bigname_storage::MIGRATOR,
        "failed to migrate stale generation-bound topic test",
    )
    .await?;
    let chain = "test-chain";
    let family = "test-family";
    let address = "0x0000000000000000000000000000000000000001";
    let old_topic = format!("0x{:064x}", 1);
    let current_topic = format!("0x{:064x}", 2);
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
        VALUES (
            'test', $1,
            jsonb_build_object(
                'topic0s_by_source_family',
                jsonb_build_object($2, jsonb_build_array($3::TEXT))
            ),
            'test', 1, 10, 'stale-topic-generation-bound',
            'completed'::backfill_lifecycle_status, now()
        )
        RETURNING backfill_job_id
        "#,
    )
    .bind(chain)
    .bind(family)
    .bind(&old_topic)
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
        VALUES ($1, $2, $3, 'address', $4, 1, 10, 'job_completion')
        "#,
    )
    .bind(job_id)
    .bind(chain)
    .bind(family)
    .bind(address)
    .execute(database.pool())
    .await?;
    let requirement = RequiredWatchedTuple {
        source_family: family.to_owned(),
        address: address.to_owned(),
        required_from_block: 1,
        required_to_block: 10,
    };
    let current_topics = BTreeMap::from([(family.to_owned(), BTreeSet::from([current_topic]))]);

    let uncovered = find_uncovered_generation_bound_coverage_with_current_topics(
        database.pool(),
        chain,
        &current_topics,
        std::slice::from_ref(&requirement),
        0,
        20,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    assert_eq!(
        uncovered,
        vec![UncoveredWatchedTuple {
            source_family: requirement.source_family,
            address: requirement.address,
            required_from_block: requirement.required_from_block,
            required_to_block: requirement.required_to_block,
        }]
    );
    database.cleanup().await
}

#[tokio::test]
async fn repeatable_read_excludes_fact_completed_after_topic_materialization() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("topic_evidence_repeatable_read_completion_race"),
        &bigname_storage::MIGRATOR,
        "failed to migrate topic completion race test",
    )
    .await?;
    let chain = "test-chain";
    let family = "test-family";
    let address = "0x0000000000000000000000000000000000000001";
    let topic = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let requirements = vec![RequiredWatchedTuple {
        source_family: family.to_owned(),
        address: address.to_owned(),
        required_from_block: 1,
        required_to_block: 10,
    }];
    let topics = BTreeMap::from([(family.to_owned(), BTreeSet::from([topic.to_owned()]))]);
    let mut proof = database.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(proof.as_mut())
        .await?;
    materialize_topic_evidence_in_transaction(proof.as_mut(), chain, &topics, 1, 10, Some(0))
        .await
        .map_err(anyhow::Error::msg)?;

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
        VALUES (
            'test', $1,
            jsonb_build_object(
                'coinbase_sql_topic_plan',
                jsonb_build_object(
                    'topic0s_by_source_family',
                    jsonb_build_object($2, jsonb_build_array($3::TEXT))
                )
            ),
            'test', 1, 10, 'completion-race',
            'completed'::backfill_lifecycle_status, now()
        )
        RETURNING backfill_job_id
        "#,
    )
    .bind(chain)
    .bind(family)
    .bind(topic)
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
        VALUES ($1, $2, $3, 'address', $4, 1, 10, 'job_completion')
        "#,
    )
    .bind(job_id)
    .bind(chain)
    .bind(family)
    .bind(address)
    .execute(database.pool())
    .await?;

    ensure_required_topic_sets_undrifted_in_transaction(proof.as_mut(), chain, &requirements)
        .await
        .map_err(anyhow::Error::msg)?;
    let uncovered = find_uncovered_required_watched_tuples_in_transaction(
        proof.as_mut(),
        chain,
        &requirements,
        20,
    )
    .await?;
    assert_eq!(
        uncovered.len(),
        1,
        "the ordinary coverage read must share the pre-completion repeatable-read snapshot"
    );
    proof.rollback().await?;
    database.cleanup().await
}
