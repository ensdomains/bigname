use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::{
    RequiredWatchedTuple, UncoveredWatchedTuple,
    find_uncovered_required_watched_tuples_for_retention_generation_in_transaction,
};
use bigname_storage::{
    BackfillTopicCoverageRequirement, BackfillTopicCoverageViolation,
    MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS, find_backfill_topic_coverage_violations,
    materialize_completed_backfill_topic_evidence,
};

/// Retention recovery uses the same compact topic-evidence proof, restricted
/// to jobs captured in the current raw-log generation.
pub(crate) async fn find_uncovered_generation_bound_coverage_with_current_topics(
    pool: &sqlx::PgPool,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    required_tuples: &[RequiredWatchedTuple],
    retention_generation: i64,
    uncovered_limit: i64,
) -> std::result::Result<Vec<UncoveredWatchedTuple>, String> {
    if required_tuples.is_empty() {
        return Ok(Vec::new());
    }
    let from_block = required_tuples
        .iter()
        .map(|tuple| tuple.required_from_block)
        .min()
        .expect("non-empty requirements must have a lower bound");
    let to_block = required_tuples
        .iter()
        .map(|tuple| tuple.required_to_block)
        .max()
        .expect("non-empty requirements must have an upper bound");
    let mut transaction = pool.begin().await.map_err(|error| error.to_string())?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await
        .map_err(|error| error.to_string())?;
    materialize_topic_evidence_in_transaction(
        transaction.as_mut(),
        chain,
        current_topic0s_by_family,
        from_block,
        to_block,
        Some(retention_generation),
    )
    .await?;
    for page in required_tuples.chunks(MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS) {
        ensure_required_topic_sets_undrifted_in_transaction(transaction.as_mut(), chain, page)
            .await?;
    }
    let uncovered = find_uncovered_required_watched_tuples_for_retention_generation_in_transaction(
        transaction.as_mut(),
        chain,
        required_tuples,
        retention_generation,
        uncovered_limit,
    )
    .await
    .map_err(|error| error.to_string())?;
    transaction
        .commit()
        .await
        .map_err(|error| error.to_string())?;
    Ok(uncovered)
}

pub(super) async fn materialize_topic_evidence_in_transaction(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    from_block: i64,
    to_block: i64,
    retention_generation: Option<i64>,
) -> std::result::Result<(), String> {
    let topics = current_topic0s_by_family
        .iter()
        .map(|(family, topics)| (family.clone(), topics.iter().cloned().collect()))
        .collect();
    materialize_completed_backfill_topic_evidence(
        connection,
        chain,
        from_block,
        to_block,
        &topics,
        retention_generation,
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

pub(super) async fn ensure_required_topic_sets_undrifted_in_transaction(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    required_tuples: &[RequiredWatchedTuple],
) -> std::result::Result<(), String> {
    let requirements = required_tuples
        .iter()
        .map(|tuple| BackfillTopicCoverageRequirement {
            source_family: tuple.source_family.clone(),
            address: tuple.address.clone(),
            required_from_block: tuple.required_from_block,
            required_to_block: tuple.required_to_block,
        })
        .collect::<Vec<_>>();
    let violation = find_backfill_topic_coverage_violations(connection, chain, &requirements, 1)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .next();
    match violation {
        Some(violation) => Err(violation_reason(&violation)),
        None => Ok(()),
    }
}

fn violation_reason(violation: &BackfillTopicCoverageViolation) -> String {
    if let Some(persisted_topic_count) = violation.persisted_topic_count {
        return format!(
            "source family {} manifest ABI topic0 set changed after completed backfill job {} was fetched (persisted {} topic0s, current {}); its relied-upon coverage facts may overclaim relative to the current ABI — re-run the affected range on the current manifest before promoting",
            violation.source_family,
            violation.backfill_job_id,
            persisted_topic_count,
            violation.current_topic_count
        );
    }
    format!(
        "source family {} was fetched by topic-filtered scan in completed backfill job {} without a persisted topic set; drift in its relied-upon coverage facts relative to the current manifest ABI cannot be ruled out — re-run the affected range on the current manifest before promoting",
        violation.source_family, violation.backfill_job_id
    )
}

#[cfg(test)]
mod tests {
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
        let uncovered =
            find_uncovered_required_watched_tuples_for_retention_generation_in_transaction(
                proof.as_mut(),
                chain,
                &requirements,
                0,
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
}
