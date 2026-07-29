use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use bigname_manifests::find_uncovered_required_watched_tuples_for_retention_generation_in_transaction;
use bigname_manifests::{
    RequiredWatchedTuple, UncoveredWatchedTuple, load_active_manifest_abi_events_by_chain,
    load_discovery_admission_epoch,
};
use bigname_storage::{
    BackfillTopicCoverageRequirement, BackfillTopicCoverageViolation,
    find_backfill_topic_coverage_violations, find_uncovered_full_closure_coverage,
    load_raw_log_staging_input_version, materialize_completed_backfill_topic_evidence,
};
use tracing::debug;

/// Retention recovery uses the same compact topic-evidence proof, restricted
/// to jobs captured in the current raw-log generation.
pub(crate) async fn find_uncovered_generation_bound_coverage_with_current_topics(
    pool: &sqlx::PgPool,
    chain: &str,
    _caller_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    required_tuples: &[RequiredWatchedTuple],
    retention_generation: i64,
    uncovered_limit: i64,
) -> std::result::Result<Vec<UncoveredWatchedTuple>, String> {
    let _scan_timer = crate::metrics::coverage_violation_scan_timer(chain);
    let required_tuples = nonempty_required_tuples(chain, required_tuples)
        .cloned()
        .collect::<Vec<_>>();
    if required_tuples.is_empty() {
        return Ok(Vec::new());
    }
    let uncovered_limit = usize::try_from(uncovered_limit)
        .map_err(|_| "generation-bound uncovered limit must be positive".to_owned())?;
    if uncovered_limit == 0 {
        return Err("generation-bound uncovered limit must be positive".to_owned());
    }
    let admission_epoch = load_discovery_admission_epoch(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    // The aggregate state is chain-wide, so its ABI authority must not vary
    // with a caller's proof-family subset. Admission-epoch fences below make
    // a concurrent active-manifest change fail closed.
    let topics = load_chain_wide_topic0s_by_family(pool, chain).await?;
    #[cfg(test)]
    let topics = if topics.is_empty() {
        // Unit tests without manifest rows can inject the authority map. The
        // cache-reuse regression test below exercises the production loader.
        _caller_topic0s_by_family
            .iter()
            .map(|(family, topics)| (family.clone(), topics.iter().cloned().collect()))
            .collect()
    } else {
        topics
    };
    let requirements = required_tuples
        .iter()
        .map(|tuple| BackfillTopicCoverageRequirement {
            source_family: tuple.source_family.clone(),
            address: tuple.address.clone(),
            required_from_block: tuple.required_from_block,
            required_to_block: tuple.required_to_block,
        })
        .collect::<Vec<_>>();
    let outcome = find_uncovered_full_closure_coverage(
        pool,
        chain,
        &topics,
        &requirements,
        retention_generation,
        admission_epoch,
        i64::try_from(uncovered_limit)
            .map_err(|_| "uncovered limit exceeds signed 64-bit".to_owned())?,
    )
    .await
    .map_err(|error| error.to_string())?;
    debug!(
        service = "indexer",
        command = "run",
        chain,
        full_rebuild = outcome.synchronization.full_rebuild,
        appended_fact_count = outcome.synchronization.appended_fact_count,
        rebuilt_key_count = outcome.synchronization.rebuilt_key_count,
        coverage_input_revision = outcome.synchronization.coverage_input_revision,
        raw_log_input_revision = outcome.synchronization.raw_log_input_revision,
        "synchronized fact-derived full-closure coverage"
    );
    let input_after = load_raw_log_staging_input_version(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    if input_after.retention_generation != retention_generation
        || input_after.revision != outcome.synchronization.raw_log_input_revision
    {
        return Err(format!(
            "raw-log staging input changed while proving generation-bound closure for chain {chain}: expected generation {retention_generation} revision {}, observed generation {} revision {}",
            outcome.synchronization.raw_log_input_revision,
            input_after.retention_generation,
            input_after.revision
        ));
    }
    let admission_epoch_after = load_discovery_admission_epoch(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    if admission_epoch_after != admission_epoch {
        return Err(format!(
            "discovery admission epoch changed while proving generation-bound closure for chain {chain}: expected {admission_epoch}, observed {admission_epoch_after}"
        ));
    }
    Ok(outcome
        .violations
        .into_iter()
        .map(|violation| UncoveredWatchedTuple {
            source_family: violation.source_family,
            address: violation.address,
            required_from_block: violation.required_from_block,
            required_to_block: violation.required_to_block,
        })
        .collect())
}

async fn load_chain_wide_topic0s_by_family(
    pool: &sqlx::PgPool,
    chain: &str,
) -> std::result::Result<BTreeMap<String, Vec<String>>, String> {
    let events = load_active_manifest_abi_events_by_chain(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    let mut topic_sets = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        if let Some(topic0) = event.topic0 {
            topic_sets
                .entry(event.source_family)
                .or_default()
                .insert(topic0.to_ascii_lowercase());
        }
    }
    Ok(topic_sets
        .into_iter()
        .map(|(family, topics)| (family, topics.into_iter().collect()))
        .collect())
}

fn nonempty_required_tuples<'a>(
    chain: &'a str,
    required_tuples: &'a [RequiredWatchedTuple],
) -> impl Iterator<Item = &'a RequiredWatchedTuple> + 'a {
    required_tuples.iter().filter(move |tuple| {
        let nonempty = tuple.required_from_block <= tuple.required_to_block;
        if !nonempty {
            debug!(
                service = "indexer",
                command = "run",
                chain,
                source_family = %tuple.source_family,
                address = %tuple.address,
                required_from_block = tuple.required_from_block,
                required_to_block = tuple.required_to_block,
                "skipping watched tuple with an empty bounded catch-up window"
            );
        }
        nonempty
    })
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
    let requirements = nonempty_required_tuples(chain, required_tuples)
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
#[path = "topic_drift/cache_tests.rs"]
mod cache_tests;

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
