use anyhow::{Context, Result};
use bigname_manifests::RequiredWatchedTuple;
use sqlx::Row;

use super::requirement_intervals_not_covered_by;
use super::requirement_intervals_not_covered_by_with_progress;
use crate::checkpoint_context::StartupAdapterProgress;
use crate::ens_v2_registry::EnsV2MissingCoverage;

pub(in crate::ens_v2_registry::live::completeness) async fn ensure_generation_bound_coverage(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
) -> Result<()> {
    ensure_requirements_have_generation_bound_coverage(
        connection,
        chain,
        requirements,
        retention_generation,
    )
    .await
}

pub(in crate::ens_v2_registry::live::completeness) async fn ensure_generation_bound_coverage_with_live_selection(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
    selected_addresses: &[String],
    selected_block_intervals: &[(i64, i64)],
) -> Result<()> {
    let selected_addresses = selected_addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let live_coverage = requirements
        .iter()
        .filter(|requirement| {
            selected_addresses.contains(&requirement.address.to_ascii_lowercase())
        })
        .flat_map(|requirement| {
            selected_block_intervals
                .iter()
                .filter_map(move |&(selected_from, selected_to)| {
                    let covered_from = requirement.required_from_block.max(selected_from);
                    let covered_to = requirement.required_to_block.min(selected_to);
                    (covered_from <= covered_to).then(|| RequiredWatchedTuple {
                        source_family: requirement.source_family.clone(),
                        address: requirement.address.clone(),
                        required_from_block: covered_from,
                        required_to_block: covered_to,
                    })
                })
        })
        .collect::<Vec<_>>();
    let remaining_requirements = requirement_intervals_not_covered_by(requirements, &live_coverage);
    ensure_newly_required_generation_bound_coverage(
        connection,
        chain,
        &remaining_requirements,
        retention_generation,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(in crate::ens_v2_registry::live::completeness) async fn ensure_generation_bound_coverage_with_live_selection_with_progress(
    pool: &sqlx::PgPool,
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
    selected_addresses: &[String],
    selected_block_intervals: &[(i64, i64)],
    progress: &mut dyn StartupAdapterProgress,
) -> Result<()> {
    let selected_addresses = selected_addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let mut live_coverage = Vec::new();
    let mut examined = 0usize;
    for requirement in requirements {
        if selected_addresses.contains(&requirement.address.to_ascii_lowercase()) {
            for &(selected_from, selected_to) in selected_block_intervals {
                let covered_from = requirement.required_from_block.max(selected_from);
                let covered_to = requirement.required_to_block.min(selected_to);
                if covered_from <= covered_to {
                    live_coverage.push(RequiredWatchedTuple {
                        source_family: requirement.source_family.clone(),
                        address: requirement.address.clone(),
                        required_from_block: covered_from,
                        required_to_block: covered_to,
                    });
                }
                examined += 1;
                if examined.is_multiple_of(super::super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
                    progress.record(pool).await?;
                }
            }
        } else {
            examined += 1;
            if examined.is_multiple_of(super::super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
                progress.record(pool).await?;
            }
        }
    }
    if examined > 0 && !examined.is_multiple_of(super::super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
        progress.record(pool).await?;
    }
    let remaining_requirements = requirement_intervals_not_covered_by_with_progress(
        pool,
        requirements,
        &live_coverage,
        progress,
    )
    .await?;
    for page in remaining_requirements.chunks(super::super::RETAINED_REQUIREMENT_PROGRESS_ROWS) {
        ensure_newly_required_generation_bound_coverage(
            connection,
            chain,
            page,
            retention_generation,
        )
        .await?;
        progress.record(pool).await?;
    }
    Ok(())
}

pub(in crate::ens_v2_registry::live::completeness) async fn ensure_newly_required_generation_bound_coverage(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
) -> Result<()> {
    ensure_requirements_have_generation_bound_coverage(
        connection,
        chain,
        requirements,
        retention_generation,
    )
    .await
}

async fn ensure_requirements_have_generation_bound_coverage(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
) -> Result<()> {
    if let Some(uncovered) = find_uncovered_generation_bound_requirement(
        connection,
        chain,
        requirements,
        retention_generation,
    )
    .await?
    {
        return Err(uncovered_coverage_error(
            chain,
            retention_generation,
            uncovered,
        ));
    }
    Ok(())
}

fn uncovered_coverage_error(
    chain: &str,
    retention_generation: i64,
    uncovered: RequiredWatchedTuple,
) -> anyhow::Error {
    EnsV2MissingCoverage {
        chain: chain.to_owned(),
        retention_generation,
        source_family: uncovered.source_family,
        address: uncovered.address,
        required_from_block: uncovered.required_from_block,
        required_to_block: uncovered.required_to_block,
    }
    .into()
}

async fn find_uncovered_generation_bound_requirement(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
) -> Result<Option<RequiredWatchedTuple>> {
    if requirements.is_empty() {
        return Ok(None);
    }
    let source_families = requirements
        .iter()
        .map(|requirement| requirement.source_family.clone())
        .collect::<Vec<_>>();
    let addresses = requirements
        .iter()
        .map(|requirement| requirement.address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let from_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_from_block)
        .collect::<Vec<_>>();
    let to_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_to_block)
        .collect::<Vec<_>>();
    let uncovered = sqlx::query(
        r#"
        WITH required_tuples AS (
            SELECT *
            FROM UNNEST(
                $2::TEXT[],
                $3::TEXT[],
                $4::BIGINT[],
                $5::BIGINT[]
            ) AS watched(
                source_family,
                address,
                required_from_block,
                required_to_block
            )
        )
        SELECT source_family, address, required_from_block, required_to_block
        FROM required_tuples watched
        WHERE NOT (
            COALESCE(
                (
                    SELECT range_agg(
                        int8range(
                            fact.covered_from_block,
                            fact.covered_to_block,
                            '[]'
                        )
                    )
                    FROM backfill_coverage_facts fact
                    JOIN backfill_jobs job
                      ON job.backfill_job_id = fact.backfill_job_id
                    WHERE fact.chain_id = $1
                      AND job.chain_id = fact.chain_id
                      AND job.status = 'completed'::backfill_lifecycle_status
                      AND job.raw_log_retention_generation = $6
                      AND (
                          job.stored_verification_raw_log_input_revision IS NULL
                          OR (
                              job.stored_verification_from_block
                                  <= fact.covered_from_block
                              AND job.stored_verification_to_block
                                  >= fact.covered_to_block
                              AND job.raw_log_retention_generation = (
                                  SELECT retained.retention_generation
                                  FROM raw_log_staging_input_revisions retained
                                  WHERE retained.chain_id = fact.chain_id
                              )
                              AND job.stored_verification_raw_log_input_revision <= (
                                  SELECT retained.revision
                                  FROM raw_log_staging_input_revisions retained
                                  WHERE retained.chain_id = fact.chain_id
                              )
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM raw_log_staging_block_revisions changed
                                  WHERE changed.chain_id = fact.chain_id
                                    AND changed.revision
                                        > job.stored_verification_raw_log_input_revision
                                    AND changed.block_number BETWEEN
                                        fact.covered_from_block AND fact.covered_to_block
                              )
                          )
                      )
                      AND fact.covered_from_block >= job.range_start_block_number
                      AND fact.covered_to_block <= job.range_end_block_number
                      AND fact.source_family = watched.source_family
                      AND (
                          (fact.scope = 'address' AND fact.address = watched.address)
                          OR (fact.scope = 'family' AND fact.address IS NULL)
                      )
                      AND fact.covered_from_block <= watched.required_to_block
                      AND fact.covered_to_block >= watched.required_from_block
                ),
                '{}'::INT8MULTIRANGE
            ) @> int8range(
                watched.required_from_block,
                watched.required_to_block,
                '[]'
            )
        )
        ORDER BY source_family, address, required_from_block
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(retention_generation)
    .fetch_optional(connection)
    .await
    .with_context(|| {
        format!("failed to verify generation {retention_generation} ENSv2 coverage for {chain}")
    })?;
    uncovered
        .map(|row| {
            Ok(RequiredWatchedTuple {
                source_family: row.try_get("source_family")?,
                address: row.try_get("address")?,
                required_from_block: row.try_get("required_from_block")?,
                required_to_block: row.try_get("required_to_block")?,
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    #[test]
    fn missing_coverage_error_keeps_exact_retry_requirement() {
        let error = uncovered_coverage_error(
            "ethereum-sepolia",
            3,
            RequiredWatchedTuple {
                source_family: "ens_v2_registry_l1".to_owned(),
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                required_from_block: 10,
                required_to_block: 20,
            },
        );

        assert_eq!(
            error.downcast_ref::<EnsV2MissingCoverage>(),
            Some(&EnsV2MissingCoverage {
                chain: "ethereum-sepolia".to_owned(),
                retention_generation: 3,
                source_family: "ens_v2_registry_l1".to_owned(),
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                required_from_block: 10,
                required_to_block: 20,
            })
        );
    }

    #[tokio::test]
    async fn stored_verified_coverage_is_invalidated_by_later_range_mutation() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("ens_v2_stored_verified_coverage_invalidation"),
            &bigname_storage::MIGRATOR,
            "failed to migrate ENSv2 stored verification invalidation test",
        )
        .await?;
        let chain = "test-chain";
        let source_family = "ens_v2_registry_l1";
        let address = "0x0000000000000000000000000000000000000001";
        sqlx::query(
            r#"
            INSERT INTO raw_log_staging_input_revisions (
                chain_id,
                revision,
                retention_generation,
                retained_history_complete,
                incomplete_since
            )
            VALUES ($1, 5, 1, false, now())
            "#,
        )
        .bind(chain)
        .execute(database.pool())
        .await?;
        let job_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO backfill_jobs (
                deployment_profile,
                chain_id,
                raw_log_retention_generation,
                source_identity,
                scan_mode,
                range_start_block_number,
                range_end_block_number,
                idempotency_key,
                status,
                completed_at,
                stored_verification_raw_log_input_revision,
                stored_verification_from_block,
                stored_verification_to_block,
                stored_verification_log_count,
                stored_verification_digest
            )
            VALUES (
                'test', $1, 1, '{}'::JSONB, 'hash_pinned_block',
                100, 120, 'ens-v2-stored-verification-current',
                'completed'::backfill_lifecycle_status, now(),
                5, 100, 120, 0, '00000000000000000000000000000000'
            )
            RETURNING backfill_job_id
            "#,
        )
        .bind(chain)
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
            VALUES ($1, $2, $3, 'address', $4, 100, 120, 'job_completion')
            "#,
        )
        .bind(job_id)
        .bind(chain)
        .bind(source_family)
        .bind(address)
        .execute(database.pool())
        .await?;
        let requirement = RequiredWatchedTuple {
            source_family: source_family.to_owned(),
            address: address.to_owned(),
            required_from_block: 100,
            required_to_block: 120,
        };
        let mut connection = database.pool().acquire().await?;
        assert!(
            find_uncovered_generation_bound_requirement(
                connection.as_mut(),
                chain,
                std::slice::from_ref(&requirement),
                1,
            )
            .await?
            .is_none()
        );
        drop(connection);

        sqlx::query(
            r#"
            INSERT INTO raw_log_staging_block_revisions (
                chain_id,
                block_hash,
                block_number,
                revision
            )
            VALUES ($1, '0xchanged', 110, 6)
            "#,
        )
        .bind(chain)
        .execute(database.pool())
        .await?;
        sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 6 WHERE chain_id = $1")
            .bind(chain)
            .execute(database.pool())
            .await?;
        let mut connection = database.pool().acquire().await?;

        assert_eq!(
            find_uncovered_generation_bound_requirement(
                connection.as_mut(),
                chain,
                std::slice::from_ref(&requirement),
                1,
            )
            .await?,
            Some(requirement)
        );
        drop(connection);
        database.cleanup().await
    }
}
