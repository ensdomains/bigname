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
}
