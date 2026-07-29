use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, Row};

use crate::backfill_jobs::BackfillTopicCoverageRequirement;

#[path = "full_closure_coverage/eligible_facts.rs"]
mod eligible_facts;
#[path = "full_closure_coverage/sync.rs"]
mod sync;

pub const FULL_CLOSURE_COVERAGE_PROOF_FORMAT_VERSION: &str = "full_closure_coverage_rollup_v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullClosureCoverageSynchronization {
    pub full_rebuild: bool,
    pub appended_fact_count: u64,
    pub rebuilt_key_count: u64,
    pub coverage_input_revision: i64,
    pub raw_log_input_revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullClosureCoverageScanOutcome {
    pub violations: Vec<BackfillTopicCoverageRequirement>,
    pub synchronization: FullClosureCoverageSynchronization,
}

/// Synchronize the current-generation coverage aggregate and compare explicit
/// watched requirements against it. The aggregate is rebuildable from
/// completed-job coverage facts and is never itself replay authority.
pub async fn find_uncovered_full_closure_coverage(
    pool: &PgPool,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, Vec<String>>,
    requirements: &[BackfillTopicCoverageRequirement],
    expected_retention_generation: i64,
    expected_discovery_admission_epoch: i64,
    limit: i64,
) -> Result<FullClosureCoverageScanOutcome> {
    validate_inputs(
        chain,
        current_topic0s_by_family,
        expected_retention_generation,
        expected_discovery_admission_epoch,
        limit,
    )?;
    crate::ensure_and_load_raw_log_retention_generation(pool, chain)
        .await
        .with_context(|| {
            format!("failed to ensure raw-log authority before full-closure proof for {chain}")
        })?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin full-closure coverage proof")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await
        .context("failed to establish repeatable-read full-closure proof snapshot")?;
    let synchronization = sync::synchronize(
        transaction.as_mut(),
        chain,
        expected_retention_generation,
        expected_discovery_admission_epoch,
        current_topic0s_by_family,
    )
    .await?;
    let violations = find_violations(
        transaction.as_mut(),
        chain,
        requirements,
        expected_retention_generation,
        limit,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit full-closure coverage proof")?;

    let observed_revision = load_full_closure_coverage_input_revision(pool, chain).await?;
    ensure!(
        observed_revision == synchronization.coverage_input_revision,
        "coverage facts changed while proving full closure for {chain}: expected input revision {}, observed {observed_revision}",
        synchronization.coverage_input_revision
    );
    Ok(FullClosureCoverageScanOutcome {
        violations,
        synchronization,
    })
}

pub async fn load_full_closure_coverage_input_revision(pool: &PgPool, chain: &str) -> Result<i64> {
    ensure!(
        !chain.trim().is_empty(),
        "full-closure coverage chain must not be empty"
    );
    sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM full_closure_coverage_input_revisions WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load full-closure coverage input revision for {chain}"))
    .map(|revision| revision.unwrap_or(0))
}

fn validate_inputs(
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, Vec<String>>,
    expected_retention_generation: i64,
    expected_discovery_admission_epoch: i64,
    limit: i64,
) -> Result<()> {
    ensure!(
        !chain.trim().is_empty(),
        "full-closure coverage chain must not be empty"
    );
    ensure!(
        expected_retention_generation >= 0,
        "full-closure coverage retention generation must not be negative"
    );
    ensure!(
        expected_discovery_admission_epoch >= 0,
        "full-closure coverage admission epoch must not be negative"
    );
    ensure!(limit > 0, "full-closure coverage limit must be positive");
    ensure!(
        current_topic0s_by_family.iter().all(|(family, topics)| {
            !family.is_empty()
                && !topics.is_empty()
                && topics.windows(2).all(|pair| pair[0] < pair[1])
                && topics.iter().all(|topic| {
                    topic.len() == 66
                        && topic.starts_with("0x")
                        && topic[2..]
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        }),
        "full-closure coverage topic sets must be nonempty, sorted, deduplicated lowercase topic0 values"
    );
    Ok(())
}

async fn find_violations(
    connection: &mut sqlx::PgConnection,
    chain: &str,
    requirements: &[BackfillTopicCoverageRequirement],
    retention_generation: i64,
    limit: i64,
) -> Result<Vec<BackfillTopicCoverageRequirement>> {
    let requirements = requirements
        .iter()
        .filter(|requirement| requirement.required_from_block <= requirement.required_to_block)
        .collect::<Vec<_>>();
    if requirements.is_empty() {
        return Ok(Vec::new());
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
    let rows = sqlx::query(
        r#"
        WITH required_tuples AS (
            SELECT *
            FROM UNNEST(
                $3::TEXT[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::BIGINT[]
            ) AS watched(
                source_family,
                address,
                required_from_block,
                required_to_block
            )
        )
        SELECT
            watched.source_family,
            watched.address,
            watched.required_from_block,
            watched.required_to_block
        FROM required_tuples watched
        LEFT JOIN full_closure_coverage_rollups address_coverage
          ON address_coverage.chain_id = $1
         AND address_coverage.raw_log_retention_generation = $2
         AND address_coverage.source_family = watched.source_family
         AND address_coverage.scope = 'address'
         AND address_coverage.address = watched.address
        LEFT JOIN full_closure_coverage_rollups family_coverage
          ON family_coverage.chain_id = $1
         AND family_coverage.raw_log_retention_generation = $2
         AND family_coverage.source_family = watched.source_family
         AND family_coverage.scope = 'family'
         AND family_coverage.address IS NULL
        WHERE NOT (
            (
                COALESCE(
                    address_coverage.covered_blocks,
                    '{}'::INT8MULTIRANGE
                )
                + COALESCE(
                    family_coverage.covered_blocks,
                    '{}'::INT8MULTIRANGE
                )
            ) @> int8range(
                watched.required_from_block,
                watched.required_to_block,
                '[]'
            )
        )
        ORDER BY
            watched.source_family,
            watched.address,
            watched.required_from_block
        LIMIT $7
        "#,
    )
    .bind(chain)
    .bind(retention_generation)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(limit)
    .fetch_all(connection)
    .await
    .with_context(|| {
        format!(
            "failed to compare {} full-closure coverage requirements for {chain}",
            requirements.len()
        )
    })?;
    rows.into_iter()
        .map(|row| {
            Ok(BackfillTopicCoverageRequirement {
                source_family: row.try_get("source_family")?,
                address: row.try_get("address")?,
                required_from_block: row.try_get("required_from_block")?,
                required_to_block: row.try_get("required_to_block")?,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "full_closure_coverage/tests.rs"]
mod tests;
