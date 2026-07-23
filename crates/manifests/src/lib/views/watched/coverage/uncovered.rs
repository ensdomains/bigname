use super::*;

/// Compare the chain's historically authoritative contract tuples
/// (manifest-declared and discovery-edge) with durable
/// `backfill_coverage_facts`, restricted to tuples whose block-number active
/// window intersects `[from_block, to_block]` and whose source family produces
/// logs (`log_producing_source_families`). Deactivation does not erase a closed
/// historical interval while its source and mapped target manifests remain
/// active. A tuple is covered when its required interval (active window ∩
/// evaluated range) is contained by the gap-free union of address-scoped facts
/// for that exact tuple and family-scoped facts for its family. Returns at most
/// `limit` violations ordered by (source_family, address).
pub async fn find_uncovered_watched_tuples(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
    if from_block > to_block {
        anyhow::bail!(
            "uncovered watched tuple scan range start {from_block} is after end {to_block}"
        );
    }
    if log_producing_source_families.is_empty() {
        return Ok(Vec::new());
    }

    let required_watched_tuples_cte = required_watched_tuples_cte();
    let query = format!(
        r#"
        {required_watched_tuples_cte}
        SELECT
            source_family,
            address,
            required_from_block,
            required_to_block
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
                    JOIN backfill_jobs fact_job
                      ON fact_job.backfill_job_id = fact.backfill_job_id
                    WHERE fact.chain_id = $1
                      AND fact_job.status = 'completed'::backfill_lifecycle_status
                      AND fact_job.chain_id = fact.chain_id
                      AND fact.covered_from_block >= fact_job.range_start_block_number
                      AND fact.covered_to_block <= fact_job.range_end_block_number
                      AND fact.source_family = watched.source_family
                      AND (
                          (
                              fact.scope = 'address'
                              AND fact.address = watched.address
                          )
                          OR (
                              fact.scope = 'family'
                              AND fact.address IS NULL
                          )
                      )
                      AND fact.covered_from_block <= watched.required_to_block
                      AND fact.covered_to_block >= watched.required_from_block
                ),
                '{{}}'::INT8MULTIRANGE
            ) @> int8range(
                watched.required_from_block,
                watched.required_to_block,
                '[]'
            )
        )
        ORDER BY source_family, address, required_from_block
        LIMIT $5
        "#
    );
    let rows = sqlx::query(&query)
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(log_producing_source_families)
    .bind(limit)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to scan uncovered watched tuples for chain {chain} over {from_block}..={to_block}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(UncoveredWatchedTuple {
                source_family: row
                    .try_get("source_family")
                    .context("missing uncovered tuple source_family")?,
                address: row
                    .try_get("address")
                    .context("missing uncovered tuple address")?,
                required_from_block: row
                    .try_get("required_from_block")
                    .context("missing uncovered tuple required_from_block")?,
                required_to_block: row
                    .try_get("required_to_block")
                    .context("missing uncovered tuple required_to_block")?,
            })
        })
        .collect()
}

/// Compare an explicit, already-diffed set of watched requirements with
/// durable coverage facts. Stored-lineage promotion uses this after a watched
/// set or topic-selector change so unchanged tuples do not need historical
/// coverage re-verification.
pub async fn find_uncovered_required_watched_tuples(
    pool: &PgPool,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire connection for explicit watched coverage verification")?;
    find_uncovered_required_watched_tuples_with_retention_generation(
        &mut connection,
        chain,
        requirements,
        None,
        limit,
    )
    .await
}

pub async fn find_uncovered_required_watched_tuples_in_transaction(
    connection: &mut PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
    find_uncovered_required_watched_tuples_with_retention_generation(
        connection,
        chain,
        requirements,
        None,
        limit,
    )
    .await
}

/// Generation-bound variant used when absence in the retained raw-log corpus
/// is replay authority. Coverage from a completed job in any older retention
/// generation is deliberately ignored even when its numeric interval and
/// watched tuple match exactly.
pub async fn find_uncovered_required_watched_tuples_for_retention_generation(
    pool: &PgPool,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
    if retention_generation < 0 {
        anyhow::bail!(
            "raw-log retention generation must not be negative, got {retention_generation}"
        );
    }
    let mut connection = pool.acquire().await.context(
        "failed to acquire connection for generation-bound watched coverage verification",
    )?;
    find_uncovered_required_watched_tuples_for_retention_generation_in_transaction(
        &mut connection,
        chain,
        requirements,
        retention_generation,
        limit,
    )
    .await
}

pub async fn find_uncovered_required_watched_tuples_for_retention_generation_in_transaction(
    connection: &mut PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: i64,
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
    if retention_generation < 0 {
        anyhow::bail!(
            "raw-log retention generation must not be negative, got {retention_generation}"
        );
    }
    find_uncovered_required_watched_tuples_with_retention_generation(
        connection,
        chain,
        requirements,
        Some(retention_generation),
        limit,
    )
    .await
}

async fn find_uncovered_required_watched_tuples_with_retention_generation(
    connection: &mut PgConnection,
    chain: &str,
    requirements: &[RequiredWatchedTuple],
    retention_generation: Option<i64>,
    limit: i64,
) -> Result<Vec<UncoveredWatchedTuple>> {
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
    let required_from_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_from_block)
        .collect::<Vec<_>>();
    let required_to_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_to_block)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
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
        SELECT
            source_family,
            address,
            required_from_block,
            required_to_block
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
                    JOIN backfill_jobs fact_job
                      ON fact_job.backfill_job_id = fact.backfill_job_id
                    WHERE fact.chain_id = $1
                      AND fact_job.status = 'completed'::backfill_lifecycle_status
                      AND fact_job.chain_id = fact.chain_id
                      AND ($6::BIGINT IS NULL OR fact_job.raw_log_retention_generation = $6)
                      AND fact.covered_from_block >= fact_job.range_start_block_number
                      AND fact.covered_to_block <= fact_job.range_end_block_number
                      AND fact.source_family = watched.source_family
                      AND (
                          (
                              fact.scope = 'address'
                              AND fact.address = watched.address
                          )
                          OR (
                              fact.scope = 'family'
                              AND fact.address IS NULL
                          )
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
        LIMIT $7
        "#,
    )
    .bind(chain)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&required_from_blocks)
    .bind(&required_to_blocks)
    .bind(retention_generation)
    .bind(limit)
    .fetch_all(connection)
    .await
    .with_context(|| {
        format!(
            "failed to scan {} explicit watched tuple coverage requirements for chain {chain}",
            requirements.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(UncoveredWatchedTuple {
                source_family: row
                    .try_get("source_family")
                    .context("missing uncovered tuple source_family")?,
                address: row
                    .try_get("address")
                    .context("missing uncovered tuple address")?,
                required_from_block: row
                    .try_get("required_from_block")
                    .context("missing uncovered tuple required_from_block")?,
                required_to_block: row
                    .try_get("required_to_block")
                    .context("missing uncovered tuple required_to_block")?,
            })
        })
        .collect()
}
