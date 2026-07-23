use std::collections::BTreeSet;

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use sqlx::{Executor, PgConnection, PgPool, Postgres, Row};

use super::ManifestRuntimeProgress;

pub(super) fn required_watched_tuples_cte() -> String {
    super::intervals::with_watched_intervals(&format!(
        r#"
, watched AS (
    SELECT
        watched.source_family,
        watched.address,
        watched.active_from_block_number,
        watched.active_to_block_number
    FROM watched_intervals watched
    WHERE {historical_predicate}
      AND watched.chain = $1
      AND watched.source_family = ANY($4::TEXT[])
),
required_tuples AS (
    SELECT DISTINCT
        source_family,
        address,
        GREATEST(COALESCE(active_from_block_number, $2::BIGINT), $2::BIGINT)
            AS required_from_block,
        LEAST(COALESCE(active_to_block_number, $3::BIGINT), $3::BIGINT)
            AS required_to_block
    FROM watched
    WHERE COALESCE(active_from_block_number, $2::BIGINT) <= $3::BIGINT
      AND COALESCE(active_to_block_number, $3::BIGINT) >= $2::BIGINT
      AND GREATEST(COALESCE(active_from_block_number, $2::BIGINT), $2::BIGINT)
          <= LEAST(COALESCE(active_to_block_number, $3::BIGINT), $3::BIGINT)
)
"#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
    ))
}

/// A historically authoritative watched tuple and the part of its block
/// interval required within the evaluated range.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RequiredWatchedTuple {
    pub source_family: String,
    pub address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
}

/// A watched (source_family, address) tuple whose required interval within the
/// evaluated block range is not fully covered by the gap-free union of its
/// exact address-scoped and family-scoped `backfill_coverage_facts` rows whose
/// parent job is completed and contains the whole fact interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UncoveredWatchedTuple {
    pub source_family: String,
    pub address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
}

/// Load every active manifest declaration and discovery tuple backed by active
/// source/target manifest authority whose block-number interval intersects the
/// evaluated range. The returned interval is clamped to that range. Closing or
/// deactivating a discovery row does not erase its bounded historical interval
/// while that manifest authority remains active; deprecated profile evidence
/// is retained audit history, not coverage authority.
pub async fn load_required_watched_tuples(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
) -> Result<Vec<RequiredWatchedTuple>> {
    load_required_watched_tuples_with_executor(
        pool,
        chain,
        from_block,
        to_block,
        log_producing_source_families,
    )
    .await
}

/// Transaction-scoped variant used when the watched set must be read under a
/// discovery-admission epoch fence.
pub async fn load_required_watched_tuples_in_transaction(
    connection: &mut PgConnection,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
) -> Result<Vec<RequiredWatchedTuple>> {
    load_required_watched_tuples_with_executor(
        connection,
        chain,
        from_block,
        to_block,
        log_producing_source_families,
    )
    .await
}

/// Progress-aware required-tuple load for callers that do not already own a
/// transaction. Rows stream from the two watch-authority branches without a
/// global `UNION`/`DISTINCT` sort; client-side ordering restores the exact
/// public result while each completed page can refresh loop liveness.
pub async fn load_required_watched_tuples_with_progress(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<RequiredWatchedTuple>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire required watched tuple stream connection")?;
    load_required_watched_tuples_in_transaction_with_progress(
        &mut connection,
        pool,
        chain,
        from_block,
        to_block,
        log_producing_source_families,
        progress,
    )
    .await
}

/// Test whether watched authority requires any tuple in the interval without
/// retaining the complete mainnet-sized tuple set.
pub async fn has_required_watched_tuples(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
) -> Result<bool> {
    has_required_watched_tuples_inner(
        pool,
        chain,
        from_block,
        to_block,
        log_producing_source_families,
    )
    .await
}

/// Progress-aware existence check. A callback follows the completed probe;
/// unlike the full loader, no watched tuples are accumulated or copied.
pub async fn has_required_watched_tuples_with_progress(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<bool> {
    let found = has_required_watched_tuples_inner(
        pool,
        chain,
        from_block,
        to_block,
        log_producing_source_families,
    )
    .await?;
    progress.record(pool).await?;
    Ok(found)
}

async fn has_required_watched_tuples_inner(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
) -> Result<bool> {
    if from_block > to_block {
        anyhow::bail!(
            "required watched tuple scan range start {from_block} is after end {to_block}"
        );
    }
    if log_producing_source_families.is_empty() {
        return Ok(false);
    }
    let query = super::intervals::with_streaming_watched_intervals(&format!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM watched_intervals watched
            WHERE {historical_predicate}
              AND watched.chain = $1
              AND watched.source_family = ANY($4::TEXT[])
              AND COALESCE(watched.active_from_block_number, $2::BIGINT) <= $3::BIGINT
              AND COALESCE(watched.active_to_block_number, $3::BIGINT) >= $2::BIGINT
              AND GREATEST(
                    COALESCE(watched.active_from_block_number, $2::BIGINT),
                    $2::BIGINT
                  ) <= LEAST(
                    COALESCE(watched.active_to_block_number, $3::BIGINT),
                    $3::BIGINT
                  )
            LIMIT 1
        )
        "#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
    ));
    sqlx::query_scalar(&query)
        .bind(chain)
        .bind(from_block)
        .bind(to_block)
        .bind(log_producing_source_families)
        .fetch_one(pool)
        .await
        .with_context(|| {
            format!(
                "failed to inspect required watched tuples for chain {chain} over {from_block}..={to_block}"
            )
        })
}

/// Transaction-scoped progress-aware variant. The heartbeat write uses the
/// pool rather than the fenced connection, so the caller's snapshot and locks
/// remain intact while forward row progress is reported.
pub async fn load_required_watched_tuples_in_transaction_with_progress(
    connection: &mut PgConnection,
    progress_pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Vec<RequiredWatchedTuple>> {
    if from_block > to_block {
        anyhow::bail!(
            "required watched tuple scan range start {from_block} is after end {to_block}"
        );
    }
    if log_producing_source_families.is_empty() {
        return Ok(Vec::new());
    }

    let query = super::intervals::with_streaming_watched_intervals(&format!(
        r#"
        SELECT
            watched.source_family,
            watched.address,
            GREATEST(
                COALESCE(watched.active_from_block_number, $2::BIGINT),
                $2::BIGINT
            ) AS required_from_block,
            LEAST(
                COALESCE(watched.active_to_block_number, $3::BIGINT),
                $3::BIGINT
            ) AS required_to_block
        FROM watched_intervals watched
        WHERE {historical_predicate}
          AND watched.chain = $1
          AND watched.source_family = ANY($4::TEXT[])
          AND COALESCE(watched.active_from_block_number, $2::BIGINT) <= $3::BIGINT
          AND COALESCE(watched.active_to_block_number, $3::BIGINT) >= $2::BIGINT
          AND GREATEST(
                COALESCE(watched.active_from_block_number, $2::BIGINT),
                $2::BIGINT
              ) <= LEAST(
                COALESCE(watched.active_to_block_number, $3::BIGINT),
                $3::BIGINT
              )
        "#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
    ));
    let mut rows = sqlx::query(&query)
        .bind(chain)
        .bind(from_block)
        .bind(to_block)
        .bind(log_producing_source_families)
        .fetch(&mut *connection);
    let mut required = BTreeSet::new();
    let mut streamed = 0usize;
    while let Some(row) = rows.try_next().await.with_context(|| {
        format!(
            "failed to stream required watched tuples for chain {chain} over {from_block}..={to_block}"
        )
    })? {
        required.insert(required_watched_tuple_from_row(&row)?);
        streamed += 1;
        if streamed.is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS) {
            progress.record(progress_pool).await?;
        }
    }
    if streamed > 0 && !streamed.is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS) {
        progress.record(progress_pool).await?;
    }

    let mut result = Vec::with_capacity(required.len());
    for requirement in required {
        result.push(requirement);
        if result
            .len()
            .is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS)
        {
            progress.record(progress_pool).await?;
        }
    }
    if !result.is_empty()
        && !result
            .len()
            .is_multiple_of(super::WATCHED_PLAN_PROGRESS_ROWS)
    {
        progress.record(progress_pool).await?;
    }
    Ok(result)
}

fn required_watched_tuple_from_row(row: &sqlx::postgres::PgRow) -> Result<RequiredWatchedTuple> {
    Ok(RequiredWatchedTuple {
        source_family: row
            .try_get("source_family")
            .context("missing required tuple source_family")?,
        address: row
            .try_get("address")
            .context("missing required tuple address")?,
        required_from_block: row
            .try_get("required_from_block")
            .context("missing required tuple required_from_block")?,
        required_to_block: row
            .try_get("required_to_block")
            .context("missing required tuple required_to_block")?,
    })
}

async fn load_required_watched_tuples_with_executor<'e, E>(
    executor: E,
    chain: &str,
    from_block: i64,
    to_block: i64,
    log_producing_source_families: &[String],
) -> Result<Vec<RequiredWatchedTuple>>
where
    E: Executor<'e, Database = Postgres>,
{
    if from_block > to_block {
        anyhow::bail!(
            "required watched tuple scan range start {from_block} is after end {to_block}"
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
        FROM required_tuples
        ORDER BY source_family, address, required_from_block
        "#
    );
    let rows = sqlx::query(&query)
        .bind(chain)
        .bind(from_block)
        .bind(to_block)
        .bind(log_producing_source_families)
        .fetch_all(executor)
        .await
        .with_context(|| {
            format!(
                "failed to load required watched tuples for chain {chain} over {from_block}..={to_block}"
            )
        })?;

    rows.iter().map(required_watched_tuple_from_row).collect()
}

#[path = "coverage/uncovered.rs"]
mod uncovered;

pub use uncovered::{
    find_uncovered_required_watched_tuples,
    find_uncovered_required_watched_tuples_for_retention_generation,
    find_uncovered_required_watched_tuples_for_retention_generation_in_transaction,
    find_uncovered_required_watched_tuples_in_transaction, find_uncovered_watched_tuples,
};
