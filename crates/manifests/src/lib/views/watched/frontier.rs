use anyhow::{Context, Result, ensure};
use bigname_storage::STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE;
use sqlx::{PgConnection, PgPool, Row};

use super::{ManifestRuntimeProgress, RequiredWatchedTuple};

const COVERAGE_SOURCE_PAGE_ROWS: i64 = 1_000;
const COVERAGE_DELTA_TABLE: &str = "stored_lineage_coverage_frontier_candidate_delta";

// Created and owned by bigname-storage's publication guard. Keeping manifest
// authority in a temporary relation lets PostgreSQL derive, diff, and publish
// large watched surfaces without transferring the full snapshot to Rust.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredLineageCoverageCandidateSummary {
    pub requirement_tuple_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredLineageCoverageDeltaCursor {
    source_family: String,
    address: String,
    required_from_block: i64,
    required_to_block: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredLineageCoverageDeltaPage {
    pub requirements: Vec<RequiredWatchedTuple>,
    pub next_cursor: Option<StoredLineageCoverageDeltaCursor>,
}

/// Earliest explicit block at which a currently declared or historically
/// admitted log-producing tuple became watched, bounded by `through_block`.
/// Unknown starts remain unknown rather than being synthesized as block zero.
pub async fn load_earliest_known_watched_block(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    log_producing_source_families: &[String],
) -> Result<Option<i64>> {
    if log_producing_source_families.is_empty() {
        return Ok(None);
    }

    let required_watched_tuples_cte = super::coverage::required_watched_tuples_cte();
    let query = format!(
        r#"
        {required_watched_tuples_cte}
        SELECT MIN(active_from_block_number)::BIGINT
        FROM watched
        WHERE active_from_block_number IS NOT NULL
          AND active_from_block_number <= $3::BIGINT
        "#
    );
    sqlx::query_scalar::<_, Option<i64>>(&query)
        .bind(chain)
        // `required_tuples` is unused by this SELECT, but its parameterized
        // definition shares the CTE so the watched-set rules stay identical.
        .bind(through_block)
        .bind(through_block)
        .bind(log_producing_source_families)
        .fetch_one(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load earliest known watched block for chain {chain} through {through_block}"
            )
        })
}

pub async fn load_earliest_known_watched_block_with_progress(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
    log_producing_source_families: &[String],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Option<i64>> {
    if log_producing_source_families.is_empty() {
        return Ok(None);
    }
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire earliest watched-block scan connection")?;
    let mut earliest = None;
    for watched_branch in ["manifest_watched_intervals", "discovery_watched_intervals"] {
        let mut after_id = 0i64;
        loop {
            let source_page_query = super::intervals::with_streaming_watched_intervals(&format!(
                r#"
                SELECT DISTINCT watched.source_row_id
                FROM {watched_branch} watched
                WHERE watched.source_row_id > $3
                  AND {historical_predicate}
                  AND watched.chain = $1
                  AND watched.source_family = ANY($4::TEXT[])
                  AND watched.active_from_block_number IS NOT NULL
                  AND watched.active_from_block_number <= $2
                ORDER BY watched.source_row_id
                LIMIT $5
                "#,
                historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
            ));
            let source_ids = sqlx::query_scalar::<_, i64>(&source_page_query)
                .bind(chain)
                .bind(through_block)
                .bind(after_id)
                .bind(log_producing_source_families)
                .bind(COVERAGE_SOURCE_PAGE_ROWS)
                .fetch_all(&mut *connection)
                .await
                .with_context(|| {
                    format!("failed to page {watched_branch} for earliest watched block")
                })?;
            let Some(last_id) = source_ids.last().copied() else {
                break;
            };
            after_id = last_id;
            let query = super::intervals::with_streaming_watched_intervals(&format!(
                r#"
                SELECT MIN(watched.active_from_block_number)::BIGINT
                FROM {watched_branch} watched
                WHERE watched.source_row_id = ANY($3::BIGINT[])
                  AND {historical_predicate}
                  AND watched.chain = $1
                  AND watched.source_family = ANY($4::TEXT[])
                  AND watched.active_from_block_number IS NOT NULL
                  AND watched.active_from_block_number <= $2
                "#,
                historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
            ));
            let page_earliest = sqlx::query_scalar::<_, Option<i64>>(&query)
                .bind(chain)
                .bind(through_block)
                .bind(&source_ids)
                .bind(log_producing_source_families)
                .fetch_one(&mut *connection)
                .await
                .with_context(|| {
                    format!("failed to inspect {watched_branch} earliest watched block page")
                })?;
            if let Some(page_earliest) = page_earliest {
                earliest =
                    Some(earliest.map_or(page_earliest, |value: i64| value.min(page_earliest)));
            }
            progress.record(pool).await?;
        }
    }
    Ok(earliest)
}

/// Materialize the complete authoritative requirement snapshot, clipped to
/// inclusive `verified_from_block..=verified_through_block`, on the storage
/// publication guard's transaction-local connection.
pub async fn materialize_stored_lineage_coverage_candidate(
    connection: &mut PgConnection,
    chain: &str,
    verified_from_block: i64,
    verified_through_block: i64,
    log_producing_source_families: &[String],
) -> Result<StoredLineageCoverageCandidateSummary> {
    ensure!(
        !chain.trim().is_empty(),
        "coverage candidate chain must not be empty"
    );
    ensure!(
        verified_from_block >= 0,
        "coverage candidate lower bound must not be negative"
    );
    ensure!(
        verified_through_block >= verified_from_block,
        "coverage candidate bounds must not be inverted"
    );
    ensure!(
        verified_through_block < i64::MAX,
        "inclusive coverage candidate upper bound cannot be represented as INT8MULTIRANGE"
    );

    sqlx::query(&format!(
        "TRUNCATE pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}"
    ))
    .execute(&mut *connection)
    .await
    .context("failed to clear stored-lineage coverage candidate")?;
    if log_producing_source_families.is_empty() {
        return Ok(StoredLineageCoverageCandidateSummary {
            requirement_tuple_count: 0,
        });
    }

    let query = super::intervals::with_watched_intervals(&format!(
        r#"
        , clipped_requirements AS (
            SELECT
                watched.source_family,
                LOWER(watched.address) AS address,
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
        )
        INSERT INTO pg_temp.{candidate_table} (
            source_family,
            address,
            required_intervals
        )
        SELECT
            source_family,
            address,
            range_agg(int8range(required_from_block, required_to_block + 1, '[)'))
        FROM clipped_requirements
        WHERE required_from_block <= required_to_block
        GROUP BY source_family, address
        "#,
        historical_predicate = super::intervals::HISTORICAL_WATCHED_INTERVAL_PREDICATE,
        candidate_table = STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE,
    ));
    sqlx::query(&query)
        .bind(chain)
        .bind(verified_from_block)
        .bind(verified_through_block)
        .bind(log_producing_source_families)
        .execute(&mut *connection)
        .await
        .with_context(|| {
            format!(
                "failed to materialize stored-lineage coverage candidate for {chain} over {verified_from_block}..={verified_through_block}"
            )
        })?;

    let requirement_tuple_count = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*)::BIGINT FROM pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}"
    ))
    .fetch_one(connection)
    .await
    .with_context(|| format!("failed to count stored-lineage coverage candidate for {chain}"))?;
    Ok(StoredLineageCoverageCandidateSummary {
        requirement_tuple_count,
    })
}

/// Progress-aware candidate materialization. Source rows are first keyset
/// paged by their primary identities; each bounded page contributes its
/// interval union to the transaction-local candidate table.
pub async fn materialize_stored_lineage_coverage_candidate_with_progress(
    connection: &mut PgConnection,
    progress_pool: &PgPool,
    chain: &str,
    verified_from_block: i64,
    verified_through_block: i64,
    log_producing_source_families: &[String],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<StoredLineageCoverageCandidateSummary> {
    ensure!(
        !chain.trim().is_empty(),
        "coverage candidate chain must not be empty"
    );
    ensure!(
        verified_from_block >= 0 && verified_through_block >= verified_from_block,
        "coverage candidate bounds must be non-negative and ordered"
    );
    ensure!(
        verified_through_block < i64::MAX,
        "inclusive coverage candidate upper bound cannot be represented as INT8MULTIRANGE"
    );
    sqlx::query(&format!(
        "TRUNCATE pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}"
    ))
    .execute(&mut *connection)
    .await
    .context("failed to clear stored-lineage coverage candidate")?;
    if log_producing_source_families.is_empty() {
        return Ok(StoredLineageCoverageCandidateSummary {
            requirement_tuple_count: 0,
        });
    }

    materialize_candidate_source_pages(
        connection,
        progress_pool,
        chain,
        verified_from_block,
        verified_through_block,
        log_producing_source_families,
        "manifest_watched_intervals",
        progress,
    )
    .await?;
    materialize_candidate_source_pages(
        connection,
        progress_pool,
        chain,
        verified_from_block,
        verified_through_block,
        log_producing_source_families,
        "discovery_watched_intervals",
        progress,
    )
    .await?;

    let mut requirement_tuple_count = 0i64;
    let mut cursor = None::<(String, String)>;
    loop {
        let rows = sqlx::query(&format!(
            r#"
            SELECT source_family, address
            FROM pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}
            WHERE $1::TEXT IS NULL OR (source_family, address) > ($1, $2)
            ORDER BY source_family, address
            LIMIT $3
            "#,
        ))
        .bind(cursor.as_ref().map(|(family, _)| family))
        .bind(cursor.as_ref().map(|(_, address)| address))
        .bind(COVERAGE_SOURCE_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .context("failed to count a stored-lineage coverage candidate page")?;
        let Some(last) = rows.last() else {
            break;
        };
        cursor = Some((last.try_get("source_family")?, last.try_get("address")?));
        requirement_tuple_count += i64::try_from(rows.len())?;
        progress.record(progress_pool).await?;
    }
    Ok(StoredLineageCoverageCandidateSummary {
        requirement_tuple_count,
    })
}

#[expect(clippy::too_many_arguments)]
#[path = "frontier/source_pages.rs"]
mod source_pages;

use source_pages::materialize_candidate_source_pages;
pub async fn materialize_stored_lineage_coverage_candidate_delta_with_progress(
    connection: &mut PgConnection,
    progress_pool: &PgPool,
    chain: &str,
    topic_changed_source_families: &[String],
    reverify_all: bool,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<()> {
    sqlx::query(&format!(
        r#"
        CREATE TEMP TABLE pg_temp.{COVERAGE_DELTA_TABLE} (
            source_family TEXT NOT NULL,
            address TEXT NOT NULL,
            required_from_block BIGINT NOT NULL,
            required_to_block BIGINT NOT NULL,
            PRIMARY KEY (source_family, address, required_from_block, required_to_block)
        ) ON COMMIT DROP
        "#,
    ))
    .execute(&mut *connection)
    .await
    .context("failed to create stored-lineage coverage delta table")?;
    let mut cursor = None::<(String, String)>;
    loop {
        let rows = sqlx::query(&format!(
            r#"
            SELECT source_family, address
            FROM pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}
            WHERE $1::TEXT IS NULL OR (source_family, address) > ($1, $2)
            ORDER BY source_family, address
            LIMIT $3
            "#,
        ))
        .bind(cursor.as_ref().map(|(family, _)| family))
        .bind(cursor.as_ref().map(|(_, address)| address))
        .bind(COVERAGE_SOURCE_PAGE_ROWS)
        .fetch_all(&mut *connection)
        .await
        .context("failed to page coverage candidates for delta materialization")?;
        let Some(last) = rows.last() else {
            break;
        };
        cursor = Some((last.try_get("source_family")?, last.try_get("address")?));
        let families = rows
            .iter()
            .map(|row| row.try_get::<String, _>("source_family"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let addresses = rows
            .iter()
            .map(|row| row.try_get::<String, _>("address"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        sqlx::query(&format!(
            r#"
            WITH page_keys AS (
                SELECT * FROM UNNEST($4::TEXT[], $5::TEXT[]) key(source_family, address)
            ), delta_by_tuple AS (
                SELECT
                    candidate.source_family,
                    candidate.address,
                    CASE WHEN $3::BOOLEAN
                           OR candidate.source_family = ANY($2::TEXT[])
                         THEN candidate.required_intervals
                         ELSE candidate.required_intervals - COALESCE(
                             persisted.required_intervals,
                             '{{}}'::INT8MULTIRANGE
                         )
                    END AS required_intervals
                FROM page_keys key
                JOIN pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE} candidate
                  USING (source_family, address)
                LEFT JOIN stored_lineage_coverage_frontier_requirements persisted
                  ON persisted.chain_id = $1
                 AND persisted.source_family = candidate.source_family
                 AND persisted.address = candidate.address
            )
            INSERT INTO pg_temp.{COVERAGE_DELTA_TABLE} (
                source_family, address, required_from_block, required_to_block
            )
            SELECT
                delta.source_family,
                delta.address,
                lower(required_range)::BIGINT,
                (upper(required_range) - 1)::BIGINT
            FROM delta_by_tuple delta
            CROSS JOIN LATERAL unnest(delta.required_intervals) required_range
            "#,
        ))
        .bind(chain)
        .bind(topic_changed_source_families)
        .bind(reverify_all)
        .bind(&families)
        .bind(&addresses)
        .execute(&mut *connection)
        .await
        .context("failed to materialize a stored-lineage coverage delta page")?;
        progress.record(progress_pool).await?;
    }
    Ok(())
}

/// Return one bounded, ordered page of candidate intervals which need fact
/// verification. Unchanged topic families return only candidate-minus-saved
/// intervals. Changed topic families (or a cold/deep rebuild) return their
/// complete candidate intervals.
pub async fn load_stored_lineage_coverage_candidate_delta_page(
    connection: &mut PgConnection,
    chain: &str,
    topic_changed_source_families: &[String],
    reverify_all: bool,
    cursor: Option<&StoredLineageCoverageDeltaCursor>,
    limit: i64,
) -> Result<StoredLineageCoverageDeltaPage> {
    ensure!(
        !chain.trim().is_empty(),
        "coverage delta chain must not be empty"
    );
    ensure!(
        (1..=1_000).contains(&limit),
        "coverage delta page limit must be between 1 and 1000"
    );
    let delta_is_materialized = sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('pg_temp.stored_lineage_coverage_frontier_candidate_delta') IS NOT NULL",
    )
    .fetch_one(&mut *connection)
    .await
    .context("failed to inspect stored-lineage coverage delta materialization")?;
    let rows = if delta_is_materialized {
        sqlx::query(&format!(
            r#"
            SELECT source_family, address, required_from_block, required_to_block
            FROM pg_temp.{COVERAGE_DELTA_TABLE}
            WHERE $1::TEXT IS NULL
               OR (source_family, address, required_from_block, required_to_block)
                  > ($1::TEXT, $2::TEXT, $3::BIGINT, $4::BIGINT)
            ORDER BY source_family, address, required_from_block, required_to_block
            LIMIT $5
            "#,
        ))
        .bind(cursor.map(|cursor| cursor.source_family.as_str()))
        .bind(cursor.map(|cursor| cursor.address.as_str()))
        .bind(cursor.map(|cursor| cursor.required_from_block))
        .bind(cursor.map(|cursor| cursor.required_to_block))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("failed to page materialized coverage delta for {chain}"))?
    } else {
        sqlx::query(&format!(
            r#"
        WITH delta_by_tuple AS (
            SELECT
                candidate.source_family,
                candidate.address,
                CASE
                    WHEN $3::BOOLEAN
                      OR candidate.source_family = ANY($2::TEXT[])
                        THEN candidate.required_intervals
                    ELSE candidate.required_intervals - COALESCE(
                        persisted.required_intervals,
                        '{{}}'::INT8MULTIRANGE
                    )
                END AS required_intervals
            FROM pg_temp.{candidate_table} candidate
            LEFT JOIN stored_lineage_coverage_frontier_requirements persisted
              ON persisted.chain_id = $1
             AND persisted.source_family = candidate.source_family
             AND persisted.address = candidate.address
        ),
        delta_intervals AS (
            SELECT
                delta.source_family,
                delta.address,
                lower(required_range)::BIGINT AS required_from_block,
                (upper(required_range) - 1)::BIGINT AS required_to_block
            FROM delta_by_tuple delta
            CROSS JOIN LATERAL unnest(delta.required_intervals) required_range
        )
        SELECT
            source_family,
            address,
            required_from_block,
            required_to_block
        FROM delta_intervals
        WHERE $4::TEXT IS NULL
           OR (source_family, address, required_from_block, required_to_block)
              > ($4::TEXT, $5::TEXT, $6::BIGINT, $7::BIGINT)
        ORDER BY source_family, address, required_from_block, required_to_block
        LIMIT $8
        "#,
            candidate_table = STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE,
        ))
        .bind(chain)
        .bind(topic_changed_source_families)
        .bind(reverify_all)
        .bind(cursor.map(|cursor| cursor.source_family.as_str()))
        .bind(cursor.map(|cursor| cursor.address.as_str()))
        .bind(cursor.map(|cursor| cursor.required_from_block))
        .bind(cursor.map(|cursor| cursor.required_to_block))
        .bind(limit)
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("failed to page stored-lineage coverage delta for {chain}"))?
    };

    let requirements = rows
        .into_iter()
        .map(|row| {
            Ok(RequiredWatchedTuple {
                source_family: row
                    .try_get("source_family")
                    .context("missing coverage delta source_family")?,
                address: row
                    .try_get("address")
                    .context("missing coverage delta address")?,
                required_from_block: row
                    .try_get("required_from_block")
                    .context("missing coverage delta lower bound")?,
                required_to_block: row
                    .try_get("required_to_block")
                    .context("missing coverage delta upper bound")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = (requirements.len() as i64 == limit).then(|| {
        let last = requirements
            .last()
            .expect("a full coverage delta page must have a last row");
        StoredLineageCoverageDeltaCursor {
            source_family: last.source_family.clone(),
            address: last.address.clone(),
            required_from_block: last.required_from_block,
            required_to_block: last.required_to_block,
        }
    });
    Ok(StoredLineageCoverageDeltaPage {
        requirements,
        next_cursor,
    })
}

#[cfg(test)]
#[path = "frontier/tests.rs"]
mod tests;
