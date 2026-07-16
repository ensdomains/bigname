use anyhow::{Context, Result, ensure};
use bigname_storage::STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE;
use sqlx::{PgConnection, PgPool, Row};

use super::RequiredWatchedTuple;

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
    let rows = sqlx::query(&format!(
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
    .fetch_all(connection)
    .await
    .with_context(|| format!("failed to page stored-lineage coverage delta for {chain}"))?;

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
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Result;
    use bigname_storage::{
        StoredLineageCoverageFrontierPublication, StoredLineageCoveragePublicationGuard,
        begin_stored_lineage_coverage_frontier_publication,
    };
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    const CHAIN: &str = "frontier-test-chain";
    const FAMILY: &str = "frontier_test_family";
    const ADDRESS_ONE: &str = "0x0000000000000000000000000000000000000001";
    const ADDRESS_TWO: &str = "0x0000000000000000000000000000000000000002";

    async fn database(name: &str) -> Result<TestDatabase> {
        TestDatabase::create_migrated(
            TestDatabaseConfig::new(name),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for manifest frontier test",
        )
        .await
    }

    fn publication() -> StoredLineageCoverageFrontierPublication {
        StoredLineageCoverageFrontierPublication {
            discovery_admission_epoch: 0,
            verified_from_block: 10,
            verified_through_block: 40,
            topic0s_by_family: BTreeMap::from([(FAMILY.to_owned(), vec![format!("0x{:064x}", 1)])]),
        }
    }

    async fn stage(
        guard: &mut StoredLineageCoveragePublicationGuard,
        rows: &[(&str, i64, i64)],
    ) -> Result<()> {
        for (address, from, through) in rows {
            sqlx::query(&format!(
                r#"
                INSERT INTO pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE} (
                    source_family,
                    address,
                    required_intervals
                )
                VALUES ($1, $2, int8multirange(int8range($3, $4 + 1, '[)')))
                "#,
            ))
            .bind(FAMILY)
            .bind(address)
            .bind(from)
            .bind(through)
            .execute(guard.connection_mut())
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn server_delta_handles_addition_removal_readmission_and_topic_change() -> Result<()> {
        let database = database("manifest_frontier_delta").await?;
        let mut first =
            begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, None, 0)
                .await?;
        stage(&mut first, &[(ADDRESS_ONE, 10, 20)]).await?;
        first.publish(&publication()).await?;

        let mut replacement =
            begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, Some(1), 0)
                .await?;
        stage(
            &mut replacement,
            &[(ADDRESS_ONE, 10, 15), (ADDRESS_TWO, 30, 40)],
        )
        .await?;
        let delta = load_stored_lineage_coverage_candidate_delta_page(
            replacement.connection_mut(),
            CHAIN,
            &[],
            false,
            None,
            32,
        )
        .await?;
        assert_eq!(
            delta.requirements,
            vec![RequiredWatchedTuple {
                source_family: FAMILY.to_owned(),
                address: ADDRESS_TWO.to_owned(),
                required_from_block: 30,
                required_to_block: 40,
            }],
            "a shortening/removal needs no fact read while an addition does"
        );
        replacement.publish(&publication()).await?;

        let mut readmission =
            begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, Some(2), 0)
                .await?;
        stage(
            &mut readmission,
            &[(ADDRESS_ONE, 10, 20), (ADDRESS_TWO, 30, 40)],
        )
        .await?;
        let delta = load_stored_lineage_coverage_candidate_delta_page(
            readmission.connection_mut(),
            CHAIN,
            &[],
            false,
            None,
            32,
        )
        .await?;
        assert_eq!(
            delta.requirements,
            vec![RequiredWatchedTuple {
                source_family: FAMILY.to_owned(),
                address: ADDRESS_ONE.to_owned(),
                required_from_block: 16,
                required_to_block: 20,
            }],
            "readmission verifies only the interval absent from the saved replacement"
        );
        let topic_delta = load_stored_lineage_coverage_candidate_delta_page(
            readmission.connection_mut(),
            CHAIN,
            &[FAMILY.to_owned()],
            false,
            None,
            32,
        )
        .await?;
        assert_eq!(topic_delta.requirements.len(), 2);
        assert_eq!(topic_delta.requirements[0].required_from_block, 10);
        assert_eq!(topic_delta.requirements[0].required_to_block, 20);
        drop(readmission);

        database.cleanup().await
    }

    #[tokio::test]
    async fn high_cardinality_candidate_returns_only_bounded_delta_pages() -> Result<()> {
        let database = database("manifest_frontier_bounded_delta").await?;
        let mut guard =
            begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, None, 0)
                .await?;
        sqlx::query(&format!(
            r#"
            INSERT INTO pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE} (
                source_family,
                address,
                required_intervals
            )
            SELECT
                $1,
                '0x' || lpad(to_hex(candidate), 40, '0'),
                int8multirange(int8range(10, 21, '[)'))
            FROM generate_series(1, 10000) candidate
            "#,
        ))
        .bind(FAMILY)
        .execute(guard.connection_mut())
        .await?;

        let first = load_stored_lineage_coverage_candidate_delta_page(
            guard.connection_mut(),
            CHAIN,
            &[],
            true,
            None,
            37,
        )
        .await?;
        assert_eq!(first.requirements.len(), 37);
        let second = load_stored_lineage_coverage_candidate_delta_page(
            guard.connection_mut(),
            CHAIN,
            &[],
            true,
            first.next_cursor.as_ref(),
            37,
        )
        .await?;
        assert_eq!(second.requirements.len(), 37);
        assert_ne!(first.requirements, second.requirements);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!(
                "SELECT COUNT(*)::BIGINT FROM pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}"
            ))
            .fetch_one(guard.connection_mut())
            .await?,
            10_000,
            "the full candidate stays server-side while each returned delta is bounded"
        );
        drop(guard);

        database.cleanup().await
    }
}
