use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, Row};

const TOPIC_EVIDENCE_TABLE: &str = "stored_lineage_backfill_topic_evidence";
pub const MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillTopicCoverageRequirement {
    pub source_family: String,
    pub address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillTopicCoverageViolation {
    pub source_family: String,
    pub address: String,
    pub required_from_block: i64,
    pub required_to_block: i64,
    pub backfill_job_id: i64,
    pub persisted_topic_count: Option<i64>,
    pub current_topic_count: i64,
}

/// Parse topic provenance once for the completed jobs that can contribute
/// facts inside this proof. Page checks subsequently join this compact
/// transaction-local relation and never transfer or reparse `source_identity`.
pub async fn materialize_completed_backfill_topic_evidence(
    connection: &mut PgConnection,
    chain: &str,
    from_block: i64,
    to_block: i64,
    current_topic0s_by_family: &BTreeMap<String, Vec<String>>,
    retention_generation: Option<i64>,
) -> Result<i64> {
    ensure!(
        !chain.trim().is_empty(),
        "topic-evidence chain must not be empty"
    );
    ensure!(
        from_block >= 0,
        "topic-evidence lower bound must not be negative"
    );
    ensure!(
        to_block >= from_block,
        "topic-evidence bounds must not be inverted"
    );
    ensure!(
        retention_generation.is_none_or(|generation| generation >= 0),
        "topic-evidence retention generation must not be negative"
    );
    let topics = serde_json::to_value(current_topic0s_by_family)
        .context("failed to encode current topic evidence")?;

    sqlx::query(&format!(
        r#"
        CREATE TEMP TABLE {TOPIC_EVIDENCE_TABLE} (
            backfill_job_id BIGINT NOT NULL,
            source_family TEXT NOT NULL,
            is_current_or_unfiltered BOOLEAN NOT NULL,
            persisted_topic_count BIGINT,
            current_topic_count BIGINT NOT NULL,
            PRIMARY KEY (backfill_job_id, source_family)
        ) ON COMMIT DROP
        "#
    ))
    .execute(&mut *connection)
    .await
    .context("failed to create transaction-local backfill topic evidence")?;

    let result = sqlx::query(&format!(
        r#"
        WITH current_topics AS (
            SELECT
                topic_family.key AS source_family,
                ARRAY(
                    SELECT DISTINCT LOWER(topic0)
                    FROM jsonb_array_elements_text(topic_family.value) AS topic(topic0)
                    ORDER BY 1
                ) AS topic0s
            FROM jsonb_each($4::JSONB) AS topic_family
            WHERE jsonb_typeof(topic_family.value) = 'array'
        ),
        relevant_job_families AS (
            SELECT DISTINCT
                job.backfill_job_id,
                fact.source_family,
                current_topics.topic0s AS current_topic0s
            FROM backfill_coverage_facts fact
            JOIN backfill_jobs job
              ON job.backfill_job_id = fact.backfill_job_id
            JOIN current_topics
              ON current_topics.source_family = fact.source_family
            WHERE fact.chain_id = $1
              AND job.chain_id = fact.chain_id
              AND job.status = 'completed'::backfill_lifecycle_status
              AND ($5::BIGINT IS NULL OR job.raw_log_retention_generation = $5)
              AND fact.covered_from_block >= job.range_start_block_number
              AND fact.covered_to_block <= job.range_end_block_number
              AND fact.covered_from_block <= $3
              AND fact.covered_to_block >= $2
        ),
        job_identities AS (
            SELECT
                relevant.*,
                job.source_identity
            FROM relevant_job_families relevant
            JOIN backfill_jobs job
              ON job.backfill_job_id = relevant.backfill_job_id
        ),
        persisted_maps AS (
            SELECT
                relevant.*,
                CASE
                    WHEN jsonb_typeof(
                        source_identity #> '{{coinbase_sql_topic_plan,topic0s_by_source_family}}'
                    ) = 'object'
                        THEN source_identity #> '{{coinbase_sql_topic_plan,topic0s_by_source_family}}'
                    WHEN jsonb_typeof(source_identity -> 'topic0s_by_source_family') = 'object'
                        THEN source_identity -> 'topic0s_by_source_family'
                    ELSE NULL
                END AS persisted_map
            FROM job_identities relevant
        ),
        normalized AS (
            SELECT
                persisted_maps.*,
                COALESCE(persisted.topic0s, ARRAY[]::TEXT[]) AS persisted_topic0s,
                EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(source_identity -> 'generic_topic_scans') = 'array'
                                THEN source_identity -> 'generic_topic_scans'
                            ELSE '[]'::JSONB
                        END
                    ) AS scan
                    WHERE scan ->> 'source_family' = persisted_maps.source_family
                ) AS has_legacy_generic_scan,
                EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements_text(
                        CASE
                            WHEN jsonb_typeof(
                                source_identity #> '{{coinbase_sql_topic_plan,source_families_without_topics}}'
                            ) = 'array'
                                THEN source_identity #> '{{coinbase_sql_topic_plan,source_families_without_topics}}'
                            ELSE '[]'::JSONB
                        END
                    ) AS unfiltered(family)
                    WHERE unfiltered.family = persisted_maps.source_family
                ) AS is_declared_topic_unfiltered
            FROM persisted_maps
            LEFT JOIN LATERAL (
                SELECT ARRAY_AGG(DISTINCT LOWER(topic0) ORDER BY LOWER(topic0)) AS topic0s
                FROM jsonb_array_elements_text(
                    CASE
                        WHEN jsonb_typeof(persisted_map -> source_family) = 'array'
                            THEN persisted_map -> source_family
                        ELSE '[]'::JSONB
                    END
                ) AS topic(topic0)
            ) persisted ON TRUE
        )
        INSERT INTO {TOPIC_EVIDENCE_TABLE} (
            backfill_job_id,
            source_family,
            is_current_or_unfiltered,
            persisted_topic_count,
            current_topic_count
        )
        SELECT
            backfill_job_id,
            source_family,
            CASE
                WHEN persisted_map ? source_family
                    THEN persisted_topic0s = current_topic0s
                WHEN is_declared_topic_unfiltered
                    THEN TRUE
                WHEN (
                      source_identity ? 'coinbase_sql_topic_plan'
                      OR (
                          source_identity ->> 'source_identity_payload_format' IN (
                              'generic_resolver_event_topics_v1',
                              'basenames_registry_scan_all_topics_v1'
                          )
                          AND source_identity ->> 'source_family' = source_family
                      )
                      OR has_legacy_generic_scan
                  )
                    THEN FALSE
                ELSE TRUE
            END,
            CASE
                WHEN persisted_map ? source_family
                  AND persisted_topic0s <> current_topic0s
                    THEN CARDINALITY(persisted_topic0s)::BIGINT
                ELSE NULL
            END,
            CARDINALITY(current_topic0s)::BIGINT
        FROM normalized
        "#
    ))
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(topics)
    .bind(retention_generation)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to materialize completed backfill topic evidence for {chain} over {from_block}..={to_block}"
        )
    })?;
    Ok(i64::try_from(result.rows_affected()).unwrap_or(i64::MAX))
}

/// Return at most one stale contributing job per requested tuple. The SQL
/// aggregates fact intervals before returning, so transfer is bounded by the
/// caller's requirement page rather than completed-job history.
pub async fn find_backfill_topic_coverage_violations(
    connection: &mut PgConnection,
    chain: &str,
    requirements: &[BackfillTopicCoverageRequirement],
    limit: i64,
) -> Result<Vec<BackfillTopicCoverageViolation>> {
    ensure!(
        requirements.len() <= MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS,
        "topic-evidence page exceeds {} requirements",
        MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS
    );
    ensure!(limit > 0, "topic-evidence violation limit must be positive");
    ensure!(
        limit as usize <= MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS,
        "topic-evidence violation limit exceeds {}",
        MAX_BACKFILL_TOPIC_EVIDENCE_REQUIREMENTS
    );
    if requirements.is_empty() {
        return Ok(Vec::new());
    }
    let source_families = requirements
        .iter()
        .map(|requirement| requirement.source_family.clone())
        .collect::<Vec<_>>();
    let addresses = requirements
        .iter()
        .map(|requirement| requirement.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_from_block)
        .collect::<Vec<_>>();
    let to_blocks = requirements
        .iter()
        .map(|requirement| requirement.required_to_block)
        .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"
        WITH requirements AS (
            SELECT *
            FROM UNNEST(
                $2::TEXT[],
                $3::TEXT[],
                $4::BIGINT[],
                $5::BIGINT[]
            ) WITH ORDINALITY AS required(
                source_family,
                address,
                required_from_block,
                required_to_block,
                requirement_ordinal
            )
        ),
        applicable AS (
            SELECT
                required.*,
                fact.backfill_job_id,
                GREATEST(fact.covered_from_block, required.required_from_block)
                    AS contributed_from_block,
                LEAST(fact.covered_to_block, required.required_to_block)
                    AS contributed_to_block,
                evidence.is_current_or_unfiltered,
                evidence.persisted_topic_count,
                evidence.current_topic_count
            FROM requirements required
            JOIN backfill_coverage_facts fact
              ON fact.chain_id = $1
             AND fact.source_family = required.source_family
             AND fact.covered_from_block <= required.required_to_block
             AND fact.covered_to_block >= required.required_from_block
             AND (
                 (
                     fact.scope = 'family'
                     AND fact.address IS NULL
                 )
                 OR (
                     fact.scope = 'address'
                     AND fact.address = required.address
                 )
             )
            JOIN backfill_jobs fact_job
              ON fact_job.backfill_job_id = fact.backfill_job_id
             AND fact_job.chain_id = fact.chain_id
             AND fact_job.status = 'completed'::backfill_lifecycle_status
             AND fact.covered_from_block >= fact_job.range_start_block_number
             AND fact.covered_to_block <= fact_job.range_end_block_number
            JOIN {TOPIC_EVIDENCE_TABLE} evidence
              ON evidence.backfill_job_id = fact.backfill_job_id
             AND evidence.source_family = fact.source_family
        ),
        coverage AS (
            SELECT
                requirement_ordinal,
                source_family,
                address,
                required_from_block,
                required_to_block,
                range_agg(
                    int8range(contributed_from_block, contributed_to_block + 1, '[)')
                ) AS all_coverage,
                COALESCE(
                    range_agg(
                        int8range(contributed_from_block, contributed_to_block + 1, '[)')
                    ) FILTER (WHERE is_current_or_unfiltered),
                    '{{}}'::INT8MULTIRANGE
                ) AS current_coverage
            FROM applicable
            GROUP BY
                requirement_ordinal,
                source_family,
                address,
                required_from_block,
                required_to_block
        ),
        stale_requirements AS (
            SELECT *
            FROM coverage
            WHERE all_coverage @> int8range(
                    required_from_block,
                    required_to_block + 1,
                    '[)'
                )
              AND NOT current_coverage @> int8range(
                    required_from_block,
                    required_to_block + 1,
                    '[)'
                )
        )
        SELECT
            stale.source_family,
            stale.address,
            stale.required_from_block,
            stale.required_to_block,
            contributing.backfill_job_id,
            contributing.persisted_topic_count,
            contributing.current_topic_count
        FROM stale_requirements stale
        JOIN LATERAL (
            SELECT
                fact.backfill_job_id,
                fact.persisted_topic_count,
                fact.current_topic_count
            FROM applicable fact
            WHERE fact.requirement_ordinal = stale.requirement_ordinal
              AND NOT fact.is_current_or_unfiltered
              AND NOT stale.current_coverage @> int8range(
                    fact.contributed_from_block,
                    fact.contributed_to_block + 1,
                    '[)'
                )
            ORDER BY fact.backfill_job_id
            LIMIT 1
        ) contributing ON TRUE
        ORDER BY stale.requirement_ordinal
        LIMIT $6
        "#
    ))
    .bind(chain)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(limit)
    .fetch_all(connection)
    .await
    .with_context(|| {
        format!(
            "failed to find bounded backfill topic-evidence violations for {} requirements on {chain}",
            requirements.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(BackfillTopicCoverageViolation {
                source_family: row.try_get("source_family")?,
                address: row.try_get("address")?,
                required_from_block: row.try_get("required_from_block")?,
                required_to_block: row.try_get("required_to_block")?,
                backfill_job_id: row.try_get("backfill_job_id")?,
                persisted_topic_count: row.try_get("persisted_topic_count")?,
                current_topic_count: row.try_get("current_topic_count")?,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "topic_evidence/tests.rs"]
mod tests;
