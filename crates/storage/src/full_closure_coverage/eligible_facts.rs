/// Common proof-authority relation for full-closure coverage aggregates.
///
/// Bindings are fixed across every caller:
/// $1 chain, $2 retention generation, $3 raw-log revision,
/// $4 block-revision evidence floor, and $5 current topic map.
const ELIGIBLE_FACTS_CTE_TEMPLATE: &str = r#"
WITH current_topics AS (
    SELECT
        topic_family.key AS source_family,
        ARRAY(
            SELECT DISTINCT LOWER(topic0)
            FROM jsonb_array_elements_text(topic_family.value) AS topic(topic0)
            ORDER BY 1
        ) AS topic0s
    FROM jsonb_each($5::JSONB) AS topic_family
    WHERE jsonb_typeof(topic_family.value) = 'array'
),
fact_candidates AS (
    SELECT
        fact.backfill_coverage_fact_id,
        fact.chain_id,
        fact.source_family,
        fact.scope,
        fact.address,
        fact.covered_from_block,
        fact.covered_to_block,
        job.source_identity,
        current_topics.topic0s AS current_topic0s
    FROM backfill_coverage_facts fact
    JOIN backfill_jobs job
      ON job.backfill_job_id = fact.backfill_job_id
    LEFT JOIN current_topics
      ON current_topics.source_family = fact.source_family
    WHERE fact.chain_id = $1
      AND (__FACT_FILTER__)
      AND job.chain_id = fact.chain_id
      AND job.status = 'completed'::backfill_lifecycle_status
      AND job.raw_log_retention_generation = $2
      AND (
          job.stored_verification_raw_log_input_revision IS NULL
          OR (
              job.stored_verification_from_block <= fact.covered_from_block
              AND job.stored_verification_to_block >= fact.covered_to_block
              AND job.stored_verification_raw_log_input_revision >= $4
              AND job.stored_verification_raw_log_input_revision <= $3
              AND NOT EXISTS (
                  SELECT 1
                  FROM raw_log_staging_block_revisions changed
                  WHERE changed.chain_id = fact.chain_id
                    AND changed.revision
                        > job.stored_verification_raw_log_input_revision
                    AND changed.revision <= $3
                    AND changed.block_number BETWEEN
                        fact.covered_from_block AND fact.covered_to_block
              )
          )
      )
      AND fact.covered_from_block >= job.range_start_block_number
      AND fact.covered_to_block <= job.range_end_block_number
),
persisted_maps AS (
    SELECT
        candidate.*,
        CASE
            WHEN jsonb_typeof(
                source_identity #> '{coinbase_sql_topic_plan,topic0s_by_source_family}'
            ) = 'object'
                THEN source_identity #> '{coinbase_sql_topic_plan,topic0s_by_source_family}'
            WHEN jsonb_typeof(source_identity -> 'topic0s_by_source_family') = 'object'
                THEN source_identity -> 'topic0s_by_source_family'
            ELSE NULL
        END AS persisted_map
    FROM fact_candidates candidate
),
normalized_topics AS (
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
                        source_identity
                            #> '{coinbase_sql_topic_plan,source_families_without_topics}'
                    ) = 'array'
                        THEN source_identity
                            #> '{coinbase_sql_topic_plan,source_families_without_topics}'
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
),
eligible_coverage_facts AS (
    SELECT
        backfill_coverage_fact_id,
        chain_id,
        source_family,
        scope,
        address,
        covered_from_block,
        covered_to_block
    FROM normalized_topics
    WHERE current_topic0s IS NULL
       -- This drift-eligibility CASE is duplicated in
       -- backfill_jobs/topic_evidence.rs; both must change together.
       OR CASE
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
          END
)
"#;

pub(super) fn eligible_facts_cte(fact_filter: &str) -> String {
    ELIGIBLE_FACTS_CTE_TEMPLATE.replace("__FACT_FILTER__", fact_filter)
}
