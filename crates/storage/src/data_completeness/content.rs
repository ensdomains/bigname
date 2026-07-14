use anyhow::{Context, Result};

use super::{
    ActiveManifestEventSource, BackfillLifecycleRow, DEFERRED_NORMALIZED_EVENT_INDEXES,
    ManifestChainNamespace, NameCurrentCount, ObservedCodeAddress, ProjectionReplayMarker,
};

pub(super) async fn load_observed_code_addresses(
    pool: &sqlx::PgPool,
) -> Result<Vec<ObservedCodeAddress>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            lower(contract_address) AS address,
            MAX(block_number) AS max_observed_block_number
        FROM raw_code_hashes
        WHERE canonicality_state <> 'orphaned'::canonicality_state
        GROUP BY chain_id, lower(contract_address)
        ORDER BY chain_id, address
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load observed code-hash addresses")?;

    rows.into_iter()
        .map(|row| {
            Ok(ObservedCodeAddress {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                address: crate::sql_row::get(&row, "address")?,
                max_observed_block_number: crate::sql_row::get(&row, "max_observed_block_number")?,
            })
        })
        .collect()
}

pub(super) async fn load_active_manifest_event_sources(
    pool: &sqlx::PgPool,
) -> Result<Vec<ActiveManifestEventSource>> {
    let rows = sqlx::query(
        r#"
        WITH active_event_sources AS (
            SELECT
                manifest.manifest_id,
                manifest.manifest_version,
                manifest.chain,
                manifest.namespace,
                manifest.source_family,
                ARRAY_AGG(DISTINCT normalized_kind.event_kind) AS normalized_event_kinds
            FROM manifest_versions manifest
            CROSS JOIN LATERAL jsonb_array_elements(
                COALESCE(manifest.manifest_payload #> '{abi,events}', '[]'::JSONB)
            ) abi_event
            CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(abi_event -> 'normalized_events', '[]'::JSONB)
            ) normalized_kind(event_kind)
            WHERE manifest.rollout_status = 'active'
            GROUP BY
                manifest.manifest_id,
                manifest.manifest_version,
                manifest.chain,
                manifest.namespace,
                manifest.source_family
        )
        SELECT
            source.manifest_id,
            source.manifest_version,
            source.chain,
            source.namespace,
            source.source_family,
            COUNT(event.normalized_event_id)::BIGINT AS normalized_event_count,
            COUNT(event.normalized_event_id) FILTER (
                WHERE event.raw_fact_ref ->> 'kind' = 'raw_log'
                  AND raw.raw_log_id IS NULL
            )::BIGINT AS normalized_events_missing_canonical_raw_log_count
        FROM active_event_sources source
        LEFT JOIN normalized_events event
          ON event.source_manifest_id = source.manifest_id
         AND event.manifest_version = source.manifest_version
         AND event.chain_id = source.chain
         AND event.namespace = source.namespace
         AND event.source_family = source.source_family
         AND event.event_kind = ANY(source.normalized_event_kinds)
         AND event.canonicality_state <> 'orphaned'::canonicality_state
        LEFT JOIN raw_logs raw
          ON raw.chain_id = event.chain_id
         AND raw.block_hash = event.block_hash
         AND raw.transaction_hash = event.transaction_hash
         AND raw.log_index = event.log_index
         AND raw.canonicality_state IN (
             'canonical'::canonicality_state,
             'safe'::canonicality_state,
             'finalized'::canonicality_state
         )
        GROUP BY
            source.manifest_id,
            source.manifest_version,
            source.chain,
            source.namespace,
            source.source_family
        ORDER BY
            source.chain,
            source.namespace,
            source.source_family,
            source.manifest_version,
            source.manifest_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest event-source counts")?;

    rows.into_iter()
        .map(|row| {
            Ok(ActiveManifestEventSource {
                manifest_id: crate::sql_row::get(&row, "manifest_id")?,
                manifest_version: crate::sql_row::get(&row, "manifest_version")?,
                chain: crate::sql_row::get(&row, "chain")?,
                namespace: crate::sql_row::get(&row, "namespace")?,
                source_family: crate::sql_row::get(&row, "source_family")?,
                normalized_event_count: crate::sql_row::get(&row, "normalized_event_count")?,
                normalized_events_missing_canonical_raw_log_count: crate::sql_row::get(
                    &row,
                    "normalized_events_missing_canonical_raw_log_count",
                )?,
            })
        })
        .collect()
}

pub(super) async fn count_table(pool: &sqlx::PgPool, table: &'static str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::BIGINT FROM {table}"))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {table}"))
}

pub(super) async fn load_name_current_counts(pool: &sqlx::PgPool) -> Result<Vec<NameCurrentCount>> {
    let rows = sqlx::query(
        r#"
        SELECT namespace, COUNT(*)::BIGINT AS count
        FROM name_current
        GROUP BY namespace
        ORDER BY namespace
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load name-current counts")?;

    rows.into_iter()
        .map(|row| {
            Ok(NameCurrentCount {
                namespace: crate::sql_row::get(&row, "namespace")?,
                count: crate::sql_row::get(&row, "count")?,
            })
        })
        .collect()
}

pub(super) async fn load_normalized_events_null_chain_id_count(pool: &sqlx::PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE chain_id IS NULL
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to count normalized events with a null chain id")
}

pub(super) async fn load_projection_replay_markers(
    pool: &sqlx::PgPool,
) -> Result<Vec<ProjectionReplayMarker>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            replay_version,
            projection,
            completed_normalized_target_block
        FROM current_projection_replay_status
        ORDER BY replay_version, projection
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load current projection replay markers")?;

    rows.into_iter()
        .map(|row| {
            Ok(ProjectionReplayMarker {
                replay_version: crate::sql_row::get(&row, "replay_version")?,
                projection: crate::sql_row::get(&row, "projection")?,
                completed_normalized_target_block: crate::sql_row::get(
                    &row,
                    "completed_normalized_target_block",
                )?,
            })
        })
        .collect()
}

/// Mirrors the target passed by automatic projection bootstrap: the greater of the raw-fact
/// normalized replay target and every chain checkpoint's furthest canonicality frontier.
pub(super) async fn load_projection_replay_required_target_block(
    pool: &sqlx::PgPool,
) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT NULLIF(MAX(target_block), -1)
        FROM (
            SELECT COALESCE(MAX(target_block_number), -1) AS target_block
            FROM normalized_replay_cursors
            WHERE cursor_kind = 'raw_fact_normalized_events'

            UNION ALL

            SELECT COALESCE(MAX(GREATEST(
                COALESCE(canonical_block_number, -1),
                COALESCE(safe_block_number, -1),
                COALESCE(finalized_block_number, -1)
            )), -1) AS target_block
            FROM chain_checkpoints
        ) bootstrap_targets
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load required projection replay target block")
}

pub(super) async fn load_backfill_lifecycle(
    pool: &sqlx::PgPool,
) -> Result<Vec<BackfillLifecycleRow>> {
    let rows = sqlx::query(
        r#"
        WITH profiles AS (
            SELECT DISTINCT deployment_profile FROM backfill_jobs
        ),
        failed_jobs AS (
            SELECT deployment_profile, COUNT(*) AS failed_job_count
            FROM backfill_jobs
            WHERE status = 'failed'
            GROUP BY deployment_profile
        ),
        ranges AS (
            SELECT
                job.deployment_profile,
                COUNT(*) FILTER (WHERE r.status = 'failed') AS failed_range_count,
                COUNT(*) FILTER (WHERE r.status IN ('pending', 'reserved', 'running'))
                    AS incomplete_range_count,
                COUNT(*) FILTER (
                    WHERE r.status IN ('reserved', 'running')
                      AND r.lease_expires_at IS NOT NULL
                      AND r.lease_expires_at < now()
                ) AS expired_lease_range_count
            FROM backfill_ranges r
            JOIN backfill_jobs job ON job.backfill_job_id = r.backfill_job_id
            GROUP BY job.deployment_profile
        )
        SELECT
            profiles.deployment_profile,
            COALESCE(failed_jobs.failed_job_count, 0)::BIGINT AS failed_job_count,
            COALESCE(ranges.failed_range_count, 0)::BIGINT AS failed_range_count,
            COALESCE(ranges.incomplete_range_count, 0)::BIGINT AS incomplete_range_count,
            COALESCE(ranges.expired_lease_range_count, 0)::BIGINT AS expired_lease_range_count
        FROM profiles
        LEFT JOIN failed_jobs ON failed_jobs.deployment_profile = profiles.deployment_profile
        LEFT JOIN ranges ON ranges.deployment_profile = profiles.deployment_profile
        ORDER BY profiles.deployment_profile
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load backfill lifecycle counts")?;

    rows.into_iter()
        .map(|row| {
            Ok(BackfillLifecycleRow {
                deployment_profile: crate::sql_row::get(&row, "deployment_profile")?,
                failed_job_count: crate::sql_row::get(&row, "failed_job_count")?,
                failed_range_count: crate::sql_row::get(&row, "failed_range_count")?,
                incomplete_range_count: crate::sql_row::get(&row, "incomplete_range_count")?,
                expired_lease_range_count: crate::sql_row::get(&row, "expired_lease_range_count")?,
            })
        })
        .collect()
}

pub(super) async fn load_present_deferred_projection_indexes(
    pool: &sqlx::PgPool,
) -> Result<Vec<String>> {
    let expected = DEFERRED_NORMALIZED_EVENT_INDEXES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT index_relation.relname
        FROM pg_index index_state
        JOIN pg_class index_relation ON index_relation.oid = index_state.indexrelid
        JOIN pg_class table_relation ON table_relation.oid = index_state.indrelid
        JOIN pg_namespace table_namespace ON table_namespace.oid = table_relation.relnamespace
        WHERE table_namespace.nspname = 'public'
          AND table_relation.relname = 'normalized_events'
          AND index_relation.relname = ANY($1::TEXT[])
          AND index_state.indisvalid
          AND index_state.indisready
        ORDER BY index_relation.relname
        "#,
    )
    .bind(&expected)
    .fetch_all(pool)
    .await
    .context("failed to load present deferred projection indexes")
}

pub(super) async fn load_manifest_chain_namespaces(
    pool: &sqlx::PgPool,
) -> Result<Vec<ManifestChainNamespace>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT chain, namespace
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY chain, namespace
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest chain namespaces")?;

    rows.into_iter()
        .map(|row| {
            Ok(ManifestChainNamespace {
                chain: crate::sql_row::get(&row, "chain")?,
                namespace: crate::sql_row::get(&row, "namespace")?,
            })
        })
        .collect()
}
