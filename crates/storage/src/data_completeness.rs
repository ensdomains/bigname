use anyhow::{Context, Result};

#[cfg(test)]
mod tests;

/// Per-chain intake frontiers.
///
/// `lineage_canonical_block_count` counts distinct non-orphaned block numbers, so a
/// contiguous lineage satisfies `count == head - floor + 1`.
///
/// `canonical_raw_log_head_block_number` is the head of the raw logs that normalized
/// replay is eligible to consume: it mirrors the replay bounds, which require both the
/// raw log and its lineage block to be canonical, safe, or finalized. The gate compares
/// replay progress against this head. `raw_log_head_block_number` is the non-orphaned
/// head including `observed` logs and is reported only, so a candidate whose newest logs
/// have not yet been promoted to canonical is not measured as lagging against them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainCompletenessRow {
    pub chain_id: String,
    pub canonical_block_number: Option<i64>,
    pub lineage_head_block_number: Option<i64>,
    pub lineage_floor_block_number: Option<i64>,
    pub lineage_canonical_block_count: i64,
    /// Block heights carrying more than one non-orphaned canonical/safe/finalized lineage
    /// row. The distinct-block-number contiguity count cannot see these, so they are counted
    /// separately; a non-zero value is a canonicality violation, not a gap.
    pub duplicate_canonical_height_count: i64,
    /// Canonical/safe/finalized lineage rows above the chain's canonical floor whose
    /// `parent_hash` matches no canonical row at the preceding height. The height-only
    /// contiguity count is blind to a branch that is complete by height but broken by hash, so
    /// a disconnected canonical branch is counted separately.
    pub disconnected_canonical_parent_count: i64,
    pub canonical_raw_log_head_block_number: Option<i64>,
    pub raw_log_head_block_number: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayCursorRow {
    pub deployment_profile: String,
    pub chain_id: String,
    pub cursor_kind: String,
    /// The completion authority. Replay is complete for a cursor's target when
    /// `next_block_number > target_block_number`; a reorg rewind lowers `next_block_number`
    /// but leaves `last_completed_block_number` at its high-water mark, so the gate reads the
    /// `next`/`target` pair and treats `last_completed_block_number` as reporting detail only.
    pub next_block_number: Option<i64>,
    pub target_block_number: Option<i64>,
    pub last_completed_block_number: Option<i64>,
    pub last_failure_reason: Option<String>,
}

/// A completed `current_projection_replay_status` marker for one `(replay_version, projection)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionReplayMarker {
    pub replay_version: i32,
    pub projection: String,
}

/// Backfill lifecycle counts, scoped by deployment profile. Reported as an advisory rather
/// than gated: without coverage-fact reconciliation a `failed` range cannot be distinguished
/// from one superseded by a later successful retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillLifecycleRow {
    pub deployment_profile: String,
    pub failed_job_count: i64,
    pub failed_range_count: i64,
    pub incomplete_range_count: i64,
    pub expired_lease_range_count: i64,
}

/// A `(chain, namespace)` an active manifest version that declares normalized-event outputs
/// carries. The expected content set is derived from these rather than from observed events, so
/// a chain declared to produce a namespace with no rows is not masked by a namespace that does
/// have rows. Execution/transport manifests that declare no event outputs are excluded, since
/// they produce no normalized events to require.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManifestChainNamespace {
    pub chain: String,
    pub namespace: String,
}

/// An active manifest-declared contract instance whose `contract_instance_addresses` row is
/// missing or deactivated. `load_watched_contracts` reads a target's address from that row, so a
/// declared instance without one is dropped from the watch view entirely instead of surfacing
/// as unobserved; it is loaded directly here so the gate can fail on it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManifestDeclaredTarget {
    pub chain: String,
    pub address: String,
    pub source_family: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplyCursorRow {
    pub cursor_name: String,
    pub last_change_id: i64,
}

/// A `(chain_id, lowercased address)` pair with at least one non-orphaned code observation,
/// carrying the highest block at which it was observed. Coverage requires an observation within
/// a target's active range, so a pre-admission observation of the same address does not satisfy
/// a target whose declared start is later.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObservedCodeAddress {
    pub chain_id: String,
    pub address: String,
    pub observed_block_number: i64,
}

/// Non-empty `normalized_events` count for one `(chain_id, namespace)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedEventCount {
    pub chain_id: String,
    pub namespace: String,
    pub count: i64,
}

/// Non-empty `name_current` count for one namespace. `name_current` carries no chain, so a
/// namespace is the finest dimension a name projection can be scoped to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentCount {
    pub namespace: String,
    pub count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataCompletenessRead {
    pub chains: Vec<ChainCompletenessRow>,
    pub replay_cursors: Vec<ReplayCursorRow>,
    pub projection_apply_cursors: Vec<ProjectionApplyCursorRow>,
    /// `MAX(change_id)` over `projection_normalized_event_changes`, loaded independently of
    /// the apply cursors so an absent cursor with a non-empty change log is detectable.
    pub max_projection_change_id: Option<i64>,
    /// Rows still queued in `projection_invalidations`. A successful apply deletes the row,
    /// so a fully applied projection queue is empty; a non-zero count is pending work.
    pub pending_projection_invalidation_count: i64,
    /// Rows in `projection_invalidation_dead_letters`: invalidations that exhausted their
    /// retries. A non-zero count is a terminal projection failure.
    pub projection_invalidation_dead_letter_count: i64,
    pub observed_code_addresses: Vec<ObservedCodeAddress>,
    /// Non-orphaned `normalized_events` counts grouped by `(chain_id, namespace)`, so content
    /// can be required per active chain instead of globally where another chain's rows would
    /// satisfy a total. NULL `chain_id` rows are excluded here and counted separately.
    pub normalized_event_counts: Vec<NormalizedEventCount>,
    /// `name_current` counts grouped by namespace.
    pub name_current_counts: Vec<NameCurrentCount>,
    /// Non-orphaned `normalized_events` rows with a NULL `chain_id` — a data-integrity fault
    /// that would otherwise abort the per-chain read.
    pub normalized_events_null_chain_id_count: i64,
    /// Completed current-projection replay markers. The gate requires all current projections
    /// present at the newest replay version, matching the worker's bootstrap handoff.
    pub projection_replay_markers: Vec<ProjectionReplayMarker>,
    /// Per-profile backfill lifecycle counts (advisory).
    pub backfill_lifecycle: Vec<BackfillLifecycleRow>,
    /// Deferred `normalized_events` projection indexes that currently exist. A fresh replay
    /// drops them and a later pass rebuilds them, so an absent index marks a mid-replay
    /// candidate.
    pub present_deferred_projection_indexes: Vec<String>,
    /// `(chain, namespace)` declared by active event-producing manifest versions — the expected
    /// content set.
    pub manifest_chain_namespaces: Vec<ManifestChainNamespace>,
    /// Active manifest-declared contract instances with no live `contract_instance_addresses`
    /// row, so the watch view cannot surface them. A non-empty list is a coverage-authority gap.
    pub manifest_declared_targets_missing_address: Vec<ManifestDeclaredTarget>,
}

/// The deferred `normalized_events` projection indexes, owned by the replay drop/rebuild path
/// in `apps/indexer/src/main/normalized_replay_catchup/indexes.rs`.
pub const DEFERRED_NORMALIZED_EVENT_INDEXES: &[&str] = &[
    "normalized_events_namespace_idx",
    "normalized_events_kind_idx",
    "normalized_events_manifest_idx",
    "normalized_events_chain_position_idx",
    "normalized_events_name_projection_replay_idx",
    "normalized_events_resource_projection_replay_idx",
    "normalized_events_name_relevant_projection_idx",
    "normalized_events_record_inventory_resource_replay_idx",
];

pub async fn load_data_completeness(pool: &sqlx::PgPool) -> Result<DataCompletenessRead> {
    Ok(DataCompletenessRead {
        chains: load_chain_completeness(pool).await?,
        replay_cursors: load_replay_cursors(pool).await?,
        projection_apply_cursors: load_projection_apply_cursors(pool).await?,
        max_projection_change_id: load_max_projection_change_id(pool).await?,
        pending_projection_invalidation_count: count_table(pool, "projection_invalidations")
            .await?,
        projection_invalidation_dead_letter_count: count_table(
            pool,
            "projection_invalidation_dead_letters",
        )
        .await?,
        observed_code_addresses: load_observed_code_addresses(pool).await?,
        normalized_event_counts: load_normalized_event_counts(pool).await?,
        name_current_counts: load_name_current_counts(pool).await?,
        normalized_events_null_chain_id_count: load_normalized_events_null_chain_id_count(pool)
            .await?,
        projection_replay_markers: load_projection_replay_markers(pool).await?,
        backfill_lifecycle: load_backfill_lifecycle(pool).await?,
        present_deferred_projection_indexes: load_present_deferred_projection_indexes(pool).await?,
        manifest_chain_namespaces: load_manifest_chain_namespaces(pool).await?,
        manifest_declared_targets_missing_address: load_manifest_declared_targets_missing_address(
            pool,
        )
        .await?,
    })
}

async fn load_chain_completeness(pool: &sqlx::PgPool) -> Result<Vec<ChainCompletenessRow>> {
    let rows = sqlx::query(
        r#"
        WITH known_chains AS (
            SELECT chain_id FROM chain_checkpoints
            UNION
            SELECT DISTINCT chain_id FROM chain_lineage
        ),
        lineage AS (
            SELECT
                chain_id,
                MAX(block_number) AS lineage_head_block_number,
                MIN(block_number) AS lineage_floor_block_number,
                COUNT(DISTINCT block_number) AS lineage_canonical_block_count
            FROM chain_lineage
            WHERE canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            GROUP BY chain_id
        ),
        canonical_raw_log_head AS (
            SELECT
                raw_logs.chain_id,
                MAX(raw_logs.block_number) AS canonical_raw_log_head_block_number
            FROM raw_logs
            JOIN chain_lineage
              ON chain_lineage.chain_id = raw_logs.chain_id
             AND chain_lineage.block_hash = raw_logs.block_hash
            WHERE raw_logs.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
              AND chain_lineage.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            GROUP BY raw_logs.chain_id
        ),
        raw_log_head AS (
            SELECT chain_id, MAX(block_number) AS raw_log_head_block_number
            FROM raw_logs
            WHERE canonicality_state <> 'orphaned'::canonicality_state
            GROUP BY chain_id
        ),
        duplicate_canonical_height AS (
            SELECT chain_id, COUNT(*) AS duplicate_canonical_height_count
            FROM (
                SELECT chain_id, block_number
                FROM chain_lineage
                WHERE canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                GROUP BY chain_id, block_number
                HAVING COUNT(*) > 1
            ) duplicated
            GROUP BY chain_id
        ),
        disconnected_canonical_parent AS (
            SELECT cur.chain_id, COUNT(*) AS disconnected_canonical_parent_count
            FROM chain_lineage cur
            WHERE cur.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
              AND EXISTS (
                SELECT 1 FROM chain_lineage prev
                WHERE prev.chain_id = cur.chain_id
                  AND prev.block_number = cur.block_number - 1
                  AND prev.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM chain_lineage prev
                WHERE prev.chain_id = cur.chain_id
                  AND prev.block_number = cur.block_number - 1
                  AND prev.block_hash = cur.parent_hash
                  AND prev.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              )
            GROUP BY cur.chain_id
        )
        SELECT
            known_chains.chain_id,
            chain_checkpoints.canonical_block_number,
            lineage.lineage_head_block_number,
            lineage.lineage_floor_block_number,
            COALESCE(lineage.lineage_canonical_block_count, 0) AS lineage_canonical_block_count,
            COALESCE(duplicate_canonical_height.duplicate_canonical_height_count, 0)
                AS duplicate_canonical_height_count,
            COALESCE(disconnected_canonical_parent.disconnected_canonical_parent_count, 0)
                AS disconnected_canonical_parent_count,
            canonical_raw_log_head.canonical_raw_log_head_block_number,
            raw_log_head.raw_log_head_block_number
        FROM known_chains
        LEFT JOIN chain_checkpoints ON chain_checkpoints.chain_id = known_chains.chain_id
        LEFT JOIN lineage ON lineage.chain_id = known_chains.chain_id
        LEFT JOIN duplicate_canonical_height
          ON duplicate_canonical_height.chain_id = known_chains.chain_id
        LEFT JOIN disconnected_canonical_parent
          ON disconnected_canonical_parent.chain_id = known_chains.chain_id
        LEFT JOIN canonical_raw_log_head
          ON canonical_raw_log_head.chain_id = known_chains.chain_id
        LEFT JOIN raw_log_head ON raw_log_head.chain_id = known_chains.chain_id
        ORDER BY known_chains.chain_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load chain completeness frontiers")?;

    rows.into_iter()
        .map(|row| {
            Ok(ChainCompletenessRow {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                canonical_block_number: crate::sql_row::get(&row, "canonical_block_number")?,
                lineage_head_block_number: crate::sql_row::get(&row, "lineage_head_block_number")?,
                lineage_floor_block_number: crate::sql_row::get(
                    &row,
                    "lineage_floor_block_number",
                )?,
                lineage_canonical_block_count: crate::sql_row::get(
                    &row,
                    "lineage_canonical_block_count",
                )?,
                duplicate_canonical_height_count: crate::sql_row::get(
                    &row,
                    "duplicate_canonical_height_count",
                )?,
                disconnected_canonical_parent_count: crate::sql_row::get(
                    &row,
                    "disconnected_canonical_parent_count",
                )?,
                canonical_raw_log_head_block_number: crate::sql_row::get(
                    &row,
                    "canonical_raw_log_head_block_number",
                )?,
                raw_log_head_block_number: crate::sql_row::get(&row, "raw_log_head_block_number")?,
            })
        })
        .collect()
}

async fn load_replay_cursors(pool: &sqlx::PgPool) -> Result<Vec<ReplayCursorRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            deployment_profile,
            chain_id,
            cursor_kind,
            next_block_number,
            target_block_number,
            last_completed_block_number,
            NULLIF(last_failure_reason, '') AS last_failure_reason
        FROM normalized_replay_cursors
        ORDER BY deployment_profile, chain_id, cursor_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized replay cursors")?;

    rows.into_iter()
        .map(|row| {
            Ok(ReplayCursorRow {
                deployment_profile: crate::sql_row::get(&row, "deployment_profile")?,
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                cursor_kind: crate::sql_row::get(&row, "cursor_kind")?,
                next_block_number: crate::sql_row::get(&row, "next_block_number")?,
                target_block_number: crate::sql_row::get(&row, "target_block_number")?,
                last_completed_block_number: crate::sql_row::get(
                    &row,
                    "last_completed_block_number",
                )?,
                last_failure_reason: crate::sql_row::get(&row, "last_failure_reason")?,
            })
        })
        .collect()
}

async fn load_projection_apply_cursors(
    pool: &sqlx::PgPool,
) -> Result<Vec<ProjectionApplyCursorRow>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor_name, last_change_id
        FROM projection_apply_cursors
        ORDER BY cursor_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load projection apply cursors")?;

    rows.into_iter()
        .map(|row| {
            Ok(ProjectionApplyCursorRow {
                cursor_name: crate::sql_row::get(&row, "cursor_name")?,
                last_change_id: crate::sql_row::get(&row, "last_change_id")?,
            })
        })
        .collect()
}

async fn load_max_projection_change_id(pool: &sqlx::PgPool) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(change_id) FROM projection_normalized_event_changes",
    )
    .fetch_one(pool)
    .await
    .context("failed to load max projection change id")
}

async fn load_observed_code_addresses(pool: &sqlx::PgPool) -> Result<Vec<ObservedCodeAddress>> {
    let rows = sqlx::query(
        r#"
        SELECT chain_id, lower(contract_address) AS address, MAX(block_number) AS observed_block_number
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
                observed_block_number: crate::sql_row::get(&row, "observed_block_number")?,
            })
        })
        .collect()
}

async fn count_table(pool: &sqlx::PgPool, table: &'static str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::BIGINT FROM {table}"))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {table}"))
}

async fn load_normalized_event_counts(pool: &sqlx::PgPool) -> Result<Vec<NormalizedEventCount>> {
    let rows = sqlx::query(
        r#"
        SELECT chain_id, namespace, COUNT(*)::BIGINT AS count
        FROM normalized_events
        WHERE chain_id IS NOT NULL
          AND canonicality_state <> 'orphaned'::canonicality_state
        GROUP BY chain_id, namespace
        ORDER BY chain_id, namespace
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized-event counts")?;

    rows.into_iter()
        .map(|row| {
            Ok(NormalizedEventCount {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                namespace: crate::sql_row::get(&row, "namespace")?,
                count: crate::sql_row::get(&row, "count")?,
            })
        })
        .collect()
}

async fn load_name_current_counts(pool: &sqlx::PgPool) -> Result<Vec<NameCurrentCount>> {
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

async fn load_normalized_events_null_chain_id_count(pool: &sqlx::PgPool) -> Result<i64> {
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

async fn load_projection_replay_markers(
    pool: &sqlx::PgPool,
) -> Result<Vec<ProjectionReplayMarker>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT replay_version, projection
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
            })
        })
        .collect()
}

async fn load_backfill_lifecycle(pool: &sqlx::PgPool) -> Result<Vec<BackfillLifecycleRow>> {
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

async fn load_present_deferred_projection_indexes(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    let expected = DEFERRED_NORMALIZED_EVENT_INDEXES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT cls.relname
        FROM pg_index idx
        JOIN pg_class cls ON cls.oid = idx.indexrelid
        JOIN pg_namespace ns ON ns.oid = cls.relnamespace
        WHERE ns.nspname = 'public'
          AND cls.relname = ANY($1::TEXT[])
          AND idx.indisvalid
          AND idx.indisready
        ORDER BY cls.relname
        "#,
    )
    .bind(&expected)
    .fetch_all(pool)
    .await
    .context("failed to load present valid deferred projection indexes")
}

async fn load_manifest_chain_namespaces(
    pool: &sqlx::PgPool,
) -> Result<Vec<ManifestChainNamespace>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT mv.chain, mv.namespace
        FROM manifest_versions mv
        WHERE mv.rollout_status = 'active'
          AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(mv.manifest_payload -> 'abi' -> 'events') = 'array'
                        THEN mv.manifest_payload -> 'abi' -> 'events'
                    ELSE '[]'::jsonb
                END
            ) AS event
            WHERE jsonb_array_length(
                COALESCE(event -> 'normalized_events', '[]'::jsonb)
            ) > 0
          )
        ORDER BY mv.chain, mv.namespace
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

async fn load_manifest_declared_targets_missing_address(
    pool: &sqlx::PgPool,
) -> Result<Vec<ManifestDeclaredTarget>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT mv.chain, lower(mci.declared_address) AS address, mv.source_family
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND NOT EXISTS (
            SELECT 1 FROM contract_instance_addresses cia
            WHERE cia.contract_instance_id = mci.contract_instance_id
              AND cia.deactivated_at IS NULL
          )
        ORDER BY mv.chain, address, mv.source_family
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest-declared targets missing an address row")?;

    rows.into_iter()
        .map(|row| {
            Ok(ManifestDeclaredTarget {
                chain: crate::sql_row::get(&row, "chain")?,
                address: crate::sql_row::get(&row, "address")?,
                source_family: crate::sql_row::get(&row, "source_family")?,
            })
        })
        .collect()
}
