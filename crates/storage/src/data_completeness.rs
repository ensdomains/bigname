use anyhow::{Context, Result};

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
    pub canonical_raw_log_head_block_number: Option<i64>,
    pub raw_log_head_block_number: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayCursorRow {
    pub deployment_profile: String,
    pub chain_id: String,
    pub cursor_kind: String,
    pub last_completed_block_number: Option<i64>,
    pub target_block_number: Option<i64>,
    pub last_failure_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplyCursorRow {
    pub cursor_name: String,
    pub last_change_id: i64,
}

/// A `(chain_id, lowercased address)` pair with at least one non-orphaned code observation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObservedCodeAddress {
    pub chain_id: String,
    pub address: String,
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
    pub normalized_event_count: i64,
    pub name_current_count: i64,
}

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
        normalized_event_count: count_table(pool, "normalized_events").await?,
        name_current_count: count_table(pool, "name_current").await?,
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
        )
        SELECT
            known_chains.chain_id,
            chain_checkpoints.canonical_block_number,
            lineage.lineage_head_block_number,
            lineage.lineage_floor_block_number,
            COALESCE(lineage.lineage_canonical_block_count, 0) AS lineage_canonical_block_count,
            canonical_raw_log_head.canonical_raw_log_head_block_number,
            raw_log_head.raw_log_head_block_number
        FROM known_chains
        LEFT JOIN chain_checkpoints ON chain_checkpoints.chain_id = known_chains.chain_id
        LEFT JOIN lineage ON lineage.chain_id = known_chains.chain_id
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
            last_completed_block_number,
            target_block_number,
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
                last_completed_block_number: crate::sql_row::get(
                    &row,
                    "last_completed_block_number",
                )?,
                target_block_number: crate::sql_row::get(&row, "target_block_number")?,
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
        SELECT DISTINCT chain_id, lower(contract_address) AS address
        FROM raw_code_hashes
        WHERE canonicality_state <> 'orphaned'::canonicality_state
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
