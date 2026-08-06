use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::{IndexingStatusChainRow, IndexingStatusRead};

pub async fn load_phase_expected_status_chain_ids(pool: &PgPool) -> Result<Vec<String>> {
    sqlx::query_scalar(
        r#"
        SELECT chain_id
        FROM (
            SELECT chain_id FROM chain_heads
            UNION
            SELECT chain_id FROM chain_phase_state
        ) AS known_chains
        ORDER BY chain_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load expected schema-v2 indexing status chains")
}

pub async fn load_phase_indexing_status(pool: &PgPool) -> Result<IndexingStatusRead> {
    let rows = sqlx::query(
        r#"
        WITH known_chains AS (
            SELECT chain_id FROM chain_heads
            UNION
            SELECT chain_id FROM chain_phase_state
        )
        SELECT
            known_chains.chain_id,
            head.latest_block_number,
            head.safe_block_number,
            head.finalized_block_number,
            latest_lineage.block_timestamp AS latest_timestamp,
            project.current_block_number AS latest_projected_block,
            projected_lineage.block_timestamp AS latest_projected_timestamp,
            project.phase_status AS project_phase_status,
            COALESCE(project.redo_in_progress, false) AS project_redo_in_progress,
            heartbeat.age_seconds AS phase_runner_heartbeat_age_seconds
        FROM known_chains
        LEFT JOIN chain_heads head
          ON head.chain_id = known_chains.chain_id
        LEFT JOIN chain_phase_state project
          ON project.chain_id = known_chains.chain_id
         AND project.phase_name = 'project'
        LEFT JOIN chain_lineage latest_lineage
          ON latest_lineage.chain_id = head.chain_id
         AND latest_lineage.block_number = head.latest_block_number
         AND latest_lineage.block_hash = head.latest_block_hash
         AND latest_lineage.canonicality_state IN (
             'canonical', 'safe', 'finalized'
         )
        LEFT JOIN chain_lineage projected_lineage
          ON projected_lineage.chain_id = project.chain_id
         AND projected_lineage.block_number = project.current_block_number
         AND projected_lineage.block_hash = project.current_block_hash
         AND projected_lineage.canonicality_state IN (
             'canonical', 'safe', 'finalized'
        )
        LEFT JOIN LATERAL (
            SELECT FLOOR(
                EXTRACT(EPOCH FROM (clock_timestamp() - MAX(heartbeat_at)))
            )::BIGINT AS age_seconds
            FROM service_heartbeats
            WHERE service_name = 'phase-runner'
              AND chain_id = known_chains.chain_id
        ) heartbeat ON TRUE
        ORDER BY known_chains.chain_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load schema-v2 indexing status")?;

    let chains = rows
        .into_iter()
        .map(|row| {
            Ok(IndexingStatusChainRow {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                canonical_block: crate::sql_row::get(&row, "latest_block_number")?,
                safe_block: crate::sql_row::get(&row, "safe_block_number")?,
                finalized_block: crate::sql_row::get(&row, "finalized_block_number")?,
                canonical_timestamp: crate::sql_row::get(&row, "latest_timestamp")?,
                latest_projected_block: crate::sql_row::get(&row, "latest_projected_block")?,
                latest_projected_timestamp: crate::sql_row::get(
                    &row,
                    "latest_projected_timestamp",
                )?,
                project_phase_status: crate::sql_row::get(&row, "project_phase_status")?,
                project_redo_in_progress: crate::sql_row::get(&row, "project_redo_in_progress")?,
                phase_runner_heartbeat_age_seconds: crate::sql_row::get(
                    &row,
                    "phase_runner_heartbeat_age_seconds",
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(IndexingStatusRead {
        chains,
        has_unscoped_pending_invalidations: false,
        pending_invalidation_count: 0,
        pending_invalidation_count_capped: false,
        dead_letter_count: 0,
    })
}
