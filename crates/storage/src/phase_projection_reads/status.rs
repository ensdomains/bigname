use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::{IndexingStatusChainRow, IndexingStatusRead};

/// The chains schema-v2 readers expect the phase runner to be working on: every chain with a
/// stored head or any phase state. Shared so per-chain readiness reads cannot drift into
/// disagreeing about which chains a missing row is missing from.
pub const PHASE_EXPECTED_CHAIN_IDS_SELECT: &str = r#"
    SELECT chain_id FROM bigname_phase.chain_heads
    UNION
    SELECT chain_id FROM bigname_phase.chain_phase_state
"#;

pub async fn load_phase_expected_status_chain_ids(pool: &PgPool) -> Result<Vec<String>> {
    sqlx::query_scalar(&format!(
        "SELECT chain_id FROM ({PHASE_EXPECTED_CHAIN_IDS_SELECT}) AS known_chains \
         ORDER BY chain_id"
    ))
    .fetch_all(pool)
    .await
    .context("failed to load expected schema-v2 indexing status chains")
}

pub async fn load_phase_indexing_status(pool: &PgPool) -> Result<IndexingStatusRead> {
    let rows = sqlx::query(&format!(
        r#"
        WITH known_chains AS ({PHASE_EXPECTED_CHAIN_IDS_SELECT})
        SELECT
            known_chains.chain_id,
            head.latest_block_number,
            head.safe_block_number,
            head.finalized_block_number,
            latest_lineage.block_timestamp AS latest_timestamp,
            project.current_block_number AS latest_projected_block,
            projected_lineage.block_timestamp AS latest_projected_timestamp,
            ingest.phase_status AS ingest_phase_status,
            project.phase_status AS project_phase_status,
            verify.phase_status AS verify_phase_status,
            verify.verification_level AS verify_verification_level,
            COALESCE(settlement.any_phase_settled_while_unconfigured, false)
                AS any_phase_settled_while_unconfigured,
            known_chains.chain_id = $2 AS provider_trusted_verification_required,
            COALESCE(
                project.input_content_hash = $1
                AND project.current_block_number <= head.latest_block_number
                AND (
                    project.current_block_number < head.latest_block_number
                    OR project.current_block_hash = head.latest_block_hash
                ),
                false
            ) AS project_generation_current,
            COALESCE(project.redo_in_progress, false) AS project_redo_in_progress,
            heartbeat.age_seconds AS phase_runner_heartbeat_age_seconds
        FROM known_chains
        LEFT JOIN chain_heads head
          ON head.chain_id = known_chains.chain_id
        LEFT JOIN chain_phase_state project
          ON project.chain_id = known_chains.chain_id
         AND project.phase_name = 'project'
        LEFT JOIN chain_phase_state ingest
          ON ingest.chain_id = known_chains.chain_id
         AND ingest.phase_name = 'ingest'
        LEFT JOIN chain_phase_state verify
          ON verify.chain_id = known_chains.chain_id
         AND verify.phase_name = 'verify'
        LEFT JOIN LATERAL (
            SELECT BOOL_OR(settled_while_unconfigured IS TRUE)
                AS any_phase_settled_while_unconfigured
            FROM chain_phase_state
            WHERE chain_id = known_chains.chain_id
        ) settlement ON TRUE
        LEFT JOIN bigname_phase.chain_lineage latest_lineage
          ON latest_lineage.chain_id = head.chain_id
         AND latest_lineage.block_number = head.latest_block_number
         AND latest_lineage.block_hash = head.latest_block_hash
         AND latest_lineage.canonicality_state IN (
             'canonical', 'safe', 'finalized'
         )
        LEFT JOIN bigname_phase.chain_lineage projected_lineage
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
        "#
    ))
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .bind(bigname_domain::vocabulary::ChainId::EthereumSepolia.as_str())
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
                ingest_phase_status: crate::sql_row::get(&row, "ingest_phase_status")?,
                project_phase_status: crate::sql_row::get(&row, "project_phase_status")?,
                verify_phase_status: crate::sql_row::get(&row, "verify_phase_status")?,
                verify_verification_level: crate::sql_row::get(&row, "verify_verification_level")?,
                any_phase_settled_while_unconfigured: crate::sql_row::get(
                    &row,
                    "any_phase_settled_while_unconfigured",
                )?,
                provider_trusted_verification_required: crate::sql_row::get(
                    &row,
                    "provider_trusted_verification_required",
                )?,
                project_generation_current: crate::sql_row::get(
                    &row,
                    "project_generation_current",
                )?,
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
