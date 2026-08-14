use anyhow::{Context, Result, ensure};
use bigname_project::Marker as ProjectMarker;
use bigname_storage::CURRENT_PROJECT_PUBLICATION_JOIN;
use sqlx::PgPool;

use super::PROJECTION_NAME_COUNT_SQL;

pub(super) async fn require_published_head(
    pool: &PgPool,
    chain_id: &str,
    head_block: i64,
) -> Result<()> {
    let query = format!(
        "SELECT EXISTS (
             SELECT 1
             FROM bigname_phase.chain_heads head
             {CURRENT_PROJECT_PUBLICATION_JOIN}
             WHERE head.chain_id = $1
               AND head.latest_block_number = $2
               AND project.input_content_hash = $3
         )"
    );
    let selected_head_is_published: bool = sqlx::query_scalar(&query)
        .bind(chain_id)
        .bind(head_block)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_one(pool)
        .await
        .context("failed to validate the published-head Project state")?;
    ensure!(
        selected_head_is_published,
        "selected head {head_block} must already be a completed Project publication at chain_heads.latest_block_number under the current interpreter content hash before the published-head re-apply"
    );
    Ok(())
}

pub(super) async fn project_marker(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
) -> Result<ProjectMarker> {
    let block_hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2 AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_one(pool)
    .await
    .context("failed to load published-head re-apply resume marker")?;
    Ok(ProjectMarker {
        number: block_number,
        hash: block_hash,
    })
}

pub(super) fn require_minimum_walk_blocks(walk_blocks: i64, minimum: u64) -> Result<()> {
    ensure!(
        u64::try_from(walk_blocks).unwrap_or_default() >= minimum,
        "Interpret walk contains {walk_blocks} blocks; release minimum is {minimum}"
    );
    Ok(())
}

pub(super) async fn projection_name_count(pool: &PgPool, chain_id: &str) -> Result<u64> {
    let count: i64 = sqlx::query_scalar(PROJECTION_NAME_COUNT_SQL)
        .bind(chain_id)
        .fetch_one(pool)
        .await
        .context("failed to count selected-chain names during projection benchmarking")?;
    u64::try_from(count).context("name_current returned a negative row count")
}
