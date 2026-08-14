use anyhow::{Context, Result, ensure};
use bigname_storage::CURRENT_PROJECT_PUBLICATION_JOIN;
use sqlx::PgPool;

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
