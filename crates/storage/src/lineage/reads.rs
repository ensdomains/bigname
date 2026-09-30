use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres};

use super::decode::decode_lineage_block;
use super::types::ChainLineageBlock;

/// Load one lineage snapshot by hash-first identity.
pub async fn load_chain_lineage_block(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>> {
    load_chain_lineage_block_internal(pool, chain_id, block_hash).await
}

pub(crate) async fn load_chain_lineage_block_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            lineage.chain_id,
            lineage.block_hash,
            lineage.parent_hash,
            lineage.block_number,
            lineage.block_timestamp,
            audit.logs_bloom,
            audit.transactions_root,
            audit.receipts_root,
            audit.state_root,
            lineage.canonicality_state::TEXT AS canonicality_state
        FROM bigname_phase.chain_lineage AS lineage
        LEFT JOIN chain_header_audit AS audit
          ON audit.chain_id = lineage.chain_id
         AND audit.block_hash = lineage.block_hash
        WHERE lineage.chain_id = $1
          AND lineage.block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to load lineage row for chain {chain_id} block {block_hash}")
    })?;

    row.map(decode_lineage_block).transpose()
}
