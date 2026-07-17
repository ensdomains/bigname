use std::collections::BTreeSet;

use anyhow::Result;
use bigname_storage::ChainLineageBlock;
use sqlx::Row;

pub(super) async fn same_height_fork_lineage_numbers(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
) -> Result<BTreeSet<i64>> {
    if path.is_empty() {
        return Ok(BTreeSet::new());
    }

    let block_numbers = path
        .iter()
        .map(|block| block.block_number)
        .collect::<Vec<_>>();
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT block_number
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = ANY($2::BIGINT[])
          AND NOT (block_hash = ANY($3::TEXT[]))
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| row.try_get("block_number").map_err(Into::into))
        .collect()
}
