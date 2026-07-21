use anyhow::{Context, Result};
use sqlx::PgPool;

use super::RawLogBounds;

pub(super) async fn load_canonical_raw_log_bounds(
    pool: &PgPool,
    chain: &str,
) -> Result<Option<RawLogBounds>> {
    let start_block = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT raw_logs.block_number
        FROM raw_logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND raw_logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            raw_logs.block_number ASC,
            raw_logs.block_hash ASC,
            raw_logs.transaction_index ASC,
            raw_logs.log_index ASC,
            raw_logs.raw_log_id ASC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load canonical raw-log bounds for chain {chain}"))?;

    let Some(start_block) = start_block else {
        return Ok(None);
    };

    let target_block = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT raw_logs.block_number
        FROM raw_logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND raw_logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            raw_logs.block_number DESC,
            raw_logs.block_hash DESC,
            raw_logs.transaction_index DESC,
            raw_logs.log_index DESC,
            raw_logs.raw_log_id DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load latest canonical raw-log block for chain {chain}"))?
    .context("latest canonical raw-log block disappeared after start was found")?;

    Ok(Some(RawLogBounds {
        start_block,
        target_block,
    }))
}
