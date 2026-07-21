use anyhow::{Context, Result};
use sqlx::PgPool;

pub(crate) async fn select_log_bounded_replay_to_block(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    hard_to_block: i64,
    max_raw_logs_per_page: usize,
) -> Result<i64> {
    if from_block >= hard_to_block {
        return Ok(hard_to_block);
    }
    let max_raw_logs_per_page = i64::try_from(max_raw_logs_per_page)
        .context("normalized replay max logs per page does not fit in i64")?;

    sqlx::query_scalar::<_, i64>(
        r#"
        WITH ordered_logs AS (
            SELECT raw_logs.block_number
            FROM raw_logs
            JOIN chain_lineage AS lineage
              ON lineage.chain_id = raw_logs.chain_id
             AND lineage.block_hash = raw_logs.block_hash
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_number >= $2
              AND raw_logs.block_number <= $3
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
            ORDER BY raw_logs.block_number ASC, raw_logs.log_index ASC
            LIMIT ($4 + 1)
        ),
        numbered_logs AS (
            SELECT block_number, ROW_NUMBER() OVER () AS ordinal
            FROM ordered_logs
        ),
        overflow AS (
            SELECT block_number
            FROM numbered_logs
            WHERE ordinal = $4 + 1
        ),
        bounded AS (
            SELECT block_number
            FROM numbered_logs
            WHERE NOT EXISTS (SELECT 1 FROM overflow)
               OR block_number < (SELECT block_number FROM overflow)
            UNION ALL
            SELECT MIN(block_number)
            FROM numbered_logs
        )
        SELECT COALESCE(MAX(block_number), $3)
        FROM bounded
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(hard_to_block)
    .bind(max_raw_logs_per_page)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to select log-bounded normalized replay range for chain {chain} range {from_block}..={hard_to_block}"
        )
    })
}
