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

pub(super) async fn select_log_bounded_replay_to_block(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    hard_to_block: i64,
    max_raw_logs_per_chunk: usize,
) -> Result<i64> {
    if from_block >= hard_to_block {
        return Ok(hard_to_block);
    }
    let max_raw_logs_per_chunk = i64::try_from(max_raw_logs_per_chunk)
        .context("normalized replay max logs per chunk does not fit in i64")?;

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
    .bind(max_raw_logs_per_chunk)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to select log-bounded normalized replay range for chain {chain} range {from_block}..={hard_to_block}"
        )
    })
}
