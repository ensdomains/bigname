use anyhow::{Context, Result};
use sqlx::{PgPool, types::time::OffsetDateTime};

use super::ChainBlockRange;

/// Resolve an inclusive timestamp window to per-chain inclusive block ranges
/// using readable `chain_lineage` rows only; no RPC is involved.
///
/// `from` maps to the first readable block whose timestamp is at or after it and
/// `to` maps to the last readable block whose timestamp is at or before it. A
/// chain with no block satisfying a supplied bound is omitted, so its rows are
/// excluded by the resulting window. Block timestamps are monotonic per chain,
/// so each bound is one ordered index probe.
pub async fn resolve_chain_block_ranges(
    pool: &PgPool,
    chain_ids: &[&str],
    from: Option<OffsetDateTime>,
    to: Option<OffsetDateTime>,
) -> Result<Vec<ChainBlockRange>> {
    let mut ranges = Vec::with_capacity(chain_ids.len());
    for chain_id in chain_ids {
        let from_block = match from {
            Some(from) => match first_readable_block_at_or_after(pool, chain_id, from).await? {
                Some(block_number) => Some(block_number),
                None => continue,
            },
            None => None,
        };
        let to_block = match to {
            Some(to) => match last_readable_block_at_or_before(pool, chain_id, to).await? {
                Some(block_number) => Some(block_number),
                None => continue,
            },
            None => None,
        };
        if matches!((from_block, to_block), (Some(from), Some(to)) if from > to) {
            continue;
        }
        ranges.push(ChainBlockRange {
            chain_id: (*chain_id).to_owned(),
            from_block,
            to_block,
        });
    }
    Ok(ranges)
}

async fn first_readable_block_at_or_after(
    pool: &PgPool,
    chain_id: &str,
    at: OffsetDateTime,
) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND block_timestamp >= $2
        ORDER BY block_timestamp ASC, block_number ASC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(at)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to resolve the first block at or after {at} on {chain_id}"))
}

async fn last_readable_block_at_or_before(
    pool: &PgPool,
    chain_id: &str,
    at: OffsetDateTime,
) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM bigname_phase.chain_lineage
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND block_timestamp <= $2
        ORDER BY block_timestamp DESC, block_number DESC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(at)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to resolve the last block at or before {at} on {chain_id}"))
}
