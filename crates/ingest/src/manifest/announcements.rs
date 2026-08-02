use sqlx::PgPool;

use crate::{IngestError, Result};

pub(super) async fn canonical(
    pool: &PgPool,
    chain_id: &str,
    to_block: i64,
    topic0: &str,
) -> Result<Vec<(String, i64)>> {
    sqlx::query_as(
        "
        SELECT lower(raw.emitting_address), min(raw.block_number)
        FROM raw_logs raw
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw.chain_id
         AND lineage.block_hash = raw.block_hash
         AND lineage.block_number = raw.block_number
        WHERE raw.chain_id = $1
          AND raw.block_number <= $2
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lower(raw.topics[1]) = lower($3)
        GROUP BY lower(raw.emitting_address)
        ORDER BY lower(raw.emitting_address)
        ",
    )
    .bind(chain_id)
    .bind(to_block)
    .bind(topic0)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!("failed to load canonical registry announcements for chain {chain_id}"),
            error,
        )
    })
}
