use sqlx::{Postgres, Transaction};

use super::{HeadMarkers, head_write_error};
use crate::error::RunnerResult;

pub(super) async fn markers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    heads: &HeadMarkers,
    lineage_orphaning_epoch: i64,
    orphaned_readable_lineage: bool,
) -> RunnerResult<()> {
    sqlx::query(
        "
        INSERT INTO chain_heads (
            chain_id,
            latest_block_hash,
            latest_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number,
            lineage_orphaning_epoch
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (chain_id) DO UPDATE
        SET latest_block_hash = EXCLUDED.latest_block_hash,
            latest_block_number = EXCLUDED.latest_block_number,
            safe_block_hash = EXCLUDED.safe_block_hash,
            safe_block_number = EXCLUDED.safe_block_number,
            finalized_block_hash = EXCLUDED.finalized_block_hash,
            finalized_block_number = EXCLUDED.finalized_block_number,
            lineage_orphaning_epoch = CASE
                WHEN $9 THEN GREATEST(
                    chain_heads.lineage_orphaning_epoch + 1,
                    EXCLUDED.lineage_orphaning_epoch
                )
                ELSE GREATEST(
                    chain_heads.lineage_orphaning_epoch,
                    EXCLUDED.lineage_orphaning_epoch
                )
            END,
            updated_at = now()
        ",
    )
    .bind(chain_id)
    .bind(&heads.latest.hash)
    .bind(heads.latest.number)
    .bind(heads.safe.as_ref().map(|marker| marker.hash.as_str()))
    .bind(heads.safe.as_ref().map(|marker| marker.number))
    .bind(heads.finalized.as_ref().map(|marker| marker.hash.as_str()))
    .bind(heads.finalized.as_ref().map(|marker| marker.number))
    .bind(lineage_orphaning_epoch)
    .bind(orphaned_readable_lineage)
    .execute(&mut **transaction)
    .await
    .map_err(|error| head_write_error("publish head markers", chain_id, error))?;
    Ok(())
}
