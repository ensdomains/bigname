use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Row};

use crate::lineage::ChainLineageBlock;

use super::{
    ChainCheckpoint, ChainCheckpointUpdate,
    advance::advance_chain_checkpoints_with_lineage_fork_policy,
    advance_chain_checkpoints_in_transaction_with_lineage_fork_policy,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum LineageForkPolicy {
    Allow,
    Reject,
}

/// Advance checkpoints only if the exact canonical path being promoted has no
/// different-hash, non-orphaned row at any of its heights. The final check and
/// checkpoint write exclude concurrent lineage writers in one transaction.
pub async fn advance_chain_checkpoints_rejecting_non_orphaned_lineage_forks(
    pool: &PgPool,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    advance_chain_checkpoints_with_lineage_fork_policy(pool, update, LineageForkPolicy::Reject)
        .await
}

pub async fn advance_chain_checkpoints_rejecting_non_orphaned_lineage_forks_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    advance_chain_checkpoints_in_transaction_with_lineage_fork_policy(
        transaction,
        update,
        LineageForkPolicy::Reject,
    )
    .await
}

pub(super) async fn lock_lineage_writes_for_guarded_promotion(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<()> {
    // Self-exclusive mode serializes guarded promotions and avoids the lock
    // upgrade deadlock possible when two transactions first take SHARE and
    // then update the promoted path.
    sqlx::query("LOCK TABLE chain_lineage IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **transaction)
        .await
        .context("failed to exclude lineage writers during guarded checkpoint advancement")?;
    Ok(())
}

pub(super) async fn ensure_promoted_path_has_no_non_orphaned_same_height_forks(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    path: &[ChainLineageBlock],
) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    let block_numbers = path
        .iter()
        .map(|block| block.block_number)
        .collect::<Vec<_>>();
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let competing = sqlx::query(
        r#"
        SELECT
            selected.block_number,
            selected.block_hash,
            competing.block_hash AS competing_block_hash
        FROM UNNEST($2::BIGINT[], $3::TEXT[]) AS selected(block_number, block_hash)
        JOIN chain_lineage AS competing
          ON competing.chain_id = $1
         AND competing.block_number = selected.block_number
         AND competing.block_hash <> selected.block_hash
         AND competing.canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY selected.block_number
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to repeat the stored-lineage fork check for chain {chain_id}")
    })?;

    if let Some(competing) = competing {
        let block_number: i64 = competing.try_get("block_number")?;
        let selected_hash: String = competing.try_get("block_hash")?;
        let competing_hash: String = competing.try_get("competing_block_hash")?;
        bail!(
            "stored lineage path for chain {chain_id} has a non-orphaned same-height fork at block {block_number} (selected {selected_hash}, competing {competing_hash}); repair the losing branch to orphaned before retrying"
        );
    }

    Ok(())
}
