use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    ChainCheckpoint, ChainCheckpointUpdate,
    advance_chain_checkpoints_in_transaction_with_lineage_fork_policy,
    stored_lineage_fork_guard::LineageForkPolicy,
};

/// Advance persisted checkpoint pointers for one chain and promote stored
/// lineage rows along the admitted ancestry path.
pub async fn advance_chain_checkpoints(
    pool: &PgPool,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    advance_chain_checkpoints_with_lineage_fork_policy(pool, update, LineageForkPolicy::Allow).await
}

pub(super) async fn advance_chain_checkpoints_with_lineage_fork_policy(
    pool: &PgPool,
    update: &ChainCheckpointUpdate,
    lineage_fork_policy: LineageForkPolicy,
) -> Result<ChainCheckpoint> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for checkpoint advancement")?;
    let checkpoint = advance_chain_checkpoints_in_transaction_with_lineage_fork_policy(
        &mut transaction,
        update,
        lineage_fork_policy,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit checkpoint advancement")?;
    Ok(checkpoint)
}
