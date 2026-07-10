use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

/// Invariant: any transaction that mutates `discovery_edges` (insert,
/// reactivation, window update, or deactivation) must bump the owning
/// chain's `discovery_admission_epochs` row in the same transaction.
/// Promotion's verified coverage frontier is versioned by this epoch; a
/// missed bump would let a stale frontier promote over a newly watched
/// tuple's unfetched logs.
pub(crate) async fn bump_discovery_admission_epochs(
    executor: &mut sqlx::postgres::PgConnection,
    chains: &BTreeSet<String>,
) -> Result<()> {
    if chains.is_empty() {
        return Ok(());
    }
    let chains = chains.iter().cloned().collect::<Vec<_>>();
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        SELECT chain_id, 1 FROM UNNEST($1::TEXT[]) AS chains(chain_id)
        ON CONFLICT (chain_id)
        DO UPDATE SET epoch = discovery_admission_epochs.epoch + 1
        "#,
    )
    .bind(&chains)
    .execute(executor)
    .await
    .context("failed to bump discovery admission epochs")?;
    Ok(())
}

/// Current admission epoch for a chain; `0` when no discovery mutation has
/// ever been recorded.
pub async fn load_discovery_admission_epoch(pool: &PgPool, chain: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1")
        .bind(chain)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("failed to load the discovery admission epoch for chain {chain}"))
        .map(|epoch| epoch.unwrap_or(0))
}
