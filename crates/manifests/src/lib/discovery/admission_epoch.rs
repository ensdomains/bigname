use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

/// Acquire the writer side of the admission-epoch fence before changing any
/// rows which can alter the watched set. The lock is held by the caller's
/// transaction until commit; a stored-lineage promotion which reaches its
/// final shared fence must therefore wait and then observe the conditional
/// epoch bump made by that same transaction.
pub(crate) async fn fence_discovery_admission_epoch_writes(
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
        SELECT chain_id, 0 FROM UNNEST($1::TEXT[]) AS chains(chain_id)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(&chains)
    .execute(&mut *executor)
    .await
    .context("failed to ensure discovery admission-epoch writer fence rows")?;
    sqlx::query(
        r#"
        SELECT chain_id
        FROM discovery_admission_epochs
        WHERE chain_id = ANY($1::TEXT[])
        ORDER BY chain_id
        FOR UPDATE
        "#,
    )
    .bind(&chains)
    .fetch_all(executor)
    .await
    .context("failed to acquire discovery admission-epoch writer fences")?;
    Ok(())
}

/// Invariant: any transaction that mutates `discovery_edges` (insert,
/// reactivation, window update, or deactivation) OR the manifest-declared
/// watched surface (manifest entries, seeded addresses, declared start
/// blocks, rollout status) must bump the owning chain's
/// `discovery_admission_epochs` row in the same transaction. Promotion's
/// verified coverage frontier is versioned by this epoch; a missed bump
/// would let a stale frontier promote over a newly watched tuple's
/// unfetched logs.
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

/// Current admission epoch for every chain that has recorded a watched-surface
/// mutation. A chain with no row has epoch `0` and is simply absent.
///
/// This is the change-detection sentinel for stored watch-plan reloads: any
/// transaction that mutates the watched surface must bump the owning chain's
/// epoch row in the same transaction (see [`bump_discovery_admission_epochs`]),
/// so an unchanged map proves the stored plan has not moved without scanning
/// the plan tables.
pub async fn load_discovery_admission_epochs(
    pool: &PgPool,
) -> Result<std::collections::BTreeMap<String, i64>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT chain_id, epoch FROM discovery_admission_epochs ORDER BY chain_id",
    )
    .fetch_all(pool)
    .await
    .context("failed to load discovery admission epochs")?;
    Ok(rows.into_iter().collect())
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
