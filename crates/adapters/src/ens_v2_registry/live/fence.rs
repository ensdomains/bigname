use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, Transaction};

pub(crate) async fn acquire_registry_sync_fence(
    pool: &PgPool,
    chain: &str,
) -> Result<Transaction<'static, Postgres>> {
    let mut transaction = pool
        .begin()
        .await
        .with_context(|| format!("failed to start ENSv2 registry sync fence for {chain}"))?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("ens_v2_registry_sync:{chain}"))
        .execute(transaction.as_mut())
        .await
        .with_context(|| format!("failed to acquire ENSv2 registry sync fence for {chain}"))?;
    Ok(transaction)
}

pub(crate) async fn release_registry_sync_fence(
    transaction: Transaction<'static, Postgres>,
    chain: &str,
) -> Result<()> {
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to release ENSv2 registry sync fence for {chain}"))
}
