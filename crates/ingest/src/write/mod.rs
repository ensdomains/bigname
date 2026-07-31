mod immutable;
mod inserts;
#[cfg(test)]
mod tests;

use sqlx::PgPool;

use crate::{
    Result, coinbase_sql::evidence::recount_loaded_window, fetching::FetchedBatch,
    manifest::WatchQuery, provider::Log,
};

pub async fn store(
    pool: &PgPool,
    chain_id: &str,
    facts: &FetchedBatch,
    coinbase_window: Option<(i64, i64, &[Log], &[WatchQuery])>,
) -> Result<()> {
    let mut transaction = pool.begin().await.map_err(|error| {
        crate::IngestError::database(
            format!("failed to begin raw-fact write for chain {chain_id}"),
            error,
        )
    })?;
    inserts::lineage(&mut transaction, chain_id, &facts.blocks).await?;
    inserts::header_audit(&mut transaction, chain_id, &facts.blocks).await?;
    inserts::transactions(&mut transaction, chain_id, &facts.transactions).await?;
    inserts::receipts(&mut transaction, chain_id, &facts.receipts).await?;
    inserts::logs(&mut transaction, chain_id, &facts.logs).await?;

    immutable::verify_lineage(&mut transaction, chain_id, &facts.blocks).await?;
    immutable::verify_transactions(&mut transaction, chain_id, &facts.transactions).await?;
    immutable::verify_receipts(&mut transaction, chain_id, &facts.receipts).await?;
    immutable::verify_logs(&mut transaction, chain_id, &facts.logs).await?;
    if let Some((from, to, provider_logs, queries)) = coinbase_window {
        recount_loaded_window(&mut transaction, chain_id, from, to, provider_logs, queries).await?;
    }
    transaction.commit().await.map_err(|error| {
        crate::IngestError::database(
            format!("failed to commit raw-fact write for chain {chain_id}"),
            error,
        )
    })
}
