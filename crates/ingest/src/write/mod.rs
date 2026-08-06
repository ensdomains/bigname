mod immutable;
mod inserts;
#[cfg(test)]
mod tests;

use sqlx::PgPool;

use crate::{
    IngestError, Result, coinbase_sql::evidence::recount_loaded_window, fetching::FetchedBatch,
    manifest::WatchQuery, provider::Log,
};

pub async fn store(
    pool: &PgPool,
    chain_id: &str,
    facts: &FetchedBatch,
    coinbase_window: Option<(i64, i64, &[Log], &[WatchQuery])>,
) -> Result<()> {
    validate_address_case(facts)?;
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
        recount_loaded_window(
            &mut transaction,
            chain_id,
            from,
            to,
            provider_logs,
            &facts.logs,
            queries,
        )
        .await?;
    }
    transaction.commit().await.map_err(|error| {
        crate::IngestError::database(
            format!("failed to commit raw-fact write for chain {chain_id}"),
            error,
        )
    })
}

fn validate_address_case(facts: &FetchedBatch) -> Result<()> {
    for transaction in &facts.transactions {
        require_lowercase("transaction sender", &transaction.from)?;
        if let Some(address) = transaction.to.as_deref() {
            require_lowercase("transaction recipient", address)?;
        }
    }
    for receipt in &facts.receipts {
        if let Some(address) = receipt.contract_address.as_deref() {
            require_lowercase("created contract", address)?;
        }
    }
    for log in &facts.logs {
        require_lowercase("log emitter", &log.address)?;
    }
    Ok(())
}

fn require_lowercase(label: &str, address: &str) -> Result<()> {
    if address != address.to_ascii_lowercase() {
        return Err(IngestError::data_integrity(format!(
            "{label} address must use lowercase hex: {address}"
        )));
    }
    Ok(())
}
