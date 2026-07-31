use std::collections::BTreeSet;

use sqlx::{Postgres, Transaction};

use crate::{IngestError, Result, manifest::WatchQuery, provider::Log};

pub async fn recount_loaded_window(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    provider_logs: &[Log],
    queries: &[WatchQuery],
) -> Result<()> {
    let mut stored_identities = BTreeSet::new();
    for query in queries {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "
            SELECT block_hash, log_index
            FROM raw_logs
            WHERE chain_id = $1
              AND block_number BETWEEN $2 AND $3
              AND lower(emitting_address) = ANY($4)
              AND lower(topics[1]) = ANY($5)
            ",
        )
        .bind(chain_id)
        .bind(query.from_block)
        .bind(query.to_block)
        .bind(&query.addresses)
        .bind(&query.topic0s)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            IngestError::database(
                format!(
                    "failed to recount Coinbase SQL load {chain_id} \
                     {from_block}..={to_block}"
                ),
                error,
            )
        })?;
        stored_identities.extend(rows);
    }
    let stored_count = stored_identities.len();
    let provider_count = provider_logs.len();
    if stored_count != provider_count {
        return Err(IngestError::data_integrity(format!(
            "Coinbase SQL ingest recount mismatch for chain {chain_id} blocks \
             {from_block}..={to_block}: provider reported {provider_count} logs, \
             storage contains {stored_count}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mismatch_message_identifies_the_exact_window() {
        let error = IngestError::data_integrity(
            "Coinbase SQL ingest recount mismatch for chain base-mainnet blocks 10..=20: \
             provider reported 118 logs, storage contains 117",
        );
        assert!(error.to_string().contains("10..=20"));
        assert!(error.to_string().contains("118"));
        assert!(error.to_string().contains("117"));
    }
}
