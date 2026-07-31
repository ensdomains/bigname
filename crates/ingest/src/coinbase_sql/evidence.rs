use std::collections::BTreeSet;

use sqlx::{Postgres, Transaction};

use crate::{IngestError, Result, manifest::WatchQuery, provider::Log};

pub async fn recount_loaded_window(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    provider_logs: &[Log],
    materialized_logs: &[Log],
    queries: &[WatchQuery],
) -> Result<()> {
    if queries.iter().any(|query| query.addresses.is_empty()) {
        return Err(IngestError::data_integrity(
            "all-emitter scopes are not supported by the Coinbase SQL bulk source",
        ));
    }

    let provider_identities = provider_logs
        .iter()
        .map(log_identity)
        .collect::<BTreeSet<_>>();
    let materialized_identities = materialized_logs
        .iter()
        .filter(|log| queries.iter().any(|query| query_matches_log(query, log)))
        .map(log_identity)
        .collect::<BTreeSet<_>>();
    let (block_hashes, log_indexes): (Vec<_>, Vec<_>) =
        materialized_identities.iter().cloned().unzip();
    let stored_identities: BTreeSet<(String, i64)> = sqlx::query_as(
        "
        SELECT stored.block_hash, stored.log_index
        FROM unnest($2::text[], $3::bigint[]) expected(block_hash, log_index)
        JOIN raw_logs stored
          ON stored.chain_id = $1
         AND stored.block_hash = expected.block_hash
         AND stored.log_index = expected.log_index
        ",
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .bind(&log_indexes)
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
    })?
    .into_iter()
    .collect();

    let stored_count = stored_identities.len();
    let provider_count = provider_logs.len();
    let materialized_count = materialized_identities.len();
    if provider_identities != materialized_identities
        || stored_identities != materialized_identities
        || provider_identities.len() != provider_count
    {
        return Err(IngestError::data_integrity(format!(
            "Coinbase SQL ingest recount mismatch for chain {chain_id} blocks \
             {from_block}..={to_block}: provider reported {provider_count} logs, \
             this load materialized {materialized_count}, storage contains \
             {stored_count} of those exact identities"
        )));
    }
    Ok(())
}

fn log_identity(log: &Log) -> (String, i64) {
    (log.block_hash.clone(), log.log_index)
}

fn query_matches_log(query: &WatchQuery, log: &Log) -> bool {
    (query.from_block..=query.to_block).contains(&log.block_number)
        && query
            .addresses
            .iter()
            .any(|address| address.eq_ignore_ascii_case(&log.address))
        && log.topics.first().is_some_and(|topic0| {
            query
                .topic0s
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(topic0))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mismatch_message_identifies_the_exact_window() {
        let error = IngestError::data_integrity(
            "Coinbase SQL ingest recount mismatch for chain base-mainnet blocks 10..=20: \
             provider reported 118 logs, this load materialized 117, storage contains \
             117 of those exact identities",
        );
        assert!(error.to_string().contains("10..=20"));
        assert!(error.to_string().contains("118"));
        assert!(error.to_string().contains("117"));
    }
}
