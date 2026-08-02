use std::collections::BTreeMap;

use crate::{
    IngestError, Result,
    coinbase_sql::{CoinbaseSqlSource, source_error},
    manifest::WatchQuery,
    provider::{Log, ResolvedBlock, SharedProvider, provider_error},
};

pub(super) async fn fetch_into(
    provider: &SharedProvider,
    resolved: &[ResolvedBlock],
    coinbase: Option<&CoinbaseSqlSource>,
    queries: &[WatchQuery],
    selected_by_identity: &mut BTreeMap<(String, i64), Log>,
) -> Result<()> {
    for query in queries {
        let logs = if let Some(coinbase) = coinbase {
            coinbase
                .fetch(
                    query.from_block,
                    query.to_block,
                    &query.addresses,
                    &query.topic0s,
                )
                .await
                .map_err(|error| {
                    source_error(
                        &format!(
                            "failed to fetch Coinbase SQL logs {}..={}",
                            query.from_block, query.to_block
                        ),
                        error,
                    )
                })?
        } else {
            let query_blocks = resolved
                .iter()
                .filter(|block| (query.from_block..=query.to_block).contains(&block.number))
                .cloned()
                .collect::<Vec<_>>();
            provider
                .logs(&query_blocks, &query.addresses, &query.topic0s)
                .await
                .map_err(|error| provider_error("failed to fetch selected chain logs", error))?
        };
        for log in logs {
            let key = (log.block_hash.clone(), log.log_index);
            if let Some(previous) = selected_by_identity.insert(key.clone(), log.clone())
                && previous != log
            {
                return Err(IngestError::data_integrity(format!(
                    "ingest sources returned conflicting log identity {} {}",
                    key.0, key.1
                )));
            }
        }
    }
    Ok(())
}
