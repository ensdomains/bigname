use std::collections::BTreeMap;

use futures_util::{StreamExt, TryStreamExt, stream};

use crate::{
    IngestError, Result,
    coinbase_sql::{CoinbaseSqlSource, source_error},
    manifest::WatchQuery,
    provider::{Log, PROVIDER_PARALLELISM, ResolvedBlock, SharedProvider},
};

use super::prefetch::{Prefetcher, window_logs};

/// Everything a window's log lookups need, independent of which queries they serve.
pub(crate) struct QueryContext<'a> {
    pub(crate) provider: &'a SharedProvider,
    pub(crate) resolved: &'a [ResolvedBlock],
    pub(crate) coinbase: Option<&'a CoinbaseSqlSource>,
    /// Present only for persisted watch queries on a batch eligible to read ahead.
    pub(crate) prefetch: Option<&'a Prefetcher<'a>>,
}

impl QueryContext<'_> {
    /// The window's block span, or `None` when the window resolved nothing.
    fn span(&self) -> Option<(i64, i64)> {
        Some((self.resolved.first()?.number, self.resolved.last()?.number))
    }

    async fn logs(&self, query: &WatchQuery) -> Result<Vec<Log>> {
        if let Some(coinbase) = self.coinbase {
            if !query.topic1s.is_empty() {
                return Err(IngestError::configuration(
                    "Coinbase SQL ingest does not support topic1-narrowed watch queries",
                ));
            }
            return coinbase
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
                });
        }
        let Some((first, last)) = self.span() else {
            return Ok(Vec::new());
        };
        let from = first.max(query.from_block);
        let to = last.min(query.to_block);
        if from > to || query.topic0s.is_empty() {
            return Ok(Vec::new());
        }
        match self.prefetch {
            Some(prefetch) => {
                prefetch
                    .logs(self.provider, self.resolved, query, from, to)
                    .await
            }
            None => window_logs(self.provider, self.resolved, query, from, to).await,
        }
    }
}

/// Runs every query's log lookup and merges the results into `selected_by_identity`.
///
/// Remote lookups overlap up to the provider's bounded parallelism; the Coinbase SQL source
/// keeps its one-at-a-time request pattern. Merging stays in query order either way, so the
/// conflict a window reports does not depend on which request finished first.
pub(crate) async fn fetch_into(
    context: &QueryContext<'_>,
    queries: &[WatchQuery],
    selected_by_identity: &mut BTreeMap<(String, i64), Log>,
) -> Result<()> {
    let parallelism = if context.coinbase.is_some() {
        1
    } else {
        PROVIDER_PARALLELISM
    };
    let mut pending = Vec::with_capacity(queries.len());
    for query in queries {
        pending.push(context.logs(query));
    }
    let results = stream::iter(pending)
        .buffered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;
    for log in results.into_iter().flatten() {
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
    Ok(())
}
