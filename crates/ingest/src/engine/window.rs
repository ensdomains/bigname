use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use crate::{
    ErrorKind, Result,
    coinbase_sql::CoinbaseSqlSource,
    fetching::{FetchedBatch, fetch_selected_facts},
    manifest::{WatchFilter, WatchQuery},
    provider::{ChainProvider, Log, ResolvedBlock, SharedProvider, provider_error},
};

use super::{
    prefetch::Prefetcher,
    query::{self, QueryContext},
};

pub(super) struct FetchedWindow {
    pub(super) resolved: Vec<ResolvedBlock>,
    pub(super) facts: FetchedBatch,
    pub(super) selected: Vec<Log>,
    pub(super) queries: Vec<WatchQuery>,
}

pub(super) struct WindowReader<'a> {
    pub(super) provider: &'a SharedProvider,
    pub(super) coinbase: Option<&'a CoinbaseSqlSource>,
    pub(super) prefetch: Option<&'a Prefetcher<'a>>,
    pub(super) filter: &'a WatchFilter,
}

impl WindowReader<'_> {
    /// Only provider reads are retried. Persistence and cursor advancement happen after
    /// this returns a completely validated window, and never consume a failed attempt.
    pub(super) async fn fetch(&self, from: i64, to: i64) -> Result<FetchedWindow> {
        let retry_rpc =
            self.coinbase.is_none() && matches!(self.provider.as_ref(), ChainProvider::JsonRpc(_));
        for attempt in 0..3 {
            match self.fetch_once(from, to, attempt == 0).await {
                Err(error) if retry_rpc && error.kind() == ErrorKind::DataIntegrity => {
                    if let Some(prefetch) = self.prefetch {
                        prefetch.invalidate().await;
                    }
                    if attempt == 2 {
                        return Err(error);
                    }
                    tracing::warn!(from, to, attempt = attempt + 1, %error,
                        "provider integrity mismatch; re-fetching ingest window without prefetch");
                    tokio::time::sleep(Duration::from_millis(250 * (attempt + 1))).await;
                }
                result => return result,
            }
        }
        unreachable!("the final attempt returns its result")
    }

    async fn fetch_once(&self, from: i64, to: i64, use_prefetch: bool) -> Result<FetchedWindow> {
        let numbers = (from..=to).collect::<Vec<_>>();
        let resolved = self.provider.resolve(&numbers).await.map_err(|error| {
            provider_error(
                &format!("failed to resolve ingest blocks {from}..={to}"),
                error,
            )
        })?;
        // Rebuild discovery admission too: no state from a rejected response may survive.
        let mut filter = self.filter.clone();
        let mut queries = filter.queries();
        let mut selected_by_identity = BTreeMap::new();
        let mut context = QueryContext {
            provider: self.provider,
            resolved: &resolved,
            coinbase: self.coinbase,
            prefetch: if use_prefetch { self.prefetch } else { None },
        };
        query::fetch_into(&context, &queries, &mut selected_by_identity).await?;
        for announcement_topic0 in filter.creation_topic0s() {
            let announcements = selected_by_identity
                .values()
                .filter(|log| {
                    log.topics
                        .first()
                        .is_some_and(|topic| topic.eq_ignore_ascii_case(&announcement_topic0))
                })
                .map(|log| (log.address.clone(), log.block_number))
                .collect::<BTreeSet<_>>();
            let supplemental =
                filter.admit_creation_announcements(&announcement_topic0, announcements, from, to);
            // Discovery queries always read the window itself, after admission.
            context.prefetch = None;
            query::fetch_into(&context, &supplemental, &mut selected_by_identity).await?;
            queries.extend(supplemental);
        }
        let mut selected = selected_by_identity.into_values().collect::<Vec<_>>();
        selected.retain(|log| filter.includes_log(&log.address, &log.topics, log.block_number));
        let facts =
            fetch_selected_facts(self.provider, &resolved, selected.clone(), &filter).await?;
        Ok(FetchedWindow {
            resolved,
            facts,
            selected,
            queries,
        })
    }
}

#[cfg(test)]
mod tests;
