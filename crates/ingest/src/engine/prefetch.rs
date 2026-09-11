//! Wide-range log prefetch, sliced into ingest windows.
//!
//! A 256-block window used to cost one `eth_getLogs` round trip per watch query. The watch
//! filter's persisted queries do not change from window to window, so one query over a wide
//! range answers dozens of windows at once. The 256-block window stays the interpret,
//! project, lineage and commit unit; only the log lookups widen.
//!
//! A cached range is unpinned: the logs carry whatever block hash the provider reported at
//! prefetch time. A window may consume them only when every cached log's block hash equals
//! the hash that window resolved. Anything else drops the cache entry and refetches the
//! window's own range, so a reorg can never smuggle a stale log into a window.

use std::collections::BTreeMap;

use tokio::sync::Mutex;

use crate::{
    Result,
    manifest::WatchQuery,
    provider::{Log, ResolvedBlock, SharedProvider, provider_error},
};

/// Blocks covered by one wide-range log prefetch.
///
/// Forty ingest windows per round trip, at the upper end of what hosted endpoints answer
/// before demanding a narrower range. A `range_too_large` refusal halves the span exactly
/// as a window-sized query already does.
pub(crate) const PREFETCH_RANGE_BLOCKS: i64 = 10_000;

/// The persisted watch query a cached range answers.
///
/// Supplemental discovery queries are deliberately absent: their addresses are admitted
/// mid-window, so a range fetched before the announcement would be incomplete.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct QueryIdentity {
    addresses: Vec<String>,
    topic0s: Vec<String>,
}

impl QueryIdentity {
    fn of(query: &WatchQuery) -> Self {
        Self {
            addresses: query.addresses.clone(),
            topic0s: query.topic0s.clone(),
        }
    }
}

struct CachedRange {
    from: i64,
    to: i64,
    logs: Vec<Log>,
}

/// One prefetched range per provider and query identity; a new prefetch replaces it.
#[derive(Default)]
pub(crate) struct RangeLogCache {
    ranges: BTreeMap<(String, QueryIdentity), CachedRange>,
}

impl RangeLogCache {
    fn covered(&self, key: &(String, QueryIdentity), from: i64, to: i64) -> Option<Vec<Log>> {
        let cached = self.ranges.get(key)?;
        (cached.from <= from && to <= cached.to).then(|| {
            cached
                .logs
                .iter()
                .filter(|log| (from..=to).contains(&log.block_number))
                .cloned()
                .collect()
        })
    }
}

/// Consults and fills the cache for one batch's provider.
pub(crate) struct Prefetcher<'a> {
    cache: &'a Mutex<RangeLogCache>,
    provider_key: String,
    /// Highest block a prefetch may reach.
    ///
    /// Prefetched ranges are read ahead of the window that consumes them, so a block whose
    /// lineage can still change must never be prefetched: a block that gains a log after
    /// the prefetch would leave the window short, and no later check would notice, because
    /// the window's re-resolve only covers blocks that already returned a log.
    ceiling: i64,
}

impl<'a> Prefetcher<'a> {
    pub(crate) fn new(cache: &'a Mutex<RangeLogCache>, provider_key: String, ceiling: i64) -> Self {
        Self {
            cache,
            provider_key,
            ceiling,
        }
    }

    /// Logs for `from..=to` of one persisted watch query, from cache where possible.
    pub(crate) async fn logs(
        &self,
        provider: &SharedProvider,
        resolved: &[ResolvedBlock],
        query: &WatchQuery,
        from: i64,
        to: i64,
    ) -> Result<Vec<Log>> {
        let key = (self.provider_key.clone(), QueryIdentity::of(query));
        if let Some(logs) = self.cache.lock().await.covered(&key, from, to)
            && let Some(logs) = pinned(resolved, logs)
        {
            return Ok(logs);
        }
        self.cache.lock().await.ranges.remove(&key);
        if to > self.ceiling {
            return window_logs(provider, resolved, query, from, to).await;
        }
        // A watch query's own bounds are already clipped to the window that loaded the
        // filter, so they cannot bound the prefetch: doing so would read exactly the
        // window again. The prefetch reads the wide range and each window takes only the
        // slice its own query admits, so reading past a watch's true end costs bytes and
        // never widens what a window stores.
        let range_to = from
            .saturating_add(PREFETCH_RANGE_BLOCKS - 1)
            .min(self.ceiling);
        let logs = provider
            .prefetch_range_logs(from, range_to, &query.addresses, &query.topic0s)
            .await
            .map_err(|error| provider_error("failed to prefetch selected chain logs", error))?;
        let Some(logs) = logs else {
            return window_logs(provider, resolved, query, from, to).await;
        };
        self.cache.lock().await.ranges.insert(
            key,
            CachedRange {
                from,
                to: range_to,
                logs,
            },
        );
        let window = self
            .cache
            .lock()
            .await
            .covered(
                &(self.provider_key.clone(), QueryIdentity::of(query)),
                from,
                to,
            )
            .unwrap_or_default();
        match pinned(resolved, window) {
            Some(logs) => Ok(logs),
            // The provider answered the wide range from a lineage this window does not
            // share. Fall back to the window's own range so the failure is the usual
            // mid-fetch reorg error rather than silently dropped logs.
            None => window_logs(provider, resolved, query, from, to).await,
        }
    }
}

/// Accepts cached logs only when every one of them sits on a hash this window resolved.
fn pinned(resolved: &[ResolvedBlock], logs: Vec<Log>) -> Option<Vec<Log>> {
    let hash_by_number = resolved
        .iter()
        .map(|block| (block.number, block.hash.as_str()))
        .collect::<BTreeMap<_, _>>();
    logs.iter()
        .all(|log| hash_by_number.get(&log.block_number) == Some(&log.block_hash.as_str()))
        .then_some(logs)
}

/// The window's own range query, used when no prefetch applies.
pub(crate) async fn window_logs(
    provider: &SharedProvider,
    resolved: &[ResolvedBlock],
    query: &WatchQuery,
    from: i64,
    to: i64,
) -> Result<Vec<Log>> {
    provider
        .range_logs(resolved, from, to, &query.addresses, &query.topic0s)
        .await
        .map_err(|error| provider_error("failed to fetch selected chain logs", error))
}
