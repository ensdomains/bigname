use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// Running totals of the JSON-RPC traffic the providers of one ingest engine send, by chain and
/// configured source. Totals only grow, so a reader can export them as counters.
#[derive(Clone, Default)]
pub struct RpcCounters {
    totals: Arc<Mutex<BTreeMap<RpcCount, u64>>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RpcCount {
    pub kind: RpcCountKind,
    pub chain: String,
    pub source: String,
    /// The JSON-RPC method, or for [`RpcCountKind::Requests`] the request's outcome.
    pub label: String,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RpcCountKind {
    /// HTTP requests sent, one per single call or batch, labelled `ok` or `failed`.
    Requests,
    /// JSON-RPC calls sent, each call of a batch counted once.
    Calls,
    /// Receipts and transactions of selected logs that the provider answered null.
    NullResults,
}

impl RpcCounters {
    pub fn snapshot(&self) -> BTreeMap<RpcCount, u64> {
        self.lock().clone()
    }

    pub(crate) fn source(&self, chain: &str, source: &str) -> SourceCounters {
        SourceCounters {
            counters: self.clone(),
            chain: chain.to_owned(),
            source: source.to_owned(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<RpcCount, u64>> {
        self.totals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The counters of one configured source.
#[derive(Clone, Default)]
pub(crate) struct SourceCounters {
    counters: RpcCounters,
    chain: String,
    source: String,
}

impl SourceCounters {
    pub(super) fn add<'a>(&self, kind: RpcCountKind, labels: impl IntoIterator<Item = &'a str>) {
        let mut totals = self.counters.lock();
        for label in labels {
            *totals
                .entry(RpcCount {
                    kind,
                    chain: self.chain.clone(),
                    source: self.source.clone(),
                    label: label.to_owned(),
                })
                .or_default() += 1;
        }
    }
}
