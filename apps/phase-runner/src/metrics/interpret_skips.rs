//! Event logs Interpret skipped because they contradict themselves or their emitter's own earlier
//! logs, counted from each committed Interpret batch.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

use anyhow::Result;
use bigname_metrics::{IntCounterVec, MetricsRegistry};

type Totals = BTreeMap<(String, &'static str), u64>;

/// Running totals by chain and failed check, which the Interpret phase adds to.
#[derive(Clone, Default)]
pub(super) struct InconsistentLogSkipTotals {
    totals: Arc<Mutex<Totals>>,
}

impl InconsistentLogSkipTotals {
    pub(super) fn record(&self, chain: &str, skips: &BTreeMap<&'static str, u64>) {
        if skips.is_empty() {
            return;
        }
        let mut totals = self.totals.lock().unwrap_or_else(PoisonError::into_inner);
        for (site, count) in skips {
            *totals.entry((chain.to_owned(), *site)).or_default() += count;
        }
    }

    pub(super) fn snapshot(&self) -> Totals {
        self.totals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[derive(Clone)]
pub(super) struct InconsistentLogSkipCounters {
    skips: IntCounterVec,
}

impl InconsistentLogSkipCounters {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            skips: registry.int_counter_vec(
                "phase_runner_interpret_inconsistent_log_skips_total",
                "Event logs from emitters no manifest declares that committed Interpret batches \
                 skipped because they contradict themselves or the emitter's earlier logs, by \
                 the check that failed. A retried batch or a redo counts its skips again.",
                &["chain", "site"],
            )?,
        })
    }

    pub(super) fn apply(&self, totals: Totals) {
        for ((chain, site), total) in totals {
            let counter = self.skips.with_label_values(&[chain.as_str(), site]);
            counter.inc_by(total.saturating_sub(counter.get()));
        }
    }
}
