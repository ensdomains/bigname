//! Bounded family publication timing, lag and duplicate-event counters.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use bigname_metrics::{GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec, MetricsRegistry};
use bigname_project::families::FamilyOutcome;

/// Batches Project reported that the metrics task has not applied yet: the newest summary of each
/// chain, the rows written since the last apply, and up to [`MAX_PENDING_FAMILY_BLOCKS`] family
/// block times per chain. All stay bounded however long the task waits.
#[derive(Clone, Default)]
pub(super) struct PendingProjectWrites {
    inner: Arc<Mutex<Pending>>,
}

/// Family block times kept per chain until the metrics task applies them; further blocks are not
/// observed. The task wakes on every family run, so only a feed nothing drains reaches it.
const MAX_PENDING_FAMILY_BLOCKS: usize = 65_536;

#[derive(Default)]
pub(super) struct Pending {
    families: BTreeMap<String, (f64, u64)>,
    family_block_ms: BTreeMap<String, Vec<u64>>,
    family_anomalies: BTreeMap<String, u64>,
}

impl PendingProjectWrites {
    pub(super) fn record_families(&self, chain: &str, outcome: &FamilyOutcome) {
        let mut pending = lock(&self.inner);
        pending.families.insert(
            chain.to_owned(),
            (outcome.elapsed_ms as f64 / 1_000.0, outcome.lag_blocks()),
        );
        let block_ms = pending.family_block_ms.entry(chain.to_owned()).or_default();
        let room = MAX_PENDING_FAMILY_BLOCKS.saturating_sub(block_ms.len());
        block_ms.extend(outcome.block_ms.iter().take(room));
        let anomalies = pending
            .family_anomalies
            .entry(chain.to_owned())
            .or_default();
        *anomalies += outcome.duplicate_anomalies;
    }

    pub(super) fn take(&self) -> Pending {
        std::mem::take(&mut *lock(&self.inner))
    }
}

#[derive(Clone)]
pub(super) struct ProjectWriteGauges {
    families_seconds: GaugeVec,
    family_lag: IntGaugeVec,
    family_block: HistogramVec,
    family_anomalies: IntCounterVec,
}

impl ProjectWriteGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            families_seconds: registry.gauge_vec(
                "phase_runner_project_families_seconds",
                "Wall time of the newest Project family publication run.",
                &["chain"],
            )?,
            family_lag: registry.int_gauge_vec(
                "phase_runner_project_family_lag_blocks",
                "Blocks between the family publication and its Project target after the newest run.",
                &["chain"],
            )?,
            family_block: registry.histogram_vec(
                "phase_runner_project_family_block_seconds",
                "Wall time of each owned key family block applied in a transaction of its own, \
                 from its first read to its commit; a rebuild range is not a block and is not \
                 observed.",
                &["chain"],
            )?,
            family_anomalies: registry.int_counter_vec(
                "phase_runner_project_family_duplicate_anomalies_total",
                "Deliveries of one normalized event identity whose position or payload disagreed \
                 with the one the family loop kept.",
                &["chain"],
            )?,
        })
    }

    pub(super) fn apply(&self, pending: Pending) {
        for (chain, (seconds, lag)) in pending.families {
            self.families_seconds
                .with_label_values(&[&chain])
                .set(seconds);
            self.family_lag
                .with_label_values(&[&chain])
                .set(gauge_value(lag));
        }
        for (chain, block_ms) in pending.family_block_ms {
            let histogram = self.family_block.with_label_values(&[&chain]);
            for elapsed_ms in block_ms {
                histogram.observe(elapsed_ms as f64 / 1_000.0);
            }
        }
        for (chain, anomalies) in pending.family_anomalies {
            self.family_anomalies
                .with_label_values(&[&chain])
                .inc_by(anomalies);
        }
    }
}

fn gauge_value(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
